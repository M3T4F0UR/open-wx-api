"""State air quality and environmental monitoring — no auth required.

Covers:
  1. California CARB  — Real-time PM2.5 and ozone from 115+ monitors (CSV)
  2. Minnesota Soil Temperature — 48 stations, 6-inch depth (ArcGIS FeatureServer)
  3. NYC FloodNet — 23+ hyperlocal flood depth sensors (API)
  4. Adirondack Lakes Survey — 52 lakes, acid rain / water chemistry (CSV)

All sources are free with no authentication required.
"""
from __future__ import annotations

import csv
import io
import logging
from datetime import datetime, timezone
from typing import Optional

from fastapi import APIRouter, HTTPException, Query

import cache
import config
import http_client
from models import Reading, Station

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Source registry metadata
# ---------------------------------------------------------------------------
SOURCE_NAME = "State Environmental Monitoring"
SOURCE_DESCRIPTION = (
    "State-level air quality and environmental monitoring: California CARB "
    "real-time PM2.5/ozone, Minnesota 6-inch soil temperature, NYC FloodNet "
    "flood depth sensors, and Adirondack Lakes Survey water chemistry."
)
SOURCE_META = {
    "category": "environmental/air_quality",
    "coverage": "California, Minnesota, New York",
    "auth_required": False,
    "update_frequency": "Hourly to seasonal",
    "params": [
        "pm2.5", "ozone", "soil_temperature", "flood_depth",
        "pH", "alkalinity", "conductivity", "dissolved_oxygen",
    ],
    "networks": ["carb_pm25", "carb_ozone", "mn_soil_temp", "nyc_floodnet", "adirondack_lakes"],
    "urls": {
        "carb_pm25": "https://www.arb.ca.gov/aqmis2/display.php",
        "carb_ozone": "https://www.arb.ca.gov/aqmis2/display.php",
        "mn_soil_temp": "https://gis.mda.state.mn.us/mda-soiltemp/",
        "nyc_floodnet": "https://www.floodnet.nyc/",
        "adirondack_lakes": "http://www.adirondacklakessurvey.org/",
    },
}

router = APIRouter(prefix="/api/state_env", tags=["State Environmental Monitoring"])

# ---------------------------------------------------------------------------
# Helper: safe float / datetime
# ---------------------------------------------------------------------------

def _safe_float(val) -> Optional[float]:
    if val is None:
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


def _parse_iso(val) -> Optional[datetime]:
    if not val:
        return None
    try:
        if isinstance(val, str):
            return datetime.fromisoformat(val.replace("Z", "+00:00"))
    except (ValueError, AttributeError):
        pass
    return None


# ---------------------------------------------------------------------------
# ── 1. California CARB Real-Time Air Quality ──────────────────────────────
# ---------------------------------------------------------------------------

_CARB_PM25_URL = "https://www.arb.ca.gov/aqmis2/download/pm25hourly/pm25hourly.csv"
_CARB_OZONE_URL = "https://www.arb.ca.gov/aqmis2/download/ozone/ozone.csv"


def _parse_carb_csv(text: str, pollutant: str) -> list[Station]:
    """Parse a CARB hourly CSV file into Station objects.

    CARB CSVs typically have columns like:
        site, name, date, start_hour, value, unit, ...
    Column names vary — we adapt to what's present.
    """
    stations: list[Station] = []
    if not text or not text.strip():
        return stations

    # Strip BOM if present
    text = text.lstrip("\ufeff")

    reader = csv.DictReader(io.StringIO(text.strip()))
    # Normalise header names to lowercase
    if reader.fieldnames:
        reader.fieldnames = [f.strip().lower() for f in reader.fieldnames]

    seen: dict[str, Station] = {}  # keyed by site id to deduplicate

    for row in reader:
        try:
            # Try common column names for the site identifier
            site_id = (
                row.get("site") or row.get("site_code") or row.get("gmt_id")
                or row.get("aqsid") or ""
            ).strip()
            site_name = (
                row.get("name") or row.get("site_name") or row.get("monitor_name")
                or site_id
            ).strip()
            if not site_id:
                continue

            # Value
            raw_val = (
                row.get("value") or row.get("concentration") or row.get("hourly_value")
                or row.get("average") or ""
            ).strip()
            if not raw_val or raw_val in ("", "-", "N/A", "NA", "--"):
                continue
            value = float(raw_val)

            # Timestamp — try date + hour columns
            date_str = (row.get("date") or row.get("sample_date") or "").strip()
            hour_str = (row.get("start_hour") or row.get("hour") or "0").strip()
            ts = None
            if date_str:
                for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%m-%d-%Y"):
                    try:
                        dt = datetime.strptime(date_str, fmt)
                        try:
                            hr = int(hour_str)
                        except (ValueError, TypeError):
                            hr = 0
                        ts = dt.replace(hour=hr, tzinfo=timezone.utc)
                        break
                    except ValueError:
                        continue

            # Location
            lat = _safe_float(row.get("latitude") or row.get("lat"))
            lon = _safe_float(row.get("longitude") or row.get("lon") or row.get("long"))

            unit = (row.get("unit") or row.get("units") or "").strip()
            if not unit:
                unit = "ug/m3" if pollutant == "pm2.5" else "ppb"

            station = Station(
                id=f"carb_{pollutant}_{site_id}".replace(" ", "_").lower(),
                name=f"CARB {pollutant.upper()} — {site_name}",
                lat=lat or 0.0,
                lon=lon or 0.0,
                source=f"carb_{pollutant}",
                network="California CARB",
                params=[pollutant],
                last_update=ts,
                extra={
                    "pollutant": pollutant,
                    "value": value,
                    "unit": unit,
                    "site_id": site_id,
                },
            )

            # Keep the most recent reading per site
            if site_id not in seen or (ts and (seen[site_id].last_update is None or ts > seen[site_id].last_update)):
                seen[site_id] = station

        except Exception:
            continue

    return list(seen.values())


@router.get("/carb/pm25")
async def carb_pm25(
    limit: int = Query(500, description="Maximum stations to return"),
):
    """California CARB real-time hourly PM2.5 from 115+ monitors.

    Returns the latest hourly PM2.5 concentrations from California Air
    Resources Board monitoring stations statewide.
    """
    cache_key = f"state_env:carb_pm25:{limit}"
    cached_val = await cache.get(cache_key)
    if cached_val is not None:
        return cached_val

    try:
        text = await http_client.fetch_text(
            _CARB_PM25_URL, source="carb_pm25", timeout=45.0,
        )
    except Exception as exc:
        logger.error("CARB PM2.5 CSV fetch failed: %s", exc)
        raise HTTPException(status_code=502, detail=f"CARB PM2.5 data unavailable: {exc}")

    stations = _parse_carb_csv(text, "pm2.5")
    if limit:
        stations = stations[:limit]

    result = {
        "source": "carb_pm25",
        "network": "California CARB",
        "count": len(stations),
        "stations": [s.model_dump(mode="json") for s in stations],
    }
    await cache.set(cache_key, result, ttl=config.CACHE_TTL)
    return result


@router.get("/carb/ozone")
async def carb_ozone(
    limit: int = Query(500, description="Maximum stations to return"),
):
    """California CARB real-time hourly ozone from 115+ monitors.

    Returns the latest hourly ozone concentrations from California Air
    Resources Board monitoring stations statewide.
    """
    cache_key = f"state_env:carb_ozone:{limit}"
    cached_val = await cache.get(cache_key)
    if cached_val is not None:
        return cached_val

    try:
        text = await http_client.fetch_text(
            _CARB_OZONE_URL, source="carb_ozone", timeout=45.0,
        )
    except Exception as exc:
        logger.error("CARB ozone CSV fetch failed: %s", exc)
        raise HTTPException(status_code=502, detail=f"CARB ozone data unavailable: {exc}")

    stations = _parse_carb_csv(text, "ozone")
    if limit:
        stations = stations[:limit]

    result = {
        "source": "carb_ozone",
        "network": "California CARB",
        "count": len(stations),
        "stations": [s.model_dump(mode="json") for s in stations],
    }
    await cache.set(cache_key, result, ttl=config.CACHE_TTL)
    return result


# ---------------------------------------------------------------------------
# ── 2. Minnesota 6-Inch Soil Temperature (ArcGIS FeatureServer) ──────────
# ---------------------------------------------------------------------------

# The MDA soil temp map is backed by an ArcGIS FeatureServer.
_MN_SOIL_TEMP_URL = (
    "https://gis.mda.state.mn.us/arcgis/rest/services/mda_soiltemp/MapServer/0/query"
)


def _parse_mn_soil_feature(feat: dict) -> Optional[Station]:
    """Parse an ArcGIS feature into a Station for MN soil temp."""
    attrs = feat.get("attributes", {})
    geom = feat.get("geometry", {})

    site_id = str(attrs.get("OBJECTID", attrs.get("StationID", attrs.get("station_id", ""))))
    name = attrs.get("StationName", attrs.get("station_name", attrs.get("Name", f"MN Soil #{site_id}")))
    if not site_id:
        return None

    lat = _safe_float(geom.get("y") or attrs.get("Latitude") or attrs.get("latitude"))
    lon = _safe_float(geom.get("x") or attrs.get("Longitude") or attrs.get("longitude"))

    # Soil temperature — try several possible field names
    soil_temp = _safe_float(
        attrs.get("SoilTemp6in") or attrs.get("soiltemp_6in") or attrs.get("Temp6in")
        or attrs.get("SoilTemp") or attrs.get("soil_temp") or attrs.get("Temperature")
    )

    # Timestamp
    ts_raw = attrs.get("ReadDate") or attrs.get("read_date") or attrs.get("DateTime")
    ts = None
    if ts_raw:
        if isinstance(ts_raw, (int, float)) and ts_raw > 1e12:
            # ArcGIS epoch milliseconds
            ts = datetime.fromtimestamp(ts_raw / 1000, tz=timezone.utc)
        elif isinstance(ts_raw, (int, float)):
            ts = datetime.fromtimestamp(ts_raw, tz=timezone.utc)
        elif isinstance(ts_raw, str):
            ts = _parse_iso(ts_raw)

    return Station(
        id=f"mn_soil_{site_id}",
        name=f"MN Soil Temp — {name}",
        lat=lat or 46.0,
        lon=lon or -94.0,
        source="mn_soil_temp",
        network="Minnesota MDA",
        params=["soil_temperature"],
        last_update=ts,
        extra={
            "soil_temp_6in_f": soil_temp,
            "unit": "F",
            "county": attrs.get("County", attrs.get("county")),
        },
    )


@router.get("/mn/soil_temp")
async def mn_soil_temperature():
    """Minnesota 6-inch soil temperature from ~48 MDA stations.

    Returns the latest 6-inch soil temperature readings across Minnesota
    from the Minnesota Department of Agriculture monitoring network.
    Data is sourced from the MDA ArcGIS FeatureServer.
    """
    cache_key = "state_env:mn_soil_temp"
    cached_val = await cache.get(cache_key)
    if cached_val is not None:
        return cached_val

    params = {
        "where": "1=1",
        "outFields": "*",
        "f": "json",
        "returnGeometry": "true",
    }

    try:
        data = await http_client.fetch_json(
            _MN_SOIL_TEMP_URL, params=params, source="mn_soil_temp", timeout=30.0,
        )
    except Exception as exc:
        logger.error("MN soil temperature fetch failed: %s", exc)
        raise HTTPException(status_code=502, detail=f"MN soil temp data unavailable: {exc}")

    features = data.get("features", [])
    stations: list[Station] = []
    for feat in features:
        try:
            st = _parse_mn_soil_feature(feat)
            if st:
                stations.append(st)
        except Exception as exc:
            logger.debug("Skipping malformed MN soil feature: %s", exc)

    result = {
        "source": "mn_soil_temp",
        "network": "Minnesota MDA",
        "count": len(stations),
        "stations": [s.model_dump(mode="json") for s in stations],
    }
    await cache.set(cache_key, result, ttl=config.CACHE_TTL)
    return result


# ---------------------------------------------------------------------------
# ── 3. NYC FloodNet — Hyperlocal Flood Depth Sensors ─────────────────────
# ---------------------------------------------------------------------------

# FloodNet provides a public API for sensor data.
_FLOODNET_API_URL = "https://api.floodnet.nyc/v1/sensors"
_FLOODNET_DATA_URL = "https://api.floodnet.nyc/v1/data"


def _parse_floodnet_sensor(sensor: dict) -> Station:
    """Parse a FloodNet sensor record into a Station."""
    sensor_id = sensor.get("sensor_id", sensor.get("deployment_id", ""))
    name = sensor.get("location_name", sensor.get("name", sensor.get("street_address", f"FloodNet #{sensor_id}")))

    lat = _safe_float(sensor.get("latitude") or sensor.get("lat"))
    lon = _safe_float(sensor.get("longitude") or sensor.get("lon") or sensor.get("lng"))

    # Latest depth reading
    depth = _safe_float(sensor.get("latest_depth") or sensor.get("depth") or sensor.get("flood_depth"))
    ts = _parse_iso(sensor.get("latest_time") or sensor.get("updated_at") or sensor.get("timestamp"))

    return Station(
        id=f"floodnet_{sensor_id}",
        name=f"FloodNet — {name}",
        lat=lat or 40.7,
        lon=lon or -74.0,
        source="nyc_floodnet",
        network="NYC FloodNet",
        params=["flood_depth"],
        last_update=ts,
        extra={
            "flood_depth_in": depth,
            "unit": "inches",
            "sensor_id": sensor_id,
            "status": sensor.get("status", sensor.get("sensor_status")),
            "neighborhood": sensor.get("neighborhood", sensor.get("community_district")),
        },
    )


@router.get("/nyc/floodnet")
async def nyc_floodnet():
    """NYC FloodNet hyperlocal flood depth sensors (23+ sensors).

    Returns the latest flood depth readings from FloodNet's network of
    ultrasonic depth sensors deployed at flood-prone locations across NYC.
    """
    cache_key = "state_env:nyc_floodnet"
    cached_val = await cache.get(cache_key)
    if cached_val is not None:
        return cached_val

    # Try the public API endpoint
    try:
        data = await http_client.fetch_json(
            _FLOODNET_API_URL, source="nyc_floodnet", timeout=30.0,
        )
    except Exception as exc:
        logger.error("NYC FloodNet sensor list fetch failed: %s", exc)
        raise HTTPException(status_code=502, detail=f"NYC FloodNet data unavailable: {exc}")

    # Response may be a list or wrapped in a dict
    sensors = data if isinstance(data, list) else data.get("sensors", data.get("data", data.get("features", [])))

    stations: list[Station] = []
    for sensor in sensors:
        try:
            stations.append(_parse_floodnet_sensor(sensor))
        except Exception as exc:
            logger.debug("Skipping malformed FloodNet sensor: %s", exc)

    result = {
        "source": "nyc_floodnet",
        "network": "NYC FloodNet",
        "count": len(stations),
        "stations": [s.model_dump(mode="json") for s in stations],
    }
    await cache.set(cache_key, result, ttl=config.CACHE_TTL)
    return result


# ---------------------------------------------------------------------------
# ── 4. Adirondack Lakes Survey — Acid Rain / Water Chemistry ─────────────
# ---------------------------------------------------------------------------

# The ALS provides downloadable CSV data.
_ALS_DATA_URL = "http://www.adirondacklakessurvey.org/data/als_lake_chemistry.csv"

# Fallback: if the CSV endpoint isn't directly available, try the data page.
_ALS_DATA_PAGE = "http://www.adirondacklakessurvey.org/data.shtml"

# Known ALS lakes with coordinates (core subset of 52 long-term monitoring lakes)
_ALS_KNOWN_LAKES: list[dict] = [
    {"id": "040752", "name": "Big Moose Lake",        "lat": 43.821, "lon": -74.853},
    {"id": "040874", "name": "Brooktrout Lake",       "lat": 43.809, "lon": -74.583},
    {"id": "060115", "name": "Cascade Lake",          "lat": 43.863, "lon": -74.835},
    {"id": "050259", "name": "Dart Lake",             "lat": 43.785, "lon": -74.816},
    {"id": "040576", "name": "Lake Rondaxe",          "lat": 43.795, "lon": -74.918},
    {"id": "020059", "name": "Heart Lake",            "lat": 44.172, "lon": -73.967},
    {"id": "040707", "name": "Moss Lake",             "lat": 43.787, "lon": -74.847},
    {"id": "060313", "name": "Queer Lake",            "lat": 43.774, "lon": -74.820},
    {"id": "040750", "name": "West Pond (Big Moose)", "lat": 43.825, "lon": -74.865},
    {"id": "070717", "name": "Arbutus Lake",          "lat": 43.980, "lon": -74.235},
    {"id": "041007", "name": "Sagamore Lake",         "lat": 43.771, "lon": -74.526},
    {"id": "020070", "name": "Avalanche Lake",        "lat": 44.143, "lon": -73.985},
    {"id": "050669", "name": "Windfall Pond",         "lat": 43.823, "lon": -74.680},
    {"id": "030170", "name": "Clear Pond",            "lat": 44.053, "lon": -74.260},
    {"id": "060187", "name": "Constable Pond",        "lat": 43.837, "lon": -74.790},
]


def _parse_als_csv(text: str) -> list[Station]:
    """Parse Adirondack Lakes Survey CSV into Station objects."""
    stations: list[Station] = []
    if not text or not text.strip():
        return stations

    text = text.lstrip("\ufeff")
    reader = csv.DictReader(io.StringIO(text.strip()))
    if reader.fieldnames:
        reader.fieldnames = [f.strip().lower() for f in reader.fieldnames]

    # Build lookup for known lake coordinates
    lake_coords = {lk["id"]: lk for lk in _ALS_KNOWN_LAKES}
    lake_coords_by_name = {lk["name"].lower(): lk for lk in _ALS_KNOWN_LAKES}

    seen: dict[str, Station] = {}

    for row in reader:
        try:
            lake_id = (
                row.get("lake_id") or row.get("pondcode") or row.get("pond_code")
                or row.get("lake") or row.get("id") or ""
            ).strip()
            lake_name = (
                row.get("lake_name") or row.get("pondname") or row.get("pond_name")
                or row.get("name") or lake_id
            ).strip()
            if not lake_id and not lake_name:
                continue

            # Get coordinates from known lakes or from data
            lat = _safe_float(row.get("latitude") or row.get("lat"))
            lon = _safe_float(row.get("longitude") or row.get("lon") or row.get("long"))
            known = lake_coords.get(lake_id) or lake_coords_by_name.get(lake_name.lower())
            if known:
                lat = lat or known["lat"]
                lon = lon or known["lon"]
                lake_name = lake_name or known["name"]

            # Chemistry parameters
            ph = _safe_float(row.get("ph") or row.get("field_ph"))
            alk = _safe_float(row.get("alkalinity") or row.get("alk") or row.get("anc"))
            cond = _safe_float(row.get("conductivity") or row.get("cond") or row.get("sp_cond"))
            do_val = _safe_float(row.get("dissolved_oxygen") or row.get("do") or row.get("do_mg_l"))
            sulfate = _safe_float(row.get("sulfate") or row.get("so4"))
            nitrate = _safe_float(row.get("nitrate") or row.get("no3"))
            calcium = _safe_float(row.get("calcium") or row.get("ca"))

            # Date
            date_str = (row.get("date") or row.get("sample_date") or row.get("collection_date") or "").strip()
            ts = None
            if date_str:
                for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%m-%d-%Y", "%d-%b-%y"):
                    try:
                        ts = datetime.strptime(date_str, fmt).replace(tzinfo=timezone.utc)
                        break
                    except ValueError:
                        continue

            key = lake_id or lake_name
            station = Station(
                id=f"als_{lake_id or lake_name.replace(' ', '_').lower()}",
                name=f"ALS — {lake_name}",
                lat=lat or 43.9,
                lon=lon or -74.5,
                source="adirondack_lakes",
                network="Adirondack Lakes Survey",
                params=["pH", "alkalinity", "conductivity", "dissolved_oxygen"],
                last_update=ts,
                extra={
                    k: v for k, v in {
                        "pH": ph,
                        "alkalinity_ueq_l": alk,
                        "conductivity_us_cm": cond,
                        "dissolved_oxygen_mg_l": do_val,
                        "sulfate_ueq_l": sulfate,
                        "nitrate_ueq_l": nitrate,
                        "calcium_ueq_l": calcium,
                    }.items() if v is not None
                },
            )

            # Keep most recent sample per lake
            if key not in seen or (ts and (seen[key].last_update is None or ts > seen[key].last_update)):
                seen[key] = station

        except Exception:
            continue

    return list(seen.values())


@router.get("/adirondack/lakes")
async def adirondack_lakes(
    limit: int = Query(100, description="Maximum lakes to return"),
):
    """Adirondack Lakes Survey water chemistry for 52 long-term monitoring lakes.

    Returns the latest water chemistry data (pH, alkalinity, conductivity,
    dissolved oxygen, sulfate, nitrate, calcium) from the Adirondack Lakes
    Survey Corporation, a unique long-term acid rain monitoring program.
    """
    cache_key = f"state_env:als:{limit}"
    cached_val = await cache.get(cache_key)
    if cached_val is not None:
        return cached_val

    # Try the direct CSV download first
    stations: list[Station] = []
    try:
        text = await http_client.fetch_text(
            _ALS_DATA_URL, source="adirondack_lakes", timeout=30.0,
        )
        stations = _parse_als_csv(text)
    except Exception as exc:
        logger.warning("ALS direct CSV failed (%s), returning known lakes only", exc)

    # If no data parsed, return the known lakes as metadata-only stations
    if not stations:
        stations = [
            Station(
                id=f"als_{lk['id']}",
                name=f"ALS — {lk['name']}",
                lat=lk["lat"],
                lon=lk["lon"],
                source="adirondack_lakes",
                network="Adirondack Lakes Survey",
                params=["pH", "alkalinity", "conductivity", "dissolved_oxygen"],
                extra={"note": "Known long-term monitoring lake; live data temporarily unavailable"},
            )
            for lk in _ALS_KNOWN_LAKES
        ]

    if limit:
        stations = stations[:limit]

    result = {
        "source": "adirondack_lakes",
        "network": "Adirondack Lakes Survey",
        "count": len(stations),
        "stations": [s.model_dump(mode="json") for s in stations],
    }
    await cache.set(cache_key, result, ttl=config.CACHE_TTL_LONG)
    return result


# ---------------------------------------------------------------------------
# Overview
# ---------------------------------------------------------------------------

@router.get("/overview")
async def state_env_overview():
    """Summary of all state environmental monitoring sources.

    Returns metadata for each integrated data source including API URLs,
    coverage, and available parameters.
    """
    return {
        "source": "state_environmental_monitoring",
        "sources": [
            {
                "id": "carb_pm25",
                "name": "California CARB PM2.5",
                "state": "California",
                "endpoint": "/api/state_env/carb/pm25",
                "data_url": _CARB_PM25_URL,
                "params": ["pm2.5"],
                "stations": "115+",
                "auth_required": False,
                "update_frequency": "Hourly",
                "notes": "Direct CSV download from California Air Resources Board",
            },
            {
                "id": "carb_ozone",
                "name": "California CARB Ozone",
                "state": "California",
                "endpoint": "/api/state_env/carb/ozone",
                "data_url": _CARB_OZONE_URL,
                "params": ["ozone"],
                "stations": "115+",
                "auth_required": False,
                "update_frequency": "Hourly",
                "notes": "Direct CSV download from California Air Resources Board",
            },
            {
                "id": "mn_soil_temp",
                "name": "Minnesota 6-Inch Soil Temperature",
                "state": "Minnesota",
                "endpoint": "/api/state_env/mn/soil_temp",
                "data_url": _MN_SOIL_TEMP_URL,
                "params": ["soil_temperature"],
                "stations": "~48",
                "auth_required": False,
                "update_frequency": "Daily",
                "notes": "ArcGIS FeatureServer from MN Dept of Agriculture",
            },
            {
                "id": "nyc_floodnet",
                "name": "NYC FloodNet Flood Depth Sensors",
                "state": "New York",
                "endpoint": "/api/state_env/nyc/floodnet",
                "data_url": _FLOODNET_API_URL,
                "params": ["flood_depth"],
                "stations": "23+",
                "auth_required": False,
                "update_frequency": "Near-real-time",
                "notes": "Hyperlocal ultrasonic flood depth sensors across NYC",
            },
            {
                "id": "adirondack_lakes",
                "name": "Adirondack Lakes Survey",
                "state": "New York",
                "endpoint": "/api/state_env/adirondack/lakes",
                "data_url": _ALS_DATA_URL,
                "params": ["pH", "alkalinity", "conductivity", "dissolved_oxygen"],
                "stations": "52",
                "auth_required": False,
                "update_frequency": "Seasonal (quarterly sampling)",
                "notes": "Long-term acid rain monitoring — unique water chemistry dataset since 1992",
            },
        ],
    }
