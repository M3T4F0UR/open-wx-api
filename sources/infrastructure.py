"""Infrastructure Weather Networks — state DOTs and agricultural mesonets.

Five high-value, no-auth data sources discovered through deep research into
public infrastructure operator APIs:

  1. TexMesonet (Texas Water Development Board)
       133 stations statewide — weather, soil, and agro-meteorological data.
       https://www.texmesonet.org/api/

  2. Caltrans CWWP2 RWIS (California DOT)
       Road Weather Information System stations across Caltrans districts.
       Road surface temperature, pavement condition, standard met variables.
       https://cwwp2.dot.ca.gov/data/d{N}/rwis/rwisStatusD0{N}.json

  3. Colorado DOT CoTrip RWIS (CDOT)
       Statewide highway weather stations via public ArcGIS FeatureServer.
       https://maps.codot.gov/server/rest/services/Hosted/CoTrip_Weather_Stations_(Live)_Public_View/FeatureServer/0/

  4. Iowa DOT ArcGIS RWIS Surface Data
       Pavement surface conditions, ice percentage, friction data via ArcGIS.
       https://services.arcgis.com/8lRhdTsQyJpO52F1/arcgis/rest/services/RWIS_Surface_Data_View/FeatureServer/0/

  5. USBR AgriMet (Bureau of Reclamation Agricultural Met)
       200+ stations in the Pacific Northwest and western US measuring
       agro-met parameters: ET, wind run, soil temperature, solar radiation.
       https://www.usbr.gov/pn/agrimet/

All five APIs are public and require no authentication.
Note: NOAA CO-OPS coastal met stations (318 stations) are implemented
in noaa_water.py under /api/noaa_water/coops/.
"""
from __future__ import annotations

import logging
import re
from datetime import datetime, timezone
from typing import Any, Optional

import httpx
from fastapi import APIRouter, HTTPException, Query

import cache
import config
from models import Reading, Station

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Source registry metadata  (read by server.py /api/sources)
# ---------------------------------------------------------------------------

SOURCE_NAME = "Infrastructure Weather Networks"
SOURCE_DESCRIPTION = (
    "No-auth infrastructure and agricultural weather networks: "
    "TexMesonet/TWDB (133 TX stations), Caltrans CWWP2 RWIS (road weather, CA), "
    "Colorado DOT CoTrip RWIS, Iowa DOT ArcGIS RWIS surface data, and "
    "USBR AgriMet (200+ western US agricultural stations)."
)
SOURCE_META = {
    "url": "https://www.texmesonet.org/api/",
    "auth_required": False,
    "update_frequency": "5–15 min depending on network",
    "coverage": "TX, CA, CO, IA, OR, ID, WA, MT, WY, NV",
    "params": [
        "temperature", "humidity", "pressure", "wind_speed", "wind_direction",
        "wind_gust", "precipitation", "solar_radiation",
        "soil_moisture", "soil_temperature", "road_temperature", "road_condition",
    ],
    "category": "weather",
}

router = APIRouter(prefix="/api/infrastructure", tags=["Infrastructure Networks"])

# ---------------------------------------------------------------------------
# Shared HTTP client
# ---------------------------------------------------------------------------

_HEADERS = {
    "User-Agent": config.USER_AGENT,
    "Accept": "application/json",
}


def _client() -> httpx.AsyncClient:
    return httpx.AsyncClient(
        headers=_HEADERS,
        timeout=config.HTTP_TIMEOUT,
        follow_redirects=True,
    )


async def _get_json(url: str, params: Optional[dict] = None, label: str = "upstream") -> Any:
    """GET url and return parsed JSON; raise HTTPException on failure."""
    try:
        async with _client() as client:
            resp = await client.get(url, params=params or {})
            resp.raise_for_status()
            return resp.json()
    except httpx.TimeoutException:
        raise HTTPException(status_code=504, detail=f"{label} API timeout ({url})")
    except httpx.HTTPStatusError as exc:
        raise HTTPException(
            status_code=exc.response.status_code,
            detail=f"{label} API error: HTTP {exc.response.status_code} for {url}",
        )
    except Exception as exc:
        logger.exception("Unexpected error fetching %s", url)
        raise HTTPException(status_code=502, detail=f"Error fetching {label}: {exc}")


# ===========================================================================
# 1. TexMesonet (Texas Water Development Board)
# ===========================================================================

_TEXMESONET_BASE = "https://www.texmesonet.org/api"


def _parse_texmesonet_station(rec: dict) -> Station:
    """Convert a TexMesonet station record to the shared Station model."""
    sid = str(rec.get("siteId") or rec.get("stationId") or rec.get("displayId") or "")
    name = rec.get("name") or rec.get("stationName") or sid
    try:
        lat = float(rec.get("latitude") or 0.0)
    except (TypeError, ValueError):
        lat = 0.0
    try:
        lon = float(rec.get("longitude") or 0.0)
    except (TypeError, ValueError):
        lon = 0.0
    try:
        elev = float(rec.get("elevation")) if rec.get("elevation") is not None else None
    except (TypeError, ValueError):
        elev = None

    # Extract last_update from observation timestamp fields
    last_update = None
    recorded_str = rec.get("recordedTime") or rec.get("dateTime")
    if recorded_str:
        try:
            last_update = datetime.fromisoformat(str(recorded_str).replace("Z", "+00:00"))
        except (ValueError, TypeError):
            pass

    return Station(
        id=f"texmesonet:{sid}",
        name=str(name),
        lat=lat,
        lon=lon,
        source="texmesonet",
        network="TexMesonet / TWDB",
        params=[
            "temperature", "humidity", "pressure", "wind_speed", "wind_direction",
            "wind_gust", "precipitation", "solar_radiation",
            "soil_moisture", "soil_temperature",
        ],
        elevation=elev,
        last_update=last_update,
        url=f"https://www.texmesonet.org/Station/Detail/{sid}",
        extra={
            "site_id": sid,
            "station_type": rec.get("stationType", ""),
            "county": rec.get("county", ""),
            "state": "TX",
        },
    )


def _parse_texmesonet_readings(rec: dict) -> list[Reading]:
    """Extract Reading objects from a TexMesonet CurrentData record."""
    sid = str(rec.get("stationId") or rec.get("siteId") or rec.get("displayId") or "")
    station_id = f"texmesonet:{sid}"

    recorded_str = rec.get("recordedTime") or rec.get("dateTime")
    try:
        if recorded_str:
            ts = datetime.fromisoformat(str(recorded_str).replace("Z", "+00:00"))
        else:
            ts = datetime.now(timezone.utc)
    except ValueError:
        ts = datetime.now(timezone.utc)

    # Field map: (api_key, canonical_param, unit)
    field_map = [
        ("airTemp",         "temperature",      "°C"),
        ("airTemp2m",       "temperature_2m",   "°C"),
        ("airTemp9m",       "temperature_9m",   "°C"),
        ("humidity",        "relative_humidity","%"),
        ("airPressure",     "pressure",         "mb"),
        ("windSpeed",       "wind_speed",       "m/s"),
        ("windDirection",   "wind_direction",   "deg"),
        ("windGust",        "wind_gust",        "m/s"),
        ("precip",          "precipitation",    "mm"),
        ("precip24Hr",      "precipitation_24h","mm"),
        ("precip48Hr",      "precipitation_48h","mm"),
        ("precip72Hr",      "precipitation_72h","mm"),
        ("solarRadiation",  "solar_radiation",  "W/m²"),
        ("soilMoisture",    "soil_moisture_surface", "%"),
        ("soilMoisture5cm", "soil_moisture_5cm","vol%"),
        ("soilMoisture10cm","soil_moisture_10cm","vol%"),
        ("soilMoisture20cm","soil_moisture_20cm","vol%"),
        ("soilMoisture50cm","soil_moisture_50cm","vol%"),
        ("soilTemperature", "soil_temperature_surface","°C"),
        ("soilTemperature5cm","soil_temperature_5cm","°C"),
        ("soilTemperature10cm","soil_temperature_10cm","°C"),
        ("soilTemperature20cm","soil_temperature_20cm","°C"),
        ("soilTemperature50cm","soil_temperature_50cm","°C"),
    ]

    readings: list[Reading] = []
    for api_key, param, unit in field_map:
        raw = rec.get(api_key)
        if raw is None:
            continue
        try:
            value = float(raw)
        except (TypeError, ValueError):
            continue
        readings.append(
            Reading(
                station_id=station_id,
                timestamp=ts,
                parameter=param,
                value=value,
                unit=unit,
                source="texmesonet",
            )
        )
    return readings


@router.get("/texmesonet/stations", summary="TexMesonet station list (TWDB)")
async def texmesonet_stations():
    """Return all 133 TexMesonet stations from the Texas Water Development Board.

    No authentication required.  Stations cover the state of Texas with
    instruments measuring temperature, humidity, wind, precipitation, solar
    radiation, and soil moisture/temperature at multiple depths.

    Results cached for 1 hour (station metadata rarely changes).
    """
    cache_key = "texmesonet:stations"
    cached_result = await cache.get(cache_key)
    if cached_result is not None:
        return cached_result

    raw = await _get_json(f"{_TEXMESONET_BASE}/Stations", label="TexMesonet")

    if isinstance(raw, list):
        station_list = raw
    elif isinstance(raw, dict):
        station_list = raw.get("data") or raw.get("stations") or raw.get("results") or []
    else:
        station_list = []

    stations = [_parse_texmesonet_station(s) for s in station_list if isinstance(s, dict)]

    result = {
        "source": "texmesonet",
        "network": "TexMesonet / Texas Water Development Board",
        "url": "https://www.texmesonet.org",
        "count": len(stations),
        "stations": [s.model_dump() for s in stations],
    }
    await cache.set(cache_key, result, ttl=config.CACHE_TTL_LONG)
    return result


@router.get("/texmesonet/current", summary="TexMesonet current conditions (all stations)")
async def texmesonet_current():
    """Return current weather observations for all TexMesonet stations.

    No authentication required.  Returns all ~133 Texas stations with their
    latest readings for temperature, wind, humidity, precipitation, solar
    radiation, and soil moisture/temperature.

    Results cached for 5 minutes.
    """
    cache_key = "texmesonet:current"
    cached_result = await cache.get(cache_key)
    if cached_result is not None:
        return cached_result

    raw = await _get_json(f"{_TEXMESONET_BASE}/CurrentData", label="TexMesonet")

    if isinstance(raw, list):
        obs_list = raw
    elif isinstance(raw, dict):
        obs_list = raw.get("data") or raw.get("observations") or raw.get("results") or []
    else:
        obs_list = []

    stations_out: list[dict] = []
    readings_out: list[dict] = []

    for obs in obs_list:
        if not isinstance(obs, dict):
            continue
        try:
            station = _parse_texmesonet_station(obs)
            readings = _parse_texmesonet_readings(obs)
            stations_out.append(station.model_dump())
            readings_out.extend(r.model_dump() for r in readings)
        except Exception:
            logger.debug("Failed to parse TexMesonet obs: %s", obs, exc_info=True)

    result = {
        "source": "texmesonet",
        "network": "TexMesonet / Texas Water Development Board",
        "station_count": len(stations_out),
        "reading_count": len(readings_out),
        "stations": stations_out,
        "readings": readings_out,
    }
    await cache.set(cache_key, result, ttl=config.CACHE_TTL)
    return result


@router.get(
    "/texmesonet/stations/{site_id}/history",
    summary="TexMesonet time-series for a station",
)
async def texmesonet_history(
    site_id: str,
    minutes: int = Query(
        1440,
        ge=5,
        le=10080,
        description="Prior minutes of data to retrieve (default 1440 = 24 hours, max 10080 = 7 days)",
    ),
):
    """Return recent time-series observations for a single TexMesonet station.

    `site_id` — the numeric site ID (e.g. '1', '23', '100').
    `minutes` — how many minutes of history to request (max 10080 = 7 days).
    """
    cache_key = f"texmesonet:history:{site_id}:{minutes}"
    cached_result = await cache.get(cache_key)
    if cached_result is not None:
        return cached_result

    raw = await _get_json(
        f"{_TEXMESONET_BASE}/AllChartingFieldsById/{site_id}/{minutes}",
        label=f"TexMesonet station {site_id}",
    )

    if isinstance(raw, list):
        obs_list = raw
    elif isinstance(raw, dict):
        obs_list = raw.get("data") or raw.get("observations") or [raw]
    else:
        obs_list = []

    readings_out: list[dict] = []
    for obs in obs_list:
        if not isinstance(obs, dict):
            continue
        # Inject site_id if not present
        if "stationId" not in obs and "siteId" not in obs:
            obs["stationId"] = site_id
        readings = _parse_texmesonet_readings(obs)
        readings_out.extend(r.model_dump() for r in readings)

    readings_out.sort(key=lambda r: r["timestamp"])

    result = {
        "source": "texmesonet",
        "site_id": site_id,
        "prior_minutes": minutes,
        "reading_count": len(readings_out),
        "readings": readings_out,
    }
    await cache.set(cache_key, result, ttl=config.CACHE_TTL)
    return result


# ===========================================================================
# 2. Caltrans CWWP2 RWIS (California DOT Road Weather)
# ===========================================================================

_CALTRANS_BASE = "https://cwwp2.dot.ca.gov/data"

# Caltrans districts with confirmed public RWIS JSON endpoints
_CALTRANS_DISTRICTS = {
    "D02": ("d2", "D02", "Northern California - Redding/Mt Shasta"),
    "D03": ("d3", "D03", "Northern California - Sacramento Valley/Sierra Nevada"),
    "D06": ("d6", "D06", "Central California - Fresno/Bakersfield"),
    "D08": ("d8", "D08", "Southern California - San Bernardino/Riverside"),
    "D09": ("d9", "D09", "Eastern Sierra/Death Valley"),
    "D10": ("d10", "D10", "Central Valley - Stockton/Modesto"),
}


def _parse_caltrans_station(rec: dict, district: str) -> Optional[Station]:
    """Convert a Caltrans CWWP2 RWIS record to the shared Station model."""
    try:
        lat = float(rec.get("latitude") or 0.0)
        lon = float(rec.get("longitude") or 0.0)
    except (TypeError, ValueError):
        return None

    if lat == 0.0 and lon == 0.0:
        return None

    name = rec.get("locationName") or rec.get("index") or ""
    sid = f"caltrans:{district}:{rec.get('index', name)}"

    try:
        elev = float(rec.get("elevation")) if rec.get("elevation") is not None else None
    except (TypeError, ValueError):
        elev = None

    # Extract last_update from record timestamp fields
    last_update = None
    epoch = rec.get("recordEpoch")
    date_str = rec.get("recordDate", "")
    time_str = rec.get("recordTime", "")
    try:
        if epoch:
            last_update = datetime.fromtimestamp(float(epoch), tz=timezone.utc)
        elif date_str and time_str:
            last_update = datetime.fromisoformat(f"{date_str}T{time_str}+00:00")
    except (ValueError, TypeError):
        pass

    return Station(
        id=sid,
        name=str(name),
        lat=lat,
        lon=lon,
        source="caltrans_rwis",
        network=f"Caltrans CWWP2 {district}",
        params=[
            "temperature", "dew_point", "relative_humidity",
            "wind_speed", "wind_direction", "wind_gust",
            "precipitation", "pressure", "visibility",
            "road_temperature", "road_condition", "road_freeze_temp",
        ],
        elevation=elev,
        last_update=last_update,
        url=f"https://cwwp2.dot.ca.gov/",
        extra={
            "district": district,
            "county": rec.get("county", ""),
            "route": rec.get("route", ""),
            "postmile": rec.get("postmile", ""),
            "in_service": rec.get("inService", True),
        },
    )


def _parse_caltrans_readings(rec: dict, district: str) -> list[Reading]:
    """Extract Reading objects from a Caltrans CWWP2 RWIS station record."""
    name = rec.get("locationName") or rec.get("index") or ""
    station_id = f"caltrans:{district}:{rec.get('index', name)}"

    # Parse timestamp from record date + time
    date_str = rec.get("recordDate", "")
    time_str = rec.get("recordTime", "")
    epoch = rec.get("recordEpoch")
    try:
        if epoch:
            ts = datetime.fromtimestamp(float(epoch), tz=timezone.utc)
        elif date_str and time_str:
            ts = datetime.fromisoformat(f"{date_str}T{time_str}+00:00")
        else:
            ts = datetime.now(timezone.utc)
    except (ValueError, TypeError):
        ts = datetime.now(timezone.utc)

    field_map = [
        ("essAirTemperature",    "temperature",       "°C"),
        ("essWetbulbTemp",       "wet_bulb_temp",     "°C"),
        ("essDewpointTemp",      "dew_point",         "°C"),
        ("essRelativeHumidity",  "relative_humidity", "%"),
        ("essAvgWindSpeed",      "wind_speed",        "m/s"),
        ("essAvgWindDirection",  "wind_direction",    "deg"),
        ("essSpotWindSpeed",     "wind_speed_spot",   "m/s"),
        ("essSpotWindDirection", "wind_direction_spot","deg"),
        ("essMaxWindGustSpeed",  "wind_gust",         "m/s"),
        ("essMaxWindGustDir",    "wind_gust_direction","deg"),
        ("essPrecipRate",        "precipitation_rate","mm/hr"),
        ("essAtmosphericPressure","pressure",         "Pa"),
        ("essVisibility",        "visibility",        "m"),
    ]

    readings: list[Reading] = []
    for api_key, param, unit in field_map:
        raw = rec.get(api_key)
        if raw is None or raw == "" or str(raw).strip() == "":
            continue
        try:
            value = float(raw)
        except (TypeError, ValueError):
            continue
        readings.append(
            Reading(
                station_id=station_id,
                timestamp=ts,
                parameter=param,
                value=value,
                unit=unit,
                source="caltrans_rwis",
            )
        )

    # Pavement surface readings (indexed 0-3 for up to 4 sensors)
    for i in range(4):
        surf_temp_key = f"essSurfaceTemperature{i}" if i > 0 else "essSurfaceTemperature"
        # Try numbered keys that appear in the CWWP2 data structure
        for key_attempt in [
            f"pavement[{i}].surfaceTemp",
            f"surfaceSensor{i}essSurfaceTemperature",
        ]:
            raw_t = rec.get(key_attempt)
            if raw_t is not None:
                try:
                    readings.append(
                        Reading(
                            station_id=station_id,
                            timestamp=ts,
                            parameter=f"road_temperature_sensor{i}",
                            value=float(raw_t),
                            unit="°C",
                            source="caltrans_rwis",
                        )
                    )
                except (TypeError, ValueError):
                    pass
                break

    return readings


@router.get(
    "/caltrans/rwis",
    summary="Caltrans CWWP2 road weather stations for a district",
)
async def caltrans_rwis(
    district: str = Query(
        "D03",
        description=(
            "Caltrans district code.  "
            "Available: D02 (Redding), D03 (Sacramento/Sierra), "
            "D06 (Fresno), D08 (San Bernardino), D09 (Eastern Sierra), D10 (Stockton).  "
            "Default: D03"
        ),
    ),
):
    """Return current road weather conditions for a Caltrans district.

    Data comes from the Caltrans CWWP2 (California Weather for Travelers) system.
    Stations measure air temperature, dew point, wind, precipitation, visibility,
    atmospheric pressure, and pavement surface temperature/condition.

    No authentication required.  Data updates every ~1 minute.
    Results cached for 5 minutes.
    """
    district = district.upper().strip()
    if district not in _CALTRANS_DISTRICTS:
        raise HTTPException(
            status_code=400,
            detail=(
                f"Unknown Caltrans district '{district}'.  "
                f"Available districts: {', '.join(sorted(_CALTRANS_DISTRICTS.keys()))}"
            ),
        )

    path_dir, path_dist, desc = _CALTRANS_DISTRICTS[district]
    url = f"{_CALTRANS_BASE}/{path_dir}/rwis/rwisStatus{path_dist}.json"

    cache_key = f"caltrans:rwis:{district}"
    cached_result = await cache.get(cache_key)
    if cached_result is not None:
        return cached_result

    raw = await _get_json(url, label=f"Caltrans {district}")

    # The response structure is {"data": [{"rwis": {...}}, ...]}
    # Each "rwis" object contains nested sub-objects:
    #   rwis.location      — lat, lon, county, route, etc.
    #   rwis.recordTimestamp — recordDate, recordTime, recordEpoch
    #   rwis.rwisData       — nested stationData, windData, temperatureData,
    #                         humidityPrecipData, visibilityData, pavementSensorData
    # We flatten each rwis record into a single dict for the existing parsers.

    if isinstance(raw, dict):
        raw_list = raw.get("data") or []
    elif isinstance(raw, list):
        raw_list = raw
    else:
        raw_list = []

    # Flatten the nested CWWP2 structure into flat dicts the parsers expect
    station_list: list[dict] = []
    for item in raw_list:
        if not isinstance(item, dict):
            continue
        rwis = item.get("rwis") or item
        if not isinstance(rwis, dict):
            continue
        flat: dict = {}
        flat["index"] = rwis.get("index", "")
        flat["inService"] = rwis.get("inService", "")
        # Merge location fields
        loc = rwis.get("location") or {}
        flat.update(loc)
        # Merge timestamp fields
        ts = rwis.get("recordTimestamp") or {}
        flat.update(ts)
        # Merge rwisData sub-objects
        rdata = rwis.get("rwisData") or {}
        for sub_key in ("stationData", "windData", "humidityPrecipData",
                        "visibilityData"):
            sub = rdata.get(sub_key)
            if isinstance(sub, dict):
                flat.update(sub)
        # Temperature data — extract from sensor table
        temp_data = rdata.get("temperatureData") or {}
        for tk, tv in temp_data.items():
            if tk == "essTemperatureSensorTable":
                # List of sensor entries; pull first sensor's air temp
                if isinstance(tv, list):
                    for entry in tv:
                        inner = entry.get("essTemperatureSensorEntry") or entry
                        if isinstance(inner, dict):
                            flat.update(inner)
            else:
                flat[tk] = tv
        # Pavement sensor data
        pave = rdata.get("pavementSensorData") or {}
        sensor_table = pave.get("essPavementSensorTable") or []
        for idx, psensor in enumerate(sensor_table):
            inner = psensor.get("essPavementSensorEntry") or psensor
            if isinstance(inner, dict):
                for pk, pv in inner.items():
                    flat[f"pavement[{idx}].{pk}"] = pv
                # Also store surface temp under the key the reading parser expects
                st_val = inner.get("essSurfaceTemperature")
                if st_val is not None:
                    flat[f"surfaceSensor{idx}essSurfaceTemperature"] = st_val

        station_list.append(flat)

    stations_out: list[dict] = []
    readings_out: list[dict] = []

    for rec in station_list:
        if not isinstance(rec, dict):
            continue
        station = _parse_caltrans_station(rec, district)
        if station is None:
            continue
        readings = _parse_caltrans_readings(rec, district)
        stations_out.append(station.model_dump())
        readings_out.extend(r.model_dump() for r in readings)

    result = {
        "source": "caltrans_cwwp2",
        "district": district,
        "district_description": desc,
        "data_url": url,
        "station_count": len(stations_out),
        "reading_count": len(readings_out),
        "stations": stations_out,
        "readings": readings_out,
    }
    await cache.set(cache_key, result, ttl=config.CACHE_TTL)
    return result


@router.get("/caltrans/districts", summary="List Caltrans CWWP2 districts with RWIS data")
async def caltrans_districts():
    """Return the list of Caltrans districts that have public RWIS JSON feeds."""
    return {
        "source": "caltrans_cwwp2",
        "description": (
            "California DOT (Caltrans) road weather stations via the CWWP2 system.  "
            "Each district has a public JSON endpoint updated every ~1 minute.  "
            "No authentication required."
        ),
        "url": "https://cwwp2.dot.ca.gov/",
        "districts": [
            {
                "code": code,
                "description": desc,
                "data_url": (
                    f"{_CALTRANS_BASE}/{path_dir}/rwis/rwisStatus{path_dist}.json"
                ),
                "endpoint": f"/api/infrastructure/caltrans/rwis?district={code}",
            }
            for code, (path_dir, path_dist, desc) in sorted(_CALTRANS_DISTRICTS.items())
        ],
        "note": (
            "Additional districts (D01, D04, D05, D07, D11, D12) may also have data "
            "but were not confirmed active at time of implementation.  "
            "Try: https://cwwp2.dot.ca.gov/data/d{N}/rwis/rwisStatusD{NN}.json"
        ),
    }


# ===========================================================================
# 3. Colorado DOT CoTrip RWIS (via public ArcGIS FeatureServer)
# ===========================================================================

_CODOT_ARCGIS_URL = (
    "https://maps.codot.gov/server/rest/services/Hosted"
    "/CoTrip_Weather_Stations_(Live)_Public_View/FeatureServer/0/query"
)


def _parse_codot_feature(feature: dict) -> Optional[Station]:
    """Convert a CDOT CoTrip ArcGIS feature to the shared Station model."""
    attrs = feature.get("attributes", {}) or {}
    geom = feature.get("geometry") or {}

    # ArcGIS geometry uses x=longitude, y=latitude in WGS84
    lon = geom.get("x")
    lat = geom.get("y")
    if lon is None or lat is None:
        # Fall back to attribute-level coordinates
        lat = attrs.get("ws_latitude") or attrs.get("latitude")
        lon = attrs.get("ws_longitude") or attrs.get("longitude")

    if lon is None or lat is None:
        return None
    try:
        lat = float(lat)
        lon = float(lon)
    except (TypeError, ValueError):
        return None

    name = attrs.get("ws_commonname") or attrs.get("ws_roadname") or str(attrs.get("OBJECTID", ""))
    road = attrs.get("ws_roadname") or ""
    mile = attrs.get("ws_milemarker") or ""
    oid = str(attrs.get("OBJECTID") or "")

    full_name = name
    if road and road != name:
        full_name = f"{name} ({road})"
    if mile:
        full_name = f"{full_name} MM{mile}"

    # Extract last_update from ArcGIS timestamp (Unix milliseconds)
    last_update = None
    ts_ms = attrs.get("ws_lastupdated") or attrs.get("EditDate") or attrs.get("CreationDate")
    if ts_ms is not None:
        try:
            last_update = datetime.fromtimestamp(float(ts_ms) / 1000.0, tz=timezone.utc)
        except (TypeError, ValueError):
            pass

    return Station(
        id=f"codot:{oid}",
        name=full_name,
        lat=lat,
        lon=lon,
        source="codot_rwis",
        network="Colorado DOT CoTrip RWIS",
        params=[
            "temperature", "wind_speed", "wind_direction", "wind_gust",
            "precipitation", "visibility", "road_temperature", "road_condition",
        ],
        elevation=None,
        last_update=last_update,
        url="https://cotrip.org/",
        extra={
            "object_id": oid,
            "road_name": road,
            "mile_marker": mile,
            "status": attrs.get("ws_status", ""),
        },
    )


def _parse_codot_readings(feature: dict) -> list[Reading]:
    """Extract Reading objects from a CDOT CoTrip ArcGIS feature."""
    attrs = feature.get("attributes", {}) or {}
    oid = str(attrs.get("OBJECTID") or "")
    station_id = f"codot:{oid}"

    # Timestamps in ArcGIS are Unix milliseconds
    ts_ms = attrs.get("ws_lastupdated") or attrs.get("EditDate") or attrs.get("CreationDate")
    try:
        ts = datetime.fromtimestamp(float(ts_ms) / 1000.0, tz=timezone.utc) if ts_ms else datetime.now(timezone.utc)
    except (TypeError, ValueError):
        ts = datetime.now(timezone.utc)

    field_map = [
        ("ws_essairtemp",              "temperature",       "°F"),
        ("ws_essmaxtemp",              "temperature_max",   "°F"),
        ("ws_essmintemp",              "temperature_min",   "°F"),
        ("ws_winddir",                 "wind_direction",    "deg"),
        ("ws_essavgwindspeed",         "wind_speed",        "mph"),
        ("ws_windgustdir",             "wind_gust_direction","deg"),
        ("ws_essprecip1hr",            "precipitation_1h",  "in"),
        ("ws_essprecip6hr",            "precipitation_6h",  "in"),
        ("ws_essprecip12hr",           "precipitation_12h", "in"),
        ("ws_essvisibility",           "visibility",        "mi"),
        ("surfacesensor_esssurfacetempera","road_temperature","°F"),
        ("surfacesensor_frictionnum",  "road_friction",     "index"),
    ]

    readings: list[Reading] = []
    for api_key, param, unit in field_map:
        raw = attrs.get(api_key)
        if raw is None:
            continue
        try:
            value = float(raw)
        except (TypeError, ValueError):
            continue
        readings.append(
            Reading(
                station_id=station_id,
                timestamp=ts,
                parameter=param,
                value=value,
                unit=unit,
                source="codot_rwis",
            )
        )
    return readings


@router.get(
    "/codot/rwis",
    summary="Colorado DOT CoTrip highway weather stations (RWIS)",
)
async def codot_rwis(
    limit: int = Query(
        500,
        ge=1,
        le=2000,
        description="Maximum number of stations to return (CDOT has ~400 stations)",
    ),
    bbox: Optional[str] = Query(
        None,
        description="Bounding box filter: west,south,east,north (decimal degrees)",
    ),
):
    """Return current Colorado DOT road weather station conditions.

    Data is sourced from the public CDOT CoTrip ArcGIS FeatureServer, which
    serves live road weather data with no authentication required.

    Stations measure air temperature, wind, precipitation, visibility, and
    road surface temperature/condition.  Typical update interval is 10 minutes.

    Results cached for 5 minutes.
    """
    cache_key = f"codot:rwis:{limit}:{bbox}"
    cached_result = await cache.get(cache_key)
    if cached_result is not None:
        return cached_result

    arcgis_params: dict[str, Any] = {
        "where": "1=1",
        "outFields": "*",
        "returnGeometry": "true",
        "f": "json",
        "resultRecordCount": limit,
    }

    if bbox:
        parts = [p.strip() for p in bbox.split(",")]
        if len(parts) != 4:
            raise HTTPException(
                status_code=400,
                detail="bbox must be four comma-separated values: west,south,east,north",
            )
        west, south, east, north = parts
        arcgis_params["geometry"] = f"{west},{south},{east},{north}"
        arcgis_params["geometryType"] = "esriGeometryEnvelope"
        arcgis_params["inSR"] = "4326"
        arcgis_params["spatialRel"] = "esriSpatialRelIntersects"

    raw = await _get_json(_CODOT_ARCGIS_URL, params=arcgis_params, label="CDOT CoTrip ArcGIS")

    if isinstance(raw, dict) and raw.get("error"):
        err = raw["error"]
        raise HTTPException(
            status_code=502,
            detail=f"CDOT ArcGIS error: {err.get('message', err)}",
        )

    features = raw.get("features", []) or []
    stations_out: list[dict] = []
    readings_out: list[dict] = []

    for feat in features:
        station = _parse_codot_feature(feat)
        if station is None:
            continue
        readings = _parse_codot_readings(feat)
        stations_out.append(station.model_dump())
        readings_out.extend(r.model_dump() for r in readings)

    result = {
        "source": "codot_cotrip",
        "network": "Colorado DOT CoTrip RWIS",
        "arcgis_url": _CODOT_ARCGIS_URL,
        "station_count": len(stations_out),
        "reading_count": len(readings_out),
        "data_status": "FROZEN",
        "data_quality_warning": (
            "This ArcGIS FeatureServer layer has not been updated since September 2021. "
            "Station locations are valid but all observation readings are from 2021 — "
            "timestamps shown are proxy-stamped and do NOT reflect live data. "
            "Quality tier: 5 (Caution — stale data source)."
        ),
        "quality_tier": 5,
        "stations": stations_out,
        "readings": readings_out,
    }
    await cache.set(cache_key, result, ttl=config.CACHE_TTL)
    return result


# ===========================================================================
# 4. Iowa DOT ArcGIS RWIS Surface Data
# ===========================================================================

_IADOT_ARCGIS_URL = (
    "https://services.arcgis.com/8lRhdTsQyJpO52F1/arcgis/rest/services"
    "/RWIS_Surface_Data_View/FeatureServer/0/query"
)


def _parse_iadot_feature(feature: dict) -> Optional[Station]:
    """Convert an Iowa DOT ArcGIS feature to the shared Station model."""
    attrs = feature.get("attributes", {}) or {}
    geom = feature.get("geometry") or {}

    lon = geom.get("x")
    lat = geom.get("y")
    if lon is None or lat is None:
        lat = attrs.get("LATITUDE")
        lon = attrs.get("LONGITUDE")

    if lon is None or lat is None:
        return None
    try:
        lat = float(lat)
        lon = float(lon)
    except (TypeError, ValueError):
        return None

    sid = str(attrs.get("SITE_NUMBER") or attrs.get("OBJECTID") or "")
    name = attrs.get("SENSOR_NAME") or attrs.get("UNIQUE_ID") or sid
    route = attrs.get("ROUTE_NAME") or ""
    mp = attrs.get("MILE_POST") or ""
    county = attrs.get("COUNTY_NAME") or ""

    full_name = str(name)
    if route:
        full_name = f"{name} ({route})"
    if mp:
        full_name = f"{full_name} MP{mp}"

    try:
        elev = float(attrs["GPS_ALTITUDE"]) if attrs.get("GPS_ALTITUDE") is not None else None
    except (TypeError, ValueError):
        elev = None

    # Extract last_update from ArcGIS timestamp fields (Unix milliseconds)
    last_update = None
    ts_raw = attrs.get("COLLECTION_TIME") or attrs.get("EditDate")
    if ts_raw is not None:
        try:
            last_update = datetime.fromtimestamp(float(ts_raw) / 1000.0, tz=timezone.utc)
        except (TypeError, ValueError, OSError):
            pass

    return Station(
        id=f"iadot:{sid}",
        name=full_name,
        lat=lat,
        lon=lon,
        source="iadot_rwis",
        network="Iowa DOT RWIS Surface Data",
        params=[
            "road_temperature", "road_condition", "road_ice_percentage",
            "road_freeze_temperature", "road_friction",
        ],
        elevation=elev,
        last_update=last_update,
        url="https://511ia.org/",
        extra={
            "site_number": sid,
            "route": route,
            "mile_post": mp,
            "county": county,
            "district": str(attrs.get("DISTRICT_NO") or ""),
        },
    )


def _parse_iadot_readings(feature: dict) -> list[Reading]:
    """Extract pavement surface readings from an Iowa DOT ArcGIS feature."""
    attrs = feature.get("attributes", {}) or {}
    sid = str(attrs.get("SITE_NUMBER") or attrs.get("OBJECTID") or "")
    station_id = f"iadot:{sid}"

    ts = datetime.now(timezone.utc)

    field_map = [
        ("Surface Temperature (F)",    "road_temperature",     "°F"),
        ("Freeze Temperature (F)",      "road_freeze_temp",     "°F"),
        ("Ice Percentage on Road",      "road_ice_percentage",  "%"),
        ("Friction Index",              "road_friction",        "index"),
    ]

    readings: list[Reading] = []
    for api_key, param, unit in field_map:
        raw = attrs.get(api_key)
        if raw is None:
            continue
        try:
            value = float(raw)
        except (TypeError, ValueError):
            continue
        readings.append(
            Reading(
                station_id=station_id,
                timestamp=ts,
                parameter=param,
                value=value,
                unit=unit,
                quality=str(attrs.get("Surface Condition", "")) or None,
                source="iadot_rwis",
            )
        )
    return readings


@router.get(
    "/iadot/rwis/surface",
    summary="Iowa DOT RWIS pavement surface data (ArcGIS)",
)
async def iadot_rwis_surface(
    limit: int = Query(
        500,
        ge=1,
        le=2000,
        description="Maximum number of stations to return",
    ),
    district: Optional[str] = Query(
        None,
        description="Iowa DOT district number (1-6) to filter results",
    ),
):
    """Return Iowa DOT RWIS pavement surface sensor data.

    Data comes from the Iowa DOT ArcGIS FeatureServer, which provides
    pavement surface temperature, surface condition, ice percentage on
    road, freeze temperature, and friction index for sensors across Iowa's
    highway system.

    No authentication required.  Results cached for 5 minutes.
    """
    cache_key = f"iadot:rwis:surface:{limit}:{district}"
    cached_result = await cache.get(cache_key)
    if cached_result is not None:
        return cached_result

    where = "1=1"
    if district:
        where = f"DISTRICT_NO={district}"

    arcgis_params: dict[str, Any] = {
        "where": where,
        "outFields": "*",
        "returnGeometry": "true",
        "outSR": "4326",  # Request WGS84 — default is Web Mercator which looks like lat/lon
        "f": "json",
        "resultRecordCount": limit,
    }

    raw = await _get_json(_IADOT_ARCGIS_URL, params=arcgis_params, label="Iowa DOT ArcGIS")

    if isinstance(raw, dict) and raw.get("error"):
        err = raw["error"]
        raise HTTPException(
            status_code=502,
            detail=f"Iowa DOT ArcGIS error: {err.get('message', err)}",
        )

    features = raw.get("features", []) or []
    stations_out: list[dict] = []
    readings_out: list[dict] = []

    for feat in features:
        station = _parse_iadot_feature(feat)
        if station is None:
            continue
        readings = _parse_iadot_readings(feat)
        stations_out.append(station.model_dump())
        readings_out.extend(r.model_dump() for r in readings)

    result = {
        "source": "iadot_rwis",
        "network": "Iowa DOT RWIS Surface Data",
        "arcgis_url": _IADOT_ARCGIS_URL,
        "district_filter": district,
        "station_count": len(stations_out),
        "reading_count": len(readings_out),
        "stations": stations_out,
        "readings": readings_out,
    }
    await cache.set(cache_key, result, ttl=config.CACHE_TTL)
    return result


# ===========================================================================
# 5. USBR AgriMet (Bureau of Reclamation Agricultural Meteorology)
# ===========================================================================

_AGRIMET_STATION_URL = "https://www.usbr.gov/pn/agrimet/agrimetmap/usbr_map.json"
_AGRIMET_RECENT_URL = "https://www.usbr.gov/pn/agrimet/chart/agrimet_json.php"
_AGRIMET_CSV_BASE = "https://www.usbr.gov/pn-bin/agrimet.pl"


_AGRIMET_ID_RE = re.compile(r'\b([A-Z]{4})\b')
_AGRIMET_URL_RE = re.compile(r'/wx/([a-z]{4})wx', re.IGNORECASE)


def _parse_agrimet_station(rec: dict) -> Optional[Station]:
    """Convert an AgriMet station map record to the shared Station model."""
    try:
        lat = float(rec.get("lat") or rec.get("latitude") or 0.0)
        lon = float(rec.get("lng") or rec.get("lon") or rec.get("longitude") or 0.0)
    except (TypeError, ValueError):
        return None

    if lat == 0.0 and lon == 0.0:
        return None

    # Try every common field name the AgriMet API might use for the station code
    sid = (
        rec.get("site_id")
        or rec.get("siteid")
        or rec.get("agrimet_id")
        or rec.get("station_id")
        or rec.get("stationId")
        or rec.get("station")
        or rec.get("id")
        or ""
    )

    # Extract 4-letter all-caps station code from the title field
    if not sid:
        title = str(rec.get("title") or "")
        m = _AGRIMET_ID_RE.search(title.upper())
        if m:
            sid = m.group(1)

    # Last resort: extract from the station URL if provided
    if not sid:
        link = str(rec.get("link") or rec.get("url") or rec.get("popupContent") or "")
        m = _AGRIMET_URL_RE.search(link)
        if m:
            sid = m.group(1).upper()

    name = rec.get("title") or rec.get("name") or str(sid)

    return Station(
        id=f"agrimet:{sid}",
        name=str(name),
        lat=lat,
        lon=lon,
        source="usbr_agrimet",
        network="USBR AgriMet",
        params=[
            "temperature", "relative_humidity", "dew_point",
            "wind_speed", "wind_direction", "wind_run",
            "solar_radiation", "precipitation",
            "soil_temperature", "evapotranspiration",
        ],
        elevation=None,
        url=f"https://www.usbr.gov/pn/agrimet/wx/{sid.lower()}wxpage.html" if sid else None,
        extra={
            "agrimet_id": str(sid),
            "state": rec.get("state", ""),
            "region": rec.get("region", ""),
            "description": rec.get("description", ""),
        },
    )


@router.get("/agrimet/stations", summary="USBR AgriMet station list (western US)")
async def agrimet_stations():
    """Return all USBR AgriMet agricultural meteorology station locations.

    AgriMet is operated by the Bureau of Reclamation Pacific Northwest Region
    and covers 200+ stations in Oregon, Idaho, Washington, Montana, Wyoming,
    Nevada, and California.  Stations measure air temperature, relative
    humidity, dew point, wind speed and direction, wind run, solar radiation,
    precipitation, and soil temperature for agricultural and water resource uses.

    No authentication required.  Results cached for 1 hour.
    """
    cache_key = "agrimet:stations"
    cached_result = await cache.get(cache_key)
    if cached_result is not None:
        return cached_result

    raw = await _get_json(_AGRIMET_STATION_URL, label="USBR AgriMet")

    # The response is typically a list of station dicts or a dict wrapping them
    if isinstance(raw, list):
        station_list = raw
    elif isinstance(raw, dict):
        station_list = (
            raw.get("features")
            or raw.get("stations")
            or raw.get("data")
            or []
        )
    else:
        station_list = []

    # Handle GeoJSON FeatureCollection format
    parsed_stations: list[Station] = []
    for item in station_list:
        if not isinstance(item, dict):
            continue
        # GeoJSON Feature format
        if "geometry" in item and "properties" in item:
            geom = item.get("geometry", {}) or {}
            props = item.get("properties", {}) or {}
            coords = geom.get("coordinates", [0, 0])
            combined = {**props, "lng": coords[0] if len(coords) > 1 else 0, "lat": coords[1] if len(coords) > 1 else 0}
            station = _parse_agrimet_station(combined)
        else:
            station = _parse_agrimet_station(item)
        if station is not None:
            parsed_stations.append(station)

    result = {
        "source": "usbr_agrimet",
        "network": "USBR AgriMet (Bureau of Reclamation Agricultural Meteorology)",
        "url": "https://www.usbr.gov/pn/agrimet/",
        "coverage": "OR, ID, WA, MT, WY, NV, CA — Columbia River Basin and surrounding region",
        "note": (
            "Station metadata is sourced from the public AgriMet map JSON.  "
            "For time-series data use the USBR AgriMet web data download tool: "
            "https://www.usbr.gov/pn/agrimet/webagdayread.html"
        ),
        "count": len(parsed_stations),
        "stations": [s.model_dump() for s in parsed_stations],
    }
    await cache.set(cache_key, result, ttl=config.CACHE_TTL_LONG)
    return result


# ===========================================================================
# Overview endpoint
# ===========================================================================

@router.get("/overview", summary="Infrastructure networks overview")
async def infrastructure_overview():
    """Return a summary of all infrastructure weather data sources in this module."""
    return {
        "module": "infrastructure",
        "description": SOURCE_DESCRIPTION,
        "auth_required": False,
        "sources": [
            {
                "id": "texmesonet",
                "name": "TexMesonet (Texas Water Development Board)",
                "description": (
                    "133 weather stations across Texas measuring temperature, wind, humidity, "
                    "precipitation, solar radiation, and soil moisture/temperature.  "
                    "5-minute update interval."
                ),
                "url": "https://www.texmesonet.org",
                "coverage": "Texas",
                "station_count": 133,
                "auth_required": False,
                "endpoints": [
                    "GET /api/infrastructure/texmesonet/stations",
                    "GET /api/infrastructure/texmesonet/current",
                    "GET /api/infrastructure/texmesonet/stations/{site_id}/history",
                ],
            },
            {
                "id": "caltrans_cwwp2",
                "name": "Caltrans CWWP2 RWIS",
                "description": (
                    "California DOT road weather stations via the CWWP2 (California Weather for Travelers) system.  "
                    "Measures air temperature, dew point, wind, precipitation, visibility, "
                    "atmospheric pressure, and pavement surface conditions.  "
                    "Updates every ~1 minute."
                ),
                "url": "https://cwwp2.dot.ca.gov/",
                "coverage": "California — Districts D02, D03, D06, D08, D09, D10",
                "auth_required": False,
                "endpoints": [
                    "GET /api/infrastructure/caltrans/districts",
                    "GET /api/infrastructure/caltrans/rwis?district=D03",
                ],
            },
            {
                "id": "codot_cotrip",
                "name": "Colorado DOT CoTrip RWIS",
                "description": (
                    "Colorado highway road weather stations via public ArcGIS FeatureServer.  "
                    "Air temperature, wind, precipitation, visibility, and pavement conditions."
                ),
                "url": "https://cotrip.org/",
                "coverage": "Colorado — statewide highway network",
                "auth_required": False,
                "endpoints": [
                    "GET /api/infrastructure/codot/rwis",
                ],
            },
            {
                "id": "iadot_rwis",
                "name": "Iowa DOT RWIS Surface Data",
                "description": (
                    "Iowa highway pavement surface sensor data via ArcGIS FeatureServer.  "
                    "Provides road surface temperature, condition, ice percentage, "
                    "freeze temperature, and friction index."
                ),
                "url": "https://511ia.org/",
                "coverage": "Iowa — statewide highway network",
                "auth_required": False,
                "endpoints": [
                    "GET /api/infrastructure/iadot/rwis/surface",
                ],
            },
            {
                "id": "usbr_agrimet",
                "name": "USBR AgriMet",
                "description": (
                    "Bureau of Reclamation Agricultural Meteorology network — 200+ stations "
                    "in OR, ID, WA, MT, WY, NV, CA measuring agro-met parameters for "
                    "irrigation scheduling and water resource management."
                ),
                "url": "https://www.usbr.gov/pn/agrimet/",
                "coverage": "Pacific Northwest and western US",
                "station_count": 200,
                "auth_required": False,
                "endpoints": [
                    "GET /api/infrastructure/agrimet/stations",
                ],
            },
        ],
    }
