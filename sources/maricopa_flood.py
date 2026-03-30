"""Maricopa County Flood Control District ALERT System — rain, stream, and weather data.

Covers the FCDMC ALERT telemetry network across Maricopa County, Arizona:
  - 378 rain gauges (tipping-bucket precipitation)
  - 170 stream gauges (stage/discharge)
  - 42 weather stations (temp, humidity, dewpoint, pressure, solar, wind)

Data updates every 5 minutes.  Two data pathways are used:

  1. ArcGIS REST MapServer — rain gauges and stream gauges with coordinates,
     returned as GeoJSON from the county GIS portal.
  2. PHP report endpoint — raw sensor data (HTML/PRE tables), used for weather
     station readings which have no dedicated MapServer layer.

All endpoints are unauthenticated.

References:
  - https://alert.fcd.maricopa.gov/
  - https://alert.fcd.maricopa.gov/showrpts_mc.html
  - https://gis.maricopa.gov/flood/rest/services/ALERT
"""
from __future__ import annotations

import logging
import re
from datetime import datetime, timedelta, timezone
from html.parser import HTMLParser
from typing import Any, Optional

import httpx
from fastapi import APIRouter, HTTPException, Query

import config
from cache import cached
from models import Reading, Station

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Module-level metadata (consumed by server.py /api/sources)
# ---------------------------------------------------------------------------

SOURCE_NAME = "Maricopa County Flood Control"
SOURCE_DESCRIPTION = (
    "Maricopa County Flood Control District ALERT System: 378 rain gauges, "
    "170 stream gauges, and 42 weather stations across the Phoenix metro area "
    "and surrounding desert.  Data updates every 5 minutes.  No auth required."
)
SOURCE_META = {
    "coverage": "Maricopa County, Arizona, USA",
    "category": "water",
    "auth_required": False,
    "params": [
        "precipitation", "stage", "discharge",
        "temperature", "humidity", "dewpoint", "pressure",
        "wind_speed", "wind_direction", "solar_radiation",
    ],
    "update_frequency": "5 min",
    "url": "https://alert.fcd.maricopa.gov/",
}

# ---------------------------------------------------------------------------
# Router
# ---------------------------------------------------------------------------

router = APIRouter(prefix="/api/maricopa", tags=["Maricopa County Flood Control"])

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_SOURCE = "maricopa_fcd"

# ArcGIS REST endpoints (GeoJSON capable, no auth)
_ARCGIS_BASE = "https://gis.maricopa.gov/flood/rest/services/ALERT"
_RAIN_URL = f"{_ARCGIS_BASE}/RainGaugeMeasurement/MapServer/5/query"
_STREAM_URL = f"{_ARCGIS_BASE}/StreamGaugeMeasurement/MapServer/0/query"
_IMPOUND_URL = f"{_ARCGIS_BASE}/ImpoundmentGaugeMeasurement/MapServer/0/query"

# PHP raw-data endpoint (POST, returns HTML PRE tables)
_RAW_DATA_URL = "https://alert.fcd.maricopa.gov/php/showdata2.php"

# Weather station registry — base gauge IDs and their locations.
# Sensor suffixes: 01=temp, 02=humidity, 03=pressure, 04=avg_wind,
#   05=wind_dir, 06=peak_wind, 11=solar_rad, 16=dewpoint
_WX_SENSOR_SUFFIXES: dict[str, tuple[str, str]] = {
    "01": ("temperature",    "°F"),
    "02": ("humidity",       "%"),
    "03": ("pressure",       "inHg"),
    "04": ("wind_speed",     "mph"),
    "05": ("wind_direction", "°"),
    "06": ("wind_gust",      "mph"),
    "11": ("solar_radiation","W/m²"),
    "16": ("dewpoint",       "°F"),
}

# Curated weather stations with known coordinates.
# Coordinates sourced from the rain-gauge GeoJSON layer (co-located gauges).
_WX_STATIONS: list[dict[str, Any]] = [
    {"id": "28300", "name": "Aguila VFD",                "lat": 33.9440, "lon": -113.1680, "sensors": ["01","02","04","05","06","16"]},
    {"id": "82700", "name": "Bartlett Lake",             "lat": 33.8200, "lon": -111.6320, "sensors": ["01","02","03","04","05","06","11","16"]},
    {"id": "46300", "name": "Belmont Mountains",         "lat": 33.6510, "lon": -112.6910, "sensors": ["01","02","04","05","06","11","16"]},
    {"id": "44000", "name": "Buckeye FRS #1",            "lat": 33.4340, "lon": -112.5840, "sensors": ["01","02","03","04","05","06","16"]},
    {"id": "87500", "name": "Camelback @ Loop 303",      "lat": 33.5090, "lon": -112.4000, "sensors": ["01","02","04","05","06","16"]},
    {"id": "21000", "name": "Carefree Ranch",            "lat": 33.8380, "lon": -111.9180, "sensors": ["01","02","04","05","06","11"]},
    {"id": "6500",  "name": "City of Glendale",          "lat": 33.5387, "lon": -112.1860, "sensors": ["01","02","03","04","05","06","11","16"]},
    {"id": "33000", "name": "Crossroads Park",           "lat": 33.4560, "lon": -111.7100, "sensors": ["01","02","03","04","05","06","11","16"]},
    {"id": "18700", "name": "Desert Mountain School",    "lat": 33.7620, "lon": -111.8380, "sensors": ["01","02","04","05","06","16"]},
    {"id": "36000", "name": "Dynamite & Pima",           "lat": 33.7470, "lon": -111.8640, "sensors": ["01","02","04","05","06","16"]},
    {"id": "90500", "name": "Falcon Field",              "lat": 33.4600, "lon": -111.7280, "sensors": ["01","02","03","04","05","06","11","16"]},
    {"id": "75000", "name": "Ft. McDowell",              "lat": 33.6000, "lon": -111.7200, "sensors": ["01","02","04","05","06","16"]},
    {"id": "81800", "name": "Gold Canyon FRS",           "lat": 33.3770, "lon": -111.4320, "sensors": ["01","02","04","05","06","16"]},
    {"id": "35000", "name": "Granite Reef Dam",          "lat": 33.5260, "lon": -111.7260, "sensors": ["01","02","04","05","06","16"]},
    {"id": "20500", "name": "Horseshoe Reservoir",       "lat": 33.9800, "lon": -111.7200, "sensors": ["01","02","03","04","05","06","11","16"]},
    {"id": "47000", "name": "Lake Pleasant",             "lat": 33.8740, "lon": -112.2740, "sensors": ["01","02","04","05","06","16"]},
    {"id": "5800",  "name": "Laveen FRS",                "lat": 33.3520, "lon": -112.1780, "sensors": ["01","02","04","05","06","16"]},
    {"id": "48500", "name": "Litchfield Park",           "lat": 33.5000, "lon": -112.3610, "sensors": ["01","02","04","05","06","16"]},
    {"id": "81300", "name": "Apache Junction FRS",       "lat": 33.4150, "lon": -111.5500, "sensors": ["01","02","04","05","06","16"]},
    {"id": "3000",  "name": "Cave Buttes Dam",           "lat": 33.6880, "lon": -112.0290, "sensors": ["01","02","03","04","05","06","11","16"]},
    {"id": "7800",  "name": "Chandler Heights",          "lat": 33.2250, "lon": -111.6840, "sensors": ["01","02","04","05","06","16"]},
    {"id": "69000", "name": "Daisy Mountain FRS",        "lat": 33.8230, "lon": -112.1000, "sensors": ["01","02","04","05","06","16"]},
    {"id": "91800", "name": "East Mesa",                 "lat": 33.3620, "lon": -111.5700, "sensors": ["01","02","04","05","06","16"]},
    {"id": "26700", "name": "Big Horn Road",             "lat": 33.6530, "lon": -112.5730, "sensors": ["01","02","04","05","06","16"]},
    {"id": "72500", "name": "McDowell Mountain Park",    "lat": 33.6600, "lon": -111.7700, "sensors": ["01","02","04","05","06","16"]},
    {"id": "62700", "name": "New River FRS",             "lat": 33.8900, "lon": -112.1350, "sensors": ["01","02","04","05","06","16"]},
    {"id": "42500", "name": "Palo Verde FRS",            "lat": 33.3900, "lon": -112.8600, "sensors": ["01","02","04","05","06","16"]},
    {"id": "80500", "name": "Peralta Trail",             "lat": 33.3970, "lon": -111.3480, "sensors": ["01","02","04","05","06","11","16"]},
    {"id": "89800", "name": "Pebble Creek",              "lat": 33.3500, "lon": -112.4500, "sensors": ["01","02","04","05","06","16"]},
    {"id": "29400", "name": "Alamo Road",                "lat": 33.8000, "lon": -113.5500, "sensors": ["01","02","04","05","06","16"]},
    {"id": "59700", "name": "Aztec Park",                "lat": 33.4470, "lon": -111.9250, "sensors": ["01","02","04","05","06","16"]},
    {"id": "50500", "name": "Black Hill",                "lat": 33.5900, "lon": -111.8680, "sensors": ["01","02","04","05","06","16"]},
    {"id": "67500", "name": "ASU South",                 "lat": 33.4160, "lon": -111.9350, "sensors": ["01","02","04","05","06","11","16"]},
]

# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------

_HEADERS = {"User-Agent": config.USER_AGENT, "Accept": "*/*"}


def _client() -> httpx.AsyncClient:
    return httpx.AsyncClient(
        timeout=config.HTTP_TIMEOUT,
        headers=_HEADERS,
        follow_redirects=True,
    )


# ---------------------------------------------------------------------------
# ArcGIS query helpers
# ---------------------------------------------------------------------------

async def _query_arcgis(url: str, *, where: str = "1=1",
                        out_fields: str = "*",
                        max_records: int = 2000) -> list[dict]:
    """Query an ArcGIS MapServer layer and return features as dicts.

    Requests GeoJSON format so coordinates come back in WGS84 (lon, lat).
    Handles the exceededTransferLimit flag by paging with resultOffset.
    """
    params: dict[str, Any] = {
        "where": where,
        "outFields": out_fields,
        "f": "geojson",
        "resultRecordCount": max_records,
    }
    features: list[dict] = []
    offset = 0

    async with _client() as client:
        while True:
            params["resultOffset"] = offset
            resp = await client.get(url, params=params)
            if resp.status_code != 200:
                log.warning("ArcGIS query %s returned %s", url, resp.status_code)
                break
            data = resp.json()
            batch = data.get("features", [])
            if not batch:
                break
            features.extend(batch)
            # Check if there are more pages
            exceeded = data.get("properties", {}).get("exceededTransferLimit", False)
            if not exceeded and not data.get("exceededTransferLimit", False):
                break
            offset += len(batch)
    return features


def _geojson_to_station(feat: dict, *, params: list[str],
                        network: str = "") -> Optional[Station]:
    """Convert a GeoJSON feature to a Station model."""
    props = feat.get("properties", {})
    geom = feat.get("geometry", {})
    coords = geom.get("coordinates", [])
    if len(coords) < 2:
        return None

    gauge_id = str(props.get("GaugeID", ""))
    name = props.get("GaugeName", f"Gauge {gauge_id}")

    return Station(
        id=gauge_id,
        name=name,
        lat=coords[1],
        lon=coords[0],
        source=_SOURCE,
        network=network,
        params=params,
    )


# ---------------------------------------------------------------------------
# Rain-gauge data (ArcGIS)
# ---------------------------------------------------------------------------

_RAIN_FIELDS = {
    "Minute10_Inch":  ("precip_10min",  "in"),
    "Minute30_Inch":  ("precip_30min",  "in"),
    "Hour1_Inch":     ("precip_1hr",    "in"),
    "Hour3_Inch":     ("precip_3hr",    "in"),
    "Hour6_Inch":     ("precip_6hr",    "in"),
    "Day1_Inch":      ("precip_1day",   "in"),
    "Day3_Inch":      ("precip_3day",   "in"),
    "Day7_Inch":      ("precip_7day",   "in"),
    "CummulativeYearToDate_Inch": ("precip_ytd", "in"),
}


@cached(ttl=300, prefix="maricopa_rain:")
async def _fetch_rain() -> list[dict]:
    return await _query_arcgis(_RAIN_URL)


@router.get("/rain", summary="Current rainfall from all ALERT rain gauges")
async def maricopa_rain(
    min_precip_1day: float = Query(0.0, description="Only return gauges with >= this 1-day total (inches)"),
) -> dict:
    """Return current precipitation accumulations from ~378 rain gauges.

    Each gauge reports 10-min through 7-day accumulations plus year-to-date.
    Data comes from the FCDMC ArcGIS MapServer (GeoJSON).
    """
    features = await _fetch_rain()

    stations: list[dict] = []
    readings: list[dict] = []

    for feat in features:
        props = feat.get("properties", {})
        geom = feat.get("geometry", {})
        coords = geom.get("coordinates", [])
        if len(coords) < 2:
            continue

        gauge_id = str(props.get("GaugeID", ""))
        name = props.get("GaugeName", f"Gauge {gauge_id}")

        # Filter by minimum daily precip
        day1 = props.get("Day1_Inch") or 0.0
        if day1 < min_precip_1day:
            continue

        # Parse timestamp (epoch ms)
        record_ms = props.get("RecordDate")
        ts = (
            datetime.fromtimestamp(record_ms / 1000, tz=timezone.utc)
            if record_ms else None
        )

        stations.append({
            "id": gauge_id,
            "name": name,
            "lat": coords[1],
            "lon": coords[0],
            "source": _SOURCE,
            "network": "rain",
            "timestamp": ts.isoformat() if ts else None,
        })

        for field, (param, unit) in _RAIN_FIELDS.items():
            val = props.get(field)
            if val is not None:
                readings.append({
                    "station_id": gauge_id,
                    "parameter": param,
                    "value": round(val, 3),
                    "unit": unit,
                    "timestamp": ts.isoformat() if ts else None,
                    "source": _SOURCE,
                })

    return {
        "source": _SOURCE,
        "network": "rain",
        "station_count": len(stations),
        "reading_count": len(readings),
        "stations": stations,
        "readings": readings,
    }


# ---------------------------------------------------------------------------
# Stream-gauge data (ArcGIS)
# ---------------------------------------------------------------------------

@cached(ttl=300, prefix="maricopa_stream:")
async def _fetch_stream() -> list[dict]:
    return await _query_arcgis(_STREAM_URL)


@router.get("/stream", summary="Current stream levels from all ALERT stream gauges")
async def maricopa_stream(
    min_discharge: float = Query(0.0, description="Only return gauges with >= this discharge (cfs)"),
) -> dict:
    """Return current stage and discharge from ~170 stream gauges.

    Data comes from the FCDMC ArcGIS MapServer (GeoJSON).
    Gauges report every 6 hours or every 1-3 minutes when flow is present.
    """
    features = await _fetch_stream()

    stations: list[dict] = []
    readings: list[dict] = []

    for feat in features:
        props = feat.get("properties", {})
        geom = feat.get("geometry", {})
        coords = geom.get("coordinates", [])
        if len(coords) < 2:
            continue

        gauge_id = str(props.get("GaugeID", ""))
        name = props.get("GaugeName", f"Gauge {gauge_id}")
        discharge = props.get("Discharge_CFS") or 0
        stage = props.get("Stage_Feet")
        nova_score = props.get("WaterLevelNovaScore")

        if discharge < min_discharge:
            continue

        record_ms = props.get("RecordDate")
        ts = (
            datetime.fromtimestamp(record_ms / 1000, tz=timezone.utc)
            if record_ms else None
        )
        ts_iso = ts.isoformat() if ts else None

        stations.append({
            "id": gauge_id,
            "name": name,
            "lat": coords[1],
            "lon": coords[0],
            "source": _SOURCE,
            "network": "stream",
            "timestamp": ts_iso,
            "nova_score": nova_score,
        })

        if discharge is not None:
            readings.append({
                "station_id": gauge_id,
                "parameter": "discharge",
                "value": float(discharge),
                "unit": "cfs",
                "timestamp": ts_iso,
                "source": _SOURCE,
            })
        if stage is not None:
            readings.append({
                "station_id": gauge_id,
                "parameter": "stage",
                "value": round(float(stage), 2),
                "unit": "ft",
                "timestamp": ts_iso,
                "source": _SOURCE,
            })

    return {
        "source": _SOURCE,
        "network": "stream",
        "station_count": len(stations),
        "reading_count": len(readings),
        "stations": stations,
        "readings": readings,
    }


# ---------------------------------------------------------------------------
# Weather station data (PHP raw-data scraping)
# ---------------------------------------------------------------------------

_PRE_RE = re.compile(
    r"(\d{2}/\d{2}/\d{4})\s+(\d{2}:\d{2}:\d{2})\s+([\d.\-]+)",
)

_AZ_TZ = timezone(timedelta(hours=-7))  # MST year-round (Arizona)


async def _fetch_wx_sensor(client: httpx.AsyncClient, sensor_id: str) -> list[dict]:
    """Fetch the latest readings for a single weather sensor via the PHP endpoint.

    Returns a list of dicts with keys: timestamp, value.
    The PHP endpoint returns an HTML page with a PRE block of tabular data.
    """
    now = datetime.now(tz=_AZ_TZ)
    form = {
        "ID": sensor_id,
        "ms": str(now.month),
        "ds": str(now.day),
        "ys": str(now.year),
        "hs": "0000",
        "ME": str(now.month),
        "DE": str(now.day),
        "YE": str(now.year),
        "HE": "2400",
        "G": "2",     # decimal places
        "R": "",
        "NM": "",
        "ST": "",
        "RD": "",
    }
    try:
        resp = await client.post(_RAW_DATA_URL, data=form)
        if resp.status_code != 200:
            return []
    except Exception:
        log.debug("Failed to fetch wx sensor %s", sensor_id, exc_info=True)
        return []

    rows: list[dict] = []
    for m in _PRE_RE.finditer(resp.text):
        try:
            dt = datetime.strptime(
                f"{m.group(1)} {m.group(2)}", "%m/%d/%Y %H:%M:%S"
            ).replace(tzinfo=_AZ_TZ)
            val = float(m.group(3))
            rows.append({"timestamp": dt, "value": val})
        except (ValueError, IndexError):
            continue
    return rows


@cached(ttl=300, prefix="maricopa_wx:")
async def _fetch_all_wx() -> dict[str, dict]:
    """Fetch latest readings for all weather stations.

    Returns {station_base_id: {sensor_suffix: latest_reading, ...}}.
    Only retrieves the most recent value per sensor to keep requests minimal.
    """
    results: dict[str, dict] = {}

    async with _client() as client:
        for stn in _WX_STATIONS:
            base_id = stn["id"]
            stn_data: dict[str, Any] = {}

            for suffix in stn["sensors"]:
                sensor_id = f"{base_id}{suffix}"
                rows = await _fetch_wx_sensor(client, sensor_id)
                if rows:
                    # Most recent reading (first row, data is newest-first)
                    latest = rows[0]
                    param, unit = _WX_SENSOR_SUFFIXES[suffix]
                    stn_data[suffix] = {
                        "parameter": param,
                        "value": latest["value"],
                        "unit": unit,
                        "timestamp": latest["timestamp"],
                    }
            results[base_id] = stn_data

    return results


@router.get("/weather", summary="Current weather station data")
async def maricopa_weather(
    station_id: Optional[str] = Query(None, description="Specific station base ID (e.g. 82700)"),
) -> dict:
    """Return latest weather readings from ALERT weather stations.

    Each station may report: temperature, humidity, dewpoint, pressure,
    wind speed, wind direction, wind gust, and solar radiation.

    **Note:** This endpoint scrapes the FCDMC raw-data report page.
    The full fetch for all ~33 stations takes several seconds; results are
    cached for 5 minutes.  Pass `station_id` for a faster single-station query.
    """
    if station_id:
        # Fast path: fetch just one station
        stn_meta = next((s for s in _WX_STATIONS if s["id"] == station_id), None)
        if not stn_meta:
            raise HTTPException(404, f"Weather station {station_id} not found")

        readings_out: list[dict] = []
        async with _client() as client:
            for suffix in stn_meta["sensors"]:
                sensor_id = f"{station_id}{suffix}"
                rows = await _fetch_wx_sensor(client, sensor_id)
                if rows:
                    latest = rows[0]
                    param, unit = _WX_SENSOR_SUFFIXES[suffix]
                    readings_out.append({
                        "station_id": station_id,
                        "parameter": param,
                        "value": latest["value"],
                        "unit": unit,
                        "timestamp": latest["timestamp"].isoformat(),
                        "source": _SOURCE,
                    })

        return {
            "source": _SOURCE,
            "network": "weather",
            "station": {
                "id": stn_meta["id"],
                "name": stn_meta["name"],
                "lat": stn_meta["lat"],
                "lon": stn_meta["lon"],
                "source": _SOURCE,
                "network": "weather",
            },
            "readings": readings_out,
        }

    # Full fetch (all stations, cached)
    all_wx = await _fetch_all_wx()

    stations_out: list[dict] = []
    readings_out = []

    for stn in _WX_STATIONS:
        base_id = stn["id"]
        stn_data = all_wx.get(base_id, {})
        if not stn_data:
            continue

        stations_out.append({
            "id": base_id,
            "name": stn["name"],
            "lat": stn["lat"],
            "lon": stn["lon"],
            "source": _SOURCE,
            "network": "weather",
        })

        for suffix, reading in stn_data.items():
            readings_out.append({
                "station_id": base_id,
                "parameter": reading["parameter"],
                "value": reading["value"],
                "unit": reading["unit"],
                "timestamp": reading["timestamp"].isoformat(),
                "source": _SOURCE,
            })

    return {
        "source": _SOURCE,
        "network": "weather",
        "station_count": len(stations_out),
        "reading_count": len(readings_out),
        "stations": stations_out,
        "readings": readings_out,
    }


# ---------------------------------------------------------------------------
# Overview
# ---------------------------------------------------------------------------

@router.get("/overview", summary="Maricopa County Flood Control ALERT overview")
async def maricopa_overview() -> dict:
    """Summary of the FCDMC ALERT system and available endpoints."""
    return {
        "source": _SOURCE,
        "name": SOURCE_NAME,
        "description": SOURCE_DESCRIPTION,
        "operator": "Flood Control District of Maricopa County (FCDMC)",
        "coverage": "Maricopa County, Arizona (~24,000 sq mi)",
        "auth_required": False,
        "update_frequency": "5 min (rain/weather); 6 hr or 1-3 min when flowing (stream)",
        "networks": [
            {
                "id": "rain",
                "name": "ALERT Rain Gauges",
                "description": "Tipping-bucket precipitation gauges",
                "station_count_approx": 378,
                "params": [
                    "precip_10min", "precip_30min", "precip_1hr",
                    "precip_3hr", "precip_6hr", "precip_1day",
                    "precip_3day", "precip_7day", "precip_ytd",
                ],
                "data_source": "ArcGIS MapServer (GeoJSON)",
                "endpoint": "/api/maricopa/rain",
            },
            {
                "id": "stream",
                "name": "ALERT Stream Gauges",
                "description": "Stage and discharge sensors on washes, rivers, and dam outlets",
                "station_count_approx": 170,
                "params": ["stage", "discharge"],
                "data_source": "ArcGIS MapServer (GeoJSON)",
                "endpoint": "/api/maricopa/stream",
                "notes": "WaterLevelNovaScore: 1=normal, 7=major flooding, -999=offline",
            },
            {
                "id": "weather",
                "name": "ALERT Weather Stations",
                "description": "Full weather stations reporting temp, humidity, dewpoint, pressure, wind, and solar",
                "station_count_approx": 42,
                "params": [
                    "temperature", "humidity", "dewpoint", "pressure",
                    "wind_speed", "wind_direction", "wind_gust", "solar_radiation",
                ],
                "data_source": "PHP raw-data scraper",
                "endpoint": "/api/maricopa/weather",
                "notes": "Weather data is scraped from HTML reports; cached for 5 min.",
            },
        ],
        "endpoints": [
            "/api/maricopa/rain",
            "/api/maricopa/stream",
            "/api/maricopa/weather",
            "/api/maricopa/overview",
        ],
        "data_urls": {
            "main": "https://alert.fcd.maricopa.gov/",
            "report_generator": "https://alert.fcd.maricopa.gov/showrpts_mc.html",
            "arcgis_rain": f"{_RAIN_URL}?where=1%3D1&outFields=*&f=geojson",
            "arcgis_stream": f"{_STREAM_URL}?where=1%3D1&outFields=*&f=geojson",
            "rainfall_map": "https://alert.fcd.maricopa.gov/alert/Google/v3/gmap.html",
        },
    }
