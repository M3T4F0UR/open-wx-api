"""Sensor.Community (formerly Luftdaten) — Open Environmental Data

Crowdsourced air quality and weather data from 35,000+ citizen-operated
sensors worldwide.  All data is CC0 (public domain), no API key required.

Endpoints use the area/country/type filter APIs to avoid the 100 MB+ bulk
data dump.

Data parameters: PM2.5, PM10, temperature, humidity, pressure.
"""

import math
from datetime import datetime, timezone
from typing import Optional

import httpx
from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import JSONResponse

import cache
import config
from models import Station, Reading, StationDetail

# ---------------------------------------------------------------------------
# Module-level metadata consumed by server.py /api/sources
# ---------------------------------------------------------------------------

SOURCE_NAME = "Sensor.Community"
SOURCE_DESCRIPTION = (
    "Open citizen-science environmental sensor network (formerly Luftdaten). "
    "35,000+ low-cost sensors reporting PM2.5, PM10, temperature, humidity, "
    "and pressure every 2–5 minutes.  CC0 licensed, no API key required."
)
SOURCE_META = {
    "category": "air_quality",
    "coverage": "Global (dense in Europe, growing elsewhere)",
    "auth_required": False,
    "update_frequency": "~2–5 min per sensor",
    "params": ["pm2.5", "pm10", "temperature", "humidity", "pressure"],
    "urls": {
        "homepage": "https://sensor.community",
        "api": "https://data.sensor.community",
        "map": "https://sensor.community/en/",
    },
}

# ---------------------------------------------------------------------------
# Router
# ---------------------------------------------------------------------------

router = APIRouter(prefix="/api/sensor_community", tags=["Sensor.Community"])

# ---------------------------------------------------------------------------
# Constants & HTTP helpers
# ---------------------------------------------------------------------------

_BASE = "https://data.sensor.community"
_AREA_BASE = f"{_BASE}/airrohr/v1/filter"

# Sensor.Community value type IDs in sensordatavalues
_VALUE_MAP = {
    "P1":          ("pm10",        "µg/m³"),
    "P2":          ("pm2.5",       "µg/m³"),
    "temperature": ("temperature", "°C"),
    "humidity":    ("humidity",    "%"),
    "pressure":    ("pressure",    "Pa"),   # returned in Pa; we convert to hPa
}

# Common sensor type slugs (for reference / filtering)
_PM_SENSOR_TYPES = {"SDS011", "SDS021", "PMS1003", "PMS3003", "PMS5003",
                    "PMS7003", "PMSx003", "HPM", "SPS30", "SEN5X"}


def _http_client() -> httpx.AsyncClient:
    return httpx.AsyncClient(
        timeout=config.HTTP_TIMEOUT,
        headers={"User-Agent": config.USER_AGENT},
        follow_redirects=True,
    )


# ---------------------------------------------------------------------------
# Data conversion helpers
# ---------------------------------------------------------------------------

def _parse_sensor(raw: dict) -> tuple[Station, list[Reading]]:
    """Convert one Sensor.Community JSON record to Station + Readings."""
    sensor = raw.get("sensor", {})
    sensor_id = sensor.get("id", 0)
    sensor_type = sensor.get("sensor_type", {}).get("name", "unknown")
    location = raw.get("location", {})

    lat = float(location.get("latitude", 0))
    lon = float(location.get("longitude", 0))
    country = location.get("country", "")

    # Parse timestamp
    ts_str = raw.get("timestamp")
    if ts_str:
        try:
            ts = datetime.fromisoformat(ts_str.replace(" ", "T")).replace(
                tzinfo=timezone.utc
            )
        except (ValueError, TypeError):
            ts = datetime.now(tz=timezone.utc)
    else:
        ts = datetime.now(tz=timezone.utc)

    # Extract readings from sensordatavalues
    readings: list[Reading] = []
    param_names: list[str] = []
    for sv in raw.get("sensordatavalues", []):
        vtype = sv.get("value_type", "")
        mapping = _VALUE_MAP.get(vtype)
        if mapping is None:
            continue
        param, unit = mapping
        try:
            val = float(sv["value"])
        except (KeyError, TypeError, ValueError):
            continue
        # Convert pressure from Pa to hPa for readability
        if param == "pressure":
            val = round(val / 100.0, 2)
            unit = "hPa"
        param_names.append(param)
        readings.append(Reading(
            station_id=f"sc_{sensor_id}",
            timestamp=ts,
            parameter=param,
            value=round(val, 2),
            unit=unit,
            source="sensor_community",
        ))

    station = Station(
        id=f"sc_{sensor_id}",
        name=f"Sensor.Community #{sensor_id} ({sensor_type})",
        lat=lat,
        lon=lon,
        source="sensor_community",
        network=sensor_type,
        params=param_names,
        url=f"https://sensor.community/en/sensor/{sensor_id}/",
        last_update=ts,
        extra={
            "sensor_id": sensor_id,
            "sensor_type": sensor_type,
            "country": country,
            "altitude": location.get("altitude"),
            "indoor": location.get("indoor", 0),
        },
    )
    return station, readings


def _deduplicate(records: list[dict]) -> list[dict]:
    """Sensor.Community returns one record per sensor *per value set*.

    A single physical node with a PM sensor + a DHT22 may produce two
    records with the same sensor_id.  We merge them so each sensor_id
    appears once, combining sensordatavalues.
    """
    by_sensor: dict[int, dict] = {}
    for rec in records:
        sid = rec.get("sensor", {}).get("id", 0)
        if sid in by_sensor:
            # Merge data values, keeping most recent timestamp
            existing = by_sensor[sid]
            existing_vals = {
                sv["value_type"]: sv for sv in existing.get("sensordatavalues", [])
            }
            for sv in rec.get("sensordatavalues", []):
                existing_vals[sv["value_type"]] = sv
            existing["sensordatavalues"] = list(existing_vals.values())
            # Keep the latest timestamp
            if rec.get("timestamp", "") > existing.get("timestamp", ""):
                existing["timestamp"] = rec["timestamp"]
        else:
            by_sensor[sid] = rec
    return list(by_sensor.values())


def _haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Great-circle distance in kilometres."""
    R = 6371.0
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (math.sin(dlat / 2) ** 2
         + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2))
         * math.sin(dlon / 2) ** 2)
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@router.get(
    "/nearby",
    summary="Nearby Sensor.Community sensors with latest data",
    response_description="Sensors within radius of a point, with latest readings",
)
async def nearby(
    lat: float = Query(..., description="Center latitude"),
    lon: float = Query(..., description="Center longitude"),
    radius_km: float = Query(25.0, ge=1, le=100, description="Search radius in km (max 100)"),
    sensor_type: Optional[str] = Query(
        None, description="Filter by sensor type, e.g. SDS011, BME280"
    ),
):
    """Return Sensor.Community sensors within *radius_km* of a point.

    Uses the ``/airrohr/v1/filter/area=`` endpoint (server-side circle filter).
    Results include last-5-minute readings for PM2.5, PM10, temperature,
    humidity, and pressure where available.
    """
    cache_key = f"sc_nearby:{lat}:{lon}:{radius_km}:{sensor_type}"
    cached_result = await cache.get(cache_key)
    if cached_result is not None:
        return cached_result

    url = f"{_AREA_BASE}/area={lat},{lon},{radius_km}"
    if sensor_type:
        url = f"{_AREA_BASE}/type={sensor_type}&area={lat},{lon},{radius_km}"

    async with _http_client() as client:
        try:
            resp = await client.get(url)
            resp.raise_for_status()
        except httpx.HTTPStatusError as exc:
            raise HTTPException(
                status_code=exc.response.status_code,
                detail=f"Sensor.Community API error: {exc}",
            )
        except httpx.RequestError as exc:
            raise HTTPException(
                status_code=502,
                detail=f"Sensor.Community request failed: {exc}",
            )

    raw_records = resp.json()
    records = _deduplicate(raw_records)

    stations = []
    for rec in records:
        station, readings = _parse_sensor(rec)
        # Skip sensors with bad coordinates
        if station.lat == 0 and station.lon == 0:
            continue
        dist = _haversine_km(lat, lon, station.lat, station.lon)
        stations.append({
            "station": station.model_dump(),
            "readings": [r.model_dump() for r in readings],
            "distance_km": round(dist, 2),
        })

    # Sort by distance
    stations.sort(key=lambda s: s["distance_km"])

    result = {
        "source": "sensor_community",
        "center": {"lat": lat, "lon": lon},
        "radius_km": radius_km,
        "count": len(stations),
        "stations": stations,
    }
    await cache.set(cache_key, result, ttl=300)  # 5-minute cache
    return result


@router.get(
    "/country/{country_code}",
    summary="Sensor.Community sensors by country",
    response_description="All sensors in a country with latest readings",
)
async def by_country(country_code: str):
    """Return all Sensor.Community sensors for a two-letter ISO country code.

    Example: ``/api/sensor_community/country/DE`` for Germany.

    **Note:** Large countries (DE, NL, etc.) may return thousands of sensors.
    """
    country_code = country_code.upper()
    if len(country_code) != 2:
        raise HTTPException(status_code=422, detail="Country code must be 2 letters (ISO 3166-1 alpha-2).")

    cache_key = f"sc_country:{country_code}"
    cached_result = await cache.get(cache_key)
    if cached_result is not None:
        return cached_result

    url = f"{_AREA_BASE}/country={country_code}"

    async with _http_client() as client:
        try:
            resp = await client.get(url, timeout=60.0)  # country queries can be large
            resp.raise_for_status()
        except httpx.HTTPStatusError as exc:
            raise HTTPException(
                status_code=exc.response.status_code,
                detail=f"Sensor.Community API error: {exc}",
            )
        except httpx.RequestError as exc:
            raise HTTPException(
                status_code=502,
                detail=f"Sensor.Community request failed: {exc}",
            )

    raw_records = resp.json()
    records = _deduplicate(raw_records)

    stations = []
    for rec in records:
        station, readings = _parse_sensor(rec)
        if station.lat == 0 and station.lon == 0:
            continue
        stations.append({
            "station": station.model_dump(),
            "readings": [r.model_dump() for r in readings],
        })

    result = {
        "source": "sensor_community",
        "country": country_code,
        "count": len(stations),
        "stations": stations,
    }
    await cache.set(cache_key, result, ttl=300)
    return result


@router.get(
    "/sensor/{sensor_id}",
    summary="Single Sensor.Community sensor latest data",
    response_description="Latest readings for one sensor",
)
async def sensor_detail(sensor_id: int):
    """Return the latest readings for a single Sensor.Community sensor.

    Uses the area filter with a tiny radius centered on the sensor's last
    known location (the API has no direct per-sensor-ID endpoint for latest
    data, so we filter by sensor ID from a small area query).  Falls back to
    the ``/static/v2/data.json`` per-sensor-type file if needed.
    """
    cache_key = f"sc_sensor:{sensor_id}"
    cached_result = await cache.get(cache_key)
    if cached_result is not None:
        return cached_result

    # The Sensor.Community API doesn't have a direct "get by sensor_id"
    # endpoint, but we can use the per-type static files which are smaller
    # than the full dump.  Type 1 = PM sensors (SDS011 etc.), which is the
    # most common.  We try types 1 and 14 (BME280 for temp/humidity).
    found_records: list[dict] = []

    async with _http_client() as client:
        for type_id in (1, 14):
            url = f"{_BASE}/static/v2/data.{type_id}.json"
            try:
                resp = await client.get(url, timeout=60.0)
                resp.raise_for_status()
                data = resp.json()
            except (httpx.HTTPError, ValueError):
                continue

            for rec in data:
                if rec.get("sensor", {}).get("id") == sensor_id:
                    found_records.append(rec)

            # If we found records in this type file, no need to check others
            if found_records:
                break

    if not found_records:
        raise HTTPException(
            status_code=404,
            detail=(
                f"Sensor {sensor_id} not found in recent data. "
                "It may be offline or the sensor ID may be incorrect."
            ),
        )

    merged = _deduplicate(found_records)
    station, readings = _parse_sensor(merged[0])

    result = StationDetail(station=station, readings=readings).model_dump()
    await cache.set(cache_key, result, ttl=120)  # 2-minute cache
    return result


@router.get(
    "/overview",
    summary="Sensor.Community source information",
    response_description="Metadata about the Sensor.Community data source",
)
async def overview():
    """Return descriptive metadata about the Sensor.Community data source."""
    return {
        "source": "sensor_community",
        "name": SOURCE_NAME,
        "description": SOURCE_DESCRIPTION,
        **SOURCE_META,
        "endpoints": [
            {
                "path": "/api/sensor_community/nearby",
                "method": "GET",
                "description": "Nearby sensors with latest data",
                "params": {
                    "lat": "Center latitude (required)",
                    "lon": "Center longitude (required)",
                    "radius_km": "Search radius in km, 1–100 (default 25)",
                    "sensor_type": "Filter by sensor type, e.g. SDS011, BME280 (optional)",
                },
            },
            {
                "path": "/api/sensor_community/country/{country_code}",
                "method": "GET",
                "description": "All sensors in a country (ISO 3166-1 alpha-2)",
            },
            {
                "path": "/api/sensor_community/sensor/{sensor_id}",
                "method": "GET",
                "description": "Latest readings for a single sensor by ID",
            },
            {
                "path": "/api/sensor_community/overview",
                "method": "GET",
                "description": "This endpoint — source metadata",
            },
        ],
        "notes": [
            "No API key required — all data is CC0 (public domain).",
            "Sensors report every 2–5 minutes; data reflects the last 5 minutes.",
            "P1 = PM10, P2 = PM2.5 in the raw API; this module normalises names.",
            "Pressure is returned in hPa (converted from raw Pa).",
            "35,000+ sensors worldwide, densest coverage in Germany and Europe.",
        ],
    }
