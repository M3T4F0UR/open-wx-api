"""Citizen & Environmental Sensors (Extended)

Additional citizen-science and environmental monitoring networks:
  - OpenSenseMap   (senseBox IoT sensors: temp, humidity, pressure, PM, UV, noise)
  - Safecast       (crowd-sourced radiation monitoring, 150M+ readings, CC0)
  - EPA RadNet     (US fixed air-monitoring network, gamma radiation)
"""

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

SOURCE_NAME = "Citizen & Environmental Sensors"
SOURCE_DESCRIPTION = (
    "Extended citizen-science and environmental sensor networks: "
    "OpenSenseMap IoT sensors (temperature, humidity, pressure, particulates, UV, noise), "
    "Safecast crowd-sourced radiation monitoring, and EPA RadNet fixed air monitors."
)
SOURCE_META = {
    "category": "environmental/citizen_science",
    "coverage": "Global (OpenSenseMap, Safecast) + US (RadNet)",
    "auth_required": False,
    "update_frequency": "varies (minutes to hours depending on network)",
    "params": [
        "temperature", "humidity", "pressure",
        "pm2.5", "pm10", "uv", "noise", "light",
        "gamma_radiation",
    ],
    "networks": ["opensensemap", "safecast", "radnet"],
    "urls": {
        "opensensemap": "https://opensensemap.org",
        "safecast": "https://safecast.org",
        "radnet": "https://www.epa.gov/radnet",
    },
}

# ---------------------------------------------------------------------------
# Router
# ---------------------------------------------------------------------------

router = APIRouter(prefix="/api/citizen_ext", tags=["Citizen & Environmental Sensors"])

# ---------------------------------------------------------------------------
# Shared HTTP helpers
# ---------------------------------------------------------------------------

_OPENSENSEMAP_BASE = "https://api.opensensemap.org"
_SAFECAST_BASE = "https://api.safecast.org"
_RADNET_BASE = "https://data.epa.gov/efservice"


def _http_client() -> httpx.AsyncClient:
    return httpx.AsyncClient(
        timeout=config.HTTP_TIMEOUT,
        headers={"User-Agent": config.USER_AGENT},
        follow_redirects=True,
    )


# ---------------------------------------------------------------------------
# OpenSenseMap helpers
# ---------------------------------------------------------------------------

def _osm_station(box: dict) -> Station:
    """Convert an OpenSenseMap senseBox record to a Station model."""
    box_id = box.get("_id", "")
    coords = box.get("currentLocation", {}).get("coordinates", [0, 0])
    # OpenSenseMap returns [lon, lat] (GeoJSON order)
    lon = float(coords[0]) if len(coords) > 0 else 0.0
    lat = float(coords[1]) if len(coords) > 1 else 0.0

    sensor_names = [s.get("title", "") for s in box.get("sensors", [])]
    params = []
    _param_keywords = {
        "temperature": "temperature",
        "temperatur": "temperature",
        "humidity": "humidity",
        "luftfeuchte": "humidity",
        "pressure": "pressure",
        "luftdruck": "pressure",
        "pm2.5": "pm2.5",
        "pm10": "pm10",
        "uv": "uv",
        "noise": "noise",
        "lautstärke": "noise",
        "light": "light",
        "beleuchtung": "light",
        "licht": "light",
    }
    for name in sensor_names:
        lower = name.lower()
        for keyword, param in _param_keywords.items():
            if keyword in lower and param not in params:
                params.append(param)

    updated = box.get("updatedAt") or box.get("lastMeasurementAt")
    last_update = None
    if updated:
        try:
            last_update = datetime.fromisoformat(updated.replace("Z", "+00:00"))
        except (ValueError, AttributeError):
            pass

    return Station(
        id=f"opensensemap_{box_id}",
        name=box.get("name", f"senseBox {box_id}"),
        lat=lat,
        lon=lon,
        source="citizen_ext",
        network="opensensemap",
        params=params,
        elevation=box.get("currentLocation", {}).get("coordinates", [0, 0, None])[2]
        if len(box.get("currentLocation", {}).get("coordinates", [])) > 2 else None,
        url=f"https://opensensemap.org/explore/{box_id}",
        last_update=last_update,
        extra={
            "box_id": box_id,
            "exposure": box.get("exposure"),
            "model": box.get("model"),
            "sensor_count": len(box.get("sensors", [])),
        },
    )


def _osm_readings(box: dict) -> list[Reading]:
    """Extract latest readings from an OpenSenseMap senseBox."""
    readings = []
    box_id = box.get("_id", "")
    station_id = f"opensensemap_{box_id}"

    for sensor in box.get("sensors", []):
        last = sensor.get("lastMeasurement")
        if not last:
            continue
        val = last.get("value")
        if val is None:
            continue
        try:
            value = float(val)
        except (ValueError, TypeError):
            continue

        ts_str = last.get("createdAt", "")
        try:
            ts = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
        except (ValueError, AttributeError):
            ts = datetime.now(tz=timezone.utc)

        readings.append(Reading(
            station_id=station_id,
            timestamp=ts,
            parameter=sensor.get("title", "unknown"),
            value=value,
            unit=sensor.get("unit", ""),
            source="opensensemap",
        ))

    return readings


# ---------------------------------------------------------------------------
# OpenSenseMap endpoints
# ---------------------------------------------------------------------------

@router.get(
    "/opensensemap/nearby",
    summary="Nearby OpenSenseMap senseBoxes",
    response_description="List of senseBox stations near a point",
)
async def opensensemap_nearby(
    lat: float = Query(..., description="Latitude of search center"),
    lon: float = Query(..., description="Longitude of search center"),
    radius_m: int = Query(10000, description="Search radius in metres (max 50000)", ge=100, le=50000),
):
    """Find OpenSenseMap senseBoxes near a given point."""
    cache_key = f"citizen_ext:osm:nearby:{lat:.3f}:{lon:.3f}:{radius_m}"
    cached = await cache.get(cache_key)
    if cached is not None:
        return JSONResponse(content=cached)

    # OpenSenseMap expects near=lon,lat (GeoJSON order)
    url = f"{_OPENSENSEMAP_BASE}/boxes"
    params = {
        "near": f"{lon},{lat}",
        "maxDistance": str(radius_m),
        "format": "json",
    }

    async with _http_client() as client:
        try:
            resp = await client.get(url, params=params)
            resp.raise_for_status()
        except httpx.HTTPStatusError as exc:
            raise HTTPException(status_code=502, detail=f"OpenSenseMap API error: {exc.response.status_code}")
        except httpx.RequestError as exc:
            raise HTTPException(status_code=502, detail=f"OpenSenseMap request failed: {exc}")

    boxes = resp.json()
    if not isinstance(boxes, list):
        boxes = []

    stations = [_osm_station(b) for b in boxes]
    result = {
        "source": "opensensemap",
        "query": {"lat": lat, "lon": lon, "radius_m": radius_m},
        "count": len(stations),
        "stations": [s.model_dump(mode="json") for s in stations],
    }
    await cache.set(cache_key, result, ttl=config.CACHE_TTL)
    return JSONResponse(content=result)


@router.get(
    "/opensensemap/{box_id}",
    summary="Single OpenSenseMap senseBox with sensors and readings",
    response_description="Detailed senseBox info including latest sensor readings",
)
async def opensensemap_box(box_id: str):
    """Get a single OpenSenseMap senseBox by ID, including latest sensor readings."""
    cache_key = f"citizen_ext:osm:box:{box_id}"
    cached = await cache.get(cache_key)
    if cached is not None:
        return JSONResponse(content=cached)

    url = f"{_OPENSENSEMAP_BASE}/boxes/{box_id}"

    async with _http_client() as client:
        try:
            resp = await client.get(url)
            resp.raise_for_status()
        except httpx.HTTPStatusError as exc:
            if exc.response.status_code == 404:
                raise HTTPException(status_code=404, detail=f"senseBox {box_id} not found")
            raise HTTPException(status_code=502, detail=f"OpenSenseMap API error: {exc.response.status_code}")
        except httpx.RequestError as exc:
            raise HTTPException(status_code=502, detail=f"OpenSenseMap request failed: {exc}")

    box = resp.json()
    station = _osm_station(box)
    readings = _osm_readings(box)

    result = {
        "source": "opensensemap",
        "station": station.model_dump(mode="json"),
        "readings": [r.model_dump(mode="json") for r in readings],
    }
    await cache.set(cache_key, result, ttl=config.CACHE_TTL)
    return JSONResponse(content=result)


# ---------------------------------------------------------------------------
# Safecast helpers
# ---------------------------------------------------------------------------

def _safecast_reading(m: dict) -> Optional[dict]:
    """Convert a Safecast measurement record to a reading dict."""
    try:
        return {
            "id": m.get("id"),
            "lat": float(m.get("latitude", 0)),
            "lon": float(m.get("longitude", 0)),
            "value": float(m.get("value", 0)),
            "unit": m.get("unit", "cpm"),
            "device_id": m.get("device_id"),
            "captured_at": m.get("captured_at"),
            "height": m.get("height"),
        }
    except (ValueError, TypeError):
        return None


# ---------------------------------------------------------------------------
# Safecast endpoints
# ---------------------------------------------------------------------------

@router.get(
    "/safecast/nearby",
    summary="Safecast radiation measurements near a point",
    response_description="Radiation readings (background gamma) near the given location",
)
async def safecast_nearby(
    lat: float = Query(..., description="Latitude of search center"),
    lon: float = Query(..., description="Longitude of search center"),
    distance: int = Query(1000, description="Search distance in metres (default 1000)", ge=100, le=100000),
):
    """Find Safecast radiation measurements near a given point."""
    cache_key = f"citizen_ext:safecast:nearby:{lat:.3f}:{lon:.3f}:{distance}"
    cached = await cache.get(cache_key)
    if cached is not None:
        return JSONResponse(content=cached)

    url = f"{_SAFECAST_BASE}/measurements.json"
    params = {
        "latitude": str(lat),
        "longitude": str(lon),
        "distance": str(distance),
    }

    async with _http_client() as client:
        try:
            resp = await client.get(url, params=params)
            resp.raise_for_status()
        except httpx.HTTPStatusError as exc:
            raise HTTPException(status_code=502, detail=f"Safecast API error: {exc.response.status_code}")
        except httpx.RequestError as exc:
            raise HTTPException(status_code=502, detail=f"Safecast request failed: {exc}")

    raw = resp.json()
    if not isinstance(raw, list):
        raw = []

    measurements = [r for r in (_safecast_reading(m) for m in raw) if r is not None]

    result = {
        "source": "safecast",
        "query": {"lat": lat, "lon": lon, "distance": distance},
        "count": len(measurements),
        "measurements": measurements,
        "note": "Values typically in CPM (counts per minute). Divide by ~334 for approximate uSv/hr.",
    }
    await cache.set(cache_key, result, ttl=config.CACHE_TTL)
    return JSONResponse(content=result)


# ---------------------------------------------------------------------------
# EPA RadNet helpers
# ---------------------------------------------------------------------------

def _radnet_station(rec: dict) -> Station:
    """Convert an EPA RadNet location record to a Station model."""
    loc_id = str(rec.get("ANA_NUM", rec.get("LOCATION_NUM", "")))
    return Station(
        id=f"radnet_{loc_id}",
        name=rec.get("CITY_NAME", rec.get("ANA_NAME", f"RadNet #{loc_id}")),
        lat=float(rec.get("LATITUDE", 0)),
        lon=float(rec.get("LONGITUDE", 0)),
        source="citizen_ext",
        network="radnet",
        params=["gamma_radiation"],
        elevation=None,
        url="https://www.epa.gov/radnet",
        last_update=None,
        extra={
            "location_id": loc_id,
            "state": rec.get("STATE_ABBR", rec.get("STATE", "")),
            "county": rec.get("COUNTY_NAME", ""),
        },
    )


# ---------------------------------------------------------------------------
# EPA RadNet endpoints
# ---------------------------------------------------------------------------

@router.get(
    "/radnet/stations",
    summary="EPA RadNet monitor locations",
    response_description="List of EPA RadNet fixed air-monitoring stations across all 50 US states",
)
async def radnet_stations():
    """Get EPA RadNet fixed air-monitoring station locations.

    RadNet operates ~140 monitors measuring gamma radiation and airborne
    radioactivity across all 50 US states.
    """
    cache_key = "citizen_ext:radnet:stations"
    cached = await cache.get(cache_key)
    if cached is not None:
        return JSONResponse(content=cached)

    url = f"{_RADNET_BASE}/ERM_LOCATION/ROWS/0:999/JSON"

    async with _http_client() as client:
        try:
            resp = await client.get(url)
            resp.raise_for_status()
        except httpx.HTTPStatusError as exc:
            raise HTTPException(status_code=502, detail=f"EPA RadNet API error: {exc.response.status_code}")
        except httpx.RequestError as exc:
            raise HTTPException(status_code=502, detail=f"EPA RadNet request failed: {exc}")

    raw = resp.json()
    if not isinstance(raw, list):
        raw = []

    # Filter to records that have lat/lon
    stations = []
    for rec in raw:
        if rec.get("LATITUDE") and rec.get("LONGITUDE"):
            try:
                stations.append(_radnet_station(rec))
            except (ValueError, TypeError):
                continue

    result = {
        "source": "radnet",
        "count": len(stations),
        "stations": [s.model_dump(mode="json") for s in stations],
    }
    await cache.set(cache_key, result, ttl=config.CACHE_TTL_LONG)
    return JSONResponse(content=result)


# ---------------------------------------------------------------------------
# Overview endpoint
# ---------------------------------------------------------------------------

@router.get(
    "/overview",
    summary="Citizen & Environmental Sensors source overview",
    response_description="Metadata about available citizen-science and environmental sensor networks",
)
async def overview():
    """Return metadata about the citizen-science and environmental sensor networks
    available through this module."""
    return JSONResponse(content={
        "source_name": SOURCE_NAME,
        "description": SOURCE_DESCRIPTION,
        "networks": {
            "opensensemap": {
                "name": "OpenSenseMap",
                "url": "https://opensensemap.org",
                "description": (
                    "Community platform for open environmental sensor data. "
                    "~14,000 senseBox IoT stations worldwide measuring temperature, "
                    "humidity, pressure, particulates (PM2.5/PM10), UV, noise, and light."
                ),
                "auth_required": False,
                "license": "ODbL",
                "parameters": ["temperature", "humidity", "pressure", "pm2.5", "pm10", "uv", "noise", "light"],
                "docs": "https://docs.opensensemap.org/",
            },
            "safecast": {
                "name": "Safecast",
                "url": "https://safecast.org",
                "description": (
                    "Crowd-sourced environmental radiation monitoring. "
                    "Over 150 million readings of background gamma radiation globally. "
                    "Data is CC0 (public domain)."
                ),
                "auth_required": False,
                "license": "CC0",
                "parameters": ["gamma_radiation"],
                "docs": "https://api.safecast.org/",
            },
            "radnet": {
                "name": "EPA RadNet",
                "url": "https://www.epa.gov/radnet",
                "description": (
                    "US Environmental Protection Agency radiation monitoring network. "
                    "~140 fixed air monitors measuring gamma radiation and airborne "
                    "radioactivity across all 50 states."
                ),
                "auth_required": False,
                "license": "US Government Public Domain",
                "parameters": ["gamma_radiation"],
                "docs": "https://www.epa.gov/radnet/radnet-data",
            },
        },
    })
