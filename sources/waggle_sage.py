"""Waggle/SAGE Urban Sensors — Argonne National Lab / University of Chicago

100+ edge-computing sensor nodes across the US (major deployment in Chicago),
successor to the Array of Things project.  Nodes carry environmental sensors
(temperature, humidity, pressure, wind, rain) plus cameras and AI accelerators
for urban-science research.

API docs: https://sagecontinuum.org/docs/
Data API: POST https://data.sagecontinuum.org/api/v1/query
Node manifests: https://auth.sagecontinuum.org/manifests/

No authentication required.  All data is publicly available.
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Optional

import httpx
from fastapi import APIRouter, HTTPException, Path, Query

import cache
import config
from models import Reading, Station, StationDetail

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Source registry metadata  (read by server.py /api/sources)
# ---------------------------------------------------------------------------

SOURCE_NAME = "Waggle/SAGE Urban Sensors"
SOURCE_DESCRIPTION = (
    "Waggle/SAGE is an NSF-funded cyberinfrastructure for AI-at-the-edge "
    "environmental sensing, operated by Argonne National Lab and the University "
    "of Chicago.  100+ nodes across the US measure temperature, humidity, "
    "pressure, wind, and rainfall.  No API key required."
)
SOURCE_META = {
    "url": "https://sagecontinuum.org/",
    "auth_required": False,
    "update_frequency": "Varies by node (typically 1–30 s)",
    "coverage": "US — major deployments in Chicago, plus partner sites",
    "params": [
        "temperature", "relative_humidity", "pressure",
        "wind_speed", "wind_direction", "precipitation",
    ],
    "category": "weather",
    "station_count": 100,
}

# ---------------------------------------------------------------------------
# Router
# ---------------------------------------------------------------------------

router = APIRouter(prefix="/api/waggle", tags=["Waggle/SAGE"])

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_DATA_API = "https://data.sagecontinuum.org/api/v1/query"
_MANIFESTS_URL = "https://auth.sagecontinuum.org/manifests/"

# Waggle sensor name -> (human name, unit)
_SENSOR_MAP: dict[str, tuple[str, str]] = {
    "env.temperature":        ("temperature",        "°C"),
    "env.relative_humidity":  ("relative_humidity",   "%"),
    "env.pressure":           ("pressure",            "hPa"),
    "env.raingauge.total_acc": ("precipitation",      "mm"),
    "env.wind.speed":         ("wind_speed",          "m/s"),
    "env.wind.direction":     ("wind_direction",      "°"),
}

# Reverse lookup: human name -> waggle sensor name
_HUMAN_TO_SENSOR = {v[0]: k for k, v in _SENSOR_MAP.items()}

# All sensor names we query by default
_ALL_SENSOR_NAMES = list(_SENSOR_MAP.keys())

_SOURCE = "waggle_sage"


# ---------------------------------------------------------------------------
# HTTP helper
# ---------------------------------------------------------------------------

def _http_client() -> httpx.AsyncClient:
    return httpx.AsyncClient(
        timeout=config.HTTP_TIMEOUT,
        headers={"User-Agent": config.USER_AGENT},
        follow_redirects=True,
    )


async def _query_sage(body: dict) -> list[dict]:
    """POST to the SAGE data API and return parsed NDJSON records.

    The SAGE data API returns newline-delimited JSON (NDJSON), not a single
    JSON array.  Each line is one measurement record.
    """
    async with _http_client() as client:
        try:
            resp = await client.post(
                _DATA_API,
                json=body,
                headers={"Content-Type": "application/json"},
            )
            resp.raise_for_status()
        except httpx.HTTPStatusError as exc:
            raise HTTPException(
                status_code=exc.response.status_code,
                detail=f"SAGE data API error: {exc}",
            )
        except httpx.RequestError as exc:
            raise HTTPException(
                status_code=502,
                detail=f"SAGE data API request failed: {exc}",
            )

    # Parse NDJSON — each line is a JSON object
    records: list[dict] = []
    import json
    for line in resp.text.strip().splitlines():
        line = line.strip()
        if line:
            try:
                records.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    return records


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _parse_timestamp(ts: str) -> datetime:
    """Parse an ISO-8601 timestamp from SAGE, always returning UTC."""
    ts = ts.rstrip("Z")
    # Handle nanosecond precision — truncate to microseconds
    if "." in ts:
        date_part, frac = ts.split(".", 1)
        frac = frac[:6]  # truncate to microseconds
        ts = f"{date_part}.{frac}"
    return datetime.fromisoformat(ts).replace(tzinfo=timezone.utc)


def _record_to_reading(rec: dict) -> Optional[Reading]:
    """Convert one SAGE NDJSON record to a Reading, or None if unrecognised."""
    sensor_name = rec.get("name", "")
    mapping = _SENSOR_MAP.get(sensor_name)
    if mapping is None:
        return None
    param, unit = mapping
    try:
        value = float(rec["value"])
    except (KeyError, TypeError, ValueError):
        return None

    vsn = rec.get("meta", {}).get("vsn", "unknown")
    ts = _parse_timestamp(rec.get("timestamp", ""))

    return Reading(
        station_id=f"waggle_{vsn}",
        timestamp=ts,
        parameter=param,
        value=round(value, 2),
        unit=unit,
        source=_SOURCE,
    )


def _build_station(manifest: dict) -> Station:
    """Build a Station from a SAGE manifest entry."""
    vsn = manifest.get("vsn", "unknown")
    lat = float(manifest.get("gps_lat", 0) or 0)
    lon = float(manifest.get("gps_lon", 0) or 0)
    name = manifest.get("name", "") or f"Waggle Node {vsn}"
    phase = manifest.get("phase", "")
    focus = manifest.get("focus", "")

    return Station(
        id=f"waggle_{vsn}",
        name=name,
        lat=lat,
        lon=lon,
        source=_SOURCE,
        network="sage",
        params=list(_SENSOR_MAP.values()),
        url=f"https://portal.sagecontinuum.org/node/{vsn}",
        extra={
            "vsn": vsn,
            "phase": phase,
            "focus": focus,
        },
    )


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@router.get(
    "/nodes",
    summary="List all Waggle/SAGE sensor nodes",
    response_description="All registered SAGE nodes with location metadata",
)
async def list_nodes():
    """Return all SAGE sensor nodes from the public manifest registry.

    Each entry includes the node VSN (identifier), GPS coordinates,
    deployment phase, and focus area.
    """
    cache_key = "waggle:nodes"
    cached = await cache.get(cache_key)
    if cached is not None:
        return cached

    async with _http_client() as client:
        try:
            resp = await client.get(_MANIFESTS_URL)
            resp.raise_for_status()
            manifests = resp.json()
        except httpx.HTTPStatusError as exc:
            raise HTTPException(
                status_code=exc.response.status_code,
                detail=f"SAGE manifests API error: {exc}",
            )
        except httpx.RequestError as exc:
            raise HTTPException(
                status_code=502,
                detail=f"SAGE manifests request failed: {exc}",
            )

    # manifests may be a list or a dict keyed by VSN
    if isinstance(manifests, dict):
        items = list(manifests.values())
    elif isinstance(manifests, list):
        items = manifests
    else:
        items = []

    nodes = []
    for m in items:
        station = _build_station(m)
        nodes.append(station.model_dump())

    result = {
        "source": _SOURCE,
        "count": len(nodes),
        "nodes": nodes,
    }
    await cache.set(cache_key, result, ttl=config.CACHE_TTL_LONG)
    return result


@router.get(
    "/latest",
    summary="Latest readings from all SAGE nodes",
    response_description="Most recent measurement for each node",
)
async def latest_readings(
    sensor: str = Query(
        "env.temperature",
        description=(
            "SAGE sensor name to query. Options: "
            "env.temperature, env.relative_humidity, env.pressure, "
            "env.raingauge.total_acc, env.wind.speed, env.wind.direction"
        ),
    ),
):
    """Return the latest reading for a given sensor across all SAGE nodes.

    Queries the last 1 hour of data and returns only the most recent
    measurement per node.
    """
    cache_key = f"waggle:latest:{sensor}"
    cached = await cache.get(cache_key)
    if cached is not None:
        return cached

    records = await _query_sage({
        "start": "-1h",
        "filter": {"name": sensor},
    })

    # Group by VSN and keep only the latest record per node
    latest_by_vsn: dict[str, dict] = {}
    for rec in records:
        vsn = rec.get("meta", {}).get("vsn", "unknown")
        existing = latest_by_vsn.get(vsn)
        if existing is None or rec.get("timestamp", "") > existing.get("timestamp", ""):
            latest_by_vsn[vsn] = rec

    stations: list[dict] = []
    for vsn, rec in latest_by_vsn.items():
        reading = _record_to_reading(rec)
        if reading is None:
            continue
        meta = rec.get("meta", {})
        lat = float(meta.get("lat", 0) or 0)
        lon = float(meta.get("lon", 0) or 0)
        station = Station(
            id=f"waggle_{vsn}",
            name=meta.get("host", f"Waggle Node {vsn}"),
            lat=lat,
            lon=lon,
            source=_SOURCE,
            network="sage",
            params=[reading.parameter],
            url=f"https://portal.sagecontinuum.org/node/{vsn}",
            last_update=reading.timestamp,
            extra={"vsn": vsn},
        )
        stations.append({
            "station": station.model_dump(),
            "reading": reading.model_dump(),
        })

    # Sort by VSN for stable ordering
    stations.sort(key=lambda s: s["station"]["id"])

    result = {
        "source": _SOURCE,
        "sensor": sensor,
        "count": len(stations),
        "stations": stations,
    }
    await cache.set(cache_key, result, ttl=300)
    return result


@router.get(
    "/node/{vsn}/data",
    summary="Time series data for a single SAGE node",
    response_description="Chronological readings for one node and sensor",
)
async def node_data(
    vsn: str = Path(..., description="Node VSN identifier (e.g. W039)"),
    sensor: str = Query(
        "env.temperature",
        description=(
            "SAGE sensor name. Options: "
            "env.temperature, env.relative_humidity, env.pressure, "
            "env.raingauge.total_acc, env.wind.speed, env.wind.direction"
        ),
    ),
    hours: int = Query(24, ge=1, le=168, description="Hours of history (max 168 = 7 days)"),
):
    """Return a time series of readings for one SAGE node and sensor.

    The ``vsn`` path parameter is the node's unique identifier (e.g. W039).
    Data is returned in chronological order.
    """
    cache_key = f"waggle:node:{vsn}:{sensor}:{hours}"
    cached = await cache.get(cache_key)
    if cached is not None:
        return cached

    records = await _query_sage({
        "start": f"-{hours}h",
        "filter": {
            "name": sensor,
            "vsn": vsn,
        },
    })

    readings: list[dict] = []
    for rec in records:
        reading = _record_to_reading(rec)
        if reading is not None:
            readings.append(reading.model_dump())

    # Sort chronologically
    readings.sort(key=lambda r: r["timestamp"])

    mapping = _SENSOR_MAP.get(sensor)
    result = {
        "source": _SOURCE,
        "vsn": vsn,
        "sensor": sensor,
        "parameter": mapping[0] if mapping else sensor,
        "unit": mapping[1] if mapping else "",
        "hours": hours,
        "count": len(readings),
        "readings": readings,
    }
    await cache.set(cache_key, result, ttl=300)
    return result


@router.get(
    "/overview",
    summary="Waggle/SAGE source information",
    response_description="Metadata about the Waggle/SAGE data source",
)
async def overview():
    """Return metadata about the Waggle/SAGE data source including
    available sensors, coverage, and API details."""
    return {
        "source": _SOURCE,
        "name": SOURCE_NAME,
        "description": SOURCE_DESCRIPTION,
        "sensors": {
            name: {"parameter": human, "unit": unit}
            for name, (human, unit) in _SENSOR_MAP.items()
        },
        "api": {
            "data_endpoint": _DATA_API,
            "manifests_endpoint": _MANIFESTS_URL,
            "auth_required": False,
            "method": "POST (NDJSON response)",
        },
        "meta": SOURCE_META,
    }
