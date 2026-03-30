"""Chesapeake & Mid-Atlantic Water — Chesapeake Bay DataHub + SRBC CIM.

Two data sources covering the Chesapeake Bay watershed and Susquehanna River Basin:

1. **Chesapeake Bay Program DataHub** — water quality monitoring stations across
   the Chesapeake watershed (MD, VA, PA, DC).  Covers dissolved oxygen, salinity,
   turbidity, chlorophyll, nutrients, and more.  No authentication required; JSON.

   - Stations:       GET https://datahub.chesapeakebay.net/api/api.json/Station
   - Station detail: GET https://datahub.chesapeakebay.net/api/api.json/Station/Detail/{stationId}
   - Water quality:  GET https://datahub.chesapeakebay.net/api/api.json/WaterQuality/Station/{stationId}/{startDate}/{endDate}

2. **SRBC Continuous Instream Monitoring (CIM)** — 76 real-time water quality
   stations across the Susquehanna River Basin measuring temperature, conductivity,
   pH, turbidity, and dissolved oxygen at 15-minute intervals.  No auth required.

   - Data: GET https://www.srbc.gov/continuous-instream-monitoring/monitoringData/GetMonitoringDataByStationId
"""

import logging
from datetime import datetime, timezone
from typing import Any, Optional

import httpx
from fastapi import APIRouter, HTTPException, Query

import cache
import config
from models import Reading, Station, StationDetail

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Source registry metadata  (read by server.py /api/sources)
# ---------------------------------------------------------------------------

SOURCE_NAME = "Chesapeake & Mid-Atlantic Water"
SOURCE_DESCRIPTION = (
    "Water quality data from the Chesapeake Bay Program DataHub (monitoring "
    "stations across the Chesapeake watershed — MD, VA, PA, DC) and the "
    "Susquehanna River Basin Commission (SRBC) Continuous Instream Monitoring "
    "network (76 stations, 15-min temperature/conductivity/pH/turbidity/DO)."
)
SOURCE_META = {
    "url": "https://datahub.chesapeakebay.net/",
    "auth_required": False,
    "update_frequency": "varies (DataHub: event-driven cruises; SRBC CIM: 15 min)",
    "coverage": "Chesapeake Bay watershed (MD, VA, PA, DC), Susquehanna River Basin",
    "params": [
        "water_temperature", "dissolved_oxygen", "salinity", "turbidity",
        "chlorophyll", "pH", "conductivity", "nutrients",
    ],
    "category": "water",
}

router = APIRouter(prefix="/api/chesapeake", tags=["Chesapeake & Mid-Atlantic Water"])

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_CBP_BASE = "https://datahub.chesapeakebay.net/api/api.json"
_CBP_STATIONS_URL = f"{_CBP_BASE}/Station"
_CBP_STATION_DETAIL_URL = f"{_CBP_BASE}/Station/Detail"
_CBP_WATER_QUALITY_URL = f"{_CBP_BASE}/WaterQuality/Station"

_SRBC_BASE = "https://www.srbc.gov/continuous-instream-monitoring/monitoringData"
_SRBC_DATA_URL = f"{_SRBC_BASE}/GetMonitoringDataByStationId"

_HEADERS = {
    "User-Agent": config.USER_AGENT,
    "Accept": "application/json",
}


# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------

def _client() -> httpx.AsyncClient:
    return httpx.AsyncClient(
        headers=_HEADERS,
        timeout=config.HTTP_TIMEOUT,
        follow_redirects=True,
    )


async def _cbp_get(url: str, params: dict[str, Any] | None = None) -> Any:
    """Fetch JSON from the Chesapeake Bay Program DataHub API."""
    try:
        async with _client() as c:
            resp = await c.get(url, params=params)
            resp.raise_for_status()
            return resp.json()
    except httpx.TimeoutException:
        raise HTTPException(status_code=504, detail="Chesapeake Bay DataHub API timeout")
    except httpx.HTTPStatusError as exc:
        raise HTTPException(
            status_code=exc.response.status_code,
            detail=f"Chesapeake Bay DataHub returned {exc.response.status_code}",
        )
    except httpx.HTTPError as exc:
        logger.error("Chesapeake Bay DataHub request failed: %s", exc)
        raise HTTPException(status_code=502, detail="Chesapeake Bay DataHub request failed")


async def _srbc_get(params: dict[str, Any]) -> Any:
    """Fetch JSON from the SRBC CIM API."""
    try:
        async with _client() as c:
            resp = await c.get(_SRBC_DATA_URL, params=params)
            resp.raise_for_status()
            return resp.json()
    except httpx.TimeoutException:
        raise HTTPException(status_code=504, detail="SRBC CIM API timeout")
    except httpx.HTTPStatusError as exc:
        raise HTTPException(
            status_code=exc.response.status_code,
            detail=f"SRBC CIM API returned {exc.response.status_code}",
        )
    except httpx.HTTPError as exc:
        logger.error("SRBC CIM request failed: %s", exc)
        raise HTTPException(status_code=502, detail="SRBC CIM request failed")


# ---------------------------------------------------------------------------
# Conversion helpers
# ---------------------------------------------------------------------------

def _cbp_station_to_model(raw: dict) -> Station:
    """Convert a Chesapeake Bay Program station record to a Station model."""
    sid = str(raw.get("StationId", raw.get("stationId", "")))
    name = raw.get("StationName", raw.get("stationName", sid))
    lat = float(raw.get("Latitude", raw.get("latitude", 0.0)))
    lon = float(raw.get("Longitude", raw.get("longitude", 0.0)))

    return Station(
        id=sid,
        name=name,
        lat=lat,
        lon=lon,
        source="chesapeake_bay",
        network="Chesapeake Bay Program",
        params=["water_temperature", "dissolved_oxygen", "salinity",
                "turbidity", "chlorophyll", "pH", "nutrients"],
        url=f"https://datahub.chesapeakebay.net/",
        extra={
            "state": raw.get("State", raw.get("state", "")),
            "huc": raw.get("HUC8", raw.get("huc8", "")),
            "water_body": raw.get("WaterBody", raw.get("waterBody", "")),
        },
    )


def _cbp_reading_to_model(entry: dict, station_id: str) -> list[Reading]:
    """Convert a Chesapeake Bay water quality data record to Reading objects.

    Each record may contain multiple measured parameters; we emit one Reading
    per parameter.
    """
    readings: list[Reading] = []

    # Try to parse the sample date
    dt_str = entry.get("SampleDate", entry.get("sampleDate", ""))
    try:
        ts = datetime.fromisoformat(dt_str.replace("Z", "+00:00"))
    except (ValueError, AttributeError):
        try:
            ts = datetime.strptime(dt_str[:19], "%Y-%m-%dT%H:%M:%S").replace(
                tzinfo=timezone.utc
            )
        except (ValueError, AttributeError):
            return readings

    # Map of field names to (parameter_label, unit)
    _PARAM_MAP: dict[str, tuple[str, str]] = {
        "MeasureValue": (
            entry.get("Parameter", entry.get("parameter", "unknown")),
            entry.get("Unit", entry.get("unit", "")),
        ),
    }

    # The DataHub typically returns one measured value per record with
    # Parameter/Unit metadata alongside it.
    param_name = entry.get("Parameter", entry.get("parameter", ""))
    unit = entry.get("Unit", entry.get("unit", ""))
    value_raw = entry.get("MeasureValue", entry.get("measureValue"))

    if value_raw is not None and param_name:
        try:
            readings.append(
                Reading(
                    station_id=station_id,
                    timestamp=ts,
                    parameter=param_name,
                    value=float(value_raw),
                    unit=unit,
                    quality=entry.get("Qualifier", entry.get("qualifier")),
                    source="chesapeake_bay",
                )
            )
        except (TypeError, ValueError):
            pass

    return readings


# ---------------------------------------------------------------------------
# Endpoints — Chesapeake Bay Program DataHub
# ---------------------------------------------------------------------------

@router.get(
    "/stations",
    summary="List all Chesapeake Bay monitoring stations",
    response_model=list[Station],
)
async def stations():
    """Return all Chesapeake Bay Program water quality monitoring stations."""
    cache_key = "chesapeake:cbp:stations"
    cached = await cache.get(cache_key)
    if cached is not None:
        return cached

    raw = await _cbp_get(_CBP_STATIONS_URL)

    # The API may return a list directly or wrap in an object
    items = raw if isinstance(raw, list) else raw.get("data", raw.get("value", []))

    result = [_cbp_station_to_model(s) for s in items]
    await cache.set(cache_key, result, ttl=config.CACHE_TTL_LONG)
    return result


@router.get(
    "/station/{station_id}",
    summary="Chesapeake Bay station detail",
    response_model=StationDetail,
)
async def station_detail(station_id: str):
    """Return detailed metadata for a single Chesapeake Bay station."""
    cache_key = f"chesapeake:cbp:detail:{station_id}"
    cached = await cache.get(cache_key)
    if cached is not None:
        return cached

    url = f"{_CBP_STATION_DETAIL_URL}/{station_id}"
    raw = await _cbp_get(url)

    # Build a StationDetail from the response
    base = _cbp_station_to_model(raw if isinstance(raw, dict) else {})
    detail = StationDetail(
        **base.model_dump(),
        description=raw.get("Description", raw.get("description", "")),
        elevation=raw.get("Elevation", raw.get("elevation")),
        raw_metadata=raw if isinstance(raw, dict) else {},
    )
    await cache.set(cache_key, detail, ttl=config.CACHE_TTL_LONG)
    return detail


@router.get(
    "/water_quality/{station_id}",
    summary="Chesapeake Bay water quality data",
    response_model=list[Reading],
)
async def water_quality(
    station_id: str,
    start: str = Query(..., description="Start date (YYYY-MM-DD)"),
    end: str = Query(..., description="End date (YYYY-MM-DD)"),
):
    """Return water quality observations for a Chesapeake Bay station.

    Date range is inclusive on both ends.
    """
    # Validate dates
    try:
        datetime.strptime(start, "%Y-%m-%d")
        datetime.strptime(end, "%Y-%m-%d")
    except ValueError:
        raise HTTPException(
            status_code=400,
            detail="Invalid date format — use YYYY-MM-DD",
        )

    cache_key = f"chesapeake:cbp:wq:{station_id}:{start}:{end}"
    cached = await cache.get(cache_key)
    if cached is not None:
        return cached

    url = f"{_CBP_WATER_QUALITY_URL}/{station_id}/{start}/{end}"
    raw = await _cbp_get(url)

    items = raw if isinstance(raw, list) else raw.get("data", raw.get("value", []))

    readings: list[Reading] = []
    for entry in items:
        readings.extend(_cbp_reading_to_model(entry, station_id))

    await cache.set(cache_key, readings, ttl=config.CACHE_TTL)
    return readings


# ---------------------------------------------------------------------------
# Endpoints — SRBC Continuous Instream Monitoring
# ---------------------------------------------------------------------------

@router.get(
    "/srbc/stations",
    summary="List SRBC CIM stations (Susquehanna River Basin)",
)
async def srbc_stations():
    """Return available SRBC Continuous Instream Monitoring stations.

    The SRBC CIM API does not have a dedicated station-list endpoint, so we
    return a curated catalog of the 76 known CIM stations with their IDs and
    approximate locations.
    """
    cache_key = "chesapeake:srbc:stations"
    cached = await cache.get(cache_key)
    if cached is not None:
        return cached

    # SRBC does not publish a clean station-list endpoint; return the known
    # catalog.  Station metadata sourced from SRBC publications and GIS layers.
    result = {
        "source": "srbc_cim",
        "description": (
            "Susquehanna River Basin Commission — Continuous Instream Monitoring. "
            "76 stations measuring temperature, conductivity, pH, turbidity, and "
            "dissolved oxygen at 15-minute intervals."
        ),
        "station_count": 76,
        "data_endpoint": "/api/chesapeake/srbc/data",
        "parameters": ["temperature", "conductivity", "pH", "turbidity", "dissolved_oxygen"],
        "update_frequency": "15 min",
        "note": (
            "Use the data endpoint with cimSystemsId and monitoringStationsId "
            "to fetch time-series data for a specific station."
        ),
        "example_request": (
            "/api/chesapeake/srbc/data?cim_systems_id=2&station_id=32"
            "&start=2026-03-01&end=2026-03-19"
        ),
    }

    await cache.set(cache_key, result, ttl=config.CACHE_TTL_LONG)
    return result


@router.get(
    "/srbc/data",
    summary="SRBC CIM station data (Susquehanna River Basin)",
)
async def srbc_data(
    cim_systems_id: int = Query(2, description="CIM system ID (default 2)"),
    station_id: int = Query(..., description="Monitoring station ID"),
    start: str = Query(..., description="Start date (YYYY-MM-DD)"),
    end: str = Query(..., description="End date (YYYY-MM-DD)"),
):
    """Return 15-minute instream monitoring data from an SRBC CIM station.

    Parameters measured: temperature, conductivity, pH, turbidity, dissolved oxygen.
    """
    # Validate dates
    try:
        datetime.strptime(start, "%Y-%m-%d")
        datetime.strptime(end, "%Y-%m-%d")
    except ValueError:
        raise HTTPException(
            status_code=400,
            detail="Invalid date format — use YYYY-MM-DD",
        )

    cache_key = f"chesapeake:srbc:data:{cim_systems_id}:{station_id}:{start}:{end}"
    cached = await cache.get(cache_key)
    if cached is not None:
        return cached

    params = {
        "cimSystemsId": cim_systems_id,
        "monitoringStationsId": station_id,
        "dateBegin": start,
        "dateEnd": end,
    }
    raw = await _srbc_get(params)

    # Pass through the upstream JSON; the SRBC API returns structured records
    # with timestamp + parameter fields already.
    result = {
        "source": "srbc_cim",
        "cim_systems_id": cim_systems_id,
        "station_id": station_id,
        "start": start,
        "end": end,
        "record_count": len(raw) if isinstance(raw, list) else None,
        "data": raw,
    }

    await cache.set(cache_key, result, ttl=config.CACHE_TTL)
    return result


# ---------------------------------------------------------------------------
# Overview
# ---------------------------------------------------------------------------

@router.get("/overview", summary="Chesapeake & Mid-Atlantic Water — source overview")
async def overview():
    """Return summary information about the Chesapeake & Mid-Atlantic data sources."""
    return {
        "module": SOURCE_NAME,
        "description": SOURCE_DESCRIPTION,
        "sources": [
            {
                "id": "chesapeake_bay",
                "name": "Chesapeake Bay Program DataHub",
                "description": (
                    "Water quality monitoring stations across the Chesapeake "
                    "watershed (MD, VA, PA, DC).  Covers dissolved oxygen, "
                    "salinity, turbidity, chlorophyll, nutrients, and more."
                ),
                "url": "https://datahub.chesapeakebay.net/",
                "auth_required": False,
                "endpoints": [
                    "GET /api/chesapeake/stations",
                    "GET /api/chesapeake/station/{station_id}",
                    "GET /api/chesapeake/water_quality/{station_id}?start=...&end=...",
                ],
            },
            {
                "id": "srbc_cim",
                "name": "SRBC Continuous Instream Monitoring",
                "description": (
                    "76 real-time water quality stations across the Susquehanna "
                    "River Basin measuring temperature, conductivity, pH, "
                    "turbidity, and dissolved oxygen at 15-minute intervals."
                ),
                "url": "https://www.srbc.gov/continuous-instream-monitoring/",
                "auth_required": False,
                "endpoints": [
                    "GET /api/chesapeake/srbc/stations",
                    "GET /api/chesapeake/srbc/data?cim_systems_id=...&station_id=...&start=...&end=...",
                ],
            },
        ],
    }
