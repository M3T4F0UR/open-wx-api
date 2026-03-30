"""Montana Mesonet API v2 — direct integration with the University of Montana network.

Provides access to ~70 stations across Montana with temperature, humidity, wind,
solar radiation, precipitation, and soil temperature/moisture observations via the
official v2 REST API with Swagger documentation.

API docs:  https://mesonet.climate.umt.edu/api/v2/docs
Base URL:  https://mesonet.climate.umt.edu/api/v2/

This module is separate from the Montana Mesonet integration in regional.py
(which serves ``/api/regional/montana/...``) to provide a cleaner, dedicated
interface using the documented v2 API endpoints.

No authentication required.
"""
from __future__ import annotations

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

SOURCE_NAME = "Montana Mesonet API v2"
SOURCE_DESCRIPTION = (
    "University of Montana Mesonet — ~70 stations statewide with temperature, "
    "humidity, wind, solar radiation, precipitation, and soil temp/moisture. "
    "Official v2 REST API with Swagger docs; no authentication required."
)
SOURCE_META = {
    "url": "https://mesonet.climate.umt.edu/api/v2/docs",
    "auth_required": False,
    "update_frequency": "15 min",
    "coverage": "Montana, USA",
    "params": [
        "temperature", "relative_humidity", "wind_speed", "wind_direction",
        "wind_gust", "precipitation", "solar_radiation", "soil_temperature",
        "soil_moisture", "pressure", "snow_depth",
    ],
    "category": "weather",
}

# ---------------------------------------------------------------------------
# Base URL & HTTP helpers
# ---------------------------------------------------------------------------

_BASE = "https://mesonet.climate.umt.edu/api/v2"

_HTTP_HEADERS = {
    "User-Agent": config.USER_AGENT,
    "Accept": "application/json",
}


def _client() -> httpx.AsyncClient:
    """Return an async HTTP client with project-standard settings."""
    return httpx.AsyncClient(
        headers=_HTTP_HEADERS,
        timeout=config.HTTP_TIMEOUT,
        follow_redirects=True,
    )


async def _get_json(url: str, params: Optional[dict] = None, *, label: str = "MT Mesonet v2") -> Any:
    """GET request returning parsed JSON; raises HTTPException on failure."""
    try:
        async with _client() as client:
            kw = {"params": params} if params else {}
            resp = await client.get(url, **kw)
            resp.raise_for_status()
            return resp.json()
    except httpx.TimeoutException:
        raise HTTPException(status_code=504, detail=f"{label} API timeout ({url})")
    except httpx.HTTPStatusError as exc:
        raise HTTPException(
            status_code=exc.response.status_code,
            detail=f"{label} API error: {exc}",
        )
    except Exception as exc:
        logger.exception("Unexpected error calling %s: %s", label, url)
        raise HTTPException(status_code=502, detail=f"{label}: {exc}")


# ---------------------------------------------------------------------------
# Variable mapping: API field name → (canonical parameter, unit)
# ---------------------------------------------------------------------------

_VAR_MAP: dict[str, tuple[str, str]] = {
    "air_temp":        ("temperature",       "°C"),
    "humidity":        ("relative_humidity",  "%"),
    "wind_speed":      ("wind_speed",         "m/s"),
    "wind_direction":  ("wind_direction",     "deg"),
    "gust":            ("wind_gust",          "m/s"),
    "precipitation":   ("precipitation",      "mm"),
    "solar_radiation": ("solar_radiation",    "W/m²"),
    "soil_temp":       ("soil_temperature",   "°C"),
    "soil_vwc":        ("soil_moisture",      "m³/m³"),
    "pressure":        ("pressure",           "hPa"),
    "snow_depth":      ("snow_depth",         "mm"),
}

# ---------------------------------------------------------------------------
# Parsing helpers
# ---------------------------------------------------------------------------


def _safe_float(val: Any) -> Optional[float]:
    """Convert a value to float, returning None on failure."""
    if val is None:
        return None
    try:
        return float(val)
    except (TypeError, ValueError):
        return None


def _parse_iso(raw: Optional[str]) -> Optional[datetime]:
    """Parse an ISO-8601 timestamp string, returning None on failure."""
    if not raw:
        return None
    try:
        dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except (ValueError, TypeError):
        return None


def _to_station(raw: dict) -> Station:
    """Convert a Montana Mesonet v2 station JSON object to the shared Station model."""
    sid = str(raw.get("station", raw.get("id", "")))
    lat = _safe_float(raw.get("latitude") or raw.get("lat"))
    lon = _safe_float(raw.get("longitude") or raw.get("lng") or raw.get("lon"))

    # Build param list from whichever keys exist in the response
    params: list[str] = []
    for api_key, (canonical, _) in _VAR_MAP.items():
        if api_key in raw and canonical not in params:
            params.append(canonical)

    # Fallback: if no variable keys present in metadata, list all known params
    if not params:
        params = sorted({canonical for canonical, _ in _VAR_MAP.values()})

    last_update = _parse_iso(raw.get("datetime") or raw.get("last_observation"))

    return Station(
        id=sid,
        name=raw.get("name", sid),
        lat=lat,
        lon=lon,
        source="montana_mesonet_v2",
        network="Montana Mesonet (UMT v2)",
        params=params,
        elevation=_safe_float(raw.get("elevation")),
        url=f"https://mesonet.climate.umt.edu/stations/{sid}/",
        last_update=last_update,
        extra={
            "county": raw.get("county", ""),
            "sub_network": raw.get("sub_network", ""),
            "active": raw.get("active", True),
        },
    )


def _obs_to_readings(obs: dict, station_id: str) -> list[Reading]:
    """Extract Reading objects from a Montana Mesonet v2 observation record."""
    ts = _parse_iso(obs.get("datetime") or obs.get("timestamp")) or datetime.now(timezone.utc)

    readings: list[Reading] = []
    for api_key, (param_name, unit) in _VAR_MAP.items():
        raw_val = obs.get(api_key)
        if raw_val is None:
            continue
        try:
            readings.append(
                Reading(
                    station_id=station_id,
                    timestamp=ts,
                    parameter=param_name,
                    value=float(raw_val),
                    unit=unit,
                    quality=obs.get("qc_flag") or obs.get("quality"),
                    source="montana_mesonet_v2",
                )
            )
        except (TypeError, ValueError):
            continue
    return readings


def _extract_list(data: Any) -> list[dict]:
    """Normalize API response to a list of dicts."""
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        return data.get("data", data.get("stations", data.get("observations", [])))
    return []


# ---------------------------------------------------------------------------
# Router
# ---------------------------------------------------------------------------

router = APIRouter(prefix="/api/mt_mesonet", tags=["Montana Mesonet v2"])


@router.get("/stations", summary="List all Montana Mesonet stations")
async def stations():
    """Return all active Montana Mesonet stations (~70 sites).

    Metadata includes station name, county, sub-network, location, and elevation.
    Stations span weather, soil moisture, and agro-meteorological monitoring
    across Montana.
    """
    cache_key = "mt_mesonet_v2:stations"
    cached = await cache.get(cache_key)
    if cached is not None:
        return cached

    data = await _get_json(f"{_BASE}/stations/")
    raw_list = _extract_list(data)

    station_list: list[dict] = []
    for raw in raw_list:
        try:
            station_list.append(_to_station(raw).model_dump())
        except Exception:
            logger.debug("Failed to parse MT Mesonet v2 station: %s", raw, exc_info=True)

    result = {
        "source": "montana_mesonet_v2",
        "network": "Montana Mesonet (University of Montana)",
        "api_docs": "https://mesonet.climate.umt.edu/api/v2/docs",
        "count": len(station_list),
        "stations": station_list,
    }
    await cache.set(cache_key, result, ttl=config.CACHE_TTL_LONG)
    return result


@router.get("/latest", summary="Latest observations for a station")
async def latest(
    station: str = Query(..., description="Station ID (e.g. 'aceabsar')"),
):
    """Return the most recent observation for a Montana Mesonet station.

    Observations are normalized to the shared Reading model with canonical
    parameter names and SI units.
    """
    cache_key = f"mt_mesonet_v2:latest:{station}"
    cached = await cache.get(cache_key)
    if cached is not None:
        return cached

    data = await _get_json(
        f"{_BASE}/observations/",
        params={"station": station, "latest": "true"},
    )
    raw_list = _extract_list(data)

    if not raw_list:
        raise HTTPException(
            status_code=404,
            detail=f"No observations found for station '{station}'",
        )

    readings: list[Reading] = []
    for obs in raw_list:
        readings.extend(_obs_to_readings(obs, station))

    # Keep only the most recent reading per parameter
    latest_map: dict[str, Reading] = {}
    for r in readings:
        existing = latest_map.get(r.parameter)
        if existing is None or r.timestamp > existing.timestamp:
            latest_map[r.parameter] = r
    readings = sorted(latest_map.values(), key=lambda r: r.parameter)

    result = {
        "source": "montana_mesonet_v2",
        "station": station,
        "count": len(readings),
        "readings": [r.model_dump() for r in readings],
    }
    await cache.set(cache_key, result, ttl=config.CACHE_TTL)
    return result


@router.get("/observations", summary="Time series observations for a station")
async def observations(
    station: str = Query(..., description="Station ID (e.g. 'aceabsar')"),
    start: str = Query(..., description="Start time (ISO-8601 date or datetime, e.g. '2026-03-18')"),
    end: str = Query(..., description="End time (ISO-8601 date or datetime, e.g. '2026-03-19')"),
):
    """Return a time series of observations for a Montana Mesonet station
    between `start` and `end` times.

    Suitable for building charts and analyzing trends.  Observations are
    normalized to the shared Reading model.
    """
    cache_key = f"mt_mesonet_v2:obs:{station}:{start}:{end}"
    cached = await cache.get(cache_key)
    if cached is not None:
        return cached

    data = await _get_json(
        f"{_BASE}/observations/",
        params={"station": station, "start_time": start, "end_time": end},
    )
    raw_list = _extract_list(data)

    readings: list[Reading] = []
    for obs in raw_list:
        readings.extend(_obs_to_readings(obs, station))
    readings.sort(key=lambda r: (r.timestamp, r.parameter))

    result = {
        "source": "montana_mesonet_v2",
        "station": station,
        "start": start,
        "end": end,
        "observation_count": len(raw_list),
        "reading_count": len(readings),
        "readings": [r.model_dump() for r in readings],
    }
    await cache.set(cache_key, result, ttl=config.CACHE_TTL)
    return result


@router.get("/overview", summary="Montana Mesonet v2 source info")
async def overview():
    """Return metadata about this data source, including API docs URL,
    coverage, parameters, and authentication requirements.
    """
    return {
        "source": "montana_mesonet_v2",
        "name": SOURCE_NAME,
        "description": SOURCE_DESCRIPTION,
        "api_base": _BASE,
        "api_docs": "https://mesonet.climate.umt.edu/api/v2/docs",
        "auth_required": False,
        "coverage": "Montana, USA (~70 stations)",
        "update_frequency": "15 min",
        "parameters": SOURCE_META["params"],
        "endpoints": {
            "stations": "/api/mt_mesonet/stations",
            "latest": "/api/mt_mesonet/latest?station={station_id}",
            "observations": "/api/mt_mesonet/observations?station={station_id}&start={start}&end={end}",
            "overview": "/api/mt_mesonet/overview",
        },
        "notes": (
            "This module uses the official v2 API with Swagger docs. "
            "The existing /api/regional/montana/ endpoints in regional.py "
            "also hit the same upstream API but with different parameter handling."
        ),
    }
