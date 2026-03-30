"""SDG&E Weather Awareness System — wxsection.com source module.

San Diego Gas & Electric operates ~217 weather stations across their service
territory in San Diego and southern Orange counties.  The public API provides
current observations (wind, temperature, humidity, dewpoint, heat index, wind
chill) along with climatological percentiles, district/zone/circuit metadata,
and per-station forecasts (GFS, NWS, AI).

No authentication required.

API base: https://weather.sdgeweather.com
Map API:  https://maps.sdgeweather.com
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Optional

from fastapi import APIRouter, HTTPException, Path

import cache
import config
import http_client
from models import Reading, Station

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Source registry metadata  (read by server.py /api/sources)
# ---------------------------------------------------------------------------

SOURCE_NAME = "SDG&E Weather Awareness"
SOURCE_DESCRIPTION = (
    "San Diego Gas & Electric Weather Awareness System.  ~217 weather stations "
    "across the SDG&E service territory (San Diego and southern Orange counties) "
    "reporting wind speed, wind gust, wind direction, temperature, relative "
    "humidity, dewpoint, heat index, wind chill, and climatological percentiles.  "
    "No authentication required."
)
SOURCE_META = {
    "url": "https://weather.sdgeweather.com",
    "auth_required": False,
    "update_frequency": "near-real-time",
    "coverage": "San Diego & southern Orange County, CA (~217 stations)",
    "params": [
        "wind_speed", "wind_gust", "wind_direction",
        "temperature", "relative_humidity", "dewpoint",
        "heat_index", "wind_chill",
    ],
    "category": "weather",
}

# ---------------------------------------------------------------------------
# Router
# ---------------------------------------------------------------------------

router = APIRouter(prefix="/api/sdge_weather", tags=["SDG&E Weather Awareness"])

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_OBS_BASE = "https://weather.sdgeweather.com/api"
_MAP_BASE = "https://maps.sdgeweather.com/api"

_SOURCE = "sdge_weather"

# SDG&E field name -> (normalised parameter, unit)
_FIELD_MAP: dict[str, tuple[str, str]] = {
    "wind_speed":     ("wind_speed",        "mph"),
    "wind_gust":      ("wind_gust",         "mph"),
    "wind_direction": ("wind_direction",     "°"),
    "temperature":    ("temperature",        "°F"),
    "rh":             ("relative_humidity",  "%"),
    "dewpoint":       ("dewpoint",           "°F"),
    "heat_index":     ("heat_index",         "°F"),
    "wind_chill":     ("wind_chill",         "°F"),
}


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

async def _fetch(url: str, params: Optional[dict] = None) -> Any:
    """Fetch JSON from SDG&E Weather API via the shared HTTP client."""
    return await http_client.fetch_json(url, params=params, source=_SOURCE)


def _parse_obs_time(raw: Any) -> Optional[datetime]:
    """Try to parse an observation timestamp from the API response."""
    if raw is None:
        return None
    if isinstance(raw, (int, float)):
        try:
            return datetime.fromtimestamp(raw, tz=timezone.utc)
        except (OSError, ValueError):
            return None
    if isinstance(raw, str):
        for fmt in ("%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%d %H:%M:%S"):
            try:
                dt = datetime.strptime(raw, fmt)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                return dt
            except ValueError:
                continue
    return None


def _station_from_obs(obs: dict) -> Station:
    """Convert one SDG&E observation object into a Station model."""
    station_code = str(obs.get("station") or obs.get("station_code") or obs.get("id") or "")
    name = str(obs.get("name") or obs.get("station_name") or station_code)
    lat = float(obs.get("lat") or obs.get("latitude") or 0)
    lon = float(obs.get("lon") or obs.get("longitude") or 0)

    # Collect available parameter names
    params: list[str] = []
    for api_field, (norm_name, _unit) in _FIELD_MAP.items():
        if obs.get(api_field) is not None:
            params.append(norm_name)

    extra: dict[str, Any] = {}
    for key in ("district", "zone", "circuit", "elevation"):
        if obs.get(key) is not None:
            extra[key] = obs[key]

    # Percentile data
    for key in list(obs.keys()):
        if "percentile" in key.lower() or "pctl" in key.lower():
            extra[key] = obs[key]

    last_update = _parse_obs_time(obs.get("time") or obs.get("timestamp") or obs.get("obs_time"))

    return Station(
        id=station_code,
        name=name,
        lat=lat,
        lon=lon,
        source=_SOURCE,
        network="sdge",
        params=params,
        elevation=obs.get("elevation"),
        last_update=last_update,
        quality_tier=2,
        extra=extra,
    )


def _readings_from_obs(obs: dict, station_id: str) -> list[dict]:
    """Extract Reading dicts from an observation object."""
    ts = _parse_obs_time(obs.get("time") or obs.get("timestamp") or obs.get("obs_time"))
    if ts is None:
        ts = datetime.now(timezone.utc)

    readings: list[dict] = []
    for api_field, (norm_name, unit) in _FIELD_MAP.items():
        val = obs.get(api_field)
        if val is not None:
            try:
                readings.append(
                    Reading(
                        station_id=station_id,
                        timestamp=ts,
                        parameter=norm_name,
                        value=float(val),
                        unit=unit,
                        source=_SOURCE,
                    ).model_dump(mode="json")
                )
            except (ValueError, TypeError):
                continue
    return readings


# ---------------------------------------------------------------------------
# 1. GET /api/sdge_weather/current — all stations, current observations
# ---------------------------------------------------------------------------

@router.get("/current")
async def sdge_current():
    """Return current observations from all ~217 SDG&E weather stations."""
    cache_key = "sdge_weather:current"
    hit = await cache.get(cache_key)
    if hit is not None:
        return hit

    try:
        data = await _fetch(f"{_OBS_BASE}/observations.json", params={"time": "current"})
    except Exception as exc:
        logger.error("SDG&E current obs fetch failed: %s", exc)
        raise HTTPException(status_code=502, detail=f"SDG&E API error: {exc}")

    if not isinstance(data, list):
        raise HTTPException(status_code=502, detail="Unexpected response format from SDG&E API")

    stations: list[dict] = []
    all_readings: list[dict] = []

    for obs in data:
        try:
            st = _station_from_obs(obs)
            readings = _readings_from_obs(obs, st.id)
            stations.append({
                "station": st.model_dump(mode="json", exclude_none=True),
                "readings": readings,
            })
            all_readings.extend(readings)
        except Exception:
            logger.debug("Skipping malformed SDG&E station entry", exc_info=True)
            continue

    result = {
        "source": SOURCE_NAME,
        "station_count": len(stations),
        "reading_count": len(all_readings),
        "stations": stations,
    }
    await cache.set(cache_key, result, ttl=config.CACHE_TTL)
    return result


# ---------------------------------------------------------------------------
# 2. GET /api/sdge_weather/station/{station_code} — single station detail
# ---------------------------------------------------------------------------

@router.get("/station/{station_code}")
async def sdge_station(station_code: str = Path(..., description="SDG&E station code")):
    """Return detailed data for a single SDG&E station (obs + forecasts)."""
    cache_key = f"sdge_weather:station:{station_code}"
    hit = await cache.get(cache_key)
    if hit is not None:
        return hit

    try:
        data = await _fetch(
            f"{_OBS_BASE}/station.json",
            params={"station": station_code},
        )
    except Exception as exc:
        logger.error("SDG&E station %s fetch failed: %s", station_code, exc)
        raise HTTPException(status_code=502, detail=f"SDG&E API error: {exc}")

    if not data:
        raise HTTPException(status_code=404, detail=f"Station '{station_code}' not found")

    # Build station + current readings from the response
    obs = data if isinstance(data, dict) else {}
    station = _station_from_obs(obs)
    readings = _readings_from_obs(obs, station.id)

    # Extract forecast sections if present
    forecasts: dict[str, Any] = {}
    for key in ("gfs_forecast", "nws_forecast", "ai_forecast", "forecast"):
        if key in obs and obs[key] is not None:
            forecasts[key] = obs[key]

    # Extract observation history if present
    history: list[dict] = []
    raw_history = obs.get("history") or obs.get("observations") or []
    if isinstance(raw_history, list):
        for h in raw_history:
            ts = _parse_obs_time(h.get("time") or h.get("timestamp"))
            entry: dict[str, Any] = {}
            if ts:
                entry["timestamp"] = ts.isoformat()
            for api_field, (norm_name, unit) in _FIELD_MAP.items():
                val = h.get(api_field)
                if val is not None:
                    entry[norm_name] = {"value": val, "unit": unit}
            if entry:
                history.append(entry)

    result: dict[str, Any] = {
        "source": SOURCE_NAME,
        "station": station.model_dump(mode="json", exclude_none=True),
        "readings": readings,
    }
    if forecasts:
        result["forecasts"] = forecasts
    if history:
        result["history"] = history

    await cache.set(cache_key, result, ttl=config.CACHE_TTL)
    return result


# ---------------------------------------------------------------------------
# 3. GET /api/sdge_weather/overview — source info
# ---------------------------------------------------------------------------

@router.get("/overview")
async def sdge_overview():
    """Return source metadata and current station list."""
    cache_key = "sdge_weather:overview"
    hit = await cache.get(cache_key)
    if hit is not None:
        return hit

    station_list: list[dict] = []
    try:
        data = await _fetch(f"{_OBS_BASE}/observations.json", params={"time": "current"})
        if isinstance(data, list):
            for obs in data:
                try:
                    st = _station_from_obs(obs)
                    station_list.append(st.model_dump(mode="json", exclude_none=True))
                except Exception:
                    continue
    except Exception:
        logger.warning("SDG&E overview: failed to fetch station list", exc_info=True)

    result = {
        "source": SOURCE_NAME,
        "description": SOURCE_DESCRIPTION,
        "meta": SOURCE_META,
        "station_count": len(station_list),
        "stations": station_list,
    }
    await cache.set(cache_key, result, ttl=config.CACHE_TTL_LONG)
    return result
