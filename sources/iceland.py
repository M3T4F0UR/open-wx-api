"""Iceland Met Office (IMO) — current weather observations and forecasts via apis.is.

Provides real-time observations from ~90 automatic weather stations across
Iceland (temperature, wind, gusts, humidity, pressure, dewpoint, cloud cover,
snow depth, road temperature) and station-level text forecasts.

API reference: https://docs.apis.is/#endpoint-weather
No authentication required.
"""
from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from typing import Optional

import httpx
from fastapi import APIRouter, HTTPException

import cache
import config
import http_client
from models import Reading, Station, StationDetail

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Source registry metadata
# ---------------------------------------------------------------------------
SOURCE_NAME = "Iceland Met Office"
SOURCE_DESCRIPTION = (
    "Iceland Met Office (IMO) via apis.is — real-time weather observations "
    "from ~90 automatic stations across Iceland (temperature, wind, gusts, "
    "humidity, pressure, dewpoint, cloud cover, snow depth, road temperature) "
    "and station-level text forecasts."
)
SOURCE_META = {
    "url": "https://apis.is/weather/",
    "auth_required": False,
    "update_frequency": "~60 min",
    "coverage": "Iceland",
    "params": [
        "temperature", "wind_direction", "wind_speed", "wind_gust",
        "humidity", "pressure", "dewpoint", "cloud_cover",
        "snow_depth", "road_temperature",
    ],
    "category": "weather",
}

router = APIRouter(prefix="/api/iceland", tags=["Iceland Met Office"])

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
APIS_IS_OBS = "https://apis.is/weather/observations/en"
APIS_IS_FORECAST = "https://apis.is/weather/forecasts/en"

# apis.is accepts a comma-separated list of station IDs.  Station IDs run
# roughly from 1 to ~1000.  To avoid a single enormous request we batch
# them into chunks.
_STATION_RANGE = range(1, 1000)
_BATCH_SIZE = 50  # stations per HTTP request

# Map of apis.is field names to (parameter, unit) tuples.
_FIELD_MAP: dict[str, tuple[str, str]] = {
    "T":   ("temperature",      "°C"),
    "D":   ("wind_direction",   "°"),
    "F":   ("wind_speed",       "m/s"),
    "FG":  ("wind_gust",        "m/s"),
    "N":   ("cloud_cover",      ""),
    "TD":  ("dewpoint",         "°C"),
    "P":   ("pressure",         "hPa"),
    "RH":  ("humidity",         "%"),
    "SNC": ("snow_depth",       "cm"),
    "RTE": ("road_temperature", "°C"),
}

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _safe_float(val) -> Optional[float]:
    """Convert a value to float, returning None on failure."""
    if val is None or val == "":
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


def _parse_timestamp(time_str: str) -> Optional[datetime]:
    """Parse an IMO time string like '2026-03-20 14:00:00' into a UTC datetime."""
    if not time_str:
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"):
        try:
            return datetime.strptime(time_str, fmt).replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    return None


def _parse_observation(item: dict) -> Optional[StationDetail]:
    """Convert a single apis.is observation result into a StationDetail."""
    station_id = str(item.get("id", "")).strip()
    name = item.get("name", "").strip()
    if not station_id or not name:
        return None

    ts = _parse_timestamp(item.get("time", ""))
    if ts is None:
        return None

    # Build readings from known fields
    readings: list[Reading] = []
    available_params: list[str] = []
    for field, (param, unit) in _FIELD_MAP.items():
        val = _safe_float(item.get(field))
        if val is not None:
            available_params.append(param)
            readings.append(Reading(
                station_id=station_id,
                timestamp=ts,
                parameter=param,
                value=val,
                unit=unit,
                source="iceland_imo",
            ))

    if not readings:
        return None

    station = Station(
        id=station_id,
        name=name,
        lat=0.0,   # apis.is observations endpoint does not return coordinates
        lon=0.0,
        source="iceland_imo",
        network="IMO",
        params=available_params,
        url=f"https://en.vedur.is/weather/stations/",
        last_update=ts,
    )

    return StationDetail(station=station, readings=readings)


async def _fetch_batch(station_ids: list[int]) -> list[dict]:
    """Fetch observations for a batch of station IDs.

    Returns the 'results' list from the apis.is response, or [] on error.
    """
    ids_param = ",".join(str(i) for i in station_ids)
    try:
        data = await http_client.fetch_json(
            APIS_IS_OBS,
            params={"stations": ids_param, "time": "1h"},
            source="iceland_imo",
        )
    except http_client.CircuitOpenError:
        return []
    except (httpx.TimeoutException, httpx.HTTPStatusError):
        return []
    except Exception:
        logger.debug("Iceland batch fetch failed for %d stations", len(station_ids))
        return []

    if isinstance(data, dict):
        return data.get("results", [])
    return []


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@router.get("/current", summary="Current weather from all Iceland stations")
async def current_weather():
    """Return current observations from all ~90 Iceland Met Office automatic
    weather stations.  Stations are queried in batches and merged.
    """
    cache_key = "iceland:current"
    cached = await cache.get(cache_key)
    if cached is not None:
        return cached

    # Build batches of station IDs
    batches: list[list[int]] = []
    current_batch: list[int] = []
    for sid in _STATION_RANGE:
        current_batch.append(sid)
        if len(current_batch) >= _BATCH_SIZE:
            batches.append(current_batch)
            current_batch = []
    if current_batch:
        batches.append(current_batch)

    # Fetch all batches concurrently
    tasks = [_fetch_batch(batch) for batch in batches]
    batch_results = await asyncio.gather(*tasks)

    # Flatten and deduplicate by station ID
    seen: set[str] = set()
    station_details: list[dict] = []
    for results in batch_results:
        for item in results:
            detail = _parse_observation(item)
            if detail is None:
                continue
            if detail.station.id in seen:
                continue
            seen.add(detail.station.id)
            station_details.append(detail.model_dump())

    result = {
        "source": "iceland_imo",
        "station_count": len(station_details),
        "stations": station_details,
    }
    await cache.set(cache_key, result, ttl=config.CACHE_TTL)
    return result


@router.get("/stations/{station_id}", summary="Single Iceland station observations")
async def get_station(station_id: int):
    """Return current observations for a single Iceland Met Office station by
    numeric ID (1-999).
    """
    cache_key = f"iceland:station:{station_id}"
    cached = await cache.get(cache_key)
    if cached is not None:
        return cached

    try:
        data = await http_client.fetch_json(
            APIS_IS_OBS,
            params={"stations": str(station_id), "time": "1h"},
            source="iceland_imo",
        )
    except http_client.CircuitOpenError as exc:
        raise HTTPException(status_code=503, detail=str(exc))
    except httpx.TimeoutException:
        raise HTTPException(status_code=504, detail="Iceland IMO API timeout")
    except httpx.HTTPStatusError as exc:
        raise HTTPException(
            status_code=exc.response.status_code,
            detail=f"Iceland IMO API error: {exc}",
        )
    except Exception as exc:
        logger.exception("Error fetching Iceland station %s", station_id)
        raise HTTPException(status_code=500, detail=str(exc))

    results = data.get("results", []) if isinstance(data, dict) else []
    if not results:
        raise HTTPException(404, f"Station {station_id} not found or has no data")

    detail = _parse_observation(results[0])
    if detail is None:
        raise HTTPException(404, f"Station {station_id} returned no valid readings")

    result = detail.model_dump()
    await cache.set(cache_key, result, ttl=config.CACHE_TTL)
    return result


@router.get("/forecast/{station_id}", summary="Forecast for an Iceland station")
async def get_forecast(station_id: int):
    """Return the text weather forecast for a single Iceland Met Office station.

    The forecast endpoint returns structured forecast periods with expected
    conditions, temperature range, and wind information.
    """
    cache_key = f"iceland:forecast:{station_id}"
    cached = await cache.get(cache_key)
    if cached is not None:
        return cached

    try:
        data = await http_client.fetch_json(
            APIS_IS_FORECAST,
            params={"stations": str(station_id)},
            source="iceland_imo",
        )
    except http_client.CircuitOpenError as exc:
        raise HTTPException(status_code=503, detail=str(exc))
    except httpx.TimeoutException:
        raise HTTPException(status_code=504, detail="Iceland IMO forecast API timeout")
    except httpx.HTTPStatusError as exc:
        raise HTTPException(
            status_code=exc.response.status_code,
            detail=f"Iceland IMO forecast API error: {exc}",
        )
    except Exception as exc:
        logger.exception("Error fetching Iceland forecast for station %s", station_id)
        raise HTTPException(status_code=500, detail=str(exc))

    results = data.get("results", []) if isinstance(data, dict) else []
    if not results:
        raise HTTPException(404, f"No forecast available for station {station_id}")

    # Pass through the forecast periods as-is with light normalization
    forecasts = []
    for item in results:
        forecasts.append({
            "time": item.get("ftime", item.get("time", "")),
            "wind_direction": item.get("D"),
            "wind_speed": _safe_float(item.get("F")),
            "wind_gust": _safe_float(item.get("FG")),
            "temperature": _safe_float(item.get("T")),
            "weather_description": item.get("W", ""),
        })

    result = {
        "source": "iceland_imo",
        "station_id": station_id,
        "forecast_count": len(forecasts),
        "forecasts": forecasts,
    }
    await cache.set(cache_key, result, ttl=config.CACHE_TTL)
    return result
