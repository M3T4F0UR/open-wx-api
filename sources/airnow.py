"""EPA AirNow — US Air Quality Observations and Forecasts.

~2,500 monitoring stations across the United States reporting real-time AQI,
PM2.5, PM10, O3, NO2, SO2, and CO.  Data includes EPA category labels
(Good / Moderate / Unhealthy for Sensitive Groups / Unhealthy / Very Unhealthy
/ Hazardous).

Requires a free API key from https://docs.airnowapi.org/account/request/
Rate limit: 500 requests/hr per endpoint.
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Optional

import httpx
from fastapi import APIRouter, HTTPException, Query, Path

import cache
import config
from models import Reading, Station, StationDetail

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Source registry metadata
# ---------------------------------------------------------------------------
SOURCE_NAME = "EPA AirNow"
SOURCE_DESCRIPTION = (
    "US EPA AirNow air quality monitoring network.  ~2,500 stations reporting "
    "AQI, PM2.5, PM10, O3, NO2, SO2, and CO with EPA category labels.  "
    "Includes current observations and forecasts by zip code or lat/lon."
)
SOURCE_META = {
    "category": "air_quality",
    "coverage": "United States (~2,500 stations)",
    "auth_required": True,
    "auth_note": (
        "Free API key at https://docs.airnowapi.org/account/request/ — "
        "set AIRNOW_API_KEY env var"
    ),
    "update_frequency": "~1 hr",
    "rate_limit": "500 req/hr per endpoint",
    "params": ["aqi", "pm2.5", "pm10", "o3", "no2", "so2", "co"],
    "urls": {
        "homepage": "https://www.airnow.gov/",
        "api_docs": "https://docs.airnowapi.org/",
        "signup": "https://docs.airnowapi.org/account/request/",
    },
}

# ---------------------------------------------------------------------------
# Router
# ---------------------------------------------------------------------------

router = APIRouter(prefix="/api/airnow", tags=["EPA AirNow"])

# ---------------------------------------------------------------------------
# Constants & config
# ---------------------------------------------------------------------------

AIRNOW_API_KEY = config.AIRNOW_API_KEY
_BASE = "https://www.airnowapi.org/aq"

# AirNow parameter name → (normalised param name, unit)
_PARAM_MAP: dict[str, tuple[str, str]] = {
    "PM2.5":  ("pm2.5",  "µg/m³"),
    "PM10":   ("pm10",   "µg/m³"),
    "O3":     ("o3",     "ppb"),
    "NO2":    ("no2",    "ppb"),
    "SO2":    ("so2",    "ppb"),
    "CO":     ("co",     "ppm"),
}

# AQI category index → label
_AQI_CATEGORIES: dict[int, str] = {
    1: "Good",
    2: "Moderate",
    3: "Unhealthy for Sensitive Groups",
    4: "Unhealthy",
    5: "Very Unhealthy",
    6: "Hazardous",
    7: "Unavailable",
}


# ---------------------------------------------------------------------------
# API key guard
# ---------------------------------------------------------------------------

def _require_key() -> str:
    """Return the configured AirNow API key or raise a descriptive 503."""
    key = AIRNOW_API_KEY
    if not key:
        raise HTTPException(
            status_code=503,
            detail=(
                "AIRNOW_API_KEY is not configured.  "
                "Obtain a free key at https://docs.airnowapi.org/account/request/ "
                "and set it as the AIRNOW_API_KEY environment variable."
            ),
        )
    return key


# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------

def _make_client() -> httpx.AsyncClient:
    return httpx.AsyncClient(
        timeout=config.HTTP_TIMEOUT,
        headers={"User-Agent": config.USER_AGENT},
        follow_redirects=True,
    )


async def _airnow_get(path: str, params: dict[str, Any]) -> list[dict]:
    """GET an AirNow endpoint; inject API key; return parsed JSON list."""
    key = _require_key()
    params = {
        **params,
        "format": "application/json",
        "API_KEY": key,
    }

    url = f"{_BASE}/{path.lstrip('/')}"
    try:
        async with _make_client() as client:
            resp = await client.get(url, params=params)
            resp.raise_for_status()
            return resp.json()
    except HTTPException:
        raise
    except httpx.TimeoutException:
        raise HTTPException(status_code=504, detail=f"AirNow API timeout ({url})")
    except httpx.HTTPStatusError as exc:
        status = exc.response.status_code
        detail = exc.response.text[:300] if exc.response.text else str(exc)
        if status == 401 or status == 403:
            raise HTTPException(
                status_code=401,
                detail=f"AirNow API authentication failed — check AIRNOW_API_KEY. {detail}",
            )
        raise HTTPException(status_code=status, detail=f"AirNow API error: {detail}")
    except Exception as exc:
        logger.exception("Unexpected error calling AirNow API: %s", url)
        raise HTTPException(status_code=502, detail=str(exc))


# ---------------------------------------------------------------------------
# Data normalisation
# ---------------------------------------------------------------------------

def _parse_observation(obs: dict) -> dict:
    """Normalise a single AirNow observation record."""
    param_name = obs.get("ParameterName", "")
    category_num = obs.get("Category", {}).get("Number", 7)

    return {
        "parameter": param_name,
        "aqi": obs.get("AQI"),
        "category": _AQI_CATEGORIES.get(category_num, "Unknown"),
        "category_number": category_num,
        "date_observed": obs.get("DateObserved", "").strip(),
        "hour_observed": obs.get("HourObserved"),
        "local_timezone": obs.get("LocalTimeZone", ""),
        "reporting_area": obs.get("ReportingArea", ""),
        "state_code": obs.get("StateCode", ""),
        "latitude": obs.get("Latitude"),
        "longitude": obs.get("Longitude"),
    }


def _obs_to_readings(observations: list[dict], station_id: str) -> list[Reading]:
    """Convert AirNow observation list into Reading models."""
    readings: list[Reading] = []
    for obs in observations:
        param_name = obs.get("ParameterName", "")
        aqi = obs.get("AQI")
        if aqi is None:
            continue

        # Build a timestamp from DateObserved + HourObserved
        date_str = obs.get("DateObserved", "").strip()
        hour = obs.get("HourObserved", 0)
        try:
            ts = datetime.strptime(date_str, "%Y-%m-%d").replace(
                hour=int(hour), tzinfo=timezone.utc,
            )
        except (ValueError, TypeError):
            ts = datetime.now(timezone.utc)

        norm_name, unit = _PARAM_MAP.get(param_name, (param_name.lower(), "AQI"))

        readings.append(Reading(
            station_id=station_id,
            timestamp=ts,
            parameter=norm_name,
            value=float(aqi),
            unit="AQI",
            source="airnow",
        ))
    return readings


def _parse_forecast(fc: dict) -> dict:
    """Normalise a single AirNow forecast record."""
    category_num = fc.get("Category", {}).get("Number", 7)
    return {
        "date": fc.get("DateForecast", "").strip(),
        "parameter": fc.get("ParameterName", ""),
        "aqi": fc.get("AQI"),
        "category": _AQI_CATEGORIES.get(category_num, "Unknown"),
        "category_number": category_num,
        "reporting_area": fc.get("ReportingArea", ""),
        "state_code": fc.get("StateCode", ""),
        "latitude": fc.get("Latitude"),
        "longitude": fc.get("Longitude"),
        "discussion": fc.get("Discussion", ""),
    }


# ---------------------------------------------------------------------------
# Endpoint: GET /api/airnow/current
# ---------------------------------------------------------------------------

@router.get(
    "/current",
    summary="Current air quality near a lat/lon point",
)
async def current_by_location(
    lat: float = Query(..., description="Latitude", ge=-90, le=90),
    lon: float = Query(..., description="Longitude", ge=-180, le=180),
    distance: int = Query(50, description="Search radius in miles", ge=1, le=500),
):
    """Return current AQI observations for monitoring stations near a point."""
    cache_key = f"airnow:current:{lat:.2f},{lon:.2f}:{distance}"
    cached = await cache.get(cache_key)
    if cached is not None:
        return cached

    data = await _airnow_get("observation/latLong/current/", {
        "latitude": lat,
        "longitude": lon,
        "distance": distance,
    })

    observations = [_parse_observation(obs) for obs in data]
    result = {
        "source": SOURCE_NAME,
        "query": {"lat": lat, "lon": lon, "distance_miles": distance},
        "count": len(observations),
        "observations": observations,
    }
    await cache.set(cache_key, result, ttl=config.CACHE_TTL)
    return result


# ---------------------------------------------------------------------------
# Endpoint: GET /api/airnow/current/zip/{zipcode}
# ---------------------------------------------------------------------------

@router.get(
    "/current/zip/{zipcode}",
    summary="Current air quality by zip code",
)
async def current_by_zip(
    zipcode: str = Path(..., description="US zip code", pattern=r"^\d{5}$"),
):
    """Return current AQI observations for a US zip code."""
    cache_key = f"airnow:current:zip:{zipcode}"
    cached = await cache.get(cache_key)
    if cached is not None:
        return cached

    data = await _airnow_get("observation/zipCode/current/", {
        "zipCode": zipcode,
    })

    observations = [_parse_observation(obs) for obs in data]
    result = {
        "source": SOURCE_NAME,
        "query": {"zipcode": zipcode},
        "count": len(observations),
        "observations": observations,
    }
    await cache.set(cache_key, result, ttl=config.CACHE_TTL)
    return result


# ---------------------------------------------------------------------------
# Endpoint: GET /api/airnow/forecast/zip/{zipcode}
# ---------------------------------------------------------------------------

@router.get(
    "/forecast/zip/{zipcode}",
    summary="Air quality forecast by zip code",
)
async def forecast_by_zip(
    zipcode: str = Path(..., description="US zip code", pattern=r"^\d{5}$"),
):
    """Return AQI forecast for a US zip code."""
    cache_key = f"airnow:forecast:zip:{zipcode}"
    cached = await cache.get(cache_key)
    if cached is not None:
        return cached

    data = await _airnow_get("forecast/zipCode/", {
        "zipCode": zipcode,
    })

    forecasts = [_parse_forecast(fc) for fc in data]
    result = {
        "source": SOURCE_NAME,
        "query": {"zipcode": zipcode},
        "count": len(forecasts),
        "forecasts": forecasts,
    }
    await cache.set(cache_key, result, ttl=config.CACHE_TTL_LONG)
    return result


# ---------------------------------------------------------------------------
# Endpoint: GET /api/airnow/info
# ---------------------------------------------------------------------------

@router.get(
    "/info",
    summary="Source information and API key status",
)
async def airnow_info():
    """Return information about the AirNow source and whether an API key is configured."""
    key_configured = bool(AIRNOW_API_KEY)
    return {
        "source": SOURCE_NAME,
        "description": SOURCE_DESCRIPTION,
        "key_configured": key_configured,
        "key_instructions": (
            None if key_configured else
            "Visit https://docs.airnowapi.org/account/request/ to register for a free API key.  "
            "Set the AIRNOW_API_KEY environment variable and restart the server."
        ),
        "api_base": _BASE,
        "docs": "https://docs.airnowapi.org/",
        **SOURCE_META,
    }
