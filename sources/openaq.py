"""OpenAQ — global open air quality data from 20,700+ locations in 134 countries.

OpenAQ aggregates real-time and historical air quality measurements from
government monitoring agencies, research institutions, and low-cost sensor
networks worldwide.  Parameters include PM2.5, PM10, NO2, SO2, O3, CO, and BC.

API v3 reference: https://docs.openaq.org/
Free API key: https://openaq.org/ (register for an API key)
Rate limit: 300 requests per 5-minute window.
"""
from __future__ import annotations

import logging
import os
from datetime import datetime
from typing import Any, Optional

from fastapi import APIRouter, HTTPException, Path, Query

import cache
import config
import http_client
from models import Reading, Station, StationDetail

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Source registry metadata  (read by server.py /api/sources)
# ---------------------------------------------------------------------------

SOURCE_NAME = "OpenAQ"
SOURCE_DESCRIPTION = (
    "OpenAQ aggregates real-time and historical air quality data from 20,700+ "
    "monitoring locations across 134 countries.  Sources include government "
    "reference-grade monitors, research stations, and low-cost sensor networks.  "
    "Parameters: PM2.5, PM10, NO2, SO2, O3, CO, BC.  Free API key required — "
    "register at openaq.org."
)
SOURCE_META = {
    "url": "https://openaq.org/",
    "auth_required": True,
    "auth_note": "Free API key at https://openaq.org/ — set OPENAQ_API_KEY env var",
    "update_frequency": "Varies by station (typically 1 hour)",
    "coverage": "Global — 134 countries, 20,700+ locations",
    "params": ["pm25", "pm10", "no2", "so2", "o3", "co", "bc"],
    "category": "air_quality",
    "station_count": 20700,
}

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

OPENAQ_BASE = "https://api.openaq.org/v3"
OPENAQ_API_KEY = os.getenv("OPENAQ_API_KEY", "")

# OpenAQ parameter name -> (human name, unit)
PARAMETER_MAP: dict[str, tuple[str, str]] = {
    "pm25":  ("PM2.5",             "ug/m3"),
    "pm10":  ("PM10",              "ug/m3"),
    "no2":   ("Nitrogen Dioxide",  "ug/m3"),
    "so2":   ("Sulfur Dioxide",    "ug/m3"),
    "o3":    ("Ozone",             "ug/m3"),
    "co":    ("Carbon Monoxide",   "ug/m3"),
    "bc":    ("Black Carbon",      "ug/m3"),
    "nox":   ("Nitrogen Oxides",   "ug/m3"),
    "no":    ("Nitric Oxide",      "ug/m3"),
    "ch4":   ("Methane",           "ug/m3"),
    "um010": ("PM1",               "ug/m3"),
}


# ---------------------------------------------------------------------------
# API key guard
# ---------------------------------------------------------------------------

def _require_key() -> str:
    """Return the configured OpenAQ API key or raise a descriptive 503."""
    key = OPENAQ_API_KEY
    if not key:
        raise HTTPException(
            status_code=503,
            detail=(
                "OPENAQ_API_KEY is not configured.  "
                "Register for a free API key at https://openaq.org/ "
                "and set it as the OPENAQ_API_KEY environment variable."
            ),
        )
    return key


# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------

async def _openaq_get(path: str, params: Optional[dict[str, Any]] = None) -> dict:
    """GET an OpenAQ v3 endpoint; inject API key header; raise on error."""
    key = _require_key()
    url = f"{OPENAQ_BASE}/{path.lstrip('/')}"
    headers = {"X-API-Key": key}

    try:
        data = await http_client.fetch_json(
            url,
            params=params or {},
            headers=headers,
            source="openaq",
        )
        return data
    except http_client.CircuitOpenError:
        raise HTTPException(
            status_code=503,
            detail="OpenAQ circuit breaker is open — too many recent failures. Try again shortly.",
        )
    except Exception as exc:
        _handle_openaq_error(exc, url)
        # _handle_openaq_error always raises, but make the type checker happy
        raise  # pragma: no cover


def _handle_openaq_error(exc: Exception, url: str) -> None:
    """Convert OpenAQ errors to meaningful HTTPExceptions."""
    import httpx

    if isinstance(exc, httpx.TimeoutException):
        raise HTTPException(status_code=504, detail=f"OpenAQ API timeout ({url})")
    if isinstance(exc, httpx.HTTPStatusError):
        status = exc.response.status_code
        try:
            body = exc.response.json()
            msg = body.get("detail", body.get("message", str(exc)))
        except Exception:
            msg = str(exc)
        if status == 401 or status == 403:
            raise HTTPException(
                status_code=401,
                detail=f"OpenAQ API authentication failed — check OPENAQ_API_KEY. Detail: {msg}",
            )
        if status == 429:
            raise HTTPException(
                status_code=429,
                detail="OpenAQ rate limit exceeded (300 requests per 5 minutes). Try again shortly.",
            )
        raise HTTPException(status_code=status, detail=f"OpenAQ API error: {msg}")
    logger.exception("Unexpected error calling OpenAQ API: %s", url)
    raise HTTPException(status_code=502, detail=str(exc))


# ---------------------------------------------------------------------------
# Data-normalisation helpers
# ---------------------------------------------------------------------------

def _parse_location_to_station(loc: dict) -> Station:
    """Convert an OpenAQ v3 location object to the shared Station model."""
    location_id = loc.get("id", "")
    name = loc.get("name", f"OpenAQ-{location_id}")

    coords = loc.get("coordinates", {}) or {}
    lat = coords.get("latitude", 0.0)
    lon = coords.get("longitude", 0.0)

    # Extract parameter names from the sensors/parameters list
    params_list = []
    sensors = loc.get("sensors", []) or []
    for sensor in sensors:
        param = sensor.get("parameter", {}) or {}
        param_name = param.get("name", "")
        if param_name:
            human, _ = PARAMETER_MAP.get(param_name, (param_name.upper(), ""))
            if human not in params_list:
                params_list.append(human)

    # Also check top-level parameters field (v3 may include it)
    for p in loc.get("parameters", []) or []:
        pname = p.get("name", "") if isinstance(p, dict) else str(p)
        if pname:
            human, _ = PARAMETER_MAP.get(pname, (pname.upper(), ""))
            if human not in params_list:
                params_list.append(human)

    # Last updated
    last_update: Optional[datetime] = None
    dt_str = loc.get("datetimeLast", {})
    if isinstance(dt_str, dict):
        dt_str = dt_str.get("utc", "")
    if dt_str:
        try:
            last_update = datetime.fromisoformat(str(dt_str).replace("Z", "+00:00"))
        except (ValueError, TypeError):
            pass

    country = loc.get("country", {}) or {}
    country_code = country.get("code", "") if isinstance(country, dict) else str(country)
    provider = loc.get("provider", {}) or {}
    provider_name = provider.get("name", "") if isinstance(provider, dict) else str(provider)
    owner = loc.get("owner", {}) or {}
    owner_name = owner.get("name", "") if isinstance(owner, dict) else str(owner)

    return Station(
        id=str(location_id),
        name=name,
        lat=float(lat) if lat else 0.0,
        lon=float(lon) if lon else 0.0,
        source="openaq",
        network=provider_name,
        params=params_list,
        elevation=None,
        url=f"https://explore.openaq.org/locations/{location_id}",
        last_update=last_update,
        extra={
            "country": country_code,
            "is_mobile": loc.get("isMobile", False),
            "is_monitor": loc.get("isMonitor", False),
            "provider": provider_name,
            "owner": owner_name,
            "sensor_count": len(sensors),
        },
    )


def _parse_latest_to_readings(location_id: str, measurements: list[dict]) -> list[Reading]:
    """Convert OpenAQ v3 latest measurements to Reading objects."""
    readings: list[Reading] = []
    for m in measurements:
        param = m.get("parameter", {}) or {}
        param_name = param.get("name", "") if isinstance(param, dict) else str(param)
        value = m.get("value", None)
        if value is None:
            continue

        try:
            numeric = float(value)
        except (TypeError, ValueError):
            continue

        human, default_unit = PARAMETER_MAP.get(param_name, (param_name.upper(), ""))
        unit_info = param.get("units", default_unit) if isinstance(param, dict) else default_unit

        # Timestamp
        dt_str = m.get("datetime", {})
        if isinstance(dt_str, dict):
            dt_str = dt_str.get("utc", "")
        ts: Optional[datetime] = None
        if dt_str:
            try:
                ts = datetime.fromisoformat(str(dt_str).replace("Z", "+00:00"))
            except (ValueError, TypeError):
                pass
        if ts is None:
            continue

        readings.append(
            Reading(
                station_id=str(location_id),
                timestamp=ts,
                parameter=human,
                value=numeric,
                unit=str(unit_info),
                quality=None,
                source="openaq",
            )
        )
    return readings


# ---------------------------------------------------------------------------
# Router
# ---------------------------------------------------------------------------

router = APIRouter(prefix="/api/openaq", tags=["OpenAQ Air Quality"])


# ---------------------------------------------------------------------------
# Endpoint: GET /api/openaq/locations
# ---------------------------------------------------------------------------

@router.get(
    "/locations",
    summary="Search air quality monitoring locations",
)
async def get_locations(
    lat: Optional[float] = Query(None, description="Latitude for proximity search"),
    lon: Optional[float] = Query(None, description="Longitude for proximity search"),
    radius_km: float = Query(25.0, ge=0.1, le=500.0, description="Search radius in km (used with lat/lon)"),
    country: Optional[str] = Query(None, description="ISO 3166-1 alpha-2 country code (e.g. 'US', 'GB')"),
    limit: int = Query(100, ge=1, le=1000, description="Maximum locations to return"),
):
    """Search for OpenAQ air quality monitoring locations.

    Provide either lat/lon for a proximity search, or a country code to list
    stations in a country.  If neither is provided, returns a global sample.
    """
    cache_key = f"openaq:locations:{lat}:{lon}:{radius_km}:{country}:{limit}"
    cached_result = await cache.get(cache_key)
    if cached_result is not None:
        return cached_result

    params: dict[str, Any] = {"limit": limit}

    if lat is not None and lon is not None:
        params["coordinates"] = f"{lat},{lon}"
        params["radius"] = int(radius_km * 1000)  # API expects meters
    elif country:
        params["countries_id"] = country.upper()

    data = await _openaq_get("locations", params)
    results = data.get("results", []) or []
    stations = [_parse_location_to_station(loc) for loc in results]

    result = {
        "query": {
            "lat": lat,
            "lon": lon,
            "radius_km": radius_km if lat is not None else None,
            "country": country,
        },
        "count": len(stations),
        "stations": [s.model_dump() for s in stations],
    }
    await cache.set(cache_key, result, ttl=config.CACHE_TTL)
    return result


# ---------------------------------------------------------------------------
# Endpoint: GET /api/openaq/locations/{location_id}/latest
# ---------------------------------------------------------------------------

@router.get(
    "/locations/{location_id}/latest",
    summary="Latest measurements for a location",
)
async def get_location_latest(
    location_id: int = Path(..., description="OpenAQ location ID"),
):
    """Return the latest air quality measurements for a specific OpenAQ location.

    Returns the most recent reading for each parameter measured at this station.
    """
    cache_key = f"openaq:latest:{location_id}"
    cached_result = await cache.get(cache_key)
    if cached_result is not None:
        return cached_result

    data = await _openaq_get(f"locations/{location_id}/latest", {})
    results = data.get("results", []) or []

    readings = _parse_latest_to_readings(location_id, results)
    readings.sort(key=lambda r: r.parameter)

    # Also fetch station metadata
    station: Optional[dict] = None
    try:
        loc_data = await _openaq_get(f"locations/{location_id}", {})
        loc_results = loc_data.get("results", []) or []
        if loc_results:
            station = _parse_location_to_station(loc_results[0]).model_dump()
    except Exception:
        logger.debug("Could not fetch metadata for OpenAQ location %s", location_id)

    result = {
        "location_id": location_id,
        "station": station,
        "count": len(readings),
        "readings": [r.model_dump() for r in readings],
    }
    await cache.set(cache_key, result, ttl=config.CACHE_TTL)
    return result


# ---------------------------------------------------------------------------
# Endpoint: GET /api/openaq/countries
# ---------------------------------------------------------------------------

@router.get(
    "/countries",
    summary="List countries with air quality data",
)
async def get_countries(
    limit: int = Query(200, ge=1, le=500, description="Maximum countries to return"),
):
    """Return the list of countries with OpenAQ air quality monitoring data.

    Results are cached for 1 hour since the country list rarely changes.
    """
    cache_key = f"openaq:countries:{limit}"
    cached_result = await cache.get(cache_key)
    if cached_result is not None:
        return cached_result

    data = await _openaq_get("countries", {"limit": limit})
    results = data.get("results", []) or []

    countries = [
        {
            "id": c.get("id", ""),
            "code": c.get("code", ""),
            "name": c.get("name", ""),
            "locations_count": c.get("locationsCount", c.get("locations", 0)),
            "parameters": [
                p.get("name", "") if isinstance(p, dict) else str(p)
                for p in (c.get("parameters", []) or [])
            ],
        }
        for c in results
    ]

    result = {
        "count": len(countries),
        "countries": countries,
    }
    await cache.set(cache_key, result, ttl=config.CACHE_TTL_LONG)
    return result


# ---------------------------------------------------------------------------
# Endpoint: GET /api/openaq/info
# ---------------------------------------------------------------------------

@router.get(
    "/info",
    summary="Source information and API key status",
)
async def openaq_info():
    """Return information about the OpenAQ source and whether an API key is configured."""
    key_configured = bool(OPENAQ_API_KEY)
    return {
        "source": SOURCE_NAME,
        "description": SOURCE_DESCRIPTION,
        "key_configured": key_configured,
        "key_instructions": (
            None if key_configured else
            "Register for a free API key at https://openaq.org/ .  "
            "Set the OPENAQ_API_KEY environment variable and restart the server."
        ),
        "api_base": OPENAQ_BASE,
        "rate_limit": "300 requests per 5-minute window",
        "docs": "https://docs.openaq.org/",
        **SOURCE_META,
    }
