"""Mexico CONAGUA SMN — national weather forecasts from Mexico's met service.

Covers:
  1. Daily municipal forecasts  — ~3,000+ municipalities
  2. Hourly forecasts           — station-level hourly data
  3. Overview                   — source metadata

The SMN (Servicio Meteorológico Nacional) is part of CONAGUA (Comisión
Nacional del Agua).  All endpoints are public and require no authentication.
Responses are gzip-compressed JSON.

~189 Automatic Weather Stations (EMAs) + ~79 Synoptic stations (ESMAs).
"""

import gzip
import json
import logging
from datetime import datetime, timezone
from typing import Any, Optional

import httpx
from fastapi import APIRouter, HTTPException, Query

import cache
import config
from models import Station

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Source registry metadata
# ---------------------------------------------------------------------------
SOURCE_NAME = "Mexico CONAGUA SMN"
SOURCE_DESCRIPTION = (
    "National weather forecasts from Mexico's Servicio Meteorológico Nacional "
    "(CONAGUA SMN).  Provides daily municipal forecasts (~3,000+ municipalities) "
    "and hourly station-level forecasts.  ~189 Automatic Weather Stations (EMAs) "
    "and ~79 Synoptic stations (ESMAs).  No authentication required."
)
SOURCE_META = {
    "url": "https://smn.conagua.gob.mx/",
    "auth_required": False,
    "update_frequency": "1–6 hr",
    "coverage": "Mexico (national)",
    "params": [
        "temperature", "temperature_max", "temperature_min",
        "precipitation_probability", "wind_direction", "wind_speed",
        "sky_condition", "humidity",
    ],
    "category": "weather",
}

router = APIRouter(prefix="/api/mexico", tags=["Mexico"])

# ---------------------------------------------------------------------------
# API URLs
# ---------------------------------------------------------------------------
_SMN_BASE = "https://smn.conagua.gob.mx/tools/GUI/webservices/"
_DAILY_URL = f"{_SMN_BASE}?method=1"
_HOURLY_URL = f"{_SMN_BASE}?method=3"

# ---------------------------------------------------------------------------
# Shared HTTP helpers
# ---------------------------------------------------------------------------
_HEADERS = {
    "User-Agent": config.USER_AGENT,
    "Accept": "application/json",
    "Accept-Encoding": "gzip, deflate",
}


def _client() -> httpx.AsyncClient:
    return httpx.AsyncClient(
        headers=_HEADERS,
        timeout=config.HTTP_TIMEOUT,
        follow_redirects=True,
    )


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


async def _fetch_smn(url: str) -> list[dict]:
    """Fetch a gzip-compressed JSON response from SMN and return parsed list.

    The SMN webservice returns raw gzip bytes with Content-Type often set to
    application/json but the body itself is gzip-compressed.  httpx may or
    may not auto-decompress depending on response headers, so we handle
    both cases.
    """
    async with _client() as client:
        resp = await client.get(url)
        resp.raise_for_status()

        body = resp.content
        # Try gzip decompress; if it fails, assume already decompressed
        try:
            body = gzip.decompress(body)
        except (gzip.BadGzipFile, OSError):
            pass

        try:
            data = json.loads(body)
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            logger.error("SMN JSON parse error for %s: %s", url, exc)
            raise HTTPException(502, f"Invalid JSON from CONAGUA SMN: {exc}")

        if not isinstance(data, list):
            # Some methods wrap in an object — try to extract list
            if isinstance(data, dict):
                for v in data.values():
                    if isinstance(v, list):
                        return v
            return [data] if data else []
        return data


# ---------------------------------------------------------------------------
# Field mapping helpers
# ---------------------------------------------------------------------------
_SKY_CODES: dict[int, str] = {
    1: "Clear",
    2: "Partly Cloudy",
    3: "Mostly Cloudy",
    4: "Overcast",
}

_WIND_DIR_MAP: dict[str, str] = {
    "Norte": "N", "Sur": "S", "Este": "E", "Oeste": "W",
    "Noreste": "NE", "Noroeste": "NW", "Sureste": "SE", "Suroeste": "SW",
}


def _safe_float(val: Any) -> Optional[float]:
    """Convert a value to float, returning None on failure."""
    if val is None or val == "":
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


def _daily_to_station(item: dict) -> Station:
    """Convert a daily forecast record to a Station object."""
    # Field names from SMN method=1:
    #   ides  — state id
    #   idmun — municipality id
    #   nes   — state name
    #   nmun  — municipality name
    #   lat, lon
    #   tmax, tmin — max/min temperature (°C)
    #   prec  — precipitation probability (%)
    #   viento_direc — wind direction (Spanish)
    #   viento_veloc — wind speed (km/h)
    #   desciel — sky description
    station_id = f"mx_{item.get('ides', '0')}_{item.get('idmun', '0')}"
    state = item.get("nes", "")
    muni = item.get("nmun", "")
    name = f"{muni}, {state}" if muni and state else (muni or state or station_id)

    lat = _safe_float(item.get("lat")) or 0.0
    lon = _safe_float(item.get("lon")) or 0.0

    params = []
    if item.get("tmax") is not None or item.get("tmin") is not None:
        params.append("temperature")
    if item.get("prec") is not None:
        params.append("precipitation_probability")
    if item.get("viento_veloc") is not None:
        params.append("wind_speed")
    if item.get("desciel") is not None:
        params.append("sky_condition")

    wind_dir_raw = item.get("viento_direc", "")
    wind_dir = _WIND_DIR_MAP.get(wind_dir_raw, wind_dir_raw)

    return Station(
        id=station_id,
        name=name,
        lat=lat,
        lon=lon,
        source="mexico_conagua",
        network=f"SMN {state}",
        params=params or ["forecast"],
        extra={
            "state_id": item.get("ides"),
            "municipality_id": item.get("idmun"),
            "state": state,
            "municipality": muni,
            "tmax_c": _safe_float(item.get("tmax")),
            "tmin_c": _safe_float(item.get("tmin")),
            "precip_prob_pct": _safe_float(item.get("prec")),
            "wind_direction": wind_dir,
            "wind_speed_kmh": _safe_float(item.get("viento_veloc")),
            "sky_description": item.get("desciel", ""),
        },
    )


def _hourly_to_station(item: dict) -> Station:
    """Convert an hourly forecast record to a Station object."""
    # Field names from SMN method=3 (vary — common fields):
    #   ides, idmun, nes, nmun, lat, lon
    #   hloc — local hour
    #   temp — temperature (°C)
    #   prec — precipitation probability (%)
    #   viento_direc, viento_veloc
    #   desciel — sky description
    #   hr — relative humidity (%)
    station_id = f"mx_{item.get('ides', '0')}_{item.get('idmun', '0')}"
    state = item.get("nes", "")
    muni = item.get("nmun", "")
    name = f"{muni}, {state}" if muni and state else (muni or state or station_id)

    lat = _safe_float(item.get("lat")) or 0.0
    lon = _safe_float(item.get("lon")) or 0.0

    params = []
    if item.get("temp") is not None:
        params.append("temperature")
    if item.get("prec") is not None:
        params.append("precipitation_probability")
    if item.get("viento_veloc") is not None:
        params.append("wind_speed")
    if item.get("hr") is not None:
        params.append("humidity")
    if item.get("desciel") is not None:
        params.append("sky_condition")

    wind_dir_raw = item.get("viento_direc", "")
    wind_dir = _WIND_DIR_MAP.get(wind_dir_raw, wind_dir_raw)

    return Station(
        id=station_id,
        name=name,
        lat=lat,
        lon=lon,
        source="mexico_conagua",
        network=f"SMN {state}",
        params=params or ["forecast"],
        extra={
            "state_id": item.get("ides"),
            "municipality_id": item.get("idmun"),
            "state": state,
            "municipality": muni,
            "local_hour": item.get("hloc"),
            "temp_c": _safe_float(item.get("temp")),
            "precip_prob_pct": _safe_float(item.get("prec")),
            "humidity_pct": _safe_float(item.get("hr")),
            "wind_direction": wind_dir,
            "wind_speed_kmh": _safe_float(item.get("viento_veloc")),
            "sky_description": item.get("desciel", ""),
        },
    )


# ===========================================================================
# 1. DAILY MUNICIPAL FORECASTS
# ===========================================================================

@router.get("/forecast/daily", summary="Municipal daily forecasts (CONAGUA SMN)")
async def forecast_daily(
    state: Optional[str] = Query(None, description="Filter by state name (Spanish, e.g. 'Jalisco')"),
    limit: int = Query(500, ge=1, le=5000, description="Max records to return"),
):
    """Fetch daily municipal weather forecasts from CONAGUA SMN (method=1).

    Returns temperature max/min, precipitation probability, wind, and sky
    condition for ~3,000+ municipalities across Mexico.
    """
    cache_key = "mexico:daily"
    cached = await cache.get(cache_key)

    if cached is not None:
        raw_data = cached
    else:
        raw_data = await _fetch_smn(_DAILY_URL)
        await cache.set(cache_key, raw_data, ttl=config.CACHE_TTL_LONG)

    # Filter by state if requested
    if state:
        state_lower = state.lower()
        raw_data = [r for r in raw_data if (r.get("nes") or "").lower() == state_lower]

    stations = [_daily_to_station(r) for r in raw_data[:limit]]

    return {
        "source": "mexico_conagua",
        "forecast_type": "daily",
        "updated": _now_utc().isoformat(),
        "record_count": len(stations),
        "total_available": len(raw_data),
        "stations": [s.model_dump() for s in stations],
    }


# ===========================================================================
# 2. HOURLY FORECASTS
# ===========================================================================

@router.get("/forecast/hourly", summary="Hourly forecasts (CONAGUA SMN)")
async def forecast_hourly(
    state: Optional[str] = Query(None, description="Filter by state name (Spanish, e.g. 'Jalisco')"),
    limit: int = Query(500, ge=1, le=5000, description="Max records to return"),
):
    """Fetch hourly weather forecasts from CONAGUA SMN (method=3).

    Returns temperature, humidity, precipitation probability, wind, and sky
    condition at hourly resolution.
    """
    cache_key = "mexico:hourly"
    cached = await cache.get(cache_key)

    if cached is not None:
        raw_data = cached
    else:
        raw_data = await _fetch_smn(_HOURLY_URL)
        await cache.set(cache_key, raw_data, ttl=config.CACHE_TTL)

    # Filter by state if requested
    if state:
        state_lower = state.lower()
        raw_data = [r for r in raw_data if (r.get("nes") or "").lower() == state_lower]

    stations = [_hourly_to_station(r) for r in raw_data[:limit]]

    return {
        "source": "mexico_conagua",
        "forecast_type": "hourly",
        "updated": _now_utc().isoformat(),
        "record_count": len(stations),
        "total_available": len(raw_data),
        "stations": [s.model_dump() for s in stations],
    }


# ===========================================================================
# 3. OVERVIEW
# ===========================================================================

@router.get("/overview", summary="Mexico CONAGUA SMN — source overview")
async def overview():
    """Return metadata about the Mexico CONAGUA SMN data source."""
    return {
        "module": SOURCE_NAME,
        "description": SOURCE_DESCRIPTION,
        "sources": [
            {
                "id": "mexico_conagua",
                "name": "Mexico CONAGUA SMN",
                "description": (
                    "National weather forecasts from Mexico's Servicio Meteorológico "
                    "Nacional.  Daily and hourly municipal forecasts covering ~3,000+ "
                    "municipalities.  ~189 Automatic Weather Stations (EMAs) and ~79 "
                    "Synoptic stations (ESMAs)."
                ),
                "url": "https://smn.conagua.gob.mx/",
                "auth_required": False,
                "update_frequency": "1–6 hr",
                "coverage": "Mexico (national)",
                "station_count": "~3,000+ municipalities",
                "params": SOURCE_META["params"],
                "endpoints": [
                    "GET /api/mexico/forecast/daily",
                    "GET /api/mexico/forecast/hourly",
                    "GET /api/mexico/overview",
                ],
            }
        ],
    }
