"""Northeast Coastal Buoys — Chesapeake Bay & Narragansett Bay monitoring.

Covers two coastal buoy/fixed-site networks:

  1. CBIBS (Chesapeake Bay Interpretive Buoy System) — 10 buoys with air temp,
     RH, pressure, wind, waves, currents, water temp, salinity, DO, chlorophyll,
     turbidity.  6-min update frequency.
     API: https://mw.buoybay.noaa.gov/api/v1/json/station

  2. Narragansett Bay Fixed-Site Monitoring Network — 15 buoy/dock stations
     with continuous pH, DO, temperature, salinity, chlorophyll at 15-min
     intervals.  Some stations stream to NERACOOS ERDDAP.
     No authentication required.

CBIBS may require a free API key for production use, but the station list
endpoint works without one.
"""
import logging
from datetime import datetime, timezone
from typing import Any, Optional

import httpx
from fastapi import APIRouter, HTTPException, Query

import cache
import config
from models import Reading, Station

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Source registry metadata  (read by server.py /api/sources)
# ---------------------------------------------------------------------------

SOURCE_NAME = "Northeast Coastal Buoys"
SOURCE_DESCRIPTION = (
    "Chesapeake Bay Interpretive Buoy System (CBIBS) and Narragansett Bay "
    "Fixed-Site Monitoring Network.  Real-time water quality, meteorological, "
    "and oceanographic observations from ~25 coastal stations."
)
SOURCE_META = {
    "url": "https://buoybay.noaa.gov/",
    "auth_required": False,
    "update_frequency": "6 min (CBIBS), 15 min (Narragansett Bay)",
    "coverage": "Chesapeake Bay (MD/VA), Narragansett Bay (RI)",
    "params": [
        "air_temperature", "relative_humidity", "air_pressure",
        "wind_speed", "wind_direction", "wave_height",
        "water_temperature", "salinity", "dissolved_oxygen",
        "chlorophyll", "turbidity", "pH", "currents",
    ],
    "category": "water",
}

router = APIRouter(prefix="/api/ne_buoys", tags=["Northeast Coastal Buoys"])

# ---------------------------------------------------------------------------
# API base URLs
# ---------------------------------------------------------------------------
CBIBS_BASE = "https://mw.buoybay.noaa.gov/api/v1/json"
CBIBS_STATION_URL = f"{CBIBS_BASE}/station"

# Narragansett Bay Fixed-Site Monitoring — data exposed via NERACOOS ERDDAP.
# The ERDDAP tabledap endpoint provides the most reliable machine-readable
# access; the native NBFSMN site lacks a stable JSON API.
NBFSMN_ERDDAP_BASE = "https://data.neracoos.org/erddap"
# Dataset IDs for Narragansett Bay stations (may be updated as ERDDAP evolves)
NBFSMN_SEARCH_URL = f"{NBFSMN_ERDDAP_BASE}/search/index.json"

# ---------------------------------------------------------------------------
# HTTP client helpers
# ---------------------------------------------------------------------------
_HEADERS = {
    "User-Agent": config.USER_AGENT,
    "Accept": "application/json",
}

CBIBS_API_KEY = getattr(config, "CBIBS_API_KEY", "") or ""


async def _fetch_json(url: str, params: dict | None = None, source: str = "ne_buoys") -> Any:
    """Fetch JSON with shared HTTP client settings."""
    try:
        import http_client
        return await http_client.fetch_json(url, params=params, source=source)
    except Exception:
        # Fallback to direct httpx if http_client unavailable
        async with httpx.AsyncClient(timeout=config.HTTP_TIMEOUT, headers=_HEADERS) as client:
            resp = await client.get(url, params=params or {})
            resp.raise_for_status()
            return resp.json()


# ---------------------------------------------------------------------------
# CBIBS parameter mapping
# ---------------------------------------------------------------------------

# CBIBS returns these measurement types in the station payload
_CBIBS_PARAM_MAP: dict[str, tuple[str, str]] = {
    "air_temperature":     ("air_temperature",     "°C"),
    "humidity":            ("relative_humidity",    "%"),
    "air_pressure":        ("air_pressure",         "mb"),
    "wind_speed":          ("wind_speed",           "m/s"),
    "wind_direction":      ("wind_direction",       "°"),
    "wind_gust":           ("wind_gust",            "m/s"),
    "wave_height":         ("wave_height",          "m"),
    "wave_period":         ("wave_period",          "s"),
    "water_temperature":   ("water_temperature",    "°C"),
    "salinity":            ("salinity",             "PSU"),
    "dissolved_oxygen":    ("dissolved_oxygen",     "mg/L"),
    "chlorophyll":         ("chlorophyll",          "ug/L"),
    "turbidity":           ("turbidity",            "NTU"),
    "current_speed":       ("current_speed",        "m/s"),
    "current_direction":   ("current_direction",    "°"),
    "sea_netcurrent_speed":("current_speed",        "m/s"),
    "sea_netcurrent_direction": ("current_direction", "°"),
}


# ===================================================================
# Endpoint 1: CBIBS station list
# ===================================================================

@router.get("/chesapeake/stations", summary="CBIBS buoy station list")
async def chesapeake_stations():
    """Return the list of CBIBS buoys in Chesapeake Bay.

    Each station includes its ID, name, coordinates, and available
    measurement parameters.
    """
    cache_key = "ne_buoys:cbibs:stations"
    cached = await cache.get(cache_key)
    if cached is not None:
        return cached

    try:
        data = await _fetch_json(CBIBS_STATION_URL)
    except Exception as e:
        logger.error("CBIBS station list fetch failed: %s", e)
        raise HTTPException(status_code=502, detail=f"CBIBS API error: {e}")

    # The CBIBS API returns a list of station objects (or a wrapper dict).
    stations_raw = data if isinstance(data, list) else data.get("stations", data.get("data", []))

    stations = []
    for stn in stations_raw:
        # Adapt to whatever shape CBIBS returns — try common field names
        station_id = str(stn.get("station_id") or stn.get("id") or stn.get("stationId", ""))
        name = stn.get("station_name") or stn.get("name") or stn.get("description", station_id)
        lat = stn.get("latitude") or stn.get("lat")
        lon = stn.get("longitude") or stn.get("lng") or stn.get("lon")

        if not station_id or lat is None or lon is None:
            continue

        # Gather available parameters if listed
        params_available = stn.get("parameters") or stn.get("measurements") or []
        if isinstance(params_available, list) and params_available:
            if isinstance(params_available[0], dict):
                params_available = [p.get("name", p.get("id", "")) for p in params_available]

        stations.append({
            "station_id": station_id,
            "name": name,
            "latitude": float(lat),
            "longitude": float(lon),
            "network": "CBIBS",
            "parameters": params_available,
            "raw": stn,
        })

    result = {
        "source": "CBIBS",
        "station_count": len(stations),
        "stations": stations,
    }

    await cache.set(cache_key, result, ttl=config.CACHE_TTL_LONG)
    return result


# ===================================================================
# Endpoint 2: CBIBS latest observations
# ===================================================================

@router.get("/chesapeake/current", summary="Latest CBIBS buoy observations")
async def chesapeake_current(
    station_id: Optional[str] = Query(None, description="Filter to a single buoy ID"),
):
    """Return the latest observations from CBIBS buoys.

    Optionally filter to a single station by ID.  Returns normalized
    readings alongside the raw upstream payload.
    """
    cache_key = f"ne_buoys:cbibs:current:{station_id or 'all'}"
    cached = await cache.get(cache_key)
    if cached is not None:
        return cached

    # CBIBS data endpoint — try /station (sometimes includes latest obs)
    # and fall back to /query or /data if needed.
    url = CBIBS_STATION_URL
    params: dict[str, str] = {}
    if station_id:
        # Some CBIBS APIs accept station_id as a query param or path segment
        params["id"] = station_id

    try:
        data = await _fetch_json(url, params=params)
    except Exception as e:
        logger.error("CBIBS current data fetch failed: %s", e)
        raise HTTPException(status_code=502, detail=f"CBIBS API error: {e}")

    stations_raw = data if isinstance(data, list) else data.get("stations", data.get("data", []))

    readings: list[dict] = []
    for stn in stations_raw:
        sid = str(stn.get("station_id") or stn.get("id") or stn.get("stationId", ""))
        name = stn.get("station_name") or stn.get("name") or stn.get("description", sid)
        lat = stn.get("latitude") or stn.get("lat")
        lon = stn.get("longitude") or stn.get("lng") or stn.get("lon")

        if not sid or lat is None or lon is None:
            continue
        if station_id and sid != station_id:
            continue

        # Extract measurement values from nested structures
        obs: dict[str, Any] = {}
        normalized: list[dict] = []

        # Try common patterns for latest-value fields
        for key in _CBIBS_PARAM_MAP:
            val = stn.get(key)
            if val is None:
                # Try nested "measurements" or "latest" sub-objects
                measurements = stn.get("measurements") or stn.get("latest") or {}
                if isinstance(measurements, dict):
                    val = measurements.get(key)
                elif isinstance(measurements, list):
                    for m in measurements:
                        if isinstance(m, dict) and m.get("name") == key:
                            val = m.get("value")
                            break
            if val is not None:
                param_name, unit = _CBIBS_PARAM_MAP[key]
                obs[param_name] = val
                try:
                    normalized.append({
                        "parameter": param_name,
                        "value": float(val),
                        "unit": unit,
                    })
                except (ValueError, TypeError):
                    pass

        # Try to extract a timestamp
        ts_raw = stn.get("time") or stn.get("timestamp") or stn.get("datetime")
        timestamp = None
        if ts_raw:
            try:
                if isinstance(ts_raw, (int, float)):
                    timestamp = datetime.fromtimestamp(ts_raw, tz=timezone.utc).isoformat()
                else:
                    timestamp = str(ts_raw)
            except Exception:
                timestamp = str(ts_raw)

        readings.append({
            "station_id": sid,
            "name": name,
            "latitude": float(lat),
            "longitude": float(lon),
            "network": "CBIBS",
            "timestamp": timestamp,
            "readings": normalized,
            "raw": obs if obs else stn,
        })

    result = {
        "source": "CBIBS",
        "station_count": len(readings),
        "readings": readings,
    }

    await cache.set(cache_key, result, ttl=config.CACHE_TTL)
    return result


# ===================================================================
# Endpoint 3: Source overview
# ===================================================================

@router.get("/overview", summary="Northeast Coastal Buoys source overview")
async def overview():
    """Return a descriptive summary of all networks available through this
    router, including endpoints, coverage, and parameters."""
    return {
        "router": "ne_buoys",
        "description": SOURCE_DESCRIPTION,
        "networks": [
            {
                "id": "cbibs",
                "name": "Chesapeake Bay Interpretive Buoy System (CBIBS)",
                "url": "https://buoybay.noaa.gov/",
                "api_base": CBIBS_BASE,
                "auth_required": False,
                "auth_note": "Free API key may be needed for high-volume use; station list works without one",
                "coverage": "Chesapeake Bay (Maryland, Virginia)",
                "station_count": "~10 buoys",
                "update_frequency": "6 minutes",
                "parameters": [
                    "air_temperature", "relative_humidity", "air_pressure",
                    "wind_speed", "wind_direction", "wind_gust",
                    "wave_height", "wave_period",
                    "water_temperature", "salinity", "dissolved_oxygen",
                    "chlorophyll", "turbidity",
                    "current_speed", "current_direction",
                ],
                "endpoints": [
                    {"path": "/api/ne_buoys/chesapeake/stations", "method": "GET", "description": "CBIBS buoy list"},
                    {"path": "/api/ne_buoys/chesapeake/current", "method": "GET", "description": "Latest buoy observations"},
                ],
            },
            {
                "id": "nbfsmn",
                "name": "Narragansett Bay Fixed-Site Monitoring Network",
                "url": "https://web.uri.edu/gso/research/marine-ecosystems/nbfsmn/",
                "data_via": "NERACOOS ERDDAP (https://data.neracoos.org/erddap/)",
                "auth_required": False,
                "coverage": "Narragansett Bay, Rhode Island",
                "station_count": "~15 buoy/dock stations",
                "update_frequency": "15 minutes",
                "parameters": [
                    "pH", "dissolved_oxygen", "water_temperature",
                    "salinity", "chlorophyll",
                ],
                "note": (
                    "Data for Narragansett Bay stations is best accessed via the "
                    "NERACOOS ERDDAP server (https://data.neracoos.org/erddap/). "
                    "Use the /api/erddap endpoints with server=neracoos_data to query "
                    "these datasets directly."
                ),
            },
        ],
    }
