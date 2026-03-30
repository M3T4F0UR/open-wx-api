"""NC State CLOUDS API — Climate Office Unified Data System.

~3,600 stations across 16 networks (ECONET, ASOS, AWOS, BUOY, CMAN,
CoCoRaHS, COOP, Duke, ECOExt, NOS, RAWS, ThreadEx, TVA, USACE, USCRN,
USGS) covering North Carolina and surrounding regions.  Provides
temperature, precipitation, humidity, wind, solar radiation, soil
moisture, and many more variables at sub-hourly to daily resolution.

Operated by the North Carolina State Climate Office (NCSCO) at NC State
University.

API docs:  https://api.climate.ncsu.edu/help
Networks:  https://api.climate.ncsu.edu/networks
Accounts:  https://accounts.climate.ncsu.edu

Authentication: requires a user-specific hash key obtained by creating
a free NCSCO account.  Set the NC_ECONET_HASH environment variable.
"""
from __future__ import annotations

import logging
from typing import Any, Optional

from fastapi import APIRouter, HTTPException, Query

import cache
import config
import http_client

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Source registry metadata  (read by server.py /api/sources)
# ---------------------------------------------------------------------------

SOURCE_NAME = "NC ECONet CLOUDS API"
SOURCE_DESCRIPTION = (
    "North Carolina State Climate Office CLOUDS API — ~3,600 stations across "
    "16 networks (ECONET, ASOS, AWOS, CoCoRaHS, COOP, RAWS, USCRN, USGS, "
    "and more).  Covers temperature, precipitation, humidity, wind, solar "
    "radiation, soil moisture, and dozens of additional variables.  Requires "
    "a free NCSCO account hash key (NC_ECONET_HASH env var)."
)
SOURCE_META = {
    "url": "https://climate.ncsu.edu/",
    "api_docs": "https://api.climate.ncsu.edu/help",
    "auth_required": True,
    "auth_type": "hash_key (free NCSCO account)",
    "update_frequency": "varies (sub-hourly to daily by network)",
    "coverage": "North Carolina + surrounding states (~3,600 stations, 16 networks)",
    "params": [
        "temperature", "precipitation", "relative_humidity", "dewpoint",
        "wind_speed", "wind_direction", "solar_radiation", "soil_moisture",
        "soil_temperature", "pressure", "snow_depth",
    ],
    "category": "weather",
    "station_count": "~3,600",
    "networks": [
        "ECONET", "ECOExt", "ASOS", "AWOS", "BUOY", "CMAN", "CoCoRaHS",
        "COOP", "Duke", "NOS", "RAWS", "ThreadEx", "TVA", "USACE", "USCRN",
        "USGS",
    ],
}

# ---------------------------------------------------------------------------
# Router
# ---------------------------------------------------------------------------

router = APIRouter(prefix="/api/nc_econet", tags=["NC ECONet CLOUDS API"])

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_API_BASE = "https://api.climate.ncsu.edu"
_DATA_URL = f"{_API_BASE}/data.php"
_NETWORKS_URL = f"{_API_BASE}/networks"
_SOURCE = "nc_econet"

# Hash key loaded from config (ultimately from NC_ECONET_HASH env var)
NC_ECONET_HASH: str = getattr(config, "NC_ECONET_HASH", "") or ""

# Known networks and their approximate station counts
_NETWORKS = {
    "ECONET":    {"name": "ECONet", "desc": "NC Environment & Climate Observing Network", "approx_stations": 42},
    "ECOExt":    {"name": "ECONet Extended", "desc": "Extended ECONet sites", "approx_stations": 20},
    "ASOS":      {"name": "ASOS", "desc": "Automated Surface Observing System", "approx_stations": 60},
    "AWOS":      {"name": "AWOS", "desc": "Automated Weather Observing System", "approx_stations": 30},
    "BUOY":      {"name": "BUOY", "desc": "Offshore buoy observations", "approx_stations": 10},
    "CMAN":      {"name": "CMAN", "desc": "Coastal-Marine Automated Network", "approx_stations": 15},
    "CoCoRaHS":  {"name": "CoCoRaHS", "desc": "Community Collaborative Rain, Hail & Snow Network", "approx_stations": 2000},
    "COOP":      {"name": "COOP", "desc": "Cooperative Observer Program", "approx_stations": 300},
    "Duke":      {"name": "Duke", "desc": "Duke University research stations", "approx_stations": 5},
    "NOS":       {"name": "NOS", "desc": "National Ocean Service", "approx_stations": 20},
    "RAWS":      {"name": "RAWS", "desc": "Remote Automated Weather Stations", "approx_stations": 40},
    "ThreadEx":  {"name": "ThreadEx", "desc": "Threaded extremes (long-record climate)", "approx_stations": 15},
    "TVA":       {"name": "TVA", "desc": "Tennessee Valley Authority", "approx_stations": 30},
    "USACE":     {"name": "USACE", "desc": "US Army Corps of Engineers", "approx_stations": 20},
    "USCRN":     {"name": "USCRN", "desc": "US Climate Reference Network", "approx_stations": 10},
    "USGS":      {"name": "USGS", "desc": "US Geological Survey streamflow/weather", "approx_stations": 900},
}

# Common CLOUDS variable names -> (normalised param, unit)
_VAR_MAP: dict[str, tuple[str, str]] = {
    "temp2m":      ("temperature",       "°F"),
    "rh2m":        ("relative_humidity", "%"),
    "dewpt2m":     ("dewpoint",          "°F"),
    "precip1m":    ("precipitation",     "in"),
    "wind2m":      ("wind_speed",        "mph"),
    "winddir2m":   ("wind_direction",    "°"),
    "solar_rad":   ("solar_radiation",   "W/m²"),
    "soil_temp":   ("soil_temperature",  "°F"),
    "soil_moist":  ("soil_moisture",     "%"),
    "press":       ("pressure",          "mb"),
    "snow_depth":  ("snow_depth",        "in"),
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _check_hash() -> str:
    """Return the hash key or raise 503 if not configured."""
    if not NC_ECONET_HASH:
        raise HTTPException(
            status_code=503,
            detail=(
                "NC ECONet CLOUDS API requires a hash key.  Set the "
                "NC_ECONET_HASH environment variable with your NCSCO account "
                "hash from https://accounts.climate.ncsu.edu"
            ),
        )
    return NC_ECONET_HASH


async def _clouds_fetch(params: dict, *, cache_key: str, ttl: int = 300) -> Any:
    """Fetch from CLOUDS data.php with hash and caching."""
    cached = await cache.get(cache_key)
    if cached is not None:
        return cached

    hash_key = _check_hash()
    params = {**params, "hash": hash_key, "output": "json"}

    try:
        data = await http_client.fetch_json(
            _DATA_URL, params=params, source=_SOURCE,
        )
    except Exception as exc:
        logger.error("CLOUDS API fetch failed: %s (params=%s)", exc, params)
        raise HTTPException(status_code=502, detail=f"CLOUDS API error: {exc}")

    await cache.set(cache_key, data, ttl=ttl)
    return data


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@router.get("/networks", summary="List available CLOUDS networks")
async def list_networks():
    """Return the 16 observation networks available through the CLOUDS API.

    This endpoint does not require a hash key — it returns locally-cached
    network metadata scraped from the CLOUDS networks page.
    """
    cache_key = "nc_econet:networks"
    cached = await cache.get(cache_key)
    if cached is not None:
        return cached

    # Try fetching live network list from the CLOUDS API
    networks_out: list[dict] = []
    try:
        data = await http_client.fetch_json(
            _NETWORKS_URL, source=_SOURCE,
        )
        # Response may be HTML or JSON — if JSON, parse it
        if isinstance(data, list):
            for item in data:
                if isinstance(item, dict):
                    networks_out.append(item)
        elif isinstance(data, dict):
            # Might have a wrapper key
            for key, val in data.items():
                if isinstance(val, list):
                    networks_out = val
                    break
    except Exception:
        logger.info("CLOUDS /networks returned non-JSON; using local metadata")

    # Fall back to / augment with local metadata
    if not networks_out:
        networks_out = [
            {
                "id": net_id,
                "name": meta["name"],
                "description": meta["desc"],
                "approx_stations": meta["approx_stations"],
            }
            for net_id, meta in _NETWORKS.items()
        ]

    result = {
        "source": _SOURCE,
        "count": len(networks_out),
        "networks": networks_out,
    }
    await cache.set(cache_key, result, ttl=3600)
    return result


@router.get("/current", summary="Current observations from a CLOUDS network")
async def get_current_observations(
    network: str = Query("ECONET", description="Network type (e.g. ECONET, ASOS, COOP, CoCoRaHS)"),
    var: str = Query("temp2m", description="Variable name (e.g. temp2m, precip1m, rh2m, wind2m)"),
    state: Optional[str] = Query(None, description="State filter (e.g. NC, SC, VA)"),
    hours: int = Query(1, ge=1, le=24, description="Hours of recent data (1-24)"),
):
    """Fetch current/recent observations from a CLOUDS network.

    Requires the NC_ECONET_HASH environment variable to be set with a valid
    NCSCO account hash.  Create a free account at
    https://accounts.climate.ncsu.edu.

    Common variables: temp2m, rh2m, dewpt2m, precip1m, wind2m, winddir2m,
    solar_rad, soil_temp, soil_moist, press, snow_depth.
    """
    state_part = f";state={state}" if state else ""
    cache_key = f"nc_econet:current:{network}:{var}:{state}:{hours}"

    params = {
        "var": var,
        "loc": f"type={network}{state_part}",
        "start": f"-{hours} hour",
        "end": "now",
    }

    data = await _clouds_fetch(params, cache_key=cache_key, ttl=300)

    return {
        "source": _SOURCE,
        "network": network,
        "variable": var,
        "hours": hours,
        "state": state,
        "data": data,
    }


@router.get("/overview", summary="NC ECONet CLOUDS API overview")
async def nc_econet_overview() -> dict:
    """Summary of the NC State CLOUDS API and available endpoints."""
    configured = bool(NC_ECONET_HASH)
    return {
        "source": _SOURCE,
        "name": SOURCE_NAME,
        "description": SOURCE_DESCRIPTION,
        "operator": "North Carolina State Climate Office (NCSCO), NC State University",
        "coverage": "North Carolina + surrounding states (~3,600 stations)",
        "auth_required": True,
        "auth_type": "hash_key",
        "auth_configured": configured,
        "auth_instructions": (
            "Create a free account at https://accounts.climate.ncsu.edu, "
            "copy your API hash from the My API page, and set it as the "
            "NC_ECONET_HASH environment variable."
        ),
        "update_frequency": "varies by network (sub-hourly to daily)",
        "api_base": _API_BASE,
        "data_endpoint": _DATA_URL,
        "output_formats": ["json", "csv", "geojson", "html", "xls", "chart", "map"],
        "networks": [
            {
                "id": net_id,
                "name": meta["name"],
                "description": meta["desc"],
                "approx_stations": meta["approx_stations"],
            }
            for net_id, meta in _NETWORKS.items()
        ],
        "common_variables": [
            {"name": "temp2m", "description": "Air temperature at 2m", "unit": "°F"},
            {"name": "rh2m", "description": "Relative humidity at 2m", "unit": "%"},
            {"name": "dewpt2m", "description": "Dewpoint at 2m", "unit": "°F"},
            {"name": "precip1m", "description": "Precipitation (1-min bucket)", "unit": "in"},
            {"name": "wind2m", "description": "Wind speed at 2m", "unit": "mph"},
            {"name": "winddir2m", "description": "Wind direction at 2m", "unit": "°"},
            {"name": "solar_rad", "description": "Solar radiation", "unit": "W/m²"},
            {"name": "soil_temp", "description": "Soil temperature", "unit": "°F"},
            {"name": "soil_moist", "description": "Soil moisture", "unit": "%"},
            {"name": "press", "description": "Barometric pressure", "unit": "mb"},
            {"name": "snow_depth", "description": "Snow depth", "unit": "in"},
        ],
        "query_parameters": {
            "var": "Comma-separated variable list (e.g. temp2m,precip1m)",
            "loc": "Location filter: type=NETWORK;state=XX or city=NAME or station=ID",
            "start": "Start time (PHP strtotime format: -1 hour, 2026-03-01, etc.)",
            "end": "End time (now, -1 day, 2026-03-20, etc.)",
            "int": "Time interval (1 hour, 1 day, any)",
            "obtype": "Observation type: D=daily, H=hourly, O=minute",
            "output": "Output format: json, csv, geojson, html, xls",
            "hash": "Your NCSCO API hash (set via NC_ECONET_HASH env var)",
        },
        "usage_notes": (
            "Data is for governmental, educational, and public informational "
            "purposes.  API hashes may not be shared with other users."
        ),
        "endpoints": [
            "/api/nc_econet/networks",
            "/api/nc_econet/current",
            "/api/nc_econet/overview",
        ],
    }
