"""IEM DCP / GOES Mesonet — MADIS mesonet data via Iowa Environmental Mesonet.

IEM DCP networks are the no-auth backdoor to MADIS mesonet data.  Each state
has a ``{STATE}_DCP`` network containing RAWS fire weather, USGS hydro, county
ALERT, snow, and reservoir stations.  CA_DCP alone has 600+ stations.  All 50
states have DCP networks (AL_DCP through WY_DCP), totalling ~10,000+ stations.

Key data paths (all free, no auth):
  - Network metadata and station lists via GeoJSON
  - Current observations (by network or WFO)
  - Station observation history (daily)
  - SHEF variable discovery and parameter-based queries

API reference: https://mesonet.agron.iastate.edu/api/1/
"""
from __future__ import annotations

import logging
from datetime import date
from typing import Optional

from fastapi import APIRouter, HTTPException, Query, Path

import cache
import http_client

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Source registry metadata
# ---------------------------------------------------------------------------
SOURCE_NAME = "IEM DCP / GOES Mesonet"
SOURCE_DESCRIPTION = (
    "MADIS mesonet data via Iowa Environmental Mesonet DCP networks.  "
    "Every US state has a {STATE}_DCP network containing RAWS fire weather, "
    "USGS hydro, county ALERT, snow, and reservoir stations — ~10,000+ total.  "
    "No authentication required."
)
SOURCE_META = {
    "url": "https://mesonet.agron.iastate.edu/api/1/",
    "auth_required": False,
    "update_frequency": "5–60 min (varies by station type)",
    "coverage": "United States (all 50 states)",
    "params": [
        "temperature", "wind_speed", "wind_direction", "wind_gust",
        "relative_humidity", "precipitation", "pressure", "fuel_moisture",
        "stream_height", "reservoir_level", "snow_depth",
    ],
    "category": "weather",
}

router = APIRouter(prefix="/api/iem_dcp", tags=["IEM DCP / GOES Mesonet"])

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
IEM_BASE = "https://mesonet.agron.iastate.edu"
IEM_API = f"{IEM_BASE}/api/1"

# All 50 US states (two-letter codes)
US_STATES = [
    "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA",
    "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD",
    "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ",
    "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC",
    "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY",
]


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@router.get("/networks", summary="List all DCP networks (50 states)")
async def list_networks():
    """Return all 50 state DCP networks with their IEM network IDs.

    Each entry includes the network code (e.g. ``CA_DCP``), state, and
    links to the station list and current observations endpoints.
    """
    cache_key = "iem_dcp:networks"
    cached = await cache.get(cache_key)
    if cached is not None:
        return cached

    networks = []
    for state in US_STATES:
        net_id = f"{state}_DCP"
        networks.append({
            "network": net_id,
            "state": state,
            "stations_url": f"{IEM_BASE}/geojson/network/{net_id}.geojson",
            "current_url": f"{IEM_API}/currents.json?network={net_id}",
        })

    result = {
        "source": "iem_dcp",
        "count": len(networks),
        "networks": networks,
    }
    await cache.set(cache_key, result, ttl=86400)  # static list, cache 24 hr
    return result


@router.get("/stations/{state}", summary="Stations in a state DCP network")
async def get_stations(
    state: str = Path(
        ...,
        description="Two-letter US state code (e.g. CA, OR, WA)",
        min_length=2,
        max_length=2,
    ),
):
    """Return GeoJSON feature collection of all DCP stations in a state.

    Data comes directly from IEM's GeoJSON network endpoint.  Each feature
    includes station ID, name, coordinates, and available parameters.
    """
    state = state.upper()
    if state not in US_STATES:
        raise HTTPException(status_code=400, detail=f"Invalid state: {state}")

    network = f"{state}_DCP"
    cache_key = f"iem_dcp:stations:{state}"
    cached = await cache.get(cache_key)
    if cached is not None:
        return cached

    url = f"{IEM_BASE}/geojson/network/{network}.geojson"
    try:
        data = await http_client.fetch_json(url, source="iem_dcp")
    except Exception as exc:
        logger.error("Failed to fetch %s stations: %s", network, exc)
        raise HTTPException(
            status_code=502,
            detail=f"IEM {network} stations unavailable: {exc}",
        )

    # Pass through the GeoJSON but annotate with our metadata
    feature_count = len(data.get("features", []))
    result = {
        "source": "iem_dcp",
        "network": network,
        "state": state,
        "count": feature_count,
        "geojson": data,
    }
    await cache.set(cache_key, result, ttl=3600)  # station lists change rarely
    return result


@router.get("/current/{state}", summary="Current obs for all DCP stations in a state")
async def get_current(
    state: str = Path(
        ...,
        description="Two-letter US state code (e.g. CA, OR, WA)",
        min_length=2,
        max_length=2,
    ),
    wfo: Optional[str] = Query(
        None,
        description="Optional: filter by NWS WFO (e.g. LOX, SFO, PQR). "
                    "When set, returns DCP obs for that WFO's CWA instead of the full state.",
    ),
):
    """Return current observations from all DCP stations in a state.

    Optionally filter by NWS Weather Forecast Office (WFO) using the ``wfo``
    query parameter — useful for fire-weather situational awareness within a
    specific forecast area.
    """
    state = state.upper()
    if state not in US_STATES:
        raise HTTPException(status_code=400, detail=f"Invalid state: {state}")

    network = f"{state}_DCP"

    if wfo:
        wfo = wfo.upper()
        cache_key = f"iem_dcp:current:wfo:{wfo}"
        params = {"wfo": wfo}
    else:
        cache_key = f"iem_dcp:current:{state}"
        params = {"network": network}

    cached = await cache.get(cache_key)
    if cached is not None:
        return cached

    url = f"{IEM_API}/currents.json"
    try:
        data = await http_client.fetch_json(url, params=params, source="iem_dcp")
    except Exception as exc:
        logger.error("Failed to fetch current obs for %s: %s", network, exc)
        raise HTTPException(
            status_code=502,
            detail=f"IEM current obs unavailable: {exc}",
        )

    station_count = len(data.get("data", []))
    result = {
        "source": "iem_dcp",
        "network": network if not wfo else f"WFO_{wfo}",
        "count": station_count,
        "data": data.get("data", []),
    }
    await cache.set(cache_key, result, ttl=300)  # 5 min for current obs
    return result


@router.get(
    "/station/{station_id}/history",
    summary="Observation history for a DCP station",
)
async def get_station_history(
    station_id: str = Path(
        ...,
        description="Station identifier (e.g. PPEC1, BTAC1)",
    ),
    network: str = Query(
        ...,
        description="IEM network code (e.g. CA_DCP, OR_DCP)",
    ),
    date: str = Query(
        None,
        description="Date in YYYY-MM-DD format (defaults to today)",
    ),
):
    """Return observation history for a single DCP station on a given date.

    The ``network`` parameter is required because IEM needs it to route
    the query.  Use the ``/stations/{state}`` endpoint to discover station
    IDs and their networks.
    """
    network = network.upper()
    station_id = station_id.upper()

    # Default to today
    if not date:
        from datetime import date as date_type
        date = date_type.today().isoformat()

    cache_key = f"iem_dcp:history:{station_id}:{network}:{date}"
    cached = await cache.get(cache_key)
    if cached is not None:
        return cached

    url = f"{IEM_API}/obhistory.json"
    params = {
        "station": station_id,
        "network": network,
        "date": date,
    }
    try:
        data = await http_client.fetch_json(url, params=params, source="iem_dcp")
    except Exception as exc:
        logger.error(
            "Failed to fetch history for %s/%s on %s: %s",
            station_id, network, date, exc,
        )
        raise HTTPException(
            status_code=502,
            detail=f"IEM observation history unavailable: {exc}",
        )

    result = {
        "source": "iem_dcp",
        "station_id": station_id,
        "network": network,
        "date": date,
        "count": len(data.get("data", [])),
        "data": data.get("data", []),
    }
    # Cache historical dates longer; today's data refreshes
    from datetime import date as date_type
    is_today = date == date_type.today().isoformat()
    await cache.set(cache_key, result, ttl=300 if is_today else 3600)
    return result


@router.get("/shef/{station_id}", summary="SHEF variables reported by a station")
async def get_shef_variables(
    station_id: str = Path(
        ...,
        description="Station identifier (e.g. PPEC1, BTAC1)",
    ),
):
    """Return all SHEF (Standard Hydrologic Exchange Format) variables that
    a station reports, with their latest values.

    SHEF variables encode parameter type, duration, and extremum — e.g.
    ``TA`` = air temperature, ``HG`` = gage height, ``PC`` = precip accumulator.
    This is the best way to discover what a DCP station actually measures.
    """
    station_id = station_id.upper()
    cache_key = f"iem_dcp:shef:{station_id}"
    cached = await cache.get(cache_key)
    if cached is not None:
        return cached

    url = f"{IEM_API}/last_shef.json"
    params = {"station": station_id}
    try:
        data = await http_client.fetch_json(url, params=params, source="iem_dcp")
    except Exception as exc:
        logger.error("Failed to fetch SHEF for %s: %s", station_id, exc)
        raise HTTPException(
            status_code=502,
            detail=f"IEM SHEF data unavailable: {exc}",
        )

    result = {
        "source": "iem_dcp",
        "station_id": station_id,
        "count": len(data.get("data", [])),
        "data": data.get("data", []),
    }
    await cache.set(cache_key, result, ttl=600)  # 10 min
    return result


@router.get("/overview", summary="IEM DCP source information")
async def overview():
    """Return metadata about the IEM DCP data source, including coverage,
    available networks, endpoint descriptions, and example queries.
    """
    return {
        "source": "iem_dcp",
        "name": SOURCE_NAME,
        "description": SOURCE_DESCRIPTION,
        "auth_required": False,
        "total_networks": len(US_STATES),
        "estimated_stations": "10,000+",
        "station_types": [
            "RAWS (fire weather)",
            "USGS (hydrology)",
            "County ALERT (flood warning)",
            "SNOTEL / snow",
            "Reservoir / dam",
            "Agricultural",
        ],
        "endpoints": {
            "networks": "/api/iem_dcp/networks",
            "stations": "/api/iem_dcp/stations/{state}",
            "current": "/api/iem_dcp/current/{state}",
            "history": "/api/iem_dcp/station/{station_id}/history?network={net}&date=YYYY-MM-DD",
            "shef": "/api/iem_dcp/shef/{station_id}",
        },
        "examples": {
            "california_stations": "/api/iem_dcp/stations/CA",
            "california_current": "/api/iem_dcp/current/CA",
            "current_by_wfo": "/api/iem_dcp/current/CA?wfo=LOX",
            "station_history": "/api/iem_dcp/station/PPEC1/history?network=CA_DCP&date=2026-03-20",
            "shef_variables": "/api/iem_dcp/shef/PPEC1",
        },
        "upstream_api": "https://mesonet.agron.iastate.edu/api/1/",
        "notes": (
            "DCP stations report via GOES satellite downlink.  Update frequency "
            "varies: RAWS typically every 10-60 min, USGS every 15-60 min, "
            "ALERT stations event-driven.  SHEF encoding describes each variable's "
            "physical element, duration, and extremum."
        ),
    }
