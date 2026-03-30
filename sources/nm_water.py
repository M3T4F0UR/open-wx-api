"""New Mexico Water Data Initiative — OGC SensorThings API (FROST-Server).

Aggregates water monitoring data from OSE, USGS, Albuquerque, and EBID via a
unified FROST-Server SensorThings v1.1 endpoint.  No authentication required.

API reference: https://st2.newmexicowaterdata.org/FROST-Server/v1.1/
"""
from __future__ import annotations

import logging
from typing import Any, Optional

import httpx
from fastapi import APIRouter, HTTPException, Query

import cache
import config
from models import Station

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Module-level metadata (read by server.py /api/sources)
# ---------------------------------------------------------------------------

SOURCE_NAME = "New Mexico Water Data"
SOURCE_DESCRIPTION = (
    "New Mexico Water Data Initiative — aggregates OSE, USGS, Albuquerque, "
    "and EBID monitoring data via a FROST-Server OGC SensorThings v1.1 API. "
    "Includes groundwater wells, stream gauges, and water-quality sensors."
)
SOURCE_META = {
    "url": "https://newmexicowaterdata.org",
    "auth_required": False,
    "update_frequency": "varies (15 min – daily)",
    "coverage": "New Mexico, USA",
    "params": ["groundwater_level", "streamflow", "water_quality"],
    "category": "water",
}

# ---------------------------------------------------------------------------
# Router
# ---------------------------------------------------------------------------

router = APIRouter(prefix="/api/nm_water", tags=["New Mexico Water Data"])

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

FROST_BASE = "https://st2.newmexicowaterdata.org/FROST-Server/v1.1"

_HEADERS = {"User-Agent": config.USER_AGENT, "Accept": "application/json"}


# ---------------------------------------------------------------------------
# Shared HTTP helpers
# ---------------------------------------------------------------------------


def _client() -> httpx.AsyncClient:
    return httpx.AsyncClient(
        timeout=config.HTTP_TIMEOUT,
        headers=_HEADERS,
        follow_redirects=True,
    )


async def _frost_get(path: str, params: dict | None = None) -> dict[str, Any]:
    """Issue a GET against the FROST-Server and return the JSON body."""
    url = f"{FROST_BASE}/{path.lstrip('/')}"
    async with _client() as client:
        resp = await client.get(url, params=params)
        if resp.status_code != 200:
            logger.error("FROST %s returned %s: %s", url, resp.status_code, resp.text[:300])
            raise HTTPException(status_code=502, detail=f"FROST-Server error: {resp.status_code}")
        return resp.json()


# ---------------------------------------------------------------------------
# 1. GET /api/nm_water/things  — list monitoring locations (Things)
# ---------------------------------------------------------------------------

@router.get("/things")
async def list_things(
    limit: int = Query(100, ge=1, le=1000, description="Max results"),
    skip: int = Query(0, ge=0, description="Number of records to skip"),
):
    """List monitoring locations (Things) from the NM Water Data FROST-Server."""
    cache_key = f"nm_water:things:{limit}:{skip}"
    hit = await cache.get(cache_key)
    if hit is not None:
        return hit

    data = await _frost_get("Things", params={"$top": limit, "$skip": skip, "$expand": "Locations"})
    items = data.get("value", [])

    things = []
    for t in items:
        # Extract first location if expanded
        loc = {}
        locations = t.get("Locations", [])
        if locations:
            coords = (locations[0].get("location", {}).get("coordinates") or [None, None])
            if len(coords) >= 2:
                loc = {"lon": coords[0], "lat": coords[1]}

        things.append({
            "id": t.get("@iot.id"),
            "name": t.get("name", ""),
            "description": t.get("description", ""),
            "properties": t.get("properties", {}),
            **loc,
        })

    result = {
        "source": "nm_water",
        "count": len(things),
        "next_link": data.get("@iot.nextLink"),
        "things": things,
    }
    await cache.set(cache_key, result, ttl=config.CACHE_TTL_LONG)
    return result


# ---------------------------------------------------------------------------
# 2. GET /api/nm_water/locations  — all locations with coordinates
# ---------------------------------------------------------------------------

@router.get("/locations")
async def list_locations(
    limit: int = Query(100, ge=1, le=1000, description="Max results"),
    skip: int = Query(0, ge=0, description="Number of records to skip"),
):
    """Return all Locations with coordinates from the NM Water Data FROST-Server."""
    cache_key = f"nm_water:locations:{limit}:{skip}"
    hit = await cache.get(cache_key)
    if hit is not None:
        return hit

    data = await _frost_get("Locations", params={"$top": limit, "$skip": skip})
    items = data.get("value", [])

    locations = []
    for loc in items:
        coords = (loc.get("location", {}).get("coordinates") or [None, None])
        lat, lon = (None, None)
        if len(coords) >= 2:
            lon, lat = coords[0], coords[1]

        locations.append({
            "id": loc.get("@iot.id"),
            "name": loc.get("name", ""),
            "description": loc.get("description", ""),
            "lat": lat,
            "lon": lon,
            "encoding_type": loc.get("encodingType", ""),
        })

    result = {
        "source": "nm_water",
        "count": len(locations),
        "next_link": data.get("@iot.nextLink"),
        "locations": locations,
    }
    await cache.set(cache_key, result, ttl=config.CACHE_TTL_LONG)
    return result


# ---------------------------------------------------------------------------
# 3. GET /api/nm_water/thing/{id}/datastreams  — available data streams
# ---------------------------------------------------------------------------

@router.get("/thing/{thing_id}/datastreams")
async def thing_datastreams(thing_id: int):
    """List Datastreams for a given Thing (monitoring location)."""
    cache_key = f"nm_water:datastreams:{thing_id}"
    hit = await cache.get(cache_key)
    if hit is not None:
        return hit

    data = await _frost_get(f"Things({thing_id})/Datastreams")
    items = data.get("value", [])

    streams = []
    for ds in items:
        obs_type = ds.get("observationType", "")
        uom = ds.get("unitOfMeasurement", {})
        streams.append({
            "id": ds.get("@iot.id"),
            "name": ds.get("name", ""),
            "description": ds.get("description", ""),
            "observation_type": obs_type,
            "unit_name": uom.get("name", ""),
            "unit_symbol": uom.get("symbol", ""),
            "observed_area": ds.get("observedArea"),
            "phenomenon_time": ds.get("phenomenonTime", ""),
        })

    result = {
        "source": "nm_water",
        "thing_id": thing_id,
        "count": len(streams),
        "datastreams": streams,
    }
    await cache.set(cache_key, result, ttl=config.CACHE_TTL)
    return result


# ---------------------------------------------------------------------------
# 4. GET /api/nm_water/observations/{datastream_id}  — recent observations
# ---------------------------------------------------------------------------

@router.get("/observations/{datastream_id}")
async def datastream_observations(
    datastream_id: int,
    limit: int = Query(100, ge=1, le=1000, description="Max observations to return"),
):
    """Fetch recent Observations for a given Datastream, newest first."""
    cache_key = f"nm_water:obs:{datastream_id}:{limit}"
    hit = await cache.get(cache_key)
    if hit is not None:
        return hit

    data = await _frost_get(
        f"Datastreams({datastream_id})/Observations",
        params={"$top": limit, "$orderby": "phenomenonTime desc"},
    )
    items = data.get("value", [])

    observations = []
    for obs in items:
        observations.append({
            "id": obs.get("@iot.id"),
            "timestamp": obs.get("phenomenonTime", ""),
            "result": obs.get("result"),
            "result_time": obs.get("resultTime", ""),
            "parameters": obs.get("parameters", {}),
        })

    result = {
        "source": "nm_water",
        "datastream_id": datastream_id,
        "count": len(observations),
        "next_link": data.get("@iot.nextLink"),
        "observations": observations,
    }
    await cache.set(cache_key, result, ttl=config.CACHE_TTL)
    return result
