"""Texas Water Authorities — LCRA Hydromet, TCEQ water quality, TWDB groundwater.

Covers three major Texas water data sources:

1. LCRA Hydromet (Lower Colorado River Authority)
   275+ automated river and weather gauges across Central Texas.
   Portal: https://hydromet.lcra.org/
   NOTE: No public JSON API discovered — the portal loads data via JS framework
   calls with no documented REST endpoints.  The /tiles/ path returns 403.
   Future iteration: HTML scraping or reverse-engineering XHR calls.
   For now we expose metadata and a pointer to the portal.

2. TCEQ SWQMIS (Surface Water Quality Monitoring Information System)
   ArcGIS FeatureServer with all SWQM monitoring stations statewide.
   REST: https://gisweb.tceq.texas.gov/arcgis/rest/services/SWQMIS/
   No authentication required.

3. TWDB (Texas Water Development Board)
   Groundwater Data Viewer (interactive app, no REST API found).
   waterdatafortexas.org/groundwater for daily levels.
   Future: scrape or use TWDB data services once an API is available.

All public endpoints used here require no authentication.
"""
from __future__ import annotations

import asyncio
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

SOURCE_NAME = "Texas Water Authorities"
SOURCE_DESCRIPTION = (
    "Texas water monitoring — LCRA Hydromet (275+ gauges, Central Texas), "
    "TCEQ SWQMIS water-quality stations (statewide ArcGIS), and TWDB "
    "groundwater database.  No authentication required for TCEQ; LCRA and "
    "TWDB lack public JSON APIs (portal-only for now)."
)
SOURCE_META = {
    "url": "https://hydromet.lcra.org/",
    "auth_required": False,
    "update_frequency": "15 min – daily (varies by agency)",
    "coverage": "Texas, USA",
    "params": [
        "water_level",
        "streamflow",
        "precipitation",
        "water_quality",
        "groundwater_level",
        "lake_level",
    ],
    "category": "water",
}

# ---------------------------------------------------------------------------
# Router
# ---------------------------------------------------------------------------

router = APIRouter(prefix="/api/texas_water", tags=["Texas Water Authorities"])

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# LCRA Hydromet — no JSON API; portal only
LCRA_PORTAL_URL = "https://hydromet.lcra.org/"
LCRA_GIS_BASE = "https://securegis.lcra.org/image/rest/services"

# TCEQ SWQMIS — ArcGIS FeatureServer (public, no auth)
TCEQ_SWQM_BASE = (
    "https://gisweb.tceq.texas.gov/arcgis/rest/services/SWQMIS"
)
TCEQ_SWQM_STATIONS_URL = (
    f"{TCEQ_SWQM_BASE}/SWQM_Stations_usrprd/FeatureServer/0/query"
)

# TWDB — no REST API; interactive viewer only
TWDB_GROUNDWATER_URL = "https://www3.twdb.texas.gov/apps/WaterDataInteractive/GroundWaterDataViewer"
TWDB_DAILY_LEVELS_URL = "https://waterdatafortexas.org/groundwater"

# ---------------------------------------------------------------------------
# HTTP client helpers
# ---------------------------------------------------------------------------

_HEADERS = {
    "User-Agent": config.USER_AGENT,
    "Accept": "application/json",
}


def _client() -> httpx.AsyncClient:
    return httpx.AsyncClient(
        timeout=config.HTTP_TIMEOUT,
        headers=_HEADERS,
        follow_redirects=True,
    )


def _arcgis_query_params(
    where: str = "1=1",
    out_fields: str = "*",
    result_offset: int = 0,
    result_record_count: int = 1000,
) -> dict:
    """Build standard ArcGIS REST query parameters."""
    return {
        "where": where,
        "outFields": out_fields,
        "f": "json",
        "resultOffset": result_offset,
        "resultRecordCount": result_record_count,
        "returnGeometry": "true",
        "spatialRel": "esriSpatialRelIntersects",
    }


async def _fetch_arcgis_stations(
    layer_url: str,
    source_key: str,
    network_label: str,
    max_records: int = 10000,
) -> list[Station]:
    """Fetch all features from a TCEQ ArcGIS query endpoint with pagination."""
    stations: list[Station] = []
    offset = 0

    try:
        async with _client() as client:
            while True:
                params = _arcgis_query_params(result_offset=offset)
                resp = await client.get(layer_url, params=params)
                resp.raise_for_status()
                data = resp.json()

                if "error" in data:
                    logger.warning(
                        "%s ArcGIS error: %s",
                        source_key,
                        data["error"].get("message", data["error"]),
                    )
                    break

                features = data.get("features", [])
                if not features:
                    break

                for feat in features:
                    attrs = feat.get("attributes", {})
                    geom = feat.get("geometry", {})

                    lat = geom.get("y") or attrs.get("LAT_DD")
                    lon = geom.get("x") or attrs.get("LONG_DD")
                    if lat is None or lon is None:
                        continue

                    station_id = (
                        str(attrs.get("STATION_ID") or attrs.get("STATION_NU") or "")
                    )
                    name = (
                        attrs.get("SHORT_DESC")
                        or attrs.get("LONG_DESC")
                        or station_id
                    )

                    stations.append(
                        Station(
                            id=f"tceq_{station_id}" if station_id else f"tceq_{offset}",
                            name=name,
                            lat=float(lat),
                            lon=float(lon),
                            source=source_key,
                            network=network_label,
                            url=f"{TCEQ_SWQM_BASE}/SWQM_Stations_usrprd/FeatureServer/0",
                            extra={
                                "station_id": station_id,
                                "county": attrs.get("CNTY_NME"),
                                "basin_id": attrs.get("BASIN_ID"),
                                "segment_id": attrs.get("SEG_ID"),
                                "active": attrs.get("ACTIVE_IND"),
                                "epa_type": attrs.get("EPA_TYPE1"),
                                "eco_region": attrs.get("ECO_REGION"),
                            },
                        )
                    )

                offset += len(features)
                # Safety: don't loop forever
                if offset >= max_records or not data.get("exceededTransferLimit"):
                    break

    except httpx.HTTPStatusError as exc:
        logger.error(
            "%s HTTP %s: %s", source_key, exc.response.status_code, exc.response.text[:300]
        )
        raise HTTPException(status_code=502, detail=f"{network_label} returned {exc.response.status_code}")
    except httpx.RequestError as exc:
        logger.error("%s connection error: %s", source_key, exc)
        raise HTTPException(status_code=502, detail=f"Cannot reach {network_label}")

    return stations


# ===========================================================================
# 1. LCRA Hydromet — current conditions (metadata only for now)
# ===========================================================================


@router.get("/lcra/conditions", summary="LCRA Hydromet current conditions")
async def lcra_conditions():
    """Return LCRA Hydromet metadata and portal link.

    LCRA Hydromet serves 275+ automated gauges across Central Texas measuring
    rainfall, lake levels, river flows, and weather conditions.  However, the
    portal does not expose a public JSON/REST API — data is loaded via internal
    JavaScript framework calls.

    This endpoint returns metadata and a portal link.  A future iteration will
    add HTML scraping or reverse-engineered XHR endpoints for live gauge data.
    """
    return {
        "source": "lcra_hydromet",
        "name": "LCRA Hydromet",
        "description": (
            "275+ automated river and weather gauges across Central Texas. "
            "Measures rainfall, lake levels, river flows, and weather conditions."
        ),
        "portal_url": LCRA_PORTAL_URL,
        "coverage": "Central Texas (Colorado River basin, Highland Lakes)",
        "gauge_count_approx": 275,
        "parameters": [
            "rainfall",
            "lake_level",
            "river_flow",
            "air_temperature",
            "humidity",
            "wind",
            "barometric_pressure",
        ],
        "update_frequency": "near-real-time (15 min typical)",
        "api_status": "no_public_api",
        "notes": (
            "LCRA Hydromet does not expose a public JSON/REST API. Data is "
            "served via internal JS framework calls on the portal. A future "
            "iteration will reverse-engineer XHR endpoints or add HTML scraping. "
            "310 LCRA sites are reportedly available via MADIS."
        ),
        "related_sources": {
            "madis": "310 LCRA sites available in MADIS public feed",
            "soil_moisture_gis": f"{LCRA_GIS_BASE}/Imagery/Soil_Moisture/ImageServer",
        },
    }


# ===========================================================================
# 2. TCEQ SWQMIS — water quality monitoring stations
# ===========================================================================


@router.get("/tceq/stations", summary="TCEQ SWQMIS water quality monitoring stations")
async def tceq_stations(
    county: Optional[str] = Query(
        None,
        description="Filter by county name (case-insensitive partial match)",
    ),
    basin_id: Optional[str] = Query(
        None,
        description="Filter by TCEQ basin ID",
    ),
    active_only: bool = Query(
        True,
        description="Return only active stations",
    ),
    limit: int = Query(
        2000,
        ge=1,
        le=10000,
        description="Max stations to return",
    ),
):
    """Return TCEQ Surface Water Quality Monitoring stations.

    Queries the TCEQ SWQMIS ArcGIS FeatureServer for monitoring station
    locations across Texas.  Supports filtering by county, basin, and
    active status.
    """
    # Build cache key from filter params
    cache_key = f"texas_water:tceq_stations:{county}:{basin_id}:{active_only}:{limit}"
    cached_result = await cache.get(cache_key)
    if cached_result is not None:
        return cached_result

    # Build WHERE clause
    clauses: list[str] = []
    if active_only:
        clauses.append("ACTIVE_IND = 'Y'")
    if county:
        clauses.append(f"UPPER(CNTY_NME) LIKE '%{county.upper()}%'")
    if basin_id:
        clauses.append(f"BASIN_ID = '{basin_id}'")

    where = " AND ".join(clauses) if clauses else "1=1"

    # Fetch from ArcGIS
    stations: list[Station] = []
    offset = 0

    try:
        async with _client() as client:
            while len(stations) < limit:
                params = _arcgis_query_params(
                    where=where,
                    result_offset=offset,
                    result_record_count=min(1000, limit - len(stations)),
                )
                resp = await client.get(TCEQ_SWQM_STATIONS_URL, params=params)
                resp.raise_for_status()
                data = resp.json()

                if "error" in data:
                    logger.warning(
                        "TCEQ ArcGIS error: %s",
                        data["error"].get("message", data["error"]),
                    )
                    break

                features = data.get("features", [])
                if not features:
                    break

                for feat in features:
                    attrs = feat.get("attributes", {})
                    geom = feat.get("geometry", {})

                    lat = geom.get("y") or attrs.get("LAT_DD")
                    lon = geom.get("x") or attrs.get("LONG_DD")
                    if lat is None or lon is None:
                        continue

                    station_id = str(
                        attrs.get("STATION_ID") or attrs.get("STATION_NU") or ""
                    )
                    name = (
                        attrs.get("SHORT_DESC")
                        or attrs.get("LONG_DESC")
                        or station_id
                    )

                    stations.append(
                        Station(
                            id=f"tceq_{station_id}",
                            name=name,
                            lat=float(lat),
                            lon=float(lon),
                            source="tceq_swqm",
                            network="TCEQ SWQMIS",
                            url=f"{TCEQ_SWQM_BASE}/SWQM_Stations_usrprd/FeatureServer/0",
                            extra={
                                "station_id": station_id,
                                "county": attrs.get("CNTY_NME"),
                                "basin_id": attrs.get("BASIN_ID"),
                                "segment_id": attrs.get("SEG_ID"),
                                "active": attrs.get("ACTIVE_IND"),
                                "epa_type": attrs.get("EPA_TYPE1"),
                                "eco_region": attrs.get("ECO_REGION"),
                                "usgs_gauge": attrs.get("USGS_GAUGE"),
                            },
                        )
                    )

                offset += len(features)
                if not data.get("exceededTransferLimit"):
                    break

    except httpx.HTTPStatusError as exc:
        logger.error("TCEQ HTTP %s: %s", exc.response.status_code, exc.response.text[:300])
        raise HTTPException(status_code=502, detail=f"TCEQ ArcGIS error: {exc.response.status_code}")
    except httpx.RequestError as exc:
        logger.error("TCEQ connection error: %s", exc)
        raise HTTPException(status_code=502, detail="Cannot reach TCEQ ArcGIS server")

    result = {
        "source": "tceq_swqm",
        "network": "TCEQ Surface Water Quality Monitoring Information System",
        "description": "Statewide water quality monitoring stations",
        "filters_applied": {
            "county": county,
            "basin_id": basin_id,
            "active_only": active_only,
        },
        "count": len(stations),
        "stations": [s.model_dump() for s in stations],
    }
    await cache.set(cache_key, result, ttl=config.CACHE_TTL_LONG)
    return result


# ===========================================================================
# 3. Overview — all Texas water sources
# ===========================================================================


@router.get("/overview", summary="Texas water monitoring sources overview")
async def overview():
    """Return a summary of all Texas water data sources.

    Fetches a station count from the TCEQ SWQMIS ArcGIS layer and combines
    it with LCRA and TWDB metadata for a unified overview.
    """
    cache_key = "texas_water:overview"
    cached_result = await cache.get(cache_key)
    if cached_result is not None:
        return cached_result

    # Get TCEQ active station count
    tceq_count = 0
    try:
        async with _client() as client:
            params = {
                "where": "ACTIVE_IND = 'Y'",
                "returnCountOnly": "true",
                "f": "json",
            }
            resp = await client.get(TCEQ_SWQM_STATIONS_URL, params=params)
            resp.raise_for_status()
            data = resp.json()
            tceq_count = data.get("count", 0)
    except Exception as exc:
        logger.warning("TCEQ station count failed: %s", exc)

    result = {
        "source": "texas_water",
        "description": "Texas water monitoring authorities overview",
        "agencies": [
            {
                "id": "lcra_hydromet",
                "name": "LCRA Hydromet",
                "full_name": "Lower Colorado River Authority Hydromet System",
                "station_count_approx": 275,
                "url": LCRA_PORTAL_URL,
                "coverage": "Central Texas (Colorado River basin, Highland Lakes)",
                "parameters": [
                    "rainfall",
                    "lake_level",
                    "river_flow",
                    "weather",
                ],
                "api_status": "no_public_api",
                "notes": (
                    "No public JSON API. Portal loads data via JS framework. "
                    "310 sites reportedly in MADIS."
                ),
            },
            {
                "id": "tceq_swqm",
                "name": "TCEQ SWQMIS",
                "full_name": "Texas Commission on Environmental Quality — Surface Water Quality Monitoring",
                "station_count": tceq_count,
                "url": "https://gis-tceq.opendata.arcgis.com/",
                "arcgis_service": f"{TCEQ_SWQM_BASE}/SWQM_Stations_usrprd/FeatureServer/0",
                "coverage": "Statewide Texas",
                "parameters": ["water_quality"],
                "api_status": "live",
            },
            {
                "id": "twdb_groundwater",
                "name": "TWDB Groundwater",
                "full_name": "Texas Water Development Board — Groundwater Database",
                "url": TWDB_DAILY_LEVELS_URL,
                "viewer_url": TWDB_GROUNDWATER_URL,
                "coverage": "Statewide Texas",
                "parameters": ["groundwater_level"],
                "api_status": "no_public_api",
                "notes": (
                    "Interactive viewer and daily level data at waterdatafortexas.org. "
                    "No REST API discovered.  Groundwater Database (GWDB) available "
                    "via reports interface at twdb.texas.gov/groundwater/data/gwdbrpt.asp."
                ),
            },
        ],
        "total_live_stations": tceq_count,
        "total_approx_stations": tceq_count + 275,
    }
    await cache.set(cache_key, result, ttl=config.CACHE_TTL_LONG)
    return result
