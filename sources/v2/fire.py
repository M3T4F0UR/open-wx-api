"""v2 fire category router.

Thin filter layer over the unified station catalog plus direct proxies to
the Esri Living Atlas VIIRS hotspot and NIFC wildfire incident feeds.
All heavy lifting (fetch, normalize, dedupe) lives in ``sources.unified``;
this module filters to fire-related sources and adds hotspot/incident endpoints.
"""
from __future__ import annotations

import logging
from datetime import UTC, datetime
from typing import Optional

from fastapi import APIRouter, HTTPException, Query

import cache
import config
import http_client
from sources.unified import (
    _apply_bbox,
    _filter_by_params,
    _get_or_build_catalog,
    _stations_within_radius,
)
from sources.v2.registry import CATEGORIES

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/fire", tags=["v2-fire"])

_CATEGORY = "fire"
_SOURCES: set[str] = set(CATEGORIES[_CATEGORY]["sources"])

# Esri Living Atlas service URLs for fire-specific data
_LIVING_ATLAS_BASE = "https://services9.arcgis.com/RHVPKKiFTONKtxq3/arcgis/rest/services"
_VIIRS_URL = f"{_LIVING_ATLAS_BASE}/Satellite_VIIRS_Thermal_Hotspots_and_Fire_Activity/FeatureServer/0/query"
_WILDFIRES_URL = f"{_LIVING_ATLAS_BASE}/USA_Wildfires_v1/FeatureServer/0/query"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _filter_category(stations: list[dict]) -> list[dict]:
    """Keep only stations whose primary source belongs to this category."""
    return [
        st for st in stations
        if (st.get("primary") or {}).get("source", "") in _SOURCES
    ]


def _slim(st: dict) -> dict:
    """Return a compact representation for list endpoints."""
    primary = st.get("primary") or {}
    return {
        "id": primary.get("id") or st.get("canonical_id", ""),
        "name": st.get("name", ""),
        "lat": st.get("lat"),
        "lon": st.get("lon"),
        "source": primary.get("source", ""),
        "quality_tier": st.get("quality_tier"),
        "params": st.get("params_normalized", []),
        "last_update": primary.get("last_update"),
        "distance_km": st.get("distance_km"),
    }


async def _query_arcgis(url: str, where: str, out_fields: str, limit: int) -> list[dict]:
    """Query an ArcGIS FeatureServer with pagination."""
    params = {
        "where": where,
        "outFields": out_fields,
        "returnGeometry": "true",
        "f": "json",
        "resultRecordCount": min(limit, 2000),
        "resultOffset": 0,
    }
    client = await http_client.get_client()
    all_features: list[dict] = []

    while len(all_features) < limit:
        params["resultOffset"] = len(all_features)
        params["resultRecordCount"] = min(2000, limit - len(all_features))
        try:
            r = await client.get(url, params=params)
            r.raise_for_status()
            data = r.json()
        except Exception as exc:
            logger.warning("ArcGIS query failed: %s", exc)
            break

        features = data.get("features", [])
        if not features:
            break
        all_features.extend(features)
        if not data.get("exceededTransferLimit", False):
            break

    return all_features


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------


@router.get("/stations", summary="Fire weather stations (RAWS and related)")
async def fire_stations(
    lat: Optional[float] = Query(None, ge=-90, le=90, description="Search center latitude"),
    lon: Optional[float] = Query(None, ge=-180, le=180, description="Search center longitude"),
    radius_km: float = Query(50.0, ge=0.1, le=500, description="Search radius in km (requires lat/lon)"),
    bbox: Optional[str] = Query(None, description="Bounding box: west,south,east,north"),
    state: Optional[str] = Query(None, description="2-letter US state code filter"),
    params: Optional[str] = Query(None, description="Comma-separated parameter filter"),
    quality_tier_max: int = Query(5, ge=1, le=5),
    limit: int = Query(500, ge=1, le=10000),
    offset: int = Query(0, ge=0),
    refresh: bool = Query(False),
):
    cache_key = f"v2:fire:stations:{lat}:{lon}:{radius_km}:{bbox}:{state}:{params}:{quality_tier_max}:{limit}:{offset}"
    if not refresh:
        cached = await cache.get(cache_key)
        if cached is not None:
            return {**cached, "from_cache": True}

    catalog = await _get_or_build_catalog(refresh=refresh)
    stations = _filter_category(catalog.get("stations", []))

    if lat is not None and lon is not None:
        stations = _stations_within_radius(stations, lat, lon, radius_km)
    if bbox:
        stations = _apply_bbox(stations, bbox)
    if state:
        state_upper = state.strip().upper()
        stations = [
            st for st in stations
            if (st.get("primary") or {}).get("state", "").upper() == state_upper
            or state_upper in (st.get("primary") or {}).get("endpoint", "")
        ]
    if params:
        param_list = [p.strip() for p in params.split(",") if p.strip()]
        stations = _filter_by_params(stations, param_list)
    if quality_tier_max < 5:
        stations = [st for st in stations if (st.get("quality_tier") or 5) <= quality_tier_max]

    total = len(stations)
    page = stations[offset : offset + limit]
    out = [_slim(st) for st in page]

    result = {
        "generated_at": datetime.now(UTC).isoformat(),
        "category": _CATEGORY,
        "total": total,
        "returned": len(out),
        "stations": out,
    }
    await cache.set(cache_key, result, ttl=config.CACHE_TTL)
    return {**result, "from_cache": False}


@router.get("/hotspots", summary="VIIRS satellite fire/thermal hotspot detections")
async def fire_hotspots(
    bbox: Optional[str] = Query(None, description="Bounding box: west,south,east,north"),
    hours: int = Query(24, ge=1, le=168, description="Lookback window in hours"),
    confidence: Optional[str] = Query(None, description="Min confidence: low, nominal, high"),
    limit: int = Query(5000, ge=1, le=50000),
    refresh: bool = Query(False),
):
    """VIIRS thermal hotspot detections from Esri Living Atlas.

    Returns satellite-detected fire/thermal activity points with brightness,
    confidence, and fire radiative power (FRP).
    """
    cache_key = f"v2:fire:hotspots:{bbox}:{hours}:{confidence}:{limit}"
    if not refresh:
        cached = await cache.get(cache_key)
        if cached is not None:
            return {**cached, "from_cache": True}

    # Build ArcGIS WHERE clause for time window
    where_parts = [f"esritimeutc >= CURRENT_TIMESTAMP - INTERVAL '{hours}' HOUR"]
    if confidence:
        conf_map = {"low": "'l'", "nominal": "'n'", "high": "'h'"}
        conf_val = conf_map.get(confidence.lower())
        if conf_val:
            where_parts.append(f"confidence >= {conf_val}")
    where = " AND ".join(where_parts)

    out_fields = "latitude,longitude,brightness,bright_ti5,confidence,frp,acq_date,acq_time,satellite,daynight"
    features = await _query_arcgis(_VIIRS_URL, where, out_fields, limit)

    hotspots = []
    for f in features:
        attrs = f.get("attributes", {})
        geom = f.get("geometry", {})
        hotspots.append({
            "lat": geom.get("y") or attrs.get("latitude"),
            "lon": geom.get("x") or attrs.get("longitude"),
            "brightness": attrs.get("brightness"),
            "bright_ti5": attrs.get("bright_ti5"),
            "confidence": attrs.get("confidence"),
            "frp": attrs.get("frp"),
            "acq_date": attrs.get("acq_date"),
            "acq_time": attrs.get("acq_time"),
            "satellite": attrs.get("satellite"),
            "daynight": attrs.get("daynight"),
        })

    # Apply bbox filter on results if specified
    if bbox:
        try:
            west, south, east, north = [float(x.strip()) for x in bbox.split(",")]
            hotspots = [
                h for h in hotspots
                if h.get("lat") is not None and h.get("lon") is not None
                and south <= h["lat"] <= north and west <= h["lon"] <= east
            ]
        except (ValueError, TypeError):
            pass

    result = {
        "generated_at": datetime.now(UTC).isoformat(),
        "category": _CATEGORY,
        "source": "esri_living_atlas/viirs",
        "hours": hours,
        "total": len(hotspots),
        "hotspots": hotspots[:limit],
    }
    await cache.set(cache_key, result, ttl=config.CACHE_TTL)
    return {**result, "from_cache": False}


@router.get("/incidents", summary="Active wildfire incidents (NIFC via Esri Living Atlas)")
async def fire_incidents(
    bbox: Optional[str] = Query(None, description="Bounding box: west,south,east,north"),
    state: Optional[str] = Query(None, description="2-letter US state code filter"),
    limit: int = Query(500, ge=1, le=5000),
    refresh: bool = Query(False),
):
    """Current US wildfire incidents from the NIFC USA Wildfires layer.

    Returns active fire incident points with name, acres burned, containment,
    and discovery date.
    """
    cache_key = f"v2:fire:incidents:{bbox}:{state}:{limit}"
    if not refresh:
        cached = await cache.get(cache_key)
        if cached is not None:
            return {**cached, "from_cache": True}

    where = "1=1"
    if state:
        where = f"POOState = '{state.strip().upper()}'"

    out_fields = (
        "IncidentName,POOState,POOCounty,DailyAcres,PercentContained,"
        "FireDiscoveryDateTime,IncidentTypeCategory,FireCause"
    )
    features = await _query_arcgis(_WILDFIRES_URL, where, out_fields, limit)

    incidents = []
    for f in features:
        attrs = f.get("attributes", {})
        geom = f.get("geometry", {})
        incidents.append({
            "name": attrs.get("IncidentName"),
            "lat": geom.get("y"),
            "lon": geom.get("x"),
            "state": attrs.get("POOState"),
            "county": attrs.get("POOCounty"),
            "acres": attrs.get("DailyAcres"),
            "percent_contained": attrs.get("PercentContained"),
            "discovery_date": attrs.get("FireDiscoveryDateTime"),
            "type": attrs.get("IncidentTypeCategory"),
            "cause": attrs.get("FireCause"),
        })

    # Apply bbox filter on results if specified
    if bbox:
        try:
            west, south, east, north = [float(x.strip()) for x in bbox.split(",")]
            incidents = [
                inc for inc in incidents
                if inc.get("lat") is not None and inc.get("lon") is not None
                and south <= inc["lat"] <= north and west <= inc["lon"] <= east
            ]
        except (ValueError, TypeError):
            pass

    result = {
        "generated_at": datetime.now(UTC).isoformat(),
        "category": _CATEGORY,
        "source": "esri_living_atlas/usa_wildfires",
        "total": len(incidents),
        "incidents": incidents[:limit],
    }
    await cache.set(cache_key, result, ttl=config.CACHE_TTL)
    return {**result, "from_cache": False}


@router.get("/sources", summary="List fire source modules")
async def fire_sources():
    return {
        "category": _CATEGORY,
        "description": CATEGORIES[_CATEGORY]["description"],
        "sources": sorted(_SOURCES),
        "source_count": len(_SOURCES),
    }


@router.get("/overview", summary="Fire category overview and station/incident counts")
async def fire_overview(refresh: bool = Query(False)):
    catalog = await _get_or_build_catalog(refresh=refresh)
    stations = _filter_category(catalog.get("stations", []))

    source_counts: dict[str, int] = {}
    for st in stations:
        src = (st.get("primary") or {}).get("source", "unknown")
        source_counts[src] = source_counts.get(src, 0) + 1

    with_coords = sum(1 for st in stations if st.get("lat") is not None and st.get("lon") is not None)

    return {
        "generated_at": datetime.now(UTC).isoformat(),
        "category": _CATEGORY,
        "description": CATEGORIES[_CATEGORY]["description"],
        "station_count": len(stations),
        "stations_with_coordinates": with_coords,
        "source_breakdown": source_counts,
        "additional_endpoints": {
            "hotspots": "/api/v2/fire/hotspots — VIIRS satellite fire detections",
            "incidents": "/api/v2/fire/incidents — NIFC active wildfire incidents",
        },
    }
