"""v2 air-quality category router.

Thin filter layer over the unified station catalog.  All heavy lifting
(fetch, normalize, dedupe) lives in ``sources.unified``; this module
just filters to air-quality sources and exposes category-specific endpoints.
"""
from __future__ import annotations

from datetime import UTC, datetime
from typing import Optional

from fastapi import APIRouter, Query

import cache
import config
from sources.unified import (
    _apply_bbox,
    _filter_by_params,
    _get_or_build_catalog,
    _stations_within_radius,
)
from sources.v2.registry import CATEGORIES

router = APIRouter(prefix="/air-quality", tags=["v2-air-quality"])

_CATEGORY = "air_quality"
_SOURCES: set[str] = set(CATEGORIES[_CATEGORY]["sources"])

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


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------


@router.get("/stations", summary="Air-quality stations from all AQ sources")
async def aq_stations(
    lat: Optional[float] = Query(None, ge=-90, le=90, description="Search center latitude"),
    lon: Optional[float] = Query(None, ge=-180, le=180, description="Search center longitude"),
    radius_km: float = Query(50.0, ge=0.1, le=500, description="Search radius in km (requires lat/lon)"),
    bbox: Optional[str] = Query(None, description="Bounding box: west,south,east,north"),
    state: Optional[str] = Query(None, description="2-letter US state code filter"),
    params: Optional[str] = Query(None, description="Comma-separated parameter filter (e.g. pm25,ozone)"),
    quality_tier_max: int = Query(5, ge=1, le=5, description="Max quality tier (1=best)"),
    limit: int = Query(500, ge=1, le=10000),
    offset: int = Query(0, ge=0),
    refresh: bool = Query(False),
):
    cache_key = f"v2:air_quality:stations:{lat}:{lon}:{radius_km}:{bbox}:{state}:{params}:{quality_tier_max}:{limit}:{offset}"
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


@router.get("/sources", summary="List air-quality source modules")
async def aq_sources():
    return {
        "category": _CATEGORY,
        "description": CATEGORIES[_CATEGORY]["description"],
        "sources": sorted(_SOURCES),
        "source_count": len(_SOURCES),
    }


@router.get("/overview", summary="Air-quality category overview and station counts")
async def aq_overview(refresh: bool = Query(False)):
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
    }
