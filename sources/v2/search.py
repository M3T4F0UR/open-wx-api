"""V2 search endpoint — geographic station search with v2 category filtering.

Wraps the unified catalog engine (sources.unified) with v2 category semantics
from sources.v2.registry.
"""
from __future__ import annotations

from datetime import UTC, datetime
from typing import Optional

from fastapi import APIRouter, Query

from sources.unified import (
    _filter_by_params,
    _get_or_build_catalog,
    _haversine_km,
    _stations_within_radius,
)
from sources.v2.registry import CATEGORIES, infer_v2_category

router = APIRouter(tags=["v2-search"])


@router.get(
    "/search",
    summary="Geographic station search with v2 category filtering",
)
async def v2_search(
    lat: float = Query(..., ge=-90, le=90, description="Latitude of search center"),
    lon: float = Query(..., ge=-180, le=180, description="Longitude of search center"),
    radius_km: float = Query(50.0, ge=0.1, le=500, description="Search radius in km"),
    category: Optional[str] = Query(
        None,
        description="V2 category filter: " + ", ".join(CATEGORIES),
    ),
    params: Optional[str] = Query(
        None,
        description="Comma-separated parameter filter (e.g. temperature,precipitation)",
    ),
    quality_tier_max: int = Query(
        5, ge=1, le=5, description="Max quality tier to include (1=best, 5=caution)"
    ),
    limit: int = Query(100, ge=1, le=5000, description="Max stations to return"),
    offset: int = Query(0, ge=0, description="Pagination offset"),
):
    """Search for stations near a geographic point with v2 category filtering.

    Returns deduplicated stations sorted by distance, each annotated with its
    inferred v2 category.  Supports pagination via limit/offset.
    """
    catalog = await _get_or_build_catalog()
    all_stations = catalog.get("stations", [])

    # Geographic filter.
    nearby = _stations_within_radius(all_stations, lat, lon, radius_km)

    # Parameter filter.
    if params:
        param_list = [p.strip() for p in params.split(",") if p.strip()]
        nearby = _filter_by_params(nearby, param_list)

    # Quality tier filter.
    if quality_tier_max < 5:
        nearby = [
            st for st in nearby if (st.get("quality_tier") or 5) <= quality_tier_max
        ]

    # Annotate each station with its v2 category.
    for st in nearby:
        st["category"] = infer_v2_category(
            st.get("params_normalized", []),
            (st.get("primary") or {}).get("source", ""),
        )

    # Category filter (applied after annotation so total reflects pre-filter).
    if category:
        nearby = [st for st in nearby if st.get("category") == category]

    total = len(nearby)

    # Pagination.
    page = nearby[offset : offset + limit]

    # Build slim response objects.
    stations_out: list[dict] = []
    for st in page:
        primary = st.get("primary") or {}
        stations_out.append(
            {
                "id": primary.get("id") or st.get("canonical_id", ""),
                "name": st.get("name", ""),
                "lat": st.get("lat"),
                "lon": st.get("lon"),
                "source": primary.get("source", ""),
                "category": st.get("category", ""),
                "quality_tier": st.get("quality_tier"),
                "distance_km": st.get("distance_km"),
                "params": st.get("params_normalized", []),
                "last_update": primary.get("last_update"),
            }
        )

    return {
        "query": {
            "lat": lat,
            "lon": lon,
            "radius_km": radius_km,
            "category": category,
        },
        "total": total,
        "returned": len(stations_out),
        "stations": stations_out,
    }
