"""v2 Water & Hydrology category router.

Provides category-scoped endpoints for streamflow, water levels, tides,
flood monitoring, and reservoir data — all filtered from the unified
station catalog and the source registry.

Router prefix: /water  (mounted under /api/v2 by __init__.py)
"""

from __future__ import annotations

import importlib
import logging
from typing import Optional

from fastapi import APIRouter, Query

import cache
import config
from sources.v2.registry import CATEGORIES

logger = logging.getLogger(__name__)

CATEGORY = "water"
_META = CATEGORIES[CATEGORY]

router = APIRouter(prefix="/water", tags=["v2-water"])


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _water_sources() -> list[str]:
    """Return the list of source module names in the water category."""
    return list(_META["sources"])


def _source_module_info(source_name: str) -> dict:
    """Import a source module and extract its metadata."""
    try:
        mod = importlib.import_module(f"sources.{source_name}")
        return {
            "id": source_name,
            "name": getattr(mod, "SOURCE_NAME", source_name),
            "description": getattr(mod, "SOURCE_DESCRIPTION", ""),
            "meta": getattr(mod, "SOURCE_META", {}),
            "has_router": hasattr(mod, "router"),
            "status": "ok",
        }
    except Exception as exc:
        return {
            "id": source_name,
            "name": source_name,
            "description": "",
            "meta": {},
            "has_router": False,
            "status": "error",
            "error": str(exc),
        }


def _filter_stations_by_source(
    stations: list[dict],
    sources: set[str],
) -> list[dict]:
    """Keep only stations whose ``source`` is in the given set."""
    return [s for s in stations if s.get("source") in sources]


def _apply_geo_filters(
    stations: list[dict],
    *,
    bbox: Optional[str] = None,
    state: Optional[str] = None,
) -> list[dict]:
    """Apply optional bounding-box and state filters."""
    if bbox:
        try:
            west, south, east, north = (float(v) for v in bbox.split(","))
            stations = [
                s for s in stations
                if (south <= (s.get("lat") or 0) <= north
                    and west <= (s.get("lon") or 0) <= east)
            ]
        except (ValueError, TypeError):
            pass  # bad bbox — skip silently

    if state:
        st_upper = state.upper()
        stations = [
            s for s in stations
            if (s.get("state") or "").upper() == st_upper
        ]

    return stations


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@router.get("/stations", summary="Water & hydrology stations")
async def water_stations(
    bbox: Optional[str] = Query(
        None,
        description="Bounding box filter: west,south,east,north",
    ),
    state: Optional[str] = Query(
        None,
        description="Two-letter US state code (e.g. OR, CA)",
    ),
    limit: int = Query(5000, ge=1, le=50000, description="Max stations to return"),
    refresh: bool = Query(False, description="Force cache rebuild"),
):
    """Return all water/hydrology stations from the unified catalog,
    filtered to sources in the *water* category.

    Supports geographic filtering by bounding box and/or state code.
    """
    # Reuse the unified catalog (already cached).
    from sources.unified import unified_stations as _unified

    raw = await _unified(
        refresh=refresh,
        include_backups=True,
        include_param_sweeps=True,
        bbox=None,  # we filter ourselves after category narrowing
        limit=250_000,
    )

    water_source_set = set(_water_sources())
    stations = _filter_stations_by_source(
        raw.get("stations", []),
        water_source_set,
    )

    stations = _apply_geo_filters(stations, bbox=bbox, state=state)
    total = len(stations)
    stations = stations[:limit]

    return {
        "category": CATEGORY,
        "total": total,
        "returned": len(stations),
        "limit": limit,
        "sources": sorted(water_source_set),
        "stations": stations,
    }


@router.get("/sources", summary="Water source modules with status")
async def water_sources():
    """List every source module in the *water* category with its
    metadata and availability status.
    """
    modules = [_source_module_info(s) for s in _water_sources()]
    ok_count = sum(1 for m in modules if m["status"] == "ok")

    return {
        "category": CATEGORY,
        "source_count": len(modules),
        "sources_ok": ok_count,
        "sources_error": len(modules) - ok_count,
        "sources": modules,
    }


@router.get("/overview", summary="Water category description and stats")
async def water_overview():
    """High-level overview of the water/hydrology category including
    description, source count, and cached station statistics.
    """
    source_names = _water_sources()

    # Try to pull a quick count from the unified cache without triggering
    # a full rebuild — if nothing is cached, return count=None.
    station_count: Optional[int] = None
    cached = await cache.get("unified:stations:v1:True")
    if cached and isinstance(cached.get("stations"), list):
        water_set = set(source_names)
        station_count = sum(
            1 for s in cached["stations"]
            if s.get("source") in water_set
        )

    return {
        "category": CATEGORY,
        "description": _META["description"],
        "source_count": len(source_names),
        "sources": source_names,
        "station_count": station_count,
    }
