"""v2 Ocean category router — thin filter over the unified catalog.

Provides category-scoped endpoints for ocean buoys, coastal monitoring,
marine weather, and sea level observations — all filtered from the
unified station catalog and the source registry.

Router prefix: /ocean  (mounted under /api/v2 by __init__.py)
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

CATEGORY = "ocean"
_META = CATEGORIES[CATEGORY]

router = APIRouter(prefix="/ocean", tags=["v2-ocean"])


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _ocean_sources() -> list[str]:
    """Return the list of source module names in the ocean category."""
    return list(_META["sources"])


_OCEAN_SOURCE_SET: set[str] = set(_META["sources"])


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


def _is_ocean_station(station: dict) -> bool:
    """Check if a unified station belongs to the ocean category.

    A station qualifies if ANY of its provider sources are in the ocean
    category registry.
    """
    provider_sources = station.get("provider_sources", [])
    return any(src in _OCEAN_SOURCE_SET for src in provider_sources)


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

@router.get("/stations", summary="Ocean & coastal stations")
async def ocean_stations(
    bbox: Optional[str] = Query(
        None,
        description="Bounding box filter: west,south,east,north",
    ),
    state: Optional[str] = Query(
        None,
        description="Two-letter US state code (e.g. MD, VA)",
    ),
    limit: int = Query(5000, ge=1, le=50000, description="Max stations to return"),
    refresh: bool = Query(False, description="Force cache rebuild"),
):
    """Return all ocean/coastal stations from the unified catalog,
    filtered to sources in the *ocean* category.

    Sources: chesapeake_buoys, ocean_extended, erddap, noaa_weather (NDBC subset).
    Supports geographic filtering by bounding box and/or state code.
    """
    from sources.unified import _get_or_build_catalog

    catalog = await _get_or_build_catalog(refresh=refresh)
    all_stations = catalog.get("stations", [])

    stations = [s for s in all_stations if _is_ocean_station(s)]
    stations = _apply_geo_filters(stations, bbox=bbox, state=state)
    total = len(stations)
    stations = stations[:limit]

    return {
        "category": CATEGORY,
        "total": total,
        "returned": len(stations),
        "limit": limit,
        "sources": sorted(_OCEAN_SOURCE_SET),
        "stations": stations,
    }


@router.get("/sources", summary="Ocean source modules with status")
async def ocean_sources():
    """List every source module in the *ocean* category with its
    metadata and availability status.
    """
    modules = [_source_module_info(s) for s in _ocean_sources()]
    ok_count = sum(1 for m in modules if m["status"] == "ok")

    return {
        "category": CATEGORY,
        "source_count": len(modules),
        "sources_ok": ok_count,
        "sources_error": len(modules) - ok_count,
        "sources": modules,
    }


@router.get("/overview", summary="Ocean category description and stats")
async def ocean_overview():
    """High-level overview of the ocean/coastal category including
    description, source count, and cached station statistics.
    """
    source_names = _ocean_sources()

    # Try to pull a quick count from the unified cache without triggering
    # a full rebuild — if nothing is cached, return count=None.
    station_count: Optional[int] = None
    cached = await cache.get("unified:stations:v1:True")
    if cached and isinstance(cached.get("stations"), list):
        station_count = sum(
            1 for s in cached["stations"]
            if _is_ocean_station(s)
        )

    return {
        "category": CATEGORY,
        "description": _META["description"],
        "source_count": len(source_names),
        "sources": source_names,
        "station_count": station_count,
    }
