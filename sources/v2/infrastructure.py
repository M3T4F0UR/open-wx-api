"""v2 Infrastructure category router — thin filter over the unified catalog.

Provides category-scoped endpoints for dams, groundwater wells, water
rights, and water quality archives — all filtered from the unified
station catalog and the source registry.

Router prefix: /infrastructure  (mounted under /api/v2 by __init__.py)
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

CATEGORY = "infrastructure"
_META = CATEGORIES[CATEGORY]

router = APIRouter(prefix="/infrastructure", tags=["v2-infrastructure"])


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _infra_sources() -> list[str]:
    """Return the list of source module names in the infrastructure category."""
    return list(_META["sources"])


_INFRA_SOURCE_SET: set[str] = set(_META["sources"])

# Sub-source sets for domain-specific endpoints.
_WELL_SOURCES: set[str] = {"state_wells", "dams_wells"}
_DAM_SOURCES: set[str] = {"dams_wells"}


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


def _is_infra_station(station: dict) -> bool:
    """Check if a unified station belongs to the infrastructure category.

    A station qualifies if ANY of its provider sources are in the
    infrastructure category registry.
    """
    provider_sources = station.get("provider_sources", [])
    return any(src in _INFRA_SOURCE_SET for src in provider_sources)


def _is_well_station(station: dict) -> bool:
    """Check if a unified station is a groundwater well."""
    provider_sources = station.get("provider_sources", [])
    if not any(src in _WELL_SOURCES for src in provider_sources):
        return False
    # Heuristic: check for groundwater-related params or name hints.
    params = set(station.get("params_normalized", []) + station.get("params", []))
    name = (station.get("name") or "").lower()
    if params & {"groundwater_level", "well_depth", "water_level", "depth_to_water"}:
        return True
    if any(kw in name for kw in ("well", "gwsi", "groundwater", "monitoring well")):
        return True
    # If source is state_wells, it's always a well.
    if "state_wells" in provider_sources:
        return True
    return False


def _is_dam_station(station: dict) -> bool:
    """Check if a unified station is from the National Inventory of Dams."""
    provider_sources = station.get("provider_sources", [])
    if "dams_wells" not in provider_sources:
        return False
    # Heuristic: dam-specific params or name hints.
    params = set(station.get("params_normalized", []) + station.get("params", []))
    name = (station.get("name") or "").lower()
    if params & {"dam_height", "storage", "hazard_class", "condition"}:
        return True
    if any(kw in name for kw in ("dam", "reservoir", "levee", "nid")):
        return True
    # Exclude wells from dams_wells — if it looks like a well, skip it.
    if params & {"groundwater_level", "well_depth", "depth_to_water"}:
        return False
    if any(kw in name for kw in ("well", "gwsi", "groundwater")):
        return False
    # Default: if it comes from dams_wells and isn't clearly a well, treat as dam.
    return True


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

@router.get("/stations", summary="Infrastructure stations (dams, wells, water quality)")
async def infra_stations(
    bbox: Optional[str] = Query(
        None,
        description="Bounding box filter: west,south,east,north",
    ),
    state: Optional[str] = Query(
        None,
        description="Two-letter US state code (e.g. CA, AZ)",
    ),
    limit: int = Query(5000, ge=1, le=50000, description="Max stations to return"),
    refresh: bool = Query(False, description="Force cache rebuild"),
):
    """Return all infrastructure stations from the unified catalog,
    filtered to sources in the *infrastructure* category.

    Sources: dams_wells, state_wells, state_water_quality, nyc_data.
    Supports geographic filtering by bounding box and/or state code.
    """
    from sources.unified import _get_or_build_catalog

    catalog = await _get_or_build_catalog(refresh=refresh)
    all_stations = catalog.get("stations", [])

    stations = [s for s in all_stations if _is_infra_station(s)]
    stations = _apply_geo_filters(stations, bbox=bbox, state=state)
    total = len(stations)
    stations = stations[:limit]

    return {
        "category": CATEGORY,
        "total": total,
        "returned": len(stations),
        "limit": limit,
        "sources": sorted(_INFRA_SOURCE_SET),
        "stations": stations,
    }


@router.get("/wells", summary="Groundwater wells")
async def infra_wells(
    bbox: Optional[str] = Query(
        None,
        description="Bounding box filter: west,south,east,north",
    ),
    state: Optional[str] = Query(
        None,
        description="Two-letter US state code (e.g. CA, AZ)",
    ),
    limit: int = Query(5000, ge=1, le=50000, description="Max wells to return"),
    refresh: bool = Query(False, description="Force cache rebuild"),
):
    """Return groundwater wells from the unified catalog.

    Filters to stations from state_wells and dams_wells sources that
    have groundwater-related parameters or naming.
    """
    from sources.unified import _get_or_build_catalog

    catalog = await _get_or_build_catalog(refresh=refresh)
    all_stations = catalog.get("stations", [])

    stations = [s for s in all_stations if _is_well_station(s)]
    stations = _apply_geo_filters(stations, bbox=bbox, state=state)
    total = len(stations)
    stations = stations[:limit]

    return {
        "category": CATEGORY,
        "domain": "groundwater",
        "total": total,
        "returned": len(stations),
        "limit": limit,
        "sources": sorted(_WELL_SOURCES),
        "stations": stations,
    }


@router.get("/dams", summary="National Inventory of Dams (NID)")
async def infra_dams(
    bbox: Optional[str] = Query(
        None,
        description="Bounding box filter: west,south,east,north",
    ),
    state: Optional[str] = Query(
        None,
        description="Two-letter US state code (e.g. CA, OR)",
    ),
    limit: int = Query(5000, ge=1, le=50000, description="Max dams to return"),
    refresh: bool = Query(False, description="Force cache rebuild"),
):
    """Return dams from the National Inventory of Dams (NID) via the
    unified catalog.

    Filters to stations from the dams_wells source that have
    dam-specific parameters or naming (excluding groundwater wells).
    """
    from sources.unified import _get_or_build_catalog

    catalog = await _get_or_build_catalog(refresh=refresh)
    all_stations = catalog.get("stations", [])

    stations = [s for s in all_stations if _is_dam_station(s)]
    stations = _apply_geo_filters(stations, bbox=bbox, state=state)
    total = len(stations)
    stations = stations[:limit]

    return {
        "category": CATEGORY,
        "domain": "dams",
        "total": total,
        "returned": len(stations),
        "limit": limit,
        "sources": sorted(_DAM_SOURCES),
        "stations": stations,
    }


@router.get("/sources", summary="Infrastructure source modules with status")
async def infra_sources():
    """List every source module in the *infrastructure* category with
    its metadata and availability status.
    """
    modules = [_source_module_info(s) for s in _infra_sources()]
    ok_count = sum(1 for m in modules if m["status"] == "ok")

    return {
        "category": CATEGORY,
        "source_count": len(modules),
        "sources_ok": ok_count,
        "sources_error": len(modules) - ok_count,
        "sources": modules,
    }


@router.get("/overview", summary="Infrastructure category description and stats")
async def infra_overview():
    """High-level overview of the infrastructure category including
    description, source count, and cached station statistics.
    """
    source_names = _infra_sources()

    # Try to pull a quick count from the unified cache without triggering
    # a full rebuild — if nothing is cached, return count=None.
    station_count: Optional[int] = None
    well_count: Optional[int] = None
    dam_count: Optional[int] = None
    cached = await cache.get("unified:stations:v1:True")
    if cached and isinstance(cached.get("stations"), list):
        all_stations = cached["stations"]
        station_count = sum(1 for s in all_stations if _is_infra_station(s))
        well_count = sum(1 for s in all_stations if _is_well_station(s))
        dam_count = sum(1 for s in all_stations if _is_dam_station(s))

    return {
        "category": CATEGORY,
        "description": _META["description"],
        "source_count": len(source_names),
        "sources": source_names,
        "station_count": station_count,
        "well_count": well_count,
        "dam_count": dam_count,
    }
