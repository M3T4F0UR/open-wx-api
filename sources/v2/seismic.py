"""v2 seismic category router.

Thin filter layer over the unified station catalog plus a direct proxy to
the USGS earthquake feed for recent events.  All station-level heavy lifting
(fetch, normalize, dedupe) lives in ``sources.unified``.
"""
from __future__ import annotations

import logging
from datetime import UTC, datetime
from typing import Optional

from fastapi import APIRouter, Query

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

router = APIRouter(prefix="/seismic", tags=["v2-seismic"])

_CATEGORY = "seismic"
_SOURCES: set[str] = set(CATEGORIES[_CATEGORY]["sources"])

# USGS earthquake feed URLs
_USGS_FEED_BASE = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary"
_USGS_FEEDS: dict[str, dict[str, str]] = {
    "hour": {
        "all": f"{_USGS_FEED_BASE}/all_hour.geojson",
        "1.0": f"{_USGS_FEED_BASE}/1.0_hour.geojson",
        "2.5": f"{_USGS_FEED_BASE}/2.5_hour.geojson",
        "4.5": f"{_USGS_FEED_BASE}/4.5_hour.geojson",
        "significant": f"{_USGS_FEED_BASE}/significant_hour.geojson",
    },
    "day": {
        "all": f"{_USGS_FEED_BASE}/all_day.geojson",
        "1.0": f"{_USGS_FEED_BASE}/1.0_day.geojson",
        "2.5": f"{_USGS_FEED_BASE}/2.5_day.geojson",
        "4.5": f"{_USGS_FEED_BASE}/4.5_day.geojson",
        "significant": f"{_USGS_FEED_BASE}/significant_day.geojson",
    },
    "week": {
        "all": f"{_USGS_FEED_BASE}/all_week.geojson",
        "1.0": f"{_USGS_FEED_BASE}/1.0_week.geojson",
        "2.5": f"{_USGS_FEED_BASE}/2.5_week.geojson",
        "4.5": f"{_USGS_FEED_BASE}/4.5_week.geojson",
        "significant": f"{_USGS_FEED_BASE}/significant_week.geojson",
    },
    "month": {
        "all": f"{_USGS_FEED_BASE}/all_month.geojson",
        "1.0": f"{_USGS_FEED_BASE}/1.0_month.geojson",
        "2.5": f"{_USGS_FEED_BASE}/2.5_month.geojson",
        "4.5": f"{_USGS_FEED_BASE}/4.5_month.geojson",
        "significant": f"{_USGS_FEED_BASE}/significant_month.geojson",
    },
}


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


@router.get("/recent", summary="Recent earthquakes from USGS feeds")
async def seismic_recent(
    period: str = Query("day", pattern="^(hour|day|week|month)$", description="Time window"),
    min_magnitude: str = Query("2.5", description="Minimum magnitude: all, 1.0, 2.5, 4.5, significant"),
    bbox: Optional[str] = Query(None, description="Bounding box: west,south,east,north"),
    limit: int = Query(500, ge=1, le=20000),
    refresh: bool = Query(False),
):
    """Recent earthquakes from the USGS real-time GeoJSON feeds.

    Returns earthquake events with magnitude, depth, location, and time.
    Updated every minute by USGS.
    """
    cache_key = f"v2:seismic:recent:{period}:{min_magnitude}:{bbox}:{limit}"
    if not refresh:
        cached = await cache.get(cache_key)
        if cached is not None:
            return {**cached, "from_cache": True}

    feeds = _USGS_FEEDS.get(period)
    if not feeds:
        feeds = _USGS_FEEDS["day"]
    feed_url = feeds.get(min_magnitude, feeds.get("2.5", feeds["all"]))

    client = await http_client.get_client()
    try:
        r = await client.get(feed_url)
        r.raise_for_status()
        data = r.json()
    except Exception as exc:
        logger.warning("USGS earthquake feed failed: %s", exc)
        return {
            "generated_at": datetime.now(UTC).isoformat(),
            "category": _CATEGORY,
            "error": str(exc),
            "total": 0,
            "earthquakes": [],
        }

    features = data.get("features", [])
    earthquakes = []
    for f in features:
        props = f.get("properties", {})
        geom = f.get("geometry", {})
        coords = geom.get("coordinates", [None, None, None])
        lon_val = coords[0] if len(coords) > 0 else None
        lat_val = coords[1] if len(coords) > 1 else None
        depth = coords[2] if len(coords) > 2 else None

        eq_time = props.get("time")
        time_iso = None
        if eq_time is not None:
            try:
                time_iso = datetime.fromtimestamp(eq_time / 1000, tz=UTC).isoformat()
            except (OSError, ValueError):
                pass

        earthquakes.append({
            "id": f.get("id"),
            "lat": lat_val,
            "lon": lon_val,
            "depth_km": depth,
            "magnitude": props.get("mag"),
            "mag_type": props.get("magType"),
            "place": props.get("place"),
            "time": time_iso,
            "status": props.get("status"),
            "tsunami": props.get("tsunami"),
            "felt": props.get("felt"),
            "cdi": props.get("cdi"),
            "mmi": props.get("mmi"),
            "alert": props.get("alert"),
            "url": props.get("url"),
        })

    # Apply bbox filter
    if bbox:
        try:
            west, south, east, north = [float(x.strip()) for x in bbox.split(",")]
            earthquakes = [
                eq for eq in earthquakes
                if eq.get("lat") is not None and eq.get("lon") is not None
                and south <= eq["lat"] <= north and west <= eq["lon"] <= east
            ]
        except (ValueError, TypeError):
            pass

    earthquakes = earthquakes[:limit]
    metadata = data.get("metadata", {})

    result = {
        "generated_at": datetime.now(UTC).isoformat(),
        "category": _CATEGORY,
        "source": "usgs",
        "feed": feed_url,
        "period": period,
        "min_magnitude": min_magnitude,
        "feed_title": metadata.get("title", ""),
        "feed_count": metadata.get("count", len(earthquakes)),
        "total": len(earthquakes),
        "earthquakes": earthquakes,
    }
    await cache.set(cache_key, result, ttl=config.CACHE_TTL)
    return {**result, "from_cache": False}


@router.get("/sources", summary="List seismic source modules")
async def seismic_sources():
    return {
        "category": _CATEGORY,
        "description": CATEGORIES[_CATEGORY]["description"],
        "sources": sorted(_SOURCES),
        "source_count": len(_SOURCES),
    }


@router.get("/overview", summary="Seismic category overview")
async def seismic_overview(refresh: bool = Query(False)):
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
            "recent": "/api/v2/seismic/recent — recent earthquakes from USGS feeds",
        },
    }
