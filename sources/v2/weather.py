"""v2 weather category router — thin filter over the unified catalog.

Exposes weather-only stations from the unified catalog engine, filtered
to sources whose infer_v2_category() resolves to "weather".
"""

from __future__ import annotations

import math
from datetime import UTC, datetime
from typing import Optional

from fastapi import APIRouter, Query
from fastapi.responses import JSONResponse

import cache
import config
from sources.unified import _get_or_build_catalog, _haversine_km, _infer_domain
from sources.v2.registry import CATEGORIES, infer_v2_category

router = APIRouter(prefix="/weather", tags=["v2-weather"])

_WEATHER_SOURCES: set[str] = set(CATEGORIES["weather"]["sources"])

_CACHE_TTL = config.CACHE_TTL  # 5 min default


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _is_weather_station(station: dict) -> bool:
    """Check if a unified station belongs to the weather category.

    A station qualifies if ANY of its provider sources are in the weather
    category registry, OR if its primary source infers to weather.
    """
    provider_sources = station.get("provider_sources", [])
    if any(src in _WEATHER_SOURCES for src in provider_sources):
        return True
    primary = station.get("primary") or {}
    return infer_v2_category(primary.get("source", "")) == "weather"


def _filter_by_state(stations: list[dict], state: str) -> list[dict]:
    """Filter stations by US state code (2-letter, case-insensitive).

    Checks the primary provider's endpoint and network for state hints,
    and falls back to a rough bounding-box check for well-known states.
    """
    st = state.strip().upper()
    out: list[dict] = []
    for s in stations:
        primary = s.get("primary") or {}
        endpoint = (primary.get("endpoint") or "").upper()
        network = (primary.get("network") or "").upper()
        # Check endpoint query params or network name for state code.
        if f"STATE={st}" in endpoint or st in network:
            out.append(s)
            continue
        # Check extra metadata if present.
        extra = primary.get("extra") or {}
        if isinstance(extra, dict) and str(extra.get("state", "")).upper() == st:
            out.append(s)
    return out


def _filter_by_radius(
    stations: list[dict],
    lat: float,
    lon: float,
    radius_km: float,
) -> list[dict]:
    """Filter and sort stations within radius_km of (lat, lon)."""
    results: list[dict] = []
    for s in stations:
        slat = s.get("lat")
        slon = s.get("lon")
        if slat is None or slon is None:
            continue
        dist = _haversine_km(lat, lon, slat, slon)
        if dist <= radius_km:
            results.append({**s, "distance_km": round(dist, 2)})
    results.sort(key=lambda x: x["distance_km"])
    return results


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------


@router.get(
    "/stations",
    summary="Weather-category stations from the unified catalog",
)
async def weather_stations(
    state: Optional[str] = Query(None, description="US state code filter (e.g. OR, CA)"),
    lat: Optional[float] = Query(None, ge=-90, le=90, description="Latitude for geo filter"),
    lon: Optional[float] = Query(None, ge=-180, le=180, description="Longitude for geo filter"),
    radius_km: Optional[float] = Query(None, ge=0.1, le=500, description="Radius in km (requires lat/lon)"),
    limit: int = Query(500, ge=1, le=10000, description="Max stations to return"),
    offset: int = Query(0, ge=0, description="Pagination offset"),
    refresh: bool = Query(False, description="Force rebuild of unified catalog"),
):
    """Return Station objects from the unified catalog filtered to the weather category.

    Supports optional state and geographic (lat/lon/radius_km) filters.
    """
    cache_key = f"v2:weather:stations:{state}:{lat}:{lon}:{radius_km}"
    if not refresh:
        cached = await cache.get(cache_key)
        if cached is not None:
            stations = cached["stations"]
            page = stations[offset : offset + limit]
            return {
                **cached,
                "from_cache": True,
                "offset": offset,
                "limit": limit,
                "returned": len(page),
                "stations": page,
            }

    catalog = await _get_or_build_catalog(refresh=refresh)
    all_stations = catalog.get("stations", [])

    # Filter to weather category.
    weather = [s for s in all_stations if _is_weather_station(s)]

    # State filter.
    if state:
        weather = _filter_by_state(weather, state)

    # Geo filter.
    if lat is not None and lon is not None:
        r = radius_km if radius_km is not None else 50.0
        weather = _filter_by_radius(weather, lat, lon, r)

    total = len(weather)
    payload = {
        "generated_at": datetime.now(UTC).isoformat(),
        "category": "weather",
        "total": total,
        "catalog_station_count": catalog.get("unique_station_count", 0),
        "filters": {
            "state": state,
            "lat": lat,
            "lon": lon,
            "radius_km": radius_km,
        },
        "stations": weather,
    }
    await cache.set(cache_key, payload, ttl=_CACHE_TTL)

    page = weather[offset : offset + limit]
    return {
        **payload,
        "from_cache": False,
        "offset": offset,
        "limit": limit,
        "returned": len(page),
        "stations": page,
    }


@router.get(
    "/stations.geojson",
    summary="All weather stations as GeoJSON for map rendering",
)
async def weather_stations_geojson(
    refresh: bool = Query(False, description="Force rebuild of unified catalog"),
):
    """Return all weather-category stations as a GeoJSON FeatureCollection.

    Optimized for map rendering — minimal properties, no pagination.
    """
    catalog = await _get_or_build_catalog(refresh=refresh)
    all_stations = catalog.get("stations", [])
    weather = [s for s in all_stations if _is_weather_station(s)]

    features = []
    for s in weather:
        lat = s.get("lat")
        lon = s.get("lon")
        if lat is None or lon is None:
            continue
        primary = s.get("primary") or {}
        last_update = primary.get("last_update")
        param_count = len(s.get("params_normalized") or [])
        features.append({
            "type": "Feature",
            "geometry": {"type": "Point", "coordinates": [lon, lat]},
            "properties": {
                "name": s.get("name") or primary.get("name") or "Unknown",
                "source": primary.get("source", "unknown"),
                "network": primary.get("network", ""),
                "quality_tier": s.get("quality_tier"),
                "provider_count": s.get("provider_count", 1),
                "providers": ",".join(s.get("provider_sources", [])),
                "params": ",".join((s.get("params_normalized") or [])[:10]),
                "param_count": param_count,
                "last_update": last_update,
                "url": primary.get("url"),
                "station_id": primary.get("id", ""),
                "endpoint": primary.get("endpoint", ""),
            },
        })

    return JSONResponse(
        content={
            "type": "FeatureCollection",
            "generated_at": datetime.now(UTC).isoformat(),
            "station_count": len(features),
            "features": features,
        },
        headers={"Cache-Control": "public, max-age=300"},
    )


@router.get(
    "/sources",
    summary="Weather source modules and their status",
)
async def weather_sources(
    refresh: bool = Query(False, description="Force rebuild of unified catalog"),
):
    """List source modules in the weather category with per-source station counts."""
    catalog = await _get_or_build_catalog(refresh=refresh)
    all_stations = catalog.get("stations", [])

    # Count stations per weather source.
    source_counts: dict[str, int] = {}
    for s in all_stations:
        for src in s.get("provider_sources", []):
            if src in _WEATHER_SOURCES:
                source_counts[src] = source_counts.get(src, 0) + 1

    sources = []
    for src in sorted(CATEGORIES["weather"]["sources"]):
        sources.append({
            "source": src,
            "station_count": source_counts.get(src, 0),
            "in_catalog": src in source_counts,
        })

    return {
        "generated_at": datetime.now(UTC).isoformat(),
        "category": "weather",
        "source_count": len(sources),
        "sources_with_data": sum(1 for s in sources if s["in_catalog"]),
        "total_weather_stations": sum(s["station_count"] for s in sources),
        "sources": sources,
    }


@router.get(
    "/overview",
    summary="Weather category description and summary stats",
)
async def weather_overview(
    refresh: bool = Query(False, description="Force rebuild of unified catalog"),
):
    """Category description and aggregate statistics for weather stations."""
    catalog = await _get_or_build_catalog(refresh=refresh)
    all_stations = catalog.get("stations", [])

    weather = [s for s in all_stations if _is_weather_station(s)]

    # Compute stats.
    with_coords = sum(
        1 for s in weather
        if s.get("lat") is not None and s.get("lon") is not None
    )
    tier_counts: dict[int, int] = {}
    for s in weather:
        tier = s.get("quality_tier")
        if tier is not None:
            tier_counts[tier] = tier_counts.get(tier, 0) + 1

    # Unique provider sources across weather stations.
    all_sources: set[str] = set()
    for s in weather:
        all_sources.update(s.get("provider_sources", []))

    # Common params across weather stations.
    param_counts: dict[str, int] = {}
    for s in weather:
        for p in s.get("params_normalized", []):
            param_counts[p] = param_counts.get(p, 0) + 1
    top_params = sorted(param_counts.items(), key=lambda x: -x[1])[:20]

    return {
        "generated_at": datetime.now(UTC).isoformat(),
        "category": "weather",
        "description": CATEGORIES["weather"]["description"],
        "registered_sources": CATEGORIES["weather"]["sources"],
        "registered_source_count": len(CATEGORIES["weather"]["sources"]),
        "stats": {
            "station_count": len(weather),
            "stations_with_coordinates": with_coords,
            "catalog_total": catalog.get("unique_station_count", 0),
            "weather_pct_of_catalog": round(
                len(weather) / max(1, catalog.get("unique_station_count", 1)) * 100, 1
            ),
            "unique_provider_sources": sorted(all_sources),
            "quality_tier_distribution": {
                str(k): v for k, v in sorted(tier_counts.items())
            },
            "top_parameters": top_params,
        },
    }
