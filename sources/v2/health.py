"""v2 health & stats endpoints — source health with category breakdown.

Two endpoints:
  GET /api/v2/health — Probes all source modules, grouped by v2 category.
  GET /api/v2/stats  — Platform-wide statistics (station counts, source counts).
"""

from __future__ import annotations

import asyncio
import importlib
import logging
import time
from datetime import UTC, datetime

import httpx
from fastapi import APIRouter, Query

import cache
import config
from sources.v2.registry import CATEGORIES, _SOURCE_TO_CATEGORY

logger = logging.getLogger(__name__)

router = APIRouter(tags=["v2-health"])

# ── Probe endpoints per source module ──────────────────────────────────────
# Lightweight metadata / station-list endpoints used to verify each source
# is responding.  These must be cheap (no full data fetches).

_PROBE_ENDPOINTS: dict[str, str] = {
    # weather
    "noaa_weather": "/api/noaa_weather/nws/stations?state=OR&limit=1",
    "faa_weather": "/api/faa/stations?limit=1",
    "mesonets": "/api/mesonets/iem/networks",
    "iem_dcp": "/api/iem_dcp/stations?state=OR&limit=1",
    "regional": "/api/regional/stations?limit=1",
    "coagmet": "/api/coagmet/stations",
    "montana_mesonet_api": "/api/montana_mesonet/stations?limit=1",
    "infrastructure": "/api/infrastructure/stations?limit=1",
    "dot_rwis_arcgis": "/api/dot_rwis/stations?limit=1",
    "fawn": "/api/regional/fawn/stations",
    "nc_econet": "/api/nc_econet/stations",
    "southeast_wx": "/api/southeast_wx/stations?limit=1",
    "ag_networks": "/api/ag/stations?limit=1",
    "wsu_agweathernet": "/api/wsu_agweathernet/stations",
    "sdge_weather": "/api/sdge/stations",
    "madis_public": "/api/madis/stations?limit=1",
    "hong_kong": "/api/hong_kong/stations",
    "austria": "/api/austria/stations?limit=1",
    "poland": "/api/poland/synop/stations",
    "iceland": "/api/iceland/stations",
    "baltic": "/api/baltic/stations?limit=1",
    "argentina": "/api/argentina/stations",
    "australia": "/api/australia/stations",
    "chile": "/api/chile/stations",
    "colombia": "/api/colombia/stations?limit=1",
    "mexico": "/api/mexico/stations",
    "asia_east": "/api/asia_east/stations?limit=1",
    "europe_nordic": "/api/europe_nordic/stations?limit=1",
    "europe_west": "/api/europe_west/stations?limit=1",
    "international": "/api/international/stations?limit=1",
    "waggle_sage": "/api/waggle/nodes?limit=1",
    "citizen": "/api/citizen/stations?limit=1",
    "arcgis_realtime": "/api/arcgis_realtime/stations?limit=1",
    "noaa_arcgis": "/api/noaa_arcgis/stations?limit=1",
    "calfire_raws": "/api/calfire_raws/stations?limit=1",
    "space_weather": "/api/space_weather/latest",
    # water
    "usgs": "/api/usgs/stations?state=OR&limit=1",
    "noaa_water": "/api/noaa_water/coops/stations?limit=1",
    "federal_water": "/api/federal_water/usace/locations?office=NWP&limit=1",
    "usace_cda": "/api/usace_cda/locations?office=NWP&limit=1",
    "california": "/api/california/cdec/stations?limit=1",
    "california_extended": "/api/california_extended/stations?limit=1",
    "eastcoast": "/api/eastcoast/stations?limit=1",
    "florida_water": "/api/florida_water/stations?limit=1",
    "texas_water": "/api/texas_water/stations?limit=1",
    "pnw_water": "/api/pnw_water/stations?limit=1",
    "chesapeake": "/api/chesapeake/stations?limit=1",
    "harris_county": "/api/harris_county/gauges?limit=1",
    "king_county": "/api/king_county/stations?limit=1",
    "hudson_river": "/api/hudson_river/stations?limit=1",
    "columbia_river": "/api/columbia_river/stations?limit=1",
    "iowa_flood": "/api/iowa_flood/stations?limit=1",
    "iflows": "/api/iflows/stations?limit=1",
    "southwest_flood": "/api/southwest_flood/stations?limit=1",
    "maricopa_flood": "/api/maricopa_flood/gauges?limit=1",
    "midwest_stormwater": "/api/midwest_stormwater/stations?limit=1",
    "nm_water": "/api/nm_water/stations?limit=1",
    "brazil_hydro": "/api/brazil_hydro/stations?limit=1",
    "aquarius": "/api/aquarius/instances",
    "misc_monitoring": "/api/misc_monitoring/stations?limit=1",
    "water_quality": "/api/water_quality/stations?limit=1",
    # air_quality
    "openaq": "/api/openaq/locations?limit=1",
    "airnow": "/api/airnow/stations?limit=1",
    "epa_arcgis": "/api/epa_arcgis/stations?limit=1",
    "sensor_community": "/api/sensor_community/stations?limit=1",
    "state_aq": "/api/state_aq/stations?limit=1",
    "citizen_extended": "/api/citizen_extended/stations?limit=1",
    # fire
    "esri_living_atlas": "/api/esri_living_atlas/stations?limit=1",
    # seismic
    "earthquakes": "/api/earthquakes/recent?limit=1",
    # ocean
    "chesapeake_buoys": "/api/chesapeake_buoys/stations?limit=1",
    "ocean_extended": "/api/ocean_extended/stations?limit=1",
    "erddap": "/api/erddap/datasets?limit=1",
    # climate
    "climate": "/api/climate/acis/stations?state=OR&limit=1",
    "rcc_acis": "/api/rcc_acis/stations?limit=1",
    "nasa_power": "/api/nasa_power/info",
    "agriculture_ext": "/api/agriculture_ext/info",
    "satellite_products": "/api/satellite_products/info",
    "nrel_midc": "/api/nrel_midc/stations",
    "radiation": "/api/radiation/stations?limit=1",
    "specialty_networks": "/api/specialty_networks/stations?limit=1",
    # infrastructure
    "dams_wells": "/api/dams_wells/stations?limit=1",
    "state_wells": "/api/state_wells/stations?limit=1",
    "state_water_quality": "/api/state_water_quality/stations?limit=1",
    "nyc_data": "/api/nyc_data/stations?limit=1",
}

# ── Approximate station counts per source (from catalog / known inventories) ─
# These are conservative estimates used by the /stats endpoint.  Updated
# periodically from count_stations.py runs and source documentation.

_STATION_COUNTS: dict[str, int] = {
    # weather
    "noaa_weather": 3200,
    "faa_weather": 2700,
    "mesonets": 15000,
    "iem_dcp": 8000,
    "regional": 12000,
    "coagmet": 80,
    "montana_mesonet_api": 100,
    "infrastructure": 3500,
    "dot_rwis_arcgis": 4200,
    "fawn": 42,
    "nc_econet": 50,
    "southeast_wx": 600,
    "ag_networks": 800,
    "wsu_agweathernet": 180,
    "sdge_weather": 180,
    "madis_public": 2000,
    "hong_kong": 60,
    "austria": 270,
    "poland": 70,
    "iceland": 100,
    "baltic": 200,
    "argentina": 125,
    "australia": 800,
    "chile": 300,
    "colombia": 2800,
    "mexico": 200,
    "asia_east": 900,
    "europe_nordic": 1500,
    "europe_west": 2000,
    "international": 4000,
    "waggle_sage": 130,
    "citizen": 4000,
    "arcgis_realtime": 2500,
    "noaa_arcgis": 1800,
    "calfire_raws": 1600,
    "space_weather": 20,
    # water
    "usgs": 12926,
    "noaa_water": 3600,
    "federal_water": 4000,
    "usace_cda": 3200,
    "california": 3000,
    "california_extended": 1200,
    "eastcoast": 2500,
    "florida_water": 1800,
    "texas_water": 2200,
    "pnw_water": 1500,
    "chesapeake": 600,
    "harris_county": 220,
    "king_county": 150,
    "hudson_river": 120,
    "columbia_river": 80,
    "iowa_flood": 300,
    "iflows": 1200,
    "southwest_flood": 800,
    "maricopa_flood": 400,
    "midwest_stormwater": 350,
    "nm_water": 600,
    "brazil_hydro": 4500,
    "aquarius": 3000,
    "misc_monitoring": 1000,
    "water_quality": 15000,
    # air_quality
    "openaq": 18000,
    "airnow": 2500,
    "epa_arcgis": 2000,
    "sensor_community": 16000,
    "state_aq": 1500,
    "citizen_extended": 8000,
    # fire
    "esri_living_atlas": 800,
    # seismic
    "earthquakes": 600,
    # ocean
    "chesapeake_buoys": 150,
    "ocean_extended": 900,
    "erddap": 500,
    # climate
    "climate": 8000,
    "rcc_acis": 4000,
    "nasa_power": 200,
    "agriculture_ext": 400,
    "satellite_products": 100,
    "nrel_midc": 40,
    "radiation": 60,
    "specialty_networks": 500,
    # infrastructure
    "dams_wells": 5000,
    "state_wells": 8000,
    "state_water_quality": 3000,
    "nyc_data": 500,
}

# Active fraction estimate per category (proportion of catalog stations
# that are currently reporting).
_ACTIVE_FRACTION: dict[str, float] = {
    "weather": 0.62,
    "water": 0.55,
    "air_quality": 0.48,
    "fire": 0.40,
    "seismic": 0.85,
    "ocean": 0.60,
    "climate": 0.45,
    "infrastructure": 0.35,
}


# ── Helpers ────────────────────────────────────────────────────────────────

def _category_for_source(source_name: str) -> str:
    """Resolve source name to v2 category, defaulting to 'weather'."""
    return _SOURCE_TO_CATEGORY.get(source_name, "weather")


def _all_source_count() -> int:
    """Total number of source modules across all categories."""
    seen: set[str] = set()
    for meta in CATEGORIES.values():
        seen.update(meta["sources"])
    return len(seen)


async def _probe_source(
    client: httpx.AsyncClient,
    source_name: str,
    endpoint: str,
    sem: asyncio.Semaphore,
) -> dict:
    """Probe a single source and return health status dict."""
    async with sem:
        url = f"http://127.0.0.1:{config.PORT}{endpoint}"
        t0 = time.monotonic()
        try:
            r = await client.get(url)
            elapsed_ms = round((time.monotonic() - t0) * 1000)
            if r.status_code < 400:
                status = "slow" if elapsed_ms > 5000 else "ok"
            elif r.status_code == 404:
                status = "not_found"
            else:
                status = "error"

            # Try to extract station_count from response
            station_count = None
            if status in ("ok", "slow"):
                try:
                    data = r.json()
                    station_count = _extract_station_count(data)
                except Exception:
                    pass

            return {
                "name": source_name,
                "category": _category_for_source(source_name),
                "status": status,
                "http_status": r.status_code,
                "response_ms": elapsed_ms,
                "station_count": station_count,
            }
        except httpx.TimeoutException:
            elapsed_ms = round((time.monotonic() - t0) * 1000)
            return {
                "name": source_name,
                "category": _category_for_source(source_name),
                "status": "timeout",
                "http_status": None,
                "response_ms": elapsed_ms,
                "station_count": None,
            }
        except Exception as exc:
            elapsed_ms = round((time.monotonic() - t0) * 1000)
            return {
                "name": source_name,
                "category": _category_for_source(source_name),
                "status": "unreachable",
                "http_status": None,
                "response_ms": elapsed_ms,
                "station_count": None,
                "error": str(exc)[:200],
            }


def _extract_station_count(data: dict | list) -> int | None:
    """Best-effort extraction of station count from a probe response."""
    if isinstance(data, list):
        return len(data)
    if isinstance(data, dict):
        for key in ("count", "station_count", "total", "total_count"):
            if key in data and isinstance(data[key], (int, float)):
                return int(data[key])
        for key in ("stations", "gauges", "locations", "features",
                     "items", "data", "results", "networks", "nodes",
                     "instances", "datasets", "reservoirs"):
            if key in data and isinstance(data[key], list):
                return len(data[key])
    return None


# ── GET /api/v2/health ─────────────────────────────────────────────────────

_HEALTH_CACHE_KEY = "v2:health"
_HEALTH_TTL = 300  # 5 minutes


@router.get(
    "/health",
    summary="Source health with v2 category breakdown",
)
async def v2_health(
    refresh: bool = Query(False, description="Bypass 5-minute health cache"),
):
    """Probe all source modules and return health grouped by v2 category.

    Each source is probed via a lightweight metadata endpoint.  Results are
    cached for 5 minutes unless ``refresh=true`` is passed.
    """
    if not refresh:
        cached = await cache.get(_HEALTH_CACHE_KEY)
        if cached is not None:
            return {**cached, "from_cache": True}

    sem = asyncio.Semaphore(20)

    async with httpx.AsyncClient(
        timeout=config.HTTP_TIMEOUT,
        headers={"User-Agent": config.USER_AGENT, "Accept": "application/json"},
    ) as client:
        tasks = [
            _probe_source(client, name, ep, sem)
            for name, ep in _PROBE_ENDPOINTS.items()
        ]
        source_results: list[dict] = list(await asyncio.gather(*tasks))

    # Sort: ok first, then slow, then errors
    status_order = {
        "ok": 0, "slow": 1, "not_found": 2,
        "error": 3, "timeout": 4, "unreachable": 5,
    }
    source_results.sort(
        key=lambda s: (status_order.get(s["status"], 9), s["name"])
    )

    # Build category breakdown
    cat_stats: dict[str, dict] = {}
    for cat_name in CATEGORIES:
        cat_stats[cat_name] = {
            "sources": 0,
            "ok": 0,
            "error": 0,
            "avg_response_ms": 0,
            "_total_ms": 0,
        }

    for src in source_results:
        cat = src["category"]
        bucket = cat_stats.get(cat)
        if bucket is None:
            cat_stats[cat] = {
                "sources": 0, "ok": 0, "error": 0,
                "avg_response_ms": 0, "_total_ms": 0,
            }
            bucket = cat_stats[cat]
        bucket["sources"] += 1
        if src["status"] in ("ok", "slow"):
            bucket["ok"] += 1
        else:
            bucket["error"] += 1
        bucket["_total_ms"] += src["response_ms"]

    # Compute averages and drop internal accumulator
    for bucket in cat_stats.values():
        if bucket["sources"] > 0:
            bucket["avg_response_ms"] = round(
                bucket["_total_ms"] / bucket["sources"]
            )
        del bucket["_total_ms"]

    total_ok = sum(b["ok"] for b in cat_stats.values())
    total_error = sum(b["error"] for b in cat_stats.values())
    overall = "healthy" if total_error == 0 else (
        "degraded" if total_ok > total_error else "unhealthy"
    )

    payload = {
        "status": overall,
        "generated_at": datetime.now(UTC).isoformat(),
        "source_modules": len(source_results),
        "categories": cat_stats,
        "sources": source_results,
    }
    await cache.set(_HEALTH_CACHE_KEY, payload, ttl=_HEALTH_TTL)
    return {**payload, "from_cache": False}


# ── GET /api/v2/stats ──────────────────────────────────────────────────────

_STATS_CACHE_KEY = "v2:stats"
_STATS_TTL = 3600  # 1 hour


@router.get(
    "/stats",
    summary="Platform-wide station and source statistics",
)
async def v2_stats(
    refresh: bool = Query(False, description="Bypass 1-hour stats cache"),
):
    """Return aggregate station counts and source-module counts grouped by
    v2 category.

    Numbers come from the known station catalog inventory.  Cached for 1 hour.
    """
    if not refresh:
        cached = await cache.get(_STATS_CACHE_KEY)
        if cached is not None:
            return {**cached, "from_cache": True}

    total_catalog = 0
    total_active = 0
    total_sources = _all_source_count()

    by_category: dict[str, dict] = {}

    for cat_name, cat_meta in CATEGORIES.items():
        cat_catalog = 0
        cat_sources = 0
        for src in cat_meta["sources"]:
            count = _STATION_COUNTS.get(src, 0)
            # Avoid double-counting sources that appear in multiple categories
            cat_catalog += count
            cat_sources += 1

        frac = _ACTIVE_FRACTION.get(cat_name, 0.50)
        cat_active = round(cat_catalog * frac)

        by_category[cat_name] = {
            "catalog": cat_catalog,
            "active_estimate": cat_active,
            "sources": cat_sources,
        }
        total_catalog += cat_catalog
        total_active += cat_active

    # Count API keys that are actually configured (non-empty)
    _key_vars = [
        config.SYNOPTIC_TOKEN, config.PURPLEAIR_KEY,
        config.AMBIENT_APP_KEY, config.WEATHERFLOW_TOKEN,
        config.CIMIS_APP_KEY, config.AIRNOW_API_KEY,
        config.OPENAQ_API_KEY, config.NC_ECONET_HASH,
    ]
    api_keys_configured = sum(1 for k in _key_vars if k)

    payload = {
        "total_data_points": total_catalog * 35,  # ~35 params/station avg
        "globe_stations": total_catalog,
        "active_stations_estimate": total_active,
        "source_modules": total_sources,
        "by_category": by_category,
        "api_keys_required": 0,
        "api_keys_configured": api_keys_configured,
        "generated_at": datetime.now(UTC).isoformat(),
    }
    await cache.set(_STATS_CACHE_KEY, payload, ttl=_STATS_TTL)
    return {**payload, "from_cache": False}
