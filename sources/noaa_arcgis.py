"""NOAA ArcGIS Services — NWS observations, river gauges, warnings, coastal stations.

Five no-auth ArcGIS REST endpoints from NOAA MapServer / FeatureServer:

1. NWS Surface Observations (4,474 METAR stations)
   Real-time METAR data: temperature, dewpoint, wind, pressure, visibility,
   cloud cover.  Updated every 5 minutes.
   MapServer layer 60 at mapservices.weather.noaa.gov.

2. NOAA AHPS River Gauges (12,521 gauges)
   Observed river/stream levels with NWS flood stage thresholds (action,
   flood, moderate, major).  Includes gauge status color coding.
   MapServer layer 0 at the eventdriven water service.

3. NWS Watches/Warnings/Advisories
   Active WWA polygons with event type, severity, urgency, certainty,
   effective/expiration times, and affected areas.
   MapServer layer 1 at the eventdriven WWA service.

4. NOAA CO-OPS Coastal Stations (10 layers)
   Station metadata for water level, currents, met, and other coastal
   observation platforms operated by CO-OPS.
   FeatureServer layer 0 at the NOS_Observations static service.

5. NWS Flood Inundation Maps (301 pre-computed flood grids)
   Available via the same eventdriven water MapServer (layer listing).

All APIs are free with no authentication required.
"""
from __future__ import annotations

import logging
from typing import Optional

from fastapi import APIRouter, HTTPException, Query

import cache
import config
import http_client
from models import Station

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Source registry metadata  (read by server.py /api/sources)
# ---------------------------------------------------------------------------

SOURCE_NAME = "NOAA ArcGIS Services"
SOURCE_DESCRIPTION = (
    "NOAA ArcGIS REST endpoints for NWS surface observations (4,474 METAR "
    "stations), AHPS river gauges (12,521), watches/warnings/advisories, "
    "CO-OPS coastal stations, and flood inundation maps.  All no-auth."
)
SOURCE_META = {
    "url": "https://mapservices.weather.noaa.gov/",
    "auth_required": False,
    "update_frequency": "5 min (METAR), 15 min (river gauges), event-driven (WWA)",
    "coverage": "United States + territories",
    "params": [
        "temperature", "dewpoint", "wind_speed", "wind_direction", "wind_gust",
        "pressure", "visibility", "cloud_cover",
        "river_stage", "flood_status",
        "watches", "warnings", "advisories",
        "water_level", "currents",
    ],
    "category": "weather",
}

router = APIRouter(prefix="/api/noaa_arcgis", tags=["NOAA ArcGIS Services"])

# ---------------------------------------------------------------------------
# Constants — ArcGIS REST base URLs
# ---------------------------------------------------------------------------

# NWS Surface Observations (METAR)
SURFACE_OBS_URL = (
    "https://mapservices.weather.noaa.gov/vector/rest/services"
    "/obs/surface_obs/MapServer/60/query"
)

# AHPS River Gauges
RIVER_GAUGES_URL = (
    "https://mapservices.weather.noaa.gov/eventdriven/rest/services"
    "/water/riv_gauges/MapServer/0/query"
)

# Watches/Warnings/Advisories
WWA_URL = (
    "https://mapservices.weather.noaa.gov/eventdriven/rest/services"
    "/WWA/watch_warn_adv/MapServer/1/query"
)

# CO-OPS Coastal Stations
COOPS_STATIONS_URL = (
    "https://mapservices.weather.noaa.gov/static/rest/services"
    "/NOS_Observations/CO_OPS_Stations/FeatureServer/0/query"
)

# Flood Inundation MapServer root (for layer listing)
FLOOD_MAPSERVER_URL = (
    "https://mapservices.weather.noaa.gov/eventdriven/rest/services"
    "/water/riv_gauges/MapServer"
)

# ArcGIS pagination page size — must match the upstream maxRecordCount
# (NOAA MapServers return at most 2000 per page).
_ARCGIS_PAGE_SIZE = 2000

# Fields of interest for each service
_SURFACE_OBS_FIELDS = (
    "stationname,locationname,elevation,"
    "temperature,dewpoint,winddir,windspeed,windgust,"
    "sealevelpress,visibility,preschange,sst,waveheight,"
    "cloudcover,cloudbase,rawdata,timeobs"
)

_RIVER_GAUGE_FIELDS = (
    "gaugelid,location,waterbody,state,county,"
    "latitude,longitude,status,observed,units,obstime,"
    "action,flood,moderate,major"
)

_WWA_FIELDS = (
    "prod_type,event,wfo,significance,urgency,severity,certainty,"
    "cap_id,url,effective,expires,headline,description,areadesc"
)

_COOPS_FIELDS = "*"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

async def _arcgis_query(
    url: str,
    *,
    where: str = "1=1",
    out_fields: str = "*",
    limit: int = 2000,
    extra_params: Optional[dict] = None,
    timeout: float = 45.0,
    paginate: bool = False,
) -> list[dict]:
    """Execute an ArcGIS REST query and return the features list.

    Handles the standard ArcGIS JSON envelope: ``{"features": [{"attributes": {...}, "geometry": {...}}, ...]}``.
    Returns a flat list of dicts merging attributes + geometry coords.

    If *paginate* is True and the server returns exactly *page_size* records,
    automatically fetches subsequent pages using ``resultOffset`` until all
    records are retrieved or *limit* is reached.
    """
    page_size = min(limit, _ARCGIS_PAGE_SIZE)
    results: list[dict] = []
    offset = 0

    while True:
        params: dict = {
            "where": where,
            "outFields": out_fields,
            "resultRecordCount": str(page_size),
            "f": "json",
            "returnGeometry": "true",
        }
        if offset > 0:
            params["resultOffset"] = str(offset)
        if extra_params:
            params.update(extra_params)

        data = await http_client.fetch_json(
            url, params=params, timeout=timeout, source="noaa_arcgis",
        )

        # ArcGIS error handling
        if "error" in data:
            err = data["error"]
            raise HTTPException(
                status_code=502,
                detail=f"ArcGIS upstream error {err.get('code', '?')}: {err.get('message', str(err))}",
            )

        features = data.get("features", [])
        for feat in features:
            rec = dict(feat.get("attributes", {}))
            geom = feat.get("geometry")
            if geom:
                rec["longitude"] = geom.get("x")
                rec["latitude"] = geom.get("y")
            results.append(rec)

        # Paginate only if requested and server returned a full page
        if not paginate or len(features) < page_size or len(results) >= limit:
            break
        offset += len(features)

    return results[:limit] if limit else results


def _epoch_to_iso(val) -> Optional[str]:
    """Convert ArcGIS epoch-ms timestamp to ISO-8601 string, or return None."""
    if val is None:
        return None
    try:
        from datetime import datetime, timezone
        return datetime.fromtimestamp(int(val) / 1000, tz=timezone.utc).isoformat()
    except (ValueError, TypeError, OSError):
        return None


# ===================================================================
# Endpoint 1: NWS Surface Observations (METAR)
# ===================================================================

@router.get("/surface_obs", summary="NWS METAR surface observations")
async def surface_obs(
    limit: int = Query(5000, ge=1, le=10000, description="Max records to return"),
):
    """Return real-time METAR observations from ~4,380 NWS stations.

    Fields include temperature, dewpoint, wind, pressure, visibility, cloud
    cover, raw METAR text, and observation time.  Paginates automatically
    through the upstream 2000-record-per-page limit.
    """
    cache_key = f"noaa_arcgis:surface_obs:{limit}"
    cached = await cache.get(cache_key)
    if cached is not None:
        return cached

    features = await _arcgis_query(
        SURFACE_OBS_URL,
        out_fields=_SURFACE_OBS_FIELDS,
        limit=limit,
        paginate=True,
    )

    stations = []
    for rec in features:
        obs_time = _epoch_to_iso(rec.get("timeobs"))
        stations.append({
            "id": rec.get("stationname"),
            "name": rec.get("locationname") or rec.get("stationname"),
            "lat": rec.get("latitude"),
            "lon": rec.get("longitude"),
            "source": "noaa_arcgis",
            "network": "METAR",
            "elevation_ft": rec.get("elevation"),
            "temperature_f": rec.get("temperature"),
            "dewpoint_f": rec.get("dewpoint"),
            "wind_direction_deg": rec.get("winddir"),
            "wind_speed_kt": rec.get("windspeed"),
            "wind_gust_kt": rec.get("windgust"),
            "sea_level_pressure_mb": rec.get("sealevelpress"),
            "visibility_mi": rec.get("visibility"),
            "pressure_change_mb": rec.get("preschange"),
            "sst_f": rec.get("sst"),
            "wave_height_ft": rec.get("waveheight"),
            "cloud_cover": rec.get("cloudcover"),
            "cloud_base_ft": rec.get("cloudbase"),
            "raw_metar": rec.get("rawdata"),
            "last_update": obs_time,
            "params": ["temperature", "dewpoint", "wind_speed", "wind_direction",
                       "pressure", "visibility", "cloud_cover", "raw_metar"],
        })

    result = {
        "source": "NWS Surface Observations (ArcGIS)",
        "station_count": len(stations),
        "stations": stations,
    }

    await cache.set(cache_key, result, ttl=config.CACHE_TTL)
    return result


# ===================================================================
# Endpoint 2: AHPS River Gauges
# ===================================================================

@router.get("/river_gauges", summary="AHPS river gauge observations")
async def river_gauges(
    state: Optional[str] = Query(None, min_length=2, max_length=2, description="Two-letter state code (e.g. OR)"),
    limit: int = Query(2000, ge=1, le=5000, description="Max records to return"),
):
    """Return observed river/stream levels from NOAA AHPS gauges.

    Includes flood stage thresholds (action, flood, moderate, major) and
    current gauge status.  Optionally filter by state.
    """
    where = "1=1"
    if state:
        where = f"state='{state.upper()}'"

    cache_key = f"noaa_arcgis:river_gauges:{state}:{limit}"
    cached = await cache.get(cache_key)
    if cached is not None:
        return cached

    features = await _arcgis_query(
        RIVER_GAUGES_URL,
        where=where,
        out_fields=_RIVER_GAUGE_FIELDS,
        limit=limit,
    )

    gauges = []
    for rec in features:
        gauges.append({
            "gauge_id": rec.get("gaugelid"),
            "location": rec.get("location"),
            "waterbody": rec.get("waterbody"),
            "state": rec.get("state"),
            "county": rec.get("county"),
            "latitude": rec.get("latitude"),
            "longitude": rec.get("longitude"),
            "status": rec.get("status"),
            "observed_level": rec.get("observed"),
            "units": rec.get("units"),
            "obs_time": _epoch_to_iso(rec.get("obstime")),
            "thresholds": {
                "action": rec.get("action"),
                "flood": rec.get("flood"),
                "moderate": rec.get("moderate"),
                "major": rec.get("major"),
            },
        })

    result = {
        "source": "NOAA AHPS River Gauges (ArcGIS)",
        "gauge_count": len(gauges),
        "state_filter": state.upper() if state else None,
        "gauges": gauges,
    }

    await cache.set(cache_key, result, ttl=config.CACHE_TTL)
    return result


# ===================================================================
# Endpoint 3: Watches / Warnings / Advisories
# ===================================================================

@router.get("/warnings", summary="Active NWS watches, warnings, and advisories")
async def warnings(
    state: Optional[str] = Query(None, min_length=2, max_length=2, description="Two-letter state code (e.g. OR)"),
):
    """Return active NWS watches, warnings, and advisories.

    Optionally filter by state (matches against the area description field).
    """
    where = "1=1"
    # WWA layer doesn't have a clean 'state' field — filter on areadesc
    if state:
        where = f"areadesc LIKE '%{state.upper()}%'"

    cache_key = f"noaa_arcgis:warnings:{state}"
    cached = await cache.get(cache_key)
    if cached is not None:
        return cached

    features = await _arcgis_query(
        WWA_URL,
        where=where,
        out_fields=_WWA_FIELDS,
        limit=2000,
    )

    alerts = []
    for rec in features:
        alerts.append({
            "product_type": rec.get("prod_type"),
            "event": rec.get("event"),
            "wfo": rec.get("wfo"),
            "significance": rec.get("significance"),
            "urgency": rec.get("urgency"),
            "severity": rec.get("severity"),
            "certainty": rec.get("certainty"),
            "cap_id": rec.get("cap_id"),
            "url": rec.get("url"),
            "effective": _epoch_to_iso(rec.get("effective")),
            "expires": _epoch_to_iso(rec.get("expires")),
            "headline": rec.get("headline"),
            "description": rec.get("description"),
            "area_description": rec.get("areadesc"),
        })

    result = {
        "source": "NWS Watches/Warnings/Advisories (ArcGIS)",
        "alert_count": len(alerts),
        "state_filter": state.upper() if state else None,
        "alerts": alerts,
    }

    await cache.set(cache_key, result, ttl=config.CACHE_TTL)
    return result


# ===================================================================
# Endpoint 4: CO-OPS Coastal Stations
# ===================================================================

@router.get("/coastal_stations", summary="CO-OPS coastal station metadata")
async def coastal_stations():
    """Return NOAA CO-OPS coastal observation station metadata.

    Includes station locations and capabilities across 10 data layers
    (water level, currents, meteorological, etc.).
    """
    cache_key = "noaa_arcgis:coastal_stations"
    cached = await cache.get(cache_key)
    if cached is not None:
        return cached

    features = await _arcgis_query(
        COOPS_STATIONS_URL,
        out_fields=_COOPS_FIELDS,
        limit=5000,
    )

    stations = []
    for rec in features:
        stations.append({
            "station_id": rec.get("stationId") or rec.get("STATION_ID") or rec.get("station_id"),
            "station_name": rec.get("stationName") or rec.get("STATION_NAME") or rec.get("station_name"),
            "latitude": rec.get("latitude") or rec.get("LATITUDE"),
            "longitude": rec.get("longitude") or rec.get("LONGITUDE"),
            "state": rec.get("state") or rec.get("STATE"),
            "attributes": {
                k: v for k, v in rec.items()
                if k not in ("latitude", "longitude", "LATITUDE", "LONGITUDE")
                and v is not None
            },
        })

    result = {
        "source": "NOAA CO-OPS Coastal Stations (ArcGIS)",
        "station_count": len(stations),
        "stations": stations,
    }

    await cache.set(cache_key, result, ttl=config.CACHE_TTL_LONG)
    return result


# ===================================================================
# Endpoint 5: Source overview
# ===================================================================

@router.get("/overview", summary="NOAA ArcGIS Services source overview")
async def overview():
    """Return a descriptive summary of all NOAA ArcGIS services available
    through this router, including endpoints, coverage, and parameters."""
    return {
        "router": "noaa_arcgis",
        "description": SOURCE_DESCRIPTION,
        "networks": [
            {
                "id": "nws_surface_obs",
                "name": "NWS Surface Observations (METAR)",
                "url": "https://mapservices.weather.noaa.gov/vector/rest/services/obs/surface_obs/MapServer",
                "auth_required": False,
                "coverage": "United States (4,474 METAR stations)",
                "update_frequency": "5 minutes",
                "parameters": [
                    "temperature", "dewpoint", "relative_humidity",
                    "wind_direction", "wind_speed", "wind_gust",
                    "sea_level_pressure", "visibility", "cloud_cover",
                    "weather", "raw_metar",
                ],
                "endpoints": [
                    {
                        "path": "/api/noaa_arcgis/surface_obs",
                        "method": "GET",
                        "description": "Real-time METAR observations",
                        "params": {"limit": "Max records (default 2000, max 5000)"},
                    },
                ],
            },
            {
                "id": "ahps_river_gauges",
                "name": "NOAA AHPS River Gauges",
                "url": "https://mapservices.weather.noaa.gov/eventdriven/rest/services/water/riv_gauges/MapServer",
                "auth_required": False,
                "coverage": "United States (12,521 gauges)",
                "update_frequency": "15 minutes",
                "parameters": [
                    "observed_level", "status",
                    "action_stage", "flood_stage", "moderate_stage", "major_stage",
                ],
                "endpoints": [
                    {
                        "path": "/api/noaa_arcgis/river_gauges",
                        "method": "GET",
                        "description": "River/stream gauge observations with flood thresholds",
                        "params": {
                            "state": "Two-letter state code filter (optional)",
                            "limit": "Max records (default 2000, max 5000)",
                        },
                    },
                ],
            },
            {
                "id": "wwa",
                "name": "NWS Watches/Warnings/Advisories",
                "url": "https://mapservices.weather.noaa.gov/eventdriven/rest/services/WWA/watch_warn_adv/MapServer",
                "auth_required": False,
                "coverage": "United States + territories",
                "update_frequency": "Event-driven (real-time)",
                "parameters": [
                    "event", "severity", "urgency", "certainty",
                    "headline", "description", "effective", "expires",
                ],
                "endpoints": [
                    {
                        "path": "/api/noaa_arcgis/warnings",
                        "method": "GET",
                        "description": "Active watches, warnings, and advisories",
                        "params": {"state": "Two-letter state code filter (optional)"},
                    },
                ],
            },
            {
                "id": "coops_stations",
                "name": "NOAA CO-OPS Coastal Stations",
                "url": "https://mapservices.weather.noaa.gov/static/rest/services/NOS_Observations/CO_OPS_Stations/FeatureServer",
                "auth_required": False,
                "coverage": "US coasts, Great Lakes, islands",
                "update_frequency": "Static metadata (stations list)",
                "parameters": [
                    "water_level", "currents", "meteorological",
                    "water_temperature", "conductivity",
                ],
                "note": (
                    "This endpoint returns station metadata.  For real-time CO-OPS "
                    "data (water levels, currents, etc.), use the /api/noaa_water "
                    "endpoints which query the CO-OPS data API directly."
                ),
                "endpoints": [
                    {
                        "path": "/api/noaa_arcgis/coastal_stations",
                        "method": "GET",
                        "description": "CO-OPS station metadata",
                    },
                ],
            },
            {
                "id": "flood_inundation",
                "name": "NWS Flood Inundation Maps",
                "url": FLOOD_MAPSERVER_URL,
                "auth_required": False,
                "coverage": "301 pre-computed flood inundation grids",
                "note": (
                    "Flood inundation map layers are available through the AHPS "
                    "river gauges MapServer.  Individual inundation grids can be "
                    "queried by gauge ID through the ArcGIS REST API directly."
                ),
            },
        ],
    }
