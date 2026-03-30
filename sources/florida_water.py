"""Florida Water Management Districts — SJRWMD, SWFWMD, SRWMD.

Covers the three Florida water management districts not already served by the
SFWMD endpoints in eastcoast.py:

1. SJRWMD (St. Johns River Water Management District)
   ~1,200 monitoring stations via ArcGIS Open Data Hub.
   API: https://data-floridaswater.opendata.arcgis.com/
   REST: https://services.arcgis.com/s8wtJX9suxFen6TA/arcgis/rest/services/

2. SWFWMD (Southwest Florida Water Management District)
   ~800 real-time water level sites via ArcGIS SCADA.
   API: https://www25.swfwmd.state.fl.us/arcgis10/rest/services/Hydrologic/DCS_NRT_SCADA_EDP/MapServer/0/query

3. SRWMD (Suwannee River Water Management District)
   207 groundwater wells + 46 surface water + 42 rain gauge stations.
   API: https://gis.srwmd.state.fl.us/arcgis/rest/services/

All APIs are free with no authentication required.
"""
import asyncio
import logging
from typing import Optional

import httpx
from fastapi import APIRouter, HTTPException, Query

import cache
import config
from models import Station

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Source registry metadata
# ---------------------------------------------------------------------------
SOURCE_NAME = "Florida Water Management Districts"
SOURCE_DESCRIPTION = (
    "St. Johns River (SJRWMD), Southwest Florida (SWFWMD), and Suwannee River "
    "(SRWMD) Water Management District monitoring networks — groundwater wells, "
    "surface water gauges, rain gauges, and real-time SCADA water levels."
)
SOURCE_META = {
    "url": "https://data-floridaswater.opendata.arcgis.com/",
    "auth_required": False,
    "update_frequency": "15 min – 1 hr",
    "coverage": "Central and north Florida",
    "params": ["water_level", "groundwater", "precipitation", "surface_water"],
    "category": "water",
}

router = APIRouter(prefix="/api/florida_water", tags=["Florida Water Management Districts"])

# ---------------------------------------------------------------------------
# API base URLs
# ---------------------------------------------------------------------------

# SJRWMD — ArcGIS Open Data Hub
SJRWMD_BASE = "https://services.arcgis.com/s8wtJX9suxFen6TA/arcgis/rest/services"

# SWFWMD — Real-time SCADA water levels
SWFWMD_SCADA_URL = (
    "https://www25.swfwmd.state.fl.us/arcgis10/rest/services"
    "/Hydrologic/DCS_NRT_SCADA_EDP/MapServer/0/query"
)

# SRWMD — ArcGIS services
SRWMD_BASE = "https://gis.srwmd.state.fl.us/arcgis/rest/services"

# ---------------------------------------------------------------------------
# HTTP client helpers
# ---------------------------------------------------------------------------
_HEADERS = {
    "User-Agent": config.USER_AGENT,
    "Accept": "application/json",
}


def _client() -> httpx.AsyncClient:
    return httpx.AsyncClient(
        headers=_HEADERS,
        timeout=config.HTTP_TIMEOUT,
        follow_redirects=True,
    )


# ---------------------------------------------------------------------------
# Shared ArcGIS helpers
# ---------------------------------------------------------------------------

def _arcgis_query_params(
    where: str = "1=1",
    out_fields: str = "*",
    result_offset: int = 0,
    result_record_count: int = 1000,
) -> dict:
    """Build standard ArcGIS query parameters."""
    return {
        "where": where,
        "outFields": out_fields,
        "f": "json",
        "resultOffset": result_offset,
        "resultRecordCount": result_record_count,
        "returnGeometry": "true",
        "spatialRel": "esriSpatialRelIntersects",
    }


def _parse_arcgis_feature(
    feature: dict,
    source_key: str,
    network_label: str,
    url_template: str = "",
) -> Optional[Station]:
    """Convert an ArcGIS feature dict into a Station model.

    Handles the most common ESRI field naming conventions across all three
    Florida WMDs.
    """
    attrs = feature.get("attributes", {})
    geom = feature.get("geometry", {})

    # Station ID — try several common field names
    station_id = None
    for id_key in (
        "STATION_ID", "StationID", "Station_ID", "SITE_ID", "SiteID",
        "SITE", "STATION", "WELL_ID", "WellID", "DBKEY",
        "OBJECTID", "FID",
    ):
        val = attrs.get(id_key)
        if val is not None and str(val).strip():
            station_id = str(val).strip()
            break
    if station_id is None:
        return None

    # Station name
    name = None
    for name_key in (
        "STATION_NAME", "StationName", "Station_Name",
        "SITE_NAME", "SiteName", "Site_Name",
        "NAME", "WELL_NAME", "WellName",
        "DESCRIPTION", "DESC",
    ):
        val = attrs.get(name_key)
        if val and str(val).strip():
            name = str(val).strip()
            break
    name = name or station_id

    # Coordinates — ArcGIS geographic SR: x=lon, y=lat
    lat = 0.0
    lon = 0.0
    if geom:
        try:
            lon = float(geom.get("x", 0.0))
            lat = float(geom.get("y", 0.0))
        except (TypeError, ValueError):
            pass

    # Elevation
    elev: Optional[float] = None
    for elev_key in ("ELEVATION", "ELEV", "ALT", "Elevation"):
        val = attrs.get(elev_key)
        if val is not None:
            try:
                elev = float(val)
                break
            except (TypeError, ValueError):
                pass

    # Parameters hint based on common attribute patterns
    params: list[str] = []
    attrs_lower = {k.lower() for k in attrs}
    if any(k in attrs_lower for k in ("water_level", "waterlevel", "stage", "level")):
        params.append("water_level")
    if any(k in attrs_lower for k in ("rainfall", "rain", "precipitation")):
        params.append("precipitation")
    if any(k in attrs_lower for k in ("well", "gw_level", "groundwater")):
        params.append("groundwater")
    if not params:
        params = ["water_level"]

    # Extra metadata — keep all non-ID/name/geometry attributes
    _skip = {
        "STATION_ID", "StationID", "Station_ID", "SITE_ID", "SiteID",
        "SITE", "STATION", "WELL_ID", "WellID", "DBKEY", "OBJECTID", "FID",
        "STATION_NAME", "StationName", "Station_Name", "SITE_NAME", "SiteName",
        "Site_Name", "NAME", "WELL_NAME", "WellName", "DESCRIPTION", "DESC",
        "ELEVATION", "ELEV", "ALT", "Elevation",
    }
    extra = {k: v for k, v in attrs.items() if v is not None and k not in _skip}

    return Station(
        id=f"{source_key}_{station_id}",
        name=name,
        lat=lat,
        lon=lon,
        source="florida_water",
        network=network_label,
        params=params,
        elevation=elev,
        url=url_template or None,
        extra=extra,
    )


async def _fetch_arcgis_layer(
    layer_url: str,
    source_key: str,
    network_label: str,
    url_template: str = "",
    max_records: int = 10000,
) -> list[Station]:
    """Fetch all features from an ArcGIS query endpoint with pagination."""
    stations: list[Station] = []
    offset = 0

    try:
        async with _client() as client:
            while True:
                params = _arcgis_query_params(result_offset=offset)
                resp = await client.get(layer_url, params=params)
                resp.raise_for_status()
                data = resp.json()

                # ArcGIS may return an error object instead of features
                if "error" in data:
                    logger.warning(
                        "%s ArcGIS error: %s", source_key, data["error"].get("message", data["error"])
                    )
                    break

                features = data.get("features", [])
                if not features:
                    break

                for feat in features:
                    station = _parse_arcgis_feature(feat, source_key, network_label, url_template)
                    if station:
                        stations.append(station)

                # Check for pagination
                if not data.get("exceededTransferLimit", False):
                    break
                offset += len(features)

                if offset > max_records:
                    logger.warning(
                        "%s: pagination cap hit at %d records", source_key, offset
                    )
                    break

    except httpx.TimeoutException:
        logger.warning("%s fetch timed out at offset %d", source_key, offset)
    except httpx.HTTPStatusError as exc:
        logger.warning("%s HTTP error: %s", source_key, exc)
    except Exception:
        logger.exception("Unexpected error fetching %s", source_key)

    return stations


# ===========================================================================
# 1. SJRWMD — St. Johns River Water Management District
# ===========================================================================

# The SJRWMD Open Data Hub exposes multiple feature services.  We query the
# main monitoring stations layer.  The exact service name may vary; we try
# the most common ones.
SJRWMD_STATIONS_URL = (
    f"{SJRWMD_BASE}/SJRWMD_Monitoring_Stations/FeatureServer/0/query"
)


@router.get("/sjrwmd/stations", summary="SJRWMD monitoring stations")
async def sjrwmd_stations():
    """Return St. Johns River Water Management District monitoring stations.

    Queries the SJRWMD ArcGIS Open Data Hub.  ~1,200 monitoring stations
    covering groundwater wells, surface water gauges, and rainfall stations
    across central and northeast Florida.

    Source: https://data-floridaswater.opendata.arcgis.com/
    """
    cache_key = "florida_water:sjrwmd:stations"
    cached_result = await cache.get(cache_key)
    if cached_result is not None:
        return cached_result

    stations = await _fetch_arcgis_layer(
        layer_url=SJRWMD_STATIONS_URL,
        source_key="sjrwmd",
        network_label="SJRWMD",
        url_template="https://data-floridaswater.opendata.arcgis.com/",
    )

    result = {
        "source": "sjrwmd",
        "district": "St. Johns River Water Management District",
        "description": "Monitoring stations (groundwater, surface water, rainfall)",
        "count": len(stations),
        "stations": [s.model_dump() for s in stations],
    }
    await cache.set(cache_key, result, ttl=config.CACHE_TTL_LONG)
    return result


# ===========================================================================
# 2. SWFWMD — Southwest Florida Water Management District
# ===========================================================================

@router.get("/swfwmd/realtime", summary="SWFWMD real-time SCADA water levels")
async def swfwmd_realtime():
    """Return Southwest Florida WMD real-time SCADA water level sites.

    Queries the SWFWMD DCS_NRT_SCADA_EDP MapServer for ~800 real-time
    water level monitoring sites across southwest Florida.

    Source: https://www25.swfwmd.state.fl.us/arcgis10/rest/services/Hydrologic/
    """
    cache_key = "florida_water:swfwmd:realtime"
    cached_result = await cache.get(cache_key)
    if cached_result is not None:
        return cached_result

    stations = await _fetch_arcgis_layer(
        layer_url=SWFWMD_SCADA_URL,
        source_key="swfwmd",
        network_label="SWFWMD SCADA",
        url_template="https://www25.swfwmd.state.fl.us/",
    )

    result = {
        "source": "swfwmd",
        "district": "Southwest Florida Water Management District",
        "description": "Real-time SCADA water level monitoring sites",
        "count": len(stations),
        "stations": [s.model_dump() for s in stations],
    }
    await cache.set(cache_key, result, ttl=config.CACHE_TTL_LONG)
    return result


# ===========================================================================
# 3. SRWMD — Suwannee River Water Management District
# ===========================================================================

# SRWMD exposes several layers.  We query the main monitoring wells + surface
# water stations.
SRWMD_WELLS_URL = f"{SRWMD_BASE}/Hydrology/MonitoringWells/MapServer/0/query"
SRWMD_SURFACE_URL = f"{SRWMD_BASE}/Hydrology/SurfaceWaterStations/MapServer/0/query"
SRWMD_RAIN_URL = f"{SRWMD_BASE}/Hydrology/RainGauges/MapServer/0/query"

# SRWMD layer name → (url, network label, description)
_SRWMD_LAYERS = {
    "wells": (SRWMD_WELLS_URL, "SRWMD Wells", "Groundwater monitoring wells"),
    "surface": (SRWMD_SURFACE_URL, "SRWMD Surface Water", "Surface water stations"),
    "rain": (SRWMD_RAIN_URL, "SRWMD Rain Gauges", "Rainfall monitoring gauges"),
}


@router.get("/srwmd/stations", summary="SRWMD monitoring stations")
async def srwmd_stations(
    layer: str = Query(
        "wells",
        description="Which SRWMD layer: 'wells', 'surface', or 'rain'",
        pattern="^(wells|surface|rain)$",
    ),
):
    """Return Suwannee River Water Management District monitoring stations.

    Queries the SRWMD ArcGIS services.  Three layers are available:
      - **wells** — 207 groundwater monitoring wells
      - **surface** — 46 surface water stations
      - **rain** — 42 rain gauges

    Source: https://gis.srwmd.state.fl.us/arcgis/rest/services/
    """
    cache_key = f"florida_water:srwmd:stations:{layer}"
    cached_result = await cache.get(cache_key)
    if cached_result is not None:
        return cached_result

    layer_url, network_label, description = _SRWMD_LAYERS[layer]

    stations = await _fetch_arcgis_layer(
        layer_url=layer_url,
        source_key="srwmd",
        network_label=network_label,
        url_template="https://gis.srwmd.state.fl.us/arcgis/rest/services/",
    )

    result = {
        "source": "srwmd",
        "district": "Suwannee River Water Management District",
        "layer": layer,
        "description": description,
        "count": len(stations),
        "stations": [s.model_dump() for s in stations],
    }
    await cache.set(cache_key, result, ttl=config.CACHE_TTL_LONG)
    return result


# ===========================================================================
# 4. Overview — all three districts
# ===========================================================================

@router.get("/overview", summary="All Florida WMD districts summary")
async def overview():
    """Return a summary of all three Florida water management districts.

    Fetches station counts from SJRWMD, SWFWMD, and SRWMD in parallel and
    returns an overview with per-district metadata.
    """
    cache_key = "florida_water:overview"
    cached_result = await cache.get(cache_key)
    if cached_result is not None:
        return cached_result

    # Fetch all districts in parallel
    sjrwmd_task = _fetch_arcgis_layer(
        SJRWMD_STATIONS_URL, "sjrwmd", "SJRWMD",
    )
    swfwmd_task = _fetch_arcgis_layer(
        SWFWMD_SCADA_URL, "swfwmd", "SWFWMD SCADA",
    )
    # For SRWMD, fetch all three layers
    srwmd_wells_task = _fetch_arcgis_layer(
        SRWMD_WELLS_URL, "srwmd", "SRWMD Wells",
    )
    srwmd_surface_task = _fetch_arcgis_layer(
        SRWMD_SURFACE_URL, "srwmd", "SRWMD Surface Water",
    )
    srwmd_rain_task = _fetch_arcgis_layer(
        SRWMD_RAIN_URL, "srwmd", "SRWMD Rain Gauges",
    )

    sjrwmd, swfwmd, srwmd_wells, srwmd_surface, srwmd_rain = await asyncio.gather(
        sjrwmd_task, swfwmd_task, srwmd_wells_task, srwmd_surface_task, srwmd_rain_task,
    )

    result = {
        "source": "florida_water",
        "description": "Florida Water Management Districts (SJRWMD, SWFWMD, SRWMD)",
        "districts": [
            {
                "id": "sjrwmd",
                "name": "St. Johns River Water Management District",
                "station_count": len(sjrwmd),
                "url": "https://data-floridaswater.opendata.arcgis.com/",
                "coverage": "Central and northeast Florida",
            },
            {
                "id": "swfwmd",
                "name": "Southwest Florida Water Management District",
                "station_count": len(swfwmd),
                "url": "https://www25.swfwmd.state.fl.us/",
                "coverage": "Southwest Florida (Tampa Bay to Charlotte Harbor)",
            },
            {
                "id": "srwmd",
                "name": "Suwannee River Water Management District",
                "station_count": {
                    "wells": len(srwmd_wells),
                    "surface": len(srwmd_surface),
                    "rain": len(srwmd_rain),
                    "total": len(srwmd_wells) + len(srwmd_surface) + len(srwmd_rain),
                },
                "url": "https://gis.srwmd.state.fl.us/arcgis/rest/services/",
                "coverage": "North-central Florida (Suwannee River basin)",
            },
        ],
        "total_stations": len(sjrwmd) + len(swfwmd) + len(srwmd_wells) + len(srwmd_surface) + len(srwmd_rain),
    }
    await cache.set(cache_key, result, ttl=config.CACHE_TTL_LONG)
    return result
