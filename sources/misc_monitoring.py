"""Miscellaneous regional monitoring networks — ArcGIS Feature Services.

Six small-to-medium public ArcGIS services covering rainfall, groundwater,
soil moisture, stormwater, and air quality across the US and Australia.
All are unauthenticated (no API key required).

Datasets:
1. Lee County FL Rainfall          (203 stations, 15-min tipping-bucket)
2. Hampton Roads VA Rain Gauges    (74 gauges, HRSD public view)
3. NH Groundwater Monitoring       (41 wells, DES groundwater levels)
4. Missouri Soil Moisture          (37 SCAN sensors)
5. Salem OR Stormwater             (23 stormwater monitors)
6. Victoria EPA Air Quality        (Australia, 30-min latest readings)
"""
from __future__ import annotations

import logging
from typing import Any, Optional

from fastapi import APIRouter, HTTPException, Query

import cache
import config
import http_client

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Source registry metadata  (read by server.py /api/sources)
# ---------------------------------------------------------------------------

SOURCE_NAME = "Regional Monitoring (ArcGIS)"
SOURCE_DESCRIPTION = (
    "Miscellaneous regional monitoring networks served via public ArcGIS "
    "Feature Services — Lee County FL rainfall (203 stations), Hampton Roads "
    "VA rain gauges (74), NH groundwater wells (41), Missouri soil moisture "
    "sensors (37), Salem OR stormwater monitors (23), and Victoria Australia "
    "EPA air quality (30-min readings).  All services are free with no "
    "authentication required."
)
SOURCE_META = {
    "category": "monitoring",
    "coverage": "FL, VA, NH, MO, OR (US) + Victoria (Australia)",
    "auth_required": False,
    "update_frequency": "15 min – 1 hr (varies by network)",
    "params": [
        "rainfall", "rain_gauge", "groundwater_level",
        "soil_moisture", "stormwater", "air_quality",
    ],
}

# ---------------------------------------------------------------------------
# Router
# ---------------------------------------------------------------------------

router = APIRouter(prefix="/api/regional_arcgis", tags=["Regional Monitoring (ArcGIS)"])

# ---------------------------------------------------------------------------
# ArcGIS Feature Service URLs
# ---------------------------------------------------------------------------

_LEE_COUNTY_RAIN_URL = (
    "https://services2.arcgis.com/LvWGAAhHwbCJ2GMP/arcgis/rest/services/"
    "Rainfall_Summary_Live_Feed/FeatureServer/0/query"
)
_HAMPTON_ROADS_RAIN_URL = (
    "https://services.arcgis.com/UdWFYYQtp7XJUBJy/arcgis/rest/services/"
    "Raingauges_HRSDWebsite_Public_VW/FeatureServer/0/query"
)
_NH_GROUNDWATER_URL = (
    "https://services1.arcgis.com/MAcUimSes4gPY4sM/arcgis/rest/services/"
    "Groundwater_Level_Monitoring___Dashboard_WFL1/FeatureServer/0/query"
)
_MISSOURI_SOIL_URL = (
    "https://services5.arcgis.com/SGaxfv2yz6XahOw9/arcgis/rest/services/"
    "Missouri_Soil_Moisture_Sensors/FeatureServer/0/query"
)
_SALEM_STORMWATER_URL = (
    "https://services.arcgis.com/kIA6yS9KDGqZL7U3/arcgis/rest/services/"
    "Stormwater_Monitors/FeatureServer/0/query"
)
_VICTORIA_AQ_URL = (
    "https://services1.arcgis.com/vHnIGBHHqDR6y0CR/arcgis/rest/services/"
    "Air_Quality_Latest_Readings/FeatureServer/0/query"
)


# ---------------------------------------------------------------------------
# Shared ArcGIS query helper
# ---------------------------------------------------------------------------

async def _query_arcgis(
    url: str,
    *,
    where: str = "1=1",
    fields: str = "*",
    limit: int = 2000,
    source_label: str = "regional_arcgis",
) -> dict[str, Any]:
    """Execute an ArcGIS Feature Service query and return the JSON response.

    Parameters
    ----------
    url : str
        Full query endpoint URL for the ArcGIS Feature Service layer.
    where : str
        SQL WHERE clause for server-side filtering (default ``"1=1"``).
    fields : str
        Comma-separated field list, or ``"*"`` for all.
    limit : int
        Maximum number of features to request (``resultRecordCount``).
    source_label : str
        Label passed to ``http_client`` for circuit-breaker tracking.

    Returns
    -------
    dict
        Raw ArcGIS JSON response (contains ``features``, ``fields``, etc.).
    """
    params: dict[str, Any] = {
        "where": where,
        "outFields": fields,
        "f": "json",
        "resultRecordCount": limit,
    }

    try:
        data = await http_client.fetch_json(
            url, params=params, source=source_label,
        )
    except http_client.CircuitOpenError:
        raise HTTPException(
            status_code=503,
            detail=f"Regional ArcGIS circuit breaker open ({source_label}).",
        )
    except Exception as exc:
        logger.exception("Regional ArcGIS query failed: %s", url)
        raise HTTPException(
            status_code=502,
            detail=f"Regional ArcGIS upstream error: {exc}",
        )

    # ArcGIS services may return an error object instead of features
    if "error" in data:
        err = data["error"]
        msg = err.get("message", str(err))
        code = err.get("code", 502)
        raise HTTPException(
            status_code=502,
            detail=f"ArcGIS error ({code}): {msg}",
        )

    return data


def _extract_features(data: dict) -> list[dict]:
    """Pull the attributes list from an ArcGIS query response."""
    features = data.get("features", [])
    return [f.get("attributes", {}) for f in features]


def _extract_features_with_geometry(data: dict) -> list[dict]:
    """Pull features preserving both attributes and geometry."""
    out = []
    for f in data.get("features", []):
        rec = {**f.get("attributes", {})}
        geom = f.get("geometry")
        if geom:
            rec["geometry"] = geom
        out.append(rec)
    return out


# ---------------------------------------------------------------------------
# Endpoint 1: GET /api/regional_arcgis/lee_county_rain
# ---------------------------------------------------------------------------

@router.get("/lee_county_rain", summary="Lee County FL rainfall (203 stations)")
async def lee_county_rain(
    limit: int = Query(
        2000,
        description="Max records to return.",
        ge=1,
        le=5000,
    ),
):
    """Return 15-minute rainfall summary data from Lee County, FL.

    Covers ~203 tipping-bucket rain gauge stations across Lee County.
    """
    cache_key = f"regional_arcgis:lee_county_rain:{limit}"
    cached = await cache.get(cache_key)
    if cached is not None:
        return cached

    data = await _query_arcgis(
        _LEE_COUNTY_RAIN_URL,
        limit=limit,
        source_label="regional_arcgis_lee_rain",
    )
    features = _extract_features_with_geometry(data)

    result = {
        "source": SOURCE_NAME,
        "dataset": "Lee County FL Rainfall",
        "count": len(features),
        "features": features,
    }
    await cache.set(cache_key, result, ttl=config.CACHE_TTL)
    return result


# ---------------------------------------------------------------------------
# Endpoint 2: GET /api/regional_arcgis/hampton_roads_rain
# ---------------------------------------------------------------------------

@router.get("/hampton_roads_rain", summary="Hampton Roads VA rain gauges (74 gauges)")
async def hampton_roads_rain(
    limit: int = Query(
        2000,
        description="Max records to return.",
        ge=1,
        le=5000,
    ),
):
    """Return rain gauge observations from Hampton Roads, VA (HRSD network).

    Covers ~74 public rain gauges across the Hampton Roads region.
    """
    cache_key = f"regional_arcgis:hampton_roads_rain:{limit}"
    cached = await cache.get(cache_key)
    if cached is not None:
        return cached

    data = await _query_arcgis(
        _HAMPTON_ROADS_RAIN_URL,
        limit=limit,
        source_label="regional_arcgis_hampton_rain",
    )
    features = _extract_features_with_geometry(data)

    result = {
        "source": SOURCE_NAME,
        "dataset": "Hampton Roads VA Rain Gauges",
        "count": len(features),
        "features": features,
    }
    await cache.set(cache_key, result, ttl=config.CACHE_TTL)
    return result


# ---------------------------------------------------------------------------
# Endpoint 3: GET /api/regional_arcgis/nh_groundwater
# ---------------------------------------------------------------------------

@router.get("/nh_groundwater", summary="NH groundwater monitoring (41 wells)")
async def nh_groundwater(
    limit: int = Query(
        2000,
        description="Max records to return.",
        ge=1,
        le=5000,
    ),
):
    """Return groundwater level monitoring data from New Hampshire.

    Covers ~41 DES monitoring wells across the state.
    """
    cache_key = f"regional_arcgis:nh_groundwater:{limit}"
    cached = await cache.get(cache_key)
    if cached is not None:
        return cached

    data = await _query_arcgis(
        _NH_GROUNDWATER_URL,
        limit=limit,
        source_label="regional_arcgis_nh_gw",
    )
    features = _extract_features_with_geometry(data)

    result = {
        "source": SOURCE_NAME,
        "dataset": "NH Groundwater Monitoring",
        "count": len(features),
        "features": features,
    }
    await cache.set(cache_key, result, ttl=config.CACHE_TTL)
    return result


# ---------------------------------------------------------------------------
# Endpoint 4: GET /api/regional_arcgis/missouri_soil
# ---------------------------------------------------------------------------

@router.get("/missouri_soil", summary="Missouri soil moisture (37 SCAN sensors)")
async def missouri_soil(
    limit: int = Query(
        2000,
        description="Max records to return.",
        ge=1,
        le=5000,
    ),
):
    """Return soil moisture sensor data from Missouri SCAN network.

    Covers ~37 NRCS SCAN sensors across Missouri.
    """
    cache_key = f"regional_arcgis:missouri_soil:{limit}"
    cached = await cache.get(cache_key)
    if cached is not None:
        return cached

    data = await _query_arcgis(
        _MISSOURI_SOIL_URL,
        limit=limit,
        source_label="regional_arcgis_mo_soil",
    )
    features = _extract_features_with_geometry(data)

    result = {
        "source": SOURCE_NAME,
        "dataset": "Missouri Soil Moisture Sensors",
        "count": len(features),
        "features": features,
    }
    await cache.set(cache_key, result, ttl=config.CACHE_TTL)
    return result


# ---------------------------------------------------------------------------
# Endpoint 5: GET /api/regional_arcgis/salem_stormwater
# ---------------------------------------------------------------------------

@router.get("/salem_stormwater", summary="Salem OR stormwater monitors (23 monitors)")
async def salem_stormwater(
    limit: int = Query(
        2000,
        description="Max records to return.",
        ge=1,
        le=5000,
    ),
):
    """Return stormwater monitoring data from Salem, Oregon.

    Covers ~23 City of Salem stormwater monitoring sites.
    """
    cache_key = f"regional_arcgis:salem_stormwater:{limit}"
    cached = await cache.get(cache_key)
    if cached is not None:
        return cached

    data = await _query_arcgis(
        _SALEM_STORMWATER_URL,
        limit=limit,
        source_label="regional_arcgis_salem_sw",
    )
    features = _extract_features_with_geometry(data)

    result = {
        "source": SOURCE_NAME,
        "dataset": "Salem OR Stormwater Monitors",
        "count": len(features),
        "features": features,
    }
    await cache.set(cache_key, result, ttl=config.CACHE_TTL)
    return result


# ---------------------------------------------------------------------------
# Endpoint 6: GET /api/regional_arcgis/victoria_aq
# ---------------------------------------------------------------------------

@router.get("/victoria_aq", summary="Victoria Australia EPA air quality (30-min)")
async def victoria_aq(
    limit: int = Query(
        2000,
        description="Max records to return.",
        ge=1,
        le=5000,
    ),
):
    """Return latest air quality readings from Victoria, Australia (EPA Victoria).

    30-minute update cycle with PM2.5, PM10, ozone, NO2, SO2, and CO readings.
    """
    cache_key = f"regional_arcgis:victoria_aq:{limit}"
    cached = await cache.get(cache_key)
    if cached is not None:
        return cached

    data = await _query_arcgis(
        _VICTORIA_AQ_URL,
        limit=limit,
        source_label="regional_arcgis_victoria_aq",
    )
    features = _extract_features_with_geometry(data)

    result = {
        "source": SOURCE_NAME,
        "dataset": "Victoria EPA Air Quality",
        "count": len(features),
        "features": features,
    }
    await cache.set(cache_key, result, ttl=config.CACHE_TTL)
    return result


# ---------------------------------------------------------------------------
# Endpoint 7: GET /api/regional_arcgis/overview
# ---------------------------------------------------------------------------

@router.get("/overview", summary="Regional ArcGIS source overview")
async def overview():
    """Return a descriptive summary of all datasets available through this
    router, including endpoints, coverage, and parameters."""
    return {
        "source": SOURCE_NAME,
        "description": SOURCE_DESCRIPTION,
        **SOURCE_META,
        "datasets": [
            {
                "id": "lee_county_rain",
                "name": "Lee County FL Rainfall",
                "url": _LEE_COUNTY_RAIN_URL.split("/query")[0],
                "station_count": "~203",
                "update_frequency": "15 min",
                "description": "Tipping-bucket rain gauge stations across Lee County, FL.",
                "endpoint": {
                    "path": "/api/regional_arcgis/lee_county_rain",
                    "method": "GET",
                    "params": ["limit"],
                },
            },
            {
                "id": "hampton_roads_rain",
                "name": "Hampton Roads VA Rain Gauges",
                "url": _HAMPTON_ROADS_RAIN_URL.split("/query")[0],
                "station_count": "~74",
                "update_frequency": "Varies",
                "description": "HRSD public rain gauge network in Hampton Roads, VA.",
                "endpoint": {
                    "path": "/api/regional_arcgis/hampton_roads_rain",
                    "method": "GET",
                    "params": ["limit"],
                },
            },
            {
                "id": "nh_groundwater",
                "name": "NH Groundwater Monitoring",
                "url": _NH_GROUNDWATER_URL.split("/query")[0],
                "station_count": "~41",
                "update_frequency": "Varies",
                "description": "DES groundwater level monitoring wells across New Hampshire.",
                "endpoint": {
                    "path": "/api/regional_arcgis/nh_groundwater",
                    "method": "GET",
                    "params": ["limit"],
                },
            },
            {
                "id": "missouri_soil",
                "name": "Missouri Soil Moisture Sensors",
                "url": _MISSOURI_SOIL_URL.split("/query")[0],
                "station_count": "~37",
                "update_frequency": "1 hr",
                "description": "NRCS SCAN soil moisture sensors across Missouri.",
                "endpoint": {
                    "path": "/api/regional_arcgis/missouri_soil",
                    "method": "GET",
                    "params": ["limit"],
                },
            },
            {
                "id": "salem_stormwater",
                "name": "Salem OR Stormwater Monitors",
                "url": _SALEM_STORMWATER_URL.split("/query")[0],
                "station_count": "~23",
                "update_frequency": "Varies",
                "description": "City of Salem stormwater monitoring sites.",
                "endpoint": {
                    "path": "/api/regional_arcgis/salem_stormwater",
                    "method": "GET",
                    "params": ["limit"],
                },
            },
            {
                "id": "victoria_aq",
                "name": "Victoria EPA Air Quality",
                "url": _VICTORIA_AQ_URL.split("/query")[0],
                "station_count": "Varies",
                "update_frequency": "30 min",
                "description": "EPA Victoria latest air quality readings (PM2.5, PM10, ozone, etc.).",
                "endpoint": {
                    "path": "/api/regional_arcgis/victoria_aq",
                    "method": "GET",
                    "params": ["limit"],
                },
            },
        ],
    }
