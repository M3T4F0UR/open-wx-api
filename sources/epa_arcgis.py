"""EPA AirNow (ArcGIS) — US air quality monitors, forecasts, contours, and smoke.

Public ArcGIS Feature Services hosted by EPA, updated hourly.  No API key
required — all endpoints are open.

Datasets:
1. AirNow Current Monitors  (~4,375 stations, hourly AQI + concentrations)
2. AQI Forecast              (~592 reporting areas, national coverage)
3. PM2.5 AQI Contours        (interpolated surface polygons)
4. Smoke Forecast             (48-hour NDGD smoke plume polygons)
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
# Source registry metadata
# ---------------------------------------------------------------------------
SOURCE_NAME = "EPA AirNow (ArcGIS)"
SOURCE_DESCRIPTION = (
    "EPA AirNow public ArcGIS Feature Services — current monitor observations "
    "(~4,375 stations), national AQI forecasts (~592 reporting areas), PM2.5 "
    "contour polygons, and 48-hour NDGD smoke forecast polygons.  All services "
    "are free with no authentication required and are updated hourly."
)
SOURCE_META = {
    "category": "air_quality",
    "coverage": "United States (~4,375 monitor stations + national grids)",
    "auth_required": False,
    "update_frequency": "~1 hr",
    "params": ["aqi", "pm2.5", "pm10", "ozone", "smoke"],
    "urls": {
        "homepage": "https://www.airnow.gov/",
        "arcgis_hub": "https://www.arcgis.com/home/group.html?id=96b5e06f16574ad3a0f0a5ed498e1a67",
    },
}

# ---------------------------------------------------------------------------
# Router
# ---------------------------------------------------------------------------

router = APIRouter(prefix="/api/epa_arcgis", tags=["EPA AirNow (ArcGIS)"])

# ---------------------------------------------------------------------------
# ArcGIS Feature Service URLs
# ---------------------------------------------------------------------------

_MONITORS_URL = (
    "https://services.arcgis.com/cJ9YHowT8TU7DUyn/arcgis/rest/services/"
    "Air%20Now%20Current%20Monitor%20Data%20Public/FeatureServer/0/query"
)
_FORECAST_URL = (
    "https://services.arcgis.com/cJ9YHowT8TU7DUyn/arcgis/rest/services/"
    "AirNow_National_Air_Quality_Index_(AQI)_Forecast/FeatureServer/0/query"
)
_PM25_CONTOURS_URL = (
    "https://services.arcgis.com/cJ9YHowT8TU7DUyn/arcgis/rest/services/"
    "AirNowLatestContoursPM25/FeatureServer/0/query"
)
_SMOKE_URL = (
    "https://services9.arcgis.com/RHVPKKiFTONKtxq3/arcgis/rest/services/"
    "NDGD_SmokeForecast_v1/FeatureServer/0/query"
)


# ---------------------------------------------------------------------------
# ArcGIS query helper
# ---------------------------------------------------------------------------

async def _arcgis_query(
    url: str,
    *,
    where: str = "1=1",
    out_fields: str = "*",
    result_record_count: Optional[int] = None,
    result_offset: int = 0,
    source_label: str = "epa_arcgis",
) -> dict[str, Any]:
    """Execute an ArcGIS Feature Service query and return the JSON response.

    Handles pagination via resultRecordCount / resultOffset.  Raises
    HTTPException on upstream errors.
    """
    params: dict[str, Any] = {
        "where": where,
        "outFields": out_fields,
        "f": "json",
        "resultOffset": result_offset,
    }
    if result_record_count is not None:
        params["resultRecordCount"] = result_record_count

    try:
        data = await http_client.fetch_json(
            url, params=params, source=source_label,
        )
    except http_client.CircuitOpenError:
        raise HTTPException(
            status_code=503,
            detail=f"EPA ArcGIS circuit breaker open — too many recent failures.",
        )
    except Exception as exc:
        logger.exception("EPA ArcGIS query failed: %s", url)
        raise HTTPException(status_code=502, detail=f"EPA ArcGIS upstream error: {exc}")

    # ArcGIS services may return an error object instead of features
    if "error" in data:
        err = data["error"]
        msg = err.get("message", str(err))
        code = err.get("code", 502)
        raise HTTPException(status_code=502, detail=f"ArcGIS error ({code}): {msg}")

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
# Endpoint 1: GET /api/epa_arcgis/monitors
# ---------------------------------------------------------------------------

@router.get("/monitors", summary="AirNow current monitor observations")
async def monitors(
    state: Optional[str] = Query(
        None,
        description="Two-letter state code filter (e.g. OR, CA).  Omit for all.",
        max_length=2,
    ),
    limit: int = Query(
        5000,
        description="Max records to return (ArcGIS page size).",
        ge=1,
        le=10000,
    ),
):
    """Return current AirNow monitor data with AQI values.

    Each record includes AQSID, SiteName, StateName, OZONE, OZONE_AQI,
    PM25, PM25_AQI, PM10, PM10_AQI, and ValidTime.
    """
    where = f"StateName='{state.upper()}'" if state else "1=1"
    cache_key = f"epa_arcgis:monitors:{where}:{limit}"
    cached = await cache.get(cache_key)
    if cached is not None:
        return cached

    data = await _arcgis_query(
        _MONITORS_URL,
        where=where,
        result_record_count=limit,
        source_label="epa_arcgis_monitors",
    )
    features = _extract_features(data)

    result = {
        "source": SOURCE_NAME,
        "dataset": "AirNow Current Monitors",
        "query": {"state": state, "limit": limit},
        "count": len(features),
        "features": features,
    }
    await cache.set(cache_key, result, ttl=config.CACHE_TTL)
    return result


# ---------------------------------------------------------------------------
# Endpoint 2: GET /api/epa_arcgis/forecast
# ---------------------------------------------------------------------------

@router.get("/forecast", summary="National AQI forecast")
async def forecast(
    limit: int = Query(
        5000,
        description="Max records to return.",
        ge=1,
        le=10000,
    ),
):
    """Return the national AQI forecast from ~592 reporting areas."""
    cache_key = f"epa_arcgis:forecast:{limit}"
    cached = await cache.get(cache_key)
    if cached is not None:
        return cached

    data = await _arcgis_query(
        _FORECAST_URL,
        result_record_count=limit,
        source_label="epa_arcgis_forecast",
    )
    features = _extract_features(data)

    result = {
        "source": SOURCE_NAME,
        "dataset": "AQI Forecast",
        "count": len(features),
        "features": features,
    }
    await cache.set(cache_key, result, ttl=config.CACHE_TTL)
    return result


# ---------------------------------------------------------------------------
# Endpoint 3: GET /api/epa_arcgis/contours/pm25
# ---------------------------------------------------------------------------

@router.get("/contours/pm25", summary="PM2.5 AQI contour polygons")
async def contours_pm25(
    limit: int = Query(
        5000,
        description="Max records to return.",
        ge=1,
        le=10000,
    ),
):
    """Return interpolated PM2.5 AQI contour polygons (surface analysis)."""
    cache_key = f"epa_arcgis:contours_pm25:{limit}"
    cached = await cache.get(cache_key)
    if cached is not None:
        return cached

    data = await _arcgis_query(
        _PM25_CONTOURS_URL,
        result_record_count=limit,
        source_label="epa_arcgis_pm25",
    )
    features = _extract_features_with_geometry(data)

    result = {
        "source": SOURCE_NAME,
        "dataset": "PM2.5 AQI Contours",
        "count": len(features),
        "features": features,
    }
    await cache.set(cache_key, result, ttl=config.CACHE_TTL)
    return result


# ---------------------------------------------------------------------------
# Endpoint 4: GET /api/epa_arcgis/smoke_forecast
# ---------------------------------------------------------------------------

@router.get("/smoke_forecast", summary="48-hour smoke forecast polygons")
async def smoke_forecast(
    limit: int = Query(
        2000,
        description="Max records to return.",
        ge=1,
        le=10000,
    ),
):
    """Return NDGD 48-hour smoke forecast polygons (~8,416 features)."""
    cache_key = f"epa_arcgis:smoke_forecast:{limit}"
    cached = await cache.get(cache_key)
    if cached is not None:
        return cached

    data = await _arcgis_query(
        _SMOKE_URL,
        result_record_count=limit,
        source_label="epa_arcgis_smoke",
    )
    features = _extract_features_with_geometry(data)

    result = {
        "source": SOURCE_NAME,
        "dataset": "Smoke Forecast (NDGD 48-hr)",
        "count": len(features),
        "features": features,
    }
    await cache.set(cache_key, result, ttl=config.CACHE_TTL)
    return result


# ---------------------------------------------------------------------------
# Endpoint 5: GET /api/epa_arcgis/overview
# ---------------------------------------------------------------------------

@router.get("/overview", summary="EPA ArcGIS source overview")
async def overview():
    """Return a descriptive summary of all datasets available through this
    router, including endpoints, coverage, and parameters."""
    return {
        "source": SOURCE_NAME,
        "description": SOURCE_DESCRIPTION,
        **SOURCE_META,
        "datasets": [
            {
                "id": "monitors",
                "name": "AirNow Current Monitor Data",
                "url": _MONITORS_URL.split("/query")[0],
                "station_count": "~4,375",
                "update_frequency": "1 hr",
                "fields": [
                    "AQSID", "SiteName", "StateName",
                    "OZONE", "OZONE_AQI",
                    "PM25", "PM25_AQI",
                    "PM10", "PM10_AQI",
                    "ValidTime",
                ],
                "endpoint": {
                    "path": "/api/epa_arcgis/monitors",
                    "method": "GET",
                    "params": ["state", "limit"],
                },
            },
            {
                "id": "forecast",
                "name": "National AQI Forecast",
                "url": _FORECAST_URL.split("/query")[0],
                "reporting_areas": "~592",
                "update_frequency": "1 hr",
                "endpoint": {
                    "path": "/api/epa_arcgis/forecast",
                    "method": "GET",
                    "params": ["limit"],
                },
            },
            {
                "id": "contours_pm25",
                "name": "PM2.5 AQI Contours",
                "url": _PM25_CONTOURS_URL.split("/query")[0],
                "description": "Interpolated PM2.5 surface analysis polygons.",
                "update_frequency": "1 hr",
                "endpoint": {
                    "path": "/api/epa_arcgis/contours/pm25",
                    "method": "GET",
                    "params": ["limit"],
                },
            },
            {
                "id": "smoke_forecast",
                "name": "NDGD Smoke Forecast",
                "url": _SMOKE_URL.split("/query")[0],
                "description": "48-hour smoke forecast polygons (~8,416 features).",
                "update_frequency": "Varies (model cycle)",
                "endpoint": {
                    "path": "/api/epa_arcgis/smoke_forecast",
                    "method": "GET",
                    "params": ["limit"],
                },
            },
        ],
    }
