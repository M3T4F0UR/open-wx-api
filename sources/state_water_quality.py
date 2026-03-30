"""State water quality monitoring on ArcGIS — no auth required.

Covers:
  1. Florida FDEP IWR Stations        — 118,482 sampling locations
  2. Florida FDEP WIN Monitoring       — 40,845 monitoring locations
  3. MassDEP Water Quality Stations    — 3,111 stations
  4. Virginia DEQ Monitoring           — 1,305 stations
  5. NYS DEC Lake Monitoring           — 1,346 lakes
  6. SCDNR Monitoring                  — 4,359 sites

All sources are public ArcGIS Feature/Map Services with no authentication.
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

SOURCE_NAME = "State Water Quality Monitoring"
SOURCE_DESCRIPTION = (
    "State-level water quality monitoring from six ArcGIS services: Florida "
    "FDEP IWR stations (118K sampling locations) and WIN monitoring (40K), "
    "MassDEP stations (3.1K), Virginia DEQ monitoring (1.3K), NYS DEC lake "
    "monitoring (1.3K lakes), and SCDNR monitoring (4.4K sites).  All public, "
    "no authentication required."
)
SOURCE_META = {
    "category": "water_quality",
    "coverage": "Florida, Massachusetts, Virginia, New York, South Carolina",
    "auth_required": False,
    "update_frequency": "Varies by agency (daily to quarterly)",
    "params": [
        "water_quality", "nutrients", "bacteria", "dissolved_oxygen",
        "ph", "turbidity", "temperature", "conductivity",
    ],
    "networks": [
        "fl_fdep_iwr", "fl_fdep_win", "massdep_wq",
        "va_deq", "nys_dec_lakes", "scdnr",
    ],
    "urls": {
        "fl_fdep": "https://ca.dep.state.fl.us/arcgis/rest/services/OpenData/IMPAIRED_WATERS/MapServer",
        "massdep": "https://arcgisserver.digital.mass.gov/arcgisserver/rest/services/AGOL/MassDEP_Water_Quality_Monitoring_Stations/FeatureServer",
        "va_deq": "https://apps.deq.virginia.gov/arcgis/rest/services/public/EDMA/MapServer",
        "nys_dec": "https://services6.arcgis.com/DZHaqZm9cxOD4CWM/arcgis/rest/services/Lake_Monitoring_Reports/FeatureServer",
        "scdnr": "https://services.arcgis.com/aa38u6OgfNoCkTJ6/arcgis/rest/services/GSP_Monitoring_Data/FeatureServer",
    },
    "station_count": 169448,
}

router = APIRouter(prefix="/api/state_wq", tags=["State Water Quality Monitoring"])

# ---------------------------------------------------------------------------
# ArcGIS Feature Service URLs
# ---------------------------------------------------------------------------

_FL_IWR_URL = (
    "https://ca.dep.state.fl.us/arcgis/rest/services/OpenData/"
    "IMPAIRED_WATERS/MapServer/1/query"
)

_FL_WIN_URL = (
    "https://ca.dep.state.fl.us/arcgis/rest/services/OpenData/"
    "IMPAIRED_WATERS/MapServer/11/query"
)

_MA_WQ_URL = (
    "https://arcgisserver.digital.mass.gov/arcgisserver/rest/services/AGOL/"
    "MassDEP_Water_Quality_Monitoring_Stations/FeatureServer/5/query"
)

_VA_DEQ_URL = (
    "https://apps.deq.virginia.gov/arcgis/rest/services/public/"
    "EDMA/MapServer/131/query"
)

_NY_LAKES_URL = (
    "https://services6.arcgis.com/DZHaqZm9cxOD4CWM/arcgis/rest/services/"
    "Lake_Monitoring_Reports/FeatureServer/0/query"
)

_SC_DNR_URL = (
    "https://services.arcgis.com/aa38u6OgfNoCkTJ6/arcgis/rest/services/"
    "GSP_Monitoring_Data/FeatureServer/0/query"
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
    source_label: str = "state_wq",
) -> dict[str, Any]:
    """Execute an ArcGIS Feature/Map Service query and return the JSON response."""
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
            detail=f"State WQ circuit breaker open ({source_label}) — too many recent failures.",
        )
    except Exception as exc:
        logger.exception("State WQ ArcGIS query failed: %s", url)
        raise HTTPException(status_code=502, detail=f"State WQ upstream error: {exc}")

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


# ---------------------------------------------------------------------------
# Endpoint 1: GET /api/state_wq/florida/stations
# ---------------------------------------------------------------------------

@router.get("/florida/stations", summary="Florida FDEP IWR sampling stations")
async def florida_stations(
    limit: int = Query(
        5000,
        description="Max records to return.",
        ge=1,
        le=10000,
    ),
):
    """Return Florida FDEP Impaired Waters Rule (IWR) sampling stations.

    ~118,482 locations with STATION_ID, STATION_NAME, WATERBODY_ID,
    LATITUDE, LONGITUDE, RESULT_COUNT, BEGIN_DATE, END_DATE.
    """
    cache_key = f"state_wq:fl_iwr:{limit}"
    cached = await cache.get(cache_key)
    if cached is not None:
        return cached

    data = await _arcgis_query(
        _FL_IWR_URL,
        result_record_count=limit,
        source_label="state_wq_fl_iwr",
    )
    features = _extract_features(data)

    result = {
        "source": SOURCE_NAME,
        "dataset": "Florida FDEP IWR Stations",
        "query": {"limit": limit},
        "count": len(features),
        "features": features,
    }
    await cache.set(cache_key, result, ttl=config.CACHE_TTL)
    return result


# ---------------------------------------------------------------------------
# Endpoint 2: GET /api/state_wq/florida/monitoring
# ---------------------------------------------------------------------------

@router.get("/florida/monitoring", summary="Florida FDEP WIN monitoring locations")
async def florida_monitoring(
    limit: int = Query(
        5000,
        description="Max records to return.",
        ge=1,
        le=10000,
    ),
):
    """Return Florida FDEP WIN (Watershed Information Network) monitoring locations.

    ~40,845 locations with station details and monitoring metadata.
    """
    cache_key = f"state_wq:fl_win:{limit}"
    cached = await cache.get(cache_key)
    if cached is not None:
        return cached

    data = await _arcgis_query(
        _FL_WIN_URL,
        result_record_count=limit,
        source_label="state_wq_fl_win",
    )
    features = _extract_features(data)

    result = {
        "source": SOURCE_NAME,
        "dataset": "Florida FDEP WIN Monitoring",
        "query": {"limit": limit},
        "count": len(features),
        "features": features,
    }
    await cache.set(cache_key, result, ttl=config.CACHE_TTL)
    return result


# ---------------------------------------------------------------------------
# Endpoint 3: GET /api/state_wq/massachusetts
# ---------------------------------------------------------------------------

@router.get("/massachusetts", summary="MassDEP water quality stations")
async def massachusetts(
    limit: int = Query(
        5000,
        description="Max records to return.",
        ge=1,
        le=10000,
    ),
):
    """Return MassDEP Water Quality Monitoring Stations.

    ~3,111 stations across Massachusetts with station metadata and
    monitoring parameters.
    """
    cache_key = f"state_wq:ma:{limit}"
    cached = await cache.get(cache_key)
    if cached is not None:
        return cached

    data = await _arcgis_query(
        _MA_WQ_URL,
        result_record_count=limit,
        source_label="state_wq_ma",
    )
    features = _extract_features(data)

    result = {
        "source": SOURCE_NAME,
        "dataset": "MassDEP Water Quality Stations",
        "query": {"limit": limit},
        "count": len(features),
        "features": features,
    }
    await cache.set(cache_key, result, ttl=config.CACHE_TTL)
    return result


# ---------------------------------------------------------------------------
# Endpoint 4: GET /api/state_wq/virginia
# ---------------------------------------------------------------------------

@router.get("/virginia", summary="Virginia DEQ monitoring stations")
async def virginia(
    limit: int = Query(
        2000,
        description="Max records to return.",
        ge=1,
        le=10000,
    ),
):
    """Return Virginia DEQ Environmental Data Monitoring and Assessment stations.

    ~1,305 stations with station identifiers, location, and monitoring status.
    """
    cache_key = f"state_wq:va:{limit}"
    cached = await cache.get(cache_key)
    if cached is not None:
        return cached

    data = await _arcgis_query(
        _VA_DEQ_URL,
        result_record_count=limit,
        source_label="state_wq_va",
    )
    features = _extract_features(data)

    result = {
        "source": SOURCE_NAME,
        "dataset": "Virginia DEQ Monitoring",
        "query": {"limit": limit},
        "count": len(features),
        "features": features,
    }
    await cache.set(cache_key, result, ttl=config.CACHE_TTL)
    return result


# ---------------------------------------------------------------------------
# Endpoint 5: GET /api/state_wq/new_york/lakes
# ---------------------------------------------------------------------------

@router.get("/new_york/lakes", summary="NYS DEC lake monitoring reports")
async def new_york_lakes(
    limit: int = Query(
        2000,
        description="Max records to return.",
        ge=1,
        le=10000,
    ),
):
    """Return NYS DEC Lake Monitoring Reports.

    ~1,346 lakes with monitoring data, classifications, and assessment
    information.
    """
    cache_key = f"state_wq:ny_lakes:{limit}"
    cached = await cache.get(cache_key)
    if cached is not None:
        return cached

    data = await _arcgis_query(
        _NY_LAKES_URL,
        result_record_count=limit,
        source_label="state_wq_ny_lakes",
    )
    features = _extract_features(data)

    result = {
        "source": SOURCE_NAME,
        "dataset": "NYS DEC Lake Monitoring",
        "query": {"limit": limit},
        "count": len(features),
        "features": features,
    }
    await cache.set(cache_key, result, ttl=config.CACHE_TTL)
    return result


# ---------------------------------------------------------------------------
# Endpoint 6: GET /api/state_wq/south_carolina
# ---------------------------------------------------------------------------

@router.get("/south_carolina", summary="SCDNR monitoring sites")
async def south_carolina(
    limit: int = Query(
        5000,
        description="Max records to return.",
        ge=1,
        le=10000,
    ),
):
    """Return SCDNR Groundwater/Surface Water Program monitoring sites.

    ~4,359 sites across South Carolina with station details and monitoring
    metadata.
    """
    cache_key = f"state_wq:sc:{limit}"
    cached = await cache.get(cache_key)
    if cached is not None:
        return cached

    data = await _arcgis_query(
        _SC_DNR_URL,
        result_record_count=limit,
        source_label="state_wq_sc",
    )
    features = _extract_features(data)

    result = {
        "source": SOURCE_NAME,
        "dataset": "SCDNR Monitoring",
        "query": {"limit": limit},
        "count": len(features),
        "features": features,
    }
    await cache.set(cache_key, result, ttl=config.CACHE_TTL)
    return result


# ---------------------------------------------------------------------------
# Endpoint 7: GET /api/state_wq/overview
# ---------------------------------------------------------------------------

@router.get("/overview", summary="State water quality source overview")
async def overview():
    """Return a descriptive summary of all datasets available through this
    router, including endpoints, coverage, and parameters."""
    return {
        "source": SOURCE_NAME,
        "description": SOURCE_DESCRIPTION,
        **SOURCE_META,
        "datasets": [
            {
                "id": "fl_fdep_iwr",
                "name": "Florida FDEP IWR Stations",
                "url": _FL_IWR_URL.split("/query")[0],
                "station_count": "~118,482",
                "update_frequency": "Quarterly",
                "fields": [
                    "STATION_ID", "STATION_NAME", "WATERBODY_ID",
                    "LATITUDE", "LONGITUDE", "RESULT_COUNT",
                    "BEGIN_DATE", "END_DATE",
                ],
                "endpoint": {
                    "path": "/api/state_wq/florida/stations",
                    "method": "GET",
                    "params": ["limit"],
                },
            },
            {
                "id": "fl_fdep_win",
                "name": "Florida FDEP WIN Monitoring",
                "url": _FL_WIN_URL.split("/query")[0],
                "station_count": "~40,845",
                "update_frequency": "Quarterly",
                "endpoint": {
                    "path": "/api/state_wq/florida/monitoring",
                    "method": "GET",
                    "params": ["limit"],
                },
            },
            {
                "id": "massdep_wq",
                "name": "MassDEP Water Quality Stations",
                "url": _MA_WQ_URL.split("/query")[0],
                "station_count": "~3,111",
                "update_frequency": "Varies",
                "endpoint": {
                    "path": "/api/state_wq/massachusetts",
                    "method": "GET",
                    "params": ["limit"],
                },
            },
            {
                "id": "va_deq",
                "name": "Virginia DEQ Monitoring",
                "url": _VA_DEQ_URL.split("/query")[0],
                "station_count": "~1,305",
                "update_frequency": "Varies",
                "endpoint": {
                    "path": "/api/state_wq/virginia",
                    "method": "GET",
                    "params": ["limit"],
                },
            },
            {
                "id": "nys_dec_lakes",
                "name": "NYS DEC Lake Monitoring",
                "url": _NY_LAKES_URL.split("/query")[0],
                "station_count": "~1,346",
                "update_frequency": "Annual",
                "endpoint": {
                    "path": "/api/state_wq/new_york/lakes",
                    "method": "GET",
                    "params": ["limit"],
                },
            },
            {
                "id": "scdnr",
                "name": "SCDNR Monitoring",
                "url": _SC_DNR_URL.split("/query")[0],
                "station_count": "~4,359",
                "update_frequency": "Varies",
                "endpoint": {
                    "path": "/api/state_wq/south_carolina",
                    "method": "GET",
                    "params": ["limit"],
                },
            },
        ],
    }
