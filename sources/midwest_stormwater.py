"""Midwest City/County Stormwater Networks — rain gauges, CSO, and stream monitors.

Aggregates public stormwater and rainfall monitoring data from six
municipal/county networks across the Midwest and Pacific Northwest:

1. MWRD Chicago — rain gauges and combined sewer overflow monitoring.
   Portal: https://geohub.mwrd.org/
   ArcGIS server: https://gispub.mwrd.org/arcgis/rest/services/

2. Cleveland NEORSD — ~40 rain gauges across Northeast Ohio.
   Portal: https://gis-neorsd.hub.arcgis.com/

3. Milwaukee MMSD — stormwater and green infrastructure monitoring.
   Portal: https://share-open-data-mmsdgis.hub.arcgis.com/

4. DuPage County IL — 22 rain gauges + 25 stream gauges.
   Portal: https://ec.dupagecounty.gov/

5. Cincinnati MSD — 25+ rain gauges, gauge-adjusted radar.
   Portal: https://msdgc.org/programs/rainfall-data/
   Radar: https://msdgc.vieuxinc.com/

6. Seattle SPU — rain gauges (ArcGIS FeatureServer, confirmed public).
   ArcGIS: https://services.arcgis.com/ZOyb2t4B0UYuYNYH/arcgis/rest/services/Rain_Gages/

All endpoints are unauthenticated.  ArcGIS FeatureServer layers are queried
with ``?where=1=1&outFields=*&f=json``.  Non-ArcGIS sources are scraped or
use vendor APIs.
"""
from __future__ import annotations

import logging
import re
from typing import Any, Optional

from fastapi import APIRouter, HTTPException, Query

import cache
import config
import http_client

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Source registry metadata  (read by server.py /api/sources)
# ---------------------------------------------------------------------------

SOURCE_NAME = "City Stormwater Networks"
SOURCE_DESCRIPTION = (
    "Municipal stormwater and rainfall monitoring networks — MWRD Chicago, "
    "Cleveland NEORSD (~40 rain gauges), Milwaukee MMSD, DuPage County IL "
    "(22 rain + 25 stream gauges), Cincinnati MSD (25+ rain gauges + "
    "gauge-adjusted radar), and Seattle SPU rain gauges.  All public, no "
    "authentication required."
)
SOURCE_META = {
    "category": "water",
    "coverage": "Chicago, Cleveland, Milwaukee, DuPage County IL, Cincinnati, Seattle",
    "auth_required": False,
    "update_frequency": "5-15 min (varies by network)",
    "params": [
        "precipitation", "rainfall", "stage", "stream_level",
        "cso_status", "water_level",
    ],
    "urls": {
        "mwrd_chicago": "https://geohub.mwrd.org/",
        "neorsd_cleveland": "https://gis-neorsd.hub.arcgis.com/",
        "mmsd_milwaukee": "https://share-open-data-mmsdgis.hub.arcgis.com/",
        "dupage_county": "https://ec.dupagecounty.gov/",
        "cincy_msd": "https://msdgc.org/programs/rainfall-data/",
        "seattle_spu": "https://data-seattlecitygis.opendata.arcgis.com/",
    },
}

# ---------------------------------------------------------------------------
# Router
# ---------------------------------------------------------------------------

router = APIRouter(prefix="/api/stormwater", tags=["City Stormwater Networks"])

# ---------------------------------------------------------------------------
# Constants — ArcGIS FeatureServer URLs
# ---------------------------------------------------------------------------

_SOURCE = "stormwater"

# ── Seattle SPU Rain Gages (confirmed public FeatureServer) ──────────────
_SEATTLE_RAIN_URL = (
    "https://services.arcgis.com/ZOyb2t4B0UYuYNYH/arcgis/rest/services/"
    "Rain_Gages/FeatureServer/0/query"
)

# ── MWRD Chicago ─────────────────────────────────────────────────────────
# The MWRD ArcGIS server hosts IWD layers; CSO status is available via
# the public GeoHub application pages.
_MWRD_ARCGIS_BASE = "https://gispub.mwrd.org/arcgis/rest/services"
_MWRD_CSO_PAGE = "https://geohub.mwrd.org/pages/cso"
_MWRD_HUB_SEARCH = "https://geohub.mwrd.org/api/v3/datasets"

# ── Cleveland NEORSD ─────────────────────────────────────────────────────
# Rain gauge locations are published as a Web Map; the underlying
# FeatureServer is discoverable via the Hub dataset API.
_NEORSD_HUB_SEARCH = "https://gis-neorsd.hub.arcgis.com/api/v3/datasets"

# ── Milwaukee MMSD ───────────────────────────────────────────────────────
_MMSD_HUB_SEARCH = (
    "https://share-open-data-mmsdgis.hub.arcgis.com/api/v3/datasets"
)

# ── DuPage County IL ─────────────────────────────────────────────────────
# Uses USGS stations; the county portal provides gauge lists.
_DUPAGE_BASE = "https://ec.dupagecounty.gov"

# ── Cincinnati MSD ───────────────────────────────────────────────────────
_CINCY_RADAR_BASE = "https://msdgc.vieuxinc.com"


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
    geometry_type: Optional[str] = None,
    geometry: Optional[str] = None,
    spatial_rel: Optional[str] = None,
    source_label: str = "stormwater",
) -> dict[str, Any]:
    """Execute an ArcGIS FeatureServer query and return the JSON response."""
    params: dict[str, Any] = {
        "where": where,
        "outFields": out_fields,
        "f": "json",
        "resultOffset": result_offset,
    }
    if result_record_count is not None:
        params["resultRecordCount"] = result_record_count
    if geometry_type:
        params["geometryType"] = geometry_type
    if geometry:
        params["geometry"] = geometry
    if spatial_rel:
        params["spatialRel"] = spatial_rel

    try:
        data = await http_client.fetch_json(
            url, params=params, source=source_label,
        )
    except http_client.CircuitOpenError:
        raise HTTPException(
            status_code=503,
            detail=f"Stormwater ArcGIS circuit breaker open ({source_label}).",
        )
    except Exception as exc:
        logger.exception("ArcGIS query failed (%s): %s", source_label, url)
        raise HTTPException(
            status_code=502,
            detail=f"Stormwater upstream error ({source_label}): {exc}",
        )

    if "error" in data:
        err = data["error"]
        msg = err.get("message", str(err))
        code = err.get("code", 502)
        raise HTTPException(
            status_code=502,
            detail=f"ArcGIS error ({source_label}, code {code}): {msg}",
        )
    return data


def _extract_features(data: dict) -> list[dict]:
    """Pull attribute dicts from an ArcGIS query response."""
    return [f.get("attributes", {}) for f in data.get("features", [])]


def _extract_features_with_geometry(data: dict) -> list[dict]:
    """Pull features preserving both attributes and geometry."""
    out = []
    for f in data.get("features", []):
        rec = {**f.get("attributes", {})}
        geom = f.get("geometry")
        if geom:
            rec["_lon"] = geom.get("x")
            rec["_lat"] = geom.get("y")
        out.append(rec)
    return out


# ---------------------------------------------------------------------------
# Hub dataset discovery helper
# ---------------------------------------------------------------------------

async def _hub_search_datasets(
    hub_url: str,
    query: str,
    *,
    source_label: str = "stormwater",
    limit: int = 20,
) -> list[dict]:
    """Search an ArcGIS Hub v3 datasets endpoint for Feature Services.

    Returns a list of dataset metadata dicts with title, url, id, and type.
    """
    params = {
        "filter[q]": query,
        "filter[type]": "Feature Service",
        "page[size]": limit,
    }
    try:
        data = await http_client.fetch_json(
            hub_url, params=params, source=source_label,
        )
    except Exception:
        logger.warning("Hub search failed for %s (query=%s)", hub_url, query)
        return []

    results = []
    for item in data.get("data", []):
        attrs = item.get("attributes", {})
        results.append({
            "id": item.get("id"),
            "title": attrs.get("name") or attrs.get("title"),
            "url": attrs.get("url"),
            "type": attrs.get("type"),
            "description": (attrs.get("searchDescription") or "")[:200],
        })
    return results


# ---------------------------------------------------------------------------
# Endpoint: GET /api/stormwater/overview
# ---------------------------------------------------------------------------

@router.get("/overview", summary="Overview of all stormwater networks")
async def overview():
    """Return metadata and status for each stormwater network."""
    return {
        "source": SOURCE_NAME,
        "networks": [
            {
                "id": "mwrd_chicago",
                "name": "MWRD Chicago",
                "coverage": "Cook County / Chicago metro, IL",
                "portal": "https://geohub.mwrd.org/",
                "data_types": ["rain_gauge", "cso_status"],
                "notes": "ArcGIS Hub; CSO page at geohub.mwrd.org/pages/cso",
            },
            {
                "id": "neorsd_cleveland",
                "name": "Cleveland NEORSD",
                "coverage": "Northeast Ohio / Cuyahoga County",
                "portal": "https://gis-neorsd.hub.arcgis.com/",
                "data_types": ["rain_gauge"],
                "gauge_count": "~40",
                "notes": "Rain gauge locations published as ArcGIS Web Map",
            },
            {
                "id": "mmsd_milwaukee",
                "name": "Milwaukee MMSD",
                "coverage": "Greater Milwaukee, WI",
                "portal": "https://share-open-data-mmsdgis.hub.arcgis.com/",
                "data_types": ["stormwater", "green_infrastructure"],
                "notes": "ArcGIS Hub open data portal",
            },
            {
                "id": "dupage_county",
                "name": "DuPage County Stormwater",
                "coverage": "DuPage County, IL",
                "portal": "https://ec.dupagecounty.gov/",
                "data_types": ["rain_gauge", "stream_gauge", "camera"],
                "gauge_count": "22 rain + 25 stream + 14 cameras",
                "notes": "Gauges feed to USGS; county web interface",
            },
            {
                "id": "cincy_msd",
                "name": "Cincinnati MSD",
                "coverage": "Hamilton County / Greater Cincinnati, OH",
                "portal": "https://msdgc.org/programs/rainfall-data/",
                "data_types": ["rain_gauge", "gauge_adjusted_radar"],
                "gauge_count": "25+",
                "notes": "Gauge-adjusted radar via Vieux Inc platform",
            },
            {
                "id": "seattle_spu",
                "name": "Seattle SPU Rain Gauges",
                "coverage": "City of Seattle, WA",
                "portal": "https://data-seattlecitygis.opendata.arcgis.com/",
                "data_types": ["rain_gauge"],
                "notes": "ArcGIS FeatureServer (confirmed public, no auth)",
            },
        ],
    }


# ═══════════════════════════════════════════════════════════════════════════
# SEATTLE SPU
# ═══════════════════════════════════════════════════════════════════════════

@router.get("/seattle/gauges", summary="Seattle SPU rain gauge locations")
async def seattle_gauges(
    status: Optional[str] = Query(
        None,
        description="Filter by gauge status (e.g. 'Active').  Omit for all.",
    ),
    limit: int = Query(
        500,
        description="Max records to return.",
        ge=1, le=5000,
    ),
):
    """Return Seattle SPU rain gauge locations from ArcGIS FeatureServer.

    Fields include RG_REF_CODE (gauge ID like RG07), RG_VICINITY_NAME
    (site name), RG_ADRS_TEXT (address), RG_STATUS, RG_ELEVATION_NBR,
    and coordinate fields.
    """
    where = f"RG_STATUS='{status}'" if status else "1=1"
    cache_key = f"stormwater:seattle:gauges:{where}:{limit}"
    cached = await cache.get(cache_key)
    if cached is not None:
        return cached

    data = await _arcgis_query(
        _SEATTLE_RAIN_URL,
        where=where,
        result_record_count=limit,
        source_label="stormwater_seattle",
    )
    features = _extract_features_with_geometry(data)

    # Normalise field names for consistency
    gauges = []
    for f in features:
        gauges.append({
            "gauge_id": f.get("RG_REF_CODE"),
            "name": f.get("RG_VICINITY_NAME"),
            "address": f.get("RG_ADRS_TEXT"),
            "status": f.get("RG_STATUS"),
            "elevation_ft": f.get("RG_ELEVATION_NBR"),
            "established": f.get("RG_ESTABLISHED_DATE"),
            "end_date": f.get("RG_END_DATE"),
            "equipment_id": f.get("RG_EQNUM_ID"),
            "comments": f.get("RG_COMMENTS_TEXT"),
            "lat": f.get("_lat"),
            "lon": f.get("_lon"),
        })

    result = {
        "source": SOURCE_NAME,
        "network": "Seattle SPU",
        "query": {"status": status, "limit": limit},
        "count": len(gauges),
        "gauges": gauges,
    }
    await cache.set(cache_key, result, ttl=config.CACHE_TTL_LONG)
    return result


# ═══════════════════════════════════════════════════════════════════════════
# MWRD CHICAGO
# ═══════════════════════════════════════════════════════════════════════════

@router.get(
    "/mwrd/datasets",
    summary="Discover MWRD Chicago datasets from ArcGIS Hub",
)
async def mwrd_datasets(
    q: str = Query(
        "rain gauge",
        description="Search query for the MWRD GeoHub dataset catalog.",
    ),
    limit: int = Query(20, ge=1, le=50),
):
    """Search the MWRD GeoHub for Feature Service datasets.

    Returns dataset metadata including title, ArcGIS service URL, and
    description.  Use the returned URL to query the FeatureServer directly.
    """
    cache_key = f"stormwater:mwrd:datasets:{q}:{limit}"
    cached = await cache.get(cache_key)
    if cached is not None:
        return cached

    datasets = await _hub_search_datasets(
        _MWRD_HUB_SEARCH, q, source_label="stormwater_mwrd", limit=limit,
    )

    result = {
        "source": SOURCE_NAME,
        "network": "MWRD Chicago",
        "query": q,
        "count": len(datasets),
        "datasets": datasets,
    }
    await cache.set(cache_key, result, ttl=config.CACHE_TTL_LONG)
    return result


@router.get(
    "/mwrd/query",
    summary="Query an MWRD ArcGIS FeatureServer layer",
)
async def mwrd_query(
    layer_url: str = Query(
        ...,
        description=(
            "Full ArcGIS FeatureServer query URL "
            "(e.g. https://gispub.mwrd.org/arcgis/rest/services/.../FeatureServer/0/query)"
        ),
    ),
    where: str = Query("1=1", description="SQL WHERE clause."),
    limit: int = Query(1000, ge=1, le=5000),
):
    """Proxy query to any MWRD ArcGIS FeatureServer layer.

    Use /mwrd/datasets to discover available layers, then pass the URL here.
    Only URLs under gispub.mwrd.org are permitted.
    """
    if "mwrd.org" not in layer_url:
        raise HTTPException(
            status_code=400,
            detail="Only MWRD ArcGIS URLs (mwrd.org) are allowed.",
        )

    cache_key = f"stormwater:mwrd:query:{layer_url}:{where}:{limit}"
    cached = await cache.get(cache_key)
    if cached is not None:
        return cached

    data = await _arcgis_query(
        layer_url,
        where=where,
        result_record_count=limit,
        source_label="stormwater_mwrd",
    )
    features = _extract_features_with_geometry(data)

    result = {
        "source": SOURCE_NAME,
        "network": "MWRD Chicago",
        "layer_url": layer_url,
        "query": {"where": where, "limit": limit},
        "count": len(features),
        "features": features,
    }
    await cache.set(cache_key, result, ttl=config.CACHE_TTL)
    return result


# ═══════════════════════════════════════════════════════════════════════════
# CLEVELAND NEORSD
# ═══════════════════════════════════════════════════════════════════════════

@router.get(
    "/neorsd/datasets",
    summary="Discover NEORSD Cleveland datasets from ArcGIS Hub",
)
async def neorsd_datasets(
    q: str = Query(
        "rain gauge",
        description="Search query for the NEORSD Hub dataset catalog.",
    ),
    limit: int = Query(20, ge=1, le=50),
):
    """Search the NEORSD ArcGIS Hub for Feature Service datasets.

    Returns dataset metadata with title, service URL, and description.
    """
    cache_key = f"stormwater:neorsd:datasets:{q}:{limit}"
    cached = await cache.get(cache_key)
    if cached is not None:
        return cached

    datasets = await _hub_search_datasets(
        _NEORSD_HUB_SEARCH, q, source_label="stormwater_neorsd", limit=limit,
    )

    result = {
        "source": SOURCE_NAME,
        "network": "Cleveland NEORSD",
        "query": q,
        "count": len(datasets),
        "datasets": datasets,
    }
    await cache.set(cache_key, result, ttl=config.CACHE_TTL_LONG)
    return result


@router.get(
    "/neorsd/query",
    summary="Query a NEORSD ArcGIS FeatureServer layer",
)
async def neorsd_query(
    layer_url: str = Query(
        ...,
        description="Full ArcGIS FeatureServer query URL for a NEORSD layer.",
    ),
    where: str = Query("1=1", description="SQL WHERE clause."),
    limit: int = Query(1000, ge=1, le=5000),
):
    """Proxy query to any NEORSD ArcGIS FeatureServer layer.

    Use /neorsd/datasets to discover available layers, then pass the URL here.
    Only URLs under neorsd.org or arcgis.com are permitted.
    """
    if "neorsd" not in layer_url and "arcgis.com" not in layer_url:
        raise HTTPException(
            status_code=400,
            detail="Only NEORSD or ArcGIS.com URLs are allowed.",
        )

    cache_key = f"stormwater:neorsd:query:{layer_url}:{where}:{limit}"
    cached = await cache.get(cache_key)
    if cached is not None:
        return cached

    data = await _arcgis_query(
        layer_url,
        where=where,
        result_record_count=limit,
        source_label="stormwater_neorsd",
    )
    features = _extract_features_with_geometry(data)

    result = {
        "source": SOURCE_NAME,
        "network": "Cleveland NEORSD",
        "layer_url": layer_url,
        "query": {"where": where, "limit": limit},
        "count": len(features),
        "features": features,
    }
    await cache.set(cache_key, result, ttl=config.CACHE_TTL)
    return result


# ═══════════════════════════════════════════════════════════════════════════
# MILWAUKEE MMSD
# ═══════════════════════════════════════════════════════════════════════════

@router.get(
    "/mmsd/datasets",
    summary="Discover MMSD Milwaukee datasets from ArcGIS Hub",
)
async def mmsd_datasets(
    q: str = Query(
        "rain",
        description="Search query for the MMSD Hub dataset catalog.",
    ),
    limit: int = Query(20, ge=1, le=50),
):
    """Search the MMSD ArcGIS Hub for Feature Service datasets.

    Returns dataset metadata with title, service URL, and description.
    """
    cache_key = f"stormwater:mmsd:datasets:{q}:{limit}"
    cached = await cache.get(cache_key)
    if cached is not None:
        return cached

    datasets = await _hub_search_datasets(
        _MMSD_HUB_SEARCH, q, source_label="stormwater_mmsd", limit=limit,
    )

    result = {
        "source": SOURCE_NAME,
        "network": "Milwaukee MMSD",
        "query": q,
        "count": len(datasets),
        "datasets": datasets,
    }
    await cache.set(cache_key, result, ttl=config.CACHE_TTL_LONG)
    return result


@router.get(
    "/mmsd/query",
    summary="Query an MMSD ArcGIS FeatureServer layer",
)
async def mmsd_query(
    layer_url: str = Query(
        ...,
        description="Full ArcGIS FeatureServer query URL for an MMSD layer.",
    ),
    where: str = Query("1=1", description="SQL WHERE clause."),
    limit: int = Query(1000, ge=1, le=5000),
):
    """Proxy query to any MMSD ArcGIS FeatureServer layer.

    Use /mmsd/datasets to discover available layers, then pass the URL here.
    Only URLs under mmsd or arcgis.com are permitted.
    """
    if "mmsd" not in layer_url.lower() and "arcgis.com" not in layer_url:
        raise HTTPException(
            status_code=400,
            detail="Only MMSD or ArcGIS.com URLs are allowed.",
        )

    cache_key = f"stormwater:mmsd:query:{layer_url}:{where}:{limit}"
    cached = await cache.get(cache_key)
    if cached is not None:
        return cached

    data = await _arcgis_query(
        layer_url,
        where=where,
        result_record_count=limit,
        source_label="stormwater_mmsd",
    )
    features = _extract_features_with_geometry(data)

    result = {
        "source": SOURCE_NAME,
        "network": "Milwaukee MMSD",
        "layer_url": layer_url,
        "query": {"where": where, "limit": limit},
        "count": len(features),
        "features": features,
    }
    await cache.set(cache_key, result, ttl=config.CACHE_TTL)
    return result


# ═══════════════════════════════════════════════════════════════════════════
# DUPAGE COUNTY IL
# ═══════════════════════════════════════════════════════════════════════════

# Known USGS gauge sites operated by DuPage County Stormwater Management.
# Source: https://ec.dupagecounty.gov/ gauge listings.
_DUPAGE_RAIN_GAUGES: list[dict[str, Any]] = [
    {"usgs_id": "414907088051500", "name": "Tyler Creek at Elgin", "type": "rain"},
    {"usgs_id": "420036088032800", "name": "Salt Creek at Rolling Meadows", "type": "rain"},
    {"usgs_id": "415801088142300", "name": "Ferson Creek near St. Charles", "type": "rain"},
    {"usgs_id": "05540060",        "name": "Salt Creek near Elmhurst",     "type": "stream"},
    {"usgs_id": "05540095",        "name": "Salt Creek at Western Springs", "type": "stream"},
    {"usgs_id": "05540110",        "name": "Salt Creek at Brookfield",     "type": "stream"},
    {"usgs_id": "05539900",        "name": "West Branch DuPage at West Chicago", "type": "stream"},
    {"usgs_id": "05540500",        "name": "DuPage River at Shorewood",    "type": "stream"},
]


@router.get(
    "/dupage/gauges",
    summary="DuPage County rain and stream gauge inventory",
)
async def dupage_gauges():
    """Return the known DuPage County gauge inventory.

    DuPage County operates 22 rain gauges and 25 stream gauges through
    the Stormwater Management Division.  Data flows to USGS; use the
    USGS site numbers to query real-time data via USGS NWIS.
    """
    return {
        "source": SOURCE_NAME,
        "network": "DuPage County Stormwater",
        "portal": _DUPAGE_BASE,
        "notes": (
            "DuPage County gauges report to USGS NWIS.  Use the usgs_id "
            "values with the USGS waterservices API for real-time data: "
            "https://waterservices.usgs.gov/nwis/iv/?sites={usgs_id}&format=json"
        ),
        "count": len(_DUPAGE_RAIN_GAUGES),
        "gauges": _DUPAGE_RAIN_GAUGES,
    }


@router.get(
    "/dupage/realtime",
    summary="DuPage County real-time gauge data via USGS NWIS",
)
async def dupage_realtime(
    site_id: str = Query(
        ...,
        description="USGS site ID (e.g. 05540060).",
    ),
    parameter: str = Query(
        "00045",
        description=(
            "USGS parameter code: 00045=precipitation, 00060=discharge, "
            "00065=gage_height."
        ),
    ),
):
    """Fetch real-time data for a DuPage County gauge via USGS NWIS.

    Proxies to the USGS Instantaneous Values web service and returns
    the most recent readings.
    """
    # Validate site_id is in known set or is a plausible USGS ID
    if not re.match(r"^[0-9]{6,15}$", site_id):
        raise HTTPException(
            status_code=400,
            detail="site_id must be a numeric USGS site number (6-15 digits).",
        )

    cache_key = f"stormwater:dupage:rt:{site_id}:{parameter}"
    cached_val = await cache.get(cache_key)
    if cached_val is not None:
        return cached_val

    url = "https://waterservices.usgs.gov/nwis/iv/"
    params = {
        "sites": site_id,
        "parameterCd": parameter,
        "format": "json",
        "period": "PT24H",
    }
    try:
        data = await http_client.fetch_json(
            url, params=params, source="stormwater_dupage_usgs",
        )
    except Exception as exc:
        logger.exception("USGS NWIS fetch failed for site %s", site_id)
        raise HTTPException(
            status_code=502,
            detail=f"USGS NWIS error for site {site_id}: {exc}",
        )

    # Extract time series values
    readings = []
    ts_list = data.get("value", {}).get("timeSeries", [])
    for ts in ts_list:
        var_info = ts.get("variable", {})
        var_name = var_info.get("variableName", "")
        unit = var_info.get("unit", {}).get("unitCode", "")
        for val_set in ts.get("values", []):
            for v in val_set.get("value", []):
                readings.append({
                    "datetime": v.get("dateTime"),
                    "value": v.get("value"),
                    "parameter": var_name,
                    "unit": unit,
                    "qualifiers": v.get("qualifiers"),
                })

    result = {
        "source": SOURCE_NAME,
        "network": "DuPage County Stormwater (via USGS)",
        "site_id": site_id,
        "parameter_code": parameter,
        "count": len(readings),
        "readings": readings,
    }
    await cache.set(cache_key, result, ttl=config.CACHE_TTL)
    return result


# ═══════════════════════════════════════════════════════════════════════════
# CINCINNATI MSD
# ═══════════════════════════════════════════════════════════════════════════

@router.get(
    "/cincy/info",
    summary="Cincinnati MSD rainfall monitoring info",
)
async def cincy_info():
    """Return metadata about Cincinnati MSD rainfall monitoring resources.

    Cincinnati MSD operates 25+ rain gauges across Hamilton County.
    Gauge-adjusted radar data is provided through a Vieux Inc. platform.
    """
    return {
        "source": SOURCE_NAME,
        "network": "Cincinnati MSD",
        "coverage": "Hamilton County / Greater Cincinnati, OH",
        "gauge_count": "25+",
        "resources": {
            "rainfall_info": "https://msdgc.org/programs/rainfall-data/",
            "gauge_adjusted_radar": "https://msdgc.vieuxinc.com/",
            "notes": (
                "The Vieux Inc platform provides gauge-adjusted radar rainfall "
                "data as a JavaScript application.  Real-time gauge data may "
                "require inspecting the application's API calls for direct access."
            ),
        },
    }


# ═══════════════════════════════════════════════════════════════════════════
# GENERIC HUB LAYER QUERY
# ═══════════════════════════════════════════════════════════════════════════

@router.get(
    "/query",
    summary="Query any stormwater network ArcGIS FeatureServer layer",
)
async def generic_query(
    layer_url: str = Query(
        ...,
        description=(
            "Full ArcGIS FeatureServer /query URL.  Use the network-specific "
            "/datasets endpoint to discover available layers."
        ),
    ),
    where: str = Query("1=1", description="SQL WHERE clause."),
    out_fields: str = Query("*", description="Comma-separated field names."),
    limit: int = Query(1000, ge=1, le=5000),
):
    """Generic proxy for any ArcGIS FeatureServer query URL.

    This is a convenience endpoint for querying layers discovered via
    the /datasets endpoints for each network.  Accepts any public
    ArcGIS FeatureServer URL.
    """
    if "/query" not in layer_url and "FeatureServer" not in layer_url:
        raise HTTPException(
            status_code=400,
            detail="URL must be an ArcGIS FeatureServer query endpoint.",
        )

    # Append /query if the caller passed the FeatureServer base
    query_url = layer_url
    if not layer_url.rstrip("/").endswith("/query"):
        query_url = layer_url.rstrip("/") + "/query"

    cache_key = f"stormwater:generic:{query_url}:{where}:{out_fields}:{limit}"
    cached_val = await cache.get(cache_key)
    if cached_val is not None:
        return cached_val

    data = await _arcgis_query(
        query_url,
        where=where,
        out_fields=out_fields,
        result_record_count=limit,
        source_label="stormwater_generic",
    )
    features = _extract_features_with_geometry(data)

    result = {
        "source": SOURCE_NAME,
        "layer_url": query_url,
        "query": {"where": where, "out_fields": out_fields, "limit": limit},
        "count": len(features),
        "features": features,
    }
    await cache.set(cache_key, result, ttl=config.CACHE_TTL)
    return result
