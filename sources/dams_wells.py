"""Dam and Groundwater Monitoring — USACE NID, CA DWR Wells, AZ GWSI.

Three no-auth ArcGIS FeatureServer sources:

1. USACE National Inventory of Dams (NID)
   91,000+ dams with 75+ fields — hazard classification, condition
   assessment, structural details, and regulatory data.
   https://services2.arcgis.com/FiaPA4ga0iQKduv3/arcgis/rest/services/National_Inventory_of_Dams/FeatureServer/0/

2. California DWR Monitoring Wells
   Enterprise Monitoring Well Network — groundwater level monitoring wells
   across California, operated by the Department of Water Resources.
   https://services.arcgis.com/aa38u6OgfNoCkTJ6/arcgis/rest/services/Enterprise_Monitoring_Well_Network_gdb/FeatureServer/0/

3. Arizona GWSI Sites (2024)
   45,984 groundwater sites from the Arizona Groundwater Site Inventory,
   including well depth, water level, and aquifer information.
   https://services.arcgis.com/C34zQ7veRS0V1t04/arcgis/rest/services/GWSI_Sites_2024/FeatureServer/0/

All three APIs are public ArcGIS FeatureServers requiring no authentication.
"""
from __future__ import annotations

import logging
from typing import Any, Optional

from fastapi import APIRouter, HTTPException, Query

import cache
import config
import http_client
from models import Station

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Source registry metadata (consumed by server.py /api/sources)
# ---------------------------------------------------------------------------

SOURCE_NAME = "Dams & Groundwater"
SOURCE_DESCRIPTION = (
    "USACE National Inventory of Dams (91K+ dams, 75+ fields), "
    "California DWR Monitoring Wells (groundwater), and "
    "Arizona GWSI Sites (45,984 groundwater sites).  "
    "All public ArcGIS FeatureServers, no auth required."
)
SOURCE_META = {
    "url": "https://nid.sec.usace.army.mil/",
    "auth_required": False,
    "update_frequency": "annual (NID) / varies (wells)",
    "coverage": "United States (NID), California (DWR), Arizona (GWSI)",
    "params": [
        "dam_height", "storage", "hazard_class", "condition",
        "groundwater_level", "well_depth",
    ],
    "category": "water",
}

router = APIRouter(prefix="/api/dams_wells", tags=["Dams & Groundwater"])

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_NID_URL = (
    "https://services2.arcgis.com/FiaPA4ga0iQKduv3/arcgis/rest/services/"
    "National_Inventory_of_Dams/FeatureServer/0/query"
)
_NID_FIELDS = (
    "DAM_NAME,STATE,COUNTY,RIVER,DAM_HEIGHT,NID_STORAGE,"
    "HAZARD,CONDITION_ASSESSMENT,LATITUDE,LONGITUDE"
)

_CA_WELLS_URL = (
    "https://services.arcgis.com/aa38u6OgfNoCkTJ6/arcgis/rest/services/"
    "Enterprise_Monitoring_Well_Network_gdb/FeatureServer/0/query"
)

_AZ_GWSI_URL = (
    "https://services.arcgis.com/C34zQ7veRS0V1t04/arcgis/rest/services/"
    "GWSI_Sites_2024/FeatureServer/0/query"
)

_MAX_RECORD_COUNT = 5000


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

async def _arcgis_query(
    url: str,
    where: str = "1=1",
    out_fields: str = "*",
    limit: int = _MAX_RECORD_COUNT,
    label: str = "ArcGIS",
) -> list[dict[str, Any]]:
    """Execute an ArcGIS FeatureServer query and return feature attributes."""
    params = {
        "where": where,
        "outFields": out_fields,
        "resultRecordCount": min(limit, _MAX_RECORD_COUNT),
        "f": "json",
    }
    data = await http_client.fetch_json(url, params=params, source=label)
    if "error" in data:
        err = data["error"]
        raise HTTPException(
            status_code=502,
            detail=f"{label} ArcGIS error: {err.get('message', err)}",
        )
    features = data.get("features", [])
    return [f.get("attributes", {}) for f in features]


def _safe_float(val: Any) -> Optional[float]:
    """Coerce a value to float, returning None on failure."""
    if val is None:
        return None
    try:
        return float(val)
    except (TypeError, ValueError):
        return None


# ---------------------------------------------------------------------------
# 1. USACE National Inventory of Dams
# ---------------------------------------------------------------------------

def _parse_nid_station(rec: dict) -> Station:
    """Convert an NID feature to a Station object."""
    name = rec.get("DAM_NAME") or "Unknown Dam"
    lat = _safe_float(rec.get("LATITUDE")) or 0.0
    lon = _safe_float(rec.get("LONGITUDE")) or 0.0
    state = rec.get("STATE") or ""
    county = rec.get("COUNTY") or ""

    return Station(
        id=f"nid:{name}:{state}:{county}".replace(" ", "_"),
        name=str(name),
        lat=lat,
        lon=lon,
        source="nid",
        network="USACE National Inventory of Dams",
        params=["dam_height", "storage", "hazard_class", "condition"],
        extra={
            "state": state,
            "county": county,
            "river": rec.get("RIVER"),
            "dam_height_ft": _safe_float(rec.get("DAM_HEIGHT")),
            "nid_storage_acre_ft": _safe_float(rec.get("NID_STORAGE")),
            "hazard": rec.get("HAZARD"),
            "condition_assessment": rec.get("CONDITION_ASSESSMENT"),
        },
    )


@router.get("/nid", summary="USACE National Inventory of Dams")
@cache.cached(ttl=3600)
async def nid_dams(
    state: Optional[str] = Query(None, description="Two-letter state code filter (e.g., OR, CA)"),
    limit: int = Query(5000, ge=1, le=5000, description="Max records to return"),
):
    """Query the USACE National Inventory of Dams (91K+ dams, 75+ fields).

    Returns dam name, state, county, river, height, storage capacity,
    hazard classification, and condition assessment.
    """
    where = f"STATE='{state.upper()}'" if state else "1=1"
    records = await _arcgis_query(
        _NID_URL,
        where=where,
        out_fields=_NID_FIELDS,
        limit=limit,
        label="USACE NID",
    )
    stations = [_parse_nid_station(r) for r in records]
    return {
        "source": "USACE National Inventory of Dams",
        "count": len(stations),
        "stations": [s.model_dump() for s in stations],
    }


# ---------------------------------------------------------------------------
# 2. California DWR Monitoring Wells
# ---------------------------------------------------------------------------

def _parse_ca_well(rec: dict) -> Station:
    """Convert a CA DWR monitoring well feature to a Station object."""
    site_code = str(rec.get("SITE_CODE") or rec.get("SiteCode") or rec.get("OBJECTID") or "unknown")
    name = rec.get("WELL_NAME") or rec.get("WellName") or rec.get("SITE_CODE") or site_code
    lat = _safe_float(rec.get("LATITUDE") or rec.get("Latitude")) or 0.0
    lon = _safe_float(rec.get("LONGITUDE") or rec.get("Longitude")) or 0.0

    return Station(
        id=f"ca_dwr_well:{site_code}",
        name=str(name),
        lat=lat,
        lon=lon,
        source="ca_dwr_wells",
        network="California DWR Monitoring Wells",
        params=["groundwater_level", "well_depth"],
        extra={k: v for k, v in rec.items() if v is not None},
    )


@router.get("/ca_wells", summary="California DWR monitoring wells")
@cache.cached(ttl=3600)
async def ca_wells(
    limit: int = Query(2000, ge=1, le=5000, description="Max records to return"),
):
    """Query the California DWR Enterprise Monitoring Well Network.

    Returns groundwater monitoring wells with location, well metadata,
    and measurement information.
    """
    records = await _arcgis_query(
        _CA_WELLS_URL,
        out_fields="*",
        limit=limit,
        label="CA DWR Wells",
    )
    stations = [_parse_ca_well(r) for r in records]
    return {
        "source": "California DWR Monitoring Wells",
        "count": len(stations),
        "stations": [s.model_dump() for s in stations],
    }


# ---------------------------------------------------------------------------
# 3. Arizona GWSI Sites
# ---------------------------------------------------------------------------

def _parse_az_gwsi(rec: dict) -> Station:
    """Convert an AZ GWSI feature to a Station object."""
    site_id = str(rec.get("SITE_ID") or rec.get("SiteID") or rec.get("OBJECTID") or "unknown")
    name = rec.get("SITE_NAME") or rec.get("SiteName") or site_id
    lat = _safe_float(rec.get("LATITUDE") or rec.get("Latitude") or rec.get("LAT_DD")) or 0.0
    lon = _safe_float(rec.get("LONGITUDE") or rec.get("Longitude") or rec.get("LON_DD")) or 0.0

    return Station(
        id=f"az_gwsi:{site_id}",
        name=str(name),
        lat=lat,
        lon=lon,
        source="az_gwsi",
        network="Arizona GWSI Sites",
        params=["groundwater_level", "well_depth"],
        extra={k: v for k, v in rec.items() if v is not None},
    )


@router.get("/az_wells", summary="Arizona GWSI groundwater sites")
@cache.cached(ttl=3600)
async def az_wells(
    limit: int = Query(5000, ge=1, le=5000, description="Max records to return"),
):
    """Query Arizona Groundwater Site Inventory (45,984 sites).

    Returns groundwater well locations with depth, water level,
    and aquifer information.
    """
    records = await _arcgis_query(
        _AZ_GWSI_URL,
        out_fields="*",
        limit=limit,
        label="AZ GWSI",
    )
    stations = [_parse_az_gwsi(r) for r in records]
    return {
        "source": "Arizona GWSI Sites",
        "count": len(stations),
        "stations": [s.model_dump() for s in stations],
    }


# ---------------------------------------------------------------------------
# 4. Overview
# ---------------------------------------------------------------------------

@router.get("/overview", summary="Dams & Groundwater — source overview")
async def overview():
    """Return summary information about all dam and groundwater data sources."""
    return {
        "module": SOURCE_NAME,
        "description": SOURCE_DESCRIPTION,
        "sources": [
            {
                "id": "nid",
                "name": "USACE National Inventory of Dams",
                "description": (
                    "91,000+ dams across the United States with 75+ fields "
                    "including hazard classification, condition assessment, "
                    "structural details, and regulatory information."
                ),
                "url": "https://nid.sec.usace.army.mil/",
                "auth_required": False,
                "update_frequency": "Annual",
                "coverage": "United States",
                "station_count": "91,000+",
                "params": ["dam_height", "storage", "hazard_class", "condition"],
                "endpoints": [
                    "GET /api/dams_wells/nid?state=OR&limit=5000",
                ],
            },
            {
                "id": "ca_dwr_wells",
                "name": "California DWR Monitoring Wells",
                "description": (
                    "Enterprise Monitoring Well Network operated by the "
                    "California Department of Water Resources for groundwater "
                    "level monitoring across the state."
                ),
                "url": "https://water.ca.gov/",
                "auth_required": False,
                "update_frequency": "Varies",
                "coverage": "California",
                "params": ["groundwater_level", "well_depth"],
                "endpoints": [
                    "GET /api/dams_wells/ca_wells?limit=2000",
                ],
            },
            {
                "id": "az_gwsi",
                "name": "Arizona GWSI Sites (2024)",
                "description": (
                    "45,984 groundwater sites from the Arizona Groundwater "
                    "Site Inventory including well depth, water level, "
                    "and aquifer information."
                ),
                "url": "https://azwater.gov/",
                "auth_required": False,
                "update_frequency": "Annual",
                "coverage": "Arizona",
                "station_count": "45,984",
                "params": ["groundwater_level", "well_depth"],
                "endpoints": [
                    "GET /api/dams_wells/az_wells?limit=5000",
                ],
            },
        ],
    }
