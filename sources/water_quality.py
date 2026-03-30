"""Water Quality Portal — EPA/USGS/State water quality data (430M+ records).

The Water Quality Portal (WQP) is a cooperative service sponsored by the
United States Geological Survey (USGS), the Environmental Protection Agency
(EPA), and the National Water Quality Monitoring Council (NWQMC).  It provides
a unified access point for water quality data collected by over 400 federal,
state, tribal, and local agencies.

No authentication required.  REST API with JSON and CSV responses.

API reference: https://www.waterqualitydata.us/webservices_documentation/
"""
from __future__ import annotations

import logging
from typing import Any, Optional

import httpx
from fastapi import APIRouter, HTTPException, Query

import cache
import config
from models import Station

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Source registry metadata  (read by server.py /api/sources)
# ---------------------------------------------------------------------------

SOURCE_NAME = "Water Quality Portal"
SOURCE_DESCRIPTION = (
    "The Water Quality Portal (WQP) aggregates water quality monitoring data "
    "from USGS, EPA, and 400+ state/tribal/local agencies.  Over 430 million "
    "result records from across the United States.  Parameters include "
    "nutrients, metals, pesticides, bacteria, physical properties, and more.  "
    "No authentication required."
)
SOURCE_META = {
    "url": "https://www.waterqualitydata.us/",
    "auth_required": False,
    "update_frequency": "Varies by provider (daily to quarterly)",
    "coverage": "United States — all 50 states, territories, and tribal lands",
    "params": [
        "nutrients", "metals", "pesticides", "bacteria",
        "temperature", "ph", "dissolved_oxygen", "conductivity",
        "turbidity", "sediment",
    ],
    "category": "water_quality",
    "station_count": 950000,
}

router = APIRouter(prefix="/api/water_quality", tags=["Water Quality Portal"])

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

WQP_BASE = "https://www.waterqualitydata.us/data"

_HEADERS = {
    "User-Agent": config.USER_AGENT,
    "Accept": "application/json",
}

# US state abbreviation → FIPS state code (used in WQP statecode param)
_STATE_FIPS: dict[str, str] = {
    "AL": "01", "AK": "02", "AZ": "04", "AR": "05", "CA": "06",
    "CO": "08", "CT": "09", "DE": "10", "FL": "12", "GA": "13",
    "HI": "15", "ID": "16", "IL": "17", "IN": "18", "IA": "19",
    "KS": "20", "KY": "21", "LA": "22", "ME": "23", "MD": "24",
    "MA": "25", "MI": "26", "MN": "27", "MS": "28", "MO": "29",
    "MT": "30", "NE": "31", "NV": "32", "NH": "33", "NJ": "34",
    "NM": "35", "NY": "36", "NC": "37", "ND": "38", "OH": "39",
    "OK": "40", "OR": "41", "PA": "42", "RI": "44", "SC": "45",
    "SD": "46", "TN": "47", "TX": "48", "UT": "49", "VT": "50",
    "VA": "51", "WA": "53", "WV": "54", "WI": "55", "WY": "56",
    "DC": "11", "PR": "72", "VI": "78", "GU": "66", "AS": "60",
}


# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------

def _client() -> httpx.AsyncClient:
    return httpx.AsyncClient(
        headers=_HEADERS,
        timeout=config.HTTP_TIMEOUT,
        follow_redirects=True,
    )


async def _wqp_get(path: str, params: dict[str, Any]) -> list[dict]:
    """Query a WQP Station/search endpoint; return the JSON array of records."""
    url = f"{WQP_BASE}/{path}"
    try:
        async with _client() as c:
            resp = await c.get(url, params=params)
            resp.raise_for_status()
            return resp.json()
    except httpx.TimeoutException:
        raise HTTPException(
            status_code=504,
            detail="Water Quality Portal API timeout",
        )
    except httpx.HTTPStatusError as exc:
        raise HTTPException(
            status_code=exc.response.status_code,
            detail=f"Water Quality Portal error: {exc}",
        )
    except Exception as exc:
        logger.exception("Error querying Water Quality Portal")
        raise HTTPException(status_code=502, detail=str(exc))


# ---------------------------------------------------------------------------
# Parsing helpers
# ---------------------------------------------------------------------------

def _safe_float(val: Any, default: float = 0.0) -> float:
    """Convert a value to float, returning *default* on failure."""
    if val is None:
        return default
    try:
        return float(val)
    except (TypeError, ValueError):
        return default


def _parse_station(raw: dict) -> Station:
    """Convert a WQP station record to the shared Station model."""
    org_id = raw.get("OrganizationIdentifier", "")
    site_id = raw.get("MonitoringLocationIdentifier", "")
    name = raw.get("MonitoringLocationName", site_id)
    lat = _safe_float(raw.get("LatitudeMeasure"))
    lon = _safe_float(raw.get("LongitudeMeasure"))
    elevation = None
    elev_raw = raw.get("VerticalMeasure", {}).get("MeasureValue")
    if elev_raw is not None:
        elevation = _safe_float(elev_raw) if elev_raw != "" else None

    loc_type = raw.get("MonitoringLocationTypeName", "")
    huc = raw.get("HUCEightDigitCode", "")
    state_code = raw.get("StateCode", "")
    county_code = raw.get("CountyCode", "")
    provider = raw.get("ProviderName", "")

    return Station(
        id=site_id,
        name=name or site_id,
        lat=lat,
        lon=lon,
        source="wqp",
        network=provider or org_id,
        params=["water_quality"],
        elevation=elevation,
        url=f"https://www.waterqualitydata.us/provider/{provider}/{org_id}/{site_id}/",
        extra={
            "organization_id": org_id,
            "location_type": loc_type,
            "huc8": huc,
            "state_code": state_code,
            "county_code": county_code,
            "provider": provider,
        },
    )


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@router.get("/stations", summary="Water Quality Portal — stations by state")
async def stations(
    state: str = Query(
        ...,
        description="Two-letter US state abbreviation (e.g., 'OR', 'CA', 'TX')",
        min_length=2,
        max_length=2,
    ),
    limit: int = Query(500, ge=1, le=5000, description="Max number of stations to return"),
    offset: int = Query(0, ge=0, description="Pagination offset (startIndex)"),
):
    """Return water quality monitoring stations for a US state.

    Queries the Water Quality Portal Station service filtered by state FIPS code.
    Results include station location, organization, and HUC-8 watershed code.
    """
    state_upper = state.upper()
    fips = _STATE_FIPS.get(state_upper)
    if not fips:
        raise HTTPException(
            status_code=400,
            detail=f"Unknown state abbreviation: '{state}'. Use two-letter code (e.g., OR, CA).",
        )

    cache_key = f"wqp:stations:{state_upper}:{limit}:{offset}"
    cached_result = await cache.get(cache_key)
    if cached_result is not None:
        return cached_result

    params: dict[str, Any] = {
        "statecode": f"US:{fips}",
        "mimeType": "json",
        "sorted": "no",
        "startIndex": offset,
    }

    rows = await _wqp_get("Station/search", params)

    # WQP may return more than requested; trim to limit
    station_list = [_parse_station(row).model_dump() for row in rows[:limit]]
    result = {
        "source": "wqp",
        "state": state_upper,
        "count": len(station_list),
        "limit": limit,
        "offset": offset,
        "stations": station_list,
    }
    await cache.set(cache_key, result, ttl=config.CACHE_TTL_LONG)
    return result


@router.get("/stations/nearby", summary="Water Quality Portal — stations near a point")
async def stations_nearby(
    lat: float = Query(..., ge=-90, le=90, description="Latitude"),
    lon: float = Query(..., ge=-180, le=180, description="Longitude"),
    radius: float = Query(25, ge=1, le=200, description="Search radius in miles"),
    limit: int = Query(100, ge=1, le=2000, description="Max number of stations to return"),
):
    """Return water quality monitoring stations within a radius of a lat/lon point.

    The WQP 'within' parameter searches by great-circle distance in miles.
    """
    cache_key = f"wqp:nearby:{lat:.3f}:{lon:.3f}:{radius}:{limit}"
    cached_result = await cache.get(cache_key)
    if cached_result is not None:
        return cached_result

    params: dict[str, Any] = {
        "lat": str(lat),
        "long": str(lon),
        "within": str(radius),
        "mimeType": "json",
        "sorted": "no",
    }

    rows = await _wqp_get("Station/search", params)

    station_list = [_parse_station(row).model_dump() for row in rows[:limit]]
    result = {
        "source": "wqp",
        "center": {"lat": lat, "lon": lon},
        "radius_miles": radius,
        "count": len(station_list),
        "limit": limit,
        "stations": station_list,
    }
    await cache.set(cache_key, result, ttl=config.CACHE_TTL_LONG)
    return result


@router.get("/overview", summary="Water Quality Portal — source overview")
async def overview():
    """Return summary information about the Water Quality Portal data source."""
    return {
        "module": SOURCE_NAME,
        "description": SOURCE_DESCRIPTION,
        "sources": [
            {
                "id": "wqp",
                "name": "Water Quality Portal",
                "description": (
                    "Unified access to water quality data from USGS, EPA, and "
                    "400+ state/tribal/local agencies.  Over 430 million result "
                    "records covering nutrients, metals, pesticides, bacteria, "
                    "physical properties, and more."
                ),
                "url": "https://www.waterqualitydata.us/",
                "api_docs": "https://www.waterqualitydata.us/webservices_documentation/",
                "auth_required": False,
                "update_frequency": "Varies by provider (daily to quarterly)",
                "coverage": "United States — all 50 states, territories, and tribal lands",
                "station_count": "~950,000",
                "providers": ["USGS", "EPA (STORET)", "400+ state/tribal/local agencies"],
                "params": SOURCE_META["params"],
                "endpoints": [
                    "GET /api/water_quality/stations?state=OR",
                    "GET /api/water_quality/stations/nearby?lat=45.5&lon=-122.6&radius=25",
                    "GET /api/water_quality/overview",
                ],
            }
        ],
    }
