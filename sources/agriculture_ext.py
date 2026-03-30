"""Agriculture & Land Data — USDM Drought Monitor, NSF NEON, USA-NPN Phenology.

Integrates three complementary agriculture/soil/ecology APIs:

1. USDM Drought Monitor (University of Nebraska-Lincoln)
   US Drought Monitor statistics by state or nationally.  CSV/JSON, no auth.
   API: https://usdmdataservices.unl.edu/api/

2. NSF NEON (National Ecological Observatory Network)
   180+ data products (soil temp, soil moisture, eddy flux, etc.) from 81 field
   sites across 20 eco-climatic domains.  JSON, optional token.
   API: https://data.neonscience.org/api/v0/

3. USA-NPN (National Phenology Network)
   Phenological observations (first bloom, leaf-out, migration, etc.)
   contributed by volunteer and professional observers.  JSON, no auth.
   API: https://www.usanpn.org/npn_portal/
"""
from __future__ import annotations

import logging
from datetime import date, datetime, timedelta, timezone
from typing import Any, Optional

import httpx
from fastapi import APIRouter, HTTPException, Query

import cache
import config

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Source registry metadata  (read by server.py /api/sources)
# ---------------------------------------------------------------------------

SOURCE_NAME = "Agriculture & Land"
SOURCE_DESCRIPTION = (
    "US Drought Monitor statistics (state and national), NSF NEON ecological "
    "observatory data (81 sites, 180+ products), and USA-NPN phenology "
    "observations (bloom, leaf-out, migration)."
)
SOURCE_META = {
    "url": "https://usdmdataservices.unl.edu/api/",
    "auth_required": False,
    "update_frequency": "Weekly (USDM), Varies (NEON), Seasonal (NPN)",
    "coverage": "United States",
    "params": [
        "drought_severity", "soil_temperature", "soil_moisture",
        "eddy_flux", "phenology", "leaf_out", "first_bloom",
    ],
    "category": "agriculture",
}

router = APIRouter(prefix="/api/agriculture", tags=["Agriculture & Land"])

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

USDM_BASE = "https://usdmdataservices.unl.edu/api"
NEON_BASE = "https://data.neonscience.org/api/v0"
NPN_BASE = "https://www.usanpn.org/npn_portal"

# State abbreviation → FIPS code (for USDM state-level queries)
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
    "DC": "11", "PR": "72",
}

# USDM drought category labels
_DROUGHT_LABELS: dict[str, str] = {
    "None": "No Drought",
    "D0": "Abnormally Dry",
    "D1": "Moderate Drought",
    "D2": "Severe Drought",
    "D3": "Extreme Drought",
    "D4": "Exceptional Drought",
}

# Optional NEON API token (increases rate limit from 200 → 2000 req/min)
NEON_TOKEN = config.__dict__.get("NEON_TOKEN", "") or ""


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

async def _get_json(
    url: str,
    params: dict[str, str] | None = None,
    headers: dict[str, str] | None = None,
    timeout: int = config.HTTP_TIMEOUT,
) -> Any:
    """GET request returning parsed JSON."""
    hdrs = {"User-Agent": config.USER_AGENT, "Accept": "application/json"}
    if headers:
        hdrs.update(headers)
    async with httpx.AsyncClient(timeout=timeout) as client:
        resp = await client.get(url, params=params, headers=hdrs)
        resp.raise_for_status()
        return resp.json()


def _resolve_state_fips(state: str) -> str:
    """Turn a state abbreviation (e.g. 'OR') into a FIPS code, or raise."""
    code = state.upper().strip()
    fips = _STATE_FIPS.get(code)
    if not fips:
        raise HTTPException(
            status_code=400,
            detail=f"Unknown state abbreviation: {state!r}.  Use two-letter code (e.g. OR, CA, TX).",
        )
    return fips


def _parse_usdm_records(raw: list[dict]) -> list[dict]:
    """Normalise USDM response records into a cleaner shape."""
    results = []
    for rec in raw:
        # USDM returns keys like 'None', 'D0', 'D1', … 'D4' with pct values
        entry: dict[str, Any] = {
            "date": rec.get("MapDate") or rec.get("ReleaseDate") or rec.get("ValidStart"),
            "area_name": rec.get("Name") or rec.get("StateName"),
            "fips": rec.get("FIPS") or rec.get("StateAbbreviation"),
        }
        for cat_key, label in _DROUGHT_LABELS.items():
            val = rec.get(cat_key)
            if val is not None:
                try:
                    entry[cat_key] = round(float(val), 2)
                except (ValueError, TypeError):
                    entry[cat_key] = val
        results.append(entry)
    return results


# ---------------------------------------------------------------------------
# 1. USDM Drought — state level
# ---------------------------------------------------------------------------

@router.get(
    "/drought",
    summary="Current drought status for a US state",
)
async def drought_state(
    state: str = Query(..., description="Two-letter state abbreviation (e.g. OR, CA)"),
    days: int = Query(90, ge=1, le=730, description="Lookback window in days"),
):
    """Return USDM Drought Monitor county-level statistics for a state.

    Percentages represent the fraction of the state's area in each drought
    category (None, D0–D4) for each weekly report in the window.
    """
    fips = _resolve_state_fips(state)
    cache_key = f"agriculture:drought:{fips}:{days}"
    cached = await cache.get(cache_key)
    if cached is not None:
        return cached

    end = date.today()
    start = end - timedelta(days=days)
    url = (
        f"{USDM_BASE}/CountyStatistics/"
        f"GetDroughtSeverityStatisticsByAreaPercent"
    )
    params = {
        "aoi": fips,
        "startdate": start.strftime("%-m/%-d/%Y") if hasattr(start, 'strftime') else f"{start.month}/{start.day}/{start.year}",
        "enddate": end.strftime("%-m/%-d/%Y") if hasattr(end, 'strftime') else f"{end.month}/{end.day}/{end.year}",
        "statisticsType": "2",
    }

    try:
        # strftime %-m is not portable on Windows; build manually
        params["startdate"] = f"{start.month}/{start.day}/{start.year}"
        params["enddate"] = f"{end.month}/{end.day}/{end.year}"
        raw = await _get_json(url, params=params)
    except httpx.HTTPStatusError as exc:
        raise HTTPException(status_code=502, detail=f"USDM API error: {exc.response.status_code}")
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"USDM request failed: {exc}")

    records = _parse_usdm_records(raw) if isinstance(raw, list) else raw
    result = {
        "source": "usdm",
        "state": state.upper(),
        "fips": fips,
        "start_date": str(start),
        "end_date": str(end),
        "drought_categories": dict(_DROUGHT_LABELS),
        "records": records,
    }
    await cache.set(cache_key, result, ttl=config.CACHE_TTL_LONG)
    return result


# ---------------------------------------------------------------------------
# 2. USDM Drought — national summary
# ---------------------------------------------------------------------------

@router.get(
    "/drought/national",
    summary="National drought summary from USDM",
)
async def drought_national(
    days: int = Query(90, ge=1, le=730, description="Lookback window in days"),
):
    """Return national-level USDM Drought Monitor statistics.

    Shows what percentage of the contiguous US falls into each drought
    category for each weekly report.
    """
    cache_key = f"agriculture:drought:national:{days}"
    cached = await cache.get(cache_key)
    if cached is not None:
        return cached

    end = date.today()
    start = end - timedelta(days=days)
    url = f"{USDM_BASE}/USStatistics/GetDroughtSeverityStatisticsByArea"
    params = {
        "aoi": "us",
        "startdate": f"{start.month}/{start.day}/{start.year}",
        "enddate": f"{end.month}/{end.day}/{end.year}",
        "statisticsType": "1",
    }

    try:
        raw = await _get_json(url, params=params)
    except httpx.HTTPStatusError as exc:
        raise HTTPException(status_code=502, detail=f"USDM API error: {exc.response.status_code}")
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"USDM request failed: {exc}")

    records = _parse_usdm_records(raw) if isinstance(raw, list) else raw
    result = {
        "source": "usdm",
        "scope": "national",
        "start_date": str(start),
        "end_date": str(end),
        "drought_categories": dict(_DROUGHT_LABELS),
        "records": records,
    }
    await cache.set(cache_key, result, ttl=config.CACHE_TTL_LONG)
    return result


# ---------------------------------------------------------------------------
# 3. NSF NEON — site list
# ---------------------------------------------------------------------------

@router.get(
    "/neon/sites",
    summary="List all NEON field sites",
)
async def neon_sites():
    """Return the full list of NEON observatory sites (81 sites, 20 domains).

    Each site includes location, domain, description, and available data
    products.
    """
    cache_key = "agriculture:neon:sites"
    cached = await cache.get(cache_key)
    if cached is not None:
        return cached

    url = f"{NEON_BASE}/sites"
    headers: dict[str, str] = {}
    if NEON_TOKEN:
        headers["X-API-Token"] = NEON_TOKEN

    try:
        raw = await _get_json(url, headers=headers)
    except httpx.HTTPStatusError as exc:
        raise HTTPException(status_code=502, detail=f"NEON API error: {exc.response.status_code}")
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"NEON request failed: {exc}")

    # Slim down the response to essentials
    sites = []
    for site in raw.get("data", []):
        sites.append({
            "site_code": site.get("siteCode"),
            "site_name": site.get("siteName"),
            "site_type": site.get("siteType"),
            "domain_code": site.get("domainCode"),
            "state": site.get("stateCode"),
            "latitude": site.get("siteLatitude"),
            "longitude": site.get("siteLongitude"),
            "terrain": site.get("siteDescription"),
            "data_product_count": len(site.get("dataProducts", [])),
        })

    result = {
        "source": "neon",
        "site_count": len(sites),
        "sites": sites,
    }
    await cache.set(cache_key, result, ttl=config.CACHE_TTL_LONG)
    return result


# ---------------------------------------------------------------------------
# 4. NSF NEON — data retrieval
# ---------------------------------------------------------------------------

@router.get(
    "/neon/data",
    summary="Retrieve NEON data for a site/product/month",
)
async def neon_data(
    site: str = Query(..., description="NEON site code (e.g. HARV, CPER)"),
    product: str = Query(..., description="NEON data product ID (e.g. DP1.00041.001 for soil temp)"),
    month: str = Query(..., description="Year-month string (e.g. 2025-06)"),
):
    """Retrieve data availability and download links for a specific NEON data
    product at a given site and month.

    The response includes file URLs, sizes, and metadata for all available
    data packages (basic and expanded).
    """
    site = site.upper().strip()
    product = product.strip()
    month = month.strip()

    cache_key = f"agriculture:neon:data:{site}:{product}:{month}"
    cached = await cache.get(cache_key)
    if cached is not None:
        return cached

    url = f"{NEON_BASE}/data/{product}/{site}/{month}"
    headers: dict[str, str] = {}
    if NEON_TOKEN:
        headers["X-API-Token"] = NEON_TOKEN

    try:
        raw = await _get_json(url, headers=headers)
    except httpx.HTTPStatusError as exc:
        if exc.response.status_code == 404:
            raise HTTPException(
                status_code=404,
                detail=f"No NEON data found for site={site}, product={product}, month={month}",
            )
        raise HTTPException(status_code=502, detail=f"NEON API error: {exc.response.status_code}")
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"NEON request failed: {exc}")

    data = raw.get("data", {})
    files = data.get("files", [])

    # Summarise files by package type
    file_summary = []
    for f in files:
        file_summary.append({
            "name": f.get("name"),
            "size_bytes": f.get("size"),
            "md5": f.get("md5"),
            "url": f.get("url"),
        })

    result = {
        "source": "neon",
        "site": site,
        "product": product,
        "month": month,
        "product_name": data.get("productName"),
        "file_count": len(file_summary),
        "files": file_summary,
    }
    await cache.set(cache_key, result, ttl=config.CACHE_TTL_LONG)
    return result


# ---------------------------------------------------------------------------
# 5. USA-NPN Phenology observations
# ---------------------------------------------------------------------------

@router.get(
    "/phenology/observations",
    summary="Phenology observations from USA-NPN",
)
async def phenology_observations(
    state: str = Query(..., description="Two-letter state abbreviation (e.g. OR)"),
    year: int = Query(..., ge=2000, le=2030, description="Observation year"),
    species_id: Optional[int] = Query(None, description="Optional NPN species ID to filter"),
):
    """Query phenology observations (leaf-out, first bloom, etc.) from the
    USA National Phenology Network for a given state and year.
    """
    state_upper = state.upper().strip()
    cache_key = f"agriculture:phenology:{state_upper}:{year}:{species_id}"
    cached = await cache.get(cache_key)
    if cached is not None:
        return cached

    url = f"{NPN_BASE}/observations/getObservations.json"
    params: dict[str, str] = {
        "start_date": f"{year}-01-01",
        "end_date": f"{year}-12-31",
        "state": state_upper,
    }
    if species_id is not None:
        params["species_id"] = str(species_id)

    try:
        raw = await _get_json(url, params=params)
    except httpx.HTTPStatusError as exc:
        raise HTTPException(status_code=502, detail=f"NPN API error: {exc.response.status_code}")
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"NPN request failed: {exc}")

    # raw is typically a list of observation dicts
    observations = raw if isinstance(raw, list) else raw.get("observations", raw)

    result = {
        "source": "usa_npn",
        "state": state_upper,
        "year": year,
        "species_id": species_id,
        "observation_count": len(observations) if isinstance(observations, list) else None,
        "observations": observations,
    }
    await cache.set(cache_key, result, ttl=config.CACHE_TTL_LONG)
    return result


# ---------------------------------------------------------------------------
# 6. Overview
# ---------------------------------------------------------------------------

@router.get(
    "/overview",
    summary="Agriculture & Land source info and available endpoints",
)
async def overview():
    """Return a summary of all agriculture and land data sources available
    through this module, including endpoint listings and coverage details."""
    return {
        "source_name": SOURCE_NAME,
        "description": SOURCE_DESCRIPTION,
        "sub_sources": [
            {
                "id": "usdm",
                "name": "US Drought Monitor (USDM)",
                "description": (
                    "Weekly drought classification maps and statistics produced "
                    "jointly by NOAA, USDA, and NDMC.  Covers all US states and "
                    "counties with categories None through D4 (Exceptional Drought)."
                ),
                "url": "https://droughtmonitor.unl.edu/",
                "auth_required": False,
                "update_frequency": "Weekly (Thursday)",
                "coverage": "United States (state + county)",
                "endpoints": [
                    "GET /api/agriculture/drought?state=OR",
                    "GET /api/agriculture/drought/national",
                ],
                "drought_categories": _DROUGHT_LABELS,
            },
            {
                "id": "neon",
                "name": "NSF NEON (National Ecological Observatory Network)",
                "description": (
                    "81 field sites across 20 eco-climatic domains collecting "
                    "180+ standardised data products: soil temperature, soil "
                    "moisture, eddy covariance flux, biodiversity, and more."
                ),
                "url": "https://www.neonscience.org/",
                "auth_required": False,
                "auth_note": "Optional API token increases rate limit (200 → 2000 req/min)",
                "update_frequency": "Varies by product (sub-daily to monthly)",
                "coverage": "United States (81 sites)",
                "endpoints": [
                    "GET /api/agriculture/neon/sites",
                    "GET /api/agriculture/neon/data?site=HARV&product=DP1.00041.001&month=2025-06",
                ],
                "example_products": {
                    "DP1.00041.001": "Soil temperature",
                    "DP1.00094.001": "Soil water content and salinity",
                    "DP4.00200.001": "Bundled eddy covariance flux",
                    "DP1.00006.001": "Precipitation",
                    "DP1.10023.001": "Herbaceous clip harvest (biomass)",
                },
            },
            {
                "id": "usa_npn",
                "name": "USA National Phenology Network (USA-NPN)",
                "description": (
                    "Phenological observations contributed by citizen scientists "
                    "and professionals: first bloom, leaf-out, bird migration, "
                    "insect emergence.  Valuable for tracking climate-driven "
                    "shifts in biological timing."
                ),
                "url": "https://www.usanpn.org/",
                "auth_required": False,
                "update_frequency": "Seasonal (observations submitted year-round)",
                "coverage": "United States",
                "endpoints": [
                    "GET /api/agriculture/phenology/observations?state=OR&year=2025",
                ],
            },
        ],
        "states_supported": sorted(_STATE_FIPS.keys()),
    }
