"""USACE CWMS Data API (CDA) — direct access to Corps water data.

The US Army Corps of Engineers Corpswide Water Management System (CWMS) Data API
provides time series, location metadata, and catalog listings for reservoirs,
locks, dams, levees, and flood-control infrastructure across all USACE districts.

- No authentication required
- REST/JSON with Swagger documentation
- Base URL: https://cwms-data.usace.army.mil/cwms-data/
- Swagger: https://cwms-data.usace.army.mil/cwms-data/swagger-ui.html
"""
from __future__ import annotations

import logging
from typing import Optional

from fastapi import APIRouter, HTTPException, Query

import cache
import config
import http_client

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Source registry metadata
# ---------------------------------------------------------------------------
SOURCE_NAME = "USACE CWMS Data API"
SOURCE_DESCRIPTION = (
    "US Army Corps of Engineers CWMS Data API — time series, locations, and "
    "catalog for reservoirs, locks, dams, and flood-control infrastructure "
    "across all USACE districts.  No authentication required."
)
SOURCE_META = {
    "url": "https://cwms-data.usace.army.mil/cwms-data/",
    "swagger": "https://cwms-data.usace.army.mil/cwms-data/swagger-ui.html",
    "auth_required": False,
    "update_frequency": "15 min – 1 hr",
    "coverage": "United States (all USACE districts)",
    "params": ["water_level", "discharge", "storage", "precipitation", "temperature"],
    "category": "water",
}

router = APIRouter(prefix="/api/usace_cda", tags=["USACE CWMS Data API"])

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
CDA_BASE = "https://cwms-data.usace.army.mil/cwms-data"

USACE_OFFICES = [
    "LRB", "LRC", "LRD", "LRE", "LRH", "LRL", "LRN", "LRP",
    "MVD", "MVK", "MVM", "MVN", "MVP", "MVR", "MVS",
    "NAB", "NAD", "NAE", "NAN", "NAO", "NAP",
    "NWD", "NWK", "NWO", "NWP", "NWS", "NWW",
    "POD", "POA",
    "SAD", "SAJ", "SAM", "SAS", "SAW",
    "SPA", "SPD", "SPK", "SPL", "SPN",
    "SWD", "SWF", "SWG", "SWL", "SWT",
    "TAD",
]

_HEADERS = {
    "Accept": "application/json;version=2",
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

async def _cda_get(path: str, params: dict | None = None) -> dict:
    """Fetch from the CWMS Data API via the shared HTTP client."""
    url = f"{CDA_BASE}/{path.lstrip('/')}"
    return await http_client.fetch_json(
        url,
        params=params,
        headers=_HEADERS,
        source="usace_cda",
    )


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@router.get("/offices", summary="List all USACE district office codes")
async def list_offices():
    """Return the full list of known USACE district office codes.

    These codes are used as the ``office`` parameter in the other endpoints.
    """
    return {
        "offices": USACE_OFFICES,
        "count": len(USACE_OFFICES),
    }


@router.get("/locations", summary="List CWMS locations for a district office")
async def list_locations(
    office: str = Query(
        ..., description="USACE district office code, e.g. 'NWP', 'SWF', 'MVR'"
    ),
    names: str = Query(
        "*", description="Location name filter (supports wildcards), e.g. 'Dam*'"
    ),
    limit: int = Query(
        500, ge=1, le=5000, description="Maximum locations to return"
    ),
):
    """Return CWMS monitoring locations for the given district office.

    Results are cached for one hour.
    """
    office_upper = office.upper()
    cache_key = f"usace_cda:locations:{office_upper}:{names}:{limit}"
    cached = await cache.get(cache_key)
    if cached is not None:
        return cached

    try:
        data = await _cda_get("locations", params={
            "office": office_upper,
            "names": names,
            "page-size": min(limit, 5000),
        })
    except Exception as exc:
        logger.error("CWMS CDA locations error for %s: %s", office_upper, exc)
        raise HTTPException(status_code=502, detail=f"CWMS CDA API error: {exc}")

    locations = []
    # The /locations endpoint returns a "locations" wrapper (v2 JSON)
    raw = data.get("locations", data) if isinstance(data, dict) else data
    entries = raw.get("entries", raw) if isinstance(raw, dict) else raw
    if isinstance(entries, list):
        for loc in entries[:limit]:
            if not isinstance(loc, dict):
                continue
            locations.append({
                "name": loc.get("name") or loc.get("location-id", ""),
                "office": loc.get("office-id", office_upper),
                "latitude": loc.get("latitude"),
                "longitude": loc.get("longitude"),
                "elevation": loc.get("elevation"),
                "kind": loc.get("location-kind", ""),
                "nation": loc.get("nation", ""),
                "state": loc.get("state-initial", ""),
                "county": loc.get("county-name", ""),
                "time_zone": loc.get("time-zone-name", ""),
                "source": "usace_cda",
            })

    result = {"office": office_upper, "count": len(locations), "locations": locations}
    await cache.set(cache_key, result, ttl=config.CACHE_TTL_LONG)
    return result


@router.get("/catalog", summary="Search CWMS time series catalog")
async def search_catalog(
    office: str = Query(
        ..., description="USACE district office code, e.g. 'NWP', 'SWF'"
    ),
    like: Optional[str] = Query(
        None,
        description=(
            "Time series name filter (supports wildcards), "
            "e.g. '*.Precip.*' or 'Old Hickory.*'"
        ),
    ),
    limit: int = Query(
        500, ge=1, le=5000, description="Maximum entries to return"
    ),
):
    """Search the CWMS time series catalog for a district office.

    Each entry describes one time series by its fully-qualified CWMS name,
    e.g. ``Old Hickory.Elev.Inst.1Hour.0.Stage-rev``.

    Use the ``like`` parameter to filter by pattern (supports ``*`` wildcards).
    Results are cached for one hour.
    """
    office_upper = office.upper()
    cache_key = f"usace_cda:catalog:{office_upper}:{like}:{limit}"
    cached = await cache.get(cache_key)
    if cached is not None:
        return cached

    params: dict = {
        "office": office_upper,
        "page-size": min(limit, 5000),
    }
    if like:
        params["like"] = like

    try:
        data = await _cda_get("catalog/TIMESERIES", params=params)
    except Exception as exc:
        logger.error("CWMS CDA catalog error for %s: %s", office_upper, exc)
        raise HTTPException(status_code=502, detail=f"CWMS CDA API error: {exc}")

    entries_raw = data.get("entries", []) if isinstance(data, dict) else []
    catalog = []
    for entry in entries_raw[:limit]:
        if not isinstance(entry, dict):
            continue
        catalog.append({
            "name": entry.get("name", ""),
            "office": entry.get("office-id", office_upper),
            "units": entry.get("units", ""),
            "interval": entry.get("interval", ""),
            "extents": entry.get("extents", []),
        })

    result = {"office": office_upper, "count": len(catalog), "catalog": catalog}
    await cache.set(cache_key, result, ttl=config.CACHE_TTL_LONG)
    return result


@router.get("/timeseries", summary="Fetch CWMS time series data")
async def get_timeseries(
    office: str = Query(
        ..., description="USACE district office code, e.g. 'SWF'"
    ),
    name: str = Query(
        ...,
        description=(
            "Fully-qualified CWMS time series name, "
            "e.g. 'Old Hickory.Elev.Inst.1Hour.0.Stage-rev'"
        ),
    ),
    begin: str = Query(
        ...,
        description="Start date/time in ISO-8601 format, e.g. '2026-03-01T00:00:00'",
    ),
    end: str = Query(
        ...,
        description="End date/time in ISO-8601 format, e.g. '2026-03-20T00:00:00'",
    ),
):
    """Fetch time series values from the CWMS Data API.

    Returns timestamped values for the requested time series between
    ``begin`` and ``end``.  Results are cached for 5 minutes.
    """
    office_upper = office.upper()
    cache_key = f"usace_cda:ts:{office_upper}:{name}:{begin}:{end}"
    cached = await cache.get(cache_key)
    if cached is not None:
        return cached

    params: dict = {
        "office": office_upper,
        "name": name,
        "begin": begin,
        "end": end,
    }

    try:
        data = await _cda_get("timeseries", params=params)
    except Exception as exc:
        logger.error("CWMS CDA timeseries error for %s/%s: %s", office_upper, name, exc)
        raise HTTPException(status_code=502, detail=f"CWMS CDA API error: {exc}")

    # Parse the CWMS v2 JSON response
    values_raw = data.get("values", [])
    units = data.get("units", "")
    ts_name = data.get("name", name)

    values = []
    for row in values_raw:
        # Each row is typically [timestamp_ms, value, quality_code]
        if not isinstance(row, (list, tuple)) or len(row) < 2:
            continue
        ts, val = row[0], row[1]
        quality = row[2] if len(row) > 2 else None
        values.append({
            "timestamp": ts,
            "value": val,
            "quality": quality,
        })

    result = {
        "office": office_upper,
        "name": ts_name,
        "units": units,
        "begin": begin,
        "end": end,
        "count": len(values),
        "values": values,
    }
    await cache.set(cache_key, result, ttl=config.CACHE_TTL)
    return result
