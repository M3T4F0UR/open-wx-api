"""State Groundwater Well Databases — massive public ArcGIS datasets.

Six state-level groundwater well inventories, all public ArcGIS FeatureServers
requiring no authentication.  These are among the largest datasets in the
entire platform:

1. Pennsylvania PAGWIS          — 657,920 wells
   https://services.arcgis.com/CPq7UDkBXVRqtgLR/arcgis/rest/services/PAGWISWaterWells/FeatureServer/0/

2. Indiana DNR                  — 414,953 wells
   https://gisdata.in.gov/server/rest/services/Hosted/WaterWells_DNR_Water_IN_1/FeatureServer/0/

3. Kansas KGS (WWC5)            — 312,492 wells
   https://services2.arcgis.com/ZOdjAzAQ2B0f85zi/arcgis/rest/services/Water_Wells_WWC5/FeatureServer/0/

4. Missouri DNR                 — 291,262 wells
   https://services2.arcgis.com/kNS2ppBA4rwAQQZy/arcgis/rest/services/MO_Wells/FeatureServer/0/

5. Nebraska DWEE                — 265,556 wells
   https://gis.ne.gov/agency3/rest/services/Groundwater_Wells_DWEE/FeatureServer/0/

6. Minnesota CWI                — 177,159 wells
   https://enterprise.gisdata.mn.gov/aghost/rest/services/us_mn_state_health/water_well_information_non_pws/FeatureServer/1/

All endpoints are open with no auth required.
"""
from __future__ import annotations

import asyncio
import logging
from typing import Any, Optional

from fastapi import APIRouter, HTTPException, Query

import cache
import config
import http_client

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Source registry metadata (consumed by server.py /api/sources)
# ---------------------------------------------------------------------------

SOURCE_NAME = "State Groundwater Well Databases"
SOURCE_DESCRIPTION = (
    "Six massive state-level groundwater well inventories totaling over "
    "2.1 million records: Pennsylvania PAGWIS (657K), Indiana DNR (414K), "
    "Kansas KGS (312K), Missouri DNR (291K), Nebraska DWEE (265K), and "
    "Minnesota CWI (177K).  All public ArcGIS FeatureServers, no auth required."
)
SOURCE_META = {
    "category": "water",
    "coverage": "PA, IN, KS, MO, NE, MN",
    "auth_required": False,
    "update_frequency": "varies by state",
    "params": ["well_depth", "groundwater_level", "aquifer", "well_use"],
    "total_records": "~2.1 million",
}

router = APIRouter(prefix="/api/state_wells", tags=["State Groundwater Wells"])

# ---------------------------------------------------------------------------
# ArcGIS Feature Service URLs
# ---------------------------------------------------------------------------

_STATES: dict[str, dict[str, Any]] = {
    "pa": {
        "name": "Pennsylvania PAGWIS",
        "url": (
            "https://services.arcgis.com/CPq7UDkBXVRqtgLR/arcgis/rest/services/"
            "PAGWISWaterWells/FeatureServer/0/query"
        ),
        "record_estimate": 657_920,
    },
    "in": {
        "name": "Indiana DNR Water Wells",
        "url": (
            "https://gisdata.in.gov/server/rest/services/Hosted/"
            "WaterWells_DNR_Water_IN_1/FeatureServer/0/query"
        ),
        "record_estimate": 414_953,
    },
    "ks": {
        "name": "Kansas KGS WWC5",
        "url": (
            "https://services2.arcgis.com/ZOdjAzAQ2B0f85zi/arcgis/rest/services/"
            "Water_Wells_WWC5/FeatureServer/0/query"
        ),
        "record_estimate": 312_492,
    },
    "mo": {
        "name": "Missouri DNR Wells",
        "url": (
            "https://services2.arcgis.com/kNS2ppBA4rwAQQZy/arcgis/rest/services/"
            "MO_Wells/FeatureServer/0/query"
        ),
        "record_estimate": 291_262,
    },
    "ne": {
        "name": "Nebraska DWEE Groundwater Wells",
        "url": (
            "https://gis.ne.gov/agency3/rest/services/"
            "Groundwater_Wells_DWEE/FeatureServer/0/query"
        ),
        "record_estimate": 265_556,
    },
    "mn": {
        "name": "Minnesota CWI",
        "url": (
            "https://enterprise.gisdata.mn.gov/aghost/rest/services/"
            "us_mn_state_health/water_well_information_non_pws/FeatureServer/1/query"
        ),
        "record_estimate": 177_159,
    },
}

_MAX_RECORD_COUNT = 5000


# ---------------------------------------------------------------------------
# Shared ArcGIS query helper with pagination support
# ---------------------------------------------------------------------------

async def _arcgis_query(
    url: str,
    *,
    where: str = "1=1",
    out_fields: str = "*",
    result_record_count: int = _MAX_RECORD_COUNT,
    result_offset: int = 0,
    source_label: str = "state_wells",
    paginate: bool = False,
    max_pages: int = 20,
) -> dict[str, Any]:
    """Execute an ArcGIS Feature Service query and return the JSON response.

    When *paginate* is True, automatically follows ``exceededTransferLimit``
    responses up to *max_pages* pages, concatenating all features into a
    single response dict.  Each page requests *result_record_count* records.

    When *paginate* is False (default), returns a single page of results.
    """
    all_features: list[dict] = []
    offset = result_offset
    pages_fetched = 0

    while True:
        params: dict[str, Any] = {
            "where": where,
            "outFields": out_fields,
            "resultRecordCount": min(result_record_count, _MAX_RECORD_COUNT),
            "resultOffset": offset,
            "f": "json",
        }

        try:
            data = await http_client.fetch_json(
                url, params=params, source=source_label,
            )
        except http_client.CircuitOpenError:
            raise HTTPException(
                status_code=503,
                detail=f"{source_label} circuit breaker open — too many recent failures.",
            )
        except Exception as exc:
            logger.exception("ArcGIS query failed: %s", url)
            raise HTTPException(
                status_code=502,
                detail=f"{source_label} upstream error: {exc}",
            )

        if "error" in data:
            err = data["error"]
            msg = err.get("message", str(err))
            code = err.get("code", 502)
            raise HTTPException(
                status_code=502,
                detail=f"ArcGIS error ({code}): {msg}",
            )

        features = data.get("features", [])
        all_features.extend(features)
        pages_fetched += 1

        # Stop if not paginating, no more results, or we hit the page cap
        exceeded = data.get("exceededTransferLimit", False)
        if not paginate or not exceeded or pages_fetched >= max_pages or not features:
            break

        offset += len(features)

    # Return a dict shaped like a standard ArcGIS response
    return {
        "features": all_features,
        "fields": data.get("fields", []),
        "exceededTransferLimit": data.get("exceededTransferLimit", False),
        "pages_fetched": pages_fetched,
    }


def _extract_features(data: dict) -> list[dict]:
    """Pull the attributes list from an ArcGIS query response."""
    return [f.get("attributes", {}) for f in data.get("features", [])]


# ---------------------------------------------------------------------------
# Endpoint 1: GET /api/state_wells/{state}
# ---------------------------------------------------------------------------

@router.get(
    "/{state}",
    summary="Query groundwater wells for a specific state",
)
async def state_wells(
    state: str,
    limit: int = Query(
        5000,
        ge=1,
        le=10000,
        description="Max records to return per page (max 10,000).",
    ),
    offset: int = Query(
        0,
        ge=0,
        description="Pagination offset (number of records to skip).",
    ),
):
    """Return groundwater well records for a given state.

    Supported states: **pa** (Pennsylvania, 657K), **in** (Indiana, 414K),
    **ks** (Kansas, 312K), **mo** (Missouri, 291K), **ne** (Nebraska, 265K),
    **mn** (Minnesota, 177K).

    Use `offset` for pagination through large result sets.
    """
    key = state.lower()
    if key not in _STATES:
        raise HTTPException(
            status_code=404,
            detail=(
                f"Unknown state '{state}'. "
                f"Supported: {', '.join(sorted(_STATES.keys()))}"
            ),
        )

    info = _STATES[key]
    cache_key = f"state_wells:{key}:{limit}:{offset}"
    cached = await cache.get(cache_key)
    if cached is not None:
        return cached

    data = await _arcgis_query(
        info["url"],
        result_record_count=limit,
        result_offset=offset,
        source_label=f"state_wells_{key}",
    )
    features = _extract_features(data)

    result = {
        "source": SOURCE_NAME,
        "dataset": info["name"],
        "state": key.upper(),
        "record_estimate": info["record_estimate"],
        "query": {"limit": limit, "offset": offset},
        "count": len(features),
        "exceeded_transfer_limit": data.get("exceededTransferLimit", False),
        "features": features,
    }
    await cache.set(cache_key, result, ttl=config.CACHE_TTL)
    return result


# ---------------------------------------------------------------------------
# Endpoint 2: GET /api/state_wells/overview
# ---------------------------------------------------------------------------

@router.get(
    "/overview",
    summary="Overview of all state groundwater well databases",
)
async def overview():
    """Return metadata and live record counts for all six state well databases.

    Queries each ArcGIS service with ``returnCountOnly=true`` to get the
    current total record count without transferring features.
    """
    cache_key = "state_wells:overview"
    cached = await cache.get(cache_key)
    if cached is not None:
        return cached

    async def _count(key: str, info: dict) -> dict:
        """Fetch the total record count for one state."""
        params = {
            "where": "1=1",
            "returnCountOnly": "true",
            "f": "json",
        }
        try:
            data = await http_client.fetch_json(
                info["url"], params=params, source=f"state_wells_{key}_count",
            )
            count = data.get("count", info["record_estimate"])
        except Exception:
            logger.warning("Could not fetch count for %s, using estimate", key)
            count = info["record_estimate"]

        return {
            "state": key.upper(),
            "name": info["name"],
            "record_count": count,
            "record_estimate": info["record_estimate"],
            "url": info["url"].replace("/query", ""),
        }

    tasks = [_count(k, v) for k, v in _STATES.items()]
    states = await asyncio.gather(*tasks)

    total = sum(s["record_count"] for s in states)

    result = {
        "source": SOURCE_NAME,
        "total_records": total,
        "states": sorted(states, key=lambda s: s["record_count"], reverse=True),
    }
    await cache.set(cache_key, result, ttl=3600)
    return result
