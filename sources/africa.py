"""Africa Climate & Environment — ClimateSERV, FAO WaPOR v3, Digital Earth Africa.

Three complementary open-data sources focused on Africa and the developing world:

1. **ClimateSERV** (NASA SERVIR) — CHIRPS rainfall, IMERG, eMODIS NDVI via an
   asynchronous submit/poll/fetch pattern.  Global coverage with an Africa focus.
2. **FAO WaPOR v3** — Evapotranspiration, water productivity, and land cover for
   continental Africa + Near East at 250 m (100 m for 26 countries).
3. **Digital Earth Africa STAC** — CHIRPS daily/monthly rainfall, Sentinel-2,
   land cover, and more via a standard STAC API.

No authentication required for any of these sources.
"""
from __future__ import annotations

import asyncio
import logging
from typing import Optional

import httpx
from fastapi import APIRouter, HTTPException, Query

import cache
import config

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Source registry metadata  (read by server.py /api/sources)
# ---------------------------------------------------------------------------

SOURCE_NAME = "Africa Climate & Environment"
SOURCE_DESCRIPTION = (
    "Three Africa-focused open-data sources: ClimateSERV (NASA SERVIR) for "
    "CHIRPS rainfall, IMERG precipitation, and eMODIS NDVI; FAO WaPOR v3 for "
    "evapotranspiration, water productivity, and land cover at 250 m / 100 m; "
    "and Digital Earth Africa STAC for CHIRPS, Sentinel-2, and land cover "
    "across the continent.  No authentication required."
)
SOURCE_META = {
    "url": "https://climateserv.servirglobal.net/",
    "auth_required": False,
    "update_frequency": "Daily (varies by dataset)",
    "coverage": "Africa / developing world",
    "params": [
        "precipitation", "ndvi", "evapotranspiration",
        "water_productivity", "land_cover",
    ],
    "category": "climate",
}

router = APIRouter(prefix="/api/africa", tags=["Africa Climate & Environment"])

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# ClimateSERV (NASA SERVIR) — async job pattern
CLIMATESERV_SUBMIT = "https://climateserv.servirglobal.net/chirps/submitDataRequest/"
CLIMATESERV_PROGRESS = "https://climateserv.servirglobal.net/chirps/getDataRequestProgress/"
CLIMATESERV_DATA = "https://climateserv.servirglobal.net/chirps/getDataFromRequest/"

CLIMATESERV_DATASETS: dict[int, str] = {
    0: "CHIRPS rainfall",
    26: "IMERG precipitation",
    1: "eMODIS NDVI West Africa",
}

# FAO WaPOR v3
WAPOR_CATALOG = "https://io.apps.fao.org/gismgr/api/v1/catalog/workspaces/WAPOR-3/mapsets"

# Digital Earth Africa STAC
DEA_STAC_BASE = "https://explorer.digitalearth.africa/stac"
DEA_COLLECTIONS = f"{DEA_STAC_BASE}/collections"
DEA_SEARCH = f"{DEA_STAC_BASE}/search"

_HTTP_HEADERS = {"User-Agent": config.USER_AGENT}

# ClimateSERV polling settings
_POLL_INTERVAL_S = 2.0
_POLL_MAX_ATTEMPTS = 60  # 2 min max wait


# ---------------------------------------------------------------------------
# Helpers — ClimateSERV async job
# ---------------------------------------------------------------------------

async def _climateserv_fetch(
    dataset_id: int,
    lat: float,
    lon: float,
    start: str,
    end: str,
) -> dict:
    """Submit a ClimateSERV request, poll until complete, return data.

    Parameters
    ----------
    dataset_id : int
        0 = CHIRPS, 26 = IMERG, 1 = eMODIS NDVI West Africa.
    lat, lon : float
        Point coordinates.
    start, end : str
        Date strings in MM/DD/YYYY format (ClimateSERV convention).
    """
    # ClimateSERV expects a small polygon geometry around the point
    delta = 0.01
    geometry = (
        f'[{{"type":"Polygon","coordinates":[['
        f'[{lon - delta},{lat - delta}],'
        f'[{lon + delta},{lat - delta}],'
        f'[{lon + delta},{lat + delta}],'
        f'[{lon - delta},{lat + delta}],'
        f'[{lon - delta},{lat - delta}]'
        f']]}}]'
    )

    submit_params = {
        "datatype": str(dataset_id),
        "begintime": start,
        "endtime": end,
        "intervaltype": "0",  # daily
        "operationtype": "5",  # average
        "geometry": geometry,
    }

    async with httpx.AsyncClient(timeout=config.HTTP_TIMEOUT, headers=_HTTP_HEADERS) as client:
        # Step 1: Submit
        try:
            resp = await client.post(CLIMATESERV_SUBMIT, data=submit_params)
            resp.raise_for_status()
        except httpx.HTTPStatusError as exc:
            logger.error("ClimateSERV submit error: %s", exc)
            raise HTTPException(
                status_code=502,
                detail=f"ClimateSERV submit failed ({exc.response.status_code}).",
            )
        except httpx.RequestError as exc:
            logger.error("ClimateSERV submit unreachable: %s", exc)
            raise HTTPException(status_code=502, detail="ClimateSERV API unreachable.")

        # The response body is the job ID (a JSON string or plain text)
        job_id = resp.text.strip().strip('"')
        if not job_id or job_id == "-1":
            raise HTTPException(
                status_code=502,
                detail="ClimateSERV returned an invalid job ID.",
            )

        # Step 2: Poll for completion
        for _ in range(_POLL_MAX_ATTEMPTS):
            await asyncio.sleep(_POLL_INTERVAL_S)
            try:
                prog_resp = await client.get(
                    CLIMATESERV_PROGRESS, params={"id": job_id}
                )
                prog_resp.raise_for_status()
            except (httpx.HTTPStatusError, httpx.RequestError):
                continue  # transient — retry

            progress = prog_resp.text.strip().strip('"')
            try:
                if float(progress) >= 100:
                    break
            except ValueError:
                continue
        else:
            raise HTTPException(
                status_code=504,
                detail="ClimateSERV job timed out after polling.",
            )

        # Step 3: Fetch data
        try:
            data_resp = await client.get(
                CLIMATESERV_DATA, params={"id": job_id}
            )
            data_resp.raise_for_status()
        except httpx.HTTPStatusError as exc:
            logger.error("ClimateSERV data fetch error: %s", exc)
            raise HTTPException(
                status_code=502,
                detail=f"ClimateSERV data fetch failed ({exc.response.status_code}).",
            )
        except httpx.RequestError as exc:
            logger.error("ClimateSERV data fetch unreachable: %s", exc)
            raise HTTPException(status_code=502, detail="ClimateSERV data fetch failed.")

    return data_resp.json()


def _reformat_date(iso_date: str) -> str:
    """Convert YYYY-MM-DD to MM/DD/YYYY for ClimateSERV."""
    parts = iso_date.split("-")
    if len(parts) != 3:
        raise HTTPException(
            status_code=400,
            detail=f"Date must be YYYY-MM-DD format, got '{iso_date}'.",
        )
    return f"{parts[1]}/{parts[2]}/{parts[0]}"


# ---------------------------------------------------------------------------
# Endpoint: GET /api/africa/chirps
# ---------------------------------------------------------------------------

@router.get(
    "/chirps",
    summary="CHIRPS rainfall at a point via ClimateSERV",
)
async def africa_chirps(
    lat: float = Query(..., ge=-50, le=50, description="Latitude"),
    lon: float = Query(..., ge=-180, le=180, description="Longitude"),
    start: str = Query(..., description="Start date YYYY-MM-DD"),
    end: str = Query(..., description="End date YYYY-MM-DD"),
    dataset: int = Query(
        0,
        description="ClimateSERV dataset ID: 0=CHIRPS, 26=IMERG, 1=eMODIS NDVI",
    ),
):
    """Fetch daily rainfall (or NDVI) from ClimateSERV for a single point.

    Uses the NASA SERVIR ClimateSERV async job API.  The request is submitted,
    polled for completion, then the data is returned.  Typical wait is 5-30 s
    depending on server load and date range.

    Dataset IDs: 0 = CHIRPS rainfall, 26 = IMERG precipitation,
    1 = eMODIS NDVI West Africa.
    """
    if dataset not in CLIMATESERV_DATASETS:
        raise HTTPException(
            status_code=400,
            detail=f"Unknown dataset ID {dataset}.  Valid: {list(CLIMATESERV_DATASETS.keys())}",
        )

    cache_key = f"africa:chirps:{dataset}:{lat}:{lon}:{start}:{end}"
    cached = await cache.get(cache_key)
    if cached is not None:
        return cached

    cs_start = _reformat_date(start)
    cs_end = _reformat_date(end)

    raw = await _climateserv_fetch(dataset, lat, lon, cs_start, cs_end)

    # ClimateSERV returns a list of {date, value} dicts
    readings = []
    if isinstance(raw, list):
        for entry in raw:
            date_val = entry.get("date", "")
            value = entry.get("value", {}).get("avg")
            if value is not None:
                readings.append({"date": date_val, "value": value})

    result = {
        "source": SOURCE_NAME,
        "provider": "ClimateSERV (NASA SERVIR)",
        "dataset": CLIMATESERV_DATASETS[dataset],
        "dataset_id": dataset,
        "latitude": lat,
        "longitude": lon,
        "start": start,
        "end": end,
        "reading_count": len(readings),
        "readings": readings,
    }

    await cache.set(cache_key, result, ttl=config.CACHE_TTL_LONG)
    return result


# ---------------------------------------------------------------------------
# Endpoint: GET /api/africa/wapor/catalog
# ---------------------------------------------------------------------------

@router.get(
    "/wapor/catalog",
    summary="FAO WaPOR v3 dataset catalog",
)
async def africa_wapor_catalog():
    """List available datasets from FAO WaPOR v3.

    Returns mapsets in the WAPOR-3 workspace: evapotranspiration, water
    productivity, land cover, and more for Africa and the Near East.
    Continental data at 250 m resolution, 100 m for 26 countries.
    """
    cache_key = "africa:wapor:catalog"
    cached = await cache.get(cache_key)
    if cached is not None:
        return cached

    async with httpx.AsyncClient(timeout=config.HTTP_TIMEOUT, headers=_HTTP_HEADERS) as client:
        try:
            resp = await client.get(WAPOR_CATALOG)
            resp.raise_for_status()
        except httpx.HTTPStatusError as exc:
            logger.error("WaPOR catalog error: %s", exc)
            raise HTTPException(
                status_code=502,
                detail=f"FAO WaPOR API returned {exc.response.status_code}.",
            )
        except httpx.RequestError as exc:
            logger.error("WaPOR catalog unreachable: %s", exc)
            raise HTTPException(status_code=502, detail="FAO WaPOR API unreachable.")

    data = resp.json()

    # Extract the mapset list from the response
    mapsets = data.get("response", data) if isinstance(data, dict) else data

    result = {
        "source": SOURCE_NAME,
        "provider": "FAO WaPOR v3",
        "workspace": "WAPOR-3",
        "api_base": "https://io.apps.fao.org/gismgr/api/v1",
        "resolution": "250 m continental, 100 m for 26 countries",
        "coverage": "Africa and Near East",
        "catalog": mapsets,
    }

    await cache.set(cache_key, result, ttl=config.CACHE_TTL_LONG)
    return result


# ---------------------------------------------------------------------------
# Endpoint: GET /api/africa/stac/collections
# ---------------------------------------------------------------------------

@router.get(
    "/stac/collections",
    summary="Digital Earth Africa STAC collections",
)
async def africa_stac_collections():
    """List all STAC collections from Digital Earth Africa.

    Includes CHIRPS daily/monthly rainfall, Sentinel-2 analysis-ready data,
    land cover, water observations, and more for the African continent.
    """
    cache_key = "africa:stac:collections"
    cached = await cache.get(cache_key)
    if cached is not None:
        return cached

    async with httpx.AsyncClient(timeout=config.HTTP_TIMEOUT, headers=_HTTP_HEADERS) as client:
        try:
            resp = await client.get(DEA_COLLECTIONS)
            resp.raise_for_status()
        except httpx.HTTPStatusError as exc:
            logger.error("DEA STAC collections error: %s", exc)
            raise HTTPException(
                status_code=502,
                detail=f"Digital Earth Africa STAC returned {exc.response.status_code}.",
            )
        except httpx.RequestError as exc:
            logger.error("DEA STAC collections unreachable: %s", exc)
            raise HTTPException(
                status_code=502,
                detail="Digital Earth Africa STAC unreachable.",
            )

    data = resp.json()

    collections = data.get("collections", data) if isinstance(data, dict) else data

    result = {
        "source": SOURCE_NAME,
        "provider": "Digital Earth Africa",
        "stac_base": DEA_STAC_BASE,
        "collection_count": len(collections) if isinstance(collections, list) else None,
        "collections": collections,
    }

    await cache.set(cache_key, result, ttl=config.CACHE_TTL_LONG)
    return result


# ---------------------------------------------------------------------------
# Endpoint: GET /api/africa/stac/search
# ---------------------------------------------------------------------------

@router.get(
    "/stac/search",
    summary="Search Digital Earth Africa STAC items",
)
async def africa_stac_search(
    collection: str = Query(
        ..., description="Collection ID, e.g. rainfall_chirps_daily"
    ),
    bbox: str = Query(
        "20,-35,50,15",
        description="Bounding box as west,south,east,north",
    ),
    limit: int = Query(10, ge=1, le=100, description="Max items to return"),
    datetime: Optional[str] = Query(
        None,
        description="Date range, e.g. 2026-03-01/2026-03-18 or single date",
    ),
):
    """Search for STAC items in a Digital Earth Africa collection.

    Supports bounding-box spatial filtering and optional datetime filtering.
    Returns GeoJSON FeatureCollection of matching items.
    """
    bbox_parts = [float(x.strip()) for x in bbox.split(",")]
    if len(bbox_parts) != 4:
        raise HTTPException(
            status_code=400,
            detail="bbox must have 4 values: west,south,east,north.",
        )

    cache_key = f"africa:stac:search:{collection}:{bbox}:{limit}:{datetime}"
    cached = await cache.get(cache_key)
    if cached is not None:
        return cached

    params: dict = {
        "collections": collection,
        "bbox": bbox,
        "limit": str(limit),
    }
    if datetime is not None:
        params["datetime"] = datetime

    async with httpx.AsyncClient(timeout=config.HTTP_TIMEOUT, headers=_HTTP_HEADERS) as client:
        try:
            resp = await client.get(DEA_SEARCH, params=params)
            resp.raise_for_status()
        except httpx.HTTPStatusError as exc:
            logger.error("DEA STAC search error: %s", exc)
            raise HTTPException(
                status_code=502,
                detail=f"Digital Earth Africa STAC search returned {exc.response.status_code}.",
            )
        except httpx.RequestError as exc:
            logger.error("DEA STAC search unreachable: %s", exc)
            raise HTTPException(
                status_code=502,
                detail="Digital Earth Africa STAC search unreachable.",
            )

    data = resp.json()
    features = data.get("features", [])

    result = {
        "source": SOURCE_NAME,
        "provider": "Digital Earth Africa",
        "collection": collection,
        "bbox": bbox_parts,
        "datetime_filter": datetime,
        "returned": len(features),
        "type": "FeatureCollection",
        "features": features,
    }

    await cache.set(cache_key, result, ttl=config.CACHE_TTL)
    return result


# ---------------------------------------------------------------------------
# Endpoint: GET /api/africa/overview
# ---------------------------------------------------------------------------

@router.get(
    "/overview",
    summary="Africa climate & environment source overview",
)
async def africa_overview():
    """Return metadata about the three Africa-focused data sources."""
    return {
        "source": SOURCE_NAME,
        "description": SOURCE_DESCRIPTION,
        "auth_required": False,
        "providers": [
            {
                "name": "ClimateSERV (NASA SERVIR)",
                "url": "https://climateserv.servirglobal.net/",
                "description": (
                    "Asynchronous API for CHIRPS rainfall, IMERG precipitation, "
                    "and eMODIS NDVI.  Global coverage with a focus on Africa "
                    "and the developing world."
                ),
                "datasets": {
                    str(k): v for k, v in CLIMATESERV_DATASETS.items()
                },
                "auth_required": False,
            },
            {
                "name": "FAO WaPOR v3",
                "url": "https://io.apps.fao.org/gismgr/api/v1/swagger-ui.html",
                "description": (
                    "Evapotranspiration, water productivity, and land cover for "
                    "Africa and the Near East.  250 m continental resolution, "
                    "100 m for 26 countries."
                ),
                "workspace": "WAPOR-3",
                "auth_required": False,
            },
            {
                "name": "Digital Earth Africa",
                "url": "https://explorer.digitalearth.africa/stac",
                "description": (
                    "STAC catalog with CHIRPS daily/monthly rainfall, Sentinel-2, "
                    "land cover, water observations, and more for the entire "
                    "African continent."
                ),
                "auth_required": False,
            },
        ],
        "endpoints": [
            {
                "path": "/api/africa/chirps",
                "method": "GET",
                "description": "CHIRPS rainfall (or IMERG/NDVI) at a point via ClimateSERV",
            },
            {
                "path": "/api/africa/wapor/catalog",
                "method": "GET",
                "description": "FAO WaPOR v3 dataset catalog",
            },
            {
                "path": "/api/africa/stac/collections",
                "method": "GET",
                "description": "Digital Earth Africa STAC collections",
            },
            {
                "path": "/api/africa/stac/search",
                "method": "GET",
                "description": "Search Digital Earth Africa STAC items",
            },
            {
                "path": "/api/africa/overview",
                "method": "GET",
                "description": "Source overview and metadata",
            },
        ],
    }
