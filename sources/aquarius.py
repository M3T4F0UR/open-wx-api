"""AQUARIUS WebPortal integration — wxsection.com data platform.

Queries all configured AQUARIUS WebPortal instances using their shared API
pattern. One integration covers 16+ water/hydrology agencies worldwide.

Key instances include Portland BES rain gauges (the original wxsection.com/prg
source), British Columbia (50,000+ sites), South Australia (6,700 sites),
US National Park Service (3,000+ locations), and many more.
"""
from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from typing import Any, Optional

import httpx
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

import cache
from config import (
    AQUARIUS_INSTANCES,
    CACHE_TTL,
    CACHE_TTL_LONG,
    HTTP_TIMEOUT,
    USER_AGENT,
)
from models import Reading, Station, StationDetail

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Module-level metadata (read by server.py /api/sources)
# ---------------------------------------------------------------------------

SOURCE_NAME = "AQUARIUS WebPortal"
SOURCE_DESCRIPTION = (
    "Aquatic Informatics AQUARIUS WebPortal instances worldwide. "
    "All instances share an identical REST API pattern — one integration "
    "covers 16+ water/hydrology agencies including Portland BES rain gauges, "
    "BC Ministry of Environment (50,000+ sites), South Australia EPA, "
    "US National Park Service, NIWA New Zealand, and more."
)
SOURCE_META = {
    "category": "water",
    "coverage": "Worldwide (US, Canada, Australia, New Zealand, Colombia, Malaysia, UK)",
    "params": ["precipitation", "streamflow", "water_level", "water_quality", "groundwater"],
    "update_frequency": "5–15 min (varies by instance and parameter)",
    "auth_required": False,
    "instance_count": len(AQUARIUS_INSTANCES),
    "url": "https://aquaticinformatics.com/products/aquarius-webportal/",
}

# ---------------------------------------------------------------------------
# Response models
# ---------------------------------------------------------------------------


class InstanceStatus(BaseModel):
    """Status record for a single AQUARIUS instance."""

    id: str
    name: str
    base_url: str
    online: bool
    error: Optional[str] = None


class InstancesResponse(BaseModel):
    instances: list[InstanceStatus]
    count: int
    online_count: int


class DashboardInfo(BaseModel):
    id: Any  # may be int or str depending on instance
    name: str
    description: str = ""
    extra: dict = {}


class DashboardsResponse(BaseModel):
    instance_id: str
    base_url: str
    dashboards: list[DashboardInfo]
    count: int


class LocationInfo(BaseModel):
    id: str
    name: str
    lat: Optional[float] = None
    lon: Optional[float] = None
    description: str = ""
    extra: dict = {}


class LocationsResponse(BaseModel):
    instance_id: str
    base_url: str
    locations: list[LocationInfo]
    count: int


class TableResponse(BaseModel):
    instance_id: str
    table_id: str
    columns: list[str]
    rows: list[dict]
    row_count: int
    raw: Optional[dict] = None


class LocationDetailResponse(BaseModel):
    instance_id: str
    location: LocationInfo
    station: Optional[Station] = None
    readings: list[Reading] = []
    raw: Optional[dict] = None


class PortlandRainfallResponse(BaseModel):
    """Portland BES rainfall convenience endpoint."""

    source: str = "portland_bes"
    base_url: str
    table_id: str
    gauges: list[dict]
    count: int
    cached: bool = False


# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------

_COMMON_HEADERS = {
    "User-Agent": USER_AGENT,
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "X-Requested-With": "XMLHttpRequest",
}


def _make_client(timeout: int = HTTP_TIMEOUT) -> httpx.AsyncClient:
    """Return a new AsyncClient with project-standard settings."""
    return httpx.AsyncClient(
        timeout=timeout,
        headers=_COMMON_HEADERS,
        follow_redirects=True,
    )


@asynccontextmanager
async def _get_session_client(base_url: str, timeout: int = HTTP_TIMEOUT):
    """Create a client and seed a session cookie by visiting the portal home.

    Several AQUARIUS instances (Portland BES, SJRWMD, Volusia) gate their
    data endpoints behind an ASP.NET session cookie.  Visiting the dashboard
    index page first causes the server to set the cookie, after which the
    data API calls succeed.
    """
    async with httpx.AsyncClient(
        timeout=timeout,
        headers=_COMMON_HEADERS,
        follow_redirects=True,
    ) as client:
        try:
            await client.get(f"{base_url}/Data/Dashboard/1")
        except Exception as exc:
            logger.debug("Session seed for %s failed (may be OK): %s", base_url, exc)
        yield client


async def _fetch_json(
    client: httpx.AsyncClient,
    url: str,
    params: Optional[dict] = None,
) -> Any:
    """GET *url* and return parsed JSON, raising HTTPException on failure."""
    try:
        response = await client.get(url, params=params)
        response.raise_for_status()
        return response.json()
    except httpx.TimeoutException as exc:
        raise HTTPException(status_code=504, detail=f"Timeout fetching {url}: {exc}") from exc
    except httpx.HTTPStatusError as exc:
        raise HTTPException(
            status_code=exc.response.status_code,
            detail=f"Upstream error from {url}: {exc.response.status_code}",
        ) from exc
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Error fetching {url}: {exc}") from exc


# ---------------------------------------------------------------------------
# Instance validation helper
# ---------------------------------------------------------------------------


def _resolve_instance(instance_id: str) -> str:
    """Return the base URL for *instance_id* or raise 404."""
    base_url = AQUARIUS_INSTANCES.get(instance_id)
    if not base_url:
        known = list(AQUARIUS_INSTANCES.keys())
        raise HTTPException(
            status_code=404,
            detail=f"Unknown AQUARIUS instance '{instance_id}'. Known: {known}",
        )
    return base_url.rstrip("/")


# ---------------------------------------------------------------------------
# Data-normalisation helpers
# ---------------------------------------------------------------------------


def _parse_location(raw: dict, instance_id: str) -> LocationInfo:
    """Best-effort normalisation of a raw AQUARIUS location/map record."""
    # AQUARIUS WebPortal uses several different JSON shapes across versions;
    # try the most common key names in order of prevalence.
    loc_id = (
        raw.get("LocationIdentifier")
        or raw.get("Identifier")
        or raw.get("id")
        or raw.get("Id")
        or raw.get("SiteCode")
        or ""
    )
    name = (
        raw.get("LocationName")
        or raw.get("Name")
        or raw.get("name")
        or raw.get("SiteName")
        or raw.get("StationName")
        or str(loc_id)
    )
    lat = raw.get("Latitude") or raw.get("lat") or raw.get("latitude") or None
    lon = raw.get("Longitude") or raw.get("lon") or raw.get("longitude") or None
    desc = raw.get("Description") or raw.get("description") or ""

    # Convert lat/lon to float if present
    try:
        lat = float(lat) if lat is not None else None
        lon = float(lon) if lon is not None else None
    except (TypeError, ValueError):
        lat = lon = None

    return LocationInfo(
        id=str(loc_id),
        name=str(name),
        lat=lat,
        lon=lon,
        description=str(desc),
        extra={k: v for k, v in raw.items() if k not in {
            "LocationIdentifier", "Identifier", "id", "Id", "SiteCode",
            "LocationName", "Name", "name", "SiteName", "StationName",
            "Latitude", "lat", "latitude", "Longitude", "lon", "longitude",
            "Description", "description",
        }},
    )


def _location_to_station(loc: LocationInfo, instance_id: str, base_url: str) -> Station:
    """Convert a LocationInfo into the shared Station model."""
    return Station(
        id=f"{instance_id}:{loc.id}",
        name=loc.name,
        lat=loc.lat if loc.lat is not None else 0.0,
        lon=loc.lon if loc.lon is not None else 0.0,
        source="aquarius",
        network=instance_id,
        url=f"{base_url}/Data/Map#{loc.id}",
        extra={**loc.extra, "aquarius_id": loc.id, "description": loc.description},
    )


def _normalise_table_data(raw: Any) -> tuple[list[str], list[dict]]:
    """Extract columns and rows from a raw GetTable API response.

    AQUARIUS returns table data in a few different shapes:
      - {columns: [...], data: [[...]]}           (most common)
      - {Columns: [...], Data: [[...]]}            (capitalised)
      - {columns: [...], rows: [{col: val}]}       (pre-formatted)
      - A direct list of row objects
    """
    if isinstance(raw, list):
        # Already a list of row dicts
        cols = list(raw[0].keys()) if raw else []
        return cols, raw

    if not isinstance(raw, dict):
        return [], []

    # Prefer the nested {columns, data} shape
    columns_raw = raw.get("columns") or raw.get("Columns") or []
    data_raw = raw.get("data") or raw.get("Data") or raw.get("rows") or raw.get("Rows") or []

    # columns may be a list of strings or a list of {name: ...} dicts
    columns: list[str] = []
    for col in columns_raw:
        if isinstance(col, dict):
            columns.append(col.get("name") or col.get("Name") or col.get("title") or str(col))
        else:
            columns.append(str(col))

    rows: list[dict] = []
    for row in data_raw:
        if isinstance(row, dict):
            rows.append(row)
        elif isinstance(row, (list, tuple)) and columns:
            rows.append(dict(zip(columns, row)))
        else:
            rows.append({"value": row})

    return columns, rows


# ---------------------------------------------------------------------------
# Portland BES-specific helpers
# ---------------------------------------------------------------------------

_PORTLAND_TABLE_ID = "RetrieveLatestRainfallStats"
_PORTLAND_DASHBOARD_ID = 8
_PORTLAND_INSTANCE = "portland_bes"

# Columns returned by the Portland BES rainfall stats table
_PORTLAND_PRECIP_COLS = {
    "15min", "30min", "1hr", "2hr", "3hr", "6hr", "12hr", "24hr",
    "since_midnight", "storm_total",
}


def _parse_portland_gauge(row: dict) -> dict:
    """Normalise a single Portland BES rainfall row into a clean gauge dict."""
    # Station name / ID
    name = (
        row.get("Location")
        or row.get("LocationName")
        or row.get("GaugeName")
        or row.get("Name")
        or ""
    )
    loc_id = row.get("LocationIdentifier") or row.get("Identifier") or row.get("Id") or name
    return {
        "id": str(loc_id),
        "name": str(name),
        "data": {k: v for k, v in row.items() if k not in {"Location", "LocationName",
                                                             "GaugeName", "Name",
                                                             "LocationIdentifier",
                                                             "Identifier", "Id"}},
    }


# ---------------------------------------------------------------------------
# Router definition
# ---------------------------------------------------------------------------

router = APIRouter(
    prefix="/api/aquarius",
    tags=["AQUARIUS WebPortal"],
)


# ---------------------------------------------------------------------------
# Endpoint: GET /api/aquarius/instances
# ---------------------------------------------------------------------------


@router.get(
    "/instances",
    summary="List all configured AQUARIUS instances",
    response_model=InstancesResponse,
)
async def list_instances(
    check_online: bool = Query(
        False,
        description=(
            "If true, probe each instance with a lightweight HTTP HEAD request "
            "to determine online status. Adds latency; cached for 5 minutes."
        ),
    ),
) -> InstancesResponse:
    """Return all configured AQUARIUS WebPortal instances.

    By default returns configuration only (fast).  Pass `?check_online=true`
    to probe each instance; results are cached for 5 minutes.
    """
    cache_key = f"aquarius:instances:status:{check_online}"
    cached_result = await cache.get(cache_key)
    if cached_result is not None:
        return InstancesResponse(**cached_result)

    statuses: list[InstanceStatus] = []

    # Human-readable names for known instances
    _NAMES = {
        "portland_bes": "Portland Bureau of Environmental Services",
        "nps": "US National Park Service",
        "usibwc": "US International Boundary & Water Commission",
        "sjrwmd": "St. Johns River Water Management District (FL)",
        "orlando": "City of Orlando Hydrology",
        "nwfwmd": "NW Florida Water Management District",
        "volusia": "Volusia County (FL)",
        "nebraska": "Nebraska DNRR",
        "bc": "BC Ministry of Forests (Canada)",
        "somerset_uk": "Somerset, UK (multi-agency)",
        "south_australia": "South Australia Water Data",
        "northern_territory": "Northern Territory (Australia)",
        "waternsw": "WaterNSW",
        "niwa_nz": "NIWA New Zealand",
        "ideam_colombia": "IDEAM Colombia",
        "malaysia": "Malaysia DID Hydroportal",
    }

    if check_online:
        async with _make_client(timeout=10) as client:
            for inst_id, base_url in AQUARIUS_INSTANCES.items():
                try:
                    resp = await client.head(
                        f"{base_url.rstrip('/')}/Data/Map",
                        timeout=10,
                    )
                    online = resp.status_code < 500
                    error = None if online else f"HTTP {resp.status_code}"
                except Exception as exc:
                    online = False
                    error = str(exc)[:120]
                statuses.append(
                    InstanceStatus(
                        id=inst_id,
                        name=_NAMES.get(inst_id, inst_id.replace("_", " ").title()),
                        base_url=base_url,
                        online=online,
                        error=error,
                    )
                )
    else:
        for inst_id, base_url in AQUARIUS_INSTANCES.items():
            statuses.append(
                InstanceStatus(
                    id=inst_id,
                    name=_NAMES.get(inst_id, inst_id.replace("_", " ").title()),
                    base_url=base_url,
                    online=True,  # optimistic — not probed
                )
            )

    result = InstancesResponse(
        instances=statuses,
        count=len(statuses),
        online_count=sum(1 for s in statuses if s.online),
    )
    await cache.set(cache_key, result.model_dump(), ttl=CACHE_TTL)
    return result


# ---------------------------------------------------------------------------
# Endpoint: GET /api/aquarius/portland/rainfall
# IMPORTANT: declared before all /{instance_id}/... routes so FastAPI
# matches this literal two-segment path first (more specific than a
# path-parameter route at the same depth).
# ---------------------------------------------------------------------------


@router.get(
    "/portland/rainfall",
    summary="Portland BES rain gauge latest stats (convenience endpoint)",
    response_model=PortlandRainfallResponse,
)
async def portland_rainfall(
    page_size: int = Query(200, ge=1, le=500, description="Max gauges to return"),
    include_raw: bool = Query(False, description="Include raw upstream JSON"),
) -> PortlandRainfallResponse:
    """Return the latest rainfall statistics for all Portland BES rain gauges.

    This is the live data behind wxsection.com/prg.  Data comes from the
    Portland AQUARIUS instance at dashboard 8, table `RetrieveLatestRainfallStats`.

    Returns one record per gauge with accumulated depths for standard
    intervals (15 min, 30 min, 1 hr, 2 hr ... 24 hr, storm total).

    Cached for 5 minutes.
    """
    base_url = AQUARIUS_INSTANCES["portland_bes"].rstrip("/")
    cache_key = f"aquarius:portland:rainfall:{page_size}"
    cached_result = await cache.get(cache_key)
    if cached_result is not None:
        return PortlandRainfallResponse(**cached_result)

    async with _get_session_client(base_url) as client:
        # Step 1: seed the session via the known Portland dashboard
        try:
            await client.get(f"{base_url}/Data/Dashboard/{_PORTLAND_DASHBOARD_ID}")
        except Exception:
            pass  # best-effort session seed

        # Step 2: fetch the rainfall stats table
        url = f"{base_url}/Data/GetTable"
        raw = await _fetch_json(
            client,
            url,
            params={
                "id": _PORTLAND_TABLE_ID,
                "arguments": "",
                "page": 1,
                "pageSize": page_size,
            },
        )

    _, rows = _normalise_table_data(raw)
    gauges = [_parse_portland_gauge(row) for row in rows]

    result = PortlandRainfallResponse(
        source=_PORTLAND_INSTANCE,
        base_url=base_url,
        table_id=_PORTLAND_TABLE_ID,
        gauges=gauges,
        count=len(gauges),
    )
    await cache.set(cache_key, result.model_dump(), ttl=CACHE_TTL)
    return result


# ---------------------------------------------------------------------------
# Endpoint: GET /api/aquarius/{instance_id}/dashboards
# ---------------------------------------------------------------------------


@router.get(
    "/{instance_id}/dashboards",
    summary="List available dashboards for an AQUARIUS instance",
    response_model=DashboardsResponse,
)
async def get_dashboards(instance_id: str) -> DashboardsResponse:
    """Retrieve the list of dashboards published by the given instance.

    Results are cached for 1 hour.
    """
    base_url = _resolve_instance(instance_id)
    cache_key = f"aquarius:dashboards:{instance_id}"
    cached_result = await cache.get(cache_key)
    if cached_result is not None:
        return DashboardsResponse(**cached_result)

    # AQUARIUS WebPortal: the navigation / dashboard list lives at the portal
    # home page rendered as JSON via the GetDashboard endpoint.  There is no
    # single "list all dashboards" endpoint so we fall back to fetching the
    # first few IDs (0-15) and collecting those that respond.
    dashboards: list[DashboardInfo] = []

    # First try a GetDashboard with no id (some instances return a menu):
    async with _get_session_client(base_url) as client:
        # Some instances expose a navigation JSON:
        for candidate_path in (
            "/Data/GetDashboard",
            "/Data/GetNavigation",
            "/Data/GetMenu",
        ):
            try:
                url = f"{base_url}{candidate_path}"
                data = await _fetch_json(client, url)
                if isinstance(data, list):
                    for item in data:
                        if isinstance(item, dict):
                            dashboards.append(
                                DashboardInfo(
                                    id=item.get("id") or item.get("Id") or item.get("DashboardId") or "",
                                    name=item.get("name") or item.get("Name") or item.get("Title") or "",
                                    description=item.get("description") or item.get("Description") or "",
                                    extra={k: v for k, v in item.items()
                                           if k not in {"id", "Id", "DashboardId",
                                                        "name", "Name", "Title",
                                                        "description", "Description"}},
                                )
                            )
                    if dashboards:
                        break
                elif isinstance(data, dict):
                    # Might be a single dashboard object
                    dash_id = data.get("id") or data.get("Id") or data.get("DashboardId") or ""
                    name = data.get("name") or data.get("Name") or data.get("Title") or ""
                    if name or dash_id:
                        dashboards.append(
                            DashboardInfo(
                                id=dash_id,
                                name=str(name),
                                description=data.get("description") or data.get("Description") or "",
                                extra={},
                            )
                        )
                        break
            except HTTPException:
                continue

        # If the navigation endpoints yielded nothing, try probing numeric IDs 1-10
        if not dashboards:
            for dash_id in range(1, 11):
                try:
                    url = f"{base_url}/Data/GetDashboard"
                    data = await _fetch_json(client, url, params={"id": dash_id})
                    if isinstance(data, dict) and (data.get("name") or data.get("Name")):
                        dashboards.append(
                            DashboardInfo(
                                id=dash_id,
                                name=data.get("name") or data.get("Name") or f"Dashboard {dash_id}",
                                description=data.get("description") or data.get("Description") or "",
                                extra={},
                            )
                        )
                except HTTPException:
                    # 404 / 500 means this ID doesn't exist; keep going
                    continue

    result = DashboardsResponse(
        instance_id=instance_id,
        base_url=base_url,
        dashboards=dashboards,
        count=len(dashboards),
    )
    await cache.set(cache_key, result.model_dump(), ttl=CACHE_TTL_LONG)
    return result


# ---------------------------------------------------------------------------
# Endpoint: GET /api/aquarius/{instance_id}/locations
# ---------------------------------------------------------------------------


@router.get(
    "/{instance_id}/locations",
    summary="All monitoring locations for an AQUARIUS instance",
    response_model=LocationsResponse,
)
async def get_locations(
    instance_id: str,
    limit: int = Query(500, ge=1, le=5000, description="Maximum locations to return"),
) -> LocationsResponse:
    """Fetch the full location/map dataset for an AQUARIUS instance.

    Uses the `/Data/Map` endpoint which returns all monitoring locations with
    coordinates.  Results are cached for 1 hour.
    """
    base_url = _resolve_instance(instance_id)
    cache_key = f"aquarius:locations:{instance_id}:{limit}"
    cached_result = await cache.get(cache_key)
    if cached_result is not None:
        return LocationsResponse(**cached_result)

    locations: list[LocationInfo] = []

    async with _get_session_client(base_url) as client:
        # Primary map endpoint
        map_url = f"{base_url}/Data/Map"
        try:
            raw = await _fetch_json(client, map_url)
        except HTTPException as exc:
            # If /Data/Map fails, try alternative paths used by some versions
            for alt_path in ("/Data/GetLocationList", "/Data/Locations"):
                try:
                    raw = await _fetch_json(client, f"{base_url}{alt_path}")
                    break
                except HTTPException:
                    continue
            else:
                raise exc

        # The map endpoint can return different structures:
        #   - a list of location objects
        #   - {"locations": [...]}
        #   - {"data": [...]}
        if isinstance(raw, list):
            raw_locs = raw
        elif isinstance(raw, dict):
            raw_locs = (
                raw.get("locations")
                or raw.get("Locations")
                or raw.get("data")
                or raw.get("Data")
                or raw.get("features")  # GeoJSON FeatureCollection
                or []
            )
            # Handle GeoJSON FeatureCollection
            if raw.get("type") == "FeatureCollection":
                features = raw.get("features") or []
                raw_locs = []
                for feat in features:
                    props = feat.get("properties") or {}
                    geom = feat.get("geometry") or {}
                    coords = geom.get("coordinates") or []
                    if len(coords) >= 2:
                        props["Longitude"] = coords[0]
                        props["Latitude"] = coords[1]
                    raw_locs.append(props)
        else:
            raw_locs = []

        for item in raw_locs[:limit]:
            if isinstance(item, dict):
                locations.append(_parse_location(item, instance_id))

    result = LocationsResponse(
        instance_id=instance_id,
        base_url=base_url,
        locations=locations,
        count=len(locations),
    )
    await cache.set(cache_key, result.model_dump(), ttl=CACHE_TTL_LONG)
    return result


# ---------------------------------------------------------------------------
# Endpoint: GET /api/aquarius/{instance_id}/table/{table_id}
# ---------------------------------------------------------------------------


@router.get(
    "/{instance_id}/table/{table_id}",
    summary="Get table data from an AQUARIUS instance",
    response_model=TableResponse,
)
async def get_table(
    instance_id: str,
    table_id: str,
    page: int = Query(1, ge=1, description="Page number"),
    page_size: int = Query(200, ge=1, le=1000, description="Rows per page"),
    arguments: str = Query("", description="Optional table arguments string"),
    include_raw: bool = Query(False, description="Include the raw upstream JSON in the response"),
) -> TableResponse:
    """Fetch tabular data from an AQUARIUS instance.

    The `table_id` maps to the AQUARIUS `GetTable?id=` parameter.  For
    Portland BES rainfall stats use `table_id=RetrieveLatestRainfallStats`.

    Results are cached for 5 minutes.
    """
    base_url = _resolve_instance(instance_id)
    cache_key = f"aquarius:table:{instance_id}:{table_id}:{page}:{page_size}:{arguments}"
    cached_result = await cache.get(cache_key)
    if cached_result is not None:
        return TableResponse(**cached_result)

    async with _get_session_client(base_url) as client:
        url = f"{base_url}/Data/GetTable"
        raw = await _fetch_json(
            client,
            url,
            params={
                "id": table_id,
                "arguments": arguments,
                "page": page,
                "pageSize": page_size,
            },
        )

    columns, rows = _normalise_table_data(raw)

    result = TableResponse(
        instance_id=instance_id,
        table_id=table_id,
        columns=columns,
        rows=rows,
        row_count=len(rows),
        raw=raw if include_raw else None,
    )
    await cache.set(cache_key, result.model_dump(), ttl=CACHE_TTL)
    return result


# ---------------------------------------------------------------------------
# Endpoint: GET /api/aquarius/{instance_id}/location/{location_id}
# ---------------------------------------------------------------------------


@router.get(
    "/{instance_id}/location/{location_id}",
    summary="Location detail with latest readings",
    response_model=LocationDetailResponse,
)
async def get_location_detail(
    instance_id: str,
    location_id: str,
    include_raw: bool = Query(False, description="Include raw upstream JSON"),
) -> LocationDetailResponse:
    """Fetch detail for a single monitoring location including parameter readings.

    Uses the `/Data/GetLocationData` endpoint.  Results cached for 5 minutes.
    """
    base_url = _resolve_instance(instance_id)
    cache_key = f"aquarius:location:{instance_id}:{location_id}"
    cached_result = await cache.get(cache_key)
    if cached_result is not None:
        return LocationDetailResponse(**cached_result)

    async with _get_session_client(base_url) as client:
        url = f"{base_url}/Data/GetLocationData"
        raw = await _fetch_json(client, url, params={"id": location_id})

    # Parse location metadata
    if isinstance(raw, dict):
        loc_data = raw
    else:
        loc_data = {}

    loc = _parse_location(loc_data, instance_id)
    if not loc.id:
        loc = LocationInfo(id=location_id, name=location_id)

    station = _location_to_station(loc, instance_id, base_url)

    # Extract readings from the location data response.
    # AQUARIUS returns time-series datasets nested under various keys.
    readings: list[Reading] = []
    ts_list = (
        loc_data.get("TimeSeries")
        or loc_data.get("timeSeries")
        or loc_data.get("datasets")
        or loc_data.get("Datasets")
        or []
    )
    if isinstance(ts_list, list):
        for ts in ts_list:
            if not isinstance(ts, dict):
                continue
            param = (
                ts.get("Parameter")
                or ts.get("parameter")
                or ts.get("Label")
                or ts.get("label")
                or "unknown"
            )
            unit = (
                ts.get("Unit")
                or ts.get("unit")
                or ts.get("Units")
                or ""
            )
            # Latest value may be a direct scalar or nested under LatestValue/Value
            latest_val = ts.get("LatestValue") or ts.get("latestValue") or ts.get("Value")
            latest_ts = ts.get("LatestTimestamp") or ts.get("latestTimestamp") or ts.get("Timestamp")
            quality = ts.get("Quality") or ts.get("quality") or None

            if latest_val is not None and latest_ts is not None:
                try:
                    from datetime import datetime, timezone

                    # AQUARIUS timestamps are ISO 8601; strip trailing Z or offset
                    ts_str = str(latest_ts).replace("Z", "+00:00")
                    dt = datetime.fromisoformat(ts_str)
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=timezone.utc)

                    readings.append(
                        Reading(
                            station_id=station.id,
                            timestamp=dt,
                            parameter=str(param).lower().replace(" ", "_"),
                            value=float(latest_val),
                            unit=str(unit),
                            quality=str(quality) if quality else None,
                            source="aquarius",
                        )
                    )
                except (ValueError, TypeError):
                    pass

    result = LocationDetailResponse(
        instance_id=instance_id,
        location=loc,
        station=station,
        readings=readings,
        raw=raw if include_raw else None,
    )
    await cache.set(cache_key, result.model_dump(), ttl=CACHE_TTL)
    return result


# ---------------------------------------------------------------------------
# Endpoint: GET /api/aquarius/{instance_id}/stations
# (alias that returns locations in the shared Station model format)
# ---------------------------------------------------------------------------


@router.get(
    "/{instance_id}/stations",
    summary="Monitoring locations as shared Station objects",
    response_model=list[Station],
)
async def get_stations(
    instance_id: str,
    limit: int = Query(500, ge=1, le=5000),
) -> list[Station]:
    """Return monitoring locations for an instance in the shared Station schema.

    This is the standard interface consumed by the wxsection front-end map.
    Internally delegates to `get_locations` and converts to Station models.
    Results inherit the 1-hour cache from that endpoint.
    """
    locs_response = await get_locations(instance_id=instance_id, limit=limit)
    base_url = _resolve_instance(instance_id)
    return [
        _location_to_station(loc, instance_id, base_url)
        for loc in locs_response.locations
        if loc.lat is not None and loc.lon is not None
    ]
