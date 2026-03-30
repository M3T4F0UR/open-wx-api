"""Federal Water Data — USACE CWMS + Bureau of Reclamation RISE.

Combines two federal water data APIs:

1. USACE CWMS Data API (US Army Corps of Engineers)
   Reservoir, lock, dam, and flood-control monitoring at ~4,000+ locations
   across the United States.
   API: https://cwms-data.usace.army.mil/cwms-data/

2. Bureau of Reclamation RISE API
   Western US reservoir, canal, diversion, and weather station data covering
   ~800+ stations in 17 western states.
   API: https://data.usbr.gov/rise/api/

Both APIs are free with no authentication required.
"""
import logging
from datetime import datetime, timedelta, timezone
from typing import Optional

import httpx
from fastapi import APIRouter, HTTPException, Query

import cache
import config
from models import Reading, Station, StationDetail

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Source registry metadata
# ---------------------------------------------------------------------------
SOURCE_NAME = "Federal Water (USACE + Reclamation)"
SOURCE_DESCRIPTION = (
    "US Army Corps of Engineers CWMS Data API (reservoirs, locks, dams, flood control) "
    "and Bureau of Reclamation RISE API (western US reservoirs, canals, diversions)."
)
SOURCE_META = {
    "url": "https://cwms-data.usace.army.mil/cwms-data/",
    "auth_required": False,
    "update_frequency": "15 min – 1 hr",
    "coverage": "United States",
    "params": ["water_level", "discharge", "storage", "precipitation", "temperature"],
    "category": "water",
}

router = APIRouter(prefix="/api/federal_water", tags=["Federal Water (USACE + Reclamation)"])

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
CWMS_BASE = "https://cwms-data.usace.army.mil/cwms-data"
RISE_BASE = "https://data.usbr.gov/rise/api"

# RISE API requires this Accept header to return JSON instead of a 406 error
_RISE_ACCEPT = "application/vnd.api+json"

# Known USACE district office codes
USACE_OFFICES = [
    "SWL", "SWD", "SWT", "SWF", "MVR", "MVS", "MVP", "MVN", "MVK",
    "LRL", "LRH", "LRB", "LRE", "LRN", "LRP",
    "NWP", "NWS", "NWD", "NWK", "NWO", "NWW",
    "SPK", "SPA", "SPL", "SPN",
    "SAM", "SAC", "SAJ", "SAS", "SAW",
    "ORD", "NAB", "NAE", "NAP", "NAN",
]

# CWMS Cloud Migration office-code mapping (FY2025-2026).
# Several NW-division districts were consolidated into new cloud office codes
# when their T7 on-premise servers were decommissioned.  The legacy codes
# (NWP, NWS, NWW, NWK, NWO) return 0 results from the cloud API; the new
# codes (NWDP, NWDM) hold all their data.
CWMS_OFFICE_MAP: dict[str, str] = {
    "NWP": "NWDP",   # Portland district -> NW Division Portland
    "NWS": "NWDP",   # Seattle district  -> NW Division Portland
    "NWW": "NWDP",   # Walla Walla       -> NW Division Portland
    "NWK": "NWDM",   # Kansas City       -> NW Division Missouri
    "NWO": "NWDM",   # Omaha             -> NW Division Missouri
}

# Map CWMS parameter names to normalized names and units
CWMS_PARAM_MAP: dict[str, tuple[str, str]] = {
    "Stage":            ("Water Level",       "ft"),
    "Flow":             ("Discharge",         "cfs"),
    "Stor":             ("Storage",           "ac-ft"),
    "Precip":           ("Precipitation",     "in"),
    "Temp-Water":       ("Water Temperature", "°F"),
    "Temp-Air":         ("Air Temperature",   "°F"),
    "Elev":             ("Elevation",         "ft"),
    "%-Capacity":       ("Capacity",          "%"),
    "Volt":             ("Voltage",           "V"),
    "Depth":            ("Depth",             "ft"),
    "Wind":             ("Wind Speed",        "mph"),
    "Dir":              ("Wind Direction",    "deg"),
}

_HTTP_HEADERS = {
    "User-Agent": config.USER_AGENT,
    "Accept": "application/json;version=2",
}


# ---------------------------------------------------------------------------
# Helper utilities
# ---------------------------------------------------------------------------

def _make_client() -> httpx.AsyncClient:
    return httpx.AsyncClient(
        headers=_HTTP_HEADERS,
        timeout=config.HTTP_TIMEOUT,
        follow_redirects=True,
    )


def _iso_utc(dt: datetime) -> str:
    """Format a datetime as an ISO-8601 UTC string suitable for CWMS API."""
    return dt.strftime("%Y-%m-%dT%H:%M:00Z")


def _parse_cwms_param(cwms_name: str) -> tuple[str, str]:
    """Derive a normalized parameter name and unit from a CWMS time-series name.

    CWMS time-series names follow the pattern:
        Location.Parameter.ParamType.Interval.Duration.Version
    e.g. 'Bagnell-Truman.Stage.Inst.15Minutes.0.Best-USGS'

    We extract the Parameter segment and look it up in CWMS_PARAM_MAP.
    """
    parts = cwms_name.split(".")
    if len(parts) >= 2:
        raw_param = parts[1]
        for key, (name, unit) in CWMS_PARAM_MAP.items():
            if raw_param.startswith(key):
                return name, unit
        return raw_param, ""
    return cwms_name, ""


def _cwms_location_to_station(raw: dict, office: str) -> Station:
    """Convert a CWMS location dict to a Station model.

    CWMS v2 nests fields: identity.name, label.public-name, geolocation.latitude, etc.
    v1 uses flat keys: name, public-name, latitude, longitude.  Handle both.
    """
    identity = raw.get("identity", {})
    label = raw.get("label", {})
    geo = raw.get("geolocation", {})

    loc_id = str(
        identity.get("name", "")
        or raw.get("name", "")
        or raw.get("location-id", "")
    )
    name = (
        label.get("public-name")
        or label.get("long-name")
        or raw.get("public-name")
        or raw.get("long-name")
        or loc_id
    )

    lat = 0.0
    lon = 0.0
    try:
        lat = float(geo.get("latitude", 0) or raw.get("latitude", 0) or 0)
        lon = float(geo.get("longitude", 0) or raw.get("longitude", 0) or 0)
    except (TypeError, ValueError):
        pass

    elev = None
    try:
        elev_raw = geo.get("elevation") or raw.get("elevation")
        if elev_raw is not None:
            elev = float(elev_raw)
    except (TypeError, ValueError):
        pass

    kind = raw.get("location-kind", raw.get("kind", ""))
    state = raw.get("state-initial", "")

    return Station(
        id=loc_id,
        name=name,
        lat=lat,
        lon=lon,
        source="federal_water",
        network=f"USACE-{office}",
        params=["water_level", "discharge", "storage", "precipitation", "temperature"],
        elevation=elev,
        url=f"https://cwms-data.usace.army.mil/cwms-data/locations?office={office}&names={loc_id}",
        extra={
            "office": office,
            "location_kind": kind,
            "state": state,
            "county": raw.get("county-name", ""),
            "timezone": raw.get("time-zone-name", ""),
            "nearest_city": raw.get("nearest-city", ""),
            "bounding_office": raw.get("bounding-office-id", ""),
        },
    )


def _parse_cwms_timeseries(data: dict, location_id: str) -> list[Reading]:
    """Parse a CWMS time-series response into a list of Reading objects.

    The CWMS API returns data under the key 'values' as a list of
    [epoch_ms, value, quality_code] triples.
    """
    readings: list[Reading] = []

    ts_name = data.get("name", "")
    param_name, unit = _parse_cwms_param(ts_name)

    units_raw = data.get("units", "")
    if units_raw:
        unit = units_raw

    values = data.get("values", [])
    if not values:
        # Some responses nest under a 'time-series' key
        values = data.get("time-series", {}).get("values", [])

    for entry in values:
        if not isinstance(entry, (list, tuple)) or len(entry) < 2:
            continue
        epoch_ms, raw_val = entry[0], entry[1]
        quality = str(entry[2]) if len(entry) > 2 else None

        if raw_val is None:
            continue
        try:
            ts = datetime.fromtimestamp(epoch_ms / 1000.0, tz=timezone.utc)
            readings.append(
                Reading(
                    station_id=location_id,
                    timestamp=ts,
                    parameter=param_name,
                    value=float(raw_val),
                    unit=unit,
                    quality=quality,
                    source="federal_water",
                )
            )
        except (TypeError, ValueError, OSError):
            continue

    return readings


def _rise_station_to_model(raw: dict) -> Station:
    """Convert a RISE /location dict to a Station model.

    The RISE location API returns JSON-API format with 'attributes' sub-dict
    containing locationName, locationCoordinates (GeoJSON), elevation, etc.
    """
    attrs = raw.get("attributes", raw)

    station_id = str(
        attrs.get("_id")
        or raw.get("id", "").rsplit("/", 1)[-1]
        or ""
    )
    name = (
        attrs.get("locationName")
        or attrs.get("itemTitle")
        or attrs.get("stationName")
        or station_id
    )

    # Coordinates from locationCoordinates GeoJSON Point
    lat = 0.0
    lon = 0.0
    try:
        coords_obj = attrs.get("locationCoordinates") or attrs.get("spatialGeometry")
        if isinstance(coords_obj, str):
            import json
            coords_obj = json.loads(coords_obj)
        if isinstance(coords_obj, dict) and coords_obj.get("type") == "Point":
            coords = coords_obj.get("coordinates", [])
            if len(coords) >= 2:
                lon = float(coords[0])
                lat = float(coords[1])
    except (TypeError, ValueError, Exception):
        pass

    elev = None
    try:
        elev_raw = attrs.get("elevation")
        if elev_raw is not None:
            elev = float(elev_raw)
    except (TypeError, ValueError):
        pass

    return Station(
        id=station_id,
        name=name,
        lat=lat,
        lon=lon,
        source="federal_water",
        network="Reclamation-RISE",
        params=["water_level", "discharge", "storage"],
        elevation=elev,
        url=f"https://data.usbr.gov/rise/api/location/{station_id}",
        extra={
            "location_description": (attrs.get("locationDescription") or "")[:200],
        },
    )


def _parse_rise_results(data: dict, station_id: str) -> list[Reading]:
    """Parse a RISE /result/download response into a list of Reading objects.

    RISE returns data as a list of dicts with 'result' entries that each
    contain 'dateTime' and 'result' (the numeric value), plus metadata
    about the parameter and unit at the top level.
    """
    readings: list[Reading] = []

    # Determine the parameter label and unit from the catalog metadata
    meta = data.get("metaData", data.get("metadata", {}))
    if not isinstance(meta, dict):
        meta = {}
    param_name = meta.get("parameterName") or meta.get("parameter") or "Value"
    unit_raw = meta.get("unit") or meta.get("units") or ""

    # Normalize common unit strings
    unit_map = {
        "ft": "ft",
        "feet": "ft",
        "cfs": "cfs",
        "af": "ac-ft",
        "acre-feet": "ac-ft",
        "acre-ft": "ac-ft",
        "inches": "in",
        "in": "in",
        "degf": "°F",
        "°f": "°F",
        "degc": "°C",
        "°c": "°C",
        "%": "%",
    }
    unit = unit_map.get(unit_raw.lower(), unit_raw) if unit_raw else ""

    # Normalize param_name for common types
    param_lower = param_name.lower()
    if any(k in param_lower for k in ("elevation", "pool elevation", "reservoir elevation")):
        param_name = "Water Level"
    elif any(k in param_lower for k in ("outflow", "release", "discharge", "flow")):
        param_name = "Discharge"
    elif any(k in param_lower for k in ("storage", "content", "volume")):
        param_name = "Storage"
    elif "precip" in param_lower:
        param_name = "Precipitation"
    elif "temp" in param_lower:
        param_name = "Temperature"

    # RISE download result structure: {"data": [{"dateTime": "...", "result": 123.4}, ...]}
    result_list = data.get("data", data.get("results", []))
    if not isinstance(result_list, list):
        return readings

    for entry in result_list:
        if not isinstance(entry, dict):
            continue
        dt_str = entry.get("dateTime") or entry.get("date") or entry.get("timestamp") or ""
        raw_val = entry.get("result") or entry.get("value")

        if not dt_str or raw_val is None:
            continue

        try:
            ts = datetime.fromisoformat(dt_str.replace("Z", "+00:00"))
        except (ValueError, AttributeError):
            continue

        try:
            readings.append(
                Reading(
                    station_id=station_id,
                    timestamp=ts,
                    parameter=param_name,
                    value=float(raw_val),
                    unit=unit,
                    quality=entry.get("qualityCode") or entry.get("quality"),
                    source="federal_water",
                )
            )
        except (TypeError, ValueError):
            continue

    return readings


# ---------------------------------------------------------------------------
# USACE CWMS Endpoints
# ---------------------------------------------------------------------------

@router.get("/usace/locations", summary="List USACE CWMS monitoring locations for an office")
async def usace_list_locations(
    office: str = Query(..., description="USACE office code, e.g. 'SWL', 'NWP', 'MVR'"),
    limit: int = Query(500, ge=1, le=5000, description="Maximum number of locations to return"),
):
    """Return USACE CWMS monitoring locations for the specified district office.

    Office codes correspond to USACE district offices, e.g.:
    - SWL = Southwestern Division / Little Rock
    - NWP = Northwestern Division / Portland
    - MVR = Mississippi Valley Division / Rock Island
    - LRL = Great Lakes and Ohio River Division / Louisville

    Results are cached for one hour.
    """
    office_upper = office.upper()
    # Translate legacy office codes to their post-migration cloud equivalents
    cwms_office = CWMS_OFFICE_MAP.get(office_upper, office_upper)
    cache_key = f"federal_water:usace:locations:{office_upper}:{limit}"
    cached = await cache.get(cache_key)
    if cached is not None:
        return cached

    # Use /catalog/locations (the /locations endpoint often returns 500 upstream).
    url = f"{CWMS_BASE}/catalog/locations"
    params_query: dict = {
        "office": cwms_office,
        "page-size": min(limit, 5000),
    }
    # When a legacy office was mapped to a consolidated cloud office, filter by
    # bounding-office so only locations belonging to the requested district are
    # returned (e.g. NWP locations within the NWDP cloud office).
    if cwms_office != office_upper:
        params_query["bounding-office-like"] = office_upper

    try:
        async with _make_client() as client:
            resp = await client.get(
                url,
                params=params_query,
                headers={"Accept": "application/json"},
            )
            resp.raise_for_status()
            # Some offices return non-UTF-8 chars (e.g. 0xa0 non-breaking space)
            import json as _json
            text = resp.content.decode("utf-8", errors="replace")
            data = _json.loads(text)
    except httpx.TimeoutException:
        raise HTTPException(status_code=504, detail="USACE CWMS API timeout")
    except httpx.HTTPStatusError as exc:
        if exc.response.status_code == 404:
            raise HTTPException(
                status_code=404,
                detail=f"USACE office '{office_upper}' not found. Valid offices: {', '.join(USACE_OFFICES)}",
            )
        raise HTTPException(
            status_code=exc.response.status_code,
            detail=f"USACE CWMS API error: {exc}",
        )
    except Exception as exc:
        logger.exception("Error fetching USACE location list for office %s", office_upper)
        raise HTTPException(status_code=500, detail=str(exc))

    # /catalog/locations returns {"entries": [...], "page-size": N, "total": N}
    raw_locations: list[dict] = data.get("entries", []) if isinstance(data, dict) else []

    if not raw_locations:
        logger.warning(
            "USACE catalog for office %s returned 0 entries (total=%s)",
            office_upper,
            data.get("total", "?") if isinstance(data, dict) else "N/A",
        )

    stations = [
        _cwms_location_to_station(loc, office_upper)
        for loc in raw_locations[:limit]
    ]

    result = {
        "office": office_upper,
        "stations": [s.model_dump() for s in stations],
        "count": len(stations),
    }
    await cache.set(cache_key, result, ttl=config.CACHE_TTL_LONG)
    return result


@router.get(
    "/usace/locations/{location_id}",
    summary="USACE CWMS location detail with latest readings",
)
async def usace_get_location(
    location_id: str,
    office: str = Query(..., description="USACE office code, e.g. 'SWL', 'NWP'"),
):
    """Return USACE CWMS location metadata and the most recent readings for
    each available time series at that location.

    The endpoint first discovers available time series via the catalog, then
    fetches the last 2 hours of data for each series and returns only the
    single most recent reading per parameter.
    """
    office_upper = office.upper()
    cwms_office = CWMS_OFFICE_MAP.get(office_upper, office_upper)
    cache_key = f"federal_water:usace:location:{office_upper}:{location_id}"
    cached = await cache.get(cache_key)
    if cached is not None:
        return cached

    # Fetch location metadata
    loc_url = f"{CWMS_BASE}/locations"
    loc_params: dict = {
        "office": cwms_office,
        "names": location_id,
        "format": "json",
    }

    try:
        async with _make_client() as client:
            loc_resp = await client.get(loc_url, params=loc_params)
            loc_resp.raise_for_status()
            loc_data = loc_resp.json()
    except httpx.HTTPStatusError as exc:
        if exc.response.status_code == 404:
            raise HTTPException(
                status_code=404,
                detail=f"USACE location '{location_id}' not found in office '{office_upper}'",
            )
        raise HTTPException(
            status_code=exc.response.status_code,
            detail=f"USACE CWMS API error: {exc}",
        )
    except httpx.TimeoutException:
        raise HTTPException(status_code=504, detail="USACE CWMS API timeout")
    except Exception as exc:
        logger.exception("Error fetching USACE location %s / %s", office_upper, location_id)
        raise HTTPException(status_code=500, detail=str(exc))

    raw_locs: list[dict] = []
    if isinstance(loc_data, list):
        raw_locs = loc_data
    elif isinstance(loc_data, dict):
        block = loc_data.get("locations", loc_data)
        if isinstance(block, dict):
            raw_locs = block.get("location", [])
        elif isinstance(block, list):
            raw_locs = block

    if not raw_locs:
        raise HTTPException(
            status_code=404,
            detail=f"USACE location '{location_id}' not found in office '{office_upper}'",
        )

    station = _cwms_location_to_station(raw_locs[0], office_upper)

    # Discover available time series for this location
    cat_url = f"{CWMS_BASE}/catalog/timeseries"
    cat_params: dict = {
        "office": cwms_office,
        "like": f"{location_id}.*",
        "format": "json",
    }

    ts_names: list[str] = []
    try:
        async with _make_client() as client:
            cat_resp = await client.get(cat_url, params=cat_params)
            cat_resp.raise_for_status()
            cat_data = cat_resp.json()

        if isinstance(cat_data, list):
            ts_names = [e.get("name", "") for e in cat_data if e.get("name")]
        elif isinstance(cat_data, dict):
            entries = cat_data.get("time-series", cat_data.get("timeseries", []))
            if isinstance(entries, list):
                ts_names = [e.get("name", "") for e in entries if e.get("name")]
    except Exception as exc:
        logger.debug(
            "Could not fetch CWMS catalog for %s / %s: %s",
            office_upper, location_id, exc,
        )

    # Fetch last 2 hours for each discovered time series (cap at 10)
    now = datetime.now(tz=timezone.utc)
    begin_dt = now - timedelta(hours=2)
    all_readings: list[Reading] = []

    ts_to_fetch = ts_names[:10]
    if not ts_to_fetch:
        # Fall back: try common Stage and Flow series directly
        ts_to_fetch = [
            f"{location_id}.Stage.Inst.15Minutes.0.Best",
            f"{location_id}.Flow.Inst.15Minutes.0.Best",
            f"{location_id}.Stor.Inst.1Hour.0.Best",
        ]

    async with _make_client() as client:
        for ts_name in ts_to_fetch:
            ts_params: dict = {
                "office": cwms_office,
                "name": ts_name,
                "begin": _iso_utc(begin_dt),
                "end": _iso_utc(now),
                "format": "json",
            }
            try:
                resp = await client.get(f"{CWMS_BASE}/timeseries", params=ts_params)
                resp.raise_for_status()
                ts_data = resp.json()

                # CWMS wraps the time series under a list or a single object
                ts_entries = ts_data if isinstance(ts_data, list) else [ts_data]
                for ts_entry in ts_entries:
                    if isinstance(ts_entry, dict):
                        all_readings.extend(_parse_cwms_timeseries(ts_entry, location_id))
            except Exception as exc:
                logger.debug(
                    "CWMS time series '%s' unavailable for %s: %s",
                    ts_name, location_id, exc,
                )

    # Keep only the most recent reading per parameter
    latest: dict[str, Reading] = {}
    for r in all_readings:
        if r.parameter not in latest or r.timestamp > latest[r.parameter].timestamp:
            latest[r.parameter] = r

    detail = StationDetail(station=station, readings=list(latest.values()))
    result = detail.model_dump()
    await cache.set(cache_key, result, ttl=config.CACHE_TTL)
    return result


@router.get("/usace/catalog", summary="List available USACE CWMS time series for an office")
async def usace_catalog(
    office: str = Query(..., description="USACE office code, e.g. 'SWL', 'NWP'"),
    limit: int = Query(200, ge=1, le=2000, description="Maximum number of time series to return"),
):
    """Return the catalog of available time series for a USACE district office.

    Each entry describes one time series by its fully-qualified CWMS name,
    e.g. 'Bagnell-Truman.Stage.Inst.15Minutes.0.Best-USGS'.

    Results are cached for one hour.
    """
    office_upper = office.upper()
    cwms_office = CWMS_OFFICE_MAP.get(office_upper, office_upper)
    cache_key = f"federal_water:usace:catalog:{office_upper}:{limit}"
    cached = await cache.get(cache_key)
    if cached is not None:
        return cached

    url = f"{CWMS_BASE}/catalog/timeseries"
    params_query: dict = {
        "office": cwms_office,
        "format": "json",
    }
    if cwms_office != office_upper:
        params_query["bounding-office-like"] = office_upper

    try:
        async with _make_client() as client:
            resp = await client.get(url, params=params_query)
            resp.raise_for_status()
            data = resp.json()
    except httpx.TimeoutException:
        raise HTTPException(status_code=504, detail="USACE CWMS catalog API timeout")
    except httpx.HTTPStatusError as exc:
        if exc.response.status_code == 404:
            raise HTTPException(
                status_code=404,
                detail=f"USACE office '{office_upper}' catalog not found",
            )
        raise HTTPException(
            status_code=exc.response.status_code,
            detail=f"USACE CWMS API error: {exc}",
        )
    except Exception as exc:
        logger.exception("Error fetching USACE catalog for office %s", office_upper)
        raise HTTPException(status_code=500, detail=str(exc))

    raw_entries: list[dict] = []
    if isinstance(data, list):
        raw_entries = data
    elif isinstance(data, dict):
        # Various response shapes seen from CWMS
        raw_entries = (
            data.get("time-series")
            or data.get("timeseries")
            or data.get("entries")
            or []
        )

    entries = raw_entries[:limit]

    # Normalize each entry to a minimal summary dict
    catalog_items = []
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        ts_name = entry.get("name", "")
        param_name, unit = _parse_cwms_param(ts_name)
        catalog_items.append(
            {
                "name": ts_name,
                "parameter": param_name,
                "unit": unit,
                "interval": entry.get("interval", ""),
                "office": entry.get("office", office_upper),
                "location": ts_name.split(".")[0] if ts_name else "",
            }
        )

    result = {
        "office": office_upper,
        "catalog": catalog_items,
        "count": len(catalog_items),
    }
    await cache.set(cache_key, result, ttl=config.CACHE_TTL_LONG)
    return result


# ---------------------------------------------------------------------------
# Bureau of Reclamation RISE Endpoints
# ---------------------------------------------------------------------------

@router.get("/reclamation/stations", summary="List Bureau of Reclamation RISE stations")
async def reclamation_list_stations(
    state: Optional[str] = Query(
        None,
        description="Two-letter US state code filter, e.g. 'AZ', 'CO', 'NV'",
    ),
    limit: int = Query(500, ge=1, le=2000, description="Maximum number of stations to return"),
):
    """Return Bureau of Reclamation RISE stations, optionally filtered by state.

    Covers ~800+ monitoring stations across 17 western states including
    reservoirs, canals, diversions, and weather stations operated or managed
    by the Bureau of Reclamation.

    Results are cached for one hour.
    """
    state_upper = state.upper() if state else None
    cache_key = f"federal_water:reclamation:stations:{state_upper}:{limit}"
    cached = await cache.get(cache_key)
    if cached is not None:
        return cached

    # Use /location endpoint (not /catalog-item) which returns actual geolocated
    # facilities (dams, reservoirs, canals, etc.) with coordinates.
    url = f"{RISE_BASE}/location"
    rise_headers = {**_HTTP_HEADERS, "Accept": _RISE_ACCEPT}

    # Fetch all locations (982 total as of 2026) — paginate in chunks of 200.
    all_locations: list[dict] = []
    page = 1
    items_per_page = 200
    try:
        async with httpx.AsyncClient(
            headers=rise_headers,
            timeout=config.HTTP_TIMEOUT,
            follow_redirects=True,
        ) as client:
            while True:
                resp = await client.get(url, params={"page": page, "itemsPerPage": items_per_page})
                resp.raise_for_status()
                data = resp.json()
                items = data.get("data", []) if isinstance(data, dict) else []
                if not items:
                    break
                all_locations.extend(items)
                total = data.get("meta", {}).get("totalItems", 0)
                if len(all_locations) >= total or len(items) < items_per_page:
                    break
                page += 1
    except httpx.TimeoutException:
        raise HTTPException(status_code=504, detail="Bureau of Reclamation RISE API timeout")
    except httpx.HTTPStatusError as exc:
        raise HTTPException(
            status_code=exc.response.status_code,
            detail=f"Bureau of Reclamation RISE API error: {exc}",
        )
    except Exception as exc:
        logger.exception("Error fetching Reclamation RISE location list")
        raise HTTPException(status_code=500, detail=str(exc))

    # Filter to Point-type locations only (skip polygons, linestrings, study areas)
    raw_stations: list[dict] = []
    for loc in all_locations:
        attrs = loc.get("attributes", loc)
        coords = attrs.get("locationCoordinates", {})
        if coords.get("type") != "Point":
            continue
        raw_stations.append(loc)

    stations = [_rise_station_to_model(s) for s in raw_stations[:limit]]

    result = {
        "state_filter": state_upper,
        "stations": [s.model_dump() for s in stations],
        "count": len(stations),
    }
    await cache.set(cache_key, result, ttl=config.CACHE_TTL_LONG)
    return result


@router.get(
    "/reclamation/stations/{station_id}",
    summary="Bureau of Reclamation RISE station detail with recent data",
)
async def reclamation_get_station(
    station_id: str,
    days: int = Query(7, ge=1, le=365, description="Number of days of history to return (max 365)"),
):
    """Return Bureau of Reclamation RISE station metadata and recent time-series data.

    Fetches station detail from the catalog and then downloads the most recent
    observations for the requested number of days (default 7, max 365).

    Station IDs are numeric RISE catalog-item IDs, e.g. '509' for Hoover Dam.
    """
    cache_key = f"federal_water:reclamation:station:{station_id}:{days}"
    cached = await cache.get(cache_key)
    if cached is not None:
        return cached

    # Fetch station detail from catalog (correct endpoint: /catalog-item/{id})
    detail_url = f"{RISE_BASE}/catalog-item/{station_id}"
    rise_headers = {**_HTTP_HEADERS, "Accept": _RISE_ACCEPT}
    try:
        async with httpx.AsyncClient(
            headers=rise_headers,
            timeout=config.HTTP_TIMEOUT,
            follow_redirects=True,
        ) as client:
            detail_resp = await client.get(detail_url)
            detail_resp.raise_for_status()
            detail_data = detail_resp.json()
    except httpx.HTTPStatusError as exc:
        if exc.response.status_code == 404:
            raise HTTPException(
                status_code=404,
                detail=f"Reclamation RISE station '{station_id}' not found",
            )
        raise HTTPException(
            status_code=exc.response.status_code,
            detail=f"Bureau of Reclamation RISE API error: {exc}",
        )
    except httpx.TimeoutException:
        raise HTTPException(status_code=504, detail="Bureau of Reclamation RISE API timeout")
    except Exception as exc:
        logger.exception("Error fetching Reclamation RISE station %s", station_id)
        raise HTTPException(status_code=500, detail=str(exc))

    # Station detail may be at top level or under 'data'
    raw_station = detail_data
    if isinstance(detail_data, dict) and "data" in detail_data:
        raw_station = detail_data["data"]
        if isinstance(raw_station, list) and raw_station:
            raw_station = raw_station[0]

    station = _rise_station_to_model(raw_station)

    # Fetch time-series data for the requested date range
    now = datetime.now(tz=timezone.utc)
    after_dt = now - timedelta(days=days)
    before_str = now.strftime("%Y-%m-%d")
    after_str = after_dt.strftime("%Y-%m-%d")

    readings: list[Reading] = []
    download_url = f"{RISE_BASE}/result/download"
    dl_params: dict = {
        "type": "json",
        "itemId": station_id,
        "before": before_str,
        "after": after_str,
    }

    try:
        async with _make_client() as client:
            dl_resp = await client.get(download_url, params=dl_params)
            dl_resp.raise_for_status()
            dl_data = dl_resp.json()

        readings = _parse_rise_results(dl_data, station_id)
        readings.sort(key=lambda r: r.timestamp)
    except httpx.TimeoutException:
        logger.warning(
            "Timeout fetching RISE data for station %s; returning metadata only",
            station_id,
        )
    except httpx.HTTPStatusError as exc:
        logger.warning(
            "RISE download error for station %s (HTTP %s); returning metadata only",
            station_id,
            exc.response.status_code,
        )
    except Exception as exc:
        logger.warning(
            "Could not fetch RISE readings for station %s: %s; returning metadata only",
            station_id, exc,
        )

    detail = StationDetail(station=station, readings=readings)
    result = {
        **detail.model_dump(),
        "days_requested": days,
        "date_range": {"after": after_str, "before": before_str},
    }
    await cache.set(cache_key, result, ttl=config.CACHE_TTL)
    return result
