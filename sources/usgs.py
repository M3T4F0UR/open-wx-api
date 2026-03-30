"""USGS National Water Information System (NWIS) — Instantaneous Values.

Provides real-time streamflow, gage height, precipitation, water temperature,
and other hydrological observations from ~10,000 active USGS monitoring sites
across the United States.

API reference: https://waterservices.usgs.gov/docs/instantaneous-values/
Flood impacts: https://api.waterdata.usgs.gov/rtfi-api/
"""
import logging
from datetime import datetime, timezone
from typing import Optional

import httpx
from fastapi import APIRouter, HTTPException, Query

import cache
import config
import http_client
from models import Reading, Station, StationDetail

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Source registry metadata
# ---------------------------------------------------------------------------
SOURCE_NAME = "USGS NWIS"
SOURCE_DESCRIPTION = (
    "USGS National Water Information System — real-time streamflow, gage height, "
    "precipitation, and water-temperature data from ~10,000 monitoring stations "
    "across the United States."
)
SOURCE_META = {
    "url": "https://waterservices.usgs.gov/",
    "auth_required": False,
    "update_frequency": "15 min",
    "coverage": "United States",
    "params": ["discharge", "gage_height", "precipitation", "water_temperature"],
    "category": "water",
}

router = APIRouter(prefix="/api/usgs", tags=["USGS NWIS"])

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
NWIS_IV_BASE = "https://waterservices.usgs.gov/nwis/iv/"
RTFI_BASE = "https://api.waterdata.usgs.gov/rtfi-api/"

# Map USGS parameter codes to human-readable names and units
PARAM_MAP: dict[str, dict[str, str]] = {
    "00060": {"name": "Discharge",          "unit": "ft³/s"},
    "00065": {"name": "Gage Height",         "unit": "ft"},
    "00045": {"name": "Precipitation",       "unit": "in"},
    "00010": {"name": "Water Temperature",   "unit": "°C"},
    "00011": {"name": "Water Temperature",   "unit": "°F"},
    "00300": {"name": "Dissolved Oxygen",    "unit": "mg/L"},
    "00400": {"name": "pH",                  "unit": "std units"},
    "00095": {"name": "Specific Conductance","unit": "µS/cm"},
    "00480": {"name": "Salinity",            "unit": "ppt"},
    "72019": {"name": "Depth to Water",      "unit": "ft"},
    "62614": {"name": "Lake/Reservoir Elevation", "unit": "ft"},
    "72020": {"name": "Elevation of Reservoir Water Surface", "unit": "ft"},
    "63160": {"name": "Stream Water Level",  "unit": "ft"},
    "00035": {"name": "Wind Speed",          "unit": "mph"},
    "00036": {"name": "Wind Direction",      "unit": "deg"},
}

DEFAULT_PARAMS = "00060,00065,00045,00010"


# ---------------------------------------------------------------------------
# Helper utilities
# ---------------------------------------------------------------------------


def _param_info(code: str) -> dict[str, str]:
    """Return human-readable name and unit for a USGS parameter code."""
    return PARAM_MAP.get(code, {"name": code, "unit": "unknown"})


def _safe_get(info: dict, key: str) -> str:
    """Safely extract a value that may be a string, list of dicts, or missing."""
    val = info.get(key, "")
    if isinstance(val, str):
        return val
    if isinstance(val, list) and val:
        item = val[0]
        return item.get("value", str(item)) if isinstance(item, dict) else str(item)
    return ""


def _parse_station(source_info: dict) -> Station:
    """Convert a USGS sourceInfo block to a Station model."""
    site_code = _safe_get(source_info, "siteCode")
    geo = source_info.get("geoLocation", {}).get("geogLocation", {})
    lat = float(geo.get("latitude", 0.0))
    lon = float(geo.get("longitude", 0.0))

    # Build list of parameters measured at this site from all timeSeries in
    # the parent response — we attach params later via _parse_time_series.
    elev_raw = source_info.get("elevation")
    try:
        elevation = float(elev_raw) if elev_raw is not None else None
    except (TypeError, ValueError):
        elevation = None

    agency = source_info.get("agencyCode", "USGS")

    return Station(
        id=site_code,
        name=source_info.get("siteName", site_code),
        lat=lat,
        lon=lon,
        source="usgs",
        network=agency,
        elevation=elevation,
        url=f"https://waterdata.usgs.gov/nwis/uv?site_no={site_code}",
        extra={
            "site_type": _safe_get(source_info, "siteType"),
            "huc": _safe_get(source_info, "hucCd"),
        },
    )


def _parse_readings(time_series_list: list[dict], station_id: str) -> tuple[list[Reading], list[str]]:
    """Parse a list of USGS timeSeries entries into Readings and param names."""
    readings: list[Reading] = []
    params: list[str] = []

    for ts in time_series_list:
        variable = ts.get("variable", {})
        param_code = _safe_get(variable, "variableCode")
        param_info = _param_info(param_code)
        param_name = param_info["name"]
        unit = variable.get("unit", {}).get("unitCode", param_info["unit"])

        if param_name not in params:
            params.append(param_name)

        values_list = ts.get("values", [])
        values_block = values_list[0].get("value", []) if values_list else []
        for entry in values_block:
            raw_val = entry.get("value")
            if raw_val in (None, "", "-999999"):
                continue
            try:
                value = float(raw_val)
            except (TypeError, ValueError):
                continue

            dt_str = entry.get("dateTime", "")
            try:
                # USGS returns ISO-8601 with offset, e.g. "2024-01-01T12:00:00.000-05:00"
                ts_dt = datetime.fromisoformat(dt_str)
            except ValueError:
                continue

            qualifiers = entry.get("qualifiers", [])
            quality = ",".join(qualifiers) if qualifiers else None

            readings.append(
                Reading(
                    station_id=station_id,
                    timestamp=ts_dt,
                    parameter=param_name,
                    value=value,
                    unit=unit,
                    quality=quality,
                    source="usgs",
                )
            )

    return readings, params


def _parse_response(data: dict) -> tuple[list[Station], dict[str, list[Reading]]]:
    """Parse full USGS IV JSON response into stations and their readings."""
    stations: list[Station] = []
    readings_map: dict[str, list[Reading]] = {}
    seen_sites: dict[str, Station] = {}

    time_series_list = data.get("value", {}).get("timeSeries", [])

    # Group time series by site ID
    ts_by_site: dict[str, list[dict]] = {}
    for ts in time_series_list:
        site_id = _safe_get(ts.get("sourceInfo", {}), "siteCode")
        if site_id:
            ts_by_site.setdefault(site_id, []).append(ts)

    for site_id, ts_list in ts_by_site.items():
        station = _parse_station(ts_list[0]["sourceInfo"])
        readings, params = _parse_readings(ts_list, site_id)

        station.params = params
        seen_sites[site_id] = station
        stations.append(station)
        readings_map[site_id] = readings

    return stations, readings_map


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@router.get("/stations", summary="List USGS NWIS monitoring stations")
async def list_stations(
    state: Optional[str] = Query(
        None,
        description="Two-letter US state code (e.g. 'OR', 'CA')",
        min_length=2,
        max_length=2,
    ),
    bbox: Optional[str] = Query(
        None,
        description="Bounding box as west,south,east,north (decimal degrees)",
    ),
    param: Optional[str] = Query(
        None,
        description="USGS parameter code(s), comma-separated (e.g. '00060', '00060,00065')",
    ),
    limit: int = Query(500, ge=1, le=10000, description="Maximum number of stations to return"),
):
    """List active USGS NWIS monitoring stations.

    Filter by state abbreviation OR bounding box. At least one filter is
    recommended; without a spatial constraint the response may be very large.
    """
    if not state and not bbox:
        raise HTTPException(
            status_code=400,
            detail="Provide at least one of: state (2-letter code) or bbox (west,south,east,north)",
        )

    param_codes = param or DEFAULT_PARAMS
    cache_key = f"usgs:stations:{state}:{bbox}:{param_codes}:{limit}"

    cached = await cache.get(cache_key)
    if cached is not None:
        return cached

    params_query: dict[str, str] = {
        "format": "json",
        "parameterCd": param_codes,
        "siteStatus": "active",
        "siteType": "ST,LK,ES,SP,AT,WE",  # streams, lakes, estuaries, springs, etc.
    }

    if state:
        params_query["stateCd"] = state.upper()
    elif bbox:
        parts = [p.strip() for p in bbox.split(",")]
        if len(parts) != 4:
            raise HTTPException(status_code=400, detail="bbox must be west,south,east,north")
        params_query["bBox"] = ",".join(parts)

    try:
        data = await http_client.fetch_json(NWIS_IV_BASE, params=params_query, source="usgs")
    except http_client.CircuitOpenError as exc:
        raise HTTPException(status_code=503, detail=str(exc))
    except httpx.TimeoutException:
        raise HTTPException(status_code=504, detail="USGS API timeout")
    except httpx.HTTPStatusError as exc:
        raise HTTPException(status_code=exc.response.status_code, detail=f"USGS API error: {exc}")
    except Exception as exc:
        logger.exception("Unexpected error fetching USGS station list")
        raise HTTPException(status_code=500, detail=str(exc))

    stations, _ = _parse_response(data)
    stations = stations[:limit]
    result = {"stations": [s.model_dump() for s in stations], "count": len(stations)}
    await cache.set(cache_key, result, ttl=config.CACHE_TTL_LONG)
    return result


@router.get("/stations/{site_id}", summary="Single USGS station with latest readings")
async def get_station(site_id: str):
    """Return metadata and the most recent reading for each parameter at a USGS site."""
    cache_key = f"usgs:station:{site_id}"
    cached = await cache.get(cache_key)
    if cached is not None:
        return cached

    params_query = {
        "format": "json",
        "sites": site_id,
        "parameterCd": DEFAULT_PARAMS,
        "period": "PT3H",  # last 3 hours — enough to get a current value
        "siteStatus": "all",
    }

    try:
        data = await http_client.fetch_json(NWIS_IV_BASE, params=params_query, source="usgs")
    except http_client.CircuitOpenError as exc:
        raise HTTPException(status_code=503, detail=str(exc))
    except httpx.TimeoutException:
        raise HTTPException(status_code=504, detail="USGS API timeout")
    except httpx.HTTPStatusError as exc:
        raise HTTPException(status_code=exc.response.status_code, detail=f"USGS API error: {exc}")
    except Exception as exc:
        logger.exception("Error fetching USGS station %s", site_id)
        raise HTTPException(status_code=500, detail=str(exc))

    stations, readings_map = _parse_response(data)
    if not stations:
        raise HTTPException(status_code=404, detail=f"USGS site '{site_id}' not found or inactive")

    station = stations[0]
    all_readings = readings_map.get(site_id, [])

    # Keep only the most recent reading per parameter
    latest: dict[str, Reading] = {}
    for r in all_readings:
        if r.parameter not in latest or r.timestamp > latest[r.parameter].timestamp:
            latest[r.parameter] = r

    detail = StationDetail(station=station, readings=list(latest.values()))
    result = detail.model_dump()
    await cache.set(cache_key, result, ttl=config.CACHE_TTL)
    return result


@router.get("/stations/{site_id}/readings", summary="Time-series readings for a USGS station")
async def get_readings(
    site_id: str,
    hours: int = Query(24, ge=1, le=2160, description="Number of hours of history (max 90 days = 2160 h)"),
    param: Optional[str] = Query(
        None,
        description="Parameter code(s), comma-separated. Defaults to 00060,00065,00045,00010",
    ),
):
    """Return time-series observations for a USGS NWIS site.

    Uses the ISO-8601 period notation accepted by the USGS IV API
    (e.g. PT24H for 24 hours).
    """
    param_codes = param or DEFAULT_PARAMS
    cache_key = f"usgs:readings:{site_id}:{hours}:{param_codes}"
    cached = await cache.get(cache_key)
    if cached is not None:
        return cached

    params_query = {
        "format": "json",
        "sites": site_id,
        "parameterCd": param_codes,
        "period": f"PT{hours}H",
        "siteStatus": "all",
    }

    try:
        data = await http_client.fetch_json(NWIS_IV_BASE, params=params_query, source="usgs")
    except http_client.CircuitOpenError as exc:
        raise HTTPException(status_code=503, detail=str(exc))
    except httpx.TimeoutException:
        raise HTTPException(status_code=504, detail="USGS API timeout")
    except httpx.HTTPStatusError as exc:
        raise HTTPException(status_code=exc.response.status_code, detail=f"USGS API error: {exc}")
    except Exception as exc:
        logger.exception("Error fetching USGS readings for %s", site_id)
        raise HTTPException(status_code=500, detail=str(exc))

    stations, readings_map = _parse_response(data)
    if not stations:
        raise HTTPException(status_code=404, detail=f"USGS site '{site_id}' not found")

    readings = readings_map.get(site_id, [])
    readings.sort(key=lambda r: r.timestamp)

    result = {
        "station_id": site_id,
        "hours": hours,
        "params": param_codes,
        "count": len(readings),
        "readings": [r.model_dump() for r in readings],
    }
    await cache.set(cache_key, result, ttl=config.CACHE_TTL)
    return result


@router.get("/flood-impacts", summary="USGS Real-Time Flood Impact data")
async def flood_impacts(
    state: Optional[str] = Query(None, description="Two-letter state code filter"),
    limit: int = Query(200, ge=1, le=2000),
):
    """Proxy to the USGS Real-Time Flood Impacts (RTFI) API.

    Returns current flood stage information and impact descriptions for
    USGS-monitored sites that have associated flood impact data.

    RTFI API docs: https://api.waterdata.usgs.gov/rtfi-api/
    """
    cache_key = f"usgs:flood_impacts:{state}:{limit}"
    cached = await cache.get(cache_key)
    if cached is not None:
        return cached

    # RTFI endpoint — returns GeoJSON-style feature list
    url = f"{RTFI_BASE}flood-impacts"
    params_query: dict[str, str | int] = {"limit": limit}
    if state:
        params_query["stateCd"] = state.upper()

    try:
        data = await http_client.fetch_json(url, params=params_query, source="usgs")
    except http_client.CircuitOpenError as exc:
        raise HTTPException(status_code=503, detail=str(exc))
    except httpx.TimeoutException:
        raise HTTPException(status_code=504, detail="USGS RTFI API timeout")
    except httpx.HTTPStatusError as exc:
        # RTFI API may return 404 when no data is available for a state filter
        if exc.response.status_code == 404:
            data = {"features": [], "count": 0}
        else:
            raise HTTPException(status_code=exc.response.status_code, detail=f"USGS RTFI error: {exc}")
    except Exception as exc:
        logger.exception("Error fetching USGS flood impacts")
        raise HTTPException(status_code=500, detail=str(exc))

    result = {
        "source": "usgs_rtfi",
        "state_filter": state,
        "data": data,
    }
    await cache.set(cache_key, result, ttl=config.CACHE_TTL)
    return result
