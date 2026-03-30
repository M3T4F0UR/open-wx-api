"""Agricultural Weather Networks — Wisconet & NEWA.

No-auth agricultural mesonet data from university-operated networks:

  1. **Wisconet** (Wisconsin Environmental Mesonet) — ~78 stations across
     Wisconsin measuring air temp, precip, wind, RH, soil temp/moisture, and
     solar radiation.  Clean REST/JSON API (v1 and v2).
     API docs: https://api.wisconet.wisc.edu/docs

  2. **NEWA** (Network for Environment and Weather Applications, Cornell) —
     ~717 farm weather stations in 28 US states.  NEWA is a Next.js web app
     with no documented public API; station metadata is fetched from a known
     GeoJSON endpoint discovered via network inspection.
     Web: https://newa.cornell.edu/

Both sources are free, no API key required.
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Optional

import httpx
from fastapi import APIRouter, HTTPException, Query

import cache
import config
from models import Reading, Station, StationDetail

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Source registry metadata  (read by server.py /api/sources)
# ---------------------------------------------------------------------------

SOURCE_NAME = "Agricultural Weather Networks"
SOURCE_DESCRIPTION = (
    "University-operated agricultural mesonets — Wisconet (Wisconsin "
    "Environmental Mesonet, ~78 stations) and NEWA (Cornell, ~717 farm "
    "stations across 28 US states).  Free, no authentication required.  "
    "Parameters include temperature, precipitation, wind, relative humidity, "
    "soil temperature/moisture, and solar radiation."
)
SOURCE_META = {
    "url": "https://api.wisconet.wisc.edu/docs",
    "auth_required": False,
    "update_frequency": "5–60 min (varies by network)",
    "coverage": "United States — Wisconsin (Wisconet), 28 states (NEWA)",
    "params": [
        "temperature", "dewpoint", "relative_humidity", "wind_speed",
        "wind_direction", "wind_gust", "precipitation", "pressure",
        "soil_temperature", "soil_moisture", "solar_radiation",
    ],
    "category": "agriculture",
    "networks": {
        "wisconet": {
            "name": "Wisconet (Wisconsin Environmental Mesonet)",
            "url": "https://api.wisconet.wisc.edu/docs",
            "station_count": 78,
        },
        "newa": {
            "name": "NEWA (Network for Environment and Weather Applications)",
            "url": "https://newa.cornell.edu/",
            "station_count": 717,
        },
    },
}

# ---------------------------------------------------------------------------
# Router
# ---------------------------------------------------------------------------

router = APIRouter(prefix="/api/ag_weather", tags=["Agricultural Weather Networks"])

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Wisconet REST API (v1 is more complete; v2 has /latest convenience)
_WISCONET_BASE_V1 = "https://api.wisconet.wisc.edu/api/v1"
_WISCONET_BASE_V2 = "https://api.wisconet.wisc.edu/api/v2"

# NEWA — discovered GeoJSON station endpoint (Next.js data route)
_NEWA_STATIONS_URL = "https://newa.cornell.edu/resources/stations.geojson"

_SOURCE = "ag_networks"

# Wisconet 5-min field -> (canonical name, unit)
_WISCONET_FIELD_MAP: dict[str, tuple[str, str]] = {
    "5min_air_temp_f_avg":          ("temperature", "°F"),
    "5min_dew_point_f_avg":         ("dewpoint", "°F"),
    "5min_relative_humidity_avg":   ("relative_humidity", "%"),
    "5min_wind_speed_mph_avg":      ("wind_speed", "mph"),
    "5min_wind_direction_avg":      ("wind_direction", "°"),
    "5min_wind_gust_mph_max":       ("wind_gust", "mph"),
    "5min_precip_in_tot":           ("precipitation", "in"),
    "5min_pressure_inhg_avg":       ("pressure", "inHg"),
    "5min_soil_temp_2in_f_avg":     ("soil_temperature_2in", "°F"),
    "5min_soil_temp_4in_f_avg":     ("soil_temperature_4in", "°F"),
    "5min_soil_moisture_vwc_avg":   ("soil_moisture", "m³/m³"),
    "5min_solar_rad_wm2_avg":       ("solar_radiation", "W/m²"),
}


# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------

def _http_client() -> httpx.AsyncClient:
    return httpx.AsyncClient(
        timeout=config.HTTP_TIMEOUT,
        headers={"User-Agent": config.USER_AGENT},
        follow_redirects=True,
    )


async def _get_json(url: str, params: dict | None = None) -> Any:
    """GET *url* and return parsed JSON; raises HTTPException on failure."""
    async with _http_client() as client:
        try:
            resp = await client.get(url, params=params)
            resp.raise_for_status()
            return resp.json()
        except httpx.HTTPStatusError as exc:
            raise HTTPException(
                status_code=exc.response.status_code,
                detail=f"Upstream API error ({url}): {exc}",
            )
        except httpx.RequestError as exc:
            raise HTTPException(
                status_code=502,
                detail=f"Upstream request failed ({url}): {exc}",
            )


# ---------------------------------------------------------------------------
# Wisconet helpers
# ---------------------------------------------------------------------------

def _parse_wisconet_station(raw: dict) -> dict:
    """Convert a Wisconet station record to our standard shape."""
    sid = raw.get("station_id", "")

    # Extract last_update from any timestamp field present in the record
    last_update = None
    ts_str = (
        raw.get("collection_time") or raw.get("last_update")
        or raw.get("last_observation") or raw.get("timestamp")
    )
    if ts_str:
        try:
            last_update = datetime.fromisoformat(str(ts_str).replace("Z", "+00:00"))
        except (ValueError, TypeError):
            pass

    return Station(
        id=f"wisconet_{sid}",
        name=raw.get("station_name", sid),
        lat=float(raw.get("latitude", 0) or 0),
        lon=float(raw.get("longitude", 0) or 0),
        source=_SOURCE,
        network="wisconet",
        params=[v[0] for v in _WISCONET_FIELD_MAP.values()],
        url=f"https://api.wisconet.wisc.edu/api/v1/stations/{sid}",
        last_update=last_update,
        extra={
            "elevation": raw.get("elevation"),
            "city": raw.get("city"),
            "county": raw.get("county"),
            "region": raw.get("region"),
            "timezone": raw.get("station_timezone"),
            "madis_id": raw.get("madis_id"),
        },
    ).model_dump()


def _parse_wisconet_readings(station_id: str, summary: dict) -> list[dict]:
    """Extract Reading dicts from a Wisconet current-summary record."""
    readings: list[dict] = []
    ts_str = summary.get("collection_time", "")
    try:
        ts = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
    except (ValueError, AttributeError):
        ts = datetime.now(timezone.utc)

    for field_name, (param, unit) in _WISCONET_FIELD_MAP.items():
        val = summary.get(field_name)
        if val is None:
            continue
        try:
            value = round(float(val), 2)
        except (TypeError, ValueError):
            continue
        readings.append(
            Reading(
                station_id=f"wisconet_{station_id}",
                timestamp=ts,
                parameter=param,
                value=value,
                unit=unit,
                source=_SOURCE,
            ).model_dump()
        )
    return readings


# ---------------------------------------------------------------------------
# NEWA helpers
# ---------------------------------------------------------------------------

def _parse_newa_station(feature: dict) -> dict | None:
    """Convert a NEWA GeoJSON Feature to our standard Station shape."""
    props = feature.get("properties", {})
    geom = feature.get("geometry", {})
    coords = geom.get("coordinates", [None, None])
    if coords[0] is None or coords[1] is None:
        return None

    sid = props.get("id") or props.get("station") or props.get("name", "unknown")
    name = props.get("name") or props.get("station_name") or str(sid)
    state = props.get("state", "")
    network = props.get("network", "")

    # Extract last_update from any timestamp field in the GeoJSON properties
    last_update = None
    ts_str = (
        props.get("last_update") or props.get("lastUpdate")
        or props.get("date") or props.get("datetime")
        or props.get("last_observation") or props.get("timestamp")
    )
    if ts_str:
        try:
            last_update = datetime.fromisoformat(str(ts_str).replace("Z", "+00:00"))
        except (ValueError, TypeError):
            pass

    return Station(
        id=f"newa_{sid}",
        name=name,
        lat=float(coords[1]),
        lon=float(coords[0]),
        source=_SOURCE,
        network="newa",
        params=["temperature", "relative_humidity", "precipitation",
                "wind_speed", "wind_direction", "solar_radiation",
                "soil_temperature", "soil_moisture"],
        url=f"https://newa.cornell.edu/weather-station-page?id={sid}",
        last_update=last_update,
        extra={
            "state": state,
            "newa_network": network,
            "elev": props.get("elev"),
        },
    ).model_dump()


# ---------------------------------------------------------------------------
# Endpoints — Wisconet
# ---------------------------------------------------------------------------

@router.get(
    "/wisconet/stations",
    summary="List all Wisconet stations",
    response_description="Station list with location and metadata",
)
async def wisconet_stations():
    """Return all Wisconet (Wisconsin Environmental Mesonet) stations.

    Data sourced from the Wisconet public API v1.
    """
    cache_key = "ag_weather:wisconet:stations"
    cached = await cache.get(cache_key)
    if cached is not None:
        return cached

    data = await _get_json(f"{_WISCONET_BASE_V1}/stations/")

    # API may return a list or a dict with a results key
    if isinstance(data, dict):
        items = data.get("results", data.get("stations", []))
        if isinstance(items, dict):
            items = list(items.values())
    elif isinstance(data, list):
        items = data
    else:
        items = []

    stations = [_parse_wisconet_station(s) for s in items if isinstance(s, dict)]

    result = {
        "source": "wisconet",
        "station_count": len(stations),
        "stations": stations,
    }
    await cache.set(cache_key, result, ttl=config.CACHE_TTL_LONG)
    return result


@router.get(
    "/wisconet/latest",
    summary="Latest data from all Wisconet stations",
    response_description="Most recent 5-min observations from every station",
)
async def wisconet_latest():
    """Return the most recent 5-minute observations from all Wisconet stations.

    Includes temperature, humidity, wind, precipitation, soil, and solar
    radiation where available.
    """
    cache_key = "ag_weather:wisconet:latest"
    cached = await cache.get(cache_key)
    if cached is not None:
        return cached

    data = await _get_json(f"{_WISCONET_BASE_V1}/stations/current_summaries")

    # Normalize response shape
    if isinstance(data, dict):
        items = data.get("results", data.get("summaries", data.get("data", [])))
        if isinstance(items, dict):
            items = list(items.values())
    elif isinstance(data, list):
        items = data
    else:
        items = []

    all_readings: list[dict] = []
    station_count = 0
    for summary in items:
        if not isinstance(summary, dict):
            continue
        sid = summary.get("station_id", "")
        if not sid:
            continue
        readings = _parse_wisconet_readings(sid, summary)
        if readings:
            station_count += 1
            all_readings.extend(readings)

    result = {
        "source": "wisconet",
        "station_count": station_count,
        "reading_count": len(all_readings),
        "readings": all_readings,
    }
    await cache.set(cache_key, result, ttl=config.CACHE_TTL)
    return result


@router.get(
    "/wisconet/station/{station_id}",
    summary="Get details for a single Wisconet station",
    response_description="Station metadata and latest measurements",
)
async def wisconet_station_detail(station_id: str):
    """Return metadata and the latest measurements for one Wisconet station.

    The *station_id* should be the Wisconet numeric/slug ID (e.g. ``1``,
    ``HANE``), **not** the prefixed ``wisconet_`` ID used in listings.
    """
    cache_key = f"ag_weather:wisconet:station:{station_id}"
    cached = await cache.get(cache_key)
    if cached is not None:
        return cached

    # Fetch station metadata and latest measures in parallel-ish fashion
    station_data = await _get_json(
        f"{_WISCONET_BASE_V1}/stations/{station_id}"
    )
    latest_data = await _get_json(
        f"{_WISCONET_BASE_V1}/stations/{station_id}/latest_measures"
    )

    station = _parse_wisconet_station(station_data) if isinstance(station_data, dict) else {}

    # latest_measures may be a list of {field_name, field_value, collection_time}
    readings: list[dict] = []
    if isinstance(latest_data, list):
        for rec in latest_data:
            fname = rec.get("field_name", "")
            mapping = _WISCONET_FIELD_MAP.get(fname)
            if mapping is None:
                continue
            param, unit = mapping
            try:
                value = round(float(rec["field_value"]), 2)
            except (KeyError, TypeError, ValueError):
                continue
            ts_str = rec.get("collection_time", "")
            try:
                ts = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
            except (ValueError, AttributeError):
                ts = datetime.now(timezone.utc)
            readings.append(
                Reading(
                    station_id=f"wisconet_{station_id}",
                    timestamp=ts,
                    parameter=param,
                    value=value,
                    unit=unit,
                    source=_SOURCE,
                ).model_dump()
            )
    elif isinstance(latest_data, dict):
        # May be wrapped in a results key
        readings = _parse_wisconet_readings(station_id, latest_data)

    result = {
        "source": "wisconet",
        "station": station,
        "reading_count": len(readings),
        "readings": readings,
    }
    await cache.set(cache_key, result, ttl=config.CACHE_TTL)
    return result


# ---------------------------------------------------------------------------
# Endpoints — NEWA
# ---------------------------------------------------------------------------

@router.get(
    "/newa/stations",
    summary="List all NEWA farm weather stations",
    response_description="Station list from the Cornell NEWA network",
)
async def newa_stations():
    """Return all NEWA (Cornell) farm weather stations.

    NEWA does not publish a documented public API.  Station metadata is
    fetched from a GeoJSON resource discovered via network inspection.
    If the endpoint changes this will return an error.
    """
    cache_key = "ag_weather:newa:stations"
    cached = await cache.get(cache_key)
    if cached is not None:
        return cached

    # Try the known GeoJSON endpoint
    stations: list[dict] = []
    error_detail: str | None = None

    try:
        data = await _get_json(_NEWA_STATIONS_URL)
    except HTTPException:
        # Fall back — the GeoJSON URL may have moved.  Try alternate paths.
        for alt in [
            "https://newa.cornell.edu/data/stations.geojson",
            "https://newa.cornell.edu/stations.json",
        ]:
            try:
                data = await _get_json(alt)
                break
            except HTTPException:
                continue
        else:
            data = None
            error_detail = (
                "NEWA station endpoint not reachable.  NEWA does not publish a "
                "stable public API; the GeoJSON URL may have changed."
            )

    if data is not None:
        # Standard GeoJSON FeatureCollection
        if isinstance(data, dict) and "features" in data:
            features = data["features"]
        elif isinstance(data, list):
            features = data
        else:
            features = []

        for f in features:
            parsed = _parse_newa_station(f)
            if parsed is not None:
                stations.append(parsed)

    result: dict[str, Any] = {
        "source": "newa",
        "station_count": len(stations),
        "stations": stations,
    }
    if error_detail:
        result["error"] = error_detail

    if stations:
        await cache.set(cache_key, result, ttl=config.CACHE_TTL_LONG)
    return result


# ---------------------------------------------------------------------------
# Endpoints — Overview
# ---------------------------------------------------------------------------

@router.get(
    "/overview",
    summary="Agricultural weather network source info",
    response_description="Metadata for all agricultural weather networks",
)
async def overview():
    """Return metadata about available agricultural weather networks.

    Includes network descriptions, coverage, station counts, and parameter
    lists for Wisconet and NEWA.
    """
    return {
        "source_name": SOURCE_NAME,
        "description": SOURCE_DESCRIPTION,
        "networks": SOURCE_META["networks"],
        "parameters": SOURCE_META["params"],
        "auth_required": False,
        "documentation": {
            "wisconet": "https://api.wisconet.wisc.edu/docs",
            "newa": "https://newa.cornell.edu/",
        },
    }
