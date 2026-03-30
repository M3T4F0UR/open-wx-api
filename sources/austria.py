"""GeoSphere Austria (formerly ZAMG) TAWES + climate station network.

GeoSphere Austria operates ~270 TAWES (Teilautomatische Wetterstationen)
stations providing 10-minute real-time observations, plus ~700 climate stations
with daily historical data.  All endpoints are public (no auth), JSON, CC-BY-4.0.

API hub: https://dataset.api.hub.geosphere.at/
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
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

SOURCE_NAME = "GeoSphere Austria"
SOURCE_DESCRIPTION = (
    "GeoSphere Austria (formerly ZAMG) operates ~270 TAWES automatic weather "
    "stations with 10-minute updates and ~700 climate stations with daily "
    "observations.  All data are public, no authentication required, licensed "
    "CC-BY-4.0."
)
SOURCE_META = {
    "url": "https://www.geosphere.at/",
    "auth_required": False,
    "auth_note": "No authentication required",
    "update_frequency": "10 min (TAWES current), daily (climate historical)",
    "coverage": "Austria",
    "params": [
        "temperature", "precipitation", "humidity",
        "wind_direction", "wind_speed", "pressure", "sunshine",
    ],
    "category": "weather",
    "station_count": 970,
    "license": "CC-BY-4.0",
}

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

GEOSPHERE_BASE = "https://dataset.api.hub.geosphere.at/v1"

# GeoSphere parameter code -> (human name, unit)
PARAM_MAP: dict[str, tuple[str, str]] = {
    "TL":   ("Air Temperature",      "degC"),
    "TL0":  ("Ground Temperature",   "degC"),
    "TS10": ("Soil Temp 10 cm",      "degC"),
    "TS20": ("Soil Temp 20 cm",      "degC"),
    "TS50": ("Soil Temp 50 cm",      "degC"),
    "RR":   ("Precipitation",        "mm"),
    "RF":   ("Relative Humidity",    "%"),
    "DD":   ("Wind Direction",       "deg"),
    "DDMIT":"Wind Dir (mean)",       # sometimes returned as tuple, but we normalise below
    "FF":   ("Wind Speed",           "m/s"),
    "FFX":  ("Wind Gust",            "m/s"),
    "P":    ("Pressure",             "hPa"),
    "P0":   ("Station Pressure",     "hPa"),
    "SO":   ("Sunshine Duration",    "min"),
    "GSX":  ("Global Radiation",     "W/m2"),
    "SCHNEE": ("Snow Depth",         "cm"),
    "TP":   ("Dew Point",            "degC"),
}
# Fix DDMIT which was mis-typed above (tuple not assigned properly)
PARAM_MAP["DDMIT"] = ("Wind Direction (mean)", "deg")

ALL_CURRENT_PARAMS = "TL,RR,RF,DD,FF,FFX,P,SO"

_HEADERS = {
    "User-Agent": config.USER_AGENT,
    "Accept": "application/json",
}


# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------

def _make_client() -> httpx.AsyncClient:
    return httpx.AsyncClient(
        headers=_HEADERS,
        timeout=config.HTTP_TIMEOUT,
        follow_redirects=True,
    )


async def _geosphere_get(url: str, params: dict[str, Any] | None = None) -> Any:
    """GET a GeoSphere endpoint; raise HTTPException on error."""
    try:
        async with _make_client() as client:
            resp = await client.get(url, params=params)
            resp.raise_for_status()
            return resp.json()
    except httpx.TimeoutException:
        raise HTTPException(status_code=504, detail=f"GeoSphere API timeout ({url})")
    except httpx.HTTPStatusError as exc:
        status = exc.response.status_code
        try:
            body = exc.response.text[:500]
        except Exception:
            body = str(exc)
        raise HTTPException(status_code=status, detail=f"GeoSphere API error: {body}")
    except Exception as exc:
        logger.exception("Unexpected error calling GeoSphere API: %s", url)
        raise HTTPException(status_code=502, detail=str(exc))


# ---------------------------------------------------------------------------
# Data normalisation helpers
# ---------------------------------------------------------------------------

def _parse_metadata_station(item: dict) -> Station:
    """Convert a GeoSphere metadata station entry to the shared Station model."""
    station_id = str(item.get("id", ""))
    name = item.get("name", station_id)
    lat = float(item.get("lat", 0.0) or 0.0)
    lon = float(item.get("lon", 0.0) or 0.0)
    elevation = None
    alt = item.get("altitude")
    if alt is not None:
        try:
            elevation = float(alt)
        except (TypeError, ValueError):
            pass

    # Parameter list from the metadata 'parameters' block if present
    raw_params = item.get("parameters", [])
    params: list[str] = []
    if isinstance(raw_params, list):
        for p in raw_params:
            code = p.get("name", "") if isinstance(p, dict) else str(p)
            human, _ = PARAM_MAP.get(code.upper(), (code, ""))
            if human and human not in params:
                params.append(human)

    state = item.get("state", "")
    valid_from = item.get("valid_from", "")
    valid_to = item.get("valid_to", "")

    return Station(
        id=station_id,
        name=name,
        lat=lat,
        lon=lon,
        source="austria_geosphere",
        network="TAWES",
        params=params,
        elevation=elevation,
        url=f"https://www.geosphere.at/",
        extra={
            "state": state,
            "valid_from": valid_from,
            "valid_to": valid_to,
        },
    )


def _parse_current_readings(
    station_id: str,
    timestamps: list[str],
    param_data: dict[str, list],
) -> list[Reading]:
    """Parse the GeoSphere current-data response into Reading objects.

    The response has a 'timestamps' array and per-parameter arrays of values
    aligned by index.
    """
    readings: list[Reading] = []
    for param_code, values in param_data.items():
        if not isinstance(values, list):
            continue
        human, unit = PARAM_MAP.get(param_code.upper(), (param_code, ""))
        for i, val in enumerate(values):
            if val is None:
                continue
            try:
                numeric = float(val)
            except (TypeError, ValueError):
                continue
            if i < len(timestamps):
                try:
                    ts = datetime.fromisoformat(timestamps[i].replace("Z", "+00:00"))
                except (ValueError, AttributeError):
                    continue
            else:
                ts = datetime.now(tz=timezone.utc)
            readings.append(
                Reading(
                    station_id=station_id,
                    timestamp=ts,
                    parameter=human,
                    value=numeric,
                    unit=unit,
                    source="austria_geosphere",
                )
            )
    return readings


def _latest_per_param(readings: list[Reading]) -> list[Reading]:
    """Return only the most-recent reading per (station_id, parameter)."""
    best: dict[tuple[str, str], Reading] = {}
    for r in readings:
        key = (r.station_id, r.parameter)
        if key not in best or r.timestamp > best[key].timestamp:
            best[key] = r
    return list(best.values())


def _extract_station_readings(data: dict) -> dict[str, dict]:
    """Extract per-station timestamps and parameter arrays from the GeoSphere
    current-data response format.

    The response structure is:
      { "timestamps": [...],
        "features": [ { "properties": { "parameters": { "TL": {"data": [...]}, ... },
                                         "station": "11035" } }, ... ] }

    Returns {station_id: {"timestamps": [...], "params": {code: [values]}}}
    """
    timestamps = data.get("timestamps", [])
    result: dict[str, dict] = {}

    features = data.get("features", [])
    for feat in features:
        props = feat.get("properties", {})
        station_id = str(props.get("station", ""))
        if not station_id:
            # Some responses embed station id in the geometry or at feature level
            station_id = str(feat.get("id", ""))
        if not station_id:
            continue

        params_block = props.get("parameters", {})
        param_arrays: dict[str, list] = {}
        for code, pobj in params_block.items():
            if isinstance(pobj, dict):
                param_arrays[code] = pobj.get("data", [])
            elif isinstance(pobj, list):
                param_arrays[code] = pobj

        result[station_id] = {
            "timestamps": timestamps,
            "params": param_arrays,
        }

    return result


# ---------------------------------------------------------------------------
# Router
# ---------------------------------------------------------------------------

router = APIRouter(prefix="/api/austria", tags=["GeoSphere Austria"])


# ---------------------------------------------------------------------------
# Endpoint: GET /api/austria/stations
# ---------------------------------------------------------------------------

@router.get(
    "/stations",
    summary="List all TAWES stations",
)
async def list_stations():
    """Return metadata for all ~270 TAWES automatic weather stations in Austria.

    Data is sourced from the GeoSphere Austria dataset hub metadata endpoint.
    Results are cached for 1 hour since the station list rarely changes.
    """
    cache_key = "austria:stations"
    cached = await cache.get(cache_key)
    if cached is not None:
        return cached

    url = f"{GEOSPHERE_BASE}/station/current/tawes-v1-10min/metadata"
    data = await _geosphere_get(url)

    raw_stations = data.get("stations", []) if isinstance(data, dict) else []
    # If the metadata response is a list at top level
    if isinstance(data, list):
        raw_stations = data

    stations = [_parse_metadata_station(s) for s in raw_stations if isinstance(s, dict)]

    result = {
        "source": SOURCE_NAME,
        "count": len(stations),
        "stations": [s.model_dump() for s in stations],
    }
    await cache.set(cache_key, result, ttl=config.CACHE_TTL_LONG)
    return result


# ---------------------------------------------------------------------------
# Endpoint: GET /api/austria/current
# ---------------------------------------------------------------------------

@router.get(
    "/current",
    summary="Current 10-min observations from all TAWES stations",
)
async def current_observations(
    parameters: str = Query(
        ALL_CURRENT_PARAMS,
        description=(
            "Comma-separated GeoSphere parameter codes: "
            "TL (temp), RR (precip), RF (humidity), DD (wind dir), "
            "FF (wind speed), FFX (gust), P (pressure), SO (sunshine)"
        ),
    ),
):
    """Return the latest 10-minute observations from all TAWES stations.

    The response includes one entry per station with current readings for
    the requested parameters.  Cached for 5 minutes.
    """
    cache_key = f"austria:current:{parameters}"
    cached = await cache.get(cache_key)
    if cached is not None:
        return cached

    url = f"{GEOSPHERE_BASE}/station/current/tawes-v1-10min"
    params = {"parameters": parameters}
    data = await _geosphere_get(url, params=params)

    station_data = _extract_station_readings(data)

    stations_out: list[dict[str, Any]] = []
    for sid, sdata in station_data.items():
        readings = _parse_current_readings(sid, sdata["timestamps"], sdata["params"])
        latest = _latest_per_param(readings)
        last_update = max((r.timestamp for r in latest), default=None) if latest else None
        stations_out.append({
            "station_id": sid,
            "last_update": last_update.isoformat() if last_update else None,
            "readings": [r.model_dump() for r in latest],
        })

    result = {
        "source": SOURCE_NAME,
        "parameters": parameters,
        "count": len(stations_out),
        "stations": stations_out,
    }
    await cache.set(cache_key, result, ttl=config.CACHE_TTL)
    return result


# ---------------------------------------------------------------------------
# Endpoint: GET /api/austria/stations/{station_id}
# ---------------------------------------------------------------------------

@router.get(
    "/stations/{station_id}",
    summary="Single station metadata + latest readings",
)
async def get_station(station_id: str):
    """Return metadata and the most recent 10-minute observations for one
    TAWES station.

    `station_id` is the GeoSphere numeric station ID (e.g. `11035` for Wien).
    """
    cache_key = f"austria:station:{station_id}"
    cached = await cache.get(cache_key)
    if cached is not None:
        return cached

    # Fetch metadata
    meta_url = f"{GEOSPHERE_BASE}/station/current/tawes-v1-10min/metadata"
    meta_data = await _geosphere_get(meta_url)

    raw_stations = meta_data.get("stations", []) if isinstance(meta_data, dict) else []
    if isinstance(meta_data, list):
        raw_stations = meta_data

    station_meta = None
    for s in raw_stations:
        if not isinstance(s, dict):
            continue
        if str(s.get("id", "")) == station_id:
            station_meta = s
            break

    if station_meta is None:
        raise HTTPException(
            status_code=404,
            detail=f"GeoSphere station '{station_id}' not found in TAWES metadata",
        )

    station = _parse_metadata_station(station_meta)

    # Fetch current data for this station
    data_url = f"{GEOSPHERE_BASE}/station/current/tawes-v1-10min"
    data = await _geosphere_get(data_url, params={
        "parameters": ALL_CURRENT_PARAMS,
        "station_ids": station_id,
    })

    station_data = _extract_station_readings(data)
    readings: list[Reading] = []
    sdata = station_data.get(station_id)
    if sdata:
        readings = _latest_per_param(
            _parse_current_readings(station_id, sdata["timestamps"], sdata["params"])
        )
        readings.sort(key=lambda r: r.parameter)

    if readings:
        station.last_update = max(r.timestamp for r in readings)

    detail = StationDetail(station=station, readings=readings)
    result = detail.model_dump()
    await cache.set(cache_key, result, ttl=config.CACHE_TTL)
    return result


# ---------------------------------------------------------------------------
# Endpoint: GET /api/austria/stations/{station_id}/readings
# ---------------------------------------------------------------------------

@router.get(
    "/stations/{station_id}/readings",
    summary="Historical time series for a station",
)
async def station_readings(
    station_id: str,
    days: int = Query(7, ge=1, le=365, description="Number of days of history (max 365)"),
    parameters: str = Query(
        "tl",
        description=(
            "Comma-separated parameter codes for the historical klima-v2-1d dataset "
            "(lowercase): tl (temp), rr (precip), rf (humidity), dd (wind dir), "
            "ff (wind speed), p (pressure), schnee (snow depth)"
        ),
    ),
):
    """Return a daily historical time series for the specified station.

    Uses the GeoSphere klima-v2-1d (daily climate) dataset.  Maximum window
    is 365 days.  Parameter codes should be lowercase for this endpoint
    (e.g. `tl`, `rr`).
    """
    cache_key = f"austria:readings:{station_id}:{days}:{parameters}"
    cached = await cache.get(cache_key)
    if cached is not None:
        return cached

    end = datetime.now(tz=timezone.utc).date()
    start = end - timedelta(days=days)

    url = f"{GEOSPHERE_BASE}/station/historical/klima-v2-1d"
    data = await _geosphere_get(url, params={
        "parameters": parameters,
        "start": start.isoformat(),
        "end": end.isoformat(),
        "station_ids": station_id,
    })

    station_data = _extract_station_readings(data)
    sdata = station_data.get(station_id)
    if not sdata:
        raise HTTPException(
            status_code=404,
            detail=(
                f"No historical data found for station '{station_id}' "
                f"in the last {days} days"
            ),
        )

    readings = _parse_current_readings(station_id, sdata["timestamps"], sdata["params"])
    readings.sort(key=lambda r: r.timestamp)

    result = {
        "station_id": station_id,
        "days": days,
        "parameters": parameters,
        "start": start.isoformat(),
        "end": end.isoformat(),
        "count": len(readings),
        "readings": [r.model_dump() for r in readings],
    }
    await cache.set(cache_key, result, ttl=config.CACHE_TTL)
    return result


# ---------------------------------------------------------------------------
# Endpoint: GET /api/austria/info
# ---------------------------------------------------------------------------

@router.get(
    "/info",
    summary="Source information",
)
async def austria_info():
    """Return information about the GeoSphere Austria data source."""
    return {
        "source": SOURCE_NAME,
        "description": SOURCE_DESCRIPTION,
        "auth_required": False,
        "api_base": GEOSPHERE_BASE,
        "docs": "https://dataset.api.hub.geosphere.at/",
        **SOURCE_META,
    }
