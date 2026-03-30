"""Florida FAWN (Florida Automated Weather Network) — UF/IFAS.

42 agricultural weather stations across Florida providing 15-minute
observations of temperature, humidity, wind, solar radiation, rainfall,
soil temperature, leaf wetness, and evapotranspiration.

Operated by the University of Florida Institute of Food and Agricultural
Sciences (IFAS).  All data is free, no authentication required.

API reference:  https://fawn.ifas.ufl.edu/data/feeds.php
Station list:   https://fawn.ifas.ufl.edu/controller.php/today/listAllStations;json
Current obs:    https://fawn.ifas.ufl.edu/controller.php/today/obs/{station_id};json
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Optional

import httpx
from fastapi import APIRouter, HTTPException, Path, Query

import cache
import config
from models import Reading, Station, StationDetail

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Source registry metadata  (read by server.py /api/sources)
# ---------------------------------------------------------------------------

SOURCE_NAME = "Florida FAWN"
SOURCE_DESCRIPTION = (
    "Florida Automated Weather Network (FAWN) — 42 agricultural weather "
    "stations operated by UF/IFAS across Florida.  15-minute observations "
    "of temperature, humidity, wind, solar radiation, rainfall, soil "
    "temperature, leaf wetness, and evapotranspiration.  No auth required."
)
SOURCE_META = {
    "url": "https://fawn.ifas.ufl.edu/",
    "auth_required": False,
    "update_frequency": "15 min",
    "coverage": "Florida, USA",
    "params": [
        "temperature", "relative_humidity", "wind_speed", "wind_direction",
        "wind_gust", "solar_radiation", "rainfall", "soil_temperature",
        "leaf_wetness", "evapotranspiration",
    ],
    "category": "weather",
    "station_count": 42,
}

# ---------------------------------------------------------------------------
# Router
# ---------------------------------------------------------------------------

router = APIRouter(prefix="/api/fawn", tags=["Florida FAWN"])

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_FAWN_BASE = "https://fawn.ifas.ufl.edu/controller.php/today"
_SOURCE = "fawn"


# ---------------------------------------------------------------------------
# HTTP helper
# ---------------------------------------------------------------------------

_HEADERS = {
    "User-Agent": config.USER_AGENT,
    "Accept": "application/json",
}


def _client() -> httpx.AsyncClient:
    return httpx.AsyncClient(
        timeout=config.HTTP_TIMEOUT,
        headers=_HEADERS,
        follow_redirects=True,
    )


async def _fawn_get(path: str) -> Any:
    """GET a FAWN API path and return parsed JSON."""
    url = f"{_FAWN_BASE}/{path}"
    try:
        async with _client() as c:
            resp = await c.get(url)
            resp.raise_for_status()
            return resp.json()
    except httpx.TimeoutException:
        raise HTTPException(status_code=504, detail="FAWN API timeout")
    except httpx.HTTPStatusError as exc:
        raise HTTPException(
            status_code=exc.response.status_code,
            detail=f"FAWN API error: {exc}",
        )
    except Exception as exc:
        logger.exception("Error querying FAWN API")
        raise HTTPException(status_code=502, detail=str(exc))


# ---------------------------------------------------------------------------
# Parsing helpers
# ---------------------------------------------------------------------------

def _safe_float(val: Any, default: Optional[float] = None) -> Optional[float]:
    """Convert a value to float, returning *default* on failure or None/empty."""
    if val is None or val == "" or val == "N/A":
        return default
    try:
        return float(val)
    except (TypeError, ValueError):
        return default


def _parse_station(raw: dict) -> Station:
    """Convert a FAWN station record to the shared Station model."""
    station_id = str(raw.get("StationID", raw.get("stationId", raw.get("id", ""))))
    name = raw.get("StationName", raw.get("stationName", raw.get("name", station_id)))
    lat = _safe_float(raw.get("Latitude", raw.get("latitude", raw.get("lat"))), 0.0)
    lon = _safe_float(raw.get("Longitude", raw.get("longitude", raw.get("lon"))), 0.0)
    county = raw.get("County", raw.get("county", ""))

    # Extract last_update from any timestamp field the API might include
    last_update = None
    ts_raw = (
        raw.get("LastUpdate") or raw.get("lastUpdate") or raw.get("last_update")
        or raw.get("reportDateTime") or raw.get("datetime")
        or raw.get("Timestamp") or raw.get("timestamp")
        or raw.get("LastDataReceived") or raw.get("lastDataReceived")
    )
    if ts_raw:
        last_update = _parse_fawn_ts(str(ts_raw))

    return Station(
        id=f"fawn_{station_id}",
        name=name or f"FAWN Station {station_id}",
        lat=lat or 0.0,
        lon=lon or 0.0,
        source=_SOURCE,
        network="fawn",
        params=SOURCE_META["params"],
        url=f"https://fawn.ifas.ufl.edu/station/{station_id}/",
        last_update=last_update,
        extra={
            "fawn_id": station_id,
            "county": county,
        },
    )


def _parse_obs(raw: dict, station_id: str) -> list[Reading]:
    """Convert a FAWN observation record into a list of Reading objects.

    FAWN returns one JSON object per observation time with keys like
    t2m, rh, ws, wd, rain, sr, etc.  We map them to standard parameter
    names.
    """
    readings: list[Reading] = []

    # Parse timestamp — FAWN uses various keys / formats
    ts_raw = (
        raw.get("reportDateTime")
        or raw.get("datetime")
        or raw.get("date")
        or raw.get("Timestamp")
        or ""
    )
    ts = _parse_fawn_ts(ts_raw)
    if ts is None:
        return readings

    # Parameter mapping: FAWN field -> (parameter name, unit)
    field_map: dict[str, tuple[str, str]] = {
        # Air temperature (various possible keys)
        "t2m":              ("temperature",        "°F"),
        "T2M":              ("temperature",        "°F"),
        "AirTemp1":         ("temperature",        "°F"),
        "airTemp":          ("temperature",        "°F"),
        # Humidity
        "rh":               ("relative_humidity",  "%"),
        "RH":               ("relative_humidity",  "%"),
        "RelHum1":          ("relative_humidity",  "%"),
        "relHumidity":      ("relative_humidity",  "%"),
        # Wind speed
        "ws":               ("wind_speed",         "mph"),
        "WS":               ("wind_speed",         "mph"),
        "WindSpeed":        ("wind_speed",         "mph"),
        "windSpeed":        ("wind_speed",         "mph"),
        # Wind direction
        "wd":               ("wind_direction",     "°"),
        "WD":               ("wind_direction",     "°"),
        "WindDir":          ("wind_direction",      "°"),
        "windDirection":    ("wind_direction",      "°"),
        # Wind gust
        "wg":               ("wind_gust",          "mph"),
        "WindGust":         ("wind_gust",          "mph"),
        "windGust":         ("wind_gust",          "mph"),
        # Solar radiation
        "sr":               ("solar_radiation",    "W/m²"),
        "SR":               ("solar_radiation",    "W/m²"),
        "TotRad":           ("solar_radiation",    "W/m²"),
        "solarRadiation":   ("solar_radiation",    "W/m²"),
        # Rainfall
        "rain":             ("rainfall",           "in"),
        "Rain":             ("rainfall",           "in"),
        "Rainfall":         ("rainfall",           "in"),
        "rainfall":         ("rainfall",           "in"),
        # Soil temperature
        "ts":               ("soil_temperature",   "°F"),
        "TS":               ("soil_temperature",   "°F"),
        "SoilTemp":         ("soil_temperature",   "°F"),
        "soilTemp":         ("soil_temperature",   "°F"),
        # Leaf wetness
        "lw":               ("leaf_wetness",       "min"),
        "LW":               ("leaf_wetness",       "min"),
        "LeafWet":          ("leaf_wetness",       "min"),
        "leafWetness":      ("leaf_wetness",       "min"),
        # Evapotranspiration
        "et":               ("evapotranspiration", "in"),
        "ET":               ("evapotranspiration", "in"),
        "ETo":              ("evapotranspiration",  "in"),
        "evapotranspiration": ("evapotranspiration", "in"),
        # Dewpoint
        "dp":               ("dewpoint",           "°F"),
        "DewPoint":         ("dewpoint",           "°F"),
        "dewPoint":         ("dewpoint",           "°F"),
    }

    for field, (param, unit) in field_map.items():
        val = _safe_float(raw.get(field))
        if val is not None:
            readings.append(Reading(
                station_id=f"fawn_{station_id}",
                timestamp=ts,
                parameter=param,
                value=round(val, 2),
                unit=unit,
                source=_SOURCE,
            ))

    return readings


def _parse_fawn_ts(raw: str) -> Optional[datetime]:
    """Parse a FAWN timestamp string into an aware UTC datetime."""
    if not raw:
        return None
    formats = [
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M:%SZ",
        "%Y-%m-%d %H:%M",
        "%m/%d/%Y %H:%M",
        "%m/%d/%Y %H:%M:%S",
    ]
    for fmt in formats:
        try:
            dt = datetime.strptime(raw.strip(), fmt)
            # FAWN times are US/Eastern; treat as approximate UTC for now
            return dt.replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    # Fallback: try fromisoformat
    try:
        dt = datetime.fromisoformat(raw.strip().rstrip("Z"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except ValueError:
        return None


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@router.get("/stations", summary="FAWN — all stations")
async def stations():
    """Return all 42 Florida FAWN stations with metadata."""
    cache_key = "fawn:stations"
    cached_val = await cache.get(cache_key)
    if cached_val is not None:
        return cached_val

    data = await _fawn_get("listAllStations;json")

    # data may be a list of station dicts or wrapped in a key
    raw_list = data if isinstance(data, list) else data.get("stations", data.get("data", []))

    station_list = [_parse_station(s).model_dump() for s in raw_list]
    result = {
        "source": _SOURCE,
        "count": len(station_list),
        "stations": station_list,
    }
    await cache.set(cache_key, result, ttl=config.CACHE_TTL_LONG)
    return result


@router.get("/current/{station_id}", summary="FAWN — current obs for one station")
async def current_obs(
    station_id: str = Path(..., description="FAWN station ID (numeric)"),
):
    """Return the latest observations for a single FAWN station."""
    cache_key = f"fawn:current:{station_id}"
    cached_val = await cache.get(cache_key)
    if cached_val is not None:
        return cached_val

    data = await _fawn_get(f"obs/{station_id};json")

    # Normalize response — may be a dict with observation data or a list
    if isinstance(data, list):
        obs_list = data
    elif isinstance(data, dict):
        obs_list = data.get("observations", data.get("data", [data]))
    else:
        obs_list = []

    readings: list[dict] = []
    for obs in obs_list:
        parsed = _parse_obs(obs, station_id)
        readings.extend(r.model_dump() for r in parsed)

    result = {
        "source": _SOURCE,
        "station_id": f"fawn_{station_id}",
        "count": len(readings),
        "readings": readings,
    }
    await cache.set(cache_key, result, ttl=config.CACHE_TTL)
    return result


@router.get("/current", summary="FAWN — current obs for all stations")
async def current_all():
    """Return the latest observations for all FAWN stations.

    Fetches the station list, then retrieves current obs for each station.
    Results are cached for 5 minutes.
    """
    cache_key = "fawn:current:all"
    cached_val = await cache.get(cache_key)
    if cached_val is not None:
        return cached_val

    # Get station list first
    station_data = await _fawn_get("listAllStations;json")
    raw_list = (
        station_data if isinstance(station_data, list)
        else station_data.get("stations", station_data.get("data", []))
    )

    all_readings: list[dict] = []
    station_count = 0

    for raw_station in raw_list:
        sid = str(
            raw_station.get("StationID")
            or raw_station.get("stationId")
            or raw_station.get("id", "")
        )
        if not sid:
            continue
        try:
            data = await _fawn_get(f"obs/{sid};json")
        except HTTPException:
            logger.warning("Failed to fetch obs for FAWN station %s", sid)
            continue

        if isinstance(data, list):
            obs_list = data
        elif isinstance(data, dict):
            obs_list = data.get("observations", data.get("data", [data]))
        else:
            obs_list = []

        for obs in obs_list:
            parsed = _parse_obs(obs, sid)
            all_readings.extend(r.model_dump() for r in parsed)
        station_count += 1

    result = {
        "source": _SOURCE,
        "station_count": station_count,
        "reading_count": len(all_readings),
        "readings": all_readings,
    }
    await cache.set(cache_key, result, ttl=config.CACHE_TTL)
    return result


@router.get("/overview", summary="Florida FAWN — source overview")
async def overview():
    """Return summary information about the Florida FAWN data source."""
    return {
        "module": SOURCE_NAME,
        "description": SOURCE_DESCRIPTION,
        "sources": [
            {
                "id": _SOURCE,
                "name": "Florida FAWN",
                "description": (
                    "Florida Automated Weather Network — 42 agricultural weather "
                    "stations operated by UF/IFAS providing 15-minute observations "
                    "of temperature, humidity, wind, solar radiation, rainfall, "
                    "soil temperature, leaf wetness, and evapotranspiration."
                ),
                "url": "https://fawn.ifas.ufl.edu/",
                "auth_required": False,
                "update_frequency": "15 min",
                "coverage": "Florida, USA",
                "station_count": 42,
                "params": SOURCE_META["params"],
                "endpoints": [
                    "GET /api/fawn/stations",
                    "GET /api/fawn/current/{station_id}",
                    "GET /api/fawn/current",
                    "GET /api/fawn/overview",
                ],
            }
        ],
    }
