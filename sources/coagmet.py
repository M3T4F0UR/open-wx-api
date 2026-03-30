"""CoAgMET — Colorado Agricultural Meteorological Network.

90+ stations across Colorado reporting temperature, humidity, dewpoint, solar
radiation, precipitation, wind, and soil temperature.  Free REST API, no
authentication required.  Data available as JSON or CSV at hourly and daily
resolution.

API docs: https://coagmet.colostate.edu/data/doc.html
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Optional

import httpx
from fastapi import APIRouter, HTTPException, Query

import cache
import config
from models import Reading, Station

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Source registry metadata  (read by server.py /api/sources)
# ---------------------------------------------------------------------------

SOURCE_NAME = "CoAgMET Colorado"
SOURCE_DESCRIPTION = (
    "Colorado Agricultural Meteorological Network (CoAgMET).  90+ stations "
    "across Colorado reporting temperature, humidity, dewpoint, solar "
    "radiation, precipitation, wind speed/direction, and soil temperature.  "
    "Operated by Colorado State University.  Free, no authentication required."
)
SOURCE_META = {
    "url": "https://coagmet.colostate.edu/",
    "api_docs": "https://coagmet.colostate.edu/data/doc.html",
    "auth_required": False,
    "update_frequency": "hourly",
    "coverage": "Colorado, USA (~90+ stations)",
    "params": [
        "temperature", "relative_humidity", "dewpoint", "solar_radiation",
        "precipitation", "wind_speed", "wind_direction", "soil_temperature",
    ],
    "category": "weather",
}

# ---------------------------------------------------------------------------
# Router
# ---------------------------------------------------------------------------

router = APIRouter(prefix="/api/coagmet", tags=["CoAgMET Colorado"])

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_BASE = "https://coagmet.colostate.edu/data"

# CoAgMET field name -> (normalised param, unit)
_FIELD_MAP: dict[str, tuple[str, str]] = {
    "t":          ("temperature",       "°C"),
    "rh":         ("relative_humidity", "%"),
    "dewpt":      ("dewpoint",          "°C"),
    "solarRad":   ("solar_radiation",   "W/m²"),
    "precip":     ("precipitation",     "mm"),
    "wind_speed": ("wind_speed",        "m/s"),
    "wind_dir":   ("wind_direction",    "°"),
    "soil_t_5cm": ("soil_temp_5cm",     "°C"),
    "t_max":      ("temperature_max",   "°C"),
    "t_min":      ("temperature_min",   "°C"),
    "wind_run":   ("wind_run",          "km"),
}

# Default hourly fields when caller does not specify
_DEFAULT_HOURLY_FIELDS = "t,rh,dewpt,solarRad,precip,wind_speed,wind_dir"

# Default daily fields
_DEFAULT_DAILY_FIELDS = "t_max,t_min,precip,wind_run"


# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------

def _client() -> httpx.AsyncClient:
    return httpx.AsyncClient(
        timeout=config.HTTP_TIMEOUT,
        headers={"User-Agent": config.USER_AGENT},
        follow_redirects=True,
    )


async def _get_json(url: str, params: dict[str, str] | None = None) -> Any:
    """GET a CoAgMET JSON endpoint; raise on HTTP error."""
    async with _client() as client:
        resp = await client.get(url, params=params)
        if resp.status_code != 200:
            logger.warning("CoAgMET %s returned %s", url, resp.status_code)
            raise HTTPException(
                status_code=502,
                detail=f"CoAgMET API returned HTTP {resp.status_code}",
            )
        return resp.json()


# ---------------------------------------------------------------------------
# Conversion helpers
# ---------------------------------------------------------------------------

def _parse_timestamp(raw: Any) -> Optional[str]:
    """Best-effort ISO timestamp from CoAgMET date fields."""
    if not raw:
        return None
    if isinstance(raw, str):
        # CoAgMET often returns ISO-ish strings already
        return raw.replace(" ", "T")
    return str(raw)


def _row_to_readings(row: dict, source: str = "coagmet") -> list[Reading]:
    """Convert a single CoAgMET row into a list of Reading objects."""
    readings: list[Reading] = []
    ts = _parse_timestamp(row.get("date_and_time") or row.get("date"))
    station_id = str(row.get("station", ""))

    for api_field, (param_name, unit) in _FIELD_MAP.items():
        val = row.get(api_field)
        if val is None:
            continue
        try:
            fval = float(val)
        except (ValueError, TypeError):
            continue
        readings.append(Reading(
            station_id=station_id,
            timestamp=ts,
            parameter=param_name,
            value=fval,
            unit=unit,
            source=source,
        ))
    return readings


def _row_to_station(row: dict) -> Station:
    """Build a Station from a CoAgMET latest-data row."""
    lat = row.get("latitude") or row.get("lat")
    lon = row.get("longitude") or row.get("lon")

    # Extract last_update from observation timestamp
    last_update = None
    ts_str = _parse_timestamp(row.get("date_and_time") or row.get("date"))
    if ts_str:
        try:
            last_update = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
            if last_update.tzinfo is None:
                last_update = last_update.replace(tzinfo=timezone.utc)
        except (ValueError, TypeError):
            pass

    return Station(
        id=str(row.get("station", "")),
        name=row.get("station_name", row.get("name", "")),
        lat=float(lat) if lat is not None else 0.0,
        lon=float(lon) if lon is not None else 0.0,
        source="coagmet",
        last_update=last_update,
    )


# ---------------------------------------------------------------------------
# 1. GET /api/coagmet/latest — latest data from all stations
# ---------------------------------------------------------------------------

@router.get("/latest")
async def coagmet_latest():
    """Return the most recent observation from every CoAgMET station."""
    cache_key = "coagmet:latest"
    hit = await cache.get(cache_key)
    if hit is not None:
        return hit

    data = await _get_json(f"{_BASE}/latest.json")
    if not isinstance(data, list):
        data = [data] if data else []

    stations: list[dict] = []
    for row in data:
        st = _row_to_station(row)
        readings = _row_to_readings(row)
        stations.append({
            "station": st.model_dump(exclude_none=True),
            "readings": [r.model_dump(exclude_none=True) for r in readings],
        })

    result = {
        "source": SOURCE_NAME,
        "station_count": len(stations),
        "stations": stations,
    }
    await cache.set(cache_key, result, ttl=config.CACHE_TTL)
    return result


# ---------------------------------------------------------------------------
# 2. GET /api/coagmet/hourly — hourly time series
# ---------------------------------------------------------------------------

@router.get("/hourly")
async def coagmet_hourly(
    stations: str = Query(..., description="Comma-separated station IDs (e.g. ctr01,drg01)"),
    start: str = Query(..., description="Start datetime ISO (e.g. 2026-03-18T00:00)"),
    end: str = Query(..., description="End datetime ISO (e.g. 2026-03-19T00:00)"),
    fields: Optional[str] = Query(None, description="Comma-separated fields (e.g. t,rh,precip)"),
):
    """Fetch hourly observations for one or more stations over a time range."""
    field_str = fields or _DEFAULT_HOURLY_FIELDS
    cache_key = f"coagmet:hourly:{stations}:{start}:{end}:{field_str}"
    hit = await cache.get(cache_key)
    if hit is not None:
        return hit

    params = {
        "stations": stations,
        "from": start,
        "to": end,
        "fields": field_str,
    }
    data = await _get_json(f"{_BASE}/hourly.json", params=params)
    if not isinstance(data, list):
        data = [data] if data else []

    readings: list[dict] = []
    for row in data:
        for r in _row_to_readings(row):
            readings.append(r.model_dump(exclude_none=True))

    result = {
        "source": SOURCE_NAME,
        "stations_requested": stations,
        "period": {"start": start, "end": end},
        "fields": field_str,
        "reading_count": len(readings),
        "readings": readings,
    }
    await cache.set(cache_key, result, ttl=config.CACHE_TTL)
    return result


# ---------------------------------------------------------------------------
# 3. GET /api/coagmet/daily — daily time series
# ---------------------------------------------------------------------------

@router.get("/daily")
async def coagmet_daily(
    stations: str = Query(..., description="Comma-separated station IDs (e.g. ctr01)"),
    start: str = Query(..., description="Start date (e.g. 2026-03-01)"),
    end: str = Query(..., description="End date (e.g. 2026-03-19)"),
    fields: Optional[str] = Query(None, description="Comma-separated fields (e.g. t_max,t_min,precip)"),
):
    """Fetch daily observations for one or more stations over a date range."""
    field_str = fields or _DEFAULT_DAILY_FIELDS
    cache_key = f"coagmet:daily:{stations}:{start}:{end}:{field_str}"
    hit = await cache.get(cache_key)
    if hit is not None:
        return hit

    params = {
        "stations": stations,
        "from": start,
        "to": end,
        "fields": field_str,
    }
    data = await _get_json(f"{_BASE}/daily.json", params=params)
    if not isinstance(data, list):
        data = [data] if data else []

    readings: list[dict] = []
    for row in data:
        for r in _row_to_readings(row):
            readings.append(r.model_dump(exclude_none=True))

    result = {
        "source": SOURCE_NAME,
        "stations_requested": stations,
        "period": {"start": start, "end": end},
        "fields": field_str,
        "reading_count": len(readings),
        "readings": readings,
    }
    await cache.set(cache_key, result, ttl=config.CACHE_TTL)
    return result


# ---------------------------------------------------------------------------
# 4. GET /api/coagmet/overview — source info with station list
# ---------------------------------------------------------------------------

@router.get("/overview")
async def coagmet_overview():
    """Return source metadata and current station list from CoAgMET."""
    cache_key = "coagmet:overview"
    hit = await cache.get(cache_key)
    if hit is not None:
        return hit

    # Fetch latest to enumerate active stations
    station_list: list[dict] = []
    try:
        data = await _get_json(f"{_BASE}/latest.json")
        if isinstance(data, list):
            for row in data:
                st = _row_to_station(row)
                station_list.append(st.model_dump(exclude_none=True))
    except Exception:
        logger.warning("CoAgMET overview: failed to fetch station list", exc_info=True)

    result = {
        "source": SOURCE_NAME,
        "description": SOURCE_DESCRIPTION,
        "meta": SOURCE_META,
        "station_count": len(station_list),
        "stations": station_list,
    }
    await cache.set(cache_key, result, ttl=config.CACHE_TTL_LONG)
    return result
