"""IMGW Poland — synoptic and hydrological observations from IMGW-PIB.

The Institute of Meteorology and Water Management (IMGW-PIB) exposes
a public JSON API at danepubliczne.imgw.pl with no authentication.

Coverage:
  - ~60 synoptic stations   (temperature, pressure, humidity, wind, precip)
  - ~400 climate stations   (temperature, precip — not yet wired)
  - ~800 hydrological stations (river level, flow, water temperature)

All upstream endpoints are public and require no authentication.
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Optional

import httpx
from fastapi import APIRouter, HTTPException

import cache
import config
from models import Reading, Station, StationDetail

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Source registry metadata
# ---------------------------------------------------------------------------
SOURCE_NAME = "IMGW Poland"
SOURCE_DESCRIPTION = (
    "Public weather and hydrology data from IMGW-PIB (Polish Institute of "
    "Meteorology and Water Management). ~60 synoptic stations with temperature, "
    "pressure, humidity, wind, and precipitation; ~800 hydrological stations "
    "with river level, flow, and water temperature."
)
SOURCE_META = {
    "url": "https://danepubliczne.imgw.pl/",
    "auth_required": False,
    "update_frequency": "~60 min (synop), ~60 min (hydro)",
    "coverage": "Poland",
    "params": [
        "temperature", "pressure", "humidity", "wind_speed",
        "wind_direction", "precipitation", "water_level",
        "discharge", "water_temperature",
    ],
    "category": "weather",
}

router = APIRouter(prefix="/api/poland", tags=["IMGW Poland"])

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
IMGW_BASE = "https://danepubliczne.imgw.pl/api/data"

# Map IMGW Polish field names to canonical (parameter, display, unit) tuples.
_SYNOP_FIELD_MAP: dict[str, tuple[str, str, str]] = {
    "temperatura":          ("temperature",     "Temperature",     "°C"),
    "cisnienie":            ("pressure",        "Pressure",        "hPa"),
    "wilgotnosc_wzgledna":  ("humidity",        "Relative Humidity", "%"),
    "predkosc_wiatru":      ("wind_speed",      "Wind Speed",      "m/s"),
    "kierunek_wiatru":      ("wind_direction",  "Wind Direction",  "°"),
    "suma_opadu":           ("precipitation",   "Precipitation",   "mm"),
}

_HYDRO_FIELD_MAP: dict[str, tuple[str, str, str]] = {
    "stan_wody":            ("water_level",       "Water Level",       "cm"),
    "przeplyw":             ("discharge",         "Discharge",         "m³/s"),
    "temperatura_wody":     ("water_temperature", "Water Temperature", "°C"),
}

# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------
_HEADERS = {
    "User-Agent": config.USER_AGENT,
    "Accept": "application/json",
}


def _client() -> httpx.AsyncClient:
    return httpx.AsyncClient(
        headers=_HEADERS,
        timeout=config.HTTP_TIMEOUT,
        follow_redirects=True,
    )


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


# ---------------------------------------------------------------------------
# Parsers
# ---------------------------------------------------------------------------

def _parse_synop_timestamp(item: dict) -> Optional[datetime]:
    """Build a UTC-ish datetime from IMGW date/time fields."""
    date_str = item.get("data_pomiaru")      # e.g. "2026-03-20"
    hour_str = item.get("godzina_pomiaru")    # e.g. "12"
    if not date_str:
        return None
    try:
        hour = int(hour_str) if hour_str else 0
        return datetime.strptime(f"{date_str} {hour:02d}", "%Y-%m-%d %H").replace(
            tzinfo=timezone.utc
        )
    except (ValueError, TypeError):
        return None


def _parse_hydro_timestamp(item: dict) -> Optional[datetime]:
    """Build a datetime from IMGW hydro date/time fields."""
    date_str = item.get("stan_wody_data_pomiaru")
    hour_str = item.get("stan_wody_godzina_pomiaru")
    if not date_str:
        return None
    try:
        hour_str = (hour_str or "00").replace(":", "")[:2]
        hour = int(hour_str) if hour_str else 0
        return datetime.strptime(f"{date_str} {hour:02d}", "%Y-%m-%d %H").replace(
            tzinfo=timezone.utc
        )
    except (ValueError, TypeError):
        return None


def _safe_float(val) -> Optional[float]:
    """Convert a value to float, returning None on failure."""
    if val is None or val == "":
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


def _parse_synop_station(item: dict) -> StationDetail:
    """Convert a single IMGW synop JSON object to StationDetail."""
    station_id = str(item.get("id_stacji", "")).strip()
    name = item.get("stacja", station_id)
    ts = _parse_synop_timestamp(item)

    station = Station(
        id=station_id,
        name=name,
        lat=0.0,  # IMGW synop API does not provide coordinates
        lon=0.0,
        source="poland_imgw",
        network="IMGW Synop",
        params=[p for p, (_, _, _) in _SYNOP_FIELD_MAP.items()
                if _safe_float(item.get(p)) is not None],
        url=f"{IMGW_BASE}/synop/id/{station_id}",
        last_update=ts,
    )

    readings: list[Reading] = []
    for field, (param, _display, unit) in _SYNOP_FIELD_MAP.items():
        val = _safe_float(item.get(field))
        if val is not None and ts is not None:
            readings.append(Reading(
                station_id=station_id,
                timestamp=ts,
                parameter=param,
                value=val,
                unit=unit,
                source="poland_imgw",
            ))

    return StationDetail(station=station, readings=readings)


def _parse_hydro_station(item: dict) -> StationDetail:
    """Convert a single IMGW hydro JSON object to StationDetail."""
    station_id = str(item.get("id_stacji", "")).strip()
    name = item.get("stacja", station_id)
    river = item.get("rzeka", "")
    ts = _parse_hydro_timestamp(item)

    station = Station(
        id=station_id,
        name=name,
        lat=0.0,
        lon=0.0,
        source="poland_imgw",
        network="IMGW Hydro",
        params=[p for p, (_, _, _) in _HYDRO_FIELD_MAP.items()
                if _safe_float(item.get(p)) is not None],
        url=f"{IMGW_BASE}/hydro/id/{station_id}",
        last_update=ts,
        extra={"river": river} if river else {},
    )

    readings: list[Reading] = []
    for field, (param, _display, unit) in _HYDRO_FIELD_MAP.items():
        val = _safe_float(item.get(field))
        if val is not None and ts is not None:
            readings.append(Reading(
                station_id=station_id,
                timestamp=ts,
                parameter=param,
                value=val,
                unit=unit,
                source="poland_imgw",
            ))

    return StationDetail(station=station, readings=readings)


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@router.get("/synop", summary="All IMGW synoptic stations")
async def get_synop_all():
    """Return current observations from all ~60 IMGW synoptic stations."""
    cached = await cache.get("poland:synop:all")
    if cached is not None:
        return cached

    async with _client() as client:
        resp = await client.get(f"{IMGW_BASE}/synop")
        if resp.status_code != 200:
            raise HTTPException(502, f"IMGW synop API returned {resp.status_code}")
        data = resp.json()

    if not isinstance(data, list):
        raise HTTPException(502, "Unexpected response format from IMGW synop API")

    results = [_parse_synop_station(item).model_dump() for item in data]
    await cache.set("poland:synop:all", results, ttl=config.CACHE_TTL)
    return results


@router.get("/synop/{station_id}", summary="Single IMGW synoptic station")
async def get_synop_station(station_id: str):
    """Return current observations for a single IMGW synoptic station by ID."""
    cache_key = f"poland:synop:{station_id}"
    cached = await cache.get(cache_key)
    if cached is not None:
        return cached

    async with _client() as client:
        resp = await client.get(f"{IMGW_BASE}/synop/id/{station_id}")
        if resp.status_code == 404:
            raise HTTPException(404, f"Station {station_id} not found")
        if resp.status_code != 200:
            raise HTTPException(502, f"IMGW synop API returned {resp.status_code}")
        data = resp.json()

    # Single-station endpoint may return a dict or a single-element list.
    if isinstance(data, list):
        if not data:
            raise HTTPException(404, f"Station {station_id} not found")
        data = data[0]

    result = _parse_synop_station(data).model_dump()
    await cache.set(cache_key, result, ttl=config.CACHE_TTL)
    return result


@router.get("/hydro", summary="All IMGW hydrological stations")
async def get_hydro_all():
    """Return current observations from all ~800 IMGW hydrological stations."""
    cached = await cache.get("poland:hydro:all")
    if cached is not None:
        return cached

    async with _client() as client:
        resp = await client.get(f"{IMGW_BASE}/hydro")
        if resp.status_code != 200:
            raise HTTPException(502, f"IMGW hydro API returned {resp.status_code}")
        data = resp.json()

    if not isinstance(data, list):
        raise HTTPException(502, "Unexpected response format from IMGW hydro API")

    results = [_parse_hydro_station(item).model_dump() for item in data]
    await cache.set("poland:hydro:all", results, ttl=config.CACHE_TTL)
    return results


@router.get("/hydro/{station_id}", summary="Single IMGW hydrological station")
async def get_hydro_station(station_id: str):
    """Return current observations for a single IMGW hydrological station by ID."""
    cache_key = f"poland:hydro:{station_id}"
    cached = await cache.get(cache_key)
    if cached is not None:
        return cached

    async with _client() as client:
        resp = await client.get(f"{IMGW_BASE}/hydro/id/{station_id}")
        if resp.status_code == 404:
            raise HTTPException(404, f"Station {station_id} not found")
        if resp.status_code != 200:
            raise HTTPException(502, f"IMGW hydro API returned {resp.status_code}")
        data = resp.json()

    if isinstance(data, list):
        if not data:
            raise HTTPException(404, f"Station {station_id} not found")
        data = data[0]

    result = _parse_hydro_station(data).model_dump()
    await cache.set(cache_key, result, ttl=config.CACHE_TTL)
    return result
