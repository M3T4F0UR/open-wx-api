"""Baltic Weather Services — Lithuania (meteo.lt) and Estonia (Ilmateenistus).

Two national meteorological services with public, no-auth APIs:

  - **Lithuania (meteo.lt)**: JSON API with ~16 synoptic stations providing
    temperature, wind, humidity, pressure, precipitation, and cloud cover.
  - **Estonia (Ilmateenistus)**: XML feed with ~20 stations providing
    temperature, wind, humidity, pressure, and precipitation.

All upstream endpoints are public and require no authentication.
"""
from __future__ import annotations

import logging
import xml.etree.ElementTree as ET
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
SOURCE_NAME = "Baltic Weather Services"
SOURCE_DESCRIPTION = (
    "Public weather observations from Lithuania (meteo.lt, ~16 stations) and "
    "Estonia (Ilmateenistus, ~20 stations). Temperature, wind, humidity, "
    "pressure, and precipitation across the Baltic region."
)
SOURCE_META = {
    "url": "https://api.meteo.lt/",
    "auth_required": False,
    "update_frequency": "~60 min",
    "coverage": "Lithuania, Estonia",
    "params": [
        "temperature", "wind_speed", "wind_gust", "wind_direction",
        "humidity", "pressure", "precipitation", "cloud_cover",
    ],
    "category": "weather",
}

router = APIRouter(prefix="/api/baltic", tags=["Baltic Weather Services"])

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
METEO_LT_BASE = "https://api.meteo.lt/v1"
ESTONIA_OBS_URL = "https://www.ilmateenistus.ee/ilma_andmed/xml/observations.php"

# Map meteo.lt observation fields to (parameter, unit).
_LT_FIELD_MAP: dict[str, tuple[str, str]] = {
    "airTemperature":        ("temperature",    "°C"),
    "windSpeed":             ("wind_speed",     "m/s"),
    "windGust":              ("wind_gust",      "m/s"),
    "windDirection":         ("wind_direction",  "°"),
    "relativeHumidity":      ("humidity",        "%"),
    "seaLevelPressure":      ("pressure",       "hPa"),
    "totalPrecipitation":    ("precipitation",   "mm"),
    "cloudCover":            ("cloud_cover",      "%"),
}

# Map Estonia XML element names to (parameter, unit).
_EE_FIELD_MAP: dict[str, tuple[str, str]] = {
    "airtemperature":     ("temperature",    "°C"),
    "windspeed":          ("wind_speed",     "m/s"),
    "windspeedmax":       ("wind_gust",      "m/s"),
    "winddirection":      ("wind_direction",  "°"),
    "relativehumidity":   ("humidity",        "%"),
    "airpressure":        ("pressure",       "hPa"),
    "precipitations":     ("precipitation",   "mm"),
}

# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------
_HEADERS = {
    "User-Agent": config.USER_AGENT,
    "Accept": "application/json",
}

_HEADERS_XML = {
    "User-Agent": config.USER_AGENT,
    "Accept": "application/xml",
}


def _client() -> httpx.AsyncClient:
    return httpx.AsyncClient(
        headers=_HEADERS,
        timeout=config.HTTP_TIMEOUT,
        follow_redirects=True,
    )


def _xml_client() -> httpx.AsyncClient:
    return httpx.AsyncClient(
        headers=_HEADERS_XML,
        timeout=config.HTTP_TIMEOUT,
        follow_redirects=True,
    )


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _safe_float(val) -> Optional[float]:
    """Convert a value to float, returning None on failure."""
    if val is None or val == "":
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


# ---------------------------------------------------------------------------
# Lithuania (meteo.lt) parsers
# ---------------------------------------------------------------------------

def _parse_lt_observation(station_code: str, station_name: str,
                          lat: float, lon: float,
                          obs: dict) -> StationDetail:
    """Convert a meteo.lt observation dict into StationDetail."""
    ts_str = obs.get("observationTimeUtc", "")
    try:
        ts = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
    except (ValueError, TypeError):
        ts = _now_utc()

    readings: list[Reading] = []
    for field, (param, unit) in _LT_FIELD_MAP.items():
        val = _safe_float(obs.get(field))
        if val is not None:
            readings.append(Reading(
                station_id=station_code,
                timestamp=ts,
                parameter=param,
                value=val,
                unit=unit,
                source="lithuania_meteo",
            ))

    station = Station(
        id=station_code,
        name=station_name,
        lat=lat,
        lon=lon,
        source="lithuania_meteo",
        network="meteo.lt",
        params=[r.parameter for r in readings],
        url=f"{METEO_LT_BASE}/stations/{station_code}/observations/latest",
        last_update=ts,
    )

    return StationDetail(station=station, readings=readings)


# ---------------------------------------------------------------------------
# Estonia (Ilmateenistus) parsers
# ---------------------------------------------------------------------------

def _parse_ee_xml(xml_text: str) -> list[StationDetail]:
    """Parse Estonia Ilmateenistus observations XML into StationDetail list."""
    root = ET.fromstring(xml_text)

    # The root element has a timestamp attribute.
    obs_timestamp_str = root.get("timestamp", "")
    try:
        obs_ts = datetime.fromisoformat(obs_timestamp_str.replace("Z", "+00:00"))
    except (ValueError, TypeError):
        obs_ts = _now_utc()

    results: list[StationDetail] = []

    for station_el in root.findall("station"):
        name_el = station_el.find("name")
        name = name_el.text.strip() if name_el is not None and name_el.text else "Unknown"
        station_id = name.lower().replace(" ", "_").replace("-", "_")

        # Coordinates
        lat = _safe_float(
            station_el.findtext("latitude")
        ) or 0.0
        lon = _safe_float(
            station_el.findtext("longitude")
        ) or 0.0

        readings: list[Reading] = []
        for xml_tag, (param, unit) in _EE_FIELD_MAP.items():
            val = _safe_float(station_el.findtext(xml_tag))
            if val is not None:
                readings.append(Reading(
                    station_id=station_id,
                    timestamp=obs_ts,
                    parameter=param,
                    value=val,
                    unit=unit,
                    source="estonia_ilmateenistus",
                ))

        station = Station(
            id=station_id,
            name=name,
            lat=lat,
            lon=lon,
            source="estonia_ilmateenistus",
            network="Ilmateenistus",
            params=[r.parameter for r in readings],
            url=ESTONIA_OBS_URL,
            last_update=obs_ts,
        )

        results.append(StationDetail(station=station, readings=readings))

    return results


# ---------------------------------------------------------------------------
# Endpoints — Lithuania
# ---------------------------------------------------------------------------

@router.get("/lithuania/stations", summary="All Lithuanian meteo.lt stations")
async def get_lt_stations():
    """Return the list of all Lithuanian meteorological stations from meteo.lt."""
    cached = await cache.get("baltic:lt:stations")
    if cached is not None:
        return cached

    async with _client() as client:
        resp = await client.get(f"{METEO_LT_BASE}/stations")
        if resp.status_code != 200:
            raise HTTPException(502, f"meteo.lt stations API returned {resp.status_code}")
        data = resp.json()

    if not isinstance(data, list):
        raise HTTPException(502, "Unexpected response format from meteo.lt stations API")

    stations = []
    for item in data:
        code = item.get("code", "")
        stations.append(Station(
            id=code,
            name=item.get("name", code),
            lat=item.get("coordinates", {}).get("latitude", 0.0),
            lon=item.get("coordinates", {}).get("longitude", 0.0),
            source="lithuania_meteo",
            network="meteo.lt",
            params=list(_LT_FIELD_MAP.keys()),
            url=f"{METEO_LT_BASE}/stations/{code}/observations/latest",
        ).model_dump())

    await cache.set("baltic:lt:stations", stations, ttl=config.CACHE_TTL_LONG)
    return stations


@router.get("/lithuania/current", summary="Latest observations from all Lithuanian stations")
async def get_lt_current():
    """Fetch latest observations from every meteo.lt station."""
    cached = await cache.get("baltic:lt:current")
    if cached is not None:
        return cached

    # First, get the station list.
    async with _client() as client:
        resp = await client.get(f"{METEO_LT_BASE}/stations")
        if resp.status_code != 200:
            raise HTTPException(502, f"meteo.lt stations API returned {resp.status_code}")
        stations_data = resp.json()

    if not isinstance(stations_data, list):
        raise HTTPException(502, "Unexpected response format from meteo.lt stations API")

    # Fetch latest observation for each station.
    results: list[dict] = []
    async with _client() as client:
        for stn in stations_data:
            code = stn.get("code", "")
            name = stn.get("name", code)
            lat = stn.get("coordinates", {}).get("latitude", 0.0)
            lon = stn.get("coordinates", {}).get("longitude", 0.0)

            try:
                resp = await client.get(
                    f"{METEO_LT_BASE}/stations/{code}/observations/latest"
                )
                if resp.status_code != 200:
                    logger.warning("meteo.lt obs for %s returned %d", code, resp.status_code)
                    continue
                obs_data = resp.json()
            except Exception:
                logger.warning("Failed to fetch meteo.lt obs for %s", code, exc_info=True)
                continue

            # The response contains an "observations" list; take the most recent.
            observations = obs_data.get("observations", [])
            if not observations:
                continue

            latest = observations[-1]
            detail = _parse_lt_observation(code, name, lat, lon, latest)
            results.append(detail.model_dump())

    await cache.set("baltic:lt:current", results, ttl=config.CACHE_TTL)
    return results


# ---------------------------------------------------------------------------
# Endpoints — Estonia
# ---------------------------------------------------------------------------

@router.get("/estonia/current", summary="Current observations from Estonian stations")
async def get_ee_current():
    """Fetch current observations from all Estonian Ilmateenistus stations (XML)."""
    cached = await cache.get("baltic:ee:current")
    if cached is not None:
        return cached

    async with _xml_client() as client:
        resp = await client.get(ESTONIA_OBS_URL)
        if resp.status_code != 200:
            raise HTTPException(502, f"Ilmateenistus XML API returned {resp.status_code}")
        xml_text = resp.text

    try:
        details = _parse_ee_xml(xml_text)
    except ET.ParseError as exc:
        raise HTTPException(502, f"Failed to parse Ilmateenistus XML: {exc}")

    results = [d.model_dump() for d in details]
    await cache.set("baltic:ee:current", results, ttl=config.CACHE_TTL)
    return results


# ---------------------------------------------------------------------------
# Endpoints — Combined overview
# ---------------------------------------------------------------------------

@router.get("/overview", summary="Combined Baltic weather overview")
async def get_overview():
    """Return a combined overview of latest observations from both Lithuania and Estonia."""
    cached = await cache.get("baltic:overview")
    if cached is not None:
        return cached

    lt_stations: list[dict] = []
    ee_stations: list[dict] = []
    errors: list[str] = []

    # Lithuania
    try:
        lt_stations = await get_lt_current()
    except Exception as exc:
        logger.warning("Failed to fetch Lithuania data: %s", exc)
        errors.append(f"Lithuania: {exc}")

    # Estonia
    try:
        ee_stations = await get_ee_current()
    except Exception as exc:
        logger.warning("Failed to fetch Estonia data: %s", exc)
        errors.append(f"Estonia: {exc}")

    overview = {
        "timestamp": _now_utc().isoformat(),
        "sources": {
            "lithuania_meteo": {
                "station_count": len(lt_stations),
                "stations": lt_stations,
            },
            "estonia_ilmateenistus": {
                "station_count": len(ee_stations),
                "stations": ee_stations,
            },
        },
        "total_stations": len(lt_stations) + len(ee_stations),
        "errors": errors if errors else None,
    }

    await cache.set("baltic:overview", overview, ttl=config.CACHE_TTL)
    return overview
