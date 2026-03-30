"""FAA Aviation Weather Center — complete US METAR station coverage.

The Aviation Weather Center (aviationweather.gov) provides free, unauthenticated
access to decoded METAR observations, TAFs, and station metadata for every
METAR-reporting station in the United States — airports, military airfields,
heliports, and AWOS/ASOS sites.  Many of these stations are NOT present in the
IEM or Synoptic aggregators, closing the VEIA/ASOS coverage gap.

API reference: https://aviationweather.gov/data/api/
All endpoints return JSON.  No authentication or API key required.
"""
from __future__ import annotations

import asyncio
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

SOURCE_NAME = "FAA Aviation Weather"
SOURCE_DESCRIPTION = (
    "FAA Aviation Weather Center provides decoded METAR observations, TAFs, "
    "and station metadata for every METAR-reporting station in the US — airports, "
    "military airfields, heliports, AWOS, and ASOS sites.  Free REST JSON API "
    "with no authentication required.  Closes the VEIA/ASOS coverage gap by "
    "including stations not present in IEM or Synoptic."
)
SOURCE_META = {
    "url": "https://aviationweather.gov/",
    "auth_required": False,
    "update_frequency": "1–5 min (METAR cycle)",
    "coverage": "United States (all METAR-reporting stations)",
    "params": [
        "temperature", "dewpoint", "wind_speed", "wind_direction",
        "wind_gust", "visibility", "altimeter", "sea_level_pressure",
        "cloud_cover", "flight_category", "raw_metar",
    ],
    "category": "weather",
}

router = APIRouter(prefix="/api/faa_weather", tags=["FAA Aviation Weather"])

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

AWC_BASE = "https://aviationweather.gov/api/data"

# METAR field -> (canonical parameter name, unit)
_METAR_FIELD_MAP: dict[str, tuple[str, str]] = {
    "temp":    ("temperature",    "°C"),
    "dewp":    ("dewpoint",       "°C"),
    "wspd":    ("wind_speed",     "kt"),
    "wdir":    ("wind_direction", "°"),
    "wgst":    ("wind_gust",      "kt"),
    "visib":   ("visibility",     "sm"),
    "altim":   ("altimeter",      "inHg"),
    "slp":     ("sea_level_pressure", "mb"),
}

# Flight category order for display
_FLIGHT_CATEGORIES = {"VFR": 1, "MVFR": 2, "IFR": 3, "LIFR": 4}

# US state codes (2-letter, used for @state queries)
_US_STATES = [
    "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA",
    "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD",
    "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ",
    "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC",
    "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY",
    "DC", "PR", "GU", "VI", "AS",
]

_HTTP_HEADERS = {
    "User-Agent": config.USER_AGENT,
    "Accept": "application/json",
}


# ---------------------------------------------------------------------------
# HTTP client helper
# ---------------------------------------------------------------------------

def _make_client() -> httpx.AsyncClient:
    return httpx.AsyncClient(
        headers=_HTTP_HEADERS,
        timeout=config.HTTP_TIMEOUT,
        follow_redirects=True,
    )


# ---------------------------------------------------------------------------
# Parsing helpers
# ---------------------------------------------------------------------------

def _parse_obs_time(raw: Any) -> Optional[datetime]:
    """Parse the observation time from a METAR record."""
    if not raw:
        return None
    try:
        # AWC returns ISO 8601 strings like "2026-03-20T18:53:00Z"
        s = str(raw).replace("Z", "+00:00")
        return datetime.fromisoformat(s)
    except (ValueError, TypeError):
        return None


def _parse_clouds(clouds: Any) -> list[dict]:
    """Parse cloud layers from the AWC clouds array."""
    if not clouds or not isinstance(clouds, list):
        return []
    result = []
    for layer in clouds:
        if isinstance(layer, dict):
            result.append({
                "cover": layer.get("cover", ""),
                "base_ft": layer.get("base"),
            })
    return result


def _metar_to_station(ob: dict) -> Station:
    """Convert an AWC METAR record to a Station object."""
    icao = ob.get("icaoId", ob.get("stationId", ""))
    name = ob.get("name", icao)
    lat = ob.get("lat", 0.0)
    lon = ob.get("lon", 0.0)
    elev_m = ob.get("elev")

    # Build parameter list from available fields
    params = []
    for field, (param_name, _unit) in _METAR_FIELD_MAP.items():
        val = ob.get(field)
        if val is not None and val != "":
            params.append(param_name)
    if ob.get("fltcat"):
        params.append("flight_category")
    if ob.get("rawOb"):
        params.append("raw_metar")

    obs_time = _parse_obs_time(ob.get("reportTime") or ob.get("obsTime"))

    return Station(
        id=icao,
        name=name,
        lat=float(lat) if lat else 0.0,
        lon=float(lon) if lon else 0.0,
        source="faa_aviation",
        network="METAR",
        params=params,
        elevation=float(elev_m) if elev_m is not None else None,
        last_update=obs_time,
        extra={
            k: ob.get(k)
            for k in ("fltcat", "rawOb", "prior", "mostRecent")
            if ob.get(k) is not None
        },
    )


def _metar_to_readings(ob: dict) -> list[Reading]:
    """Extract Reading objects from a decoded METAR record."""
    icao = ob.get("icaoId", ob.get("stationId", ""))
    obs_time = _parse_obs_time(ob.get("reportTime") or ob.get("obsTime"))
    if not obs_time:
        obs_time = datetime.now(timezone.utc)

    readings: list[Reading] = []

    for field, (param_name, unit) in _METAR_FIELD_MAP.items():
        val = ob.get(field)
        if val is None or val == "":
            continue
        try:
            readings.append(Reading(
                station_id=icao,
                timestamp=obs_time,
                parameter=param_name,
                value=float(val),
                unit=unit,
                source="faa_aviation",
            ))
        except (ValueError, TypeError):
            pass

    # Flight category as a coded reading (VFR=1, MVFR=2, IFR=3, LIFR=4)
    fltcat = ob.get("fltcat")
    if fltcat and fltcat in _FLIGHT_CATEGORIES:
        readings.append(Reading(
            station_id=icao,
            timestamp=obs_time,
            parameter="flight_category",
            value=float(_FLIGHT_CATEGORIES[fltcat]),
            unit=fltcat,
            source="faa_aviation",
        ))

    return readings


def _metar_to_detail(ob: dict) -> StationDetail:
    """Build a full StationDetail from a single METAR record."""
    return StationDetail(
        station=_metar_to_station(ob),
        readings=_metar_to_readings(ob),
    )


# ---------------------------------------------------------------------------
# API fetch helpers
# ---------------------------------------------------------------------------

async def _fetch_metar_by_state(state: str) -> list[dict]:
    """Fetch current METARs for a US state (2-letter code)."""
    cache_key = f"faa_weather:metar:state:{state.upper()}"
    cached = await cache.get(cache_key)
    if cached is not None:
        return cached

    url = f"{AWC_BASE}/metar"
    params = {"ids": f"@{state.lower()}", "format": "json"}

    async with _make_client() as client:
        resp = await client.get(url, params=params)
        if resp.status_code == 204:
            # No stations in this state/territory — return empty list.
            await cache.set(cache_key, [], ttl=config.CACHE_TTL)
            return []
        if resp.status_code != 200:
            logger.error("AWC METAR state=%s returned %d", state, resp.status_code)
            raise HTTPException(
                status_code=502,
                detail=f"AWC API returned {resp.status_code} for state={state}",
            )
        data = resp.json()

    # API returns a list of METAR objects directly
    if not isinstance(data, list):
        data = []

    await cache.set(cache_key, data, ttl=config.CACHE_TTL)
    return data


async def _fetch_metar_all_us() -> list[dict]:
    """Fetch current METARs for all US stations by sweeping all states.

    The AWC ``~us`` query caps at ~400 results.  Instead, we issue parallel
    per-state ``@state`` queries which collectively return ~2,500+ unique
    METAR-reporting stations with live observation timestamps.
    """
    cache_key = "faa_weather:metar:all_us"
    cached = await cache.get(cache_key)
    if cached is not None:
        return cached

    sem = asyncio.Semaphore(10)  # limit concurrency to be polite

    async def _fetch_state(client: httpx.AsyncClient, state: str) -> list[dict]:
        async with sem:
            # Try per-state cache first
            st_cache_key = f"faa_weather:metar:state:{state}"
            st_cached = await cache.get(st_cache_key)
            if st_cached is not None:
                return st_cached
            try:
                url = f"{AWC_BASE}/metar"
                resp = await client.get(url, params={"ids": f"@{state.lower()}", "format": "json"})
                if resp.status_code != 200:
                    logger.warning("AWC METAR state=%s returned %d", state, resp.status_code)
                    return []
                data = resp.json()
                if not isinstance(data, list):
                    return []
                await cache.set(st_cache_key, data, ttl=config.CACHE_TTL)
                return data
            except Exception:
                logger.debug("AWC METAR state=%s failed", state, exc_info=True)
                return []

    all_obs: list[dict] = []
    seen_ids: set[str] = set()

    async with _make_client() as client:
        results = await asyncio.gather(
            *[_fetch_state(client, st) for st in _US_STATES]
        )
        for state_obs in results:
            for ob in state_obs:
                icao = ob.get("icaoId", "")
                if icao and icao not in seen_ids:
                    seen_ids.add(icao)
                    all_obs.append(ob)

    logger.info("AWC METAR all-US: %d unique stations from %d state queries", len(all_obs), len(_US_STATES))
    await cache.set(cache_key, all_obs, ttl=config.CACHE_TTL)
    return all_obs


async def _fetch_station_info(ids: str) -> list[dict]:
    """Fetch station metadata for one or more ICAO IDs (comma-separated)."""
    cache_key = f"faa_weather:stationinfo:{ids.upper()}"
    cached = await cache.get(cache_key)
    if cached is not None:
        return cached

    url = f"{AWC_BASE}/stationinfo"
    params = {"ids": ids.upper(), "format": "json"}

    async with _make_client() as client:
        resp = await client.get(url, params=params)
        if resp.status_code != 200:
            logger.error("AWC stationinfo ids=%s returned %d", ids, resp.status_code)
            raise HTTPException(
                status_code=502,
                detail=f"AWC API returned {resp.status_code} for stationinfo ids={ids}",
            )
        data = resp.json()

    if not isinstance(data, list):
        data = []

    await cache.set(cache_key, data, ttl=config.CACHE_TTL_LONG)
    return data


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@router.get(
    "/metar",
    response_model=list[StationDetail],
    summary="Current METAR observations for a US state",
)
async def metar_by_state(
    state: str = Query(
        ...,
        min_length=2,
        max_length=2,
        description="US state 2-letter code (e.g. OR, CA, TX)",
        examples=["OR", "CA", "TX"],
    ),
):
    """Return decoded METAR observations for every reporting station in a state.

    Uses the AWC ``@state`` prefix query to get all stations in that state.
    Includes airports, military fields, heliports, AWOS, and ASOS sites —
    many of which are not available through IEM or Synoptic.
    """
    state = state.upper()
    if state not in _US_STATES:
        raise HTTPException(status_code=400, detail=f"Unknown state code: {state}")

    data = await _fetch_metar_by_state(state)
    return [_metar_to_detail(ob) for ob in data]


@router.get(
    "/metar/all",
    response_model=list[StationDetail],
    summary="All current US METAR observations",
)
async def metar_all_us():
    """Return decoded METAR observations for every reporting station in the US.

    This is a large response (~2,000–3,000+ stations).  Consider using the
    ``/metar?state=XX`` endpoint for targeted queries.
    """
    data = await _fetch_metar_all_us()
    return [_metar_to_detail(ob) for ob in data]


@router.get(
    "/metar/all/stations",
    summary="All current US METAR stations (flat list for catalog)",
)
async def metar_all_us_stations():
    """Return flat station dicts for every current US METAR reporter.

    Unlike ``/metar/all`` which returns full StationDetail objects (station +
    readings), this endpoint returns a simple ``{stations: [...]}`` envelope
    where each station dict has ``id``, ``name``, ``lat``, ``lon``,
    ``source``, ``last_update``, ``params``, etc. at the top level.

    Designed for consumption by the unified station catalog.  Fetches ~2,500+
    unique stations via parallel per-state AWC queries.
    """
    data = await _fetch_metar_all_us()
    stations = []
    for ob in data:
        st = _metar_to_station(ob)
        stations.append(st.model_dump())
    return {"source": "faa_aviation", "station_count": len(stations), "stations": stations}


@router.get(
    "/stations",
    response_model=list[Station],
    summary="Station metadata lookup",
)
async def station_info(
    ids: str = Query(
        ...,
        description=(
            "Comma-separated ICAO station IDs (e.g. KPDX,KSFO,KJFK). "
            "Or use @state (e.g. @OR) for all stations in a state."
        ),
        examples=["KPDX", "KPDX,KSFO", "@OR"],
    ),
):
    """Return station metadata (location, elevation, type, country, state).

    Accepts individual ICAO IDs or the ``@state`` prefix for bulk lookup.
    """
    # If it's a state query, use the stationinfo endpoint with @prefix
    query_ids = ids.strip()
    if query_ids.startswith("@"):
        # State query — pass through directly
        pass
    else:
        # Individual IDs — normalize to uppercase
        query_ids = ",".join(s.strip().upper() for s in query_ids.split(",") if s.strip())

    data = await _fetch_station_info(query_ids)

    stations = []
    for stn in data:
        icao = stn.get("icaoId", stn.get("stationId", ""))
        lat = stn.get("lat", 0.0)
        lon = stn.get("lon", 0.0)
        elev_m = stn.get("elev")
        name = stn.get("name") or stn.get("site", icao)
        stn_type = stn.get("type", "")
        country = stn.get("country", "")
        state_code = stn.get("state", "")

        stations.append(Station(
            id=icao,
            name=name,
            lat=float(lat) if lat else 0.0,
            lon=float(lon) if lon else 0.0,
            source="faa_aviation",
            network=stn_type or "METAR",
            elevation=float(elev_m) if elev_m is not None else None,
            extra={
                "country": country,
                "state": state_code,
                "type": stn_type,
                "runways": stn.get("runways"),
            },
        ))

    return stations


@router.get(
    "/overview",
    summary="FAA Aviation Weather source overview",
)
async def overview():
    """Summary of the FAA Aviation Weather data source.

    Returns endpoint descriptions, coverage details, and available parameters.
    """
    return {
        "source": SOURCE_NAME,
        "description": SOURCE_DESCRIPTION,
        "base_url": AWC_BASE,
        "auth_required": False,
        "update_frequency": "1–5 min (METAR cycle)",
        "coverage": "All US METAR-reporting stations (airports, military, heliports, AWOS/ASOS)",
        "note": (
            "Closes the VEIA/ASOS gap — includes stations not present in IEM or "
            "Synoptic aggregators.  The ~us prefix returns all US stations; the "
            "@state prefix returns all stations in a given state."
        ),
        "parameters": list(_METAR_FIELD_MAP.keys()) + ["flight_category", "clouds", "raw_metar"],
        "flight_categories": _FLIGHT_CATEGORIES,
        "endpoints": [
            {
                "method": "GET",
                "path": "/api/faa_weather/metar?state=OR",
                "description": "Current METAR observations for a US state",
            },
            {
                "method": "GET",
                "path": "/api/faa_weather/metar/all",
                "description": "All current US METAR observations (~2,000–3,000+ stations)",
            },
            {
                "method": "GET",
                "path": "/api/faa_weather/stations?ids=KPDX",
                "description": "Station metadata (location, elevation, type)",
            },
            {
                "method": "GET",
                "path": "/api/faa_weather/overview",
                "description": "This endpoint — source summary and documentation",
            },
        ],
        "upstream_api": {
            "metar": f"{AWC_BASE}/metar?ids=~us&format=json",
            "metar_by_state": f"{AWC_BASE}/metar?ids=@OR&format=json",
            "stationinfo": f"{AWC_BASE}/stationinfo?ids=KPDX&format=json",
            "taf": f"{AWC_BASE}/taf?ids=KPDX&format=json",
        },
    }
