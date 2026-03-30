"""Nordic Weather Services — Norway (Frost), Denmark (DMI), Finland (FMI).

Three national meteorological services covering Scandinavia and the Nordic
countries, all offering free API keys:

  - **Norway Frost API** (MET Norway): ~370 stations, JSON-LD format.
    Register at https://frost.met.no/ — client_id used as HTTP Basic username.
  - **Denmark DMI**: ~90 stations (+ Greenland/Faroes), Open Data gateway.
    Register at https://opendatadocs.dmi.govcloud.dk/ — API key in query param.
  - **Finland FMI**: ~400 stations, WFS (XML/GML) interface.
    Register at https://en.ilmatieteenlaitos.fi/open-data — API key in query param.
"""
from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from typing import Any, Optional
from xml.etree import ElementTree

import httpx
from fastapi import APIRouter, HTTPException, Query

import cache
import config
from models import Reading, Station

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Source registry metadata  (read by server.py /api/sources)
# ---------------------------------------------------------------------------

SOURCE_NAME = "Nordic Weather Services"
SOURCE_DESCRIPTION = (
    "Aggregated access to three Nordic national meteorological services: "
    "MET Norway Frost API (~370 stations), Danish Meteorological Institute "
    "DMI (~90 stations plus Greenland and Faroes), and Finnish Meteorological "
    "Institute FMI (~400 stations).  All three services offer free API keys."
)
SOURCE_META = {
    "url": "https://frost.met.no/",
    "auth_required": True,
    "auth_note": (
        "Each service requires its own free API key.  "
        "Set FROST_CLIENT_ID, DMI_API_KEY, and/or FMI_API_KEY env vars."
    ),
    "update_frequency": "10–60 min (varies by service and station)",
    "coverage": "Norway, Denmark (incl. Greenland/Faroes), Finland",
    "params": [
        "air_temperature", "wind_speed", "wind_direction",
        "relative_humidity", "precipitation", "pressure",
        "dew_point", "cloud_cover", "visibility",
    ],
    "category": "weather",
    "station_count": 860,
}

# ---------------------------------------------------------------------------
# Config helpers
# ---------------------------------------------------------------------------

FROST_CLIENT_ID = os.getenv("FROST_CLIENT_ID", "")
DMI_API_KEY = os.getenv("DMI_API_KEY", "")
FMI_API_KEY = os.getenv("FMI_API_KEY", "")

_HTTP_HEADERS = {
    "User-Agent": config.USER_AGENT,
    "Accept": "application/json",
}


def _require_frost() -> str:
    """Return the configured Frost client_id or raise 503."""
    cid = config.FROST_CLIENT_ID
    if not cid:
        raise HTTPException(
            status_code=503,
            detail=(
                "FROST_CLIENT_ID is not configured.  "
                "Register for a free client_id at https://frost.met.no/ "
                "and set FROST_CLIENT_ID in your environment."
            ),
        )
    return cid


def _require_dmi() -> str:
    """Return the configured DMI API key or raise 503."""
    key = config.DMI_API_KEY
    if not key:
        raise HTTPException(
            status_code=503,
            detail=(
                "DMI_API_KEY is not configured.  "
                "Register for a free key at https://opendatadocs.dmi.govcloud.dk/ "
                "and set DMI_API_KEY in your environment."
            ),
        )
    return key


def _require_fmi() -> str:
    """Return the configured FMI API key or raise 503."""
    key = config.FMI_API_KEY
    if not key:
        raise HTTPException(
            status_code=503,
            detail=(
                "FMI_API_KEY is not configured.  "
                "Register for a free key at https://en.ilmatieteenlaitos.fi/open-data "
                "and set FMI_API_KEY in your environment."
            ),
        )
    return key


# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------

def _make_client(**kwargs: Any) -> httpx.AsyncClient:
    return httpx.AsyncClient(
        headers=_HTTP_HEADERS,
        timeout=config.HTTP_TIMEOUT,
        follow_redirects=True,
        **kwargs,
    )


async def _frost_get(path: str, params: dict[str, Any]) -> Any:
    """GET a Frost API endpoint with Basic Auth (client_id as username)."""
    client_id = _require_frost()
    url = f"https://frost.met.no/{path.lstrip('/')}"
    try:
        async with _make_client() as client:
            resp = await client.get(url, params=params, auth=(client_id, ""))
            resp.raise_for_status()
            return resp.json()
    except HTTPException:
        raise
    except httpx.HTTPStatusError as exc:
        status = exc.response.status_code
        try:
            body = exc.response.json()
            msg = body.get("error", {}).get("message", str(exc))
        except Exception:
            msg = str(exc)
        if status == 401:
            raise HTTPException(
                status_code=401,
                detail=f"Frost API authentication failed — check FROST_CLIENT_ID. {msg}",
            )
        raise HTTPException(status_code=status, detail=f"Frost API error: {msg}")
    except httpx.TimeoutException:
        raise HTTPException(status_code=504, detail=f"Frost API timeout ({url})")
    except Exception as exc:
        logger.exception("Unexpected error calling Frost API: %s", url)
        raise HTTPException(status_code=502, detail=str(exc))


async def _dmi_get(path: str, params: dict[str, Any]) -> Any:
    """GET a DMI Open Data endpoint with api-key query param."""
    key = _require_dmi()
    params = {**params, "api-key": key}
    url = f"https://dmigw.govcloud.dk/{path.lstrip('/')}"
    try:
        async with _make_client() as client:
            resp = await client.get(url, params=params)
            resp.raise_for_status()
            return resp.json()
    except HTTPException:
        raise
    except httpx.HTTPStatusError as exc:
        status = exc.response.status_code
        try:
            body = exc.response.json()
            msg = body.get("message", str(exc))
        except Exception:
            msg = str(exc)
        if status == 401:
            raise HTTPException(
                status_code=401,
                detail=f"DMI API authentication failed — check DMI_API_KEY. {msg}",
            )
        raise HTTPException(status_code=status, detail=f"DMI API error: {msg}")
    except httpx.TimeoutException:
        raise HTTPException(status_code=504, detail=f"DMI API timeout ({url})")
    except Exception as exc:
        logger.exception("Unexpected error calling DMI API: %s", url)
        raise HTTPException(status_code=502, detail=str(exc))


async def _fmi_get_xml(params: dict[str, Any]) -> ElementTree.Element:
    """GET FMI WFS endpoint and return parsed XML root."""
    key = _require_fmi()
    params = {**params, "apikey": key}
    url = "https://opendata.fmi.fi/wfs"
    try:
        async with _make_client() as client:
            resp = await client.get(url, params=params)
            resp.raise_for_status()
            root = ElementTree.fromstring(resp.text)
            return root
    except HTTPException:
        raise
    except httpx.HTTPStatusError as exc:
        status = exc.response.status_code
        msg = str(exc)
        if status == 401:
            raise HTTPException(
                status_code=401,
                detail=f"FMI API authentication failed — check FMI_API_KEY. {msg}",
            )
        raise HTTPException(status_code=status, detail=f"FMI API error: {msg}")
    except httpx.TimeoutException:
        raise HTTPException(status_code=504, detail=f"FMI API timeout ({url})")
    except ElementTree.ParseError as exc:
        logger.exception("Failed to parse FMI XML response")
        raise HTTPException(status_code=502, detail=f"FMI returned invalid XML: {exc}")
    except Exception as exc:
        logger.exception("Unexpected error calling FMI API: %s", url)
        raise HTTPException(status_code=502, detail=str(exc))


# ---------------------------------------------------------------------------
# Parsing helpers — Norway Frost
# ---------------------------------------------------------------------------

def _parse_frost_station(raw: dict) -> Station:
    """Convert a Frost source object to a Station."""
    geo = raw.get("geometry", {})
    coords = geo.get("coordinates", [0.0, 0.0])
    lon = coords[0] if len(coords) > 0 else 0.0
    lat = coords[1] if len(coords) > 1 else 0.0
    elev = coords[2] if len(coords) > 2 else None

    valid_from = raw.get("validFrom", "")
    valid_to = raw.get("validTo")

    return Station(
        id=raw.get("id", ""),
        name=raw.get("name", raw.get("id", "")),
        lat=lat,
        lon=lon,
        source="norway_frost",
        network="MET Norway",
        params=[],
        elevation=elev,
        url=f"https://frost.met.no/sources/v0.jsonld?ids={raw.get('id', '')}",
        extra={
            "country": raw.get("country", "NO"),
            "county": raw.get("county", ""),
            "municipality": raw.get("municipality", ""),
            "wmo_id": raw.get("wmoId"),
            "valid_from": valid_from,
            "valid_to": valid_to,
        },
    )


def _parse_frost_observations(data: dict, station_id: str) -> list[Reading]:
    """Convert Frost observation data to Reading objects."""
    readings: list[Reading] = []
    for item in data.get("data", []):
        ref_time = item.get("referenceTime", "")
        try:
            ts = datetime.fromisoformat(ref_time.replace("Z", "+00:00"))
        except (ValueError, AttributeError):
            continue

        for obs in item.get("observations", []):
            element = obs.get("elementId", "")
            value = obs.get("value")
            unit = obs.get("unit", "")
            if value is None:
                continue
            try:
                numeric = float(value)
            except (TypeError, ValueError):
                continue

            readings.append(
                Reading(
                    station_id=station_id,
                    timestamp=ts,
                    parameter=element,
                    value=numeric,
                    unit=unit,
                    quality=obs.get("qualityCode"),
                    source="norway_frost",
                )
            )
    return readings


# ---------------------------------------------------------------------------
# Parsing helpers — Denmark DMI
# ---------------------------------------------------------------------------

def _parse_dmi_station(raw: dict) -> Station:
    """Convert a DMI station feature to a Station."""
    props = raw.get("properties", raw)
    loc = props.get("location", {})
    lat = loc.get("latitude", 0.0)
    lon = loc.get("longitude", 0.0)
    elev = loc.get("height")

    station_id = props.get("stationId", raw.get("id", ""))

    return Station(
        id=str(station_id),
        name=props.get("name", str(station_id)),
        lat=lat,
        lon=lon,
        source="denmark_dmi",
        network="DMI",
        params=[],
        elevation=elev,
        url=f"https://www.dmi.dk/",
        extra={
            "country": props.get("country", "DK"),
            "owner": props.get("owner", ""),
            "type": props.get("type", ""),
            "status": props.get("status", ""),
            "operating_from": props.get("operatingFrom", ""),
            "operating_to": props.get("operatingTo"),
        },
    )


def _parse_dmi_observations(data: dict, station_id: str) -> list[Reading]:
    """Convert DMI observation items to Reading objects."""
    readings: list[Reading] = []

    # DMI returns features in a GeoJSON-like structure
    features = data.get("features", [])
    if not features:
        # Fallback: might be a flat list
        features = data.get("items", [])

    for feat in features:
        props = feat.get("properties", feat)
        param_id = props.get("parameterId", "")
        value = props.get("value")
        if value is None:
            continue
        try:
            numeric = float(value)
        except (TypeError, ValueError):
            continue

        observed = props.get("observed", props.get("timeObserved", ""))
        try:
            ts = datetime.fromisoformat(observed.replace("Z", "+00:00"))
        except (ValueError, AttributeError):
            continue

        readings.append(
            Reading(
                station_id=str(station_id),
                timestamp=ts,
                parameter=param_id,
                value=numeric,
                unit="",  # DMI does not always include units in obs
                quality=None,
                source="denmark_dmi",
            )
        )
    return readings


# ---------------------------------------------------------------------------
# Parsing helpers — Finland FMI (XML/GML)
# ---------------------------------------------------------------------------

# FMI WFS namespaces
_FMI_NS = {
    "wfs": "http://www.opengis.net/wfs/2.0",
    "gml": "http://www.opengis.net/gml/3.2",
    "BsWfs": "http://xml.fmi.fi/schema/wfs/2.0",
}


def _parse_fmi_observations(root: ElementTree.Element, place: str) -> list[Reading]:
    """Parse FMI simple observation XML into Reading objects."""
    readings: list[Reading] = []

    for member in root.findall(".//BsWfs:BsWfsElement", _FMI_NS):
        time_el = member.find("BsWfs:Time", _FMI_NS)
        param_el = member.find("BsWfs:ParameterName", _FMI_NS)
        value_el = member.find("BsWfs:ParameterValue", _FMI_NS)

        if time_el is None or param_el is None or value_el is None:
            continue
        if time_el.text is None or param_el.text is None or value_el.text is None:
            continue

        val_text = value_el.text.strip()
        if val_text in ("NaN", "", "nan"):
            continue
        try:
            numeric = float(val_text)
        except (TypeError, ValueError):
            continue

        try:
            ts = datetime.fromisoformat(time_el.text.replace("Z", "+00:00"))
        except (ValueError, AttributeError):
            continue

        readings.append(
            Reading(
                station_id=place,
                timestamp=ts,
                parameter=param_el.text.strip(),
                value=numeric,
                unit="",
                quality=None,
                source="finland_fmi",
            )
        )
    return readings


# ---------------------------------------------------------------------------
# Router
# ---------------------------------------------------------------------------

router = APIRouter(prefix="/api/nordic", tags=["Nordic Weather Services"])


# ---------------------------------------------------------------------------
# Norway endpoints
# ---------------------------------------------------------------------------

@router.get(
    "/norway/stations",
    summary="List Norwegian weather stations from Frost API",
)
async def norway_stations(
    county: Optional[str] = Query(None, description="Filter by county name"),
):
    """Return stations from the MET Norway Frost API.

    Approximately 370 active stations across Norway.  Optionally filter by
    county name (e.g. 'Oslo', 'Troms').
    """
    cache_key = f"nordic:norway:stations:{county}"
    cached = await cache.get(cache_key)
    if cached is not None:
        return cached

    params: dict[str, Any] = {"country": "NO"}
    if county:
        params["county"] = county

    data = await _frost_get("sources/v0.jsonld", params)

    raw_sources = data.get("data", [])
    stations = [_parse_frost_station(s) for s in raw_sources]

    result = {
        "source": "MET Norway Frost API",
        "count": len(stations),
        "filters": {"county": county},
        "stations": [s.model_dump() for s in stations],
    }
    await cache.set(cache_key, result, ttl=config.CACHE_TTL_LONG)
    return result


@router.get(
    "/norway/observations",
    summary="Norwegian station observations from Frost API",
)
async def norway_observations(
    station: str = Query(..., description="Frost station ID (e.g. 'SN18700')"),
    date: str = Query(
        ...,
        description="Date as YYYY-MM-DD — returns full 24 hours of observations",
    ),
    elements: str = Query(
        "air_temperature",
        description="Comma-separated element IDs (e.g. 'air_temperature,wind_speed')",
    ),
):
    """Fetch observations for a Norwegian station on a given date.

    Uses the Frost observations endpoint with the specified station and
    date range.  Element IDs follow Frost naming: air_temperature,
    wind_speed, wind_from_direction, sum(precipitation_amount PT1H), etc.
    """
    cache_key = f"nordic:norway:obs:{station}:{date}:{elements}"
    cached = await cache.get(cache_key)
    if cached is not None:
        return cached

    # Build ISO reference time range for the full day
    reftime = f"{date}/{date}T23:59:59"

    data = await _frost_get(
        "observations/v0.jsonld",
        {
            "sources": station,
            "referencetime": reftime,
            "elements": elements,
        },
    )

    readings = _parse_frost_observations(data, station)
    readings.sort(key=lambda r: r.timestamp)

    result = {
        "source": "MET Norway Frost API",
        "station": station,
        "date": date,
        "elements": elements,
        "count": len(readings),
        "readings": [r.model_dump() for r in readings],
    }
    await cache.set(cache_key, result, ttl=config.CACHE_TTL)
    return result


# ---------------------------------------------------------------------------
# Denmark endpoints
# ---------------------------------------------------------------------------

@router.get(
    "/denmark/stations",
    summary="List Danish weather stations from DMI",
)
async def denmark_stations(
    status: Optional[str] = Query(None, description="Filter by status: 'Active' or 'Inactive'"),
):
    """Return stations from the Danish Meteorological Institute (DMI).

    Approximately 90 stations in Denmark plus stations in Greenland and the
    Faroe Islands.
    """
    cache_key = f"nordic:denmark:stations:{status}"
    cached = await cache.get(cache_key)
    if cached is not None:
        return cached

    params: dict[str, Any] = {}
    if status:
        params["status"] = status

    data = await _dmi_get("v2/metObs/collections/station/items", params)

    raw_features = data.get("features", data.get("items", []))
    stations = [_parse_dmi_station(f) for f in raw_features]

    result = {
        "source": "DMI (Danish Meteorological Institute)",
        "count": len(stations),
        "filters": {"status": status},
        "stations": [s.model_dump() for s in stations],
    }
    await cache.set(cache_key, result, ttl=config.CACHE_TTL_LONG)
    return result


@router.get(
    "/denmark/observations",
    summary="Danish station observations from DMI",
)
async def denmark_observations(
    station: str = Query(..., description="DMI station ID (e.g. '06180')"),
    start: Optional[str] = Query(
        None,
        description="Start datetime ISO 8601 (e.g. '2026-03-18T00:00:00Z'). Defaults to last 24h.",
    ),
    end: Optional[str] = Query(
        None,
        description="End datetime ISO 8601 (e.g. '2026-03-19T00:00:00Z')",
    ),
    limit: int = Query(1000, ge=1, le=10000, description="Maximum observations to return"),
):
    """Fetch observations for a Danish station.

    If no start/end provided, returns the most recent available data.
    Parameter IDs follow DMI naming: temp_dry, wind_speed, wind_dir,
    humidity, precip_past1h, pressure, etc.
    """
    cache_key = f"nordic:denmark:obs:{station}:{start}:{end}:{limit}"
    cached = await cache.get(cache_key)
    if cached is not None:
        return cached

    params: dict[str, Any] = {
        "stationId": station,
        "limit": limit,
        "sortorder": "observed,DESC",
    }
    if start and end:
        params["datetime"] = f"{start}/{end}"
    elif start:
        params["datetime"] = f"{start}/.."
    elif end:
        params["datetime"] = f"../{end}"

    data = await _dmi_get("v2/metObs/collections/observation/items", params)

    readings = _parse_dmi_observations(data, station)
    readings.sort(key=lambda r: r.timestamp)

    result = {
        "source": "DMI (Danish Meteorological Institute)",
        "station": station,
        "count": len(readings),
        "readings": [r.model_dump() for r in readings],
    }
    await cache.set(cache_key, result, ttl=config.CACHE_TTL)
    return result


# ---------------------------------------------------------------------------
# Finland endpoints
# ---------------------------------------------------------------------------

@router.get(
    "/finland/observations",
    summary="Finnish weather observations from FMI (WFS/XML)",
)
async def finland_observations(
    place: str = Query(..., description="Place name (e.g. 'helsinki', 'tampere', 'oulu')"),
    timestep: int = Query(60, ge=10, le=1440, description="Timestep in minutes (default 60)"),
):
    """Fetch current weather observations for a Finnish location from FMI.

    FMI returns the latest available observations (typically last 12 hours).
    The WFS XML response is parsed into structured readings.  Common
    parameters: t2m (temperature), ws_10min (wind speed), wg_10min (gust),
    wd_10min (wind dir), rh (humidity), r_1h (precip), p_sea (pressure).
    """
    cache_key = f"nordic:finland:obs:{place}:{timestep}"
    cached = await cache.get(cache_key)
    if cached is not None:
        return cached

    root = await _fmi_get_xml({
        "service": "WFS",
        "version": "2.0.0",
        "request": "getFeature",
        "storedquery_id": "fmi::observations::weather::simple",
        "place": place,
        "timestep": str(timestep),
    })

    readings = _parse_fmi_observations(root, place)
    readings.sort(key=lambda r: r.timestamp)

    result = {
        "source": "FMI (Finnish Meteorological Institute)",
        "place": place,
        "timestep_minutes": timestep,
        "count": len(readings),
        "readings": [r.model_dump() for r in readings],
    }
    await cache.set(cache_key, result, ttl=config.CACHE_TTL)
    return result


# ---------------------------------------------------------------------------
# Info endpoint
# ---------------------------------------------------------------------------

@router.get(
    "/info",
    summary="Nordic services configuration status",
)
async def nordic_info():
    """Return configuration status for all three Nordic weather services."""
    frost_ok = bool(config.FROST_CLIENT_ID)
    dmi_ok = bool(config.DMI_API_KEY)
    fmi_ok = bool(config.FMI_API_KEY)

    return {
        "source": SOURCE_NAME,
        "description": SOURCE_DESCRIPTION,
        "services": {
            "norway_frost": {
                "configured": frost_ok,
                "env_var": "FROST_CLIENT_ID",
                "register_url": "https://frost.met.no/",
                "station_count": 370,
                "coverage": "Norway",
                "note": None if frost_ok else (
                    "Register for a free client_id at https://frost.met.no/ "
                    "and set FROST_CLIENT_ID."
                ),
            },
            "denmark_dmi": {
                "configured": dmi_ok,
                "env_var": "DMI_API_KEY",
                "register_url": "https://opendatadocs.dmi.govcloud.dk/",
                "station_count": 90,
                "coverage": "Denmark, Greenland, Faroe Islands",
                "note": None if dmi_ok else (
                    "Register for a free API key at https://opendatadocs.dmi.govcloud.dk/ "
                    "and set DMI_API_KEY."
                ),
            },
            "finland_fmi": {
                "configured": fmi_ok,
                "env_var": "FMI_API_KEY",
                "register_url": "https://en.ilmatieteenlaitos.fi/open-data",
                "station_count": 400,
                "coverage": "Finland",
                "note": None if fmi_ok else (
                    "Register for a free API key at https://en.ilmatieteenlaitos.fi/open-data "
                    "and set FMI_API_KEY."
                ),
            },
        },
        "all_configured": frost_ok and dmi_ok and fmi_ok,
        **SOURCE_META,
    }
