"""Synoptic Data / MesoWest mega-aggregator — wxsection.com data platform.

Synoptic Data aggregates real-time and historical observations from 170,000+
stations across 320+ contributing networks (NWS, FAA, DOT, RAWS, CoCoRaHS,
mesonets, universities, utilities, ski resorts, and much more).

A free public token is available at: https://synopticdata.com/mesonet-api/

API reference: https://docs.synopticdata.com/services/
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

SOURCE_NAME = "Synoptic Data / MesoWest"
SOURCE_DESCRIPTION = (
    "Synoptic Data aggregates real-time and historical weather observations from "
    "170,000+ stations across 320+ contributing networks worldwide — including NWS, "
    "FAA ASOS/AWOS, RAWS fire-weather, CoCoRaHS, state mesonets, DOT road sensors, "
    "ski resorts, universities, and utilities.  A single API token provides access "
    "to all networks.  Free public tokens available at synopticdata.com."
)
SOURCE_META = {
    "url": "https://synopticdata.com/",
    "auth_required": True,
    "auth_note": "Free token at https://synopticdata.com/mesonet-api/  — set SYNOPTIC_TOKEN env var",
    "update_frequency": "1–60 min (varies by network)",
    "coverage": "United States + international partner networks",
    "params": [
        "air_temp", "wind_speed", "wind_direction", "wind_gust",
        "relative_humidity", "dew_point_temperature",
        "precip_accum", "precip_accum_24_hour",
        "altimeter", "sea_level_pressure", "pressure",
        "visibility", "cloud_layer_1", "weather_condition",
        "snow_depth", "snow_water_equiv",
        "solar_radiation", "soil_temp", "soil_moisture",
        "road_temp", "road_condition",
    ],
    "category": "weather",
    "station_count": 170000,
    "network_count": 320,
}

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

SYNOPTIC_BASE = "https://api.synopticdata.com/v2"

# Synoptic variable → (human name, SI unit)
VARIABLE_MAP: dict[str, tuple[str, str]] = {
    "air_temp":                    ("Air Temperature",           "°C"),
    "dew_point_temperature":       ("Dew Point",                 "°C"),
    "relative_humidity":           ("Relative Humidity",         "%"),
    "wind_speed":                  ("Wind Speed",                "m/s"),
    "wind_direction":              ("Wind Direction",            "°"),
    "wind_gust":                   ("Wind Gust",                 "m/s"),
    "precip_accum":                ("Precipitation Accumulated", "mm"),
    "precip_accum_24_hour":        ("Precipitation 24-hr",       "mm"),
    "precip_accum_one_hour":       ("Precipitation 1-hr",        "mm"),
    "altimeter":                   ("Altimeter Setting",         "inHg"),
    "sea_level_pressure":          ("Sea Level Pressure",        "mb"),
    "pressure":                    ("Station Pressure",          "mb"),
    "visibility":                  ("Visibility",                "mi"),
    "solar_radiation":             ("Solar Radiation",           "W/m²"),
    "snow_depth":                  ("Snow Depth",                "in"),
    "snow_water_equiv":            ("Snow Water Equivalent",     "in"),
    "soil_temp":                   ("Soil Temperature",          "°C"),
    "soil_moisture":               ("Soil Moisture",             "fraction"),
    "road_temp":                   ("Road Temperature",          "°C"),
    "road_condition":              ("Road Condition",            "code"),
    "ceiling":                     ("Cloud Ceiling",             "ft"),
    "cloud_layer_1":               ("Cloud Layer 1",             "code"),
    "weather_condition":           ("Weather Condition",         "code"),
    "volt":                        ("Battery Voltage",           "V"),
    "water_temp":                  ("Water Temperature",         "°C"),
    "stream_flow":                 ("Stream Flow",               "ft³/s"),
    "water_level":                 ("Water Level",               "ft"),
}

DEFAULT_VARS = "air_temp,wind_speed,wind_direction,wind_gust,relative_humidity,precip_accum"

_HTTP_HEADERS = {
    "User-Agent": config.USER_AGENT,
    "Accept": "application/json",
}


# ---------------------------------------------------------------------------
# Token guard
# ---------------------------------------------------------------------------

def _require_token() -> str:
    """Return the configured Synoptic token or raise a descriptive 503."""
    token = config.SYNOPTIC_TOKEN
    if not token:
        raise HTTPException(
            status_code=503,
            detail=(
                "SYNOPTIC_TOKEN is not configured.  "
                "Obtain a free public token at https://synopticdata.com/mesonet-api/ "
                "and set it as the SYNOPTIC_TOKEN environment variable.  "
                "The free tier provides access to all 170,000+ stations."
            ),
        )
    return token


# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------

def _make_client() -> httpx.AsyncClient:
    return httpx.AsyncClient(
        headers=_HTTP_HEADERS,
        timeout=config.HTTP_TIMEOUT,
        follow_redirects=True,
    )


async def _synoptic_get(path: str, params: dict[str, Any]) -> dict:
    """GET a Synoptic endpoint; inject token; raise HTTPException on error."""
    token = _require_token()
    params = {**params, "token": token}

    url = f"{SYNOPTIC_BASE}/{path.lstrip('/')}"
    try:
        async with _make_client() as client:
            resp = await client.get(url, params=params)
            resp.raise_for_status()
            return resp.json()
    except HTTPException:
        raise
    except httpx.TimeoutException:
        raise HTTPException(status_code=504, detail=f"Synoptic API timeout ({url})")
    except httpx.HTTPStatusError as exc:
        _handle_synoptic_http_error(exc)
    except Exception as exc:
        logger.exception("Unexpected error calling Synoptic API: %s", url)
        raise HTTPException(status_code=502, detail=str(exc))


def _handle_synoptic_http_error(exc: httpx.HTTPStatusError) -> None:
    """Convert Synoptic HTTP errors to meaningful HTTPExceptions."""
    status = exc.response.status_code
    try:
        body = exc.response.json()
        summary = body.get("SUMMARY", {})
        msg = summary.get("RESPONSE_MESSAGE", str(exc))
    except Exception:
        msg = str(exc)

    if status == 401:
        raise HTTPException(
            status_code=401,
            detail=f"Synoptic API authentication failed — check SYNOPTIC_TOKEN. Detail: {msg}",
        )
    raise HTTPException(status_code=status, detail=f"Synoptic API error: {msg}")


def _check_synoptic_summary(data: dict) -> None:
    """Raise HTTPException if the Synoptic SUMMARY block signals an error."""
    summary = data.get("SUMMARY", {})
    response_code = summary.get("RESPONSE_CODE", 1)
    if response_code != 1:
        msg = summary.get("RESPONSE_MESSAGE", "Unknown Synoptic API error")
        raise HTTPException(status_code=400, detail=f"Synoptic API: {msg}")


# ---------------------------------------------------------------------------
# Data-normalisation helpers
# ---------------------------------------------------------------------------

def _parse_station(raw: dict) -> Station:
    """Convert a Synoptic STATION object to the shared Station model."""
    stid = raw.get("STID", "")
    obs = raw.get("OBSERVATIONS", {})
    period = raw.get("PERIOD_OF_RECORD", {})

    # Build parameter list from observations block keys (strip trailing set index)
    params = []
    for key in obs:
        # Keys are like "air_temp_value_1", "wind_speed_value_1", etc.
        base = key.rsplit("_value_", 1)[0] if "_value_" in key else key
        human, _ = VARIABLE_MAP.get(base, (base.replace("_", " ").title(), ""))
        if human and human not in params:
            params.append(human)

    try:
        lat = float(raw.get("LATITUDE", 0.0))
    except (TypeError, ValueError):
        lat = 0.0
    try:
        lon = float(raw.get("LONGITUDE", 0.0))
    except (TypeError, ValueError):
        lon = 0.0
    try:
        elev_ft = raw.get("ELEVATION")
        elevation = float(elev_ft) * 0.3048 if elev_ft is not None else None  # ft → m
    except (TypeError, ValueError):
        elevation = None

    # Last observation timestamp
    last_update: Optional[datetime] = None
    obs_dt = raw.get("OBSERVATIONS", {})
    # Prefer date_time array if present
    dt_vals = obs_dt.get("date_time", [])
    if dt_vals:
        try:
            last_str = dt_vals[-1]
            last_update = datetime.fromisoformat(last_str.replace("Z", "+00:00"))
        except (ValueError, IndexError):
            pass

    network_name = raw.get("MNET_SHORTNAME", raw.get("MNET_ID", ""))

    return Station(
        id=stid,
        name=raw.get("NAME", stid),
        lat=lat,
        lon=lon,
        source="synoptic",
        network=str(network_name),
        params=params,
        elevation=elevation,
        url=f"https://mesowest.utah.edu/cgi-bin/droman/meso_base_dyn.cgi?stn={stid}",
        last_update=last_update,
        extra={
            "state": raw.get("STATE", ""),
            "country": raw.get("COUNTRY", "US"),
            "timezone": raw.get("TIMEZONE", ""),
            "mnet_id": raw.get("MNET_ID", ""),
            "status": raw.get("STATUS", ""),
            "restricted": raw.get("RESTRICTED", False),
            "period_of_record": {
                "start": period.get("start", ""),
                "end": period.get("end", ""),
            } if period else {},
        },
    )


def _parse_observations(raw: dict) -> list[Reading]:
    """Convert a Synoptic OBSERVATIONS block to Reading objects."""
    stid = raw.get("STID", "")
    obs = raw.get("OBSERVATIONS", {})
    if not obs:
        return []

    # date_time array is the timestamp spine
    date_times = obs.get("date_time", [])

    readings: list[Reading] = []

    # Variables are keyed as "{var_name}_value_{set_index}"
    # Iterate all keys except date_time
    for key, values in obs.items():
        if key == "date_time":
            continue
        if not isinstance(values, list):
            continue

        # Derive the variable name from the key
        if "_value_" in key:
            var_name, _ = key.rsplit("_value_", 1)
        else:
            var_name = key

        human_name, unit = VARIABLE_MAP.get(var_name, (var_name.replace("_", " ").title(), ""))

        # Pair each value with the corresponding timestamp
        for i, val in enumerate(values):
            if val is None:
                continue
            try:
                numeric = float(val)
            except (TypeError, ValueError):
                continue

            if i < len(date_times):
                try:
                    dt_str = date_times[i].replace("Z", "+00:00")
                    ts = datetime.fromisoformat(dt_str)
                except (ValueError, AttributeError):
                    continue
            else:
                # Fall back to now if index overruns the timestamp array
                ts = datetime.now(tz=timezone.utc)

            readings.append(
                Reading(
                    station_id=stid,
                    timestamp=ts,
                    parameter=human_name,
                    value=numeric,
                    unit=unit,
                    quality=None,
                    source="synoptic",
                )
            )

    return readings


def _latest_per_param(readings: list[Reading]) -> list[Reading]:
    """Return only the most-recent reading per (station_id, parameter) pair."""
    best: dict[tuple[str, str], Reading] = {}
    for r in readings:
        key = (r.station_id, r.parameter)
        if key not in best or r.timestamp > best[key].timestamp:
            best[key] = r
    return list(best.values())


# ---------------------------------------------------------------------------
# Router
# ---------------------------------------------------------------------------

router = APIRouter(prefix="/api/synoptic", tags=["Synoptic / MesoWest"])


# ---------------------------------------------------------------------------
# Endpoint: GET /api/synoptic/stations/nearest
# NOTE: declared before /{station_id} to avoid path-param capture
# ---------------------------------------------------------------------------

@router.get(
    "/stations/nearest",
    summary="Nearest stations to a lat/lon point",
)
async def nearest_stations(
    lat: float = Query(..., description="Latitude (decimal degrees)"),
    lon: float = Query(..., description="Longitude (decimal degrees)"),
    radius: float = Query(25.0, ge=0.1, le=500.0, description="Search radius in miles"),
    limit: int = Query(20, ge=1, le=500, description="Maximum stations to return"),
    vars: str = Query(
        DEFAULT_VARS,
        description="Comma-separated Synoptic variable names to retrieve",
    ),
):
    """Find the nearest active Synoptic stations to a geographic point.

    Returns stations within `radius` miles that reported within the last 60
    minutes.  Stations are ordered by distance from the query point.
    """
    cache_key = f"synoptic:nearest:{lat}:{lon}:{radius}:{limit}:{vars}"
    cached_result = await cache.get(cache_key)
    if cached_result is not None:
        return cached_result

    data = await _synoptic_get(
        "stations/nearesttime",
        {
            "lat": lat,
            "lon": lon,
            "radius": f"{radius}",
            "within": 60,
            "limit": limit,
            "vars": vars,
            "obtimezone": "UTC",
            "units": "metric",
        },
    )
    _check_synoptic_summary(data)

    raw_stations = data.get("STATION", []) or []
    stations = [_parse_station(s) for s in raw_stations]
    readings_by_station = {
        s.get("STID", ""): _latest_per_param(_parse_observations(s))
        for s in raw_stations
    }

    result = {
        "query": {"lat": lat, "lon": lon, "radius_miles": radius},
        "count": len(stations),
        "stations": [
            {
                **st.model_dump(),
                "latest_readings": [r.model_dump() for r in readings_by_station.get(st.id, [])],
            }
            for st in stations
        ],
    }
    await cache.set(cache_key, result, ttl=config.CACHE_TTL)
    return result


# ---------------------------------------------------------------------------
# Endpoint: GET /api/synoptic/stations/latest
# ---------------------------------------------------------------------------

@router.get(
    "/stations/latest",
    summary="Latest observations — filter by state, bbox, network, or variables",
)
async def stations_latest(
    state: Optional[str] = Query(
        None,
        description="Two-letter US state code (e.g. 'OR', 'CA').  Multiple allowed: 'OR,WA'",
    ),
    bbox: Optional[str] = Query(
        None,
        description="Bounding box as west,south,east,north decimal degrees",
    ),
    network: Optional[str] = Query(
        None,
        description="Synoptic network ID(s), comma-separated (e.g. '1,2,71')",
    ),
    vars: str = Query(
        DEFAULT_VARS,
        description="Comma-separated Synoptic variable names",
    ),
    limit: int = Query(100, ge=1, le=2000, description="Maximum stations to return"),
    status: str = Query("active", description="Station status filter: 'active', 'inactive', or 'all'"),
):
    """Retrieve the latest observations from Synoptic stations.

    At least one spatial filter (state or bbox) is recommended; without one the
    result set can be very large.

    Variable names follow the Synoptic naming convention:
    `air_temp`, `wind_speed`, `wind_direction`, `wind_gust`,
    `relative_humidity`, `precip_accum`, `dew_point_temperature`, etc.
    Full list: https://docs.synopticdata.com/services/variables
    """
    if not state and not bbox:
        raise HTTPException(
            status_code=400,
            detail=(
                "Provide at least one spatial filter: "
                "'state' (e.g. ?state=OR) or 'bbox' (west,south,east,north)"
            ),
        )

    cache_key = f"synoptic:latest:{state}:{bbox}:{network}:{vars}:{limit}:{status}"
    cached_result = await cache.get(cache_key)
    if cached_result is not None:
        return cached_result

    params: dict[str, Any] = {
        "vars": vars,
        "limit": limit,
        "obtimezone": "UTC",
        "units": "metric",
    }
    if state:
        params["state"] = state.upper()
    if bbox:
        parts = [p.strip() for p in bbox.split(",")]
        if len(parts) != 4:
            raise HTTPException(
                status_code=400,
                detail="bbox must be four comma-separated values: west,south,east,north",
            )
        params["bbox"] = ",".join(parts)
    if network:
        params["network"] = network
    if status != "all":
        params["status"] = status

    data = await _synoptic_get("stations/latest", params)
    _check_synoptic_summary(data)

    raw_stations = data.get("STATION", []) or []
    stations = [_parse_station(s) for s in raw_stations]
    readings_by_station = {
        s.get("STID", ""): _latest_per_param(_parse_observations(s))
        for s in raw_stations
    }

    result = {
        "count": len(stations),
        "filters": {"state": state, "bbox": bbox, "network": network, "vars": vars},
        "stations": [
            {
                **st.model_dump(),
                "latest_readings": [r.model_dump() for r in readings_by_station.get(st.id, [])],
            }
            for st in stations
        ],
    }
    await cache.set(cache_key, result, ttl=config.CACHE_TTL)
    return result


# ---------------------------------------------------------------------------
# Endpoint: GET /api/synoptic/stations/{station_id}
# ---------------------------------------------------------------------------

@router.get(
    "/stations/{station_id}",
    summary="Station metadata + latest observations",
)
async def get_station(station_id: str):
    """Return full metadata and most-recent observations for a Synoptic station.

    `station_id` is the Synoptic station ID (STID), e.g. `KPDX`, `RAWS001`.
    """
    cache_key = f"synoptic:station:{station_id}"
    cached_result = await cache.get(cache_key)
    if cached_result is not None:
        return cached_result

    data = await _synoptic_get(
        "stations/latest",
        {
            "stid": station_id.upper(),
            "vars": DEFAULT_VARS,
            "obtimezone": "UTC",
            "units": "metric",
        },
    )
    _check_synoptic_summary(data)

    raw_stations = data.get("STATION", []) or []
    if not raw_stations:
        raise HTTPException(
            status_code=404,
            detail=f"Synoptic station '{station_id}' not found or has no recent data",
        )

    raw = raw_stations[0]
    station = _parse_station(raw)
    readings = _latest_per_param(_parse_observations(raw))
    readings.sort(key=lambda r: r.parameter)

    detail = StationDetail(station=station, readings=readings)
    result = detail.model_dump()
    await cache.set(cache_key, result, ttl=config.CACHE_TTL)
    return result


# ---------------------------------------------------------------------------
# Endpoint: GET /api/synoptic/stations/{station_id}/timeseries
# ---------------------------------------------------------------------------

@router.get(
    "/stations/{station_id}/timeseries",
    summary="Time-series observations for a station",
)
async def station_timeseries(
    station_id: str,
    hours: int = Query(24, ge=1, le=2160, description="Hours of history to retrieve (max 90 days)"),
    vars: str = Query(
        DEFAULT_VARS,
        description="Comma-separated Synoptic variable names",
    ),
):
    """Return a time series of observations for the specified station.

    Data is returned in chronological order.  The Synoptic API will return
    data at the station's native reporting interval (typically 5–60 minutes).
    Maximum window: 90 days (2160 hours).
    """
    cache_key = f"synoptic:timeseries:{station_id}:{hours}:{vars}"
    cached_result = await cache.get(cache_key)
    if cached_result is not None:
        return cached_result

    data = await _synoptic_get(
        "stations/timeseries",
        {
            "stid": station_id.upper(),
            "recent": hours * 60,   # Synoptic accepts minutes
            "vars": vars,
            "obtimezone": "UTC",
            "units": "metric",
        },
    )
    _check_synoptic_summary(data)

    raw_stations = data.get("STATION", []) or []
    if not raw_stations:
        raise HTTPException(
            status_code=404,
            detail=f"Synoptic station '{station_id}' not found or has no data in the requested window",
        )

    raw = raw_stations[0]
    station = _parse_station(raw)
    readings = _parse_observations(raw)
    readings.sort(key=lambda r: r.timestamp)

    result = {
        "station": station.model_dump(),
        "hours": hours,
        "vars": vars,
        "count": len(readings),
        "readings": [r.model_dump() for r in readings],
    }
    await cache.set(cache_key, result, ttl=config.CACHE_TTL)
    return result


# ---------------------------------------------------------------------------
# Endpoint: GET /api/synoptic/networks
# ---------------------------------------------------------------------------

@router.get(
    "/networks",
    summary="List all 320+ networks contributing to Synoptic",
)
async def list_networks(
    shortname: Optional[str] = Query(None, description="Filter by network short name (case-insensitive substring)"),
):
    """Return the full list of networks that contribute stations to Synoptic.

    Examples of contributing networks: NWS ASOS, FAA, RAWS, MesoWest,
    CoCoRaHS, state DOTs, university mesonets, utility SCADA networks.

    Results are cached for 1 hour (network list rarely changes).
    """
    cache_key = f"synoptic:networks:{shortname}"
    cached_result = await cache.get(cache_key)
    if cached_result is not None:
        return cached_result

    data = await _synoptic_get("networks", {})
    _check_synoptic_summary(data)

    networks = data.get("MNET", []) or []

    if shortname:
        needle = shortname.lower()
        networks = [
            n for n in networks
            if needle in (n.get("SHORTNAME", "") or "").lower()
            or needle in (n.get("LONGNAME", "") or "").lower()
        ]

    result = {
        "count": len(networks),
        "filter": {"shortname": shortname},
        "networks": [
            {
                "id": n.get("ID", ""),
                "shortname": n.get("SHORTNAME", ""),
                "longname": n.get("LONGNAME", ""),
                "url": n.get("URL", ""),
                "category": n.get("CATEGORY", ""),
                "last_observation": n.get("LAST_OBSERVATION", ""),
                "active_stations": n.get("ACTIVE_STATIONS", None),
                "total_stations": n.get("TOTAL_STATIONS", None),
            }
            for n in networks
        ],
    }
    await cache.set(cache_key, result, ttl=config.CACHE_TTL_LONG)
    return result


# ---------------------------------------------------------------------------
# Endpoint: GET /api/synoptic/precipitation
# ---------------------------------------------------------------------------

@router.get(
    "/precipitation",
    summary="Precipitation accumulation data",
)
async def precipitation(
    state: Optional[str] = Query(
        None,
        description="Two-letter state code (e.g. 'OR').  Multiple: 'OR,WA'",
    ),
    stid: Optional[str] = Query(
        None,
        description="Comma-separated station IDs (e.g. 'KPDX,KSEA')",
    ),
    hours: int = Query(24, ge=1, le=720, description="Accumulation period in hours (max 30 days)"),
    bbox: Optional[str] = Query(
        None,
        description="Bounding box: west,south,east,north",
    ),
):
    """Return precipitation accumulation data from Synoptic stations.

    Synoptic derives precipitation totals from raw tipping-bucket or gauge
    readings.  Data includes accumulated totals for the requested period.

    At least one of `state`, `stid`, or `bbox` must be provided.
    """
    if not state and not stid and not bbox:
        raise HTTPException(
            status_code=400,
            detail="Provide at least one of: state, stid (station IDs), or bbox",
        )

    cache_key = f"synoptic:precip:{state}:{stid}:{hours}:{bbox}"
    cached_result = await cache.get(cache_key)
    if cached_result is not None:
        return cached_result

    params: dict[str, Any] = {
        "recent": hours * 60,
        "obtimezone": "UTC",
        "units": "metric",
    }
    if state:
        params["state"] = state.upper()
    if stid:
        params["stid"] = stid.upper()
    if bbox:
        parts = [p.strip() for p in bbox.split(",")]
        if len(parts) != 4:
            raise HTTPException(
                status_code=400,
                detail="bbox must be four comma-separated values: west,south,east,north",
            )
        params["bbox"] = ",".join(parts)

    data = await _synoptic_get("stations/precipitation", params)
    _check_synoptic_summary(data)

    raw_stations = data.get("STATION", []) or []

    result_stations = []
    for raw in raw_stations:
        station = _parse_station(raw)
        precip_obs = raw.get("OBSERVATIONS", {})
        precip_sets = raw.get("PRECIPITATION", []) or []

        station_entry: dict[str, Any] = {**station.model_dump()}

        # Synoptic precipitation endpoint returns a PRECIPITATION array with
        # {period, total, report_type} objects per sensor set
        station_entry["precipitation_totals"] = [
            {
                "total_mm": p.get("total"),
                "report_type": p.get("report_type", ""),
                "first_report": p.get("first_report", ""),
                "last_report": p.get("last_report", ""),
            }
            for p in precip_sets
        ]
        result_stations.append(station_entry)

    result = {
        "hours": hours,
        "filters": {"state": state, "stid": stid, "bbox": bbox},
        "count": len(result_stations),
        "stations": result_stations,
    }
    await cache.set(cache_key, result, ttl=config.CACHE_TTL)
    return result


# ---------------------------------------------------------------------------
# Endpoint: GET /api/synoptic/info
# ---------------------------------------------------------------------------

@router.get(
    "/info",
    summary="Source information and configuration status",
)
async def synoptic_info():
    """Return information about the Synoptic source and whether a token is configured."""
    token_configured = bool(config.SYNOPTIC_TOKEN)
    return {
        "source": SOURCE_NAME,
        "description": SOURCE_DESCRIPTION,
        "token_configured": token_configured,
        "token_instructions": (
            None if token_configured else
            "Visit https://synopticdata.com/mesonet-api/ to register for a free public token.  "
            "Set the SYNOPTIC_TOKEN environment variable and restart the server."
        ),
        "api_base": SYNOPTIC_BASE,
        "docs": "https://docs.synopticdata.com/services/",
        **SOURCE_META,
    }
