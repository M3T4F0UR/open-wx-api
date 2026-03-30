"""US State Mesonets and Regional Weather Networks.

Aggregates observations from state mesonets and regional networks across the
United States.  Two data paths are supported:

  1. Iowa Environmental Mesonet (IEM) — free, no auth, aggregates ASOS, COOP,
     RWIS, and state mesonets.  This is the primary path.
  2. Synoptic / MesoWest API — requires SYNOPTIC_TOKEN; covers named state
     mesonets (Oklahoma, Kansas, Nebraska, Kentucky, West Texas, NYS, etc.)
     with higher data quality and richer metadata.

The Oklahoma Mesonet is the gold standard for density and data quality (~120
stations).  It is reachable through both IEM (OK_ASOS / OK_MESONET networks)
and the Synoptic API.

IEM API reference:   https://mesonet.agron.iastate.edu/api/1/
Synoptic reference:  https://api.synopticdata.com/v2/
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone, date
from typing import Optional

import httpx
from fastapi import APIRouter, HTTPException, Query, Path

import cache
import config
from models import Reading, Station, StationDetail

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Source registry metadata
# ---------------------------------------------------------------------------
SOURCE_NAME = "US Mesonets"
SOURCE_DESCRIPTION = (
    "US state mesonets and regional weather networks — Oklahoma Mesonet, Iowa "
    "Environmental Mesonet (IEM), ASOS, COOP, RWIS, and named state mesonets "
    "via Synoptic/MesoWest.  Free IEM path requires no authentication; the "
    "Synoptic path requires a SYNOPTIC_TOKEN."
)
SOURCE_META = {
    "url": "https://mesonet.agron.iastate.edu/api/1/",
    "auth_required": False,
    "update_frequency": "5 min",
    "coverage": "United States",
    "params": [
        "temperature", "dewpoint", "relative_humidity", "wind_speed",
        "wind_direction", "wind_gust", "precipitation", "pressure",
        "visibility", "road_temperature", "road_condition",
    ],
    "category": "weather",
}

router = APIRouter(prefix="/api/mesonets", tags=["US Mesonets"])

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
IEM_BASE = "https://mesonet.agron.iastate.edu/api/1"
SYNOPTIC_BASE = "https://api.synopticdata.com/v2"

# Well-known IEM network suffixes — used to build descriptions
_IEM_NETWORK_TYPES = {
    "ASOS": "Automated Surface Observing System",
    "COOP": "Cooperative Observer Network",
    "RWIS": "Road Weather Information System",
    "MESONET": "State Mesonet",
    "DCP": "Data Collection Platform",
    "ISUSM": "Iowa State University Soil Moisture",
    "AWOS": "Automated Weather Observing System",
}

# Synoptic network IDs for named state mesonets
_SYNOPTIC_MESONETS: dict[str, dict] = {
    "OK_MESONET": {
        "name": "Oklahoma Mesonet",
        "state": "OK",
        "description": "120-station gold-standard state mesonet since 1994",
        "url": "https://www.mesonet.org/",
    },
    "KS_MESONET": {
        "name": "Kansas Mesonet",
        "state": "KS",
        "description": "Kansas State University statewide mesonet",
        "url": "https://mesonet.k-state.edu/",
    },
    "NE_MESONET": {
        "name": "Nebraska Mesonet",
        "state": "NE",
        "description": "University of Nebraska statewide mesonet",
        "url": "https://mesonet.unl.edu/",
    },
    "KY_MESONET": {
        "name": "Kentucky Mesonet",
        "state": "KY",
        "description": "Western Kentucky University statewide mesonet",
        "url": "https://www.kymesonet.org/",
    },
    "WTXM": {
        "name": "West Texas Mesonet",
        "state": "TX",
        "description": "Texas Tech University west-Texas network",
        "url": "https://www.mesonet.ttu.edu/",
    },
    "NYSM": {
        "name": "New York State Mesonet",
        "state": "NY",
        "description": "University at Albany statewide mesonet",
        "url": "https://www.nysmesonet.org/",
    },
    "AL_ASOS": {
        "name": "Alabama Mesonet / ASOS",
        "state": "AL",
        "description": "Alabama cooperative ASOS network",
        "url": "https://www.alabamamesonet.org/",
    },
}

# IEM variable -> (canonical name, unit)
_IEM_VAR_MAP: dict[str, tuple[str, str]] = {
    "tmpf":   ("temperature", "°F"),
    "tmpc":   ("temperature", "°C"),
    "dwpf":   ("dewpoint", "°F"),
    "dwpc":   ("dewpoint", "°C"),
    "relh":   ("relative_humidity", "%"),
    "sknt":   ("wind_speed", "kt"),
    "drct":   ("wind_direction", "deg"),
    "gust":   ("wind_gust", "kt"),
    "p01i":   ("precipitation_1h", "in"),
    "p01m":   ("precipitation_1h", "mm"),
    "alti":   ("altimeter", "inHg"),
    "mslp":   ("pressure_msl", "mb"),
    "vsby":   ("visibility", "mi"),
    "skyc1":  ("sky_cover_1", "oktas"),
    "skyl1":  ("sky_level_1", "ft"),
    "feel":   ("apparent_temperature", "°F"),
    "ice_accretion_1hr": ("ice_accretion_1h", "in"),
    "ice_accretion_3hr": ("ice_accretion_3h", "in"),
    "ice_accretion_6hr": ("ice_accretion_6h", "in"),
    "peak_wind_gust":    ("peak_wind_gust", "kt"),
    "peak_wind_drct":    ("peak_wind_direction", "deg"),
    "snowdepth":         ("snow_depth", "in"),
    # RWIS extras
    "road_tmpf":         ("road_temperature", "°F"),
    "subf":              ("subsurface_temperature", "°F"),
    "scond0":            ("road_condition", "code"),
    "pcpn":              ("precipitation", "in"),
    "tsoil":             ("soil_temperature", "°F"),
}

# Synoptic variable -> (canonical name, unit source key)
_SYNOPTIC_VAR_MAP: dict[str, tuple[str, str]] = {
    "air_temp":              ("temperature", "°C"),
    "dew_point_temperature": ("dewpoint", "°C"),
    "relative_humidity":     ("relative_humidity", "%"),
    "wind_speed":            ("wind_speed", "m/s"),
    "wind_direction":        ("wind_direction", "deg"),
    "wind_gust":             ("wind_gust", "m/s"),
    "precip_accum_one_hour": ("precipitation_1h", "mm"),
    "precip_accum":          ("precipitation", "mm"),
    "pressure":              ("pressure", "mb"),
    "sea_level_pressure":    ("pressure_msl", "mb"),
    "visibility":            ("visibility", "km"),
    "snow_depth":            ("snow_depth", "mm"),
    "road_temp":             ("road_temperature", "°C"),
    "road_condition":        ("road_condition", "code"),
    "soil_temp":             ("soil_temperature", "°C"),
    "solar_radiation":       ("solar_radiation", "W/m²"),
    "fuel_moisture":         ("fuel_moisture", "%"),
}

_HTTP_HEADERS = {
    "User-Agent": config.USER_AGENT,
    "Accept": "application/json",
}

TODAY = date.today().isoformat()


# ---------------------------------------------------------------------------
# HTTP helper
# ---------------------------------------------------------------------------

def _client() -> httpx.AsyncClient:
    return httpx.AsyncClient(
        headers=_HTTP_HEADERS,
        timeout=config.HTTP_TIMEOUT,
        follow_redirects=True,
    )


# ---------------------------------------------------------------------------
# IEM parsers
# ---------------------------------------------------------------------------

def _iem_station_from_current(obs: dict, network: str) -> Station:
    """Build a Station from an IEM currents observation dict."""
    sid = obs.get("station") or obs.get("id", "")
    lat = float(obs.get("lat") or obs.get("latitude") or 0.0)
    lon = float(obs.get("lon") or obs.get("longitude") or 0.0)
    elev_raw = obs.get("elevation") or obs.get("elev")
    try:
        elevation = float(elev_raw) if elev_raw is not None else None
    except (TypeError, ValueError):
        elevation = None

    valid_str = obs.get("utc_valid") or obs.get("valid")
    last_update: Optional[datetime] = None
    if valid_str:
        try:
            last_update = datetime.fromisoformat(
                valid_str.replace("Z", "+00:00")
            )
        except ValueError:
            pass

    params_list = [
        _IEM_VAR_MAP[k][0]
        for k in _IEM_VAR_MAP
        if obs.get(k) is not None and obs.get(k) != "M"
    ]

    return Station(
        id=sid,
        name=obs.get("name") or obs.get("sname") or sid,
        lat=lat,
        lon=lon,
        source="iem",
        network=network,
        params=params_list,
        elevation=elevation,
        url=f"https://mesonet.agron.iastate.edu/sites/site.php?station={sid}&network={network}",
        last_update=last_update,
        extra={
            "network": network,
            "county": obs.get("county", ""),
            "state": obs.get("state", ""),
        },
    )


def _iem_readings_from_current(obs: dict, network: str) -> list[Reading]:
    """Extract Reading objects from a single IEM currents observation dict."""
    sid = obs.get("station") or obs.get("id", "")
    valid_str = obs.get("utc_valid") or obs.get("valid")
    try:
        ts = datetime.fromisoformat(
            (valid_str or "").replace("Z", "+00:00")
        )
    except ValueError:
        ts = datetime.now(timezone.utc)

    readings: list[Reading] = []
    for iem_key, (param_name, unit) in _IEM_VAR_MAP.items():
        raw = obs.get(iem_key)
        if raw is None or raw == "M":
            continue
        try:
            value = float(raw)
        except (TypeError, ValueError):
            continue
        readings.append(
            Reading(
                station_id=sid,
                timestamp=ts,
                parameter=param_name,
                value=value,
                unit=unit,
                quality=None,
                source="iem",
            )
        )
    return readings


def _iem_readings_from_history(rows: list[dict], station_id: str) -> list[Reading]:
    """Parse IEM obhistory rows into Reading objects."""
    readings: list[Reading] = []
    for row in rows:
        valid_str = row.get("utc_valid") or row.get("valid")
        try:
            ts = datetime.fromisoformat(
                (valid_str or "").replace("Z", "+00:00")
            )
        except ValueError:
            continue

        for iem_key, (param_name, unit) in _IEM_VAR_MAP.items():
            raw = row.get(iem_key)
            if raw is None or raw == "M":
                continue
            try:
                value = float(raw)
            except (TypeError, ValueError):
                continue
            readings.append(
                Reading(
                    station_id=station_id,
                    timestamp=ts,
                    parameter=param_name,
                    value=value,
                    unit=unit,
                    quality=None,
                    source="iem",
                )
            )
    return readings


# ---------------------------------------------------------------------------
# Synoptic parsers
# ---------------------------------------------------------------------------

def _synoptic_station(obs: dict) -> Station:
    """Build a Station from a Synoptic STATION observation dict."""
    sid = obs.get("STID", "")
    lat = float(obs.get("LATITUDE") or 0.0)
    lon = float(obs.get("LONGITUDE") or 0.0)
    elev_raw = obs.get("ELEVATION")
    try:
        elevation = float(elev_raw) if elev_raw is not None else None
    except (TypeError, ValueError):
        elevation = None

    network_name = obs.get("MNET_SHORTNAME") or obs.get("NETWORK") or ""

    obs_block = obs.get("OBSERVATIONS", {})
    params_list: list[str] = []
    latest_ts: Optional[datetime] = None
    for skey, (cname, _) in _SYNOPTIC_VAR_MAP.items():
        if f"{skey}_value_1" in obs_block and cname not in params_list:
            params_list.append(cname)
        # Track the latest observation timestamp across all variables
        ts_str = obs_block.get(f"{skey}_value_1_time")
        if ts_str:
            try:
                ts = datetime.fromisoformat(
                    ts_str.replace("Z", "+00:00")
                )
                if latest_ts is None or ts > latest_ts:
                    latest_ts = ts
            except (ValueError, AttributeError):
                pass

    return Station(
        id=sid,
        name=obs.get("NAME") or sid,
        lat=lat,
        lon=lon,
        source="synoptic",
        network=network_name,
        params=params_list,
        elevation=elevation,
        last_update=latest_ts,
        url=f"https://api.synopticdata.com/v2/stations/latest?stid={sid}",
        extra={
            "status": obs.get("STATUS", ""),
            "mnet_id": obs.get("MNET_ID", ""),
            "period_of_record": obs.get("PERIOD_OF_RECORD", {}),
            "county": obs.get("COUNTY", ""),
            "state": obs.get("STATE", ""),
            "timezone": obs.get("TIMEZONE", ""),
            "qc_flagged": obs.get("QC_FLAGGED", False),
        },
    )


def _synoptic_readings(obs: dict) -> list[Reading]:
    """Extract Reading objects from a Synoptic STATION observation block."""
    sid = obs.get("STID", "")
    obs_block = obs.get("OBSERVATIONS", {})

    readings: list[Reading] = []
    for skey, (param_name, unit) in _SYNOPTIC_VAR_MAP.items():
        value_key = f"{skey}_value_1"
        time_key = f"{skey}_value_1_time"
        raw = obs_block.get(value_key)
        if raw is None:
            continue
        try:
            value = float(raw)
        except (TypeError, ValueError):
            continue

        ts_str = obs_block.get(time_key)
        try:
            ts = datetime.fromisoformat(
                (ts_str or "").replace("Z", "+00:00")
            )
        except (ValueError, AttributeError):
            ts = datetime.now(timezone.utc)

        readings.append(
            Reading(
                station_id=sid,
                timestamp=ts,
                parameter=param_name,
                value=value,
                unit=unit,
                quality="qc_flagged" if obs.get("QC_FLAGGED") else None,
                source="synoptic",
            )
        )
    return readings


# ---------------------------------------------------------------------------
# IEM endpoints
# ---------------------------------------------------------------------------

@router.get(
    "/iem/networks",
    summary="List IEM networks",
)
async def iem_networks(
    state: Optional[str] = Query(
        None,
        description="Filter by 2-letter state code (e.g. 'OR').  Returns all types for that state.",
    ),
):
    """Return a list of IEM network identifiers.

    When no state is specified the endpoint returns the master network list
    from IEM.  When a state is specified it returns the common network IDs
    for that state (ASOS, COOP, RWIS).
    """
    if state:
        state = state.upper()
        networks = [
            {
                "id": f"{state}_ASOS",
                "name": f"{state} ASOS",
                "description": _IEM_NETWORK_TYPES["ASOS"],
                "url": f"{IEM_BASE}/currents.json?network={state}_ASOS",
            },
            {
                "id": f"{state}_COOP",
                "name": f"{state} COOP",
                "description": _IEM_NETWORK_TYPES["COOP"],
                "url": f"{IEM_BASE}/currents.json?network={state}_COOP",
            },
            {
                "id": f"{state}_RWIS",
                "name": f"{state} RWIS",
                "description": _IEM_NETWORK_TYPES["RWIS"],
                "url": f"{IEM_BASE}/currents.json?network={state}_RWIS",
            },
        ]
        if state == "OK":
            networks.append(
                {
                    "id": "OK_MESONET",
                    "name": "Oklahoma Mesonet",
                    "description": _IEM_NETWORK_TYPES["MESONET"]
                    + " — 120 stations since 1994",
                    "url": f"{IEM_BASE}/currents.json?network=OK_MESONET",
                }
            )
        if state == "IA":
            networks.append(
                {
                    "id": "ISUSM",
                    "name": "Iowa State University Soil Moisture",
                    "description": _IEM_NETWORK_TYPES["ISUSM"],
                    "url": f"{IEM_BASE}/currents.json?network=ISUSM",
                }
            )
        return {
            "state": state,
            "networks": networks,
            "count": len(networks),
            "note": "Use the 'id' field as the 'network' parameter for other endpoints.",
        }

    # Full network list from IEM
    cache_key = "iem:networks:all"
    cached_result = await cache.get(cache_key)
    if cached_result is not None:
        return cached_result

    try:
        async with _client() as client:
            resp = await client.get(f"{IEM_BASE}/networks.json")
            resp.raise_for_status()
            data = resp.json()
    except httpx.TimeoutException:
        raise HTTPException(status_code=504, detail="IEM API timeout")
    except httpx.HTTPStatusError as exc:
        raise HTTPException(
            status_code=exc.response.status_code,
            detail=f"IEM API error: {exc}",
        )
    except Exception as exc:
        logger.exception("Error fetching IEM network list")
        raise HTTPException(status_code=500, detail=str(exc))

    result = {
        "source": "iem",
        "data": data,
        "tip": "Pass a network 'id' to GET /api/mesonets/iem/current?network=<id>",
    }
    await cache.set(cache_key, result, ttl=config.CACHE_TTL_LONG)
    return result


@router.get(
    "/iem/current",
    summary="IEM current conditions for a network",
)
async def iem_current(
    network: Optional[str] = Query(
        None,
        description="IEM network ID, e.g. 'OR_ASOS', 'OK_MESONET', 'IA_RWIS'",
    ),
    state: Optional[str] = Query(
        None,
        description="2-letter state code; automatically picks {STATE}_ASOS if network not specified",
    ),
):
    """Return current conditions for all stations in an IEM network.

    Provide either `network` (exact IEM network ID) or `state` (2-letter code,
    which defaults to the ASOS network for that state).
    """
    if not network and not state:
        raise HTTPException(
            status_code=400,
            detail="Provide 'network' (e.g. OR_ASOS) or 'state' (2-letter code)",
        )

    resolved_network = network or f"{state.upper()}_ASOS"
    cache_key = f"iem:current:{resolved_network}"
    cached_result = await cache.get(cache_key)
    if cached_result is not None:
        return cached_result

    try:
        async with _client() as client:
            resp = await client.get(
                f"{IEM_BASE}/currents.json",
                params={"network": resolved_network},
            )
            resp.raise_for_status()
            data = resp.json()
    except httpx.TimeoutException:
        raise HTTPException(status_code=504, detail="IEM API timeout")
    except httpx.HTTPStatusError as exc:
        raise HTTPException(
            status_code=exc.response.status_code,
            detail=f"IEM API error: {exc}",
        )
    except Exception as exc:
        logger.exception("Error fetching IEM current conditions for %s", resolved_network)
        raise HTTPException(status_code=500, detail=str(exc))

    raw_obs = data.get("data", [])
    stations_out: list[dict] = []
    readings_out: list[dict] = []

    for obs in raw_obs:
        try:
            station = _iem_station_from_current(obs, resolved_network)
            readings = _iem_readings_from_current(obs, resolved_network)
            stations_out.append(station.model_dump())
            readings_out.extend(r.model_dump() for r in readings)
        except Exception:
            logger.debug("Failed to parse IEM obs: %s", obs, exc_info=True)

    result = {
        "source": "iem",
        "network": resolved_network,
        "station_count": len(stations_out),
        "reading_count": len(readings_out),
        "stations": stations_out,
        "readings": readings_out,
    }
    await cache.set(cache_key, result, ttl=config.CACHE_TTL)
    return result


@router.get(
    "/iem/stations/{station_id}/history",
    summary="IEM observation history for a station",
)
async def iem_station_history(
    station_id: str = Path(description="IEM station identifier, e.g. 'KOKC' or 'KTOP'"),
    network: str = Query(
        ...,
        description="IEM network the station belongs to, e.g. 'OK_ASOS'",
    ),
    date: Optional[str] = Query(
        None,
        description=f"Date in YYYY-MM-DD format (defaults to today: {TODAY})",
    ),
):
    """Return observation history for a single IEM station on a given date.

    The IEM obhistory endpoint returns all available observations for the
    requested UTC date, typically at 5- or 60-minute intervals depending
    on the network.
    """
    obs_date = date or TODAY
    # Validate date format
    try:
        datetime.strptime(obs_date, "%Y-%m-%d")
    except ValueError:
        raise HTTPException(
            status_code=400,
            detail="date must be in YYYY-MM-DD format",
        )

    cache_key = f"iem:history:{station_id}:{network}:{obs_date}"
    cached_result = await cache.get(cache_key)
    if cached_result is not None:
        return cached_result

    try:
        async with _client() as client:
            resp = await client.get(
                f"{IEM_BASE}/obhistory.json",
                params={
                    "station": station_id,
                    "network": network,
                    "date": obs_date,
                },
            )
            resp.raise_for_status()
            data = resp.json()
    except httpx.TimeoutException:
        raise HTTPException(status_code=504, detail="IEM API timeout")
    except httpx.HTTPStatusError as exc:
        raise HTTPException(
            status_code=exc.response.status_code,
            detail=f"IEM API error: {exc}",
        )
    except Exception as exc:
        logger.exception("Error fetching IEM history for %s / %s", station_id, obs_date)
        raise HTTPException(status_code=500, detail=str(exc))

    raw_rows = data.get("data", [])
    if not raw_rows:
        raise HTTPException(
            status_code=404,
            detail=f"No IEM observations found for station '{station_id}' "
                   f"in network '{network}' on {obs_date}",
        )

    readings = _iem_readings_from_history(raw_rows, station_id)
    readings.sort(key=lambda r: r.timestamp)

    # Pull station metadata from the first row
    station_meta: Optional[dict] = None
    if raw_rows:
        try:
            station_meta = _iem_station_from_current(
                raw_rows[0], network
            ).model_dump()
        except Exception:
            pass

    result = {
        "source": "iem",
        "station_id": station_id,
        "network": network,
        "date": obs_date,
        "reading_count": len(readings),
        "station": station_meta,
        "readings": [r.model_dump() for r in readings],
    }

    # Cache historical data longer; today's data uses normal TTL
    ttl = config.CACHE_TTL_LONG if obs_date != TODAY else config.CACHE_TTL
    await cache.set(cache_key, result, ttl=ttl)
    return result


# ---------------------------------------------------------------------------
# Synoptic endpoints
# ---------------------------------------------------------------------------

def _synoptic_params(
    state: Optional[str],
    network: Optional[str],
    vars_str: Optional[str],
    limit: int,
) -> dict:
    """Build query-param dict for Synoptic latest endpoint."""
    p: dict = {
        "token": config.SYNOPTIC_TOKEN,
        "limit": limit,
        "units": "metric",
        "output": "json",
    }
    if state:
        p["state"] = state.upper()
    if network:
        p["network"] = network
    if vars_str:
        p["vars"] = vars_str
    else:
        p["vars"] = ",".join(_SYNOPTIC_VAR_MAP.keys())
    return p


@router.get(
    "/synoptic/latest",
    summary="Latest observations from Synoptic mesonet API",
)
async def synoptic_latest(
    state: Optional[str] = Query(
        None,
        description="2-letter state code filter (e.g. 'OK', 'KS')",
    ),
    network: Optional[str] = Query(
        None,
        description="Synoptic network ID (e.g. 'OK_MESONET', 'NYSM').  "
                    "See GET /api/mesonets/synoptic/networks for options.",
    ),
    vars: Optional[str] = Query(
        None,
        description="Comma-separated Synoptic variable names.  "
                    "Defaults to all mapped variables.",
    ),
    limit: int = Query(
        500, ge=1, le=5000,
        description="Maximum number of stations to return",
    ),
):
    """Return latest observations from any Synoptic-accessible mesonet.

    Requires SYNOPTIC_TOKEN environment variable.  Returns station metadata
    and the most recent value for each requested variable.
    """
    if not config.SYNOPTIC_TOKEN:
        raise HTTPException(
            status_code=503,
            detail="SYNOPTIC_TOKEN is not configured.  "
                   "Set the environment variable to enable Synoptic endpoints.",
        )
    if not state and not network:
        raise HTTPException(
            status_code=400,
            detail="Provide at least 'state' or 'network'",
        )

    cache_key = f"synoptic:latest:{state}:{network}:{vars}:{limit}"
    cached_result = await cache.get(cache_key)
    if cached_result is not None:
        return cached_result

    params_q = _synoptic_params(state, network, vars, limit)

    try:
        async with _client() as client:
            resp = await client.get(f"{SYNOPTIC_BASE}/stations/latest", params=params_q)
            resp.raise_for_status()
            data = resp.json()
    except httpx.TimeoutException:
        raise HTTPException(status_code=504, detail="Synoptic API timeout")
    except httpx.HTTPStatusError as exc:
        raise HTTPException(
            status_code=exc.response.status_code,
            detail=f"Synoptic API error: {exc}",
        )
    except Exception as exc:
        logger.exception("Error fetching Synoptic latest")
        raise HTTPException(status_code=500, detail=str(exc))

    summary = data.get("SUMMARY", {})
    if summary.get("RESPONSE_CODE") not in (1, None):
        raise HTTPException(
            status_code=502,
            detail=f"Synoptic API error: {summary.get('RESPONSE_MESSAGE', 'unknown')}",
        )

    raw_stations = data.get("STATION", []) or []
    stations_out: list[dict] = []
    readings_out: list[dict] = []

    for obs in raw_stations:
        try:
            station = _synoptic_station(obs)
            readings = _synoptic_readings(obs)
            stations_out.append(station.model_dump())
            readings_out.extend(r.model_dump() for r in readings)
        except Exception:
            logger.debug("Failed to parse Synoptic station: %s", obs.get("STID"), exc_info=True)

    result = {
        "source": "synoptic",
        "state_filter": state,
        "network_filter": network,
        "station_count": len(stations_out),
        "reading_count": len(readings_out),
        "summary": summary,
        "stations": stations_out,
        "readings": readings_out,
    }
    await cache.set(cache_key, result, ttl=config.CACHE_TTL)
    return result


@router.get(
    "/synoptic/networks",
    summary="Named state mesonet networks available via Synoptic",
)
async def synoptic_networks():
    """Return the list of named state mesonet networks accessible via Synoptic.

    These networks are available if SYNOPTIC_TOKEN is configured.  Each entry
    includes the network ID to pass to GET /api/mesonets/synoptic/latest.
    """
    token_configured = bool(config.SYNOPTIC_TOKEN)

    networks_out = []
    for net_id, meta in _SYNOPTIC_MESONETS.items():
        networks_out.append(
            {
                "id": net_id,
                "name": meta["name"],
                "state": meta["state"],
                "description": meta["description"],
                "url": meta["url"],
                "synoptic_endpoint": (
                    f"/api/mesonets/synoptic/latest?network={net_id}"
                ),
            }
        )

    return {
        "token_configured": token_configured,
        "note": (
            "Set SYNOPTIC_TOKEN environment variable to enable Synoptic endpoints."
            if not token_configured
            else "SYNOPTIC_TOKEN is configured.  All networks are available."
        ),
        "networks": networks_out,
        "count": len(networks_out),
    }


# ---------------------------------------------------------------------------
# RWIS endpoint
# ---------------------------------------------------------------------------

@router.get(
    "/rwis/current",
    summary="Current Road Weather Information System (RWIS) conditions",
)
async def rwis_current(
    state: str = Query(
        ...,
        description="2-letter US state code (required), e.g. 'OR', 'MN', 'WI'",
        min_length=2,
        max_length=2,
    ),
):
    """Return current RWIS (Road Weather Information System) observations for a state.

    Data is sourced from IEM, which aggregates DOT RWIS sensors.  RWIS stations
    measure road surface temperature, road condition codes, subsurface
    temperature, air temperature, precipitation, and visibility.

    Not all states have RWIS data in IEM.  States with dense RWIS coverage
    include MN, WI, IA, OR, WA, CO, and most Midwest/Great Plains states.
    """
    state = state.upper()
    network = f"{state}_RWIS"
    cache_key = f"iem:rwis:{state}"
    cached_result = await cache.get(cache_key)
    if cached_result is not None:
        return cached_result

    try:
        async with _client() as client:
            resp = await client.get(
                f"{IEM_BASE}/currents.json",
                params={"network": network},
            )
            resp.raise_for_status()
            data = resp.json()
    except httpx.TimeoutException:
        raise HTTPException(status_code=504, detail="IEM API timeout")
    except httpx.HTTPStatusError as exc:
        raise HTTPException(
            status_code=exc.response.status_code,
            detail=f"IEM API error fetching RWIS for {state}: {exc}",
        )
    except Exception as exc:
        logger.exception("Error fetching RWIS data for %s", state)
        raise HTTPException(status_code=500, detail=str(exc))

    raw_obs = data.get("data", [])
    if not raw_obs:
        raise HTTPException(
            status_code=404,
            detail=f"No RWIS data found for state '{state}'.  "
                   "Not all states have RWIS coverage in IEM.",
        )

    stations_out: list[dict] = []
    readings_out: list[dict] = []

    for obs in raw_obs:
        try:
            station = _iem_station_from_current(obs, network)
            readings = _iem_readings_from_current(obs, network)
            stations_out.append(station.model_dump())
            readings_out.extend(r.model_dump() for r in readings)
        except Exception:
            logger.debug("Failed to parse RWIS obs: %s", obs, exc_info=True)

    result = {
        "source": "iem_rwis",
        "state": state,
        "network": network,
        "station_count": len(stations_out),
        "reading_count": len(readings_out),
        "stations": stations_out,
        "readings": readings_out,
    }
    await cache.set(cache_key, result, ttl=config.CACHE_TTL)
    return result


# ---------------------------------------------------------------------------
# Overview endpoint
# ---------------------------------------------------------------------------

@router.get(
    "/overview",
    summary="Summary of all available mesonet sources and their status",
)
async def overview():
    """Return a summary of all available mesonet data sources.

    Includes IEM networks, RWIS availability note, Synoptic token status,
    and the named state mesonet directory.
    """
    synoptic_available = bool(config.SYNOPTIC_TOKEN)

    # Common US state codes for which IEM has good data
    well_covered_states = [
        "AL", "AR", "AZ", "CA", "CO", "FL", "GA", "IA", "ID", "IL",
        "IN", "KS", "KY", "LA", "MD", "MI", "MN", "MO", "MS", "MT",
        "NC", "ND", "NE", "NM", "NV", "NY", "OH", "OK", "OR", "PA",
        "SC", "SD", "TN", "TX", "UT", "VA", "WA", "WI", "WV", "WY",
    ]

    sources = [
        {
            "id": "iem",
            "name": "Iowa Environmental Mesonet (IEM)",
            "description": (
                "Free, no-auth aggregator of ASOS, COOP, RWIS, and state "
                "mesonets across the US.  Backbone of this module."
            ),
            "url": "https://mesonet.agron.iastate.edu/api/1/",
            "auth_required": False,
            "endpoints": [
                "GET /api/mesonets/iem/networks",
                "GET /api/mesonets/iem/current?network=OR_ASOS",
                "GET /api/mesonets/iem/stations/{id}/history?network=OK_ASOS",
            ],
            "network_types": list(_IEM_NETWORK_TYPES.keys()),
            "well_covered_states": well_covered_states,
        },
        {
            "id": "iem_rwis",
            "name": "State DOT RWIS via IEM",
            "description": (
                "Road Weather Information System sensors aggregated by IEM.  "
                "Provides road surface temperature, road condition, and standard "
                "met variables at highway sensor sites."
            ),
            "url": "https://mesonet.agron.iastate.edu/api/1/currents.json?network=OR_RWIS",
            "auth_required": False,
            "endpoints": [
                "GET /api/mesonets/rwis/current?state=OR",
            ],
            "note": "Coverage varies by state; best in MN, WI, IA, OR, WA, CO.",
        },
        {
            "id": "synoptic",
            "name": "Synoptic / MesoWest",
            "description": (
                "Commercial mesonet aggregator with high-quality QC data from "
                "named state mesonets.  Requires SYNOPTIC_TOKEN."
            ),
            "url": "https://api.synopticdata.com/v2/",
            "auth_required": True,
            "token_configured": synoptic_available,
            "endpoints": [
                "GET /api/mesonets/synoptic/networks",
                "GET /api/mesonets/synoptic/latest?network=OK_MESONET",
            ],
            "named_mesonets": list(_SYNOPTIC_MESONETS.keys()),
        },
        {
            "id": "oklahoma_mesonet",
            "name": "Oklahoma Mesonet (gold standard)",
            "description": (
                "120-station statewide mesonet operated by OU and OSU since 1994.  "
                "5-minute data with exceptional QC.  Accessible via IEM "
                "(network=OK_MESONET) or Synoptic."
            ),
            "url": "https://www.mesonet.org/",
            "auth_required": False,
            "iem_network": "OK_MESONET",
            "iem_endpoint": "GET /api/mesonets/iem/current?network=OK_MESONET",
        },
    ]

    return {
        "module": "mesonets",
        "source_count": len(sources),
        "synoptic_token_configured": synoptic_available,
        "primary_free_source": "Iowa Environmental Mesonet (IEM)",
        "iem_base_url": IEM_BASE,
        "sources": sources,
        "variable_map": {
            "iem": {k: {"parameter": v[0], "unit": v[1]} for k, v in _IEM_VAR_MAP.items()},
            "synoptic": {k: {"parameter": v[0], "unit": v[1]} for k, v in _SYNOPTIC_VAR_MAP.items()},
        },
    }
