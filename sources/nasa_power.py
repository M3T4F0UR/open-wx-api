"""NASA POWER — Global gridded climate data for any point on Earth.

NASA's Prediction Of Worldwide Energy Resources (POWER) project provides
solar and meteorological data from NASA research for support of renewable
energy, building energy efficiency, and agricultural needs.

API: https://power.larc.nasa.gov/
No authentication required.  JSON responses.  380+ parameters available.
Temporal coverage: daily (1981–present, ~2-day lag), hourly (2001–present),
monthly (1981–present), and long-term climatology.

Communities:
  AG — Agroclimatology
  RE — Renewable Energy
  SB — Sustainable Buildings

Max 20 parameters per request.
"""
from __future__ import annotations

import logging
from typing import Optional

import httpx
from fastapi import APIRouter, HTTPException, Query

import cache
import config

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Source registry metadata  (read by server.py /api/sources)
# ---------------------------------------------------------------------------

SOURCE_NAME = "NASA POWER"
SOURCE_DESCRIPTION = (
    "NASA POWER global gridded climate data — 380+ parameters at any point "
    "on Earth.  Daily, hourly, monthly, and climatology temporal resolutions.  "
    "Covers solar radiation, temperature, precipitation, humidity, wind, and "
    "more.  No authentication required."
)
SOURCE_META = {
    "url": "https://power.larc.nasa.gov/",
    "auth_required": False,
    "update_frequency": "Daily (~2-day lag)",
    "coverage": "Global (any lat/lon point)",
    "params": [
        "temperature", "precipitation", "humidity", "wind_speed",
        "solar_radiation", "dewpoint", "pressure", "frost_days",
    ],
    "category": "climate",
}

router = APIRouter(prefix="/api/nasa_power", tags=["NASA POWER"])

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

POWER_BASE = "https://power.larc.nasa.gov/api/temporal"

DEFAULT_COMMUNITY = "AG"

# Key parameters with human-readable descriptions and units
PARAMETER_CATALOG: dict[str, tuple[str, str]] = {
    "T2M":                ("Temperature at 2m",                "°C"),
    "T2M_MAX":            ("Max Temperature at 2m",            "°C"),
    "T2M_MIN":            ("Min Temperature at 2m",            "°C"),
    "T2MDEW":             ("Dewpoint Temperature at 2m",       "°C"),
    "T2MWET":             ("Wet Bulb Temperature at 2m",       "°C"),
    "TS":                 ("Earth Skin Temperature",           "°C"),
    "PRECTOTCORR":        ("Precipitation Corrected",          "mm/day"),
    "RH2M":               ("Relative Humidity at 2m",          "%"),
    "QV2M":               ("Specific Humidity at 2m",          "g/kg"),
    "WS2M":               ("Wind Speed at 2m",                 "m/s"),
    "WS10M":              ("Wind Speed at 10m",                "m/s"),
    "WD10M":              ("Wind Direction at 10m",            "degrees"),
    "WS50M":              ("Wind Speed at 50m",                "m/s"),
    "WD50M":              ("Wind Direction at 50m",            "degrees"),
    "PS":                 ("Surface Pressure",                 "kPa"),
    "PSC":                ("Corrected Atmospheric Pressure",   "kPa"),
    "ALLSKY_SFC_SW_DWN":  ("All Sky Surface SW Downward Irradiance", "kW-hr/m²/day"),
    "CLRSKY_SFC_SW_DWN":  ("Clear Sky Surface SW Downward Irradiance", "kW-hr/m²/day"),
    "ALLSKY_SFC_LW_DWN":  ("All Sky Surface LW Downward Irradiance", "kW-hr/m²/day"),
    "ALLSKY_SFC_PAR_TOT": ("All Sky Surface PAR Total",       "W/m²"),
    "ALLSKY_SFC_UV_INDEX": ("All Sky UV Index",                "dimensionless"),
    "FROST_DAYS":         ("Frost Days (Tmin < 0°C)",         "days"),
    "CLOUD_AMT":          ("Cloud Amount",                     "%"),
    "TOA_SW_DWN":         ("TOA Shortwave Downward Irradiance", "kW-hr/m²/day"),
    "GWETPROF":           ("Profile Soil Moisture",            "dimensionless"),
    "GWETROOT":           ("Root Zone Soil Moisture",          "dimensionless"),
    "GWETTOP":            ("Surface Soil Wetness",             "dimensionless"),
    "T2M_RANGE":          ("Temperature Range at 2m",          "°C"),
    "EVPTRNS":            ("Evapotranspiration Energy Flux",   "W/m²"),
    "PW":                 ("Precipitable Water",               "cm"),
}

DEFAULT_DAILY_PARAMS = "T2M,PRECTOTCORR,RH2M,WS10M,ALLSKY_SFC_SW_DWN"
DEFAULT_HOURLY_PARAMS = "T2M,WS10M,RH2M"
DEFAULT_CLIMO_PARAMS = "T2M,PRECTOTCORR,RH2M,ALLSKY_SFC_SW_DWN"

MAX_PARAMS_PER_REQUEST = 20

_HTTP_HEADERS = {"User-Agent": config.USER_AGENT}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

async def _fetch_power(
    temporal: str,
    lat: float,
    lon: float,
    params: str,
    community: str = DEFAULT_COMMUNITY,
    start: Optional[str] = None,
    end: Optional[str] = None,
) -> dict:
    """Fetch data from the NASA POWER API.

    Parameters
    ----------
    temporal : str
        One of "daily", "hourly", "monthly", "climatology".
    lat, lon : float
        Point coordinates.
    params : str
        Comma-separated parameter names (max 20).
    community : str
        AG, RE, or SB.
    start, end : str or None
        Date strings (YYYYMMDD for daily/hourly, YYYY for monthly).
        Not used for climatology.
    """
    param_list = [p.strip() for p in params.split(",") if p.strip()]
    if len(param_list) > MAX_PARAMS_PER_REQUEST:
        raise HTTPException(
            status_code=400,
            detail=f"Maximum {MAX_PARAMS_PER_REQUEST} parameters per request "
                   f"(got {len(param_list)}).",
        )

    url = f"{POWER_BASE}/{temporal}/point"
    query: dict[str, str] = {
        "parameters": ",".join(param_list),
        "community": community.upper(),
        "longitude": str(lon),
        "latitude": str(lat),
        "format": "JSON",
    }
    if start is not None:
        query["start"] = start
    if end is not None:
        query["end"] = end

    cache_key = f"nasa_power:{temporal}:{lat}:{lon}:{params}:{community}:{start}:{end}"
    cached = await cache.get(cache_key)
    if cached is not None:
        return cached

    async with httpx.AsyncClient(timeout=config.HTTP_TIMEOUT, headers=_HTTP_HEADERS) as client:
        try:
            resp = await client.get(url, params=query)
            resp.raise_for_status()
        except httpx.HTTPStatusError as exc:
            logger.error("NASA POWER %s error: %s", temporal, exc)
            raise HTTPException(
                status_code=502,
                detail=f"NASA POWER API returned {exc.response.status_code}: "
                       f"{exc.response.text[:500]}",
            )
        except httpx.RequestError as exc:
            logger.error("NASA POWER request failed: %s", exc)
            raise HTTPException(status_code=502, detail="NASA POWER API unreachable.")

    data = resp.json()

    # The POWER API nests results under "properties" -> "parameter"
    raw_params = data.get("properties", {}).get("parameter", {})

    # Build a clean result keyed by parameter name
    readings: dict[str, dict] = {}
    for pname, pdata in raw_params.items():
        info = PARAMETER_CATALOG.get(pname, (pname, ""))
        readings[pname] = {
            "name": info[0],
            "unit": info[1],
            "values": pdata,  # dict of date_string -> value
        }

    result = {
        "source": SOURCE_NAME,
        "temporal": temporal,
        "latitude": lat,
        "longitude": lon,
        "community": community.upper(),
        "parameters_requested": param_list,
        "start": start,
        "end": end,
        "parameter_count": len(readings),
        "readings": readings,
    }

    # Cache: daily/monthly 1 hour, hourly 30 min, climatology 24 hours
    ttl = {
        "daily": config.CACHE_TTL_LONG,
        "hourly": config.CACHE_TTL_LONG // 2,
        "monthly": config.CACHE_TTL_LONG,
        "climatology": config.CACHE_TTL_LONG * 24,
    }.get(temporal, config.CACHE_TTL_LONG)

    await cache.set(cache_key, result, ttl=ttl)
    return result


# ---------------------------------------------------------------------------
# Endpoint: GET /api/nasa_power/daily
# ---------------------------------------------------------------------------

@router.get(
    "/daily",
    summary="Daily time series at a point",
)
async def nasa_power_daily(
    lat: float = Query(..., ge=-90, le=90, description="Latitude"),
    lon: float = Query(..., ge=-180, le=180, description="Longitude"),
    start: str = Query(..., description="Start date YYYYMMDD"),
    end: str = Query(..., description="End date YYYYMMDD"),
    params: str = Query(
        DEFAULT_DAILY_PARAMS,
        description="Comma-separated parameter names (max 20)",
    ),
    community: str = Query(DEFAULT_COMMUNITY, description="Community: AG, RE, or SB"),
):
    """Fetch daily time series from NASA POWER for a single point.

    Returns one entry per day for each requested parameter.  Data coverage
    starts in 1981; there is approximately a 2-day lag for the most recent
    data.
    """
    return await _fetch_power("daily", lat, lon, params, community, start, end)


# ---------------------------------------------------------------------------
# Endpoint: GET /api/nasa_power/hourly
# ---------------------------------------------------------------------------

@router.get(
    "/hourly",
    summary="Hourly time series at a point",
)
async def nasa_power_hourly(
    lat: float = Query(..., ge=-90, le=90, description="Latitude"),
    lon: float = Query(..., ge=-180, le=180, description="Longitude"),
    start: str = Query(..., description="Start date YYYYMMDD"),
    end: str = Query(..., description="End date YYYYMMDD"),
    params: str = Query(
        DEFAULT_HOURLY_PARAMS,
        description="Comma-separated parameter names (max 20)",
    ),
    community: str = Query(DEFAULT_COMMUNITY, description="Community: AG, RE, or SB"),
):
    """Fetch hourly time series from NASA POWER for a single point.

    Hourly data is available from 2001 onward.  Note that the POWER API
    returns hourly values keyed as YYYYMMDDHH strings.
    """
    return await _fetch_power("hourly", lat, lon, params, community, start, end)


# ---------------------------------------------------------------------------
# Endpoint: GET /api/nasa_power/climatology
# ---------------------------------------------------------------------------

@router.get(
    "/climatology",
    summary="Long-term monthly climatology at a point",
)
async def nasa_power_climatology(
    lat: float = Query(..., ge=-90, le=90, description="Latitude"),
    lon: float = Query(..., ge=-180, le=180, description="Longitude"),
    params: str = Query(
        DEFAULT_CLIMO_PARAMS,
        description="Comma-separated parameter names (max 20)",
    ),
    community: str = Query(DEFAULT_COMMUNITY, description="Community: AG, RE, or SB"),
):
    """Fetch long-term monthly climatology from NASA POWER.

    Returns average values for each month (keyed "1" through "12" plus "13"
    for the annual mean).  Based on the full period of record (~1981–present).
    """
    return await _fetch_power("climatology", lat, lon, params, community)


# ---------------------------------------------------------------------------
# Endpoint: GET /api/nasa_power/parameters
# ---------------------------------------------------------------------------

@router.get(
    "/parameters",
    summary="List available NASA POWER parameters",
)
async def nasa_power_parameters():
    """Return the catalog of commonly used NASA POWER parameters.

    Each entry includes the parameter code, human-readable name, and unit.
    The full POWER API supports 380+ parameters; this list covers the most
    useful ones for weather, agriculture, and energy applications.
    """
    return {
        "source": SOURCE_NAME,
        "max_per_request": MAX_PARAMS_PER_REQUEST,
        "parameter_count": len(PARAMETER_CATALOG),
        "parameters": {
            code: {"name": name, "unit": unit}
            for code, (name, unit) in PARAMETER_CATALOG.items()
        },
    }


# ---------------------------------------------------------------------------
# Endpoint: GET /api/nasa_power/info
# ---------------------------------------------------------------------------

@router.get(
    "/info",
    summary="NASA POWER source information",
)
async def nasa_power_info():
    """Return metadata about the NASA POWER data source."""
    return {
        "source": SOURCE_NAME,
        "description": SOURCE_DESCRIPTION,
        "auth_required": False,
        "api_base": POWER_BASE,
        "communities": {
            "AG": "Agroclimatology",
            "RE": "Renewable Energy",
            "SB": "Sustainable Buildings",
        },
        "temporal_resolutions": ["daily", "hourly", "monthly", "climatology"],
        "coverage": "Global (any latitude/longitude point)",
        "data_lag": "~2 days for daily data",
        "daily_coverage": "1981–present",
        "hourly_coverage": "2001–present",
        "max_params_per_request": MAX_PARAMS_PER_REQUEST,
        "total_parameters": "380+",
        "catalog_parameters": len(PARAMETER_CATALOG),
        "documentation": "https://power.larc.nasa.gov/docs/",
        "endpoints": [
            {"path": "/api/nasa_power/daily", "method": "GET", "description": "Daily time series at a point"},
            {"path": "/api/nasa_power/hourly", "method": "GET", "description": "Hourly time series at a point"},
            {"path": "/api/nasa_power/climatology", "method": "GET", "description": "Long-term monthly climatology"},
            {"path": "/api/nasa_power/parameters", "method": "GET", "description": "List available parameters"},
            {"path": "/api/nasa_power/info", "method": "GET", "description": "Source information"},
        ],
    }
