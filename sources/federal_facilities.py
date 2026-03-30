"""DOE National Lab + NPS weather stations — no auth required.

Aggregates publicly available meteorological data from U.S. federal research
facilities and National Park Service monitoring networks:

1. **INL/NOAA Mesonet** — 35 stations, 5-min, Idaho National Laboratory
   - https://niwc.noaa.inl.gov/weather/currentobs.asp
   - Feeds into MADIS

2. **BNL Met Services** — 1 station (85m tower), Brookhaven National Lab
   - https://wx1.bnl.gov/download.htm
   - CSV, max 1 year/param per download, data since 1949

3. **ORNL Daymet** — 1km daily gridded meteorological data, REST API
   - https://daymet.ornl.gov/single-pixel/api/data
   - Returns CSV with tmax, tmin, prcp, srad, vp, swe, dayl

4. **KSC Weather Archive** — 46 sensor locations at Cape Canaveral
   - https://kscwxarchive.ksc.nasa.gov/
   - Web search interface only

5. **LANL Weather Machine** — Los Alamos National Laboratory
   - https://weather.lanl.gov/
   - Web interface only

6. **NPS I&M Climate Data** — via IRMA DataStore
   - https://irma.nps.gov/DataStore/
   - REST services at https://irmaservices.nps.gov/

No authentication required for any source.
"""
from __future__ import annotations

import csv
import io
import logging
from datetime import datetime, timezone
from typing import Optional

import httpx
from fastapi import APIRouter, HTTPException, Query

import cache
import config
from models import Station

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Source registry metadata  (read by server.py /api/sources)
# ---------------------------------------------------------------------------

SOURCE_NAME = "Federal Facility Weather"
SOURCE_DESCRIPTION = (
    "Meteorological data from U.S. federal research facilities and National "
    "Park Service monitoring networks.  Includes DOE national labs (INL, BNL, "
    "ORNL, LANL, KSC) and NPS Inventory & Monitoring climate stations.  "
    "No authentication required."
)
SOURCE_META = {
    "url": "https://daymet.ornl.gov/",
    "auth_required": False,
    "update_frequency": "Varies (5-min to daily)",
    "coverage": "United States (federal facility sites + 1km Daymet grid)",
    "params": [
        "temperature", "precipitation", "solar_radiation",
        "vapor_pressure", "swe", "day_length",
        "wind_speed", "wind_direction", "humidity", "pressure",
    ],
    "category": "government",
}

router = APIRouter(prefix="/api/fed_facilities", tags=["Federal Facility Weather"])

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

DAYMET_BASE = "https://daymet.ornl.gov/single-pixel/api/data"
INL_OBS_URL = "https://niwc.noaa.inl.gov/weather/currentobs.asp"
BNL_DOWNLOAD_URL = "https://wx1.bnl.gov/download.htm"
KSC_ARCHIVE_URL = "https://kscwxarchive.ksc.nasa.gov/"
LANL_WEATHER_URL = "https://weather.lanl.gov/"
NPS_IRMA_URL = "https://irma.nps.gov/DataStore/"
NPS_SERVICES_URL = "https://irmaservices.nps.gov/"

_HTTP_HEADERS = {"User-Agent": config.USER_AGENT}

# ---------------------------------------------------------------------------
# Daymet parameter catalog
# ---------------------------------------------------------------------------

DAYMET_PARAMS: dict[str, tuple[str, str]] = {
    "tmax":  ("Daily maximum 2-meter air temperature", "°C"),
    "tmin":  ("Daily minimum 2-meter air temperature", "°C"),
    "prcp":  ("Daily total precipitation",             "mm/day"),
    "srad":  ("Incident shortwave radiation flux",     "W/m²"),
    "vp":    ("Water vapor pressure",                  "Pa"),
    "swe":   ("Snow water equivalent",                 "kg/m²"),
    "dayl":  ("Duration of the daylight period",       "s/day"),
}

DEFAULT_DAYMET_PARAMS = "tmax,tmin,prcp,srad,vp,swe,dayl"

# ---------------------------------------------------------------------------
# INL/NOAA Mesonet station catalog  (35 stations in SE Idaho)
# ---------------------------------------------------------------------------
# Representative subset — full list at niwc.noaa.inl.gov

INL_STATIONS: dict[str, dict] = {
    "ATRF": {
        "name": "Advanced Test Reactor Facility",
        "lat": 43.588,
        "lon": -112.968,
        "elevation": 1498,
        "location": "INL Site, ID",
    },
    "CFA": {
        "name": "Central Facilities Area",
        "lat": 43.533,
        "lon": -112.949,
        "elevation": 1490,
        "location": "INL Site, ID",
    },
    "EBR": {
        "name": "Experimental Breeder Reactor",
        "lat": 43.514,
        "lon": -113.000,
        "elevation": 1509,
        "location": "INL Site, ID",
    },
    "GRID3": {
        "name": "Grid 3 (NW Corner)",
        "lat": 43.833,
        "lon": -113.250,
        "elevation": 1524,
        "location": "INL Site, ID",
    },
    "INTEC": {
        "name": "Idaho Nuclear Technology & Engineering Center",
        "lat": 43.573,
        "lon": -112.926,
        "elevation": 1490,
        "location": "INL Site, ID",
    },
    "LOFT": {
        "name": "Loss of Fluid Test Facility",
        "lat": 43.588,
        "lon": -112.924,
        "elevation": 1490,
        "location": "INL Site, ID",
    },
    "MFC": {
        "name": "Materials and Fuels Complex",
        "lat": 43.594,
        "lon": -112.652,
        "elevation": 1579,
        "location": "INL Site, ID",
    },
    "NRF": {
        "name": "Naval Reactors Facility",
        "lat": 43.652,
        "lon": -112.931,
        "elevation": 1484,
        "location": "INL Site, ID",
    },
    "PBF": {
        "name": "Power Burst Facility",
        "lat": 43.561,
        "lon": -112.865,
        "elevation": 1502,
        "location": "INL Site, ID",
    },
    "RWMC": {
        "name": "Radioactive Waste Management Complex",
        "lat": 43.504,
        "lon": -113.044,
        "elevation": 1527,
        "location": "INL Site, ID",
    },
    "SMC": {
        "name": "Specific Manufacturing Capability",
        "lat": 43.597,
        "lon": -112.913,
        "elevation": 1490,
        "location": "INL Site, ID",
    },
    "TAN": {
        "name": "Test Area North",
        "lat": 43.773,
        "lon": -112.710,
        "elevation": 1490,
        "location": "INL Site, ID",
    },
    "TRA": {
        "name": "Test Reactor Area",
        "lat": 43.587,
        "lon": -112.959,
        "elevation": 1494,
        "location": "INL Site, ID",
    },
    "IF": {
        "name": "Idaho Falls (NOAA)",
        "lat": 43.467,
        "lon": -112.017,
        "elevation": 1432,
        "location": "Idaho Falls, ID",
    },
    "ARCO": {
        "name": "Arco",
        "lat": 43.639,
        "lon": -113.302,
        "elevation": 1616,
        "location": "Arco, ID",
    },
}


# ---------------------------------------------------------------------------
# Helpers — Daymet CSV parser
# ---------------------------------------------------------------------------

def _parse_daymet_csv(text: str) -> tuple[dict, list[dict]]:
    """Parse Daymet single-pixel CSV response.

    Daymet CSV starts with header comment lines (starting with ``year,yday``),
    preceded by 6-7 lines of metadata comments.

    Returns
    -------
    metadata : dict
        Extracted from the comment header lines.
    rows : list[dict]
        One dict per day with year, yday, and parameter values.
    """
    metadata: dict = {}
    data_lines: list[str] = []
    header_line: Optional[str] = None

    for line in text.splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        # Daymet comment lines don't start with a standard marker but
        # the header/data section starts with "year,yday"
        if header_line is None:
            if stripped.startswith("year"):
                header_line = stripped
            else:
                # Parse metadata comments — often "key: value" or "key,value"
                if ":" in stripped:
                    k, _, v = stripped.partition(":")
                    metadata[k.strip().lstrip("#").strip()] = v.strip()
        else:
            data_lines.append(stripped)

    if header_line is None:
        return metadata, []

    columns = [c.strip() for c in header_line.split(",")]
    rows: list[dict] = []
    for line in data_lines:
        vals = line.split(",")
        row: dict = {}
        for col, val in zip(columns, vals):
            val = val.strip()
            if val == "" or val == "NA":
                row[col] = None
            else:
                try:
                    row[col] = int(val) if col in ("year", "yday") else float(val)
                except ValueError:
                    row[col] = val
        rows.append(row)

    return metadata, rows


# ---------------------------------------------------------------------------
# Endpoint: GET /api/fed_facilities/daymet
# ---------------------------------------------------------------------------

@router.get(
    "/daymet",
    summary="Daymet daily gridded data at a point",
)
async def daymet_data(
    lat: float = Query(..., ge=-90, le=90, description="Latitude"),
    lon: float = Query(..., ge=-180, le=180, description="Longitude"),
    years: str = Query(
        ...,
        description="Comma-separated years or range (e.g. '2023' or '2020,2021,2022')",
    ),
    params: str = Query(
        DEFAULT_DAYMET_PARAMS,
        description="Comma-separated Daymet variables: tmax,tmin,prcp,srad,vp,swe,dayl",
    ),
):
    """Fetch daily gridded meteorological data from ORNL Daymet.

    Daymet provides 1 km spatial resolution daily data for North America
    (continental US, Canada, Mexico, Hawaii, Puerto Rico).  Variables include
    temperature extremes, precipitation, shortwave radiation, vapor pressure,
    snow water equivalent, and day length.

    Coverage: 1980-present (1-2 year lag for most recent complete year).
    """
    # Validate params
    param_list = [p.strip() for p in params.split(",") if p.strip()]
    invalid = [p for p in param_list if p not in DAYMET_PARAMS]
    if invalid:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid Daymet parameter(s): {invalid}.  "
                   f"Valid: {list(DAYMET_PARAMS.keys())}",
        )

    cache_key = f"daymet:{lat}:{lon}:{years}:{params}"
    cached = await cache.get(cache_key)
    if cached is not None:
        return cached

    query = {
        "lat": str(lat),
        "lon": str(lon),
        "years": years,
        "measuredParams": ",".join(param_list),
    }

    async with httpx.AsyncClient(
        timeout=config.HTTP_TIMEOUT, headers=_HTTP_HEADERS
    ) as client:
        try:
            resp = await client.get(DAYMET_BASE, params=query)
            resp.raise_for_status()
        except httpx.HTTPStatusError as exc:
            logger.error("Daymet API error: %s", exc)
            raise HTTPException(
                status_code=502,
                detail=f"Daymet API returned {exc.response.status_code}: "
                       f"{exc.response.text[:500]}",
            )
        except httpx.RequestError as exc:
            logger.error("Daymet request failed: %s", exc)
            raise HTTPException(status_code=502, detail="Daymet API unreachable.")

    metadata, rows = _parse_daymet_csv(resp.text)

    if not rows:
        raise HTTPException(
            status_code=404,
            detail=f"No Daymet data returned for lat={lat}, lon={lon}, years={years}.",
        )

    result = {
        "source": SOURCE_NAME,
        "dataset": "ORNL Daymet",
        "latitude": lat,
        "longitude": lon,
        "years": years,
        "spatial_resolution": "1 km",
        "parameters_requested": param_list,
        "parameters": {
            code: {"name": name, "unit": unit}
            for code, (name, unit) in DAYMET_PARAMS.items()
            if code in param_list
        },
        "metadata": metadata,
        "record_count": len(rows),
        "data": rows,
    }

    # Daily gridded data rarely changes — cache aggressively
    await cache.set(cache_key, result, ttl=config.CACHE_TTL_LONG * 24)
    return result


# ---------------------------------------------------------------------------
# Endpoint: GET /api/fed_facilities/daymet/parameters
# ---------------------------------------------------------------------------

@router.get(
    "/daymet/parameters",
    summary="List available Daymet parameters",
)
async def daymet_parameters():
    """Return the catalog of Daymet variables with descriptions and units."""
    return {
        "source": SOURCE_NAME,
        "dataset": "ORNL Daymet",
        "parameter_count": len(DAYMET_PARAMS),
        "parameters": {
            code: {"name": name, "unit": unit}
            for code, (name, unit) in DAYMET_PARAMS.items()
        },
    }


# ---------------------------------------------------------------------------
# Endpoint: GET /api/fed_facilities/inl/stations
# ---------------------------------------------------------------------------

@router.get(
    "/inl/stations",
    summary="List INL/NOAA mesonet stations",
)
async def inl_stations():
    """Return all known INL/NOAA Mesonet stations in SE Idaho.

    The INL Mesonet is operated jointly by DOE and NOAA's Air Resources
    Laboratory.  It provides 5-minute surface meteorological observations
    from 35 stations across the Idaho National Laboratory site and
    surrounding area.  Data feeds into MADIS.
    """
    stations = []
    for code, info in INL_STATIONS.items():
        stations.append(
            Station(
                id=f"INL_{code}",
                name=info["name"],
                lat=info["lat"],
                lon=info["lon"],
                source="fed_facilities_inl",
                params=["temperature", "wind_speed", "wind_direction",
                        "humidity", "pressure", "precipitation"],
                elevation=info.get("elevation"),
                url=INL_OBS_URL,
                extra={
                    "location": info.get("location", ""),
                    "network": "INL/NOAA Mesonet",
                    "update_frequency": "5-minute",
                    "feeds_madis": True,
                },
            )
        )
    return {
        "source": SOURCE_NAME,
        "network": "INL/NOAA Mesonet",
        "station_count": len(stations),
        "note": "Current observations available at the INL website; "
                "data also accessible via MADIS.",
        "obs_url": INL_OBS_URL,
        "stations": [s.model_dump() for s in stations],
    }


# ---------------------------------------------------------------------------
# Endpoint: GET /api/fed_facilities/bnl
# ---------------------------------------------------------------------------

@router.get(
    "/bnl",
    summary="BNL Meteorological Services overview",
)
async def bnl_overview():
    """Return metadata about the Brookhaven National Laboratory met tower.

    BNL operates an 85-meter instrumented tower with continuous meteorological
    monitoring since 1949.  Data is available as CSV downloads, one parameter
    and up to one year at a time.  Parameters include wind (multiple levels),
    temperature, humidity, pressure, precipitation, and solar radiation.
    """
    return {
        "source": SOURCE_NAME,
        "facility": "Brookhaven National Laboratory",
        "dataset": "BNL Meteorological Services",
        "location": {
            "lat": 40.868,
            "lon": -72.886,
            "elevation_m": 26,
            "site": "Upton, NY",
        },
        "tower_height_m": 85,
        "data_start": 1949,
        "parameters": [
            "Wind speed (multiple levels to 85m)",
            "Wind direction (multiple levels to 85m)",
            "Temperature (multiple levels)",
            "Relative humidity",
            "Barometric pressure",
            "Precipitation",
            "Solar radiation",
        ],
        "access": {
            "type": "web_download",
            "url": BNL_DOWNLOAD_URL,
            "format": "CSV",
            "limitation": "Max 1 year per parameter per download",
            "auth_required": False,
        },
        "note": "One of the longest continuous met records at a U.S. national lab.",
    }


# ---------------------------------------------------------------------------
# Endpoint: GET /api/fed_facilities/ksc
# ---------------------------------------------------------------------------

@router.get(
    "/ksc",
    summary="KSC Weather Archive overview",
)
async def ksc_overview():
    """Return metadata about the Kennedy Space Center Weather Archive.

    The KSC/Cape Canaveral weather network includes 46 sensor locations
    providing wind, temperature, pressure, lightning, and precipitation data
    critical for launch operations.  Historical data available through the
    web archive search interface.
    """
    return {
        "source": SOURCE_NAME,
        "facility": "Kennedy Space Center / Cape Canaveral",
        "dataset": "KSC Weather Archive",
        "location": {
            "lat": 28.524,
            "lon": -80.651,
            "site": "Cape Canaveral, FL",
        },
        "sensor_locations": 46,
        "parameters": [
            "Wind speed and direction (tower network)",
            "Temperature",
            "Pressure",
            "Precipitation",
            "Lightning detection",
            "Upper-air (rawinsonde)",
        ],
        "access": {
            "type": "web_search",
            "url": KSC_ARCHIVE_URL,
            "auth_required": False,
            "note": "Data accessible through web search interface; "
                    "no bulk download API.",
        },
        "note": "Dense coastal observation network supporting NASA launch operations.",
    }


# ---------------------------------------------------------------------------
# Endpoint: GET /api/fed_facilities/lanl
# ---------------------------------------------------------------------------

@router.get(
    "/lanl",
    summary="LANL Weather Machine overview",
)
async def lanl_overview():
    """Return metadata about the Los Alamos National Laboratory Weather Machine.

    LANL maintains a meteorological monitoring network across the Los Alamos
    area for environmental surveillance and emergency response.  Data includes
    wind, temperature, humidity, pressure, precipitation, and solar radiation.
    """
    return {
        "source": SOURCE_NAME,
        "facility": "Los Alamos National Laboratory",
        "dataset": "LANL Weather Machine",
        "location": {
            "lat": 35.878,
            "lon": -106.306,
            "elevation_m": 2231,
            "site": "Los Alamos, NM",
        },
        "parameters": [
            "Wind speed and direction",
            "Temperature",
            "Relative humidity",
            "Barometric pressure",
            "Precipitation",
            "Solar radiation",
        ],
        "access": {
            "type": "web_interface",
            "url": LANL_WEATHER_URL,
            "auth_required": False,
        },
        "note": "Operated by LANL Environmental Protection and Compliance Division.",
    }


# ---------------------------------------------------------------------------
# Endpoint: GET /api/fed_facilities/nps
# ---------------------------------------------------------------------------

@router.get(
    "/nps",
    summary="NPS Inventory & Monitoring climate data overview",
)
async def nps_overview():
    """Return metadata about National Park Service climate monitoring.

    The NPS Inventory & Monitoring (I&M) program collects climate and weather
    data across 32 monitoring networks spanning ~270 national parks.  Data is
    published through the IRMA DataStore, which provides both a web interface
    and REST services.  Parameters include air temperature, precipitation,
    humidity, wind, and derived climate indices.
    """
    return {
        "source": SOURCE_NAME,
        "facility": "National Park Service",
        "dataset": "NPS Inventory & Monitoring Climate Data",
        "monitoring_networks": 32,
        "approximate_parks": 270,
        "parameters": [
            "Air temperature (min, max, mean)",
            "Precipitation",
            "Relative humidity",
            "Wind speed and direction",
            "Solar radiation",
            "Snow depth / SWE (selected parks)",
        ],
        "access": {
            "data_store": {
                "url": NPS_IRMA_URL,
                "type": "web_interface",
                "auth_required": False,
            },
            "rest_services": {
                "url": NPS_SERVICES_URL,
                "type": "REST API",
                "auth_required": False,
                "note": "IRMA REST services for programmatic access to datasets.",
            },
        },
        "note": "Comprehensive climate monitoring supporting natural resource "
                "management across the National Park System.",
    }


# ---------------------------------------------------------------------------
# Endpoint: GET /api/fed_facilities/overview
# ---------------------------------------------------------------------------

@router.get(
    "/overview",
    summary="Federal Facility Weather — source information",
)
async def fed_facilities_overview():
    """Return metadata about all federal facility weather data sources."""
    return {
        "source": SOURCE_NAME,
        "description": SOURCE_DESCRIPTION,
        "auth_required": False,
        "facilities": [
            {
                "name": "INL/NOAA Mesonet",
                "type": "operational_network",
                "stations": 35,
                "frequency": "5-minute",
                "location": "Idaho National Laboratory, SE Idaho",
                "url": INL_OBS_URL,
                "has_api": False,
                "feeds_madis": True,
            },
            {
                "name": "BNL Meteorological Services",
                "type": "single_tower",
                "stations": 1,
                "frequency": "Continuous (downloadable as hourly/daily)",
                "location": "Brookhaven National Lab, Upton, NY",
                "url": BNL_DOWNLOAD_URL,
                "has_api": False,
                "data_start": 1949,
            },
            {
                "name": "ORNL Daymet",
                "type": "gridded_analysis",
                "spatial_resolution": "1 km",
                "frequency": "Daily",
                "coverage": "North America",
                "url": "https://daymet.ornl.gov/",
                "has_api": True,
                "api_endpoint": "/api/fed_facilities/daymet",
            },
            {
                "name": "KSC Weather Archive",
                "type": "operational_network",
                "stations": 46,
                "location": "Cape Canaveral, FL",
                "url": KSC_ARCHIVE_URL,
                "has_api": False,
            },
            {
                "name": "LANL Weather Machine",
                "type": "operational_network",
                "location": "Los Alamos, NM",
                "url": LANL_WEATHER_URL,
                "has_api": False,
            },
            {
                "name": "NPS I&M Climate Data",
                "type": "monitoring_program",
                "networks": 32,
                "approximate_parks": 270,
                "url": NPS_IRMA_URL,
                "rest_services": NPS_SERVICES_URL,
                "has_api": True,
            },
        ],
        "endpoints": [
            {"path": "/api/fed_facilities/daymet", "method": "GET",
             "description": "Daymet daily gridded data at a point"},
            {"path": "/api/fed_facilities/daymet/parameters", "method": "GET",
             "description": "List Daymet parameters"},
            {"path": "/api/fed_facilities/inl/stations", "method": "GET",
             "description": "List INL/NOAA mesonet stations"},
            {"path": "/api/fed_facilities/bnl", "method": "GET",
             "description": "BNL met tower overview"},
            {"path": "/api/fed_facilities/ksc", "method": "GET",
             "description": "KSC Weather Archive overview"},
            {"path": "/api/fed_facilities/lanl", "method": "GET",
             "description": "LANL Weather Machine overview"},
            {"path": "/api/fed_facilities/nps", "method": "GET",
             "description": "NPS I&M climate data overview"},
            {"path": "/api/fed_facilities/overview", "method": "GET",
             "description": "Source information (all facilities)"},
        ],
    }
