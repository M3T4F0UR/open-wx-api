"""NREL MIDC — Measurement & Instrumentation Data Center.

NREL's MIDC provides high-quality solar radiation and meteorological data from
research-grade instruments at multiple sites across the United States.  130+
data elements at 1-minute resolution including GHI, DNI, DHI irradiance, UV,
IR, spectral, temperature, humidity, pressure, precipitation, snow depth, and
wind.

API: https://midcdmz.nrel.gov/
No authentication required.  REST CSV responses.
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

SOURCE_NAME = "NREL MIDC Solar & Met"
SOURCE_DESCRIPTION = (
    "NREL Measurement & Instrumentation Data Center (MIDC).  Research-grade "
    "solar radiation and meteorological observations at 1-minute resolution.  "
    "130+ data elements including GHI, DNI, DHI irradiance, UV, IR, spectral, "
    "temperature, humidity, pressure, precipitation, snow depth, and wind.  "
    "No authentication required."
)
SOURCE_META = {
    "url": "https://midcdmz.nrel.gov/",
    "auth_required": False,
    "update_frequency": "1-minute",
    "coverage": "United States (NREL research sites)",
    "params": [
        "ghi", "dni", "dhi", "temperature", "humidity",
        "pressure", "wind_speed", "wind_direction", "precipitation",
        "uv", "infrared", "snow_depth",
    ],
    "category": "solar",
}

router = APIRouter(prefix="/api/nrel", tags=["NREL MIDC"])

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

MIDC_BASE = "https://midcdmz.nrel.gov"

_HTTP_HEADERS = {"User-Agent": config.USER_AGENT}

# ---------------------------------------------------------------------------
# MIDC station catalog
# ---------------------------------------------------------------------------
# Sourced from https://midcdmz.nrel.gov/ station listing.
# Each entry: site code -> (name, lat, lon, elevation_m, description)

MIDC_STATIONS: dict[str, dict] = {
    "SRRL": {
        "name": "Solar Radiation Research Laboratory",
        "lat": 39.742,
        "lon": -105.18,
        "elevation": 1829,
        "location": "Golden, CO",
        "description": "NREL's flagship solar measurement facility on South Table Mountain.",
    },
    "BMS": {
        "name": "Baseline Measurement System",
        "lat": 39.742,
        "lon": -105.18,
        "elevation": 1829,
        "location": "Golden, CO",
        "description": "Baseline broadband solar and meteorological measurements at NREL.",
    },
    "STAC": {
        "name": "South Table Mountain Atmospheric Corrosion",
        "lat": 39.742,
        "lon": -105.18,
        "elevation": 1829,
        "location": "Golden, CO",
        "description": "Atmospheric corrosion test site co-located with SRRL.",
    },
    "NWTC": {
        "name": "National Wind Technology Center",
        "lat": 39.913,
        "lon": -105.234,
        "elevation": 1855,
        "location": "Boulder, CO",
        "description": "Wind energy research center with meteorological instruments.",
    },
    "UNLV": {
        "name": "University of Nevada Las Vegas",
        "lat": 36.114,
        "lon": -115.147,
        "elevation": 615,
        "location": "Las Vegas, NV",
        "description": "Desert solar radiation monitoring station.",
    },
    "ORNL": {
        "name": "Oak Ridge National Laboratory",
        "lat": 35.932,
        "lon": -84.317,
        "elevation": 275,
        "location": "Oak Ridge, TN",
        "description": "Solar and meteorological station at ORNL campus.",
    },
    "UOSMRL": {
        "name": "University of Oregon Solar Monitoring Lab",
        "lat": 44.045,
        "lon": -123.075,
        "elevation": 134,
        "location": "Eugene, OR",
        "description": "Long-running solar radiation monitoring program.",
    },
    "SOLP": {
        "name": "Solar Technology Acceleration Center",
        "lat": 38.83,
        "lon": -106.86,
        "elevation": 2670,
        "location": "Buena Vista, CO",
        "description": "High-altitude solar test site in the Colorado Rockies.",
    },
    "EPRI": {
        "name": "EPRI Knoxville",
        "lat": 35.935,
        "lon": -83.926,
        "elevation": 274,
        "location": "Knoxville, TN",
        "description": "Electric Power Research Institute solar monitoring site.",
    },
    "SGPN": {
        "name": "Southern Great Plains Norman",
        "lat": 35.237,
        "lon": -97.462,
        "elevation": 357,
        "location": "Norman, OK",
        "description": "ARM Southern Great Plains extended facility.",
    },
    "FINGERLAKES": {
        "name": "Finger Lakes Community College",
        "lat": 42.856,
        "lon": -77.244,
        "elevation": 232,
        "location": "Canandaigua, NY",
        "description": "Solar monitoring in the Finger Lakes region of New York.",
    },
    "SSIM": {
        "name": "Sandia Solar Irradiance Monitor",
        "lat": 35.052,
        "lon": -106.543,
        "elevation": 1617,
        "location": "Albuquerque, NM",
        "description": "Sandia National Laboratories solar irradiance station.",
    },
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _parse_csv_data(text: str) -> list[dict]:
    """Parse MIDC CSV response into a list of row dicts.

    MIDC CSV has a header row with column names.  Values may contain
    empty strings for missing data.
    """
    rows: list[dict] = []
    reader = csv.DictReader(io.StringIO(text))
    for row in reader:
        cleaned: dict = {}
        for key, val in row.items():
            if key is None:
                continue
            key = key.strip()
            if not key:
                continue
            # Try numeric conversion
            if val is not None:
                val = val.strip()
                if val == "" or val == "-":
                    cleaned[key] = None
                else:
                    try:
                        cleaned[key] = float(val)
                    except ValueError:
                        cleaned[key] = val
            else:
                cleaned[key] = None
        if cleaned:
            rows.append(cleaned)
    return rows


def _format_date(date_str: str) -> str:
    """Normalise user date input to YYYYMMDD for MIDC URLs.

    Accepts YYYY-MM-DD or YYYYMMDD.
    """
    return date_str.replace("-", "")


# ---------------------------------------------------------------------------
# Endpoint: GET /api/nrel/stations
# ---------------------------------------------------------------------------

@router.get(
    "/stations",
    summary="List NREL MIDC stations",
)
async def nrel_stations():
    """Return all known NREL MIDC monitoring stations.

    Each station includes its site code, name, coordinates, elevation,
    and a brief description.  Use the site code in the ``site`` parameter
    of the ``/api/nrel/data`` endpoint.
    """
    stations = []
    for code, info in MIDC_STATIONS.items():
        stations.append(
            Station(
                id=code,
                name=info["name"],
                lat=info["lat"],
                lon=info["lon"],
                source="nrel_midc",
                params=SOURCE_META["params"],
                elevation=info.get("elevation"),
                url=f"{MIDC_BASE}/apps/sitehome.pl?site={code}",
                extra={
                    "location": info.get("location", ""),
                    "description": info.get("description", ""),
                },
            )
        )
    return {
        "source": SOURCE_NAME,
        "station_count": len(stations),
        "stations": [s.model_dump() for s in stations],
    }


# ---------------------------------------------------------------------------
# Endpoint: GET /api/nrel/data
# ---------------------------------------------------------------------------

@router.get(
    "/data",
    summary="Daily observation data for a MIDC station",
)
async def nrel_data(
    site: str = Query(..., description="MIDC site code (e.g. SRRL, BMS, NWTC)"),
    date: str = Query(
        ...,
        description="Date as YYYY-MM-DD or YYYYMMDD",
    ),
):
    """Fetch one day of observation data from an NREL MIDC station.

    Returns CSV-parsed rows at the native time resolution (typically
    1-minute for most stations).  Each row includes all data elements
    available at that station — commonly GHI, DNI, DHI, temperature,
    humidity, wind speed/direction, pressure, and more.
    """
    site = site.upper()
    date_clean = _format_date(date)

    # Validate date format
    try:
        datetime.strptime(date_clean, "%Y%m%d")
    except ValueError:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid date format: {date!r}.  Use YYYY-MM-DD or YYYYMMDD.",
        )

    cache_key = f"nrel_midc:data:{site}:{date_clean}"
    cached = await cache.get(cache_key)
    if cached is not None:
        return cached

    url = f"{MIDC_BASE}/apps/data_api.pl"
    params = {
        "site": site,
        "begin": date_clean,
        "end": date_clean,
    }

    async with httpx.AsyncClient(timeout=config.HTTP_TIMEOUT, headers=_HTTP_HEADERS) as client:
        try:
            resp = await client.get(url, params=params)
            resp.raise_for_status()
        except httpx.HTTPStatusError as exc:
            logger.error("NREL MIDC data error for %s: %s", site, exc)
            raise HTTPException(
                status_code=502,
                detail=f"NREL MIDC API returned {exc.response.status_code}.",
            )
        except httpx.RequestError as exc:
            logger.error("NREL MIDC request failed: %s", exc)
            raise HTTPException(status_code=502, detail="NREL MIDC API unreachable.")

    text = resp.text
    if not text.strip():
        raise HTTPException(
            status_code=404,
            detail=f"No data returned for site {site} on {date}.",
        )

    rows = _parse_csv_data(text)
    if not rows:
        raise HTTPException(
            status_code=404,
            detail=f"No parseable data for site {site} on {date}.",
        )

    # Extract column names from the first row
    columns = list(rows[0].keys()) if rows else []

    result = {
        "source": SOURCE_NAME,
        "site": site,
        "site_name": MIDC_STATIONS.get(site, {}).get("name", site),
        "date": date_clean,
        "record_count": len(rows),
        "columns": columns,
        "data": rows,
    }

    await cache.set(cache_key, result, ttl=config.CACHE_TTL_LONG)
    return result


# ---------------------------------------------------------------------------
# Endpoint: GET /api/nrel/overview
# ---------------------------------------------------------------------------

@router.get(
    "/overview",
    summary="NREL MIDC source information",
)
async def nrel_overview():
    """Return metadata about the NREL MIDC data source."""
    return {
        "source": SOURCE_NAME,
        "description": SOURCE_DESCRIPTION,
        "auth_required": False,
        "api_base": MIDC_BASE,
        "station_count": len(MIDC_STATIONS),
        "resolution": "1-minute (most stations)",
        "data_elements": "130+",
        "element_categories": [
            "Global Horizontal Irradiance (GHI)",
            "Direct Normal Irradiance (DNI)",
            "Diffuse Horizontal Irradiance (DHI)",
            "Ultraviolet (UV-A, UV-B)",
            "Infrared (downwelling, upwelling)",
            "Spectral irradiance",
            "Temperature (air, dewpoint, wet bulb)",
            "Relative humidity",
            "Barometric pressure",
            "Wind speed & direction",
            "Precipitation",
            "Snow depth",
        ],
        "coverage": "United States (NREL research sites)",
        "documentation": "https://midcdmz.nrel.gov/",
        "endpoints": [
            {"path": "/api/nrel/stations", "method": "GET", "description": "List MIDC stations"},
            {"path": "/api/nrel/data", "method": "GET", "description": "Daily observation data for a station"},
            {"path": "/api/nrel/overview", "method": "GET", "description": "Source information"},
        ],
    }
