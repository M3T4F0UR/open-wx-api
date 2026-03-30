"""MADIS Public Mesonet netCDF — catalog of NCEP public mesonet archives.

MADIS (Meteorological Assimilation Data Ingest System) publishes hourly mesonet
observations as compressed netCDF files on a public HTTP directory.  These files
contain ALL mesonet stations ingested by MADIS including utility networks:

  - PG&E (1,561 stations), SCE (1,921), SDG&E (215)
  - State DOT, RAWS, CoCoRaHS, universities, and many more

Files are ~30 MB compressed / ~420 MB uncompressed netCDF4.  A 5-day rolling
window is maintained.  Because the files are large and require netCDF4 or scipy
to parse, this module only catalogs what is available — it does not download or
parse the data.  For real-time station-level JSON access, use:

  - IEM DCP (``/api/iem_dcp``) — free, no auth, covers all MADIS mesonet stations
  - SDG&E Weather API — if available separately

URL: https://madis-data.ncep.noaa.gov/madisPublic1/data/LDAD/mesonet/netCDF/
"""
from __future__ import annotations

import logging
import re
from typing import Optional

from fastapi import APIRouter, HTTPException, Query

import http_client

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Source registry metadata  (read by server.py /api/sources)
# ---------------------------------------------------------------------------

SOURCE_NAME = "MADIS Public Mesonet"
SOURCE_DESCRIPTION = (
    "NCEP MADIS public mesonet archive — hourly compressed netCDF files "
    "containing all MADIS mesonet stations (utility networks like PG&E, SCE, "
    "SDG&E, plus RAWS, DOT, university, and other providers).  5-day rolling "
    "window, ~30 MB/file compressed.  This module catalogs available files; "
    "use IEM DCP or SDG&E Weather API for parsed JSON station data."
)
SOURCE_META = {
    "url": "https://madis-data.ncep.noaa.gov/madisPublic1/data/LDAD/mesonet/netCDF/",
    "auth_required": False,
    "update_frequency": "Hourly",
    "coverage": "United States (all MADIS mesonet providers)",
    "params": [
        "temperature", "dewpoint", "relHumidity", "windDir", "windSpeed",
        "windGust", "solarRadiation", "precipAccum",
    ],
    "category": "weather",
    "notes": "Files are large netCDF (~30 MB gz / ~420 MB uncompressed). "
             "This source catalogs files only — use IEM DCP for parsed data.",
}

router = APIRouter(prefix="/api/madis_public", tags=["MADIS Public Mesonet"])

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

MADIS_DIR_URL = (
    "https://madis-data.ncep.noaa.gov/madisPublic1/data/LDAD/mesonet/netCDF/"
)

# Matches filenames like 20260320_1200.gz
_FILE_PATTERN = re.compile(r"(\d{8}_\d{4})\.gz")

# Known data providers in MADIS mesonet files
_KNOWN_PROVIDERS = [
    {"id": "PGE", "name": "Pacific Gas & Electric", "approx_stations": 1561},
    {"id": "SCE", "name": "Southern California Edison", "approx_stations": 1921},
    {"id": "SDGE", "name": "San Diego Gas & Electric", "approx_stations": 215},
    {"id": "RAWS", "name": "Remote Automated Weather Stations", "approx_stations": None},
    {"id": "DOT", "name": "State Departments of Transportation", "approx_stations": None},
    {"id": "NWS-COOP", "name": "NWS Cooperative Observer Program", "approx_stations": None},
    {"id": "AWOS", "name": "Automated Weather Observing System", "approx_stations": None},
    {"id": "CoCoRaHS", "name": "Community Collaborative Rain/Hail/Snow", "approx_stations": None},
]

# netCDF variable documentation
_VARIABLES = {
    "temperature": {"units": "K", "description": "Air temperature at station height"},
    "dewpoint": {"units": "K", "description": "Dewpoint temperature"},
    "relHumidity": {"units": "%", "description": "Relative humidity"},
    "windDir": {"units": "degrees", "description": "Wind direction (meteorological, from)"},
    "windSpeed": {"units": "m/s", "description": "Sustained wind speed"},
    "windGust": {"units": "m/s", "description": "Wind gust speed"},
    "solarRadiation": {"units": "W/m²", "description": "Incoming solar radiation"},
    "precipAccum": {"units": "mm", "description": "Accumulated precipitation"},
    "latitude": {"units": "degrees_north", "description": "Station latitude"},
    "longitude": {"units": "degrees_east", "description": "Station longitude"},
    "observationTime": {"units": "seconds since 1970-01-01", "description": "Unix epoch observation time"},
    "stationName": {"units": "string", "description": "Station identifier"},
    "dataProvider": {"units": "string", "description": "Contributing network / provider name"},
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

async def _fetch_file_list() -> list[dict]:
    """Parse the MADIS HTTP directory listing and return sorted file entries."""
    html = await http_client.fetch_text(MADIS_DIR_URL, source="madis_public")
    if not html:
        raise HTTPException(status_code=502, detail="Could not fetch MADIS directory listing")

    files = []
    for match in _FILE_PATTERN.finditer(html):
        filename = match.group(0)       # e.g. 20260320_1200.gz
        timestamp = match.group(1)      # e.g. 20260320_1200
        files.append({
            "filename": filename,
            "timestamp": timestamp,
            "url": f"{MADIS_DIR_URL}{filename}",
        })

    # Sort chronologically (filenames sort naturally)
    files.sort(key=lambda f: f["timestamp"])
    return files


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@router.get("/files", summary="List available MADIS mesonet netCDF files")
async def list_files(
    limit: int = Query(default=0, ge=0, description="Max files to return (0 = all)"),
):
    """Parse the MADIS public directory listing and return available compressed
    netCDF files.  Files follow the naming pattern ``YYYYMMDD_HHMM.gz`` and
    cover a 5-day rolling window (~120 files).

    Each entry includes the filename, timestamp, and direct download URL.
    Files are ~30 MB compressed (~420 MB uncompressed netCDF4).
    """
    files = await _fetch_file_list()
    if limit > 0:
        files = files[-limit:]
    return {
        "source": SOURCE_NAME,
        "directory_url": MADIS_DIR_URL,
        "file_count": len(files),
        "files": files,
    }


@router.get("/latest_file", summary="URL to the most recent MADIS mesonet file")
async def latest_file():
    """Return the most recent available MADIS mesonet netCDF file.

    Provides the filename, timestamp, and direct download URL.  The file is
    typically ~30 MB compressed and requires netCDF4 or scipy to parse.
    """
    files = await _fetch_file_list()
    if not files:
        raise HTTPException(status_code=404, detail="No MADIS mesonet files found")
    latest = files[-1]
    return {
        "source": SOURCE_NAME,
        "latest": latest,
        "note": (
            "File is ~30 MB gzip-compressed netCDF4 (~420 MB uncompressed).  "
            "Requires netCDF4 or scipy to parse.  For JSON station data, "
            "use /api/iem_dcp endpoints instead."
        ),
    }


@router.get("/overview", summary="MADIS mesonet source overview and format docs")
async def overview():
    """Comprehensive overview of the MADIS public mesonet data source.

    Includes file format documentation, netCDF variable descriptions,
    known data providers, and recommendations for accessing station data.
    """
    return {
        "source": SOURCE_NAME,
        "description": SOURCE_DESCRIPTION,
        "directory_url": MADIS_DIR_URL,
        "auth_required": False,
        "file_format": {
            "naming": "YYYYMMDD_HHMM.gz",
            "compression": "gzip",
            "format": "netCDF4",
            "approx_size_compressed": "~30 MB",
            "approx_size_uncompressed": "~420 MB",
            "rolling_window": "5 days",
            "frequency": "hourly",
        },
        "variables": _VARIABLES,
        "known_providers": _KNOWN_PROVIDERS,
        "access_recommendations": [
            {
                "source": "IEM DCP",
                "endpoint": "/api/iem_dcp",
                "description": (
                    "Iowa Environmental Mesonet DCP networks — free JSON API "
                    "providing the same MADIS mesonet stations without needing "
                    "to download/parse large netCDF files."
                ),
            },
            {
                "source": "SDG&E Weather API",
                "description": (
                    "If available separately, provides direct JSON access "
                    "to SDG&E utility weather station observations."
                ),
            },
        ],
    }
