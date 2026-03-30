"""NOAA Radiation Networks — SURFRAD & SOLRAD.

Two surface radiation monitoring networks, no authentication required:

1. SURFRAD (Surface Radiation Budget Network) — 7 stations, 1-min data
   High-quality measurements of downwelling/upwelling shortwave & longwave
   radiation, direct normal, diffuse, UV, plus standard met (T, RH, wind).
   URL: https://gml.noaa.gov/grad/surfrad/
   Data: https://gml.noaa.gov/aftp/data/radiation/surfrad/

2. SOLRAD (Solar Radiation Network) — ~15 stations
   Downwelling solar radiation for NWS forecast verification.
   URL: https://gml.noaa.gov/grad/solrad/

All data is publicly available via HTTP (FTP-style directory listings).
"""
from __future__ import annotations

import logging
import re
from datetime import datetime, timezone
from typing import Any, Optional

from fastapi import APIRouter, HTTPException, Query

import cache
import http_client

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Source registry metadata  (read by server.py /api/sources)
# ---------------------------------------------------------------------------

SOURCE_NAME = "NOAA Radiation Networks"
SOURCE_DESCRIPTION = (
    "Surface radiation measurements from NOAA's SURFRAD (7 stations, 1-min "
    "broadband radiation budget) and SOLRAD (~15 stations, solar irradiance) "
    "networks.  No authentication required."
)
SOURCE_META = {
    "url": "https://gml.noaa.gov/grad/surfrad/",
    "auth_required": False,
    "update_frequency": "1-minute (SURFRAD), 1-3 minute (SOLRAD)",
    "coverage": "Continental US — 7 SURFRAD + ~15 SOLRAD stations",
    "params": [
        "downwelling_shortwave", "upwelling_shortwave",
        "downwelling_longwave", "upwelling_longwave",
        "direct_normal", "diffuse_horizontal",
        "temperature", "relative_humidity", "wind_speed", "pressure",
    ],
    "category": "radiation",
}

router = APIRouter(prefix="/api/radiation", tags=["NOAA Radiation Networks"])

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

SURFRAD_BASE = "https://gml.noaa.gov/aftp/data/radiation/surfrad"
SOLRAD_BASE = "https://gml.noaa.gov/aftp/data/radiation/solrad"

SOURCE = "radiation"

# Cache TTLs (seconds)
TTL_LATEST = 300      # 5 min — data arrives every 1 min but we don't need to hammer
TTL_STATIONS = 86400  # 24 h — station list is static
TTL_OVERVIEW = 600    # 10 min

# ---------------------------------------------------------------------------
# SURFRAD station metadata
# ---------------------------------------------------------------------------

SURFRAD_STATIONS: dict[str, dict[str, Any]] = {
    "bon": {
        "name": "Bondville",
        "state": "IL",
        "lat": 40.0519,
        "lon": -88.3731,
        "elevation_m": 213,
        "description": "Bondville, Illinois — agricultural flatland",
    },
    "fpk": {
        "name": "Fort Peck",
        "state": "MT",
        "lat": 48.3078,
        "lon": -105.1017,
        "elevation_m": 634,
        "description": "Fort Peck, Montana — northern Great Plains grassland",
    },
    "gwn": {
        "name": "Goodwin Creek",
        "state": "MS",
        "lat": 34.2547,
        "lon": -89.8729,
        "elevation_m": 98,
        "description": "Goodwin Creek, Mississippi — pasture/forest mix",
    },
    "tbl": {
        "name": "Table Mountain",
        "state": "CO",
        "lat": 40.1253,
        "lon": -105.2368,
        "elevation_m": 1689,
        "description": "Table Mountain, Colorado — Front Range foothill grassland",
    },
    "dra": {
        "name": "Desert Rock",
        "state": "NV",
        "lat": 36.6233,
        "lon": -116.0194,
        "elevation_m": 1007,
        "description": "Desert Rock, Nevada — arid Mojave desert scrub",
    },
    "psu": {
        "name": "Penn State",
        "state": "PA",
        "lat": 40.7203,
        "lon": -77.9310,
        "elevation_m": 376,
        "description": "Penn State University, Pennsylvania — mixed agriculture/forest",
    },
    "sxf": {
        "name": "Sioux Falls",
        "state": "SD",
        "lat": 43.7340,
        "lon": -96.6233,
        "elevation_m": 473,
        "description": "Sioux Falls, South Dakota — prairie grassland",
    },
}

# SURFRAD daily-file column layout (space-delimited, fixed-width).
# Columns (0-indexed after year/jday/month/day/hour/min):
#   6  dt (decimal time, hours)
#   7  zen (solar zenith angle, degrees)
#   8  dw_solar (downwelling shortwave, W/m²)
#   9  dw_solar_qc
#  10  uw_solar (upwelling shortwave, W/m²)
#  11  uw_solar_qc
#  12  direct_n (direct normal, W/m²)
#  13  direct_n_qc
#  14  diffuse (diffuse horizontal, W/m²)
#  15  diffuse_qc
#  16  dw_ir (downwelling longwave, W/m²)
#  17  dw_ir_qc
#  18  dw_casetemp (case temp, K)
#  19  dw_dometemp (dome temp, K)
#  20  uw_ir (upwelling longwave, W/m²)
#  21  uw_ir_qc
#  22  uw_casetemp (case temp, K)
#  23  uw_dometemp (dome temp, K)
#  24  uvb (UV-B, milliWatts/m²)
#  25  uvb_qc
#  26  par (photosynthetically active, W/m²)
#  27  par_qc
#  28  netsolar (net shortwave, W/m²)
#  29  netsolar_qc
#  30  netir (net longwave, W/m²)
#  31  netir_qc
#  32  totalnet (total net radiation, W/m²)
#  33  totalnet_qc
#  34  temp (air temperature, °C)
#  35  temp_qc
#  36  rh (relative humidity, %)
#  37  rh_qc
#  38  windspd (wind speed, m/s)
#  39  windspd_qc
#  40  winddir (wind direction, degrees)
#  41  winddir_qc
#  42  pressure (station pressure, mb)
#  43  pressure_qc

# QC flag meanings: 0 = good, 1 = estimated, 2 = questionable, >2 = bad/missing
_QC_LABELS = {0: "good", 1: "estimated", 2: "questionable"}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _parse_surfrad_line(line: str) -> Optional[dict[str, Any]]:
    """Parse a single data line from a SURFRAD daily file.

    Returns None if the line is a header or malformed.
    """
    parts = line.split()
    if len(parts) < 44:
        return None

    try:
        year = int(parts[0])
        jday = int(parts[1])
        month = int(parts[2])
        day = int(parts[3])
        hour = int(parts[4])
        minute = int(parts[5])
    except (ValueError, IndexError):
        return None

    # Sanity check — skip header-like rows
    if year < 1990 or year > 2100:
        return None

    def _fval(idx: int) -> Optional[float]:
        """Extract float, returning None for missing sentinel -9999.9."""
        try:
            v = float(parts[idx])
            return None if v < -9000 else v
        except (ValueError, IndexError):
            return None

    def _qc(idx: int) -> str:
        try:
            return _QC_LABELS.get(int(parts[idx]), "bad")
        except (ValueError, IndexError):
            return "unknown"

    timestamp = f"{year:04d}-{month:02d}-{day:02d}T{hour:02d}:{minute:02d}:00Z"

    return {
        "timestamp": timestamp,
        "year": year,
        "jday": jday,
        "solar_zenith_deg": _fval(7),
        "downwelling_shortwave_wm2": _fval(8),
        "downwelling_shortwave_qc": _qc(9),
        "upwelling_shortwave_wm2": _fval(10),
        "upwelling_shortwave_qc": _qc(11),
        "direct_normal_wm2": _fval(12),
        "direct_normal_qc": _qc(13),
        "diffuse_horizontal_wm2": _fval(14),
        "diffuse_horizontal_qc": _qc(15),
        "downwelling_longwave_wm2": _fval(16),
        "downwelling_longwave_qc": _qc(17),
        "upwelling_longwave_wm2": _fval(20),
        "upwelling_longwave_qc": _qc(21),
        "uvb_mwm2": _fval(24),
        "uvb_qc": _qc(25),
        "par_wm2": _fval(26),
        "par_qc": _qc(27),
        "net_shortwave_wm2": _fval(28),
        "net_longwave_wm2": _fval(30),
        "total_net_wm2": _fval(32),
        "temperature_c": _fval(34),
        "temperature_qc": _qc(35),
        "relative_humidity_pct": _fval(36),
        "relative_humidity_qc": _qc(37),
        "wind_speed_ms": _fval(38),
        "wind_speed_qc": _qc(39),
        "wind_direction_deg": _fval(40),
        "pressure_mb": _fval(42),
        "pressure_qc": _qc(43),
    }


def _build_surfrad_daily_url(station: str, dt: datetime) -> str:
    """Build the URL for a SURFRAD daily data file.

    Files are organized as: /surfrad/{station}/{year}/{station}{yy}{jday}.dat
    Example: /surfrad/bon/2026/bon26079.dat  (Bondville, 2026, day 079)
    """
    yy = dt.strftime("%y")
    jday = dt.strftime("%j")
    return f"{SURFRAD_BASE}/{station}/{dt.year}/{station}{yy}{jday}.dat"


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@router.get("/surfrad/stations")
async def list_surfrad_stations():
    """List all 7 SURFRAD stations with coordinates and metadata.

    The Surface Radiation Budget Network provides high-quality broadband
    radiation measurements (shortwave, longwave, UV-B, PAR) plus standard
    meteorological observations at 1-minute resolution.
    """
    stations = []
    for code, info in SURFRAD_STATIONS.items():
        stations.append({
            "station_id": code,
            **info,
            "data_url": f"{SURFRAD_BASE}/{code}/",
        })
    return {
        "source": "NOAA SURFRAD",
        "network": "Surface Radiation Budget Network",
        "count": len(stations),
        "stations": stations,
    }


@router.get("/surfrad/{station}/latest")
async def get_surfrad_latest(
    station: str,
    limit: int = Query(60, ge=1, le=1440, description="Number of most recent 1-min records"),
):
    """Latest SURFRAD readings for a station (parsed from today's data file).

    Returns the most recent 1-minute observations including downwelling and
    upwelling shortwave/longwave radiation, direct normal, diffuse horizontal,
    UV-B, PAR, net radiation, air temperature, humidity, wind, and pressure.

    Station codes: bon, fpk, gwn, tbl, dra, psu, sxf
    """
    station = station.lower()
    if station not in SURFRAD_STATIONS:
        raise HTTPException(
            404,
            f"Unknown SURFRAD station '{station}'. "
            f"Valid: {', '.join(sorted(SURFRAD_STATIONS))}",
        )

    cache_key = f"{SOURCE}:surfrad:{station}:latest"
    cached = await cache.get(cache_key)
    if cached is not None:
        records = cached
    else:
        now = datetime.now(timezone.utc)
        url = _build_surfrad_daily_url(station, now)

        try:
            text = await http_client.fetch_text(url, source=SOURCE, timeout=15.0)
        except Exception:
            # Today's file may not exist yet early in the UTC day; try yesterday
            yesterday = datetime(
                now.year, now.month, now.day, tzinfo=timezone.utc
            )
            from datetime import timedelta
            yesterday = now - timedelta(days=1)
            url = _build_surfrad_daily_url(station, yesterday)
            try:
                text = await http_client.fetch_text(url, source=SOURCE, timeout=15.0)
            except Exception as exc:
                logger.error("SURFRAD %s fetch failed: %s", station, exc)
                raise HTTPException(
                    502,
                    f"SURFRAD data unavailable for {station}: {exc}",
                )

        # Parse all data lines (skip header lines)
        records = []
        for line in text.strip().splitlines():
            parsed = _parse_surfrad_line(line)
            if parsed is not None:
                records.append(parsed)

        if records:
            await cache.set(cache_key, records, TTL_LATEST)

    # Return most recent N records
    result = records[-limit:] if limit < len(records) else records
    latest = result[-1] if result else None
    meta = SURFRAD_STATIONS[station]

    return {
        "source": "NOAA SURFRAD",
        "station_id": station,
        "station_name": meta["name"],
        "state": meta["state"],
        "lat": meta["lat"],
        "lon": meta["lon"],
        "elevation_m": meta["elevation_m"],
        "count": len(result),
        "latest": latest,
        "data": result,
    }


@router.get("/overview")
async def get_overview():
    """Radiation network overview — station map, source info, and coordinates.

    Returns metadata for SURFRAD and SOLRAD networks including all station
    locations, network descriptions, and data access URLs.
    """
    cache_key = f"{SOURCE}:overview"
    cached = await cache.get(cache_key)
    if cached is not None:
        return cached

    surfrad_list = []
    for code, info in SURFRAD_STATIONS.items():
        surfrad_list.append({
            "station_id": code,
            **info,
        })

    # SOLRAD station list — these are co-located with NWS upper-air stations.
    # Coordinates sourced from NOAA GML SOLRAD documentation.
    solrad_stations = [
        {"station_id": "abq", "name": "Albuquerque", "state": "NM", "lat": 35.0389, "lon": -106.6222},
        {"station_id": "bis", "name": "Bismarck", "state": "ND", "lat": 46.7714, "lon": -100.7558},
        {"station_id": "hnx", "name": "Hanford", "state": "CA", "lat": 36.3142, "lon": -119.6328},
        {"station_id": "mad", "name": "Madison", "state": "WI", "lat": 43.1300, "lon": -89.3267},
        {"station_id": "slc", "name": "Salt Lake City", "state": "UT", "lat": 40.7717, "lon": -111.9647},
        {"station_id": "ste", "name": "Sterling", "state": "VA", "lat": 38.9750, "lon": -77.4869},
    ]

    overview = {
        "source": "NOAA Radiation Networks",
        "generated_utc": datetime.now(timezone.utc).isoformat(),
        "networks": {
            "surfrad": {
                "name": "Surface Radiation Budget Network",
                "description": (
                    "7 stations measuring broadband shortwave, longwave, UV-B, "
                    "and PAR radiation plus standard meteorological variables "
                    "at 1-minute resolution.  Operated by NOAA GML."
                ),
                "url": "https://gml.noaa.gov/grad/surfrad/",
                "data_url": SURFRAD_BASE,
                "station_count": len(SURFRAD_STATIONS),
                "stations": surfrad_list,
            },
            "solrad": {
                "name": "Solar Radiation Network",
                "description": (
                    "~15 stations providing downwelling solar irradiance "
                    "measurements for NWS forecast verification.  "
                    "Operated by NOAA GML."
                ),
                "url": "https://gml.noaa.gov/grad/solrad/",
                "data_url": SOLRAD_BASE,
                "station_count": len(solrad_stations),
                "stations": solrad_stations,
            },
        },
    }

    await cache.set(cache_key, overview, TTL_OVERVIEW)
    return overview
