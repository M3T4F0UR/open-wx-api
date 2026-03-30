"""East Asian Weather — Taiwan CWA and South Korea KMA observations.

Provides real-time surface weather observations from two national met services:

  1. Taiwan CWA (Central Weather Administration) — ~700 automatic stations
     - Temperature, wind, humidity, pressure, rainfall
     - JSON API with free API key
     - https://opendata.cwa.gov.tw/dist/opendata-swagger.html

  2. South Korea KMA (Korea Meteorological Administration) — ~558 stations
     - ~94 ASOS + ~464 AWS stations
     - Temperature, wind, humidity, pressure, precipitation
     - Text/CSV API via data.go.kr (free API key)
     - https://apihub.kma.go.kr/
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Optional

import httpx
from fastapi import APIRouter, HTTPException

import cache
import config
import http_client
from models import Reading, Station, StationDetail

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Source registry metadata
# ---------------------------------------------------------------------------
SOURCE_NAME = "East Asian Weather"
SOURCE_DESCRIPTION = (
    "Real-time weather observations from Taiwan CWA (~700 stations) and "
    "South Korea KMA (~558 stations).  Temperature, wind, humidity, pressure, "
    "and precipitation from both national meteorological services."
)
SOURCE_META = {
    "url": "https://opendata.cwa.gov.tw/",
    "auth_required": True,
    "update_frequency": "10–60 min",
    "coverage": "Taiwan, South Korea",
    "params": [
        "temperature", "wind_speed", "wind_direction", "wind_gust",
        "humidity", "pressure", "precipitation", "rainfall",
    ],
    "category": "weather",
}

router = APIRouter(prefix="/api/asia_east", tags=["East Asian Weather"])

# ---------------------------------------------------------------------------
# API keys (loaded from config, sourced from environment)
# ---------------------------------------------------------------------------
CWA_AUTH_KEY = config.CWA_AUTH_KEY
KMA_SERVICE_KEY = config.KMA_SERVICE_KEY

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
CWA_BASE = "https://opendata.cwa.gov.tw/api/v1/rest/datastore"
CWA_AUTO_OBS = f"{CWA_BASE}/O-A0001-001"

KMA_BASE = "https://apihub.kma.go.kr/api/typ01/url"

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _require_key(key_value: str, key_name: str, service: str) -> None:
    """Raise 503 with a helpful message when an API key is not configured."""
    if not key_value:
        raise HTTPException(
            status_code=503,
            detail=(
                f"{service} requires an API key. "
                f"Set the {key_name} environment variable to enable this endpoint."
            ),
        )


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _safe_float(val) -> Optional[float]:
    """Convert a value to float, returning None on failure."""
    if val is None or val == "" or val == "-99" or val == "-99.0":
        return None
    try:
        f = float(val)
        return None if f <= -99.0 else f
    except (TypeError, ValueError):
        return None


# ---------------------------------------------------------------------------
# Taiwan CWA parser
# ---------------------------------------------------------------------------

def _parse_cwa_observations(data: dict) -> tuple[list[Station], dict[str, list[Reading]]]:
    """Parse CWA O-A0001-001 response into Station and Reading objects."""
    stations_map: dict[str, Station] = {}
    readings_map: dict[str, list[Reading]] = {}

    records = data.get("records", {})
    obs_list = records.get("Station", [])
    if not obs_list:
        # Older format uses "location"
        obs_list = records.get("location", [])

    for item in obs_list:
        # Station identity
        station_id = item.get("StationId") or item.get("stationId", "")
        name = item.get("StationName") or item.get("locationName", "")
        if not station_id:
            continue

        # Coordinates
        geo = item.get("GeoInfo", {})
        lat = _safe_float(geo.get("Coordinates", {}).get("StationLatitude")
                          or item.get("lat"))
        lon = _safe_float(geo.get("Coordinates", {}).get("StationLongitude")
                          or item.get("lon"))
        if lat is None or lon is None:
            continue

        # Observation data
        obs = item.get("WeatherElement", {}) or item.get("weatherElement", {})
        obs_time_str = item.get("ObsTime", {}).get("DateTime", "")

        try:
            ts = datetime.fromisoformat(obs_time_str) if obs_time_str else _now_utc()
        except (ValueError, TypeError):
            ts = _now_utc()

        station = Station(
            id=f"cwa_{station_id}",
            name=name,
            lat=lat,
            lon=lon,
            source="asia_east",
            network="CWA",
            params=[],
            last_update=ts,
            url="https://www.cwa.gov.tw/",
        )

        readings: list[Reading] = []

        # Field mapping: CWA key -> (parameter, display, unit)
        field_map = {
            "AirTemperature": ("temperature", "Temperature", "°C"),
            "RelativeHumidity": ("humidity", "Relative Humidity", "%"),
            "AirPressure": ("pressure", "Pressure", "hPa"),
            "WindSpeed": ("wind_speed", "Wind Speed", "m/s"),
            "WindDirection": ("wind_direction", "Wind Direction", "°"),
            "GustInfo": None,  # handled separately
            "Now": None,  # precipitation handled separately
        }

        for key, mapping in field_map.items():
            if mapping is None:
                continue
            param, display, unit = mapping
            val = _safe_float(obs.get(key))
            if val is not None:
                station.params.append(display)
                readings.append(Reading(
                    station_id=station.id,
                    timestamp=ts,
                    parameter=display,
                    value=val,
                    unit=unit,
                    source="asia_east",
                ))

        # Wind gust (nested)
        gust_info = obs.get("GustInfo", {})
        if isinstance(gust_info, dict):
            gust_val = _safe_float(gust_info.get("PeakGust", {}).get("WindSpeed"))
            if gust_val is not None:
                station.params.append("Wind Gust")
                readings.append(Reading(
                    station_id=station.id,
                    timestamp=ts,
                    parameter="Wind Gust",
                    value=gust_val,
                    unit="m/s",
                    source="asia_east",
                ))

        # Precipitation (nested under Now -> Precipitation)
        now_block = obs.get("Now", {})
        if isinstance(now_block, dict):
            precip_val = _safe_float(now_block.get("Precipitation"))
            if precip_val is not None:
                station.params.append("Precipitation")
                readings.append(Reading(
                    station_id=station.id,
                    timestamp=ts,
                    parameter="Precipitation",
                    value=precip_val,
                    unit="mm",
                    source="asia_east",
                ))

        if readings:
            stations_map[station.id] = station
            readings_map[station.id] = readings

    return list(stations_map.values()), readings_map


# ---------------------------------------------------------------------------
# South Korea KMA parser
# ---------------------------------------------------------------------------

def _parse_kma_text(text: str) -> tuple[list[Station], dict[str, list[Reading]]]:
    """Parse KMA kma_sfctm3 text/CSV response into Station and Reading objects.

    The KMA API returns a header block (lines starting with '#') followed by
    whitespace-delimited data lines.  The column layout for kma_sfctm3 is:
        TM  STN  WD  WS  GST  PA  PS  PT  PR  TD  TA  HM  ...
    """
    stations_map: dict[str, Station] = {}
    readings_map: dict[str, list[Reading]] = {}

    lines = text.strip().splitlines()

    # Determine header and column names
    header_cols: list[str] = []
    data_start = 0
    for i, line in enumerate(lines):
        stripped = line.strip()
        if not stripped:
            continue
        # Header lines start with '#'
        if stripped.startswith("#"):
            # The last header line typically contains column names
            header_cols = stripped.lstrip("#").split()
            data_start = i + 1
            continue
        if not header_cols:
            # No header found yet, skip
            continue
        break

    if not header_cols:
        # Fallback: standard kma_sfctm3 column order
        header_cols = [
            "TM", "STN", "WD", "WS", "GST", "PA", "PS", "PT", "PR",
            "TD", "TA", "HM", "PV", "RN", "RN_DAY", "RN_JUN", "RN_INT",
            "SD_HR3", "SD_DAY", "SD_TOT", "WC", "WP", "WW", "CA_TOT",
            "CA_MID", "CH_MIN", "CT", "CT_TOP", "CT_MID", "CT_LOW",
            "VS", "SS", "SI", "ST_GD", "TS", "TE_005", "TE_01", "TE_02",
            "TE_03", "ST_SEA", "WH", "BF", "IR", "IX",
        ]

    # Column mapping: KMA col -> (parameter, display, unit)
    col_map = {
        "TA":     ("temperature",    "Temperature",       "°C"),
        "HM":     ("humidity",       "Relative Humidity", "%"),
        "PA":     ("pressure",       "Station Pressure",  "hPa"),
        "PS":     ("pressure_msl",   "Sea-Level Pressure","hPa"),
        "WS":     ("wind_speed",     "Wind Speed",        "m/s"),
        "WD":     ("wind_direction", "Wind Direction",    "°"),
        "GST":    ("wind_gust",      "Wind Gust",         "m/s"),
        "TD":     ("dewpoint",       "Dew Point",         "°C"),
        "RN":     ("precipitation",  "Precipitation",     "mm"),
        "RN_DAY": ("precip_daily",   "Daily Precip",      "mm"),
    }

    for line in lines[data_start:]:
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue

        parts = stripped.split()
        if len(parts) < 3:
            continue

        # Build a column -> value dict
        row: dict[str, str] = {}
        for j, col in enumerate(header_cols):
            if j < len(parts):
                row[col] = parts[j]

        stn_id = row.get("STN", "")
        tm_str = row.get("TM", "")  # Format: YYYYMMDDHHMI or YYYYMMDDHH00
        if not stn_id:
            continue

        # Parse timestamp
        try:
            if len(tm_str) >= 12:
                ts = datetime.strptime(tm_str[:12], "%Y%m%d%H%M").replace(
                    tzinfo=timezone.utc
                )
            elif len(tm_str) >= 10:
                ts = datetime.strptime(tm_str[:10], "%Y%m%d%H").replace(
                    tzinfo=timezone.utc
                )
            else:
                ts = _now_utc()
        except (ValueError, TypeError):
            ts = _now_utc()

        station_key = f"kma_{stn_id}"
        station = Station(
            id=station_key,
            name=f"KMA Station {stn_id}",
            lat=0.0,  # KMA text API does not include coordinates
            lon=0.0,
            source="asia_east",
            network="KMA",
            params=[],
            last_update=ts,
            url="https://data.kma.go.kr/",
        )

        readings: list[Reading] = []

        for col_name, (param, display, unit) in col_map.items():
            val = _safe_float(row.get(col_name))
            if val is not None:
                station.params.append(display)
                readings.append(Reading(
                    station_id=station_key,
                    timestamp=ts,
                    parameter=display,
                    value=val,
                    unit=unit,
                    source="asia_east",
                ))

        if readings:
            stations_map[station_key] = station
            readings_map[station_key] = readings

    return list(stations_map.values()), readings_map


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@router.get(
    "/taiwan/current",
    summary="Current observations from all Taiwan CWA stations",
)
async def taiwan_current():
    """Return current temperature, wind, humidity, pressure, and rainfall
    from all Taiwan CWA automatic weather stations (~700 stations).

    Requires the CWA_AUTH_KEY environment variable.
    """
    _require_key(CWA_AUTH_KEY, "CWA_AUTH_KEY", "Taiwan CWA")

    cache_key = "asia_east:taiwan:current"
    cached = await cache.get(cache_key)
    if cached is not None:
        return cached

    try:
        data = await http_client.fetch_json(
            CWA_AUTO_OBS,
            params={"Authorization": CWA_AUTH_KEY},
            source="asia_east_cwa",
        )
    except http_client.CircuitOpenError as exc:
        raise HTTPException(status_code=503, detail=str(exc))
    except httpx.TimeoutException:
        raise HTTPException(status_code=504, detail="CWA API timeout")
    except httpx.HTTPStatusError as exc:
        raise HTTPException(
            status_code=exc.response.status_code,
            detail=f"CWA API error: {exc}",
        )
    except Exception as exc:
        logger.exception("Error fetching Taiwan CWA observations")
        raise HTTPException(status_code=500, detail=str(exc))

    stations, readings_map = _parse_cwa_observations(data)

    station_details = []
    for station in stations:
        readings = readings_map.get(station.id, [])
        detail = StationDetail(station=station, readings=readings)
        station_details.append(detail.model_dump())

    result = {
        "source": "asia_east",
        "network": "CWA",
        "region": "Taiwan",
        "station_count": len(stations),
        "stations": station_details,
    }
    await cache.set(cache_key, result, ttl=config.CACHE_TTL)
    return result


@router.get(
    "/korea/current",
    summary="Current observations from all South Korea KMA stations",
)
async def korea_current():
    """Return current temperature, wind, humidity, pressure, and precipitation
    from all South Korea KMA stations (~94 ASOS + ~464 AWS).

    Requires the KMA_SERVICE_KEY environment variable.
    """
    _require_key(KMA_SERVICE_KEY, "KMA_SERVICE_KEY", "South Korea KMA")

    cache_key = "asia_east:korea:current"
    cached = await cache.get(cache_key)
    if cached is not None:
        return cached

    # Build the KMA request — stn=0 means all stations
    now = _now_utc()
    tm = now.strftime("%Y%m%d%H00")

    try:
        text = await http_client.fetch_text(
            f"{KMA_BASE}/kma_sfctm3.php",
            params={
                "tm": tm,
                "stn": "0",
                "help": "0",
                "authKey": KMA_SERVICE_KEY,
            },
            source="asia_east_kma",
        )
    except http_client.CircuitOpenError as exc:
        raise HTTPException(status_code=503, detail=str(exc))
    except httpx.TimeoutException:
        raise HTTPException(status_code=504, detail="KMA API timeout")
    except httpx.HTTPStatusError as exc:
        raise HTTPException(
            status_code=exc.response.status_code,
            detail=f"KMA API error: {exc}",
        )
    except Exception as exc:
        logger.exception("Error fetching South Korea KMA observations")
        raise HTTPException(status_code=500, detail=str(exc))

    stations, readings_map = _parse_kma_text(text)

    station_details = []
    for station in stations:
        readings = readings_map.get(station.id, [])
        detail = StationDetail(station=station, readings=readings)
        station_details.append(detail.model_dump())

    result = {
        "source": "asia_east",
        "network": "KMA",
        "region": "South Korea",
        "query_time": tm,
        "station_count": len(stations),
        "stations": station_details,
    }
    await cache.set(cache_key, result, ttl=config.CACHE_TTL)
    return result


# ---------------------------------------------------------------------------
# Endpoint: GET /api/asia_east/info
# ---------------------------------------------------------------------------

@router.get(
    "/info",
    summary="Source information and API key status",
)
async def info():
    """Return information about the East Asian Weather source and whether
    API keys are configured for Taiwan CWA and South Korea KMA."""
    cwa_configured = bool(CWA_AUTH_KEY)
    kma_configured = bool(KMA_SERVICE_KEY)

    return {
        "source": SOURCE_NAME,
        "description": SOURCE_DESCRIPTION,
        "services": {
            "taiwan_cwa": {
                "key_configured": cwa_configured,
                "key_env_var": "CWA_AUTH_KEY",
                "stations": "~700 automatic weather stations",
                "api_docs": "https://opendata.cwa.gov.tw/dist/opendata-swagger.html",
                "instructions": (
                    None if cwa_configured else
                    "Register at https://opendata.cwa.gov.tw/ for a free API key. "
                    "Set the CWA_AUTH_KEY environment variable and restart the server."
                ),
            },
            "south_korea_kma": {
                "key_configured": kma_configured,
                "key_env_var": "KMA_SERVICE_KEY",
                "stations": "~94 ASOS + ~464 AWS (~558 total)",
                "api_docs": "https://apihub.kma.go.kr/",
                "instructions": (
                    None if kma_configured else
                    "Register at https://data.go.kr/ for a free API key. "
                    "Set the KMA_SERVICE_KEY environment variable and restart the server."
                ),
            },
        },
        **SOURCE_META,
    }
