"""Ocean & River Extended — GLOS Seagull, Open-Meteo Marine, Open-Meteo Flood.

Three complementary ocean/river data sources, all free with no authentication:

1. GLOS Seagull (Great Lakes Observing System)
   ~150 buoy and shore platforms across the Great Lakes measuring water
   temperature, waves, currents, wind, chlorophyll, and dissolved oxygen.
   API: https://seagull.glos.org/

2. Open-Meteo Marine
   Global marine weather forecast (wave height, direction, period, swell,
   ocean currents) derived from NOAA GFS-Wave and ECMWF WAM models.
   API: https://open-meteo.com/en/docs/marine-weather-api

3. Open-Meteo Flood (GloFAS wrapper)
   Global river discharge forecasts from the Copernicus GloFAS system.
   API: https://open-meteo.com/en/docs/flood-api
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Optional

import httpx
from fastapi import APIRouter, HTTPException, Query

import cache
import config
from models import Reading, Station

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Source registry metadata  (read by server.py /api/sources)
# ---------------------------------------------------------------------------

SOURCE_NAME = "Ocean & River Extended"
SOURCE_DESCRIPTION = (
    "Three free ocean/river data sources: "
    "(1) GLOS Seagull — ~150 Great Lakes buoys and shore stations (water temp, waves, "
    "currents, wind, chlorophyll, dissolved oxygen); "
    "(2) Open-Meteo Marine — global wave/swell/current forecasts; "
    "(3) Open-Meteo Flood — global river discharge forecasts (GloFAS)."
)
SOURCE_META = {
    "url": "https://seagull.glos.org/",
    "auth_required": False,
    "update_frequency": "10 min (GLOS), 1 hr (Marine), 6 hr (Flood)",
    "coverage": "Great Lakes (GLOS), Global ocean (Marine), Global rivers (Flood)",
    "params": [
        "water_temperature", "wave_height", "wave_direction", "wave_period",
        "swell_height", "ocean_current", "wind_speed", "wind_direction",
        "chlorophyll", "dissolved_oxygen", "river_discharge",
    ],
    "category": "water",
}

# Sub-source identifiers
SOURCES = ("glos_seagull", "open_meteo_marine", "open_meteo_flood")

router = APIRouter(prefix="/api/ocean", tags=["Ocean & River Extended"])

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# GLOS Seagull
GLOS_BASE = "https://seagull-api.glos.org/api"
GLOS_OBS_DATASET_ID = 3  # standard meteorological/oceanographic obs

# Open-Meteo Marine
MARINE_BASE = "https://marine-api.open-meteo.com/v1/marine"
MARINE_HOURLY_PARAMS = (
    "wave_height,wave_direction,wave_period,"
    "swell_wave_height,swell_wave_direction,swell_wave_period,"
    "ocean_current_velocity,ocean_current_direction"
)

# Open-Meteo Flood (GloFAS)
FLOOD_BASE = "https://flood-api.open-meteo.com/v1/flood"

# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------

_HTTP_HEADERS = {
    "User-Agent": config.USER_AGENT,
    "Accept": "application/json",
}


def _make_client() -> httpx.AsyncClient:
    return httpx.AsyncClient(
        headers=_HTTP_HEADERS,
        timeout=config.HTTP_TIMEOUT,
        follow_redirects=True,
    )


async def _get_json(url: str, params: Optional[dict] = None, label: str = "upstream") -> Any:
    """GET url, return parsed JSON; raise HTTPException on failure."""
    try:
        async with _make_client() as client:
            resp = await client.get(url, params=params or {})
            resp.raise_for_status()
            return resp.json()
    except httpx.TimeoutException:
        raise HTTPException(status_code=504, detail=f"{label} API timeout ({url})")
    except httpx.HTTPStatusError as exc:
        raise HTTPException(
            status_code=exc.response.status_code,
            detail=f"{label} API error: HTTP {exc.response.status_code} for {url}",
        )
    except Exception as exc:
        logger.exception("Unexpected error fetching %s", url)
        raise HTTPException(status_code=502, detail=f"Error fetching {label}: {exc}")


# ---------------------------------------------------------------------------
# GLOS Seagull — Great Lakes
# ---------------------------------------------------------------------------

def _parse_glos_platform(raw: dict) -> Station:
    """Convert a GLOS Seagull platform record to the shared Station model."""
    plat_id = str(raw.get("id", raw.get("platform_id", "")))
    name = raw.get("name", raw.get("platform_name", plat_id))
    lat = float(raw.get("latitude", 0.0))
    lon = float(raw.get("longitude", 0.0))

    # Collect parameter names from the obs if available
    params: list[str] = []
    for obs in raw.get("obs", raw.get("observations", [])):
        param = obs.get("parameter", obs.get("obs_type", ""))
        if param and param not in params:
            params.append(param)

    return Station(
        id=f"glos_{plat_id}",
        name=name,
        lat=lat,
        lon=lon,
        source="glos_seagull",
        network="GLOS Seagull",
        params=params or ["water_temperature", "wave_height", "wind_speed"],
        url=f"https://seagull.glos.org/station/{plat_id}",
        extra={
            "platform_id": plat_id,
            "platform_type": raw.get("platform_type", ""),
            "lake": raw.get("lake", raw.get("region", "")),
        },
    )


def _parse_glos_readings(raw: dict, platform_id: str) -> list[Reading]:
    """Extract readings from a GLOS observation record."""
    readings: list[Reading] = []
    station_id = f"glos_{platform_id}"

    for obs in raw.get("obs", raw.get("observations", [])):
        param = obs.get("parameter", obs.get("obs_type", ""))
        value = obs.get("value")
        unit = obs.get("unit", "")
        timestamp_str = obs.get("timestamp", obs.get("time", ""))

        if value is None or param == "":
            continue

        try:
            val = float(value)
        except (TypeError, ValueError):
            continue

        try:
            ts = datetime.fromisoformat(timestamp_str.replace("Z", "+00:00"))
        except (ValueError, AttributeError):
            ts = datetime.now(timezone.utc)

        readings.append(Reading(
            station_id=station_id,
            timestamp=ts,
            parameter=param,
            value=val,
            unit=unit,
            source="glos_seagull",
        ))

    return readings


@router.get("/great_lakes/latest")
async def glos_latest_obs():
    """Latest observations from all GLOS Seagull Great Lakes platforms.

    Returns buoy and shore station data including water temperature, waves,
    currents, wind, chlorophyll, and dissolved oxygen from ~150 platforms.
    """
    cache_key = "ocean:glos:latest"
    cached = await cache.get(cache_key)
    if cached is not None:
        return cached

    data = await _get_json(
        f"{GLOS_BASE}/v1/obs-latest",
        params={"obs_dataset_id": str(GLOS_OBS_DATASET_ID)},
        label="GLOS Seagull",
    )

    # The API returns a list of platform objects with nested observations
    platforms = data if isinstance(data, list) else data.get("data", data.get("results", []))

    stations: list[dict] = []
    all_readings: list[dict] = []

    for plat in platforms:
        try:
            station = _parse_glos_platform(plat)
            readings = _parse_glos_readings(plat, str(plat.get("id", plat.get("platform_id", ""))))
            stations.append(station.model_dump(mode="json"))
            all_readings.extend(r.model_dump(mode="json") for r in readings)
        except Exception:
            logger.debug("Skipping malformed GLOS platform: %s", plat.get("id", "?"), exc_info=True)
            continue

    result = {
        "source": "glos_seagull",
        "station_count": len(stations),
        "reading_count": len(all_readings),
        "stations": stations,
        "readings": all_readings,
    }

    await cache.set(cache_key, result, ttl=config.CACHE_TTL)
    return result


# ---------------------------------------------------------------------------
# Open-Meteo Marine — global wave/swell forecast
# ---------------------------------------------------------------------------

@router.get("/marine")
async def open_meteo_marine(
    lat: float = Query(..., description="Latitude (-90 to 90)", ge=-90, le=90),
    lon: float = Query(..., description="Longitude (-180 to 180)", ge=-180, le=180),
    forecast_days: int = Query(7, description="Forecast days (1-16)", ge=1, le=16),
):
    """Marine weather forecast at a point — wave height, direction, period, swell, currents.

    Uses Open-Meteo's Marine API (free, no auth) backed by NOAA GFS-Wave and
    ECMWF WAM models. Returns hourly forecast data.
    """
    cache_key = f"ocean:marine:{lat:.2f}:{lon:.2f}:{forecast_days}"
    cached = await cache.get(cache_key)
    if cached is not None:
        return cached

    data = await _get_json(
        MARINE_BASE,
        params={
            "latitude": str(lat),
            "longitude": str(lon),
            "hourly": MARINE_HOURLY_PARAMS,
            "forecast_days": str(forecast_days),
        },
        label="Open-Meteo Marine",
    )

    # Build a structured response
    hourly = data.get("hourly", {})
    times = hourly.get("time", [])
    hourly_units = data.get("hourly_units", {})

    forecasts: list[dict] = []
    for i, t in enumerate(times):
        entry: dict[str, Any] = {"time": t}
        for param in (
            "wave_height", "wave_direction", "wave_period",
            "swell_wave_height", "swell_wave_direction", "swell_wave_period",
            "ocean_current_velocity", "ocean_current_direction",
        ):
            values = hourly.get(param, [])
            if i < len(values) and values[i] is not None:
                entry[param] = values[i]
        forecasts.append(entry)

    result = {
        "source": "open_meteo_marine",
        "latitude": data.get("latitude", lat),
        "longitude": data.get("longitude", lon),
        "elevation": data.get("elevation"),
        "timezone": data.get("timezone", "UTC"),
        "units": hourly_units,
        "forecast_count": len(forecasts),
        "hourly": forecasts,
    }

    await cache.set(cache_key, result, ttl=config.CACHE_TTL)
    return result


# ---------------------------------------------------------------------------
# Open-Meteo Flood — GloFAS river discharge forecast
# ---------------------------------------------------------------------------

@router.get("/flood")
async def open_meteo_flood(
    lat: float = Query(..., description="Latitude (-90 to 90)", ge=-90, le=90),
    lon: float = Query(..., description="Longitude (-180 to 180)", ge=-180, le=180),
    forecast_days: int = Query(52, description="Forecast days (1-210)", ge=1, le=210),
):
    """River discharge forecast at a point — daily m³/s from the GloFAS model.

    Uses Open-Meteo's Flood API (free, no auth) wrapping the Copernicus
    Global Flood Awareness System. Returns daily river discharge forecasts.
    """
    cache_key = f"ocean:flood:{lat:.2f}:{lon:.2f}:{forecast_days}"
    cached = await cache.get(cache_key)
    if cached is not None:
        return cached

    data = await _get_json(
        FLOOD_BASE,
        params={
            "latitude": str(lat),
            "longitude": str(lon),
            "daily": "river_discharge",
            "forecast_days": str(forecast_days),
        },
        label="Open-Meteo Flood",
    )

    daily = data.get("daily", {})
    times = daily.get("time", [])
    discharges = daily.get("river_discharge", [])
    daily_units = data.get("daily_units", {})

    forecasts: list[dict] = []
    for i, t in enumerate(times):
        entry: dict[str, Any] = {"date": t}
        if i < len(discharges) and discharges[i] is not None:
            entry["river_discharge_m3s"] = discharges[i]
        forecasts.append(entry)

    result = {
        "source": "open_meteo_flood",
        "latitude": data.get("latitude", lat),
        "longitude": data.get("longitude", lon),
        "units": daily_units,
        "forecast_count": len(forecasts),
        "daily": forecasts,
    }

    await cache.set(cache_key, result, ttl=config.CACHE_TTL)
    return result


# ---------------------------------------------------------------------------
# Overview endpoint
# ---------------------------------------------------------------------------

@router.get("/overview")
async def ocean_overview():
    """Source information for all three ocean/river data sources in this module."""
    return {
        "module": SOURCE_NAME,
        "description": SOURCE_DESCRIPTION,
        "sources": {
            "glos_seagull": {
                "name": "GLOS Seagull",
                "description": "Great Lakes Observing System — ~150 buoy and shore platforms",
                "url": "https://seagull.glos.org/",
                "auth_required": False,
                "update_frequency": "~10 min",
                "coverage": "Great Lakes (Superior, Michigan, Huron, Erie, Ontario)",
                "params": [
                    "water_temperature", "wave_height", "wave_period",
                    "current_speed", "wind_speed", "wind_direction",
                    "chlorophyll", "dissolved_oxygen",
                ],
                "endpoints": ["/api/ocean/great_lakes/latest"],
            },
            "open_meteo_marine": {
                "name": "Open-Meteo Marine",
                "description": "Global marine weather forecasts (GFS-Wave / ECMWF WAM)",
                "url": "https://open-meteo.com/en/docs/marine-weather-api",
                "auth_required": False,
                "update_frequency": "~1 hr",
                "coverage": "Global ocean",
                "params": [
                    "wave_height", "wave_direction", "wave_period",
                    "swell_wave_height", "swell_wave_direction", "swell_wave_period",
                    "ocean_current_velocity", "ocean_current_direction",
                ],
                "endpoints": ["/api/ocean/marine"],
            },
            "open_meteo_flood": {
                "name": "Open-Meteo Flood (GloFAS)",
                "description": "Global river discharge forecasts from Copernicus GloFAS",
                "url": "https://open-meteo.com/en/docs/flood-api",
                "auth_required": False,
                "update_frequency": "~6 hr",
                "coverage": "Global rivers",
                "params": ["river_discharge"],
                "endpoints": ["/api/ocean/flood"],
            },
        },
    }
