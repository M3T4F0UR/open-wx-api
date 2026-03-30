"""Satellite & Gridded Products — freely available satellite and gridded data.

Four data sources are integrated here, all requiring NO authentication:

1. ORNL Daymet — 1km daily gridded climate for North America (1980–present)
   https://daymet.ornl.gov/
   Parameters: tmax, tmin, prcp, srad, vp, swe, dayl

2. GOES GLM Lightning — 20-second lightning events on AWS S3
   s3://noaa-goes18/GLM-L2-LCFA/ (anonymous access)
   NetCDF files with lightning events/groups/flashes

3. NOHRSC Snow Analysis — 1km daily snow depth and SWE from NOAA
   https://mapservices.weather.noaa.gov/raster/rest/services/snow/NOHRSC_Snow_Analysis/MapServer

4. Open-Meteo Air Quality — global gridded AQ forecasts and analysis
   https://air-quality-api.open-meteo.com/
   Parameters: pm10, pm2_5, ozone, nitrogen_dioxide, and more
"""
from __future__ import annotations

import csv
import io
import logging
from datetime import datetime, timezone
from typing import Any, Optional

from fastapi import APIRouter, HTTPException, Query

import cache
import config
import http_client

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Source registry metadata  (read by server.py /api/sources)
# ---------------------------------------------------------------------------

SOURCE_NAME = "Satellite & Gridded Products"
SOURCE_DESCRIPTION = (
    "Freely available satellite and gridded data products: ORNL Daymet 1km daily "
    "climate (1980–present), GOES GLM lightning, NOHRSC snow analysis, and "
    "Open-Meteo air quality.  No authentication required for any product."
)
SOURCE_META = {
    "url": "https://daymet.ornl.gov/",
    "auth_required": False,
    "update_frequency": "Daily (Daymet, snow) to hourly (air quality)",
    "coverage": "North America (Daymet, snow) / Global (air quality, lightning)",
    "params": [
        "tmax", "tmin", "precipitation", "solar_radiation", "vapor_pressure",
        "snow_water_equivalent", "day_length", "snow_depth",
        "pm2_5", "pm10", "ozone", "nitrogen_dioxide",
    ],
    "category": "satellite",
}

router = APIRouter(prefix="/api/satellite", tags=["Satellite & Gridded Products"])

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

DAYMET_BASE = "https://daymet.ornl.gov/single-pixel/api/data"
OPEN_METEO_AQ_BASE = "https://air-quality-api.open-meteo.com/v1/air-quality"
NOHRSC_MAPSERVER = (
    "https://mapservices.weather.noaa.gov/raster/rest/services/"
    "snow/NOHRSC_Snow_Analysis/MapServer"
)
GOES_GLM_S3_PREFIX = "s3://noaa-goes18/GLM-L2-LCFA/"

# Daymet available parameters with descriptions and units
DAYMET_PARAMS: dict[str, tuple[str, str]] = {
    "tmax":  ("Maximum Temperature",       "°C"),
    "tmin":  ("Minimum Temperature",       "°C"),
    "prcp":  ("Precipitation",             "mm/day"),
    "srad":  ("Shortwave Radiation",       "W/m²"),
    "vp":    ("Vapor Pressure",            "Pa"),
    "swe":   ("Snow Water Equivalent",     "kg/m²"),
    "dayl":  ("Day Length",                "s"),
}

# Open-Meteo AQ parameters with descriptions and units
OPEN_METEO_AQ_PARAMS: dict[str, tuple[str, str]] = {
    "pm10":                ("PM10",                      "μg/m³"),
    "pm2_5":               ("PM2.5",                     "μg/m³"),
    "ozone":               ("Ozone (O₃)",                "μg/m³"),
    "nitrogen_dioxide":    ("Nitrogen Dioxide (NO₂)",    "μg/m³"),
    "sulphur_dioxide":     ("Sulphur Dioxide (SO₂)",     "μg/m³"),
    "carbon_monoxide":     ("Carbon Monoxide (CO)",      "μg/m³"),
    "dust":                ("Dust",                      "μg/m³"),
    "ammonia":             ("Ammonia (NH₃)",             "μg/m³"),
    "uv_index":            ("UV Index",                  ""),
    "uv_index_clear_sky":  ("UV Index (Clear Sky)",      ""),
    "alder_pollen":        ("Alder Pollen",              "grains/m³"),
    "birch_pollen":        ("Birch Pollen",              "grains/m³"),
    "grass_pollen":        ("Grass Pollen",              "grains/m³"),
    "us_aqi":              ("US AQI",                    ""),
    "us_aqi_pm2_5":        ("US AQI (PM2.5)",            ""),
    "us_aqi_pm10":         ("US AQI (PM10)",             ""),
    "us_aqi_ozone":        ("US AQI (Ozone)",            ""),
}

# NOHRSC Snow Analysis layer IDs (from MapServer)
NOHRSC_LAYERS: dict[int, tuple[str, str]] = {
    1:  ("Snow Depth",               "inches"),
    3:  ("Snow Water Equivalent",    "inches"),
    5:  ("Snowfall (24-hour)",       "inches"),
    7:  ("Snow Temperature",         "°F"),
}


# ---------------------------------------------------------------------------
# Endpoint 1 — Daymet point data
# ---------------------------------------------------------------------------

@router.get(
    "/daymet",
    summary="Daymet 1km daily gridded climate data for a point",
)
async def get_daymet(
    lat: float = Query(
        ...,
        ge=-90,
        le=90,
        description="Latitude (North America only: ~14°N to ~83°N)",
    ),
    lon: float = Query(
        ...,
        ge=-180,
        le=180,
        description="Longitude (North America only: ~-179° to ~-52°)",
    ),
    year: int = Query(
        ...,
        ge=1980,
        description="Year to retrieve (1980–present, ~2-day lag)",
    ),
    params: str = Query(
        "tmax,tmin,prcp",
        description=(
            "Comma-separated Daymet parameters.  "
            "Options: tmax, tmin, prcp, srad, vp, swe, dayl"
        ),
    ),
):
    """Return daily Daymet climate data for a single point and year.

    ORNL Daymet provides 1km-resolution daily gridded climate data for
    North America from 1980 to the most recent complete calendar year
    (typically with a ~2-day lag).

    The response includes the raw daily time series plus summary
    statistics for each requested parameter.
    """
    # Validate parameters
    param_list = [p.strip().lower() for p in params.split(",") if p.strip()]
    invalid = [p for p in param_list if p not in DAYMET_PARAMS]
    if invalid:
        raise HTTPException(
            status_code=400,
            detail=(
                f"Invalid Daymet parameter(s): {', '.join(invalid)}.  "
                f"Valid options: {', '.join(sorted(DAYMET_PARAMS))}."
            ),
        )

    cache_key = f"satellite:daymet:{lat}:{lon}:{year}:{','.join(sorted(param_list))}"
    cached = await cache.get(cache_key)
    if cached is not None:
        return cached

    # Daymet API returns CSV
    url = DAYMET_BASE
    api_params = {
        "lat": lat,
        "lon": lon,
        "years": str(year),
        "measuredParams": ",".join(param_list),
    }

    try:
        text = await http_client.fetch_text(
            url, params=api_params, source="daymet",
        )
    except Exception as exc:
        logger.exception("Daymet API error for lat=%s lon=%s year=%s", lat, lon, year)
        raise HTTPException(status_code=502, detail=f"Daymet API error: {exc}")

    # Parse CSV — Daymet CSV has comment lines starting with "year" header
    # and data lines like: 1980,1,tmax,-5.0
    records: list[dict[str, Any]] = []
    summary: dict[str, dict[str, Any]] = {}

    try:
        # Skip comment lines (lines starting with non-numeric chars before header)
        lines = text.strip().splitlines()
        # Find the header line
        header_idx = 0
        for i, line in enumerate(lines):
            if line.startswith("year"):
                header_idx = i
                break

        csv_text = "\n".join(lines[header_idx:])
        reader = csv.DictReader(io.StringIO(csv_text))
        for row in reader:
            record: dict[str, Any] = {}
            for key, val in row.items():
                if key is None:
                    continue
                key = key.strip()
                try:
                    record[key] = int(val) if key in ("year", "yday") else float(val)
                except (ValueError, TypeError):
                    record[key] = val
            records.append(record)

        # Build summary statistics per parameter
        for p in param_list:
            values = [r[p] for r in records if p in r and isinstance(r[p], (int, float))]
            if values:
                summary[p] = {
                    "description": DAYMET_PARAMS[p][0],
                    "unit": DAYMET_PARAMS[p][1],
                    "count": len(values),
                    "min": min(values),
                    "max": max(values),
                    "mean": round(sum(values) / len(values), 2),
                }
    except Exception as exc:
        logger.warning("Error parsing Daymet CSV: %s", exc)
        # Return raw text if parsing fails
        return {
            "source": "daymet",
            "lat": lat,
            "lon": lon,
            "year": year,
            "params": param_list,
            "parse_error": str(exc),
            "raw_text": text[:5000],
        }

    result = {
        "source": "daymet",
        "lat": lat,
        "lon": lon,
        "year": year,
        "params": param_list,
        "record_count": len(records),
        "summary": summary,
        "data": records,
    }
    await cache.set(cache_key, result, ttl=config.CACHE_TTL_LONG)
    return result


# ---------------------------------------------------------------------------
# Endpoint 2 — Open-Meteo Air Quality
# ---------------------------------------------------------------------------

@router.get(
    "/air_quality",
    summary="Open-Meteo global air quality data for a point",
)
async def get_air_quality(
    lat: float = Query(
        ...,
        ge=-90,
        le=90,
        description="Latitude",
    ),
    lon: float = Query(
        ...,
        ge=-180,
        le=180,
        description="Longitude",
    ),
    params: str = Query(
        "pm10,pm2_5,ozone,nitrogen_dioxide",
        description=(
            "Comma-separated AQ parameters.  "
            "Options: pm10, pm2_5, ozone, nitrogen_dioxide, sulphur_dioxide, "
            "carbon_monoxide, dust, ammonia, uv_index, us_aqi, us_aqi_pm2_5, "
            "us_aqi_pm10, us_aqi_ozone"
        ),
    ),
    forecast_days: int = Query(
        3,
        ge=1,
        le=7,
        description="Number of forecast days (1–7)",
    ),
):
    """Return hourly air quality data and forecasts from Open-Meteo.

    Open-Meteo provides global gridded air quality analysis and forecasts
    with hourly resolution.  Data is derived from CAMS (Copernicus Atmosphere
    Monitoring Service) and other models.  No authentication required.
    """
    param_list = [p.strip().lower() for p in params.split(",") if p.strip()]
    invalid = [p for p in param_list if p not in OPEN_METEO_AQ_PARAMS]
    if invalid:
        raise HTTPException(
            status_code=400,
            detail=(
                f"Invalid AQ parameter(s): {', '.join(invalid)}.  "
                f"Valid options: {', '.join(sorted(OPEN_METEO_AQ_PARAMS))}."
            ),
        )

    cache_key = (
        f"satellite:aq:{lat}:{lon}:{','.join(sorted(param_list))}:{forecast_days}"
    )
    cached = await cache.get(cache_key)
    if cached is not None:
        return cached

    api_params = {
        "latitude": lat,
        "longitude": lon,
        "hourly": ",".join(param_list),
        "forecast_days": forecast_days,
    }

    try:
        data = await http_client.fetch_json(
            OPEN_METEO_AQ_BASE, params=api_params, source="open_meteo_aq",
        )
    except Exception as exc:
        logger.exception("Open-Meteo AQ error for lat=%s lon=%s", lat, lon)
        raise HTTPException(
            status_code=502, detail=f"Open-Meteo AQ API error: {exc}",
        )

    # Parse hourly data into structured records
    hourly = data.get("hourly", {})
    times = hourly.get("time", [])
    records: list[dict[str, Any]] = []
    for i, t in enumerate(times):
        record: dict[str, Any] = {"time": t}
        for p in param_list:
            vals = hourly.get(p, [])
            record[p] = vals[i] if i < len(vals) else None
        records.append(record)

    # Build summary per parameter
    summary: dict[str, dict[str, Any]] = {}
    for p in param_list:
        vals = hourly.get(p, [])
        numeric = [v for v in vals if v is not None]
        if numeric:
            summary[p] = {
                "description": OPEN_METEO_AQ_PARAMS[p][0],
                "unit": OPEN_METEO_AQ_PARAMS[p][1],
                "count": len(numeric),
                "min": min(numeric),
                "max": max(numeric),
                "mean": round(sum(numeric) / len(numeric), 2),
            }

    result = {
        "source": "open_meteo_aq",
        "lat": data.get("latitude", lat),
        "lon": data.get("longitude", lon),
        "elevation": data.get("elevation"),
        "timezone": data.get("timezone"),
        "forecast_days": forecast_days,
        "params": param_list,
        "record_count": len(records),
        "summary": summary,
        "data": records,
    }
    await cache.set(cache_key, result, ttl=config.CACHE_TTL)
    return result


# ---------------------------------------------------------------------------
# Endpoint 3 — Product overview
# ---------------------------------------------------------------------------

@router.get(
    "/overview",
    summary="List all available satellite and gridded data products",
)
async def overview():
    """Return metadata for all satellite and gridded data products.

    Describes each product's coverage, resolution, parameters, and
    access patterns.  Useful for discovery before querying specific
    endpoints.
    """
    return {
        "source": "Satellite & Gridded Products",
        "products": [
            {
                "name": "ORNL Daymet",
                "endpoint": "/api/satellite/daymet",
                "description": (
                    "1km daily gridded climate data for North America.  "
                    "Derived from weather station observations using spatial "
                    "interpolation and terrain adjustment."
                ),
                "url": "https://daymet.ornl.gov/",
                "auth_required": False,
                "temporal_coverage": "1980–present (~2-day lag)",
                "spatial_coverage": "North America (~14°N to ~83°N)",
                "spatial_resolution": "1 km",
                "temporal_resolution": "Daily",
                "parameters": {
                    k: {"description": v[0], "unit": v[1]}
                    for k, v in DAYMET_PARAMS.items()
                },
            },
            {
                "name": "GOES GLM Lightning",
                "endpoint": None,
                "description": (
                    "Geostationary Lightning Mapper data from GOES-16/18 "
                    "on AWS S3 (anonymous access).  20-second temporal resolution "
                    "with lightning events, groups, and flashes in NetCDF format."
                ),
                "url": "https://registry.opendata.aws/noaa-goes/",
                "s3_prefix": GOES_GLM_S3_PREFIX,
                "auth_required": False,
                "temporal_coverage": "2017–present",
                "spatial_coverage": "Western Hemisphere",
                "spatial_resolution": "~8 km (nadir)",
                "temporal_resolution": "20 seconds",
                "note": (
                    "S3 access only — use AWS CLI or boto3 with no-sign-request.  "
                    "Example: aws s3 ls s3://noaa-goes18/GLM-L2-LCFA/ --no-sign-request"
                ),
            },
            {
                "name": "NOHRSC Snow Analysis",
                "endpoint": None,
                "description": (
                    "National Operational Hydrologic Remote Sensing Center daily "
                    "snow analysis grids at 1km resolution.  Includes snow depth, "
                    "SWE, 24-hour snowfall, and snow temperature."
                ),
                "url": "https://www.nohrsc.noaa.gov/",
                "mapserver_url": NOHRSC_MAPSERVER,
                "auth_required": False,
                "temporal_coverage": "2003–present (daily)",
                "spatial_coverage": "CONUS + southern Canada",
                "spatial_resolution": "1 km",
                "temporal_resolution": "Daily",
                "layers": {
                    str(k): {"description": v[0], "unit": v[1]}
                    for k, v in NOHRSC_LAYERS.items()
                },
                "note": (
                    "ArcGIS MapServer — query via identify, export, or REST API.  "
                    "Example: {mapserver}/identify?geometry=-122,45&geometryType="
                    "esriGeometryPoint&layers=all&f=json"
                ),
            },
            {
                "name": "Open-Meteo Air Quality",
                "endpoint": "/api/satellite/air_quality",
                "description": (
                    "Global gridded air quality analysis and forecasts from "
                    "CAMS and other models.  Hourly resolution, up to 7 days "
                    "of forecast data.  Includes pollutants, UV index, and "
                    "pollen concentrations."
                ),
                "url": "https://open-meteo.com/en/docs/air-quality-api",
                "auth_required": False,
                "temporal_coverage": "Current + 7-day forecast",
                "spatial_coverage": "Global",
                "spatial_resolution": "~11 km (CAMS)",
                "temporal_resolution": "Hourly",
                "parameters": {
                    k: {"description": v[0], "unit": v[1]}
                    for k, v in OPEN_METEO_AQ_PARAMS.items()
                },
            },
        ],
    }
