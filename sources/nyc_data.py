"""City Open Data — NYC, Chicago, Boston, Philadelphia.

Aggregates public environmental data from municipal open-data portals:

1. NYC Harbor Water Quality (Socrata SODA API)
   40+ harbor monitoring sites — dissolved oxygen, bacteria, temperature,
   salinity, turbidity.
   Endpoint: https://data.cityofnewyork.us/resource/5uug-f49n.json

2. NYC Real-Time Air Quality (NYCCAS)
   Street-level PM2.5 monitoring across New York City.
   Portal: https://a816-dohbesp.nyc.gov/IndicatorPublic/data-features/realtime-air-quality/

3. NYC Beach Water Quality (Socrata SODA API)
   Bacteria levels at NYC public beaches.
   Endpoint: https://data.cityofnewyork.us/resource/2xir-kwzz.json

4. Chicago Beach Weather (Socrata SODA API)
   ~30 beach sensors — air temp, humidity, rain, pressure, wind.
   Endpoint: https://data.cityofchicago.org/resource/k7hf-8y75.json

5. Boston Rainfall (CKAN)
   Rain gauge data from the City of Boston open-data portal.
   Endpoint: https://data.boston.gov/api/3/action/package_show?id=rainfall-data

6. Philadelphia Rain Gauges (ArcGIS REST)
   24 tipping-bucket rain gauges with 2.5-minute resolution.

All APIs are free with no authentication required.
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Optional

from fastapi import APIRouter, HTTPException, Query

import cache
import config
import http_client
from models import Station, Reading, StationDetail

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Source registry metadata
# ---------------------------------------------------------------------------
SOURCE_NAME = "City Open Data"
SOURCE_DESCRIPTION = (
    "Municipal environmental data from NYC, Chicago, Boston, and Philadelphia "
    "open-data portals — harbor water quality, beach sensors, air quality, "
    "and rain gauge networks."
)
SOURCE_META = {
    "category": "environmental/municipal",
    "coverage": "NYC, Chicago, Boston, Philadelphia",
    "auth_required": False,
    "update_frequency": "Varies (hourly to daily)",
    "params": [
        "dissolved_oxygen", "bacteria", "temperature", "salinity", "turbidity",
        "pm2.5", "air_temp", "humidity", "rain_intensity", "barometric_pressure",
        "wind_speed", "wind_direction", "rainfall",
    ],
    "networks": ["nyc_harbor", "nyc_air", "nyc_beach", "chicago_beach", "boston_rain", "philly_rain"],
    "urls": {
        "nyc_harbor": "https://data.cityofnewyork.us/resource/5uug-f49n.json",
        "nyc_air": "https://a816-dohbesp.nyc.gov/IndicatorPublic/data-features/realtime-air-quality/",
        "nyc_beach": "https://data.cityofnewyork.us/resource/2xir-kwzz.json",
        "chicago_beach": "https://data.cityofchicago.org/resource/k7hf-8y75.json",
        "boston_rain": "https://data.boston.gov/api/3/action/package_show?id=rainfall-data",
    },
}

router = APIRouter(prefix="/api/city_data", tags=["City Open Data"])

# ---------------------------------------------------------------------------
# API base URLs
# ---------------------------------------------------------------------------
NYC_HARBOR_URL = "https://data.cityofnewyork.us/resource/5uug-f49n.json"
NYC_BEACH_URL = "https://data.cityofnewyork.us/resource/2xir-kwzz.json"
NYC_AIR_URL = "https://a816-dohbesp.nyc.gov/IndicatorPublic/data-features/realtime-air-quality/data/realtime-air-quality.json"
CHICAGO_BEACH_URL = "https://data.cityofchicago.org/resource/k7hf-8y75.json"
BOSTON_RAIN_META_URL = "https://data.boston.gov/api/3/action/package_show"
PHILLY_RAIN_URL = "https://stormwater-api.phila.gov/api/v1/gauges"

# ---------------------------------------------------------------------------
# Helper: safe float/datetime parsing
# ---------------------------------------------------------------------------

def _safe_float(val) -> Optional[float]:
    """Convert a value to float, returning None on failure."""
    if val is None:
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


def _parse_iso(val) -> Optional[datetime]:
    """Parse an ISO 8601 string, returning None on failure."""
    if not val:
        return None
    try:
        if isinstance(val, str):
            return datetime.fromisoformat(val.replace("Z", "+00:00"))
    except (ValueError, AttributeError):
        pass
    return None


# ---------------------------------------------------------------------------
# NYC Harbor Water Quality
# ---------------------------------------------------------------------------

def _parse_harbor_record(rec: dict) -> Station:
    """Convert a NYC harbor water-quality record to a Station."""
    site = rec.get("site", rec.get("site_name", "Unknown Site"))
    station_id = f"nyc_harbor_{site}".replace(" ", "_").lower()

    lat = _safe_float(rec.get("latitude"))
    lon = _safe_float(rec.get("longitude"))
    ts = _parse_iso(rec.get("date") or rec.get("sample_date"))

    return Station(
        id=station_id,
        name=f"NYC Harbor — {site}",
        lat=lat or 40.7,
        lon=lon or -74.0,
        source="nyc_harbor",
        network="NYC DEP",
        params=["dissolved_oxygen", "bacteria", "temperature", "salinity", "turbidity"],
        last_update=ts,
        extra={
            "dissolved_oxygen": _safe_float(rec.get("do", rec.get("dissolved_oxygen"))),
            "bacteria": _safe_float(rec.get("bacteria", rec.get("enterococci"))),
            "temperature": _safe_float(rec.get("temperature", rec.get("water_temp"))),
            "salinity": _safe_float(rec.get("salinity")),
            "turbidity": _safe_float(rec.get("turbidity")),
            "site": site,
        },
    )


@router.get("/nyc/harbor")
async def nyc_harbor_water_quality(
    limit: int = Query(100, description="Number of records (max 5000)"),
    site: Optional[str] = Query(None, description="Filter by site name"),
):
    """NYC harbor water quality from 40+ monitoring sites.

    Returns dissolved oxygen, bacteria, temperature, salinity, and turbidity
    from the NYC DEP harbor survey via the Socrata SODA API.
    """
    cache_key = f"city_data:nyc_harbor:{limit}:{site}"
    cached_val = await cache.get(cache_key)
    if cached_val is not None:
        return cached_val

    params: dict = {
        "$limit": min(limit, 5000),
        "$order": "date DESC",
    }
    if site:
        params["$where"] = f"upper(site) like '%{site.upper()}%'"

    try:
        data = await http_client.fetch_json(
            NYC_HARBOR_URL, params=params, source="nyc_harbor"
        )
    except Exception as exc:
        logger.error("NYC harbor water quality fetch failed: %s", exc)
        raise HTTPException(status_code=502, detail=f"NYC harbor data unavailable: {exc}")

    if not isinstance(data, list):
        data = []

    stations = []
    for rec in data:
        try:
            stations.append(_parse_harbor_record(rec))
        except Exception as exc:
            logger.debug("Skipping malformed harbor record: %s", exc)

    result = {
        "source": "nyc_harbor",
        "count": len(stations),
        "stations": [s.model_dump(mode="json") for s in stations],
    }
    await cache.set(cache_key, result, ttl=config.CACHE_TTL)
    return result


# ---------------------------------------------------------------------------
# NYC Real-Time Air Quality (NYCCAS PM2.5)
# ---------------------------------------------------------------------------

def _parse_air_record(rec: dict) -> Station:
    """Convert a NYC real-time air quality record to a Station."""
    name = rec.get("name", rec.get("location", "Unknown Location"))
    station_id = f"nyc_air_{name}".replace(" ", "_").lower()

    lat = _safe_float(rec.get("latitude", rec.get("lat")))
    lon = _safe_float(rec.get("longitude", rec.get("lon")))
    ts = _parse_iso(rec.get("time", rec.get("date")))

    pm25 = _safe_float(rec.get("pm25", rec.get("pm2.5", rec.get("value"))))

    return Station(
        id=station_id,
        name=f"NYC Air — {name}",
        lat=lat or 40.7,
        lon=lon or -74.0,
        source="nyc_air",
        network="NYCCAS",
        params=["pm2.5"],
        last_update=ts,
        extra={
            "pm2.5": pm25,
            "location": name,
            "aqi_category": rec.get("aqi_category"),
        },
    )


@router.get("/nyc/air_quality")
async def nyc_air_quality():
    """NYC real-time street-level PM2.5 from the NYCCAS network.

    Returns PM2.5 readings from NYC Community Air Survey monitoring sites.
    """
    cache_key = "city_data:nyc_air"
    cached_val = await cache.get(cache_key)
    if cached_val is not None:
        return cached_val

    try:
        data = await http_client.fetch_json(NYC_AIR_URL, source="nyc_air")
    except Exception as exc:
        logger.error("NYC air quality fetch failed: %s", exc)
        raise HTTPException(status_code=502, detail=f"NYC air quality data unavailable: {exc}")

    records = data if isinstance(data, list) else data.get("data", data.get("features", []))

    stations = []
    for rec in records:
        try:
            stations.append(_parse_air_record(rec))
        except Exception as exc:
            logger.debug("Skipping malformed air quality record: %s", exc)

    result = {
        "source": "nyc_air",
        "count": len(stations),
        "stations": [s.model_dump(mode="json") for s in stations],
    }
    await cache.set(cache_key, result, ttl=config.CACHE_TTL)
    return result


# ---------------------------------------------------------------------------
# NYC Beach Water Quality
# ---------------------------------------------------------------------------

def _parse_beach_record(rec: dict) -> Station:
    """Convert a NYC beach water-quality record to a Station."""
    beach = rec.get("beach_name", rec.get("beach", "Unknown Beach"))
    station_id = f"nyc_beach_{beach}".replace(" ", "_").lower()

    lat = _safe_float(rec.get("latitude"))
    lon = _safe_float(rec.get("longitude"))
    ts = _parse_iso(rec.get("sample_date", rec.get("date")))

    return Station(
        id=station_id,
        name=f"NYC Beach — {beach}",
        lat=lat or 40.57,
        lon=lon or -73.97,
        source="nyc_beach",
        network="NYC DOHMH",
        params=["bacteria"],
        last_update=ts,
        extra={
            "bacteria": _safe_float(rec.get("result", rec.get("enterococci"))),
            "beach": beach,
            "sample_location": rec.get("sample_location"),
            "advisory": rec.get("advisory", rec.get("beach_advisory")),
        },
    )


@router.get("/nyc/beach")
async def nyc_beach_water_quality(
    limit: int = Query(100, description="Number of records (max 5000)"),
):
    """NYC beach water quality — bacteria levels at public beaches.

    Returns enterococci bacteria counts from NYC beaches via Socrata SODA API.
    """
    cache_key = f"city_data:nyc_beach:{limit}"
    cached_val = await cache.get(cache_key)
    if cached_val is not None:
        return cached_val

    params = {
        "$limit": min(limit, 5000),
        "$order": "sample_date DESC",
    }

    try:
        data = await http_client.fetch_json(
            NYC_BEACH_URL, params=params, source="nyc_beach"
        )
    except Exception as exc:
        logger.error("NYC beach water quality fetch failed: %s", exc)
        raise HTTPException(status_code=502, detail=f"NYC beach data unavailable: {exc}")

    if not isinstance(data, list):
        data = []

    stations = []
    for rec in data:
        try:
            stations.append(_parse_beach_record(rec))
        except Exception as exc:
            logger.debug("Skipping malformed beach record: %s", exc)

    result = {
        "source": "nyc_beach",
        "count": len(stations),
        "stations": [s.model_dump(mode="json") for s in stations],
    }
    await cache.set(cache_key, result, ttl=config.CACHE_TTL)
    return result


# ---------------------------------------------------------------------------
# Chicago Beach Weather
# ---------------------------------------------------------------------------

def _parse_chicago_beach(rec: dict) -> Station:
    """Convert a Chicago beach weather record to a Station."""
    beach = rec.get("station_name", rec.get("beach", "Unknown Beach"))
    station_id = f"chi_beach_{beach}".replace(" ", "_").lower()

    lat = _safe_float(rec.get("latitude"))
    lon = _safe_float(rec.get("longitude"))
    ts = _parse_iso(rec.get("measurement_timestamp", rec.get("date")))

    return Station(
        id=station_id,
        name=f"Chicago Beach — {beach}",
        lat=lat or 41.88,
        lon=lon or -87.62,
        source="chicago_beach",
        network="Chicago Park District",
        params=["air_temp", "humidity", "rain_intensity", "barometric_pressure",
                "wind_speed", "wind_direction"],
        last_update=ts,
        extra={
            "air_temp": _safe_float(rec.get("air_temperature")),
            "humidity": _safe_float(rec.get("humidity")),
            "rain_intensity": _safe_float(rec.get("rain_intensity")),
            "barometric_pressure": _safe_float(rec.get("barometric_pressure")),
            "wind_speed": _safe_float(rec.get("wind_speed")),
            "wind_direction": _safe_float(rec.get("wind_direction")),
            "wet_bulb_temperature": _safe_float(rec.get("wet_bulb_temperature")),
            "water_temperature": _safe_float(rec.get("water_temperature")),
            "wave_height": _safe_float(rec.get("wave_height")),
            "wave_period": _safe_float(rec.get("wave_period")),
            "battery_life": _safe_float(rec.get("battery_life")),
            "measurement_id": rec.get("measurement_id"),
        },
    )


@router.get("/chicago/beach_weather")
async def chicago_beach_weather(
    limit: int = Query(100, description="Number of records (max 5000)"),
):
    """Chicago beach weather from ~30 lakefront sensors.

    Returns air temperature, humidity, rain intensity, pressure, wind speed
    and direction from Chicago Park District beach weather stations via
    the Socrata SODA API.
    """
    cache_key = f"city_data:chicago_beach:{limit}"
    cached_val = await cache.get(cache_key)
    if cached_val is not None:
        return cached_val

    params = {
        "$limit": min(limit, 5000),
        "$order": "measurement_timestamp DESC",
    }

    try:
        data = await http_client.fetch_json(
            CHICAGO_BEACH_URL, params=params, source="chicago_beach"
        )
    except Exception as exc:
        logger.error("Chicago beach weather fetch failed: %s", exc)
        raise HTTPException(status_code=502, detail=f"Chicago beach data unavailable: {exc}")

    if not isinstance(data, list):
        data = []

    stations = []
    for rec in data:
        try:
            stations.append(_parse_chicago_beach(rec))
        except Exception as exc:
            logger.debug("Skipping malformed Chicago beach record: %s", exc)

    result = {
        "source": "chicago_beach",
        "count": len(stations),
        "stations": [s.model_dump(mode="json") for s in stations],
    }
    await cache.set(cache_key, result, ttl=config.CACHE_TTL)
    return result


# ---------------------------------------------------------------------------
# Boston Rainfall (CKAN)
# ---------------------------------------------------------------------------

@router.get("/boston/rainfall")
async def boston_rainfall():
    """Boston rain gauge data from the City of Boston open-data portal.

    Fetches the rainfall dataset metadata from the CKAN API, locates the
    most recent CSV/JSON resource, and returns parsed rain gauge readings.
    """
    cache_key = "city_data:boston_rain"
    cached_val = await cache.get(cache_key)
    if cached_val is not None:
        return cached_val

    # Step 1: get dataset metadata to find the data resource URL
    try:
        meta = await http_client.fetch_json(
            BOSTON_RAIN_META_URL,
            params={"id": "rainfall-data"},
            source="boston_rain",
        )
    except Exception as exc:
        logger.error("Boston rainfall metadata fetch failed: %s", exc)
        raise HTTPException(status_code=502, detail=f"Boston rainfall data unavailable: {exc}")

    if not meta.get("success"):
        raise HTTPException(status_code=502, detail="Boston CKAN API returned failure")

    dataset = meta.get("result", {})
    resources = dataset.get("resources", [])

    # Find a JSON or CSV resource
    data_url = None
    for res in resources:
        fmt = (res.get("format") or "").upper()
        if fmt in ("JSON", "CSV", "GEOJSON"):
            data_url = res.get("url")
            break

    if not data_url:
        # Return metadata-only response if no downloadable resource is found
        result = {
            "source": "boston_rain",
            "count": 0,
            "dataset_title": dataset.get("title", "Boston Rainfall"),
            "dataset_notes": dataset.get("notes", ""),
            "resources": [
                {
                    "name": r.get("name"),
                    "format": r.get("format"),
                    "url": r.get("url"),
                    "last_modified": r.get("last_modified"),
                }
                for r in resources
            ],
            "stations": [],
        }
        await cache.set(cache_key, result, ttl=config.CACHE_TTL_LONG)
        return result

    # Step 2: fetch the actual data
    try:
        data = await http_client.fetch_json(data_url, source="boston_rain")
    except Exception:
        # May be CSV — try text fallback
        try:
            text = await http_client.fetch_text(data_url, source="boston_rain")
            # Return raw text with metadata
            result = {
                "source": "boston_rain",
                "format": "csv",
                "dataset_title": dataset.get("title", "Boston Rainfall"),
                "raw_preview": text[:2000] if text else "",
                "download_url": data_url,
                "stations": [],
            }
            await cache.set(cache_key, result, ttl=config.CACHE_TTL_LONG)
            return result
        except Exception as exc2:
            logger.error("Boston rainfall data fetch failed: %s", exc2)
            raise HTTPException(
                status_code=502, detail=f"Boston rainfall data unavailable: {exc2}"
            )

    # Parse if JSON list
    stations = []
    records = data if isinstance(data, list) else data.get("result", data.get("records", []))
    for rec in records[:500]:
        try:
            gauge = rec.get("gauge", rec.get("station", rec.get("name", "Unknown")))
            station_id = f"boston_rain_{gauge}".replace(" ", "_").lower()
            lat = _safe_float(rec.get("latitude", rec.get("lat")))
            lon = _safe_float(rec.get("longitude", rec.get("lon")))
            ts = _parse_iso(rec.get("date", rec.get("timestamp")))

            stations.append(Station(
                id=station_id,
                name=f"Boston Rain — {gauge}",
                lat=lat or 42.36,
                lon=lon or -71.06,
                source="boston_rain",
                network="City of Boston",
                params=["rainfall"],
                last_update=ts,
                extra={
                    "rainfall": _safe_float(rec.get("rainfall", rec.get("inches"))),
                    "gauge": gauge,
                },
            ))
        except Exception as exc:
            logger.debug("Skipping malformed Boston rain record: %s", exc)

    result = {
        "source": "boston_rain",
        "count": len(stations),
        "dataset_title": dataset.get("title", "Boston Rainfall"),
        "stations": [s.model_dump(mode="json") for s in stations],
    }
    await cache.set(cache_key, result, ttl=config.CACHE_TTL_LONG)
    return result


# ---------------------------------------------------------------------------
# Philadelphia Rain Gauges (ArcGIS REST)
# ---------------------------------------------------------------------------

@router.get("/philly/rain_gauges")
async def philly_rain_gauges():
    """Philadelphia tipping-bucket rain gauge network.

    Returns data from 24 rain gauges with 2.5-minute resolution via the
    Philadelphia Water Department stormwater API.
    """
    cache_key = "city_data:philly_rain"
    cached_val = await cache.get(cache_key)
    if cached_val is not None:
        return cached_val

    try:
        data = await http_client.fetch_json(PHILLY_RAIN_URL, source="philly_rain")
    except Exception as exc:
        logger.error("Philadelphia rain gauge fetch failed: %s", exc)
        raise HTTPException(
            status_code=502, detail=f"Philadelphia rain gauge data unavailable: {exc}"
        )

    gauges = data if isinstance(data, list) else data.get("data", data.get("gauges", []))

    stations = []
    for rec in gauges:
        try:
            name = rec.get("gauge_name", rec.get("name", "Unknown Gauge"))
            station_id = f"philly_rain_{name}".replace(" ", "_").lower()
            lat = _safe_float(rec.get("latitude", rec.get("lat")))
            lon = _safe_float(rec.get("longitude", rec.get("lon")))
            ts = _parse_iso(rec.get("timestamp", rec.get("last_reading_time")))

            stations.append(Station(
                id=station_id,
                name=f"Philadelphia Rain — {name}",
                lat=lat or 39.95,
                lon=lon or -75.17,
                source="philly_rain",
                network="Philadelphia Water Dept",
                params=["rainfall"],
                last_update=ts,
                extra={
                    "rainfall": _safe_float(rec.get("rainfall", rec.get("value"))),
                    "gauge_name": name,
                    "resolution_minutes": 2.5,
                },
            ))
        except Exception as exc:
            logger.debug("Skipping malformed Philly rain gauge: %s", exc)

    result = {
        "source": "philly_rain",
        "count": len(stations),
        "stations": [s.model_dump(mode="json") for s in stations],
    }
    await cache.set(cache_key, result, ttl=config.CACHE_TTL)
    return result


# ---------------------------------------------------------------------------
# Overview: all city data sources
# ---------------------------------------------------------------------------

@router.get("/overview")
async def city_data_overview():
    """Summary of all available city open-data sources.

    Returns metadata for each integrated city data source including
    API URLs, coverage, and available parameters.
    """
    return {
        "source": "city_open_data",
        "sources": [
            {
                "id": "nyc_harbor",
                "name": "NYC Harbor Water Quality",
                "city": "New York",
                "endpoint": "/api/city_data/nyc/harbor",
                "api_url": NYC_HARBOR_URL,
                "params": ["dissolved_oxygen", "bacteria", "temperature", "salinity", "turbidity"],
                "sites": "40+",
                "auth_required": False,
            },
            {
                "id": "nyc_air",
                "name": "NYC Real-Time Air Quality (NYCCAS)",
                "city": "New York",
                "endpoint": "/api/city_data/nyc/air_quality",
                "api_url": NYC_AIR_URL,
                "params": ["pm2.5"],
                "auth_required": False,
            },
            {
                "id": "nyc_beach",
                "name": "NYC Beach Water Quality",
                "city": "New York",
                "endpoint": "/api/city_data/nyc/beach",
                "api_url": NYC_BEACH_URL,
                "params": ["bacteria"],
                "auth_required": False,
            },
            {
                "id": "chicago_beach",
                "name": "Chicago Beach Weather",
                "city": "Chicago",
                "endpoint": "/api/city_data/chicago/beach_weather",
                "api_url": CHICAGO_BEACH_URL,
                "params": ["air_temp", "humidity", "rain_intensity",
                           "barometric_pressure", "wind_speed", "wind_direction"],
                "sites": "~30",
                "auth_required": False,
            },
            {
                "id": "boston_rain",
                "name": "Boston Rainfall",
                "city": "Boston",
                "endpoint": "/api/city_data/boston/rainfall",
                "api_url": BOSTON_RAIN_META_URL,
                "params": ["rainfall"],
                "auth_required": False,
            },
            {
                "id": "philly_rain",
                "name": "Philadelphia Rain Gauges",
                "city": "Philadelphia",
                "endpoint": "/api/city_data/philly/rain_gauges",
                "api_url": PHILLY_RAIN_URL,
                "params": ["rainfall"],
                "sites": "24",
                "resolution": "2.5 min",
                "auth_required": False,
            },
        ],
    }
