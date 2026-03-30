"""Hong Kong Observatory — open weather data API.

Provides real-time temperature, humidity, and rainfall observations from ~40
stations across Hong Kong, plus 9-day forecasts and active weather warnings.

API reference: https://data.weather.gov.hk/weatherAPI/doc/HKO_Open_Data_API_Documentation.pdf
"""
import logging
from datetime import datetime, timezone
from typing import Optional

import httpx
from fastapi import APIRouter, HTTPException, Query

import cache
import config
import http_client
from models import Reading, Station, StationDetail

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Source registry metadata
# ---------------------------------------------------------------------------
SOURCE_NAME = "Hong Kong Observatory"
SOURCE_DESCRIPTION = (
    "Hong Kong Observatory — real-time temperature, humidity, and rainfall "
    "observations from ~40 automatic weather stations across Hong Kong, "
    "plus 9-day forecasts and active weather warnings."
)
SOURCE_META = {
    "url": "https://data.weather.gov.hk/weatherAPI/",
    "auth_required": False,
    "update_frequency": "1 min",
    "coverage": "Hong Kong",
    "params": ["temperature", "humidity", "rainfall"],
    "category": "weather",
}

router = APIRouter(prefix="/api/hong_kong", tags=["Hong Kong Observatory"])

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
HKO_BASE = "https://data.weather.gov.hk/weatherAPI/opendata/weather.php"

# Approximate coordinates for known HKO station names.
# The rhrread endpoint does not return lat/lon, so we map them here.
# Covers major stations; unknown stations get a central HK fallback.
_STATION_COORDS: dict[str, tuple[float, float]] = {
    "Hong Kong Observatory":          (22.3019, 114.1742),
    "King's Park":                    (22.3119, 114.1728),
    "Wong Chuk Hang":                 (22.2480, 114.1740),
    "Ta Kwu Ling":                    (22.5288, 114.1567),
    "Lau Fau Shan":                   (22.4686, 113.9839),
    "Tai Po":                         (22.4445, 114.1790),
    "Sha Tin":                        (22.4025, 114.2101),
    "Tuen Mun":                       (22.3858, 113.9645),
    "Tseung Kwan O":                  (22.3157, 114.2556),
    "Sai Kung":                       (22.3755, 114.2742),
    "Cheung Chau":                    (22.2013, 114.0268),
    "Chek Lap Kok":                   (22.3089, 113.9216),
    "Tsing Yi":                       (22.3440, 114.1100),
    "Shek Kong":                      (22.4360, 114.0845),
    "Tsuen Wan Ho Koon":              (22.3838, 114.1074),
    "Tsuen Wan Shing Mun Valley":     (22.3757, 114.1025),
    "Hong Kong Park":                 (22.2780, 114.1622),
    "Shau Kei Wan":                   (22.2794, 114.2360),
    "Kowloon City":                   (22.3350, 114.1847),
    "Happy Valley":                   (22.2718, 114.1837),
    "Wong Tai Sin":                   (22.3425, 114.1929),
    "Stanley":                        (22.2193, 114.2143),
    "Kwun Tong":                      (22.3124, 114.2263),
    "Sham Shui Po":                   (22.3308, 114.1590),
    "Kai Tak Runway Park":            (22.3049, 114.2134),
    "Yuen Long Park":                 (22.4434, 114.0207),
    "Tai Mei Tuk":                    (22.4750, 114.2370),
    "Tate's Cairn":                   (22.3575, 114.2173),
    "Pak Tam Chung":                  (22.4017, 114.3217),
    "Waglan Island":                  (22.1822, 114.3033),
    "Green Island":                   (22.2844, 114.1118),
    "Star Ferry":                     (22.2933, 114.1686),
    "Ngong Ping":                     (22.2589, 113.9069),
    "Peng Chau":                      (22.2907, 114.0393),
    "Sha Tin (Hong Kong Heritage Museum)": (22.3760, 114.1854),
    "Central & Western District Promenade": (22.2867, 114.1563),
    "North Point":                    (22.2930, 114.2006),
    "Ping Chau":                      (22.5343, 114.3295),
    "Tai Lung":                       (22.3981, 114.1200),
    "Wetland Park":                   (22.4680, 114.0088),
    "Yau Ma Tei":                     (22.3133, 114.1704),
    "Wan Chai":                       (22.2783, 114.1747),
    "Deep Water Bay":                 (22.2439, 114.1820),
    "Aberdeen":                       (22.2479, 114.1555),
    "Causeway Bay":                   (22.2803, 114.1850),
}

_HK_FALLBACK = (22.302, 114.174)  # central Hong Kong


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _station_coords(name: str) -> tuple[float, float]:
    """Look up lat/lon for a station name, falling back to central HK."""
    return _STATION_COORDS.get(name, _HK_FALLBACK)


def _parse_hko_datetime(date_str: str) -> datetime:
    """Parse an HKO date string like '2024-01-15T14:00:00+08:00' into a datetime."""
    try:
        return datetime.fromisoformat(date_str)
    except (ValueError, TypeError):
        return datetime.now(timezone.utc)


def _parse_current_weather(data: dict) -> tuple[list[Station], dict[str, list[Reading]]]:
    """Parse the rhrread response into Station and Reading objects.

    The rhrread endpoint returns separate arrays for temperature, humidity,
    and rainfall, each with their own station names.  We unify them by
    station name.
    """
    stations_map: dict[str, Station] = {}
    readings_map: dict[str, list[Reading]] = {}

    update_time = data.get("updateTime", "")
    ts = _parse_hko_datetime(update_time) if update_time else datetime.now(timezone.utc)

    # --- Temperature ---
    for item in data.get("temperature", {}).get("data", []):
        name = item.get("place", "")
        if not name:
            continue
        station_id = name.replace(" ", "_").lower()
        value = item.get("value")
        unit = item.get("unit", "C")
        if value is None:
            continue

        if station_id not in stations_map:
            lat, lon = _station_coords(name)
            stations_map[station_id] = Station(
                id=station_id,
                name=name,
                lat=lat,
                lon=lon,
                source="hong_kong",
                network="HKO",
                params=[],
                last_update=ts,
                url=f"https://www.hko.gov.hk/en/wxinfo/ts/index.htm",
            )
        stations_map[station_id].params.append("Temperature")
        readings_map.setdefault(station_id, []).append(
            Reading(
                station_id=station_id,
                timestamp=ts,
                parameter="Temperature",
                value=float(value),
                unit=f"°{unit}",
                source="hong_kong",
            )
        )

    # --- Humidity ---
    for item in data.get("humidity", {}).get("data", []):
        name = item.get("place", "")
        if not name:
            continue
        station_id = name.replace(" ", "_").lower()
        value = item.get("value")
        unit = item.get("unit", "percent")
        if value is None:
            continue

        if station_id not in stations_map:
            lat, lon = _station_coords(name)
            stations_map[station_id] = Station(
                id=station_id,
                name=name,
                lat=lat,
                lon=lon,
                source="hong_kong",
                network="HKO",
                params=[],
                last_update=ts,
                url=f"https://www.hko.gov.hk/en/wxinfo/ts/index.htm",
            )
        stations_map[station_id].params.append("Humidity")
        readings_map.setdefault(station_id, []).append(
            Reading(
                station_id=station_id,
                timestamp=ts,
                parameter="Humidity",
                value=float(value),
                unit="%",
                source="hong_kong",
            )
        )

    # --- Rainfall ---
    for item in data.get("rainfall", {}).get("data", []):
        name = item.get("place", "")
        if not name:
            continue
        station_id = name.replace(" ", "_").lower()
        main_val = item.get("main", "")
        unit = item.get("unit", "mm")
        if main_val == "":
            continue
        # Rainfall "main" can be a string like "0" or a trace indicator
        try:
            value = float(main_val)
        except (TypeError, ValueError):
            continue

        if station_id not in stations_map:
            lat, lon = _station_coords(name)
            stations_map[station_id] = Station(
                id=station_id,
                name=name,
                lat=lat,
                lon=lon,
                source="hong_kong",
                network="HKO",
                params=[],
                last_update=ts,
                url=f"https://www.hko.gov.hk/en/wxinfo/ts/index.htm",
            )
        if "Rainfall" not in stations_map[station_id].params:
            stations_map[station_id].params.append("Rainfall")
        readings_map.setdefault(station_id, []).append(
            Reading(
                station_id=station_id,
                timestamp=ts,
                parameter="Rainfall",
                value=value,
                unit=unit,
                source="hong_kong",
            )
        )

    stations = list(stations_map.values())
    return stations, readings_map


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@router.get("/current", summary="Current weather from all HKO stations")
async def current_weather():
    """Return current temperature, humidity, and rainfall from all
    Hong Kong Observatory automatic weather stations (~40 stations).
    """
    cache_key = "hong_kong:current"
    cached = await cache.get(cache_key)
    if cached is not None:
        return cached

    try:
        data = await http_client.fetch_json(
            HKO_BASE,
            params={"dataType": "rhrread", "lang": "en"},
            source="hong_kong",
        )
    except http_client.CircuitOpenError as exc:
        raise HTTPException(status_code=503, detail=str(exc))
    except httpx.TimeoutException:
        raise HTTPException(status_code=504, detail="HKO API timeout")
    except httpx.HTTPStatusError as exc:
        raise HTTPException(
            status_code=exc.response.status_code,
            detail=f"HKO API error: {exc}",
        )
    except Exception as exc:
        logger.exception("Error fetching HKO current weather")
        raise HTTPException(status_code=500, detail=str(exc))

    stations, readings_map = _parse_current_weather(data)

    # Build per-station detail list
    station_details = []
    for station in stations:
        readings = readings_map.get(station.id, [])
        detail = StationDetail(station=station, readings=readings)
        station_details.append(detail.model_dump())

    result = {
        "source": "hong_kong",
        "update_time": data.get("updateTime"),
        "station_count": len(stations),
        "stations": station_details,
    }
    await cache.set(cache_key, result, ttl=config.CACHE_TTL)
    return result


@router.get("/forecast", summary="9-day weather forecast for Hong Kong")
async def forecast():
    """Return the Hong Kong Observatory 9-day general weather forecast."""
    cache_key = "hong_kong:forecast"
    cached = await cache.get(cache_key)
    if cached is not None:
        return cached

    try:
        data = await http_client.fetch_json(
            HKO_BASE,
            params={"dataType": "fnd", "lang": "en"},
            source="hong_kong",
        )
    except http_client.CircuitOpenError as exc:
        raise HTTPException(status_code=503, detail=str(exc))
    except httpx.TimeoutException:
        raise HTTPException(status_code=504, detail="HKO API timeout")
    except httpx.HTTPStatusError as exc:
        raise HTTPException(
            status_code=exc.response.status_code,
            detail=f"HKO API error: {exc}",
        )
    except Exception as exc:
        logger.exception("Error fetching HKO 9-day forecast")
        raise HTTPException(status_code=500, detail=str(exc))

    # Parse the forecast days into a cleaner structure
    days = []
    for day in data.get("weatherForecast", []):
        days.append({
            "date": day.get("forecastDate", ""),
            "week": day.get("week", ""),
            "wind": day.get("forecastWind", ""),
            "weather": day.get("forecastWeather", ""),
            "temp_min": day.get("forecastMintemp", {}).get("value"),
            "temp_max": day.get("forecastMaxtemp", {}).get("value"),
            "temp_unit": day.get("forecastMintemp", {}).get("unit", "C"),
            "humidity_min": day.get("forecastMinrh", {}).get("value"),
            "humidity_max": day.get("forecastMaxrh", {}).get("value"),
            "icon": day.get("ForecastIcon"),
            "psr": day.get("PSR", ""),  # probability of significant rain
        })

    # Also include the general situation text
    result = {
        "source": "hong_kong",
        "update_time": data.get("updateTime"),
        "general_situation": data.get("generalSituation", ""),
        "days": days,
    }
    await cache.set(cache_key, result, ttl=config.CACHE_TTL)
    return result


@router.get("/warnings", summary="Active HKO weather warnings")
async def warnings():
    """Return currently active weather warnings issued by the
    Hong Kong Observatory (typhoon signals, rainstorm, thunderstorm, etc.).
    """
    cache_key = "hong_kong:warnings"
    cached = await cache.get(cache_key)
    if cached is not None:
        return cached

    try:
        data = await http_client.fetch_json(
            HKO_BASE,
            params={"dataType": "warnsum", "lang": "en"},
            source="hong_kong",
        )
    except http_client.CircuitOpenError as exc:
        raise HTTPException(status_code=503, detail=str(exc))
    except httpx.TimeoutException:
        raise HTTPException(status_code=504, detail="HKO API timeout")
    except httpx.HTTPStatusError as exc:
        raise HTTPException(
            status_code=exc.response.status_code,
            detail=f"HKO API error: {exc}",
        )
    except Exception as exc:
        logger.exception("Error fetching HKO warnings")
        raise HTTPException(status_code=500, detail=str(exc))

    # The warnsum response is a dict where each key is a warning type
    # and the value contains name, code, actionCode, issueTime, etc.
    # An empty dict means no active warnings.
    active_warnings = []
    if isinstance(data, dict):
        for warning_type, details in data.items():
            if not isinstance(details, dict):
                continue
            active_warnings.append({
                "type": warning_type,
                "name": details.get("name", warning_type),
                "code": details.get("code", ""),
                "action_code": details.get("actionCode", ""),
                "issue_time": details.get("issueTime", ""),
                "update_time": details.get("updateTime", ""),
            })

    result = {
        "source": "hong_kong",
        "warning_count": len(active_warnings),
        "warnings": active_warnings,
    }
    await cache.set(cache_key, result, ttl=config.CACHE_TTL)
    return result
