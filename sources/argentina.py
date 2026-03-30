"""Argentina Servicio Meteorologico Nacional (SMN) — current weather, forecast, and alerts.

Provides real-time observations from ~125 weather stations across Argentina,
1-to-3-day forecasts, and active weather alerts/warnings.

API reference: https://www.smn.gob.ar/
No authentication required.
"""
import logging
from datetime import datetime, timezone
from typing import Optional

import httpx
from fastapi import APIRouter, HTTPException, Query

import cache
import config
import http_client
from models import Reading, Station

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Source registry metadata
# ---------------------------------------------------------------------------
SOURCE_NAME = "Argentina SMN"
SOURCE_DESCRIPTION = (
    "Servicio Meteorologico Nacional — real-time weather observations from ~125 "
    "stations across Argentina, 1-to-3-day forecasts, and active weather alerts."
)
SOURCE_META = {
    "url": "https://www.smn.gob.ar/",
    "auth_required": False,
    "update_frequency": "30 min",
    "coverage": "Argentina",
    "params": ["temperature", "humidity", "wind_speed", "wind_direction", "pressure", "visibility"],
    "category": "weather",
}

router = APIRouter(prefix="/api/argentina", tags=["Argentina SMN"])

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
SMN_WEATHER_URL = "https://ws.smn.gob.ar/map_items/weather"
SMN_FORECAST_URL = "https://ws.smn.gob.ar/map_items/forecast"
SMN_ALERTS_URL = "https://ws.smn.gob.ar/alerts/type"


# ---------------------------------------------------------------------------
# Helper utilities
# ---------------------------------------------------------------------------

def _parse_station(item: dict) -> Station:
    """Convert an SMN weather item into a Station model."""
    station_id = str(item.get("lid", item.get("id", "")))
    name = item.get("name", station_id)
    lat = float(item.get("lat", 0.0))
    lon = float(item.get("lon", 0.0))

    province = item.get("province", "")
    dist = item.get("dist", "")

    # SMN weather endpoint returns current observations; use update timestamp if available
    last_update = None
    update_str = item.get("updated") or item.get("date")
    if update_str:
        try:
            last_update = datetime.fromisoformat(str(update_str).replace("Z", "+00:00"))
        except (ValueError, TypeError):
            pass
    if last_update is None:
        last_update = datetime.now(timezone.utc)

    return Station(
        id=station_id,
        name=name,
        lat=lat,
        lon=lon,
        source="argentina_smn",
        network="SMN",
        params=_extract_params(item.get("weather", {})),
        last_update=last_update,
        url="https://www.smn.gob.ar/",
        extra={
            "province": province,
            "district": dist,
        },
    )


def _extract_params(weather: dict) -> list[str]:
    """Build a list of parameter names from available fields."""
    params: list[str] = []
    field_map = {
        "temp": "temperature",
        "humidity": "humidity",
        "wind_speed": "wind_speed",
        "wind_deg": "wind_direction",
        "pressure": "pressure",
        "visibility": "visibility",
        "st": "feels_like",
    }
    for key, param_name in field_map.items():
        if weather.get(key) is not None:
            params.append(param_name)
    return params


def _parse_readings(item: dict, station_id: str) -> list[Reading]:
    """Extract readings from an SMN weather item."""
    weather = item.get("weather", {})
    if not weather:
        return []

    now = datetime.now(timezone.utc)
    readings: list[Reading] = []

    field_map = {
        "temp": ("temperature", "°C"),
        "humidity": ("humidity", "%"),
        "wind_speed": ("wind_speed", "km/h"),
        "wind_deg": ("wind_direction", "°"),
        "pressure": ("pressure", "hPa"),
        "visibility": ("visibility", "km"),
        "st": ("feels_like", "°C"),
    }

    for key, (param_name, unit) in field_map.items():
        raw = weather.get(key)
        if raw is None or raw == "":
            continue
        try:
            value = float(raw)
        except (TypeError, ValueError):
            continue

        readings.append(
            Reading(
                station_id=station_id,
                timestamp=now,
                parameter=param_name,
                value=value,
                unit=unit,
                source="argentina_smn",
            )
        )

    # Description (weather condition text) stored as extra info, not a numeric reading
    return readings


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@router.get("/current", summary="Current weather observations from Argentina SMN")
async def current_weather():
    """Return current weather observations from all ~125 Argentina SMN stations.

    Each station includes parsed Station metadata and its latest readings
    (temperature, humidity, wind speed/direction, pressure, visibility, feels-like).
    """
    cache_key = "argentina:current"
    cached = await cache.get(cache_key)
    if cached is not None:
        return cached

    try:
        data = await http_client.fetch_json(SMN_WEATHER_URL, source="argentina_smn")
    except http_client.CircuitOpenError as exc:
        raise HTTPException(status_code=503, detail=str(exc))
    except httpx.TimeoutException:
        raise HTTPException(status_code=504, detail="Argentina SMN API timeout")
    except httpx.HTTPStatusError as exc:
        raise HTTPException(
            status_code=exc.response.status_code,
            detail=f"Argentina SMN API error: {exc}",
        )
    except Exception as exc:
        logger.exception("Unexpected error fetching Argentina SMN current weather")
        raise HTTPException(status_code=500, detail=str(exc))

    if not isinstance(data, list):
        raise HTTPException(status_code=502, detail="Unexpected response format from Argentina SMN")

    stations = []
    all_readings = []
    for item in data:
        station = _parse_station(item)
        readings = _parse_readings(item, station.id)
        station.params = _extract_params(item.get("weather", {}))
        stations.append(station.model_dump())
        all_readings.extend([r.model_dump() for r in readings])

    result = {
        "source": "argentina_smn",
        "station_count": len(stations),
        "stations": stations,
        "readings": all_readings,
    }
    await cache.set(cache_key, result, ttl=config.CACHE_TTL)
    return result


@router.get("/forecast", summary="Argentina SMN forecast")
async def forecast(
    days: int = Query(1, ge=1, le=3, description="Forecast range: 1, 2, or 3 days"),
):
    """Return forecast data from the Argentina SMN.

    Supports 1-day, 2-day, or 3-day forecasts. Each item includes location
    metadata and forecast conditions (temperature ranges, weather description,
    wind, etc.).
    """
    cache_key = f"argentina:forecast:{days}"
    cached = await cache.get(cache_key)
    if cached is not None:
        return cached

    url = f"{SMN_FORECAST_URL}/{days}"

    try:
        data = await http_client.fetch_json(url, source="argentina_smn")
    except http_client.CircuitOpenError as exc:
        raise HTTPException(status_code=503, detail=str(exc))
    except httpx.TimeoutException:
        raise HTTPException(status_code=504, detail="Argentina SMN forecast API timeout")
    except httpx.HTTPStatusError as exc:
        raise HTTPException(
            status_code=exc.response.status_code,
            detail=f"Argentina SMN forecast error: {exc}",
        )
    except Exception as exc:
        logger.exception("Error fetching Argentina SMN %d-day forecast", days)
        raise HTTPException(status_code=500, detail=str(exc))

    if not isinstance(data, list):
        raise HTTPException(status_code=502, detail="Unexpected response format from Argentina SMN")

    result = {
        "source": "argentina_smn",
        "days": days,
        "count": len(data),
        "forecasts": data,
    }
    await cache.set(cache_key, result, ttl=config.CACHE_TTL_LONG)
    return result


@router.get("/alerts", summary="Active weather alerts from Argentina SMN")
async def alerts(
    alert_type: str = Query(
        "AL",
        description="Alert type: 'AL' for alerts, 'AC' for short-term warnings",
        pattern="^(AL|AC)$",
    ),
):
    """Return active weather alerts and warnings from the Argentina SMN.

    - `AL` — standard alerts (longer-duration, broader area)
    - `AC` — short-term warnings (avisos a corto plazo)
    """
    cache_key = f"argentina:alerts:{alert_type}"
    cached = await cache.get(cache_key)
    if cached is not None:
        return cached

    url = f"{SMN_ALERTS_URL}/{alert_type}"

    try:
        data = await http_client.fetch_json(url, source="argentina_smn")
    except http_client.CircuitOpenError as exc:
        raise HTTPException(status_code=503, detail=str(exc))
    except httpx.TimeoutException:
        raise HTTPException(status_code=504, detail="Argentina SMN alerts API timeout")
    except httpx.HTTPStatusError as exc:
        if exc.response.status_code == 404:
            data = []
        else:
            raise HTTPException(
                status_code=exc.response.status_code,
                detail=f"Argentina SMN alerts error: {exc}",
            )
    except Exception as exc:
        logger.exception("Error fetching Argentina SMN alerts")
        raise HTTPException(status_code=500, detail=str(exc))

    if not isinstance(data, list):
        # Alerts endpoint may return a dict wrapper or an empty object when none active
        if isinstance(data, dict):
            data = data.get("alerts", data.get("data", []))
        else:
            data = []

    result = {
        "source": "argentina_smn",
        "alert_type": alert_type,
        "count": len(data),
        "alerts": data,
    }
    await cache.set(cache_key, result, ttl=config.CACHE_TTL)
    return result
