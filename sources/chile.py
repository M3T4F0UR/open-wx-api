"""Chile MeteoChile — EMA stations, recent observations, and WIS2 climate data.

Provides access to Chile's Dirección Meteorológica de Chile (DMC) network of
~300 automatic weather stations (EMA) with real-time observations, plus the
WIS2-compliant OGC API for monthly/daily climate data in WMO format.

MeteoChile API requires a free token (email + token auth params).
WIS2 OGC API is open — no authentication required.

API reference:
  - MeteoChile: https://climatologia.meteochile.gob.cl/
  - WIS2 OGC:   https://wischile.meteochile.gob.cl/oapi/
"""

import logging
import os
from datetime import datetime, timezone
from typing import Any, Optional

import httpx
from fastapi import APIRouter, HTTPException, Path, Query

import cache
import config
import http_client
from models import Reading, Station

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Source registry metadata  (read by server.py /api/sources)
# ---------------------------------------------------------------------------

SOURCE_NAME = "Chile MeteoChile"
SOURCE_DESCRIPTION = (
    "Dirección Meteorológica de Chile (DMC) — ~300 automatic weather stations "
    "(EMA) with real-time observations across Chile, plus WIS2 OGC API for "
    "monthly/daily climate observations in WMO format.  MeteoChile endpoints "
    "require a free token; WIS2 OGC API is open."
)
SOURCE_META = {
    "url": "https://climatologia.meteochile.gob.cl/",
    "auth_required": True,
    "update_frequency": "10–60 min (EMA)",
    "coverage": "Chile",
    "params": [
        "temperature", "humidity", "wind_speed", "wind_direction",
        "pressure", "precipitation", "solar_radiation",
    ],
    "category": "weather",
    "station_count": 300,
}

router = APIRouter(prefix="/api/chile", tags=["Chile"])

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

METEOCHILE_USER = os.getenv("METEOCHILE_USER", "")
METEOCHILE_TOKEN = os.getenv("METEOCHILE_TOKEN", "")

# MeteoChile API base URLs
_MC_BASE = "https://climatologia.meteochile.gob.cl/application"
_STATIONS_GEO_URL = f"{_MC_BASE}/geoservicios/getCatastroEstacionesGeo"
_STATIONS_EMA_URL = f"{_MC_BASE}/productos/estacionesRedEma"
_RECENT_EMA_URL = f"{_MC_BASE}/productos/datosRecientesEma"  # /{station_code}

# WIS2 OGC API (no auth)
_WIS2_BASE = "https://wischile.meteochile.gob.cl/oapi"
_WIS2_COLLECTIONS_URL = f"{_WIS2_BASE}/collections"

# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------

_HEADERS = {
    "User-Agent": config.USER_AGENT,
    "Accept": "application/json",
}


def _auth_params() -> dict[str, str]:
    """Return MeteoChile auth query parameters."""
    return {"usuario": METEOCHILE_USER, "token": METEOCHILE_TOKEN}


def _has_credentials() -> bool:
    return bool(METEOCHILE_USER and METEOCHILE_TOKEN)


def _client() -> httpx.AsyncClient:
    return httpx.AsyncClient(
        headers=_HEADERS,
        timeout=config.HTTP_TIMEOUT,
        follow_redirects=True,
    )


async def _mc_get(url: str, extra_params: dict[str, str] | None = None) -> Any:
    """Fetch from MeteoChile API with auth params.  Raises HTTPException on error."""
    if not _has_credentials():
        raise HTTPException(
            status_code=503,
            detail="MeteoChile credentials not configured (set METEOCHILE_USER and METEOCHILE_TOKEN)",
        )
    params = _auth_params()
    if extra_params:
        params.update(extra_params)
    try:
        async with _client() as c:
            resp = await c.get(url, params=params)
            resp.raise_for_status()
            return resp.json()
    except httpx.TimeoutException:
        raise HTTPException(status_code=504, detail="MeteoChile API timeout")
    except httpx.HTTPStatusError as exc:
        raise HTTPException(
            status_code=exc.response.status_code,
            detail=f"MeteoChile API error: {exc}",
        )
    except Exception as exc:
        logger.exception("Error fetching from MeteoChile")
        raise HTTPException(status_code=502, detail=str(exc))


async def _wis2_get(url: str, params: dict[str, str] | None = None) -> Any:
    """Fetch from WIS2 OGC API (no auth).  Raises HTTPException on error."""
    try:
        async with _client() as c:
            resp = await c.get(url, params=params)
            resp.raise_for_status()
            return resp.json()
    except httpx.TimeoutException:
        raise HTTPException(status_code=504, detail="WIS2 Chile OGC API timeout")
    except httpx.HTTPStatusError as exc:
        raise HTTPException(
            status_code=exc.response.status_code,
            detail=f"WIS2 Chile API error: {exc}",
        )
    except Exception as exc:
        logger.exception("Error fetching from WIS2 Chile OGC API")
        raise HTTPException(status_code=502, detail=str(exc))


# ---------------------------------------------------------------------------
# Parsing helpers
# ---------------------------------------------------------------------------

def _safe_float(val: Any, default: float | None = None) -> float | None:
    """Convert a value to float, returning *default* on failure."""
    if val is None:
        return default
    try:
        return float(val)
    except (TypeError, ValueError):
        return default


def _parse_station_geo(feature: dict) -> Station:
    """Parse a GeoJSON Feature from the station catalog into a Station model."""
    props = feature.get("properties", {})
    geom = feature.get("geometry", {})
    coords = geom.get("coordinates", [0.0, 0.0])

    station_id = str(props.get("codigo", props.get("codigoNacional", "")))
    name = props.get("nombre", station_id)
    lon = _safe_float(coords[0] if len(coords) > 0 else None, 0.0)
    lat = _safe_float(coords[1] if len(coords) > 1 else None, 0.0)
    elevation = _safe_float(coords[2] if len(coords) > 2 else props.get("altitud"))

    return Station(
        id=station_id,
        name=name or station_id,
        lat=lat,
        lon=lon,
        source="chile_meteochile",
        network=props.get("institucion", "MeteoChile"),
        params=_station_params(props),
        elevation=elevation,
        url=f"https://climatologia.meteochile.gob.cl/",
        extra={
            k: v
            for k, v in props.items()
            if k not in ("codigo", "codigoNacional", "nombre", "altitud", "institucion")
            and v is not None
        },
    )


def _station_params(props: dict) -> list[str]:
    """Infer available parameters from station properties/type."""
    # EMA stations typically report all standard surface params
    return [
        "temperature", "humidity", "wind_speed", "wind_direction",
        "pressure", "precipitation",
    ]


def _parse_readings(data: Any, station_code: str) -> list[Reading]:
    """Parse recent EMA observation data into Reading objects."""
    readings: list[Reading] = []
    if not data:
        return readings

    # MeteoChile returns a list of observation records or a dict with data key
    records = data if isinstance(data, list) else data.get("datos", data.get("data", []))
    if not isinstance(records, list):
        return readings

    field_map = {
        "temperatura": ("temperature", "\u00b0C"),
        "humedad": ("humidity", "%"),
        "presion": ("pressure", "hPa"),
        "velocidadViento": ("wind_speed", "km/h"),
        "direccionViento": ("wind_direction", "\u00b0"),
        "precipitacion": ("precipitation", "mm"),
        "radiacionSolar": ("solar_radiation", "W/m\u00b2"),
    }

    for record in records:
        ts_raw = record.get("momento") or record.get("fechaHora")
        try:
            ts = datetime.fromisoformat(str(ts_raw).replace("Z", "+00:00")) if ts_raw else datetime.now(timezone.utc)
        except (TypeError, ValueError):
            ts = datetime.now(timezone.utc)

        for api_key, (param_name, unit) in field_map.items():
            val = _safe_float(record.get(api_key))
            if val is None:
                continue
            readings.append(
                Reading(
                    station_id=station_code,
                    timestamp=ts,
                    parameter=param_name,
                    value=val,
                    unit=unit,
                    source="chile_meteochile",
                )
            )

    return readings


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@router.get("/stations", summary="Chile MeteoChile — station catalog (GeoJSON)")
async def stations():
    """Return the MeteoChile station catalog as GeoJSON (~300 EMA stations).

    Requires METEOCHILE_USER and METEOCHILE_TOKEN environment variables.
    """
    cache_key = "chile:stations"
    cached = await cache.get(cache_key)
    if cached is not None:
        return cached

    data = await _mc_get(_STATIONS_GEO_URL)

    # Response is GeoJSON FeatureCollection
    features = []
    if isinstance(data, dict):
        features = data.get("features", [])
    elif isinstance(data, list):
        features = data

    station_list = []
    for feature in features:
        try:
            station_list.append(_parse_station_geo(feature).model_dump())
        except Exception:
            logger.debug("Skipping unparseable station feature", exc_info=True)
            continue

    result = {
        "source": "chile_meteochile",
        "count": len(station_list),
        "stations": station_list,
        "raw_geojson": data,
    }
    await cache.set(cache_key, result, ttl=config.CACHE_TTL_LONG)
    return result


@router.get("/current/{station_code}", summary="Chile MeteoChile — recent EMA observations")
async def current(
    station_code: str = Path(..., description="MeteoChile EMA station code"),
):
    """Return recent observations from a specific EMA station.

    Requires METEOCHILE_USER and METEOCHILE_TOKEN environment variables.
    """
    cache_key = f"chile:current:{station_code}"
    cached = await cache.get(cache_key)
    if cached is not None:
        return cached

    url = f"{_RECENT_EMA_URL}/{station_code}"
    data = await _mc_get(url)

    readings = _parse_readings(data, station_code)

    result = {
        "source": "chile_meteochile",
        "station_code": station_code,
        "reading_count": len(readings),
        "readings": [r.model_dump() for r in readings],
        "raw": data,
    }
    await cache.set(cache_key, result, ttl=config.CACHE_TTL)
    return result


@router.get("/wis2/collections", summary="Chile WIS2 OGC API — collections")
async def wis2_collections():
    """Return available WIS2 data collections from Chile's OGC API.

    No authentication required.  Returns monthly/daily climate observation
    collections in WMO format.
    """
    cache_key = "chile:wis2:collections"
    cached = await cache.get(cache_key)
    if cached is not None:
        return cached

    data = await _wis2_get(_WIS2_COLLECTIONS_URL, params={"f": "json"})

    collections = []
    if isinstance(data, dict):
        collections = data.get("collections", [])

    result = {
        "source": "chile_wis2",
        "count": len(collections),
        "collections": collections,
    }
    await cache.set(cache_key, result, ttl=config.CACHE_TTL_LONG)
    return result


@router.get("/info", summary="Chile MeteoChile — source info & key status")
async def info():
    """Return summary information about the Chile data sources and API key status."""
    return {
        "module": SOURCE_NAME,
        "description": SOURCE_DESCRIPTION,
        "credentials_configured": _has_credentials(),
        "sources": [
            {
                "id": "chile_meteochile",
                "name": "Chile MeteoChile (DMC)",
                "description": (
                    "~300 automatic weather stations (EMA) across Chile with "
                    "real-time observations.  Requires free token."
                ),
                "url": "https://climatologia.meteochile.gob.cl/",
                "auth_required": True,
                "update_frequency": "10–60 min",
                "coverage": "Chile (national)",
                "station_count": "~300",
                "params": SOURCE_META["params"],
                "endpoints": [
                    "GET /api/chile/stations",
                    "GET /api/chile/current/{station_code}",
                ],
            },
            {
                "id": "chile_wis2",
                "name": "Chile WIS2 OGC API",
                "description": (
                    "WIS2-compliant OGC API serving monthly/daily climate "
                    "observations in WMO format.  No authentication required."
                ),
                "url": "https://wischile.meteochile.gob.cl/oapi/",
                "auth_required": False,
                "update_frequency": "daily/monthly",
                "coverage": "Chile (national)",
                "params": ["temperature", "precipitation", "humidity", "wind_speed", "pressure"],
                "endpoints": [
                    "GET /api/chile/wis2/collections",
                ],
            },
        ],
    }
