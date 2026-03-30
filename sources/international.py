"""International Weather APIs — no-auth data sources from 8 countries.

Covers:
  1. UK Environment Agency (EA)  — rainfall, ~4,000 stations
  2. Japan JMA AMeDAS            — 1,300 stations, 10-min updates
  3. Germany Bright Sky (DWD)    — CORS-enabled DWD wrapper
  4. Singapore NEA               — multi-parameter, cleanest JSON
  5. Brazil INMET                — 530+ automatic stations
  6. Environment Canada          — SWOB surface obs + hydrometric
  7. Sweden SMHI                 — latest-hour obs, 6 parameters
  8. Overview                    — summary of all sources

All upstream APIs are public and require no authentication.
"""

import logging
from datetime import datetime, timezone, timedelta
from typing import Optional, Any

import httpx
from fastapi import APIRouter, HTTPException, Query

import cache
import config
from models import Reading, Station, StationDetail

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Source registry metadata
# ---------------------------------------------------------------------------
SOURCE_NAME = "International Weather APIs"
SOURCE_DESCRIPTION = (
    "A collection of no-auth international weather and environmental data APIs: "
    "UK Environment Agency rainfall, Japan JMA AMeDAS (1,300 stations), "
    "Germany Bright Sky / DWD, Singapore NEA, Brazil INMET (530+ stations), "
    "Environment Canada SWOB + hydrometric, and Sweden SMHI."
)
SOURCE_META = {
    "url": "https://wxsection.com/",
    "auth_required": False,
    "update_frequency": "5–60 min depending on source",
    "coverage": "UK, Japan, Germany, Singapore, Brazil, Canada, Sweden",
    "params": [
        "rainfall", "temperature", "humidity", "wind_speed", "wind_direction",
        "pressure", "snow_depth", "discharge", "water_level",
    ],
    "category": "weather",
}

router = APIRouter(prefix="/api/international", tags=["International"])

# ---------------------------------------------------------------------------
# Shared HTTP helpers
# ---------------------------------------------------------------------------
_HEADERS = {
    "User-Agent": config.USER_AGENT,
    "Accept": "application/json",
}


def _client() -> httpx.AsyncClient:
    return httpx.AsyncClient(
        headers=_HEADERS,
        timeout=config.HTTP_TIMEOUT,
        follow_redirects=True,
    )


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


# ===========================================================================
# 1. UK ENVIRONMENT AGENCY — RAINFALL
# ===========================================================================
UK_EA_BASE = "https://environment.data.gov.uk/flood-monitoring"

UK_EA_QUALITY_MAP = {
    "Good": "good",
    "Estimated": "estimated",
    "Suspect": "suspect",
    "Missing": "missing",
    "Unchecked": "unchecked",
}


def _uk_ea_parse_station(item: dict) -> Station:
    """Convert an EA flood-monitoring station item to the Station model."""
    station_id = item.get("notation") or item.get("stationReference", "")
    # The station URL in the response is a JSON-LD @id URI
    url = item.get("@id") or f"{UK_EA_BASE}/id/stations/{station_id}"
    # Lat/lon may be in eaRegionName or in lat/long keys directly
    lat = float(item.get("lat", 0.0) or 0.0)
    lon = float(item.get("long", 0.0) or 0.0)

    measures = item.get("measures", [])
    params: list[str] = []
    if isinstance(measures, list):
        for m in measures:
            if isinstance(m, dict):
                label = m.get("parameterName") or m.get("parameter", "")
                if label and label not in params:
                    params.append(label)
            elif isinstance(m, str):
                # Sometimes measures is a list of URIs
                part = m.split("/")[-1]
                if part and part not in params:
                    params.append(part)
    if not params:
        params = ["rainfall"]

    # Parse latest reading timestamp if available from the station listing
    last_update = None
    date_opened = item.get("dateOpened", "")
    # The EA station listing may include latestReading with dateTime
    latest_reading = item.get("latestReading")
    if isinstance(latest_reading, dict):
        lr_dt = latest_reading.get("dateTime", "")
        if lr_dt:
            try:
                last_update = datetime.fromisoformat(lr_dt.replace("Z", "+00:00"))
            except (ValueError, TypeError):
                pass
    elif isinstance(latest_reading, str) and latest_reading:
        # Sometimes latestReading is just a URI string
        pass

    return Station(
        id=station_id,
        name=item.get("label", station_id),
        lat=lat,
        lon=lon,
        source="uk_ea",
        network=item.get("eaRegionName", "EA England"),
        params=params,
        elevation=None,
        last_update=last_update,
        url=url,
        extra={
            "town": item.get("town", ""),
            "river_name": item.get("riverName", ""),
            "catchment_name": item.get("catchmentName", ""),
            "station_reference": item.get("stationReference", ""),
            "status": item.get("status", ""),
        },
    )


def _uk_ea_parse_reading(item: dict, station_id: str) -> Optional[Reading]:
    """Convert an EA reading item to the Reading model."""
    raw_val = item.get("value")
    if raw_val is None:
        return None
    try:
        value = float(raw_val)
    except (TypeError, ValueError):
        return None

    dt_str = item.get("dateTime", "")
    try:
        ts = datetime.fromisoformat(dt_str.replace("Z", "+00:00"))
    except (ValueError, AttributeError):
        return None

    measure_uri = item.get("measure", "")
    # Extract parameter name from the measure URI, e.g. ".../rainfall-tipping_bucket_raingauge-15_min-mm"
    param = "rainfall"
    if isinstance(measure_uri, str) and "/" in measure_uri:
        segment = measure_uri.split("/")[-1]
        param = segment.split("-")[0] if "-" in segment else segment

    unit = "mm"  # EA rainfall is always mm

    return Reading(
        station_id=station_id,
        timestamp=ts,
        parameter=param,
        value=value,
        unit=unit,
        quality=UK_EA_QUALITY_MAP.get(item.get("quality", ""), item.get("quality")),
        source="uk_ea",
    )


@router.get("/uk_ea/stations", summary="UK EA — all rainfall stations")
async def uk_ea_stations(
    lat: Optional[float] = Query(None, description="Latitude for nearby search"),
    lon: Optional[float] = Query(None, description="Longitude for nearby search"),
    dist: Optional[int] = Query(None, description="Search radius in km (requires lat & lon)"),
):
    """Return UK Environment Agency rainfall monitoring stations.

    Without lat/lon returns all stations (~4,000). With lat, lon, and dist
    returns nearby stations within the specified radius in km.
    """
    cache_key = f"uk_ea:stations:{lat}:{lon}:{dist}"
    cached_result = await cache.get(cache_key)
    if cached_result is not None:
        return cached_result

    params: dict[str, Any] = {"parameter": "rainfall", "_limit": 5000}
    if lat is not None and lon is not None:
        params["lat"] = lat
        params["long"] = lon
        if dist is not None:
            params["dist"] = dist

    try:
        async with _client() as c:
            resp = await c.get(f"{UK_EA_BASE}/id/stations", params=params)
            resp.raise_for_status()
            data = resp.json()
    except httpx.TimeoutException:
        raise HTTPException(status_code=504, detail="UK EA API timeout")
    except httpx.HTTPStatusError as exc:
        raise HTTPException(status_code=exc.response.status_code, detail=f"UK EA error: {exc}")
    except Exception as exc:
        logger.exception("Error fetching UK EA stations")
        raise HTTPException(status_code=500, detail=str(exc))

    items = data.get("items", [])
    stations = [_uk_ea_parse_station(item).model_dump() for item in items]
    result = {
        "source": "uk_ea",
        "count": len(stations),
        "stations": stations,
    }
    await cache.set(cache_key, result, ttl=config.CACHE_TTL_LONG)
    return result


@router.get("/uk_ea/stations/{station_id}", summary="UK EA — single station detail")
async def uk_ea_station_detail(station_id: str):
    """Return metadata for a single UK Environment Agency rainfall station."""
    cache_key = f"uk_ea:station:{station_id}"
    cached_result = await cache.get(cache_key)
    if cached_result is not None:
        return cached_result

    try:
        async with _client() as c:
            resp = await c.get(f"{UK_EA_BASE}/id/stations/{station_id}")
            resp.raise_for_status()
            data = resp.json()
    except httpx.TimeoutException:
        raise HTTPException(status_code=504, detail="UK EA API timeout")
    except httpx.HTTPStatusError as exc:
        if exc.response.status_code == 404:
            raise HTTPException(status_code=404, detail=f"Station '{station_id}' not found")
        raise HTTPException(status_code=exc.response.status_code, detail=f"UK EA error: {exc}")
    except Exception as exc:
        logger.exception("Error fetching UK EA station %s", station_id)
        raise HTTPException(status_code=500, detail=str(exc))

    item = data.get("items", data)
    # items may be a single dict or a list with one element
    if isinstance(item, list):
        item = item[0] if item else {}

    station = _uk_ea_parse_station(item)
    result = station.model_dump()
    await cache.set(cache_key, result, ttl=config.CACHE_TTL_LONG)
    return result


@router.get("/uk_ea/stations/{station_id}/readings", summary="UK EA — station readings")
async def uk_ea_readings(
    station_id: str,
    today: bool = Query(False, description="Return only today's readings"),
    limit: int = Query(96, ge=1, le=2000, description="Max number of readings to return"),
):
    """Return rainfall readings for a UK Environment Agency station.

    Set today=true to get all readings since midnight UTC today.
    Otherwise returns the latest N readings (sorted descending).
    """
    cache_key = f"uk_ea:readings:{station_id}:{today}:{limit}"
    cached_result = await cache.get(cache_key)
    if cached_result is not None:
        return cached_result

    if today:
        url = f"{UK_EA_BASE}/id/stations/{station_id}/readings"
        params: dict[str, Any] = {"today": True}
    else:
        url = f"{UK_EA_BASE}/id/stations/{station_id}/readings"
        params = {"_sorted": True, "_limit": limit}

    try:
        async with _client() as c:
            resp = await c.get(url, params=params)
            resp.raise_for_status()
            data = resp.json()
    except httpx.TimeoutException:
        raise HTTPException(status_code=504, detail="UK EA API timeout")
    except httpx.HTTPStatusError as exc:
        if exc.response.status_code == 404:
            raise HTTPException(status_code=404, detail=f"Station '{station_id}' not found")
        raise HTTPException(status_code=exc.response.status_code, detail=f"UK EA error: {exc}")
    except Exception as exc:
        logger.exception("Error fetching UK EA readings for %s", station_id)
        raise HTTPException(status_code=500, detail=str(exc))

    items = data.get("items", [])
    readings = []
    for item in items:
        r = _uk_ea_parse_reading(item, station_id)
        if r is not None:
            readings.append(r.model_dump())

    result = {
        "source": "uk_ea",
        "station_id": station_id,
        "count": len(readings),
        "readings": readings,
    }
    await cache.set(cache_key, result, ttl=config.CACHE_TTL)
    return result


# ===========================================================================
# 2. JAPAN AMeDAS (JMA)
# ===========================================================================
JMA_BASE = "https://www.jma.go.jp/bosai/amedas"
JMA_TABLE_URL = f"{JMA_BASE}/const/amedastable.json"
JMA_LATEST_TIME_URL = f"{JMA_BASE}/data/latest_time.txt"

# Maps AMeDAS field name → (parameter name, unit)
AMEDAS_PARAM_MAP: dict[str, tuple[str, str]] = {
    "temp":           ("temperature",        "°C"),
    "humidity":       ("relative_humidity",  "%"),
    "wind":           ("wind_speed",         "m/s"),
    "windDirection":  ("wind_direction",     "16dir"),
    "normalPressure": ("pressure_station",   "hPa"),
    "seaLevelPressure": ("pressure_sea",     "hPa"),
    "precipitation10m": ("precipitation_10min", "mm"),
    "precipitation1h":  ("precipitation_1h",    "mm"),
    "precipitation3h":  ("precipitation_3h",    "mm"),
    "precipitation24h": ("precipitation_24h",   "mm"),
    "snow":           ("snow_depth",         "cm"),
    "sunshine":       ("sunshine_duration",  "h"),
    "visibility":     ("visibility",         "km"),
    "iceArea":        ("ice_area",           "m²"),
}


def _jma_dms_to_decimal(deg_min: list) -> float:
    """Convert JMA [degrees, decimal_minutes] to decimal degrees."""
    if not deg_min or len(deg_min) < 2:
        return 0.0
    try:
        return float(deg_min[0]) + float(deg_min[1]) / 60.0
    except (TypeError, ValueError):
        return 0.0


def _jma_round_to_10min(dt: datetime) -> str:
    """Round a datetime down to the nearest 10-minute interval for AMeDAS map URLs."""
    minute = (dt.minute // 10) * 10
    rounded = dt.replace(minute=minute, second=0, microsecond=0)
    return rounded.strftime("%Y%m%d%H%M%S")


async def _jma_get_station_table() -> dict:
    """Fetch and cache the AMeDAS station metadata table (cache 1 hour)."""
    cache_key = "jma:station_table"
    cached_result = await cache.get(cache_key)
    if cached_result is not None:
        return cached_result

    try:
        async with _client() as c:
            resp = await c.get(JMA_TABLE_URL)
            resp.raise_for_status()
            data = resp.json()
    except Exception as exc:
        logger.exception("Error fetching JMA station table")
        raise HTTPException(status_code=502, detail=f"JMA station table unavailable: {exc}")

    await cache.set(cache_key, data, ttl=config.CACHE_TTL_LONG)
    return data


async def _jma_get_latest_timestamp() -> str:
    """Fetch the latest AMeDAS data timestamp string (e.g. '20260218160000')."""
    try:
        async with _client() as c:
            resp = await c.get(JMA_LATEST_TIME_URL)
            resp.raise_for_status()
            text = resp.text.strip()
            # Format returned: "2026-02-18T16:00:00+09:00" or similar ISO string
            # Convert to the YYYYMMDDHHmmss format used in the map URL
            try:
                dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
                # AMeDAS map keys use JST (UTC+9); round to 10 min
                dt_utc = dt.astimezone(timezone.utc)
                return _jma_round_to_10min(dt_utc)
            except ValueError:
                # If already in YYYYMMDDHHmmSS form, use as-is
                return text
    except Exception as exc:
        logger.warning("JMA latest_time fetch failed, using current time: %s", exc)
        # Fall back to current time rounded to 10-min
        return _jma_round_to_10min(_now_utc())


def _jma_parse_station(station_id: str, meta: dict) -> Station:
    lat = _jma_dms_to_decimal(meta.get("lat", []))
    lon = _jma_dms_to_decimal(meta.get("lon", []))
    return Station(
        id=station_id,
        name=meta.get("enName", meta.get("kjName", station_id)),
        lat=lat,
        lon=lon,
        source="japan_jma",
        network="AMeDAS",
        params=list(AMEDAS_PARAM_MAP.values().__class__(v[0] for v in AMEDAS_PARAM_MAP.values())),
        elevation=float(meta.get("alt", 0) or 0),
        url=f"https://www.jma.go.jp/bosai/map.html#contents=amedas&amedas={station_id}",
        extra={
            "kj_name": meta.get("kjName", ""),
            "en_name": meta.get("enName", ""),
            "type": meta.get("type", ""),
            "altitude": meta.get("alt"),
        },
    )


def _jma_extract_readings(station_id: str, obs: dict, timestamp_str: str) -> list[Reading]:
    """Extract Reading objects from a single AMeDAS station observation dict."""
    readings: list[Reading] = []

    try:
        ts = datetime.strptime(timestamp_str, "%Y%m%d%H%M%S").replace(tzinfo=timezone.utc)
    except ValueError:
        ts = _now_utc()

    for field, (param_name, unit) in AMEDAS_PARAM_MAP.items():
        raw = obs.get(field)
        if raw is None:
            continue
        # AMeDAS format: [value, quality_flag] or just a scalar
        if isinstance(raw, list) and len(raw) >= 1:
            val_raw = raw[0]
            quality_flag = str(raw[1]) if len(raw) > 1 else None
        else:
            val_raw = raw
            quality_flag = None

        if val_raw is None:
            continue
        try:
            value = float(val_raw)
        except (TypeError, ValueError):
            continue

        # Wind direction is stored as a 16-direction string ("北" etc.) or index
        # Keep as numeric index if possible, otherwise skip
        if field == "windDirection":
            if isinstance(val_raw, str):
                # Map 16-direction compass string to degrees
                _dir16 = {
                    "北": 0, "北北東": 22.5, "北東": 45, "東北東": 67.5,
                    "東": 90, "東南東": 112.5, "南東": 135, "南南東": 157.5,
                    "南": 180, "南南西": 202.5, "南西": 225, "西南西": 247.5,
                    "西": 270, "西北西": 292.5, "北西": 315, "北北西": 337.5,
                    "calm": -1,
                }
                value = _dir16.get(val_raw, -999)
                if value == -999:
                    continue
                unit = "degrees"

        readings.append(
            Reading(
                station_id=station_id,
                timestamp=ts,
                parameter=param_name,
                value=value,
                unit=unit,
                quality=quality_flag,
                source="japan_jma",
            )
        )

    return readings


@router.get("/japan/stations", summary="Japan AMeDAS — all stations with metadata")
async def japan_stations():
    """Return all ~1,300 JMA AMeDAS station metadata records."""
    cache_key = "jma:stations_list"
    cached_result = await cache.get(cache_key)
    if cached_result is not None:
        return cached_result

    table = await _jma_get_station_table()
    stations = []
    for sid, meta in table.items():
        try:
            station = _jma_parse_station(sid, meta)
            stations.append(station.model_dump())
        except Exception:
            continue

    result = {
        "source": "japan_jma",
        "network": "AMeDAS",
        "count": len(stations),
        "stations": stations,
    }
    await cache.set(cache_key, result, ttl=config.CACHE_TTL_LONG)
    return result


@router.get("/japan/current", summary="Japan AMeDAS — current observations all stations")
async def japan_current():
    """Return current AMeDAS observations for all stations from the JMA map endpoint."""
    cache_key = "jma:current_all"
    cached_result = await cache.get(cache_key)
    if cached_result is not None:
        return cached_result

    timestamp_str = await _jma_get_latest_timestamp()
    map_url = f"{JMA_BASE}/data/map/{timestamp_str}.json"

    try:
        async with _client() as c:
            resp = await c.get(map_url)
            resp.raise_for_status()
            obs_data = resp.json()
    except httpx.TimeoutException:
        raise HTTPException(status_code=504, detail="JMA API timeout")
    except httpx.HTTPStatusError as exc:
        raise HTTPException(status_code=exc.response.status_code, detail=f"JMA error: {exc}")
    except Exception as exc:
        logger.exception("Error fetching JMA map data")
        raise HTTPException(status_code=500, detail=str(exc))

    # Fetch station table for metadata enrichment (best-effort)
    try:
        table = await _jma_get_station_table()
    except Exception:
        table = {}

    stations_out: list[dict] = []
    for sid, obs in obs_data.items():
        if not isinstance(obs, dict):
            continue
        meta = table.get(sid, {})
        station_info: dict[str, Any] = {
            "station_id": sid,
            "name": meta.get("enName") or meta.get("kjName", sid),
            "lat": _jma_dms_to_decimal(meta.get("lat", [])),
            "lon": _jma_dms_to_decimal(meta.get("lon", [])),
            "altitude": meta.get("alt"),
        }
        readings = _jma_extract_readings(sid, obs, timestamp_str)
        station_info["readings"] = [r.model_dump() for r in readings]
        stations_out.append(station_info)

    result = {
        "source": "japan_jma",
        "timestamp": timestamp_str,
        "station_count": len(stations_out),
        "stations": stations_out,
    }
    await cache.set(cache_key, result, ttl=config.CACHE_TTL)
    return result


@router.get("/japan/stations/{station_id}", summary="Japan AMeDAS — single station current obs")
async def japan_station(station_id: str):
    """Return the current AMeDAS observation for a single station."""
    cache_key = f"jma:station:{station_id}"
    cached_result = await cache.get(cache_key)
    if cached_result is not None:
        return cached_result

    timestamp_str = await _jma_get_latest_timestamp()
    map_url = f"{JMA_BASE}/data/map/{timestamp_str}.json"

    try:
        async with _client() as c:
            resp = await c.get(map_url)
            resp.raise_for_status()
            obs_data = resp.json()
    except httpx.TimeoutException:
        raise HTTPException(status_code=504, detail="JMA API timeout")
    except httpx.HTTPStatusError as exc:
        raise HTTPException(status_code=exc.response.status_code, detail=f"JMA error: {exc}")
    except Exception as exc:
        logger.exception("Error fetching JMA map data for station %s", station_id)
        raise HTTPException(status_code=500, detail=str(exc))

    obs = obs_data.get(station_id)
    if obs is None:
        raise HTTPException(status_code=404, detail=f"AMeDAS station '{station_id}' not found in current data")

    try:
        table = await _jma_get_station_table()
    except Exception:
        table = {}

    meta = table.get(station_id, {})
    station = _jma_parse_station(station_id, meta)
    readings = _jma_extract_readings(station_id, obs, timestamp_str)

    result = {
        "source": "japan_jma",
        "timestamp": timestamp_str,
        "station": station.model_dump(),
        "readings": [r.model_dump() for r in readings],
    }
    await cache.set(cache_key, result, ttl=config.CACHE_TTL)
    return result


# ===========================================================================
# 3. GERMANY BRIGHT SKY (DWD WRAPPER)
# ===========================================================================
BRIGHTSKY_BASE = "https://api.brightsky.dev"

BRIGHTSKY_UNIT_MAP: dict[str, str] = {
    "temperature": "°C",
    "wind_speed": "km/h",
    "wind_direction": "degrees",
    "wind_gust_speed": "km/h",
    "wind_gust_direction": "degrees",
    "precipitation": "mm",
    "pressure_msl": "hPa",
    "sunshine": "min",
    "visibility": "m",
    "cloud_cover": "%",
    "dew_point": "°C",
    "relative_humidity": "%",
    "precipitation_probability": "%",
    "precipitation_probability_6h": "%",
    "solar": "kWh/m²",
    "icon": "",
    "condition": "",
}


def _brightsky_parse_obs(obs: dict, station_meta: Optional[dict] = None) -> list[Reading]:
    """Extract Reading objects from a single Bright Sky weather record."""
    readings: list[Reading] = []
    ts_raw = obs.get("timestamp", "")
    try:
        ts = datetime.fromisoformat(ts_raw.replace("Z", "+00:00"))
    except (ValueError, AttributeError):
        return readings

    station_id = str(obs.get("station_id", "brightsky"))

    for field, unit in BRIGHTSKY_UNIT_MAP.items():
        val = obs.get(field)
        if val is None or not isinstance(val, (int, float)):
            continue
        readings.append(
            Reading(
                station_id=station_id,
                timestamp=ts,
                parameter=field,
                value=float(val),
                unit=unit,
                quality=obs.get("fallback_source_ids", {}).get(field),
                source="germany_dwd",
            )
        )

    return readings


@router.get("/germany/current", summary="Germany DWD — current weather at location")
async def germany_current(
    lat: float = Query(..., description="Latitude (decimal degrees)"),
    lon: float = Query(..., description="Longitude (decimal degrees)"),
):
    """Return current weather conditions at a latitude/longitude from Bright Sky (DWD).

    Bright Sky automatically selects the nearest DWD weather station(s).
    """
    cache_key = f"brightsky:current:{lat:.4f}:{lon:.4f}"
    cached_result = await cache.get(cache_key)
    if cached_result is not None:
        return cached_result

    try:
        async with _client() as c:
            resp = await c.get(
                f"{BRIGHTSKY_BASE}/current_weather",
                params={"lat": lat, "lon": lon},
            )
            resp.raise_for_status()
            data = resp.json()
    except httpx.TimeoutException:
        raise HTTPException(status_code=504, detail="Bright Sky API timeout")
    except httpx.HTTPStatusError as exc:
        raise HTTPException(status_code=exc.response.status_code, detail=f"Bright Sky error: {exc}")
    except Exception as exc:
        logger.exception("Error fetching Bright Sky current weather")
        raise HTTPException(status_code=500, detail=str(exc))

    weather = data.get("weather", {})
    sources = data.get("sources", [])
    readings = _brightsky_parse_obs(weather)

    # Build a station summary from the first source
    station_info: dict[str, Any] = {}
    if sources:
        src = sources[0]
        station_info = {
            "station_id": src.get("id"),
            "dwd_station_id": src.get("dwd_station_id"),
            "wmo_station_id": src.get("wmo_station_id"),
            "name": src.get("station_name", ""),
            "lat": src.get("lat"),
            "lon": src.get("lon"),
            "elevation": src.get("height"),
            "distance": src.get("distance"),
        }

    result = {
        "source": "germany_dwd",
        "query": {"lat": lat, "lon": lon},
        "station": station_info,
        "timestamp": weather.get("timestamp"),
        "readings": [r.model_dump() for r in readings],
        "raw_sources": sources,
    }
    await cache.set(cache_key, result, ttl=config.CACHE_TTL)
    return result


@router.get("/germany/weather", summary="Germany DWD — time-series weather at location")
async def germany_weather(
    lat: float = Query(..., description="Latitude (decimal degrees)"),
    lon: float = Query(..., description="Longitude (decimal degrees)"),
    date: str = Query(..., description="Start date YYYY-MM-DD (or ISO datetime)"),
    last_date: Optional[str] = Query(None, description="End date YYYY-MM-DD (defaults to date+24h)"),
):
    """Return hourly weather observations/forecasts at a lat/lon from Bright Sky (DWD).

    Date can be historical (observations) or future (forecast).
    """
    cache_key = f"brightsky:weather:{lat:.4f}:{lon:.4f}:{date}:{last_date}"
    cached_result = await cache.get(cache_key)
    if cached_result is not None:
        return cached_result

    params: dict[str, Any] = {"lat": lat, "lon": lon, "date": date}
    if last_date:
        params["last_date"] = last_date

    try:
        async with _client() as c:
            resp = await c.get(f"{BRIGHTSKY_BASE}/weather", params=params)
            resp.raise_for_status()
            data = resp.json()
    except httpx.TimeoutException:
        raise HTTPException(status_code=504, detail="Bright Sky API timeout")
    except httpx.HTTPStatusError as exc:
        raise HTTPException(status_code=exc.response.status_code, detail=f"Bright Sky error: {exc}")
    except Exception as exc:
        logger.exception("Error fetching Bright Sky weather time series")
        raise HTTPException(status_code=500, detail=str(exc))

    weather_list = data.get("weather", [])
    sources = data.get("sources", [])

    all_readings: list[dict] = []
    for obs in weather_list:
        readings = _brightsky_parse_obs(obs)
        all_readings.extend(r.model_dump() for r in readings)

    result = {
        "source": "germany_dwd",
        "query": {"lat": lat, "lon": lon, "date": date, "last_date": last_date},
        "record_count": len(weather_list),
        "reading_count": len(all_readings),
        "sources": sources,
        "readings": all_readings,
    }
    await cache.set(cache_key, result, ttl=config.CACHE_TTL)
    return result


# ===========================================================================
# 4. SINGAPORE NEA
# ===========================================================================
NEA_BASE = "https://api.data.gov.sg/v1/environment"

NEA_ENDPOINTS: dict[str, str] = {
    "rainfall":         f"{NEA_BASE}/rainfall",
    "air-temperature":  f"{NEA_BASE}/air-temperature",
    "relative-humidity": f"{NEA_BASE}/relative-humidity",
    "wind-direction":   f"{NEA_BASE}/wind-direction",
    "wind-speed":       f"{NEA_BASE}/wind-speed",
}

NEA_UNIT_MAP: dict[str, str] = {
    "rainfall":          "mm",
    "air-temperature":   "°C",
    "relative-humidity": "%",
    "wind-direction":    "degrees",
    "wind-speed":        "km/h",
}

NEA_PARAM_MAP: dict[str, str] = {
    "rainfall":          "precipitation",
    "air-temperature":   "temperature",
    "relative-humidity": "relative_humidity",
    "wind-direction":    "wind_direction",
    "wind-speed":        "wind_speed",
}


async def _nea_fetch_parameter(param_key: str) -> dict:
    """Fetch a single NEA environmental parameter; returns the raw JSON."""
    url = NEA_ENDPOINTS[param_key]
    try:
        async with _client() as c:
            resp = await c.get(url)
            resp.raise_for_status()
            return resp.json()
    except httpx.TimeoutException:
        raise HTTPException(status_code=504, detail=f"NEA API timeout for {param_key}")
    except httpx.HTTPStatusError as exc:
        raise HTTPException(status_code=exc.response.status_code, detail=f"NEA error: {exc}")
    except Exception as exc:
        logger.exception("Error fetching NEA parameter %s", param_key)
        raise HTTPException(status_code=500, detail=str(exc))


def _nea_parse(data: dict, param_key: str) -> dict:
    """Parse NEA API response into a stations+readings structure."""
    items = data.get("items", [{}])
    item = items[0] if items else {}
    timestamp_raw = item.get("timestamp", "")
    try:
        ts = datetime.fromisoformat(timestamp_raw.replace("Z", "+00:00"))
    except (ValueError, AttributeError):
        ts = _now_utc()

    # Build station index from metadata
    meta = data.get("metadata", {})
    stations_meta: dict[str, dict] = {}
    for s in meta.get("stations", []):
        sid = s.get("id", "")
        stations_meta[sid] = {
            "name": s.get("name", sid),
            "lat": s.get("location", {}).get("latitude", 0.0),
            "lon": s.get("location", {}).get("longitude", 0.0),
        }

    param_name = NEA_PARAM_MAP.get(param_key, param_key)
    unit = NEA_UNIT_MAP.get(param_key, "")
    readings: list[dict] = []
    stations_out: list[dict] = []

    for reading in item.get("readings", []):
        sid = reading.get("station_id", "")
        val = reading.get("value")
        if val is None:
            continue
        try:
            value = float(val)
        except (TypeError, ValueError):
            continue

        smeta = stations_meta.get(sid, {"name": sid, "lat": 0.0, "lon": 0.0})
        station = Station(
            id=sid,
            name=smeta["name"],
            lat=smeta["lat"],
            lon=smeta["lon"],
            source="singapore_nea",
            network="NEA",
            params=[param_name],
            last_update=ts,
            url=f"https://data.gov.sg/dataset/realtime-weather-readings",
            extra={},
        )
        r = Reading(
            station_id=sid,
            timestamp=ts,
            parameter=param_name,
            value=value,
            unit=unit,
            source="singapore_nea",
        )
        stations_out.append(station.model_dump())
        readings.append(r.model_dump())

    return {
        "source": "singapore_nea",
        "parameter": param_key,
        "timestamp": ts.isoformat(),
        "station_count": len(stations_out),
        "stations": stations_out,
        "readings": readings,
    }


@router.get("/singapore/current", summary="Singapore NEA — all parameters, all stations")
async def singapore_current():
    """Return all current NEA environmental readings (rainfall, temp, humidity, wind)."""
    cache_key = "nea:current:all"
    cached_result = await cache.get(cache_key)
    if cached_result is not None:
        return cached_result

    import asyncio

    async def _fetch_one(pk: str):
        try:
            raw = await _nea_fetch_parameter(pk)
            return pk, _nea_parse(raw, pk)
        except Exception as exc:
            logger.warning("NEA parameter %s failed: %s", pk, exc)
            return pk, {"error": str(exc)}

    tasks = [_fetch_one(pk) for pk in NEA_ENDPOINTS]
    results_list = await asyncio.gather(*tasks)

    combined: dict[str, Any] = {
        "source": "singapore_nea",
        "retrieved_at": _now_utc().isoformat(),
        "parameters": {},
    }
    for pk, payload in results_list:
        combined["parameters"][pk] = payload

    await cache.set(cache_key, combined, ttl=config.CACHE_TTL)
    return combined


@router.get("/singapore/{parameter}", summary="Singapore NEA — single parameter readings")
async def singapore_parameter(
    parameter: str,
):
    """Return current NEA readings for a single parameter.

    Valid parameter values: rainfall, air-temperature, relative-humidity,
    wind-direction, wind-speed.
    """
    if parameter not in NEA_ENDPOINTS:
        valid = ", ".join(NEA_ENDPOINTS.keys())
        raise HTTPException(
            status_code=400,
            detail=f"Unknown parameter '{parameter}'. Valid: {valid}",
        )

    cache_key = f"nea:param:{parameter}"
    cached_result = await cache.get(cache_key)
    if cached_result is not None:
        return cached_result

    raw = await _nea_fetch_parameter(parameter)
    result = _nea_parse(raw, parameter)
    await cache.set(cache_key, result, ttl=config.CACHE_TTL)
    return result


# ===========================================================================
# 5. BRAZIL INMET
# ===========================================================================
INMET_BASE = "https://apitempo.inmet.gov.br"

INMET_PARAM_MAP: dict[str, tuple[str, str]] = {
    "TEM_INS": ("temperature",          "°C"),
    "TEM_MAX": ("temperature_max",      "°C"),
    "TEM_MIN": ("temperature_min",      "°C"),
    "UMD_INS": ("relative_humidity",    "%"),
    "UMD_MAX": ("relative_humidity_max","%"),
    "UMD_MIN": ("relative_humidity_min","%"),
    "PTO_INS": ("dew_point",            "°C"),
    "PTO_MAX": ("dew_point_max",        "°C"),
    "PTO_MIN": ("dew_point_min",        "°C"),
    "PRE_INS": ("pressure",             "hPa"),
    "PRE_MAX": ("pressure_max",         "hPa"),
    "PRE_MIN": ("pressure_min",         "hPa"),
    "VEN_VEL": ("wind_speed",           "m/s"),
    "VEN_DIR": ("wind_direction",       "degrees"),
    "VEN_RAJ": ("wind_gust",            "m/s"),
    "CHUVA":   ("precipitation",        "mm"),
    "RAD_GLO": ("solar_radiation",      "kJ/m²"),
}


def _inmet_parse_station(raw: dict) -> Station:
    try:
        lat = float(raw.get("VL_LATITUDE", 0) or 0)
        lon = float(raw.get("VL_LONGITUDE", 0) or 0)
        alt = float(raw.get("VL_ALTITUDE", 0) or 0)
    except (TypeError, ValueError):
        lat = lon = alt = 0.0

    station_id = raw.get("CD_ESTACAO", raw.get("TP_ESTACAO", ""))
    return Station(
        id=station_id,
        name=raw.get("DC_NOME", station_id),
        lat=lat,
        lon=lon,
        source="brazil_inmet",
        network=raw.get("SG_ESTADO", "INMET"),
        params=["temperature", "humidity", "precipitation", "wind_speed", "pressure"],
        elevation=alt,
        url=f"https://tempo.inmet.gov.br/graficos/automatica/{station_id}",
        extra={
            "state": raw.get("SG_ESTADO", ""),
            "municipality": raw.get("DC_NOME", ""),
            "station_type": raw.get("TP_ESTACAO", ""),
            "start_date": raw.get("DT_INICIO_OPERACAO", ""),
        },
    )


def _inmet_parse_readings(records: list[dict], station_id: str) -> list[Reading]:
    readings: list[Reading] = []
    for rec in records:
        dt_str = rec.get("DT_MEDICAO", "")
        hr_str = rec.get("HR_MEDICAO", "0000")
        try:
            hour = int(str(hr_str).zfill(4)[:2])
            minute = int(str(hr_str).zfill(4)[2:])
            ts = datetime.strptime(dt_str, "%Y-%m-%d").replace(
                hour=hour, minute=minute, tzinfo=timezone.utc
            )
        except (ValueError, TypeError):
            continue

        for field, (param_name, unit) in INMET_PARAM_MAP.items():
            raw_val = rec.get(field)
            if raw_val is None or raw_val == "":
                continue
            try:
                value = float(str(raw_val).replace(",", "."))
            except (TypeError, ValueError):
                continue

            readings.append(
                Reading(
                    station_id=station_id,
                    timestamp=ts,
                    parameter=param_name,
                    value=value,
                    unit=unit,
                    source="brazil_inmet",
                )
            )

    return readings


@router.get("/brazil/stations", summary="Brazil INMET — all automatic stations")
async def brazil_stations():
    """Return all 530+ INMET automatic weather stations in Brazil."""
    cache_key = "inmet:stations"
    cached_result = await cache.get(cache_key)
    if cached_result is not None:
        return cached_result

    try:
        async with _client() as c:
            resp = await c.get(f"{INMET_BASE}/estacoes/T")
            resp.raise_for_status()
            data = resp.json()
    except httpx.TimeoutException:
        raise HTTPException(status_code=504, detail="INMET API timeout")
    except httpx.HTTPStatusError as exc:
        raise HTTPException(status_code=exc.response.status_code, detail=f"INMET error: {exc}")
    except Exception as exc:
        logger.exception("Error fetching INMET stations")
        raise HTTPException(status_code=500, detail=str(exc))

    if not isinstance(data, list):
        data = data.get("data", data.get("estacoes", []))

    stations = []
    for raw in data:
        try:
            stations.append(_inmet_parse_station(raw).model_dump())
        except Exception:
            continue

    result = {
        "source": "brazil_inmet",
        "count": len(stations),
        "stations": stations,
    }
    await cache.set(cache_key, result, ttl=config.CACHE_TTL_LONG)
    return result


@router.get(
    "/brazil/stations/{station_code}/readings",
    summary="Brazil INMET — station time-series readings",
)
async def brazil_readings(
    station_code: str,
    days: int = Query(1, ge=1, le=30, description="Number of days of data to retrieve"),
):
    """Return INMET time-series data for a specific automatic station.

    Fetches up to 30 days of observations. Uses today as the end date.
    """
    cache_key = f"inmet:readings:{station_code}:{days}"
    cached_result = await cache.get(cache_key)
    if cached_result is not None:
        return cached_result

    now = _now_utc()
    end_date = now.strftime("%Y-%m-%d")
    start_date = (now - timedelta(days=days)).strftime("%Y-%m-%d")

    url = f"{INMET_BASE}/estacao/{start_date}/{end_date}/{station_code}"
    try:
        async with _client() as c:
            resp = await c.get(url)
            resp.raise_for_status()
            data = resp.json()
    except httpx.TimeoutException:
        raise HTTPException(status_code=504, detail="INMET API timeout")
    except httpx.HTTPStatusError as exc:
        if exc.response.status_code == 404:
            raise HTTPException(status_code=404, detail=f"INMET station '{station_code}' not found")
        raise HTTPException(status_code=exc.response.status_code, detail=f"INMET error: {exc}")
    except Exception as exc:
        logger.exception("Error fetching INMET readings for %s", station_code)
        raise HTTPException(status_code=500, detail=str(exc))

    if isinstance(data, dict):
        records = data.get("data", data.get("records", []))
    elif isinstance(data, list):
        records = data
    else:
        records = []

    readings = _inmet_parse_readings(records, station_code)
    result = {
        "source": "brazil_inmet",
        "station_code": station_code,
        "start_date": start_date,
        "end_date": end_date,
        "days": days,
        "count": len(readings),
        "readings": [r.model_dump() for r in readings],
    }
    await cache.set(cache_key, result, ttl=config.CACHE_TTL)
    return result


# ===========================================================================
# 6. ENVIRONMENT CANADA
# ===========================================================================
CANADA_BASE = "https://api.weather.gc.ca/collections"

CANADA_SWOB_PARAM_MAP: dict[str, tuple[str, str]] = {
    "air_temperature":       ("temperature",       "°C"),
    "dewpoint_temperature":  ("dew_point",         "°C"),
    "relative_humidity":     ("relative_humidity", "%"),
    "wind_speed":            ("wind_speed",        "km/h"),
    "wind_direction":        ("wind_direction",    "degrees"),
    "precipitation_amount":  ("precipitation",     "mm"),
    "visibility":            ("visibility",        "m"),
    "station_pressure":      ("pressure",          "hPa"),
    "mslp":                  ("pressure_sea",      "hPa"),
    "total_cloud_cover":     ("cloud_cover",       "oktas"),
    "snow_depth":            ("snow_depth",        "cm"),
}


def _canada_parse_swob_feature(feature: dict) -> tuple[Optional[Station], list[Reading]]:
    """Parse a GeoJSON feature from the SWOB real-time endpoint."""
    props = feature.get("properties", {})
    geom = feature.get("geometry") or {}
    coords = geom.get("coordinates", [None, None])

    try:
        lon = float(coords[0]) if coords[0] is not None else 0.0
        lat = float(coords[1]) if coords[1] is not None else 0.0
    except (TypeError, ValueError, IndexError):
        lat = lon = 0.0

    station_id = props.get("STN_ID") or props.get("station_id") or props.get("wmo_id", "")
    if not station_id:
        return None, []

    station_name = props.get("STN_NAM") or props.get("station_name", str(station_id))
    province = props.get("PROV_STATE_TERR_CODE", props.get("province", ""))

    obs_dt_str = props.get("obs_date_tm") or props.get("date_tm", "")
    try:
        ts = datetime.fromisoformat(obs_dt_str.replace("Z", "+00:00"))
    except (ValueError, AttributeError):
        ts = _now_utc()

    station = Station(
        id=str(station_id),
        name=station_name,
        lat=lat,
        lon=lon,
        source="canada_ec",
        network=province,
        params=list(CANADA_SWOB_PARAM_MAP.values().__class__(v[0] for v in CANADA_SWOB_PARAM_MAP.values())),
        elevation=props.get("ELEVATION"),
        last_update=ts,
        url=f"https://weather.gc.ca/past_conditions/index_e.html?station={station_id}",
        extra={"province": province, "wmo_id": props.get("wmo_id", "")},
    )

    readings: list[Reading] = []
    for ec_field, (param_name, unit) in CANADA_SWOB_PARAM_MAP.items():
        val = props.get(ec_field)
        if val is None:
            continue
        try:
            value = float(val)
        except (TypeError, ValueError):
            continue
        readings.append(
            Reading(
                station_id=str(station_id),
                timestamp=ts,
                parameter=param_name,
                value=value,
                unit=unit,
                source="canada_ec",
            )
        )

    return station, readings


@router.get("/canada/observations", summary="Environment Canada — SWOB surface observations")
async def canada_observations(
    limit: int = Query(500, ge=1, le=5000, description="Max features to return"),
    bbox: Optional[str] = Query(
        None,
        description="Bounding box as west,south,east,north (decimal degrees)",
    ),
):
    """Return recent surface weather observations from Environment Canada SWOB real-time.

    GeoJSON feature collection. Each feature includes station metadata and
    current observations (temperature, wind, humidity, pressure, etc.).
    """
    cache_key = f"canada:swob:{limit}:{bbox}"
    cached_result = await cache.get(cache_key)
    if cached_result is not None:
        return cached_result

    params: dict[str, Any] = {"limit": limit, "f": "json"}
    if bbox:
        parts = [p.strip() for p in bbox.split(",")]
        if len(parts) != 4:
            raise HTTPException(status_code=400, detail="bbox must be west,south,east,north")
        params["bbox"] = bbox

    try:
        async with _client() as c:
            resp = await c.get(f"{CANADA_BASE}/swob-realtime/items", params=params)
            resp.raise_for_status()
            data = resp.json()
    except httpx.TimeoutException:
        raise HTTPException(status_code=504, detail="Environment Canada API timeout")
    except httpx.HTTPStatusError as exc:
        raise HTTPException(status_code=exc.response.status_code, detail=f"EC API error: {exc}")
    except Exception as exc:
        logger.exception("Error fetching EC SWOB observations")
        raise HTTPException(status_code=500, detail=str(exc))

    features = data.get("features", [])
    stations_out: list[dict] = []
    readings_out: list[dict] = []

    for feat in features:
        station, readings = _canada_parse_swob_feature(feat)
        if station:
            stations_out.append(station.model_dump())
            readings_out.extend(r.model_dump() for r in readings)

    result = {
        "source": "canada_ec",
        "collection": "swob-realtime",
        "station_count": len(stations_out),
        "reading_count": len(readings_out),
        "stations": stations_out,
        "readings": readings_out,
    }
    await cache.set(cache_key, result, ttl=config.CACHE_TTL)
    return result


@router.get("/canada/hydrometric", summary="Environment Canada — hydrometric real-time")
async def canada_hydrometric(
    limit: int = Query(500, ge=1, le=5000, description="Max features to return"),
    bbox: Optional[str] = Query(
        None,
        description="Bounding box as west,south,east,north (decimal degrees)",
    ),
):
    """Return real-time hydrometric (water level / streamflow) data from Environment Canada.

    GeoJSON feature collection of active hydrometric stations with latest readings.
    """
    cache_key = f"canada:hydro:{limit}:{bbox}"
    cached_result = await cache.get(cache_key)
    if cached_result is not None:
        return cached_result

    params: dict[str, Any] = {"limit": limit, "f": "json"}
    if bbox:
        parts = [p.strip() for p in bbox.split(",")]
        if len(parts) != 4:
            raise HTTPException(status_code=400, detail="bbox must be west,south,east,north")
        params["bbox"] = bbox

    try:
        async with _client() as c:
            resp = await c.get(f"{CANADA_BASE}/hydrometric-realtime/items", params=params)
            resp.raise_for_status()
            data = resp.json()
    except httpx.TimeoutException:
        raise HTTPException(status_code=504, detail="Environment Canada API timeout")
    except httpx.HTTPStatusError as exc:
        raise HTTPException(status_code=exc.response.status_code, detail=f"EC API error: {exc}")
    except Exception as exc:
        logger.exception("Error fetching EC hydrometric data")
        raise HTTPException(status_code=500, detail=str(exc))

    features = data.get("features", [])
    stations_out: list[dict] = []
    readings_out: list[dict] = []

    for feat in features:
        props = feat.get("properties", {})
        geom = feat.get("geometry") or {}
        coords = geom.get("coordinates", [None, None])
        try:
            lon_f = float(coords[0]) if coords[0] is not None else 0.0
            lat_f = float(coords[1]) if coords[1] is not None else 0.0
        except (TypeError, ValueError, IndexError):
            lat_f = lon_f = 0.0

        station_id = str(props.get("STATION_NUMBER") or props.get("station_number", ""))
        if not station_id:
            continue

        dt_str = props.get("DATETIME") or props.get("datetime", "")
        try:
            ts = datetime.fromisoformat(dt_str.replace("Z", "+00:00"))
        except (ValueError, AttributeError):
            ts = _now_utc()

        station = Station(
            id=station_id,
            name=props.get("STATION_NAME") or props.get("station_name", station_id),
            lat=lat_f,
            lon=lon_f,
            source="canada_ec",
            network="WSC",
            params=["water_level", "discharge"],
            last_update=ts,
            url=f"https://wateroffice.ec.gc.ca/report/real_time_e.html?stn={station_id}",
            extra={
                "province": props.get("PROV_TERR_STATE_LOC", ""),
                "contributor": props.get("CONTRIBUTOR_EN", ""),
                "datum": props.get("DATUM_EN", ""),
            },
        )
        stations_out.append(station.model_dump())

        for field, param, unit in [
            ("LEVEL", "water_level", "m"),
            ("DISCHARGE", "discharge", "m³/s"),
        ]:
            val = props.get(field)
            if val is None:
                continue
            try:
                value = float(val)
            except (TypeError, ValueError):
                continue
            readings_out.append(
                Reading(
                    station_id=station_id,
                    timestamp=ts,
                    parameter=param,
                    value=value,
                    unit=unit,
                    source="canada_ec",
                ).model_dump()
            )

    result = {
        "source": "canada_ec",
        "collection": "hydrometric-realtime",
        "station_count": len(stations_out),
        "reading_count": len(readings_out),
        "stations": stations_out,
        "readings": readings_out,
    }
    await cache.set(cache_key, result, ttl=config.CACHE_TTL)
    return result


# ===========================================================================
# 7. SWEDEN SMHI
# ===========================================================================
SMHI_BASE = "https://opendata-download-metobs.smhi.se/api/version/1.0"

# SMHI parameter IDs → (name, unit)
SMHI_PARAMS: dict[int, tuple[str, str]] = {
    1:  ("temperature",       "°C"),
    2:  ("temperature_2m",    "°C"),
    3:  ("wind_direction",    "degrees"),
    4:  ("wind_speed",        "m/s"),
    5:  ("precipitation_1h",  "mm"),
    6:  ("relative_humidity", "%"),
    7:  ("snow_depth",        "cm"),
    8:  ("pressure",          "hPa"),
    9:  ("visibility",        "m"),
    10: ("sunshine_duration", "h"),
    11: ("global_radiation",  "W/m²"),
    12: ("visibility_prev",   "m"),
    13: ("wind_speed_max",    "m/s"),
    14: ("precipitation_15min","mm"),
    15: ("precipitation_12h", "mm"),
    16: ("total_cloud_cover", "oktas"),
    17: ("precipitation_corrected", "mm"),
    18: ("precipitation_24h", "mm"),
    19: ("snow_depth_24h",    "cm"),
    20: ("dew_point",         "°C"),
    21: ("temperature_soil",  "°C"),
    22: ("wind_speed_max2",   "m/s"),
    23: ("temperature_max",   "°C"),
    24: ("temperature_min",   "°C"),
    25: ("wind_gust",         "m/s"),
    26: ("air_pressure",      "hPa"),
    27: ("precipitation_1h_alt", "mm"),
    28: ("lightning_flashes", "count"),
    29: ("thunder_distance",  "km"),
    30: ("freeze_index",      "°C"),
    31: ("fresh_snow",        "cm"),
    32: ("snow_water_equiv",  "mm"),
    33: ("ground_state",      "code"),
    34: ("precipitation_3h",  "mm"),
    35: ("precipitation_6h",  "mm"),
    36: ("precipitation_12h_alt","mm"),
    37: ("precipitation_24h_alt","mm"),
    38: ("precipitation_48h", "mm"),
    39: ("snow_cover_type",   "code"),
    40: ("cloud_base",        "m"),
}


def _smhi_parse_period_station(raw: dict, param_id: int) -> tuple[Optional[Station], list[Reading]]:
    """Parse an SMHI station+value entry from the period data response."""
    param_name, unit = SMHI_PARAMS.get(param_id, (f"param_{param_id}", ""))

    station_key = raw.get("key", "")
    station_id = str(station_key)
    name = raw.get("name", station_id)

    # Coordinates: SMHI returns them in separate "latitude"/"longitude" keys
    try:
        lat = float(raw.get("latitude", 0) or 0)
        lon = float(raw.get("longitude", 0) or 0)
    except (TypeError, ValueError):
        lat = lon = 0.0

    # Parse readings first to capture the latest timestamp for last_update
    readings: list[Reading] = []
    latest_ts: Optional[datetime] = None
    for val_entry in raw.get("value", []):
        raw_val = val_entry.get("value")
        if raw_val is None:
            continue
        try:
            value = float(str(raw_val).replace(",", "."))
        except (TypeError, ValueError):
            continue

        # SMHI timestamps are epoch milliseconds
        epoch_ms = val_entry.get("date")
        if epoch_ms is not None:
            try:
                ts = datetime.fromtimestamp(int(epoch_ms) / 1000, tz=timezone.utc)
            except (TypeError, ValueError, OSError):
                ts = _now_utc()
        else:
            ts = _now_utc()

        if latest_ts is None or ts > latest_ts:
            latest_ts = ts

        quality = val_entry.get("quality")
        readings.append(
            Reading(
                station_id=station_id,
                timestamp=ts,
                parameter=param_name,
                value=value,
                unit=unit,
                quality=quality,
                source="sweden_smhi",
            )
        )

    station = Station(
        id=station_id,
        name=name,
        lat=lat,
        lon=lon,
        source="sweden_smhi",
        network="SMHI",
        params=[param_name],
        elevation=raw.get("height"),
        last_update=latest_ts,
        url=f"https://opendata.smhi.se/apidocs/metobs/data.html",
        extra={"owner": raw.get("owner", ""), "measuring_stations": raw.get("measuringStations", "")},
    )

    return station, readings


@router.get("/sweden/current", summary="Sweden SMHI — latest-hour observations all stations")
async def sweden_current(
    parameter: int = Query(1, description="SMHI parameter ID (1=temp, 5=precip, 6=RH, 7=snow_depth, 13=wind_speed, 25=wind_gust)"),
):
    """Return the latest-hour observations for all SMHI stations for a given parameter.

    Common parameter IDs:
    - 1: Air temperature (°C)
    - 5: Precipitation 1h (mm)
    - 6: Relative humidity (%)
    - 7: Snow depth (cm)
    - 13: Wind speed max (m/s)
    - 25: Wind gust (m/s)
    """
    if parameter not in SMHI_PARAMS:
        valid = ", ".join(f"{k}={v[0]}" for k, v in sorted(SMHI_PARAMS.items())[:15])
        raise HTTPException(
            status_code=400,
            detail=f"Unknown parameter ID {parameter}. Examples: {valid}",
        )

    cache_key = f"smhi:current:{parameter}"
    cached_result = await cache.get(cache_key)
    if cached_result is not None:
        return cached_result

    param_name, unit = SMHI_PARAMS[parameter]
    url = (
        f"{SMHI_BASE}/parameter/{parameter}/station-set/all/period/latest-hour/data.json"
    )

    try:
        async with _client() as c:
            resp = await c.get(url)
            resp.raise_for_status()
            data = resp.json()
    except httpx.TimeoutException:
        raise HTTPException(status_code=504, detail="SMHI API timeout")
    except httpx.HTTPStatusError as exc:
        if exc.response.status_code == 404:
            raise HTTPException(
                status_code=404,
                detail=f"SMHI parameter {parameter} not available for latest-hour station-set",
            )
        raise HTTPException(status_code=exc.response.status_code, detail=f"SMHI error: {exc}")
    except Exception as exc:
        logger.exception("Error fetching SMHI current data for parameter %s", parameter)
        raise HTTPException(status_code=500, detail=str(exc))

    raw_stations = data.get("station", [])
    stations_out: list[dict] = []
    readings_out: list[dict] = []

    for raw in raw_stations:
        station, readings = _smhi_parse_period_station(raw, parameter)
        if station:
            stations_out.append(station.model_dump())
            readings_out.extend(r.model_dump() for r in readings)

    # Surface-level metadata from the response
    updated_raw = data.get("updated")
    try:
        updated = datetime.fromtimestamp(int(updated_raw) / 1000, tz=timezone.utc).isoformat()
    except (TypeError, ValueError):
        updated = None

    result = {
        "source": "sweden_smhi",
        "parameter_id": parameter,
        "parameter_name": param_name,
        "unit": unit,
        "updated": updated,
        "station_count": len(stations_out),
        "reading_count": len(readings_out),
        "stations": stations_out,
        "readings": readings_out,
    }
    await cache.set(cache_key, result, ttl=config.CACHE_TTL)
    return result


# ===========================================================================
# 8. OVERVIEW
# ===========================================================================

@router.get("/overview", summary="International sources — overview of all APIs")
async def overview():
    """Return a summary of all international weather data sources in this module."""
    return {
        "module": SOURCE_NAME,
        "description": SOURCE_DESCRIPTION,
        "sources": [
            {
                "id": "uk_ea",
                "name": "UK Environment Agency",
                "description": "Real-time rainfall monitoring across England — ~4,000 tipping-bucket gauges.",
                "url": "https://environment.data.gov.uk/flood-monitoring/doc/reference",
                "auth_required": False,
                "update_frequency": "15 min",
                "coverage": "England",
                "station_count": "~4,000",
                "params": ["rainfall"],
                "endpoints": [
                    "GET /api/international/uk_ea/stations",
                    "GET /api/international/uk_ea/stations/{station_id}",
                    "GET /api/international/uk_ea/stations/{station_id}/readings",
                ],
            },
            {
                "id": "japan_jma",
                "name": "Japan JMA AMeDAS",
                "description": "Japan Meteorological Agency Automated Meteorological Data Acquisition System — 1,300 stations.",
                "url": "https://www.jma.go.jp/bosai/amedas/",
                "auth_required": False,
                "update_frequency": "10 min",
                "coverage": "Japan",
                "station_count": "~1,300",
                "params": ["temperature", "humidity", "precipitation", "wind_speed", "wind_direction", "pressure", "snow_depth", "sunshine"],
                "endpoints": [
                    "GET /api/international/japan/stations",
                    "GET /api/international/japan/current",
                    "GET /api/international/japan/stations/{station_id}",
                ],
            },
            {
                "id": "germany_dwd",
                "name": "Germany Bright Sky (DWD)",
                "description": "CORS-enabled JSON wrapper around Germany's Deutscher Wetterdienst (DWD) open data. Supports current conditions and historical/forecast time series by lat/lon.",
                "url": "https://brightsky.dev/",
                "auth_required": False,
                "update_frequency": "1 h",
                "coverage": "Germany (and border regions)",
                "station_count": "~600",
                "params": ["temperature", "wind_speed", "wind_direction", "precipitation", "pressure", "humidity", "sunshine", "visibility", "cloud_cover"],
                "endpoints": [
                    "GET /api/international/germany/current?lat=&lon=",
                    "GET /api/international/germany/weather?lat=&lon=&date=",
                ],
            },
            {
                "id": "singapore_nea",
                "name": "Singapore NEA",
                "description": "National Environment Agency of Singapore real-time environmental readings. Cleanest JSON API — 5 parameters, ~60 stations.",
                "url": "https://data.gov.sg/dataset/realtime-weather-readings",
                "auth_required": False,
                "update_frequency": "1 min",
                "coverage": "Singapore",
                "station_count": "~60",
                "params": ["rainfall", "air_temperature", "relative_humidity", "wind_direction", "wind_speed"],
                "endpoints": [
                    "GET /api/international/singapore/current",
                    "GET /api/international/singapore/{parameter}",
                ],
            },
            {
                "id": "brazil_inmet",
                "name": "Brazil INMET",
                "description": "Instituto Nacional de Meteorologia automatic weather station network — 530+ stations across Brazil.",
                "url": "https://portal.inmet.gov.br/",
                "auth_required": False,
                "update_frequency": "1 h",
                "coverage": "Brazil",
                "station_count": "530+",
                "params": ["temperature", "humidity", "precipitation", "wind_speed", "wind_direction", "pressure", "solar_radiation"],
                "endpoints": [
                    "GET /api/international/brazil/stations",
                    "GET /api/international/brazil/stations/{station_code}/readings",
                ],
            },
            {
                "id": "canada_ec",
                "name": "Environment Canada",
                "description": "Environment and Climate Change Canada OGC API — SWOB real-time surface observations and WSC hydrometric data.",
                "url": "https://api.weather.gc.ca/",
                "auth_required": False,
                "update_frequency": "1 h (SWOB), 15 min (hydro)",
                "coverage": "Canada",
                "station_count": "1,800+ (SWOB), 2,000+ (hydro)",
                "params": ["temperature", "humidity", "wind_speed", "wind_direction", "precipitation", "pressure", "water_level", "discharge"],
                "endpoints": [
                    "GET /api/international/canada/observations",
                    "GET /api/international/canada/hydrometric",
                ],
            },
            {
                "id": "sweden_smhi",
                "name": "Sweden SMHI",
                "description": "Swedish Meteorological and Hydrological Institute open data — latest-hour observations across Sweden.",
                "url": "https://opendata.smhi.se/apidocs/metobs/",
                "auth_required": False,
                "update_frequency": "1 h",
                "coverage": "Sweden",
                "station_count": "~500",
                "params": ["temperature", "precipitation", "relative_humidity", "snow_depth", "wind_speed", "wind_gust", "pressure", "sunshine"],
                "endpoints": [
                    "GET /api/international/sweden/current?parameter=1",
                ],
                "parameter_ids": {
                    1: "temperature (°C)",
                    5: "precipitation_1h (mm)",
                    6: "relative_humidity (%)",
                    7: "snow_depth (cm)",
                    13: "wind_speed_max (m/s)",
                    25: "wind_gust (m/s)",
                },
            },
        ],
    }
