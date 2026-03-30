"""Citizen / Crowdsource Weather Networks

Aggregates data from volunteer and consumer-grade sensor networks:
  - PurpleAir     (air quality + basic weather, ~30 000 sensors globally)
  - WeatherFlow Tempest  (ultrasonic personal weather stations)
  - Ambient Weather      (DIY / hobbyist stations via RT API)
  - CoCoRaHS             (volunteer precipitation observers, via ACIS)
  - CWOP/APRS            (amateur-radio weather stations, via findU)
"""

import asyncio
import math
import re
from datetime import datetime, date, timezone
from typing import Optional
from urllib.parse import urlencode

import httpx
from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import JSONResponse

import cache
import config
from models import Station, Reading, StationDetail

# ---------------------------------------------------------------------------
# Module-level metadata consumed by server.py /api/sources
# ---------------------------------------------------------------------------

SOURCE_NAME = "Citizen / Crowdsource Networks"
SOURCE_DESCRIPTION = (
    "Volunteer and hobbyist weather sensor networks: PurpleAir air-quality, "
    "WeatherFlow Tempest, Ambient Weather, CoCoRaHS precipitation observers, "
    "and CWOP/APRS amateur-radio stations."
)
SOURCE_META = {
    "category": "weather/air_quality",
    "coverage": "Global (varies by network)",
    "auth_required": True,   # some networks require keys
    "update_frequency": "1–5 min (varies by network)",
    "params": [
        "pm2.5", "pm2.5_10minute", "pm2.5_30minute", "pm2.5_60minute",
        "temperature", "humidity", "pressure",
        "wind_speed", "wind_direction", "wind_gust",
        "precipitation", "lightning_strike_count", "uv_index", "solar_radiation",
    ],
    "networks": ["purpleair", "tempest", "ambient", "cocorahs", "cwop"],
    "urls": {
        "purpleair": "https://www.purpleair.com",
        "tempest": "https://weatherflow.com/tempest-weather-system/",
        "ambient": "https://ambientweather.net",
        "cocorahs": "https://www.cocorahs.org",
        "cwop": "https://www.wxqa.com",
    },
}

# ---------------------------------------------------------------------------
# Router
# ---------------------------------------------------------------------------

router = APIRouter(prefix="/api/citizen", tags=["Citizen Networks"])

# ---------------------------------------------------------------------------
# Shared HTTP helpers
# ---------------------------------------------------------------------------

def _http_client() -> httpx.AsyncClient:
    return httpx.AsyncClient(
        timeout=config.HTTP_TIMEOUT,
        headers={"User-Agent": config.USER_AGENT},
        follow_redirects=True,
    )


def _require_key(key_value: str, key_name: str, network: str) -> None:
    """Raise a 503 with a helpful message when an API key is not configured."""
    if not key_value:
        raise HTTPException(
            status_code=503,
            detail=(
                f"{network} requires an API key. "
                f"Set the {key_name} environment variable to enable this endpoint."
            ),
        )


# ---------------------------------------------------------------------------
# Geometry helpers
# ---------------------------------------------------------------------------

def _radius_to_bbox(lat: float, lon: float, radius_km: float):
    """Return (nwlat, nwlng, selat, selng) bounding box for a circle."""
    # 1 degree lat ~ 111.32 km; 1 degree lon varies with cos(lat)
    delta_lat = radius_km / 111.32
    delta_lon = radius_km / (111.32 * math.cos(math.radians(lat)))
    return (
        round(lat + delta_lat, 6),   # nwlat (north)
        round(lon - delta_lon, 6),   # nwlng (west)
        round(lat - delta_lat, 6),   # selat (south)
        round(lon + delta_lon, 6),   # selng (east)
    )


# ---------------------------------------------------------------------------
# PurpleAir helpers
# ---------------------------------------------------------------------------

_PURPLEAIR_BASE = "https://api.purpleair.com/v1"

_PURPLEAIR_SENSOR_LIST_FIELDS = (
    "name,latitude,longitude,pm2.5,temperature,humidity,pressure,"
    "pm2.5_10minute,pm2.5_30minute,pm2.5_60minute,last_seen,altitude,location_type"
)
_PURPLEAIR_SENSOR_DETAIL_FIELDS = (
    "name,latitude,longitude,pm2.5,pm2.5_10minute,pm2.5_30minute,pm2.5_60minute,"
    "temperature,humidity,pressure,last_seen,altitude,location_type,"
    "voc,ozone1,pm1.0,pm10.0,analog_input"
)


def _pa_station(sensor_id: int, data: dict) -> Station:
    """Convert a PurpleAir sensor record to a Station model."""
    return Station(
        id=f"purpleair_{sensor_id}",
        name=data.get("name", f"PurpleAir #{sensor_id}"),
        lat=float(data.get("latitude", 0)),
        lon=float(data.get("longitude", 0)),
        source="citizen",
        network="purpleair",
        params=[p for p in ("pm2.5", "pm1.0", "pm10.0", "temperature", "humidity", "pressure") if data.get(p) is not None],
        elevation=data.get("altitude"),
        url=f"https://www.purpleair.com/map?select={sensor_id}",
        last_update=datetime.fromtimestamp(data["last_seen"], tz=timezone.utc) if data.get("last_seen") else None,
        extra={
            "sensor_id": sensor_id,
            "location_type": data.get("location_type"),   # 0=outside, 1=inside
        },
    )


def _pa_readings(sensor_id: int, data: dict) -> list[Reading]:
    """Extract all available readings from a PurpleAir sensor record."""
    readings = []
    station_id = f"purpleair_{sensor_id}"
    ts = (
        datetime.fromtimestamp(data["last_seen"], tz=timezone.utc)
        if data.get("last_seen")
        else datetime.now(tz=timezone.utc)
    )

    _field_map = [
        ("pm2.5",          "pm2.5",          "µg/m³"),
        ("pm2.5_10minute", "pm2.5_10min_avg","µg/m³"),
        ("pm2.5_30minute", "pm2.5_30min_avg","µg/m³"),
        ("pm2.5_60minute", "pm2.5_60min_avg","µg/m³"),
        ("pm1.0",          "pm1.0",          "µg/m³"),
        ("pm10.0",         "pm10.0",         "µg/m³"),
        ("temperature",    "temperature",    "°F"),
        ("humidity",       "humidity",       "%"),
        ("pressure",       "pressure",       "hPa"),
        ("voc",            "voc",            "iaq"),
        ("ozone1",         "ozone",          "ppb"),
        ("analog_input",   "analog_input",   "V"),
    ]
    for api_key, param, unit in _field_map:
        val = data.get(api_key)
        if val is not None:
            try:
                readings.append(Reading(
                    station_id=station_id,
                    timestamp=ts,
                    parameter=param,
                    value=float(val),
                    unit=unit,
                    source="purpleair",
                ))
            except (TypeError, ValueError):
                pass
    return readings


# ---------------------------------------------------------------------------
# PurpleAir endpoints
# ---------------------------------------------------------------------------

@router.get(
    "/purpleair/sensors",
    summary="PurpleAir sensors by bounding box or radius",
    response_description="List of PurpleAir sensor stations with latest readings",
)
async def purpleair_sensors(
    lat: Optional[float] = Query(None, description="Center latitude (use with lon and radius_km)"),
    lon: Optional[float] = Query(None, description="Center longitude (use with lat and radius_km)"),
    radius_km: float = Query(25.0, description="Search radius in kilometres (used with lat/lon)"),
    nwlat: Optional[float] = Query(None, description="Bounding box north latitude"),
    nwlng: Optional[float] = Query(None, description="Bounding box west longitude"),
    selat: Optional[float] = Query(None, description="Bounding box south latitude"),
    selng: Optional[float] = Query(None, description="Bounding box east longitude"),
    location_type: int = Query(0, description="0=outside, 1=inside, 2=both (PurpleAir filter)"),
    max_age: int = Query(3600, description="Maximum sensor age in seconds"),
):
    """
    Return PurpleAir outdoor (or indoor) air-quality sensors within a geographic area.

    Supply either `lat`+`lon`+`radius_km` **or** an explicit bounding box
    via `nwlat`, `nwlng`, `selat`, `selng`.
    """
    _require_key(config.PURPLEAIR_KEY, "PURPLEAIR_KEY", "PurpleAir")

    # Resolve bounding box
    if nwlat is not None and nwlng is not None and selat is not None and selng is not None:
        bb = (nwlat, nwlng, selat, selng)
    elif lat is not None and lon is not None:
        bb = _radius_to_bbox(lat, lon, radius_km)
    else:
        raise HTTPException(
            status_code=422,
            detail="Supply either lat+lon (+radius_km) or nwlat+nwlng+selat+selng.",
        )

    nwlat_v, nwlng_v, selat_v, selng_v = bb
    cache_key = f"pa_sensors:{nwlat_v}:{nwlng_v}:{selat_v}:{selng_v}:{location_type}:{max_age}"
    cached_result = await cache.get(cache_key)
    if cached_result is not None:
        return cached_result

    params: dict = {
        "fields": _PURPLEAIR_SENSOR_LIST_FIELDS,
        "location_type": location_type,
        "nwlng": nwlng_v,
        "nwlat": nwlat_v,
        "selng": selng_v,
        "selat": selat_v,
        "max_age": max_age,
    }

    async with _http_client() as client:
        try:
            resp = await client.get(
                f"{_PURPLEAIR_BASE}/sensors",
                params=params,
                headers={"X-API-Key": config.PURPLEAIR_KEY},
            )
            resp.raise_for_status()
        except httpx.HTTPStatusError as exc:
            raise HTTPException(status_code=exc.response.status_code, detail=str(exc))
        except httpx.RequestError as exc:
            raise HTTPException(status_code=502, detail=f"PurpleAir request failed: {exc}")

    raw = resp.json()
    field_names: list[str] = raw.get("fields", [])
    sensor_rows: list[list] = raw.get("data", [])

    stations = []
    for row in sensor_rows:
        data = dict(zip(field_names, row))
        sensor_id = data.pop("sensor_index", None) or row[0]
        station = _pa_station(int(sensor_id), data)
        readings = _pa_readings(int(sensor_id), data)
        stations.append({"station": station.model_dump(), "readings": [r.model_dump() for r in readings]})

    result = {
        "network": "purpleair",
        "bbox": {"nwlat": nwlat_v, "nwlng": nwlng_v, "selat": selat_v, "selng": selng_v},
        "count": len(stations),
        "stations": stations,
    }
    await cache.set(cache_key, result, ttl=600)   # 10-minute cache for sensor lists
    return result


@router.get(
    "/purpleair/sensors/{sensor_id}",
    summary="PurpleAir single sensor detail",
    response_description="Detailed readings for one PurpleAir sensor",
)
async def purpleair_sensor_detail(sensor_id: int):
    """Return detailed, multi-average readings for a single PurpleAir sensor."""
    _require_key(config.PURPLEAIR_KEY, "PURPLEAIR_KEY", "PurpleAir")

    cache_key = f"pa_sensor:{sensor_id}"
    cached_result = await cache.get(cache_key)
    if cached_result is not None:
        return cached_result

    async with _http_client() as client:
        try:
            resp = await client.get(
                f"{_PURPLEAIR_BASE}/sensors/{sensor_id}",
                params={"fields": _PURPLEAIR_SENSOR_DETAIL_FIELDS},
                headers={"X-API-Key": config.PURPLEAIR_KEY},
            )
            resp.raise_for_status()
        except httpx.HTTPStatusError as exc:
            if exc.response.status_code == 404:
                raise HTTPException(status_code=404, detail=f"PurpleAir sensor {sensor_id} not found.")
            raise HTTPException(status_code=exc.response.status_code, detail=str(exc))
        except httpx.RequestError as exc:
            raise HTTPException(status_code=502, detail=f"PurpleAir request failed: {exc}")

    raw = resp.json()
    data: dict = raw.get("sensor", {})
    station = _pa_station(sensor_id, data)
    readings = _pa_readings(sensor_id, data)

    result = StationDetail(station=station, readings=readings).model_dump()
    await cache.set(cache_key, result, ttl=120)   # 2-minute cache for individual readings
    return result


# ---------------------------------------------------------------------------
# WeatherFlow Tempest helpers
# ---------------------------------------------------------------------------

_TEMPEST_BASE = "https://swd.weatherflow.com/swd/rest"


def _tempest_station(raw_station: dict) -> Station:
    """Convert a WeatherFlow station record to a Station model."""
    sid = raw_station.get("station_id", 0)
    # Find a device with latitude/longitude (station or device level)
    lat = raw_station.get("latitude") or raw_station.get("station_meta", {}).get("latitude")
    lon = raw_station.get("longitude") or raw_station.get("station_meta", {}).get("longitude")
    elev = raw_station.get("station_meta", {}).get("elevation")

    # Extract last_modified timestamp from station metadata
    last_update = None
    last_mod = raw_station.get("last_modified_epoch")
    if last_mod:
        try:
            last_update = datetime.fromtimestamp(int(last_mod), tz=timezone.utc)
        except (ValueError, TypeError, OSError):
            pass

    return Station(
        id=f"tempest_{sid}",
        name=raw_station.get("name", f"Tempest #{sid}"),
        lat=float(lat or 0),
        lon=float(lon or 0),
        source="citizen",
        network="tempest",
        params=[
            "temperature", "humidity", "pressure", "wind_speed", "wind_direction",
            "wind_gust", "precipitation", "solar_radiation", "uv_index",
            "lightning_strike_count", "lightning_strike_avg_distance",
        ],
        elevation=float(elev) if elev is not None else None,
        last_update=last_update,
        url=f"https://tempestwx.com/station/{sid}/graph",
        extra={
            "station_id": sid,
            "timezone": raw_station.get("timezone"),
            "devices": [d.get("device_id") for d in raw_station.get("devices", [])],
        },
    )


def _tempest_readings(station_id: int, obs: dict) -> list[Reading]:
    """
    Parse a WeatherFlow latest-observation dict.
    The 'obs' array has a single element; its fields match the documented order.
    """
    readings = []
    station_str = f"tempest_{station_id}"

    obs_list = obs.get("obs", [])
    if not obs_list:
        return readings

    entry = obs_list[0]
    # Tempest observation array field order (documented)
    # [0]timestamp [1]wind_lull [2]wind_avg [3]wind_gust [4]wind_dir
    # [5]wind_sample_interval [6]station_pressure [7]air_temp [8]relative_humidity
    # [9]illuminance [10]uv [11]solar_radiation [12]rain_accumulated [13]precip_type
    # [14]lightning_avg_distance [15]lightning_count [16]battery [17]report_interval
    # [18]local_daily_rain [19]rain_accumulated_final [20]local_daily_rain_final [21]precip_analysis_type
    _field_map = [
        (0,  "timestamp",         None,         None),
        (1,  "wind_lull",         "wind_lull",  "m/s"),
        (2,  "wind_speed",        "wind_speed", "m/s"),
        (3,  "wind_gust",         "wind_gust",  "m/s"),
        (4,  "wind_direction",    "wind_direction", "°"),
        (6,  "station_pressure",  "pressure",   "hPa"),
        (7,  "air_temp",          "temperature","°C"),
        (8,  "relative_humidity", "humidity",   "%"),
        (9,  "illuminance",       "illuminance","lux"),
        (10, "uv",                "uv_index",   "index"),
        (11, "solar_radiation",   "solar_radiation","W/m²"),
        (12, "rain_accumulated",  "precipitation","mm"),
        (14, "lightning_avg_distance","lightning_strike_avg_distance","km"),
        (15, "lightning_count",   "lightning_strike_count","count"),
    ]

    # Timestamp
    try:
        ts = datetime.fromtimestamp(entry[0], tz=timezone.utc)
    except (IndexError, TypeError, ValueError):
        ts = datetime.now(tz=timezone.utc)

    for idx, api_key, param, unit in _field_map:
        if param is None or unit is None:
            continue
        try:
            val = entry[idx]
            if val is not None:
                readings.append(Reading(
                    station_id=station_str,
                    timestamp=ts,
                    parameter=param,
                    value=float(val),
                    unit=unit,
                    source="tempest",
                ))
        except (IndexError, TypeError, ValueError):
            pass

    return readings


# ---------------------------------------------------------------------------
# WeatherFlow Tempest endpoints
# ---------------------------------------------------------------------------

@router.get(
    "/tempest/stations/{station_id}",
    summary="WeatherFlow Tempest station + latest observation",
    response_description="Station metadata with current readings",
)
async def tempest_station(station_id: int):
    """
    Return station metadata and the latest observation for a WeatherFlow Tempest station.

    Requires `WEATHERFLOW_TOKEN` environment variable.
    """
    _require_key(config.WEATHERFLOW_TOKEN, "WEATHERFLOW_TOKEN", "WeatherFlow Tempest")

    cache_key = f"tempest_station:{station_id}"
    cached_result = await cache.get(cache_key)
    if cached_result is not None:
        return cached_result

    token_param = {"token": config.WEATHERFLOW_TOKEN}

    async with _http_client() as client:
        try:
            st_resp, obs_resp = await asyncio.gather(
                client.get(f"{_TEMPEST_BASE}/stations/{station_id}", params=token_param),
                client.get(f"{_TEMPEST_BASE}/observations/station/{station_id}", params=token_param),
            )
            st_resp.raise_for_status()
        except httpx.HTTPStatusError as exc:
            code = exc.response.status_code
            if code == 404:
                raise HTTPException(status_code=404, detail=f"Tempest station {station_id} not found.")
            raise HTTPException(status_code=code, detail=str(exc))
        except httpx.RequestError as exc:
            raise HTTPException(status_code=502, detail=f"WeatherFlow request failed: {exc}")

    st_data = st_resp.json()
    raw_stations = st_data.get("stations", [])
    if not raw_stations:
        raise HTTPException(status_code=404, detail=f"Tempest station {station_id} not found.")

    station = _tempest_station(raw_stations[0])
    readings: list[Reading] = []

    try:
        obs_resp.raise_for_status()
        obs_data = obs_resp.json()
        readings = _tempest_readings(station_id, obs_data)
    except Exception:
        # Observation fetch failure is non-fatal; return station without readings
        pass

    result = StationDetail(station=station, readings=readings).model_dump()
    await cache.set(cache_key, result, ttl=120)
    return result


# ---------------------------------------------------------------------------
# Ambient Weather helpers
# ---------------------------------------------------------------------------

_AMBIENT_BASE = "https://rt.ambientweather.net/v1"


def _ambient_station(device: dict) -> Station:
    """Convert an Ambient Weather device record to a Station model."""
    info = device.get("info", {})
    mac = device.get("macAddress", "unknown")
    last_data = device.get("lastData", {})
    coords = info.get("coords", {})
    loc = coords.get("location", {})

    lat = coords.get("lat") or loc.get("lat") or 0.0
    lon = coords.get("lon") or loc.get("lon") or 0.0

    return Station(
        id=f"ambient_{mac.replace(':', '')}",
        name=info.get("name", mac),
        lat=float(lat),
        lon=float(lon),
        source="citizen",
        network="ambient",
        params=[k for k in (
            "tempf", "humidity", "baromrelin", "baromabsin",
            "windspeedmph", "winddir", "windgustmph", "maxdailygust",
            "hourlyrainin", "eventrainin", "dailyrainin", "weeklyrainin",
            "monthlyrainin", "yearlyrainin",
            "uv", "solarradiation",
            "temp1f", "temp2f", "temp3f", "temp4f",
        ) if last_data.get(k) is not None],
        elevation=None,
        url=f"https://ambientweather.net/dashboard/{mac}",
        last_update=(
            datetime.fromtimestamp(last_data["dateutc"] / 1000, tz=timezone.utc)
            if last_data.get("dateutc") else None
        ),
        extra={"mac": mac, "location_name": coords.get("address", "")},
    )


def _ambient_readings(mac: str, data_point: dict) -> list[Reading]:
    """Convert a single Ambient Weather data record to Reading objects."""
    readings = []
    station_id = f"ambient_{mac.replace(':', '')}"
    ts_ms = data_point.get("dateutc")
    ts = (
        datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
        if ts_ms
        else datetime.now(tz=timezone.utc)
    )

    _field_map = [
        ("tempf",          "temperature",     "°F"),
        ("feelsLike",      "feels_like",      "°F"),
        ("dewPoint",       "dew_point",       "°F"),
        ("humidity",       "humidity",        "%"),
        ("baromrelin",     "pressure_relative","inHg"),
        ("baromabsin",     "pressure_absolute","inHg"),
        ("windspeedmph",   "wind_speed",      "mph"),
        ("winddir",        "wind_direction",  "°"),
        ("windgustmph",    "wind_gust",       "mph"),
        ("maxdailygust",   "wind_gust_daily_max","mph"),
        ("hourlyrainin",   "precipitation_hourly","in"),
        ("eventrainin",    "precipitation_event","in"),
        ("dailyrainin",    "precipitation_daily","in"),
        ("weeklyrainin",   "precipitation_weekly","in"),
        ("monthlyrainin",  "precipitation_monthly","in"),
        ("yearlyrainin",   "precipitation_yearly","in"),
        ("uv",             "uv_index",        "index"),
        ("solarradiation", "solar_radiation", "W/m²"),
        ("co2",            "co2",             "ppm"),
        ("pm25",           "pm2.5",           "µg/m³"),
        ("pm25_24h",       "pm2.5_24h_avg",   "µg/m³"),
        ("aqi_pm25",       "aqi_pm2.5",       "AQI"),
        ("aqi_pm25_24h",   "aqi_pm2.5_24h",   "AQI"),
        # Extra temp/humidity probes
        ("temp1f",         "temperature_ch1", "°F"),
        ("humidity1",      "humidity_ch1",    "%"),
        ("temp2f",         "temperature_ch2", "°F"),
        ("humidity2",      "humidity_ch2",    "%"),
        ("temp3f",         "temperature_ch3", "°F"),
        ("humidity3",      "humidity_ch3",    "%"),
        ("temp4f",         "temperature_ch4", "°F"),
        ("humidity4",      "humidity_ch4",    "%"),
    ]
    for api_key, param, unit in _field_map:
        val = data_point.get(api_key)
        if val is not None:
            try:
                readings.append(Reading(
                    station_id=station_id,
                    timestamp=ts,
                    parameter=param,
                    value=float(val),
                    unit=unit,
                    source="ambient",
                ))
            except (TypeError, ValueError):
                pass
    return readings


# ---------------------------------------------------------------------------
# Ambient Weather endpoints
# ---------------------------------------------------------------------------

@router.get(
    "/ambient/devices",
    summary="All Ambient Weather devices for configured API key",
    response_description="List of Ambient Weather stations with latest readings",
)
async def ambient_devices():
    """
    Return all Ambient Weather devices registered to the configured account.

    Requires `AMBIENT_APP_KEY` and `AMBIENT_API_KEY` environment variables.
    """
    _require_key(config.AMBIENT_APP_KEY, "AMBIENT_APP_KEY", "Ambient Weather")
    _require_key(config.AMBIENT_API_KEY, "AMBIENT_API_KEY", "Ambient Weather")

    cache_key = "ambient_devices"
    cached_result = await cache.get(cache_key)
    if cached_result is not None:
        return cached_result

    params = {
        "applicationKey": config.AMBIENT_APP_KEY,
        "apiKey": config.AMBIENT_API_KEY,
    }

    async with _http_client() as client:
        try:
            resp = await client.get(f"{_AMBIENT_BASE}/devices", params=params)
            resp.raise_for_status()
        except httpx.HTTPStatusError as exc:
            raise HTTPException(status_code=exc.response.status_code, detail=str(exc))
        except httpx.RequestError as exc:
            raise HTTPException(status_code=502, detail=f"Ambient Weather request failed: {exc}")

    devices_raw = resp.json()
    stations = []
    for device in devices_raw:
        mac = device.get("macAddress", "unknown")
        station = _ambient_station(device)
        last_data = device.get("lastData", {})
        readings = _ambient_readings(mac, last_data) if last_data else []
        stations.append({"station": station.model_dump(), "readings": [r.model_dump() for r in readings]})

    result = {
        "network": "ambient",
        "count": len(stations),
        "stations": stations,
    }
    await cache.set(cache_key, result, ttl=600)   # 10-minute cache for device list
    return result


@router.get(
    "/ambient/devices/{mac}",
    summary="Ambient Weather device readings history",
    response_description="Recent readings for one Ambient Weather device",
)
async def ambient_device_readings(
    mac: str,
    limit: int = Query(288, ge=1, le=288, description="Number of data records to return (max 288 = 24 h at 5-min resolution)"),
):
    """
    Return recent readings for a single Ambient Weather device.

    `mac` — MAC address of the device (e.g. `AA:BB:CC:DD:EE:FF` or `AABBCCDDEEFF`).
    Requires `AMBIENT_APP_KEY` and `AMBIENT_API_KEY` environment variables.
    """
    _require_key(config.AMBIENT_APP_KEY, "AMBIENT_APP_KEY", "Ambient Weather")
    _require_key(config.AMBIENT_API_KEY, "AMBIENT_API_KEY", "Ambient Weather")

    cache_key = f"ambient_device:{mac}:{limit}"
    cached_result = await cache.get(cache_key)
    if cached_result is not None:
        return cached_result

    params = {
        "applicationKey": config.AMBIENT_APP_KEY,
        "apiKey": config.AMBIENT_API_KEY,
        "limit": limit,
    }

    async with _http_client() as client:
        try:
            resp = await client.get(f"{_AMBIENT_BASE}/devices/{mac}", params=params)
            resp.raise_for_status()
        except httpx.HTTPStatusError as exc:
            if exc.response.status_code == 404:
                raise HTTPException(status_code=404, detail=f"Ambient device {mac!r} not found.")
            raise HTTPException(status_code=exc.response.status_code, detail=str(exc))
        except httpx.RequestError as exc:
            raise HTTPException(status_code=502, detail=f"Ambient Weather request failed: {exc}")

    records = resp.json()
    readings_out = []
    for record in records:
        readings_out.extend([r.model_dump() for r in _ambient_readings(mac, record)])

    result = {
        "network": "ambient",
        "mac": mac,
        "count": len(records),
        "readings": readings_out,
    }
    await cache.set(cache_key, result, ttl=120)   # 2-minute cache for readings
    return result


# ---------------------------------------------------------------------------
# CoCoRaHS / ACIS helpers
# ---------------------------------------------------------------------------

_ACIS_URL = "https://data.rcc-acis.org/MultiStnData"

_COCORAHS_STATES = {
    "AL","AK","AZ","AR","CA","CO","CT","DE","FL","GA","HI","ID","IL","IN","IA",
    "KS","KY","LA","ME","MD","MA","MI","MN","MS","MO","MT","NE","NV","NH","NJ",
    "NM","NY","NC","ND","OH","OK","OR","PA","RI","SC","SD","TN","TX","UT","VT",
    "VA","WA","WV","WI","WY","DC","PR","VI",
}


def _cocorahs_station(site: dict, obs_date: str = "") -> Station:
    """Convert an ACIS CoCoRaHS site record to a Station model."""
    meta = site.get("meta", {})
    ll = meta.get("ll", [0.0, 0.0])   # [lon, lat]
    uid = meta.get("uid", "")
    name = meta.get("name", uid)
    elev = meta.get("elev")

    # Parse observation date as last_update
    last_update = None
    if obs_date:
        try:
            last_update = datetime.strptime(obs_date, "%Y-%m-%d").replace(
                hour=12, tzinfo=timezone.utc
            )
        except (ValueError, TypeError):
            pass

    return Station(
        id=f"cocorahs_{uid}",
        name=name,
        lat=float(ll[1]) if len(ll) >= 2 else 0.0,
        lon=float(ll[0]) if len(ll) >= 1 else 0.0,
        source="citizen",
        network="cocorahs",
        params=["precipitation"],
        elevation=float(elev) if elev is not None else None,
        last_update=last_update,
        url="https://www.cocorahs.org",
        extra={"uid": uid},
    )


def _cocorahs_reading(station_id: str, obs_date: str, pcpn_str) -> Optional[Reading]:
    """Parse a CoCoRaHS precipitation string into a Reading."""
    if pcpn_str is None or pcpn_str in ("M", "T", "S", ""):
        # M=missing, T=trace, S=snow cover
        return None
    try:
        # Dates come as "YYYY-MM-DD"; readings are daily totals
        ts = datetime.strptime(obs_date, "%Y-%m-%d").replace(
            hour=12, tzinfo=timezone.utc
        )
        val = float(pcpn_str)
        return Reading(
            station_id=station_id,
            timestamp=ts,
            parameter="precipitation",
            value=val,
            unit="in",
            quality="cocorahs_observer",
            source="cocorahs",
        )
    except (ValueError, TypeError):
        return None


# ---------------------------------------------------------------------------
# CoCoRaHS endpoints
# ---------------------------------------------------------------------------

@router.get(
    "/cocorahs/observations",
    summary="CoCoRaHS daily precipitation by state",
    response_description="Daily precipitation readings from CoCoRaHS volunteer observers",
)
async def cocorahs_observations(
    state: str = Query("OR", description="Two-letter US state abbreviation"),
    obs_date: str = Query(
        default_factory=lambda: date.today().isoformat(),
        alias="date",
        description="Observation date (YYYY-MM-DD). Defaults to today.",
    ),
):
    """
    Return daily precipitation observations from CoCoRaHS volunteer rain-gauge
    observers for the given US state and date, sourced via the ACIS API.

    No API key required.
    """
    state = state.upper().strip()
    if state not in _COCORAHS_STATES:
        raise HTTPException(
            status_code=422,
            detail=f"Unknown state code {state!r}. Use a two-letter US state abbreviation.",
        )

    # Validate date
    try:
        datetime.strptime(obs_date, "%Y-%m-%d")
    except ValueError:
        raise HTTPException(status_code=422, detail="date must be YYYY-MM-DD format.")

    cache_key = f"cocorahs:{state}:{obs_date}"
    cached_result = await cache.get(cache_key)
    if cached_result is not None:
        return cached_result

    payload = {
        "state": state,
        "date": obs_date,
        "elems": "pcpn",
        "meta": "name,ll,elev,uid",
    }

    async with _http_client() as client:
        try:
            resp = await client.post(_ACIS_URL, json=payload)
            resp.raise_for_status()
        except httpx.HTTPStatusError as exc:
            raise HTTPException(status_code=exc.response.status_code, detail=str(exc))
        except httpx.RequestError as exc:
            raise HTTPException(status_code=502, detail=f"ACIS/CoCoRaHS request failed: {exc}")

    raw = resp.json()
    sites = raw.get("data", [])

    stations = []
    for site in sites:
        station = _cocorahs_station(site, obs_date=obs_date)
        uid = site.get("meta", {}).get("uid", "")
        station_id = f"cocorahs_{uid}"
        data_vals = site.get("data", [])
        # data_vals is a list of one element per requested elem; first is pcpn
        pcpn_val = data_vals[0] if data_vals else None
        reading = _cocorahs_reading(station_id, obs_date, pcpn_val)
        stations.append({
            "station": station.model_dump(),
            "readings": [reading.model_dump()] if reading else [],
        })

    result = {
        "network": "cocorahs",
        "state": state,
        "date": obs_date,
        "count": len(stations),
        "stations": stations,
    }
    # Cache for 10 minutes; daily obs don't change frequently but QC may update
    await cache.set(cache_key, result, ttl=600)
    return result


# ---------------------------------------------------------------------------
# CWOP/APRS helpers
# ---------------------------------------------------------------------------

_FINDU_URL = "http://www.findu.com/cgi-bin/wxnear.cgi"

# Regex for parsing findU wxnear HTML rows
# Typical line: EW4567   45.123  -122.456  Portland OR  120.3  2.5 mi
_CWOP_ROW_RE = re.compile(
    r"([A-Z][A-Z0-9]{3,8})"          # callsign
    r"\s+([-\d.]+)"                   # latitude
    r"\s+([-\d.]+)"                   # longitude
    r"\s+(.+?)"                       # location description (greedy stopped by numbers)
    r"\s+([\d.]+)\s*mi",              # distance in miles
    re.IGNORECASE,
)

# Parameter patterns in the APRS weather string
_WX_TEMP_RE    = re.compile(r"t([-\d]{3})")          # temperature °F
_WX_HUM_RE     = re.compile(r"h(\d{2})")             # humidity %
_WX_PRES_RE    = re.compile(r"b(\d{5})")             # pressure 0.1 hPa
_WX_RAIN_RE    = re.compile(r"r(\d{3})")             # rain last hour (0.01 in)
_WX_WIND_RE    = re.compile(r"(\d{3})/(\d{3})")      # wind dir/speed (mph)
_WX_GUST_RE    = re.compile(r"g(\d{3})")             # wind gust (mph)


def _parse_cwop_html(html: str, lat: float, lon: float) -> list[dict]:
    """Extract CWOP station records from a findU wxnear HTML response."""
    stations = []
    for match in _CWOP_ROW_RE.finditer(html):
        callsign = match.group(1).strip()
        try:
            st_lat = float(match.group(2))
            st_lon = float(match.group(3))
            distance_mi = float(match.group(5))
        except ValueError:
            continue

        station = Station(
            id=f"cwop_{callsign}",
            name=callsign,
            lat=st_lat,
            lon=st_lon,
            source="citizen",
            network="cwop",
            params=["temperature", "humidity", "pressure", "wind_speed", "wind_direction", "precipitation"],
            url=f"http://www.findu.com/cgi-bin/find.cgi?call={callsign}&last=100&type=wx",
            extra={"callsign": callsign, "distance_mi": distance_mi},
        )
        stations.append({"station": station.model_dump(), "distance_mi": distance_mi})
    return stations


# ---------------------------------------------------------------------------
# Overview endpoint
# ---------------------------------------------------------------------------

@router.get(
    "/overview",
    summary="Summary of all citizen weather networks",
    response_description="Status and key information for each citizen network",
)
async def citizen_overview():
    """
    Return a summary of all citizen/crowdsource networks available through
    this module, including configuration status (whether API keys are set).
    """
    networks = [
        {
            "id": "purpleair",
            "name": "PurpleAir",
            "description": "Consumer air-quality sensors with PM2.5, temperature, humidity and pressure.",
            "url": "https://www.purpleair.com",
            "configured": bool(config.PURPLEAIR_KEY),
            "auth_required": True,
            "key_env": "PURPLEAIR_KEY",
            "endpoints": [
                "/api/citizen/purpleair/sensors",
                "/api/citizen/purpleair/sensors/{sensor_id}",
            ],
            "params": ["pm2.5", "pm1.0", "pm10.0", "temperature", "humidity", "pressure"],
            "update_frequency": "~2 min",
            "coverage": "Global (~30 000+ sensors)",
        },
        {
            "id": "tempest",
            "name": "WeatherFlow Tempest",
            "description": "Ultrasonic personal weather stations with lightning detection.",
            "url": "https://weatherflow.com/tempest-weather-system/",
            "configured": bool(config.WEATHERFLOW_TOKEN),
            "auth_required": True,
            "key_env": "WEATHERFLOW_TOKEN",
            "endpoints": ["/api/citizen/tempest/stations/{station_id}"],
            "params": [
                "temperature", "humidity", "pressure", "wind_speed", "wind_direction",
                "wind_gust", "precipitation", "solar_radiation", "uv_index",
                "lightning_strike_count",
            ],
            "update_frequency": "1 min",
            "coverage": "Personal stations (by station ID)",
        },
        {
            "id": "ambient",
            "name": "Ambient Weather",
            "description": "DIY and hobbyist personal weather stations via real-time API.",
            "url": "https://ambientweather.net",
            "configured": bool(config.AMBIENT_APP_KEY and config.AMBIENT_API_KEY),
            "auth_required": True,
            "key_env": "AMBIENT_APP_KEY + AMBIENT_API_KEY",
            "endpoints": [
                "/api/citizen/ambient/devices",
                "/api/citizen/ambient/devices/{mac}",
            ],
            "params": ["temperature", "humidity", "pressure", "wind_speed", "precipitation", "uv_index"],
            "update_frequency": "5 min",
            "coverage": "Account devices only",
        },
        {
            "id": "cocorahs",
            "name": "CoCoRaHS",
            "description": "Volunteer rain-gauge network — daily precipitation observations across the US.",
            "url": "https://www.cocorahs.org",
            "configured": True,   # No auth required; always available
            "auth_required": False,
            "key_env": None,
            "endpoints": ["/api/citizen/cocorahs/observations"],
            "params": ["precipitation"],
            "update_frequency": "Daily",
            "coverage": "United States (50 states + DC + territories)",
        },
        {
            "id": "cwop",
            "name": "CWOP/APRS",
            "description": "Amateur-radio weather stations that transmit data over APRS.",
            "url": "https://www.wxqa.com",
            "configured": True,   # No auth required; always available
            "auth_required": False,
            "key_env": None,
            "endpoints": ["/api/citizen/cwop/nearby"],
            "params": ["temperature", "humidity", "pressure", "wind_speed", "wind_direction", "precipitation"],
            "update_frequency": "~10 min",
            "coverage": "Global (via findU proxy)",
        },
    ]

    configured_count = sum(1 for n in networks if n["configured"])
    return {
        "source": "citizen",
        "networks": networks,
        "total_networks": len(networks),
        "configured_networks": configured_count,
        "note": (
            "Networks marked configured=false require API key environment variables. "
            "CoCoRaHS and CWOP require no authentication."
        ),
    }


# ---------------------------------------------------------------------------
# CWOP/APRS endpoint
# ---------------------------------------------------------------------------

@router.get(
    "/cwop/nearby",
    summary="Nearby CWOP/APRS weather stations via findU",
    response_description="CWOP stations near the given location",
)
async def cwop_nearby(
    lat: float = Query(..., description="Center latitude"),
    lon: float = Query(..., description="Center longitude"),
    max_dist: float = Query(50.0, description="Search radius in miles"),
    max_age: int = Query(2, description="Maximum data age in hours"),
):
    """
    Return CWOP/APRS amateur-radio weather stations near the given coordinates,
    scraped from the findU service.

    No API key required; data is parsed from findU HTML.
    """
    cache_key = f"cwop_nearby:{lat}:{lon}:{max_dist}:{max_age}"
    cached_result = await cache.get(cache_key)
    if cached_result is not None:
        return cached_result

    params = {
        "lat": lat,
        "lon": lon,
        "max_dist": max_dist,
        "max_age": max_age,
    }

    async with _http_client() as client:
        try:
            resp = await client.get(_FINDU_URL, params=params)
            resp.raise_for_status()
        except httpx.HTTPStatusError as exc:
            raise HTTPException(status_code=exc.response.status_code, detail=str(exc))
        except httpx.RequestError as exc:
            raise HTTPException(status_code=502, detail=f"findU/CWOP request failed: {exc}")

    html_text = resp.text
    stations = _parse_cwop_html(html_text, lat, lon)

    result = {
        "network": "cwop",
        "center": {"lat": lat, "lon": lon},
        "max_dist_miles": max_dist,
        "count": len(stations),
        "stations": stations,
        "source_url": str(resp.url),
    }
    await cache.set(cache_key, result, ttl=600)   # 10-minute cache for nearby lists
    return result
