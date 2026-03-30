"""NOAA Water Data — CO-OPS Tides & Currents + NWPS River Forecasts.

Combines two NOAA water data APIs:

1. CO-OPS (Center for Operational Oceanographic Products and Services)
   Tidal stations measuring water level, water temperature, air temperature,
   wind, and air pressure at ~400 coastal and Great Lakes stations.
   API: https://api.tidesandcurrents.noaa.gov/api/prod/datagetter

2. NWPS (National Water Prediction Service)
   River forecast gauges with observed levels and NWS flood stage forecasts.
   API: https://api.water.noaa.gov/nwps/v1/

Both APIs are free with no authentication required.
"""
import logging
from datetime import datetime, timedelta, timezone
from typing import Optional

import httpx
from fastapi import APIRouter, HTTPException, Query

import cache
import config
from models import Reading, Station, StationDetail

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Source registry metadata
# ---------------------------------------------------------------------------
SOURCE_NAME = "NOAA Water (CO-OPS + NWPS)"
SOURCE_DESCRIPTION = (
    "NOAA CO-OPS tidal/coastal observations (water level, temperature, wind, "
    "air pressure) and NOAA NWPS river gauge data with NWS flood-stage forecasts."
)
SOURCE_META = {
    "url": "https://tidesandcurrents.noaa.gov/",
    "auth_required": False,
    "update_frequency": "6 min (CO-OPS), 15 min (NWPS)",
    "coverage": "United States coasts, Great Lakes, major rivers",
    "params": ["water_level", "water_temperature", "air_temperature", "wind", "air_pressure", "river_stage"],
    "category": "water",
}

router = APIRouter(prefix="/api/noaa_water", tags=["NOAA Water"])

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
COOPS_DATA_BASE = "https://api.tidesandcurrents.noaa.gov/api/prod/datagetter"
COOPS_META_BASE = "https://api.tidesandcurrents.noaa.gov/mdapi/prod/webapi"
NWPS_BASE = "https://api.water.noaa.gov/nwps/v1"

# Regional bounding boxes for NWPS — the full US bbox times out upstream,
# so we split into manageable regions and fetch in parallel.
_NWPS_REGIONS = [
    {"bbox.xmin": "-125", "bbox.ymin": "25", "bbox.xmax": "-100", "bbox.ymax": "50", "srid": "EPSG_4326"},  # West
    {"bbox.xmin": "-100", "bbox.ymin": "25", "bbox.xmax": "-80",  "bbox.ymax": "50", "srid": "EPSG_4326"},  # Central
    {"bbox.xmin": "-80",  "bbox.ymin": "25", "bbox.xmax": "-65",  "bbox.ymax": "50", "srid": "EPSG_4326"},  # East
    {"bbox.xmin": "-170", "bbox.ymin": "50", "bbox.xmax": "-130", "bbox.ymax": "72", "srid": "EPSG_4326"},  # Alaska
    {"bbox.xmin": "-160", "bbox.ymin": "17", "bbox.xmax": "-154", "bbox.ymax": "23", "srid": "EPSG_4326"},  # Hawaii
]

# CO-OPS products and their human-readable labels / units
COOPS_PRODUCTS: dict[str, dict[str, str]] = {
    "water_level":          {"name": "Water Level",         "unit": "ft"},
    "air_temperature":      {"name": "Air Temperature",     "unit": "°F"},
    "water_temperature":    {"name": "Water Temperature",   "unit": "°F"},
    "wind":                 {"name": "Wind Speed",          "unit": "knots"},
    "air_pressure":         {"name": "Air Pressure",        "unit": "mb"},
    "humidity":             {"name": "Relative Humidity",   "unit": "%"},
    "hourly_height":        {"name": "Hourly Water Level",  "unit": "ft"},
    "high_low":             {"name": "High/Low Water Level","unit": "ft"},
    "conductivity":         {"name": "Conductivity",        "unit": "mS/cm"},
    "visibility":           {"name": "Visibility",          "unit": "nmi"},
    "salinity":             {"name": "Salinity",            "unit": "PSU"},
}

DEFAULT_COOPS_PRODUCT = "water_level"
DEFAULT_DATUM = "MLLW"

_HTTP_HEADERS = {
    "User-Agent": config.USER_AGENT,
    "Accept": "application/json",
}


# ---------------------------------------------------------------------------
# Helper utilities
# ---------------------------------------------------------------------------

def _make_client() -> httpx.AsyncClient:
    return httpx.AsyncClient(
        headers=_HTTP_HEADERS,
        timeout=config.HTTP_TIMEOUT,
        follow_redirects=True,
    )


def _dt_to_coops(dt: datetime) -> str:
    """Format a datetime as the CO-OPS API date string: YYYYMMdd HH:MM."""
    return dt.strftime("%Y%m%d %H:%M")


def _coops_station_to_model(raw: dict) -> Station:
    """Convert a CO-OPS station metadata dict to a Station model."""
    sid = str(raw.get("id", raw.get("stationId", "")))
    name = raw.get("name", sid)
    lat = float(raw.get("lat", raw.get("latitude", 0.0)))
    lon = float(raw.get("lng", raw.get("longitude", 0.0)))

    # Available parameters flags
    params: list[str] = []
    if raw.get("waterLevel"):
        params.append("water_level")
    if raw.get("airTemp"):
        params.append("air_temperature")
    if raw.get("waterTemp"):
        params.append("water_temperature")
    if raw.get("wind"):
        params.append("wind")
    if raw.get("airPressure"):
        params.append("air_pressure")
    if raw.get("humidity"):
        params.append("humidity")
    # If no flags present (some endpoints omit them), mark common ones
    if not params:
        params = ["water_level", "air_temperature", "water_temperature", "wind", "air_pressure"]

    return Station(
        id=sid,
        name=name,
        lat=lat,
        lon=lon,
        source="noaa_water",
        network="CO-OPS",
        params=params,
        url=f"https://tidesandcurrents.noaa.gov/stationhome.html?id={sid}",
        extra={
            "state": raw.get("state", ""),
            "timezone": raw.get("timezonecorr", raw.get("timezone", "")),
            "station_type": raw.get("stationType", raw.get("type", "")),
        },
    )


def _parse_coops_data(
    data: dict,
    station_id: str,
    product: str,
) -> list[Reading]:
    """Parse CO-OPS datagetter JSON into a list of Reading objects.

    The CO-OPS API wraps data in a top-level key that varies by product:
      - Most products: {"data": [{t, v, q}, ...]}
      - Wind:          {"data": [{t, s, d, dr, g, f}, ...]}  (s=speed, d=dir, g=gust)
    """
    product_meta = COOPS_PRODUCTS.get(product, {"name": product, "unit": ""})
    readings: list[Reading] = []

    raw_list = data.get("data", [])
    if not raw_list:
        return readings

    for entry in raw_list:
        dt_str = entry.get("t", "")
        try:
            ts = datetime.strptime(dt_str, "%Y-%m-%d %H:%M").replace(tzinfo=timezone.utc)
        except ValueError:
            continue

        quality = entry.get("q") or entry.get("f") or None

        if product == "wind":
            # Emit separate readings for speed, direction, and gust
            for key, pname, unit in (
                ("s", "Wind Speed",      "knots"),
                ("d", "Wind Direction",  "deg"),
                ("g", "Wind Gust",       "knots"),
            ):
                raw_val = entry.get(key)
                if raw_val in (None, ""):
                    continue
                try:
                    readings.append(
                        Reading(
                            station_id=station_id,
                            timestamp=ts,
                            parameter=pname,
                            value=float(raw_val),
                            unit=unit,
                            quality=quality,
                            source="noaa_water",
                        )
                    )
                except (TypeError, ValueError):
                    pass
        else:
            raw_val = entry.get("v")
            if raw_val in (None, ""):
                continue
            try:
                readings.append(
                    Reading(
                        station_id=station_id,
                        timestamp=ts,
                        parameter=product_meta["name"],
                        value=float(raw_val),
                        unit=product_meta["unit"],
                        quality=quality,
                        source="noaa_water",
                    )
                )
            except (TypeError, ValueError):
                pass

    return readings


def _nwps_gauge_state(raw: dict) -> str:
    """Extract the two-letter state abbreviation from an NWPS gauge dict.

    The NWPS API now returns ``state`` as a nested object
    ``{"abbreviation": "OR", "name": "Oregon"}`` instead of a plain string.
    Handle both formats for backward compatibility.
    """
    state_val = raw.get("state", "")
    if isinstance(state_val, dict):
        return state_val.get("abbreviation", "")
    return str(state_val)


def _nwps_gauge_to_station(raw: dict) -> Station:
    """Convert an NWPS gauge JSON object to a Station model."""
    gid = raw.get("lid", raw.get("gaugeId", ""))
    name = raw.get("name", gid)

    # Coordinates may live in a nested "latitude"/"longitude" or top-level
    lat = float(raw.get("latitude", raw.get("lat", 0.0)))
    lon = float(raw.get("longitude", raw.get("lon", 0.0)))

    state_abbr = _nwps_gauge_state(raw)

    # Extract flood stage thresholds for extra metadata.
    # The NWPS v1 API nests categories under flood.categories.{level}.stage;
    # the older format used flood.{level}Stage or flood.{level} directly.
    flood_stages: dict[str, float] = {}
    thresholds = raw.get("flood", {})
    if isinstance(thresholds, dict):
        categories = thresholds.get("categories", thresholds)
        if isinstance(categories, dict):
            for level in ("action", "minor", "moderate", "major"):
                # New format: {"categories": {"action": {"stage": 19, "flow": -9999}}}
                level_obj = categories.get(level, {})
                if isinstance(level_obj, dict):
                    val = level_obj.get("stage")
                else:
                    val = level_obj
                # Legacy format fallback: flood.actionStage, flood.action
                if val is None:
                    val = thresholds.get(f"{level}Stage") or thresholds.get(level)
                if val is not None:
                    try:
                        fval = float(val)
                        if fval > -999:
                            flood_stages[f"{level}_stage_ft"] = fval
                    except (TypeError, ValueError):
                        pass

    return Station(
        id=gid,
        name=name,
        lat=lat,
        lon=lon,
        source="noaa_water",
        network="NWPS",
        params=["river_stage", "river_discharge"],
        url=f"https://water.noaa.gov/gauges/{gid.lower()}",
        extra={
            "nws_lid": gid,
            "state": state_abbr,
            "huc": raw.get("huc", ""),
            "usgs_site": raw.get("usgs", {}).get("siteCode", "") if isinstance(raw.get("usgs"), dict) else "",
            **flood_stages,
        },
    )


def _parse_nwps_stageflow(stageflow_data: dict, gauge_id: str) -> list[Reading]:
    """Parse an NWPS stageflow (observed or forecast) response into Readings.

    The NWPS v1 API moved observed/forecast data to separate endpoints::

        GET /gauges/{id}/stageflow/observed
        GET /gauges/{id}/stageflow/forecast

    Each returns ``{"data": [{"validTime": "...", "primary": N, "secondary": N}, ...], ...}``.
    ``primary`` is typically stage (ft) and ``secondary`` is flow (kcfs).
    Units are given in ``primaryUnits`` and ``secondaryUnits`` at the top level.
    """
    readings: list[Reading] = []

    primary_unit = stageflow_data.get("primaryUnits", "ft")
    secondary_unit = stageflow_data.get("secondaryUnits", "kcfs")

    for entry in stageflow_data.get("data", []):
        dt_str = entry.get("validTime", "")
        raw_primary = entry.get("primary")
        raw_secondary = entry.get("secondary")

        try:
            ts = datetime.fromisoformat(dt_str.replace("Z", "+00:00"))
        except (ValueError, AttributeError):
            continue

        if raw_primary is not None:
            try:
                val = float(raw_primary)
                # -999 / -9999 are NWPS sentinels for missing data
                if val > -999:
                    readings.append(
                        Reading(
                            station_id=gauge_id,
                            timestamp=ts,
                            parameter="River Stage",
                            value=val,
                            unit=primary_unit,
                            quality=entry.get("qualityCode"),
                            source="noaa_water",
                        )
                    )
            except (TypeError, ValueError):
                pass

        if raw_secondary is not None:
            try:
                val = float(raw_secondary)
                if val > -999:
                    readings.append(
                        Reading(
                            station_id=gauge_id,
                            timestamp=ts,
                            parameter="River Discharge",
                            value=val,
                            unit=secondary_unit,
                            quality=entry.get("qualityCode"),
                            source="noaa_water",
                        )
                    )
            except (TypeError, ValueError):
                pass

    return readings


# ---------------------------------------------------------------------------
# CO-OPS Endpoints
# ---------------------------------------------------------------------------

@router.get("/coops/stations", summary="List all NOAA CO-OPS tidal/coastal stations")
async def coops_list_stations():
    """Return all active CO-OPS stations from the NOAA metadata API.

    Includes ~400 primary tidal stations and subordinate stations across
    US coasts, Puerto Rico, Guam, and the Great Lakes.
    """
    cache_key = "noaa_water:coops:stations"
    cached = await cache.get(cache_key)
    if cached is not None:
        return cached

    url = f"{COOPS_META_BASE}/stations.json"
    # The CO-OPS metadata API only accepts a single 'type' value; comma-separated
    # values cause HTTP 400.  Use 'waterlevels' to return all water-level stations
    # (the most broadly useful set covering tidal, Great Lakes, and river gauges).
    params_query = {"type": "waterlevels", "units": "english"}

    try:
        async with _make_client() as client:
            resp = await client.get(url, params=params_query)
            resp.raise_for_status()
            data = resp.json()
    except httpx.TimeoutException:
        raise HTTPException(status_code=504, detail="NOAA CO-OPS metadata API timeout")
    except httpx.HTTPStatusError as exc:
        raise HTTPException(status_code=exc.response.status_code, detail=f"NOAA CO-OPS error: {exc}")
    except Exception as exc:
        logger.exception("Error fetching CO-OPS station list")
        raise HTTPException(status_code=500, detail=str(exc))

    raw_stations = data.get("stations", [])
    if not raw_stations:
        logger.warning("CO-OPS station list returned empty; raw keys: %s", list(data.keys()))

    stations = [_coops_station_to_model(s) for s in raw_stations]
    result = {"stations": [s.model_dump() for s in stations], "count": len(stations)}
    await cache.set(cache_key, result, ttl=config.CACHE_TTL_LONG)
    return result


@router.get("/coops/stations/{station_id}", summary="CO-OPS station detail with latest readings")
async def coops_get_station(station_id: str):
    """Return CO-OPS station metadata plus the most recent observation for each
    available product (water level, temperature, wind, pressure).
    """
    cache_key = f"noaa_water:coops:station:{station_id}"
    cached = await cache.get(cache_key)
    if cached is not None:
        return cached

    # Fetch station metadata
    meta_url = f"{COOPS_META_BASE}/stations/{station_id}.json"
    try:
        async with _make_client() as client:
            meta_resp = await client.get(meta_url)
            meta_resp.raise_for_status()
            meta_data = meta_resp.json()
    except httpx.HTTPStatusError as exc:
        if exc.response.status_code == 404:
            raise HTTPException(status_code=404, detail=f"CO-OPS station '{station_id}' not found")
        raise HTTPException(status_code=exc.response.status_code, detail=f"NOAA CO-OPS error: {exc}")
    except httpx.TimeoutException:
        raise HTTPException(status_code=504, detail="NOAA CO-OPS API timeout")
    except Exception as exc:
        logger.exception("Error fetching CO-OPS station metadata for %s", station_id)
        raise HTTPException(status_code=500, detail=str(exc))

    # The single-station metadata endpoint returns the station under "stations"[0]
    raw_list = meta_data.get("stations", [])
    if not raw_list:
        raise HTTPException(status_code=404, detail=f"CO-OPS station '{station_id}' not found")

    station = _coops_station_to_model(raw_list[0])

    # Fetch latest readings (last 2 hours) for each available product
    now = datetime.now(tz=timezone.utc)
    begin_dt = now - timedelta(hours=2)
    all_readings: list[Reading] = []

    products_to_fetch = [p for p in station.params if p in COOPS_PRODUCTS]
    if not products_to_fetch:
        products_to_fetch = [DEFAULT_COOPS_PRODUCT]

    async with _make_client() as client:
        for product in products_to_fetch:
            params_query = {
                "begin_date": _dt_to_coops(begin_dt),
                "end_date": _dt_to_coops(now),
                "station": station_id,
                "product": product,
                "datum": DEFAULT_DATUM if product == "water_level" else "STND",
                "units": "english",
                "time_zone": "gmt",
                "format": "json",
                "application": "wxsection",
            }
            try:
                resp = await client.get(COOPS_DATA_BASE, params=params_query)
                resp.raise_for_status()
                product_data = resp.json()
                if "error" not in product_data:
                    all_readings.extend(_parse_coops_data(product_data, station_id, product))
            except Exception as exc:
                # Non-fatal: station may not support every product
                logger.debug("CO-OPS product '%s' unavailable for %s: %s", product, station_id, exc)

    # Keep only the latest reading per parameter
    latest: dict[str, Reading] = {}
    for r in all_readings:
        if r.parameter not in latest or r.timestamp > latest[r.parameter].timestamp:
            latest[r.parameter] = r

    detail = StationDetail(station=station, readings=list(latest.values()))
    result = detail.model_dump()
    await cache.set(cache_key, result, ttl=config.CACHE_TTL)
    return result


@router.get(
    "/coops/stations/{station_id}/readings",
    summary="CO-OPS time-series observations",
)
async def coops_get_readings(
    station_id: str,
    product: str = Query(
        DEFAULT_COOPS_PRODUCT,
        description=(
            "Observation product: water_level, air_temperature, water_temperature, "
            "wind, air_pressure, humidity, conductivity"
        ),
    ),
    hours: int = Query(24, ge=1, le=8760, description="Hours of history (max 1 year = 8760 h)"),
    datum: str = Query(
        DEFAULT_DATUM,
        description="Tidal datum for water_level: MLLW (default), MSL, NAVD, STND, etc.",
    ),
):
    """Return time-series CO-OPS observations for a station.

    The CO-OPS API returns up to 31 days per request; this endpoint transparently
    handles any hours value up to 8760 (1 year) by splitting into monthly chunks
    when necessary.
    """
    if product not in COOPS_PRODUCTS:
        raise HTTPException(
            status_code=400,
            detail=f"Unknown product '{product}'. Valid options: {', '.join(COOPS_PRODUCTS)}",
        )

    cache_key = f"noaa_water:coops:readings:{station_id}:{product}:{hours}:{datum}"
    cached = await cache.get(cache_key)
    if cached is not None:
        return cached

    now = datetime.now(tz=timezone.utc)
    begin_dt = now - timedelta(hours=hours)

    # CO-OPS limits to 31-day windows; split if needed
    max_window_days = 31
    all_readings: list[Reading] = []

    chunk_start = begin_dt
    while chunk_start < now:
        chunk_end = min(chunk_start + timedelta(days=max_window_days), now)

        params_query = {
            "begin_date": _dt_to_coops(chunk_start),
            "end_date": _dt_to_coops(chunk_end),
            "station": station_id,
            "product": product,
            "datum": datum if product == "water_level" else "STND",
            "units": "english",
            "time_zone": "gmt",
            "format": "json",
            "application": "wxsection",
        }

        try:
            async with _make_client() as client:
                resp = await client.get(COOPS_DATA_BASE, params=params_query)
                resp.raise_for_status()
                data = resp.json()
        except httpx.TimeoutException:
            raise HTTPException(status_code=504, detail="NOAA CO-OPS API timeout")
        except httpx.HTTPStatusError as exc:
            raise HTTPException(status_code=exc.response.status_code, detail=f"NOAA CO-OPS error: {exc}")
        except Exception as exc:
            logger.exception("Error fetching CO-OPS readings for %s / %s", station_id, product)
            raise HTTPException(status_code=500, detail=str(exc))

        if "error" in data:
            err_msg = data["error"].get("message", str(data["error"]))
            # Some stations genuinely lack certain products — surface a clean error
            raise HTTPException(
                status_code=422,
                detail=f"CO-OPS API error for station {station_id} / {product}: {err_msg}",
            )

        all_readings.extend(_parse_coops_data(data, station_id, product))
        chunk_start = chunk_end

    all_readings.sort(key=lambda r: r.timestamp)

    result = {
        "station_id": station_id,
        "product": product,
        "datum": datum,
        "hours": hours,
        "count": len(all_readings),
        "readings": [r.model_dump() for r in all_readings],
    }
    await cache.set(cache_key, result, ttl=config.CACHE_TTL)
    return result


# ---------------------------------------------------------------------------
# NWPS (River Forecast) Endpoints
# ---------------------------------------------------------------------------

@router.get("/nwps/gauges", summary="List all NWPS river forecast gauges")
async def nwps_list_gauges(
    state: Optional[str] = Query(None, description="Two-letter state code filter (e.g. 'OR')"),
):
    """Return NWPS river forecast gauge locations.

    The NWPS /gauges endpoint requires a bounding-box query with an explicit
    SRID to avoid server-side timeouts.  A US-wide bbox is used by default.
    The optional state filter is applied client-side.
    """
    cache_key = f"noaa_water:nwps:gauges:{state}"
    cached = await cache.get(cache_key)
    if cached is not None:
        return cached

    url = f"{NWPS_BASE}/gauges"
    # Fetch all regions in parallel — full US bbox times out upstream
    raw_gauges: list[dict] = []
    seen_ids: set[str] = set()
    try:
        async with _make_client() as client:
            import asyncio as _asyncio

            async def _fetch_region(params: dict) -> list[dict]:
                r = await client.get(url, params=params, timeout=30)
                r.raise_for_status()
                d = r.json()
                return d if isinstance(d, list) else d.get("gauges", [])

            results = await _asyncio.gather(
                *[_fetch_region(p) for p in _NWPS_REGIONS],
                return_exceptions=True,
            )
            for res in results:
                if isinstance(res, list):
                    for g in res:
                        gid = g.get("lid") or g.get("id") or g.get("gaugeId", "")
                        if gid and gid not in seen_ids:
                            seen_ids.add(gid)
                            raw_gauges.append(g)
                elif isinstance(res, Exception):
                    logger.debug("NWPS region fetch error: %s", res)
    except httpx.TimeoutException:
        raise HTTPException(status_code=504, detail="NOAA NWPS API timeout")
    except httpx.HTTPStatusError as exc:
        raise HTTPException(status_code=exc.response.status_code, detail=f"NOAA NWPS error: {exc}")
    except Exception as exc:
        logger.exception("Error fetching NWPS gauge list")
        raise HTTPException(status_code=500, detail=str(exc))

    if state:
        state_upper = state.upper()
        raw_gauges = [
            g for g in raw_gauges
            if _nwps_gauge_state(g).upper() == state_upper
        ]

    stations = [_nwps_gauge_to_station(g) for g in raw_gauges]
    result = {"gauges": [s.model_dump() for s in stations], "count": len(stations)}
    await cache.set(cache_key, result, ttl=config.CACHE_TTL_LONG)
    return result


@router.get("/nwps/gauges/{gauge_id}", summary="NWPS river gauge detail with forecast")
async def nwps_get_gauge(gauge_id: str):
    """Return detailed NWPS river gauge data including:

    - Station metadata and flood-stage thresholds
    - Recent observed stage and discharge readings
    - NWS flood-stage forecast (if available)

    Gauge IDs are NWS Location IDs (LIDs), e.g. 'PTLO3' for Portland, OR.

    The NWPS v1 API serves observed and forecast stageflow data at separate
    sub-endpoints (``/stageflow/observed`` and ``/stageflow/forecast``)
    rather than embedding them in the gauge metadata response.
    """
    cache_key = f"noaa_water:nwps:gauge:{gauge_id}"
    cached = await cache.get(cache_key)
    if cached is not None:
        return cached

    gid = gauge_id.upper()
    url = f"{NWPS_BASE}/gauges/{gid}"
    try:
        async with _make_client() as client:
            resp = await client.get(url)
            resp.raise_for_status()
            data = resp.json()
    except httpx.HTTPStatusError as exc:
        if exc.response.status_code == 404:
            raise HTTPException(status_code=404, detail=f"NWPS gauge '{gauge_id}' not found")
        raise HTTPException(status_code=exc.response.status_code, detail=f"NOAA NWPS error: {exc}")
    except httpx.TimeoutException:
        raise HTTPException(status_code=504, detail="NOAA NWPS API timeout")
    except Exception as exc:
        logger.exception("Error fetching NWPS gauge %s", gauge_id)
        raise HTTPException(status_code=500, detail=str(exc))

    station = _nwps_gauge_to_station(data)

    # --- Fetch observed stageflow from the dedicated sub-endpoint ---
    observed_readings: list[Reading] = []
    try:
        async with _make_client() as client:
            obs_resp = await client.get(f"{NWPS_BASE}/gauges/{gid}/stageflow/observed")
            obs_resp.raise_for_status()
            obs_data = obs_resp.json()
        observed_readings = _parse_nwps_stageflow(obs_data, gid)
        observed_readings.sort(key=lambda r: r.timestamp)
    except Exception as exc:
        logger.debug("NWPS observed stageflow unavailable for %s: %s", gid, exc)

    # --- Fetch forecast stageflow from the dedicated sub-endpoint ---
    forecast_data: dict = {}
    try:
        async with _make_client() as client:
            fcst_resp = await client.get(f"{NWPS_BASE}/gauges/{gid}/stageflow/forecast")
            fcst_resp.raise_for_status()
            fcst_raw = fcst_resp.json()

        forecast_entries = []
        for entry in fcst_raw.get("data", []):
            dt_str = entry.get("validTime", "")
            try:
                ts = datetime.fromisoformat(dt_str.replace("Z", "+00:00"))
                primary = entry.get("primary")
                secondary = entry.get("secondary")
                forecast_entries.append(
                    {
                        "timestamp": ts.isoformat(),
                        "stage_ft": primary if primary not in (None, -999, -9999) else None,
                        "discharge_kcfs": secondary if secondary not in (None, -999, -9999) else None,
                        "stage_status": entry.get("primaryStatus"),
                    }
                )
            except (ValueError, AttributeError):
                continue

        forecast_data = {
            "issued": fcst_raw.get("issuedTime"),
            "count": len(forecast_entries),
            "entries": forecast_entries,
        }
    except Exception as exc:
        logger.debug("NWPS forecast stageflow unavailable for %s: %s", gid, exc)

    # --- Extract flood categories from the new nested structure ---
    flood_raw = data.get("flood", {})
    flood_categories: dict = {}
    if isinstance(flood_raw, dict):
        cats = flood_raw.get("categories", flood_raw)
        if isinstance(cats, dict):
            flood_categories = {
                "stage_units": flood_raw.get("stageUnits", "ft"),
                "flow_units": flood_raw.get("flowUnits", "cfs"),
                "categories": cats,
            }

    detail = StationDetail(station=station, readings=observed_readings)
    result = {
        **detail.model_dump(),
        "forecast": forecast_data,
        "flood_categories": flood_categories,
        "status": data.get("status", {}),
    }
    await cache.set(cache_key, result, ttl=config.CACHE_TTL)
    return result
