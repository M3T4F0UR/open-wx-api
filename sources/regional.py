"""Regional Weather & Water Networks — direct API integrations.

Covers five state/regional networks that publish their own REST APIs,
separate from the Synoptic/IEM aggregators already implemented:

  1. Montana Mesonet (University of Montana)
       216+ stations statewide — weather, soil, and agro-meteorological data.
       https://mesonet.climate.umt.edu/api/v2/

  2. Kansas Mesonet (Kansas State University)
       100+ stations — 5-minute weather, soil moisture, and ET data.
       https://mesonet.k-state.edu/rest/

  3. AZMet (Arizona Meteorological Network, University of Arizona)
       ~40 active stations — hourly and daily ag-weather with ET.
       https://api.azmet.arizona.edu/v1/

  4. Colorado DWR CDSS (Division of Water Resources)
       6,000+ surface-water monitoring stations — streamflow, diversions, storage.
       https://dwr.state.co.us/rest/get/api/v2/

  5. TVA (Tennessee Valley Authority)
       49 reservoirs across 7 southeastern states — pool/tailwater elevations,
       inflow, and outflow.
       https://www.tva.com/environment/lake-levels/

All five APIs are unauthenticated and return JSON.
"""
from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

import httpx
from fastapi import APIRouter, HTTPException, Query

import cache
import config
from models import Reading, Station, StationDetail

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Source registry metadata  (read by server.py /api/sources)
# ---------------------------------------------------------------------------

SOURCE_NAME = "Regional Networks"
SOURCE_DESCRIPTION = (
    "Direct integrations with state and regional monitoring networks: "
    "Montana Mesonet, Kansas Mesonet, AZMet (Arizona), Colorado DWR CDSS "
    "surface-water stations, and TVA lake/reservoir levels.  All sources are "
    "unauthenticated REST APIs with no API key required."
)
SOURCE_META = {
    "url": "https://mesonet.climate.umt.edu/api/v2/",
    "auth_required": False,
    "update_frequency": "5 min – 1 hr (varies by network)",
    "coverage": "MT, KS, AZ, CO, TN/AL/GA/KY/MS/NC/VA",
    "params": [
        "temperature", "precipitation", "wind", "humidity",
        "soil_moisture", "solar_radiation", "evapotranspiration",
        "streamflow", "reservoir_level",
    ],
    "category": "weather",
}

# ---------------------------------------------------------------------------
# Base URLs
# ---------------------------------------------------------------------------

_MONTANA_BASE  = "https://mesonet.climate.umt.edu/api/v2"
_KANSAS_BASE   = "https://mesonet.k-state.edu/rest"
_AZMET_BASE    = "https://api.azmet.arizona.edu/v1"
_CDSS_BASE     = "https://dwr.state.co.us/rest/get/api/v2"
_TVA_JSON_URL  = (
    "https://www.tva.com/file_source/TVA/Site%20Content/Environment"
    "/Lake%20Levels/lake-levels-data.json"
)
# NOTE: TVA's old file_source URL returns HTTP 403.  We now use the
# RestApi/observed-data/{code}.json endpoint discovered in the TVA website,
# which provides per-reservoir hourly pool elevation, tailwater, and discharge.

# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------

_HTTP_HEADERS = {
    "User-Agent": config.USER_AGENT,
    "Accept": "application/json",
}


def _client() -> httpx.AsyncClient:
    """Return a shared async HTTP client with project-standard settings."""
    return httpx.AsyncClient(
        headers=_HTTP_HEADERS,
        timeout=config.HTTP_TIMEOUT,
        follow_redirects=True,
    )


async def _get_json(url: str, params: Optional[dict] = None, *, source_label: str) -> Any:
    """Perform a GET request and return parsed JSON; raise HTTPException on failure.

    Note: ``params`` is only passed to httpx when it is non-empty.  Passing an
    empty dict to httpx causes it to strip any query-string already embedded in
    ``url``, which would silently break URLs like
    ``…/network.php?network=NECLIMATE``.
    """
    try:
        async with _client() as client:
            # Only forward the params dict when it contains actual key/value pairs.
            # An empty dict would cause httpx to discard any query-string already
            # present in the url (httpx merges/replaces the query component).
            kw = {"params": params} if params else {}
            resp = await client.get(url, **kw)
            resp.raise_for_status()
            return resp.json()
    except httpx.TimeoutException:
        raise HTTPException(
            status_code=504,
            detail=f"{source_label} API timeout ({url})",
        )
    except httpx.HTTPStatusError as exc:
        raise HTTPException(
            status_code=exc.response.status_code,
            detail=f"{source_label} API error: {exc}",
        )
    except Exception as exc:
        logger.exception("Unexpected error calling %s: %s", source_label, url)
        raise HTTPException(status_code=502, detail=f"{source_label}: {exc}")


# ---------------------------------------------------------------------------
# Router
# ---------------------------------------------------------------------------

router = APIRouter(prefix="/api/regional", tags=["Regional Networks"])


# ===========================================================================
# 1. Montana Mesonet
# ===========================================================================

# Montana Mesonet variable → (canonical parameter name, unit)
_MONTANA_VAR_MAP: dict[str, tuple[str, str]] = {
    "air_temp":          ("temperature",       "°C"),
    "humidity":          ("relative_humidity", "%"),
    "wind_speed":        ("wind_speed",        "m/s"),
    "wind_direction":    ("wind_direction",     "deg"),
    "precipitation":     ("precipitation",     "mm"),
    "soil_temp":         ("soil_temperature",  "°C"),
    "soil_vwc":          ("soil_moisture",     "m³/m³"),
    "solar_radiation":   ("solar_radiation",   "W/m²"),
    "snow_depth":        ("snow_depth",        "mm"),
    "gust":              ("wind_gust",         "m/s"),
    "pressure":          ("pressure",          "hPa"),
}


def _montana_to_station(raw: dict) -> Station:
    """Convert a Montana Mesonet station JSON object to the shared Station model."""
    sid = str(raw.get("station", raw.get("id", "")))
    lat = _safe_float(raw.get("latitude") or raw.get("lat"))
    lon = _safe_float(raw.get("longitude") or raw.get("lng") or raw.get("lon"))

    # Build param list from whichever keys exist in the response
    params: list[str] = []
    for api_key, (canonical, _) in _MONTANA_VAR_MAP.items():
        if api_key in raw and canonical not in params:
            params.append(canonical)

    last_update = _parse_iso(raw.get("datetime") or raw.get("last_observation"))

    return Station(
        id=sid,
        name=raw.get("name", sid),
        lat=lat,
        lon=lon,
        source="montana_mesonet",
        network="Montana Mesonet",
        params=params if params else list(_MONTANA_VAR_MAP[k][0] for k in list(_MONTANA_VAR_MAP)[:6]),
        elevation=_safe_float(raw.get("elevation")),
        url=f"https://mesonet.climate.umt.edu/stations/{sid}/",
        last_update=last_update,
        extra={
            "county":   raw.get("county", ""),
            "sub_network": raw.get("sub_network", ""),
            "active":   raw.get("active", True),
        },
    )


def _montana_obs_to_readings(obs: dict, station_id: str) -> list[Reading]:
    """Extract Reading objects from a Montana Mesonet observation record."""
    ts_str = obs.get("datetime") or obs.get("timestamp")
    ts = _parse_iso(ts_str) or datetime.now(timezone.utc)

    readings: list[Reading] = []
    for api_key, (param_name, unit) in _MONTANA_VAR_MAP.items():
        raw_val = obs.get(api_key)
        if raw_val is None:
            continue
        try:
            readings.append(
                Reading(
                    station_id=station_id,
                    timestamp=ts,
                    parameter=param_name,
                    value=float(raw_val),
                    unit=unit,
                    quality=obs.get("qc_flag") or obs.get("quality"),
                    source="montana_mesonet",
                )
            )
        except (TypeError, ValueError):
            continue
    return readings


@router.get(
    "/montana/stations",
    summary="List all Montana Mesonet stations",
)
async def montana_stations():
    """Return the full list of Montana Mesonet stations (216+ sites).

    Metadata includes station name, county, sub-network affiliation, location,
    and elevation.  Stations span weather, soil moisture, and agro-meteorological
    monitoring across Montana.
    """
    cache_key = "regional:montana:stations"
    cached = await cache.get(cache_key)
    if cached is not None:
        return cached

    # The Montana Mesonet API requires ?type=json; without it the endpoint
    # returns an HTML page (200 OK) instead of JSON data.
    data = await _get_json(
        f"{_MONTANA_BASE}/stations/",
        params={"type": "json", "active": "true"},
        source_label="Montana Mesonet",
    )

    # The API returns a list directly or wraps it in a "data" key
    raw_list: list[dict] = data if isinstance(data, list) else data.get("data", data.get("stations", []))

    stations: list[dict] = []
    for raw in raw_list:
        try:
            stations.append(_montana_to_station(raw).model_dump())
        except Exception:
            logger.debug("Failed to parse Montana station: %s", raw, exc_info=True)

    result = {
        "source": "montana_mesonet",
        "network": "Montana Mesonet (University of Montana)",
        "url": "https://mesonet.climate.umt.edu/",
        "count": len(stations),
        "stations": stations,
    }
    await cache.set(cache_key, result, ttl=config.CACHE_TTL_LONG)
    return result


@router.get(
    "/montana/stations/{station_id}",
    summary="Montana Mesonet station detail with latest readings",
)
async def montana_station_detail(station_id: str):
    """Return metadata and the most recent observations for a single Montana Mesonet station.

    `station_id` is the Montana Mesonet station identifier (e.g. ``aceabsaroka``).
    Latest observations are fetched from the ``/latest/`` endpoint and normalised
    to the shared Reading model.
    """
    cache_key = f"regional:montana:station:{station_id}"
    cached = await cache.get(cache_key)
    if cached is not None:
        return cached

    # Fetch station metadata and latest observation in parallel
    meta_url   = f"{_MONTANA_BASE}/stations/{station_id}/"
    latest_url = f"{_MONTANA_BASE}/latest/"

    try:
        async with _client() as http:
            meta_resp, latest_resp = await _gather_two(
                http.get(meta_url),
                http.get(latest_url, params={"station": station_id}),
            )
    except HTTPException:
        raise
    except httpx.TimeoutException:
        raise HTTPException(status_code=504, detail="Montana Mesonet API timeout")
    except httpx.HTTPStatusError as exc:
        if exc.response.status_code == 404:
            raise HTTPException(
                status_code=404,
                detail=f"Montana Mesonet station '{station_id}' not found",
            )
        raise HTTPException(
            status_code=exc.response.status_code,
            detail=f"Montana Mesonet API error: {exc}",
        )
    except Exception as exc:
        logger.exception("Error fetching Montana station %s", station_id)
        raise HTTPException(status_code=502, detail=str(exc))

    # Parse metadata
    meta_data = meta_resp.json()
    raw_meta = meta_data if isinstance(meta_data, dict) else (meta_data[0] if meta_data else {})
    if not raw_meta:
        raise HTTPException(
            status_code=404,
            detail=f"Montana Mesonet station '{station_id}' not found",
        )
    station = _montana_to_station(raw_meta)

    # Parse latest observations
    latest_data = latest_resp.json()
    raw_obs_list: list[dict] = (
        latest_data if isinstance(latest_data, list)
        else latest_data.get("data", latest_data.get("observations", []))
    )
    readings: list[Reading] = []
    for obs in raw_obs_list:
        readings.extend(_montana_obs_to_readings(obs, station_id))

    # Keep only the most recent reading per parameter
    readings = _latest_per_param(readings)
    readings.sort(key=lambda r: r.parameter)

    detail = StationDetail(station=station, readings=readings)
    result = detail.model_dump()
    await cache.set(cache_key, result, ttl=config.CACHE_TTL)
    return result


# ===========================================================================
# 2. Kansas Mesonet
# ===========================================================================

# Kansas Mesonet field → (canonical parameter name, unit)
_KANSAS_VAR_MAP: dict[str, tuple[str, str]] = {
    "TEMP2M":    ("temperature",         "°C"),
    "TEMP10M":   ("temperature_10m",     "°C"),
    "RELH":      ("relative_humidity",   "%"),
    "WSPD":      ("wind_speed",          "m/s"),
    "WDIR":      ("wind_direction",      "deg"),
    "WMAX":      ("wind_gust",           "m/s"),
    "PRECIP":    ("precipitation",       "mm"),
    "SRAD":      ("solar_radiation",     "W/m²"),
    "PRES":      ("pressure",            "hPa"),
    "TSOIL5":    ("soil_temp_5cm",       "°C"),
    "TSOIL10":   ("soil_temp_10cm",      "°C"),
    "TSOIL20":   ("soil_temp_20cm",      "°C"),
    "VWCS":      ("soil_moisture",       "m³/m³"),
    "EVAP":      ("evapotranspiration",  "mm"),
    "LEAFWET":   ("leaf_wetness",        "min"),
}


def _kansas_to_station(raw: dict) -> Station:
    """Convert a Kansas Mesonet station JSON object to the shared Station model."""
    sid = str(raw.get("STID") or raw.get("stid") or raw.get("station", ""))
    lat = _safe_float(raw.get("LATITUDE") or raw.get("lat"))
    lon = _safe_float(raw.get("LONGITUDE") or raw.get("lon"))

    # Parse timestamp from current-conditions response (TIMESTAMP or TIME field)
    last_update = _parse_iso(
        raw.get("TIMESTAMP") or raw.get("TIME") or raw.get("datetime")
    )

    return Station(
        id=sid,
        name=raw.get("STNM") or raw.get("name") or sid,
        lat=lat,
        lon=lon,
        source="kansas_mesonet",
        network="Kansas Mesonet",
        params=list(_KANSAS_VAR_MAP[k][0] for k in list(_KANSAS_VAR_MAP)[:8]),
        elevation=_safe_float(raw.get("ELEV") or raw.get("elevation")),
        url=f"https://mesonet.k-state.edu/station/{sid}/",
        last_update=last_update,
        extra={
            "county":    raw.get("COUNTY", raw.get("county", "")),
            "start_date": raw.get("START_DATE", raw.get("start_date", "")),
        },
    )


def _kansas_obs_to_readings(obs: dict, station_id: str, ts: datetime) -> list[Reading]:
    """Extract Reading objects from a single Kansas Mesonet observation row."""
    readings: list[Reading] = []
    for field, (param_name, unit) in _KANSAS_VAR_MAP.items():
        raw_val = obs.get(field)
        if raw_val is None:
            continue
        try:
            readings.append(
                Reading(
                    station_id=station_id,
                    timestamp=ts,
                    parameter=param_name,
                    value=float(raw_val),
                    unit=unit,
                    quality=None,
                    source="kansas_mesonet",
                )
            )
        except (TypeError, ValueError):
            continue
    return readings


@router.get(
    "/kansas/stations",
    summary="List all Kansas Mesonet stations",
)
async def kansas_stations():
    """Return the full list of Kansas Mesonet stations.

    Data is sourced from the KSU stationlist endpoint.  Each station entry
    includes location, elevation, county, and the date it joined the network.
    """
    cache_key = "regional:kansas:stations"
    cached = await cache.get(cache_key)
    if cached is not None:
        return cached

    # Kansas Mesonet station list is available as CSV at /rest/stationnames/
    # (the old /rest/stationlist/ endpoint returns HTTP 404).
    # The CSV has columns: NAME,COUNTY,LATITUDE,LONGITUDE,ELEVATION,NETWORK,ABBR,...
    import csv as _csv
    import io as _io

    try:
        async with _client() as client:
            resp = await client.get(f"{_KANSAS_BASE}/stationnames/")
            resp.raise_for_status()
            csv_text = resp.text
    except httpx.TimeoutException:
        raise HTTPException(status_code=504, detail="Kansas Mesonet API timeout")
    except httpx.HTTPStatusError as exc:
        raise HTTPException(
            status_code=exc.response.status_code,
            detail=f"Kansas Mesonet API error: {exc}",
        )
    except Exception as exc:
        logger.exception("Unexpected error fetching Kansas Mesonet station list")
        raise HTTPException(status_code=502, detail=str(exc))

    # Parse the CSV into a list of dicts matching the expected _kansas_to_station fields
    raw_list: list[dict] = []
    try:
        reader = _csv.DictReader(_io.StringIO(csv_text))
        for row in reader:
            # Map CSV columns → field names expected by _kansas_to_station
            raw_list.append({
                "STID":      row.get("ABBR", "").strip(),
                "stid":      row.get("ABBR", "").strip(),
                "STNM":      row.get("NAME", "").strip(),
                "LATITUDE":  row.get("LATITUDE", "").strip(),
                "LONGITUDE": row.get("LONGITUDE", "").strip(),
                "ELEV":      row.get("ELEVATION", "").strip(),
                "COUNTY":    row.get("COUNTY", "").strip(),
            })
    except Exception as exc:
        logger.warning("Failed to parse Kansas Mesonet CSV: %s", exc)

    stations: list[dict] = []
    for raw in raw_list:
        try:
            stations.append(_kansas_to_station(raw).model_dump())
        except Exception:
            logger.debug("Failed to parse Kansas station: %s", raw, exc_info=True)

    result = {
        "source": "kansas_mesonet",
        "network": "Kansas Mesonet (Kansas State University)",
        "url": "https://mesonet.k-state.edu/",
        "count": len(stations),
        "stations": stations,
    }
    await cache.set(cache_key, result, ttl=config.CACHE_TTL_LONG)
    return result


@router.get(
    "/kansas/current",
    summary="Current conditions for all Kansas Mesonet stations",
)
async def kansas_current():
    """Return the most recent 5-minute observations for all Kansas Mesonet stations.

    Readings include temperature, humidity, wind speed, wind direction, wind gust,
    precipitation, solar radiation, soil temperatures, soil moisture, and
    Penman-Monteith reference ET where available.
    """
    cache_key = "regional:kansas:current"
    cached = await cache.get(cache_key)
    if cached is not None:
        return cached

    data = await _get_json(f"{_KANSAS_BASE}/current/", source_label="Kansas Mesonet")

    raw_list: list[dict] = data if isinstance(data, list) else data.get("current", data.get("data", []))

    stations_out: list[dict] = []
    readings_out: list[dict] = []
    now = datetime.now(timezone.utc)

    for obs in raw_list:
        sid = str(obs.get("STID") or obs.get("stid") or obs.get("station", ""))
        if not sid:
            continue

        # Parse timestamp from observation; fall back to now
        ts_raw = obs.get("TIMESTAMP") or obs.get("TIME") or obs.get("datetime")
        ts = _parse_iso(ts_raw) or now

        try:
            station = _kansas_to_station(obs)
            stations_out.append(station.model_dump())
        except Exception:
            logger.debug("Failed to parse Kansas station obs: %s", obs, exc_info=True)
            continue

        readings = _kansas_obs_to_readings(obs, sid, ts)
        readings_out.extend(r.model_dump() for r in readings)

    result = {
        "source": "kansas_mesonet",
        "station_count": len(stations_out),
        "reading_count": len(readings_out),
        "stations": stations_out,
        "readings": readings_out,
    }
    await cache.set(cache_key, result, ttl=config.CACHE_TTL)
    return result


# ===========================================================================
# 3. AZMet (Arizona Meteorological Network)
# ===========================================================================

# Station metadata from https://azmet.arizona.edu/about/station-metadata
# (station_id → {name, lat, lon, elevation_ft, county, active})
_AZMET_STATION_META: dict[str, dict] = {
    "az01": {"name": "Tucson",               "lat": 32.28,  "lon": -110.95, "elev_ft": 2343, "county": "Pima",           "active": True},
    "az02": {"name": "Yuma Valley",           "lat": 32.71,  "lon": -114.70, "elev_ft":  118, "county": "Yuma",           "active": True},
    "az03": {"name": "Yuma Mesa",             "lat": 32.61,  "lon": -114.64, "elev_ft":  190, "county": "Yuma",           "active": False},
    "az04": {"name": "Safford",               "lat": 32.81,  "lon": -109.68, "elev_ft": 2963, "county": "Graham",         "active": True},
    "az05": {"name": "Coolidge",              "lat": 32.98,  "lon": -111.61, "elev_ft": 1388, "county": "Pinal",          "active": True},
    "az06": {"name": "Maricopa",              "lat": 33.07,  "lon": -111.97, "elev_ft": 1188, "county": "Pinal",          "active": True},
    "az07": {"name": "Aguila",                "lat": 33.95,  "lon": -113.19, "elev_ft": 2155, "county": "Maricopa",       "active": True},
    "az08": {"name": "Parker",                "lat": 33.96,  "lon": -114.49, "elev_ft":  322, "county": "La Paz",         "active": True},
    "az09": {"name": "Bonita",                "lat": 32.48,  "lon": -109.93, "elev_ft": 4416, "county": "Graham",         "active": True},
    "az10": {"name": "Waddell",               "lat": 33.62,  "lon": -112.46, "elev_ft": 1332, "county": "Maricopa",       "active": False},
    "az11": {"name": "Litchfield",            "lat": 33.47,  "lon": -112.40, "elev_ft": 1007, "county": "Maricopa",       "active": False},
    "az12": {"name": "Phoenix Greenway",      "lat": 33.62,  "lon": -112.11, "elev_ft": 1322, "county": "Maricopa",       "active": True},
    "az13": {"name": "Marana",                "lat": 32.42,  "lon": -111.16, "elev_ft": 2041, "county": "Pima",           "active": False},
    "az14": {"name": "Yuma N.Gila",           "lat": 32.78,  "lon": -114.52, "elev_ft":  140, "county": "Yuma",           "active": True},
    "az15": {"name": "Phoenix Encanto",       "lat": 33.48,  "lon": -112.10, "elev_ft": 1096, "county": "Maricopa",       "active": True},
    "az16": {"name": "Eloy",                  "lat": 32.81,  "lon": -111.56, "elev_ft": 1512, "county": "Pinal",          "active": False},
    "az17": {"name": "Dateland",              "lat": 32.99,  "lon": -113.49, "elev_ft":  531, "county": "Yuma",           "active": False},
    "az18": {"name": "Scottsdale",            "lat": 33.64,  "lon": -111.90, "elev_ft": 1562, "county": "Maricopa",       "active": False},
    "az19": {"name": "Paloma",                "lat": 32.93,  "lon": -112.90, "elev_ft":  725, "county": "Maricopa",       "active": True},
    "az20": {"name": "Mohave",                "lat": 34.97,  "lon": -114.61, "elev_ft":  482, "county": "Mohave",         "active": True},
    "az21": {"name": "Laveen",                "lat": 33.38,  "lon": -112.15, "elev_ft": 1332, "county": "Maricopa",       "active": False},
    "az22": {"name": "Queen Creek",           "lat": 33.19,  "lon": -111.53, "elev_ft": 1516, "county": "Maricopa",       "active": True},
    "az23": {"name": "Harquahala",            "lat": 33.49,  "lon": -113.11, "elev_ft": 1143, "county": "Maricopa",       "active": True},
    "az24": {"name": "Roll",                  "lat": 32.76,  "lon": -113.97, "elev_ft":  266, "county": "Yuma",           "active": True},
    "az26": {"name": "Buckeye",               "lat": 33.41,  "lon": -112.68, "elev_ft":  988, "county": "Maricopa",       "active": True},
    "az27": {"name": "Desert Ridge",          "lat": 33.69,  "lon": -111.96, "elev_ft": 1676, "county": "Maricopa",       "active": True},
    "az28": {"name": "Mohave-2",              "lat": 34.88,  "lon": -114.57, "elev_ft":  466, "county": "Mohave",         "active": True},
    "az29": {"name": "Mesa",                  "lat": 33.39,  "lon": -111.87, "elev_ft": 1207, "county": "Maricopa",       "active": False},
    "az30": {"name": "Flagstaff",             "lat": 35.21,  "lon": -111.58, "elev_ft": 6765, "county": "Coconino",       "active": False},
    "az31": {"name": "Prescott",              "lat": 34.59,  "lon": -112.42, "elev_ft": 5197, "county": "Yavapai",        "active": False},
    "az32": {"name": "Payson",                "lat": 34.23,  "lon": -111.35, "elev_ft": 4852, "county": "Gila",           "active": True},
    "az33": {"name": "Bowie",                 "lat": 32.29,  "lon": -109.47, "elev_ft": 3812, "county": "Cochise",        "active": True},
    "az34": {"name": "Kansas Settlement",     "lat": 32.12,  "lon": -109.74, "elev_ft": 4213, "county": "Cochise",        "active": False},
    "az35": {"name": "Parker-2",              "lat": 33.86,  "lon": -114.48, "elev_ft":  302, "county": "La Paz",         "active": True},
    "az36": {"name": "Yuma South",            "lat": 32.57,  "lon": -114.74, "elev_ft":   92, "county": "Yuma",           "active": True},
    "az37": {"name": "San Simon",             "lat": 32.20,  "lon": -109.12, "elev_ft": 3707, "county": "Cochise",        "active": True},
    "az38": {"name": "Sahuarita",             "lat": 31.97,  "lon": -110.95, "elev_ft": 2756, "county": "Pima",           "active": True},
    "az39": {"name": "Willcox Bench",         "lat": 32.12,  "lon": -109.71, "elev_ft": 4285, "county": "Cochise",        "active": True},
    "az40": {"name": "Ft Mohave CA",          "lat": 34.97,  "lon": -114.65, "elev_ft":  482, "county": "San Bernardino", "active": True},
    "az41": {"name": "Salome",                "lat": 33.85,  "lon": -113.54, "elev_ft": 1919, "county": "La Paz",         "active": True},
    "az42": {"name": "Wellton ETo",           "lat": 32.69,  "lon": -114.07, "elev_ft":  240, "county": "Yuma",           "active": True},
    "az43": {"name": "Yuma Valley ETo",       "lat": 32.71,  "lon": -114.70, "elev_ft":  118, "county": "Yuma",           "active": True},
    "az44": {"name": "Mohave ETo",            "lat": 34.90,  "lon": -114.62, "elev_ft":  476, "county": "Mohave",         "active": True},
    "az45": {"name": "Chino Valley",          "lat": 34.785, "lon": -112.417,"elev_ft": 4639, "county": "Yavapai",        "active": True},
    "az47": {"name": "Elgin",                 "lat": 31.66,  "lon": -110.58, "elev_ft": 4898, "county": "Santa Cruz",     "active": True},
}

# AZMet field → (canonical parameter name, unit)
_AZMET_VAR_MAP: dict[str, tuple[str, str]] = {
    "temp_air_meanC":        ("temperature",           "°C"),
    "temp_air_maxC":         ("temperature_max",       "°C"),
    "temp_air_minC":         ("temperature_min",       "°C"),
    "relative_humidity_mean":("relative_humidity",     "%"),
    "relative_humidity_max": ("relative_humidity_max", "%"),
    "relative_humidity_min": ("relative_humidity_min", "%"),
    "wind_speed_mean_mps":   ("wind_speed",            "m/s"),
    "wind_vector_dir":       ("wind_direction",        "deg"),
    "wind_speed_max_mps":    ("wind_gust",             "m/s"),
    "precipitation_total_mm":("precipitation",         "mm"),
    "sol_rad_total":         ("solar_radiation",       "MJ/m²"),
    "sol_rad_mean":          ("solar_radiation",       "W/m²"),
    "eto_asce_mm":           ("evapotranspiration",    "mm"),
    "temp_soil_10cmC":       ("soil_temp_10cm",        "°C"),
    "temp_soil_50cmC":       ("soil_temp_50cm",        "°C"),
    "dwpt":                  ("dewpoint",              "°C"),
    "vp_deficit_mean":       ("vapor_pressure_deficit","kPa"),
}


def _azmet_to_station(raw: dict) -> Station:
    """Convert an AZMet station JSON object to the shared Station model."""
    sid = str(raw.get("meta", {}).get("station_id") or raw.get("station_id") or raw.get("id", ""))
    meta = raw.get("meta", raw)

    # Look up authoritative station metadata (lat/lon/county/etc.) from the
    # hardcoded table sourced from https://azmet.arizona.edu/about/station-metadata
    known = _AZMET_STATION_META.get(sid, {})

    lat  = _safe_float(meta.get("latitude")  or meta.get("lat")) or known.get("lat")
    lon  = _safe_float(meta.get("longitude") or meta.get("lon")) or known.get("lon")
    name = meta.get("name") or meta.get("station_name") or known.get("name") or sid

    # Parse timestamp from observation data (datetime field present in daily/hourly obs)
    last_update = _parse_iso(
        raw.get("datetime") or raw.get("date_time") or raw.get("date")
    )

    return Station(
        id=sid,
        name=name,
        lat=lat,
        lon=lon,
        source="azmet",
        network="AZMet",
        params=["temperature", "relative_humidity", "wind_speed", "precipitation",
                "solar_radiation", "evapotranspiration"],
        elevation=_safe_float(meta.get("elevation")) or _safe_float(known.get("elev_ft")),
        url=f"https://ag.arizona.edu/azmet/",
        last_update=last_update,
        extra={
            "county":       meta.get("county", "") or known.get("county", ""),
            "start_year":   meta.get("start_year", ""),
            "active":       meta.get("active", known.get("active", True)),
        },
    )


def _azmet_obs_to_readings(obs: dict, station_id: str) -> list[Reading]:
    """Extract Reading objects from a single AZMet observation dict."""
    ts_raw = obs.get("datetime") or obs.get("date_time") or obs.get("date")
    ts = _parse_iso(ts_raw) or datetime.now(timezone.utc)

    readings: list[Reading] = []
    for field, (param_name, unit) in _AZMET_VAR_MAP.items():
        raw_val = obs.get(field)
        if raw_val is None:
            continue
        try:
            readings.append(
                Reading(
                    station_id=station_id,
                    timestamp=ts,
                    parameter=param_name,
                    value=float(raw_val),
                    unit=unit,
                    quality=None,
                    source="azmet",
                )
            )
        except (TypeError, ValueError):
            continue
    return readings


@router.get(
    "/azmet/stations",
    summary="List all AZMet (Arizona Meteorological Network) stations",
)
async def azmet_stations():
    """Return the list of active AZMet stations (~40 sites across Arizona).

    AZMet stations are operated by the University of Arizona and focus on
    agricultural weather — temperature, humidity, wind, precipitation, solar
    radiation, and Penman-Monteith ET.
    """
    cache_key = "regional:azmet:stations"
    cached = await cache.get(cache_key)
    if cached is not None:
        return cached

    # The AZMet API has no dedicated /stations endpoint (returns 404).
    # Instead, we fetch today's daily observations which includes one record
    # per active station with meta_station_id and meta_station_name fields,
    # then derive the station list from those records.
    data = await _get_json(f"{_AZMET_BASE}/observations/daily", source_label="AZMet")

    obs_list: list[dict] = data.get("data", []) if isinstance(data, dict) else (data if isinstance(data, list) else [])

    # Build one station record per unique station from the observations
    seen_stations: dict[str, dict] = {}
    for obs in obs_list:
        sid = obs.get("meta_station_id", "")
        if not sid or sid in seen_stations:
            continue
        seen_stations[sid] = {
            "datetime": obs.get("datetime") or obs.get("date_time") or obs.get("date"),
            "meta": {
                "station_id": sid,
                "name": obs.get("meta_station_name", sid),
                "latitude": None,
                "longitude": None,
                "elevation": None,
                "county": "",
                "active": True,
            }
        }

    stations: list[dict] = []
    for raw in seen_stations.values():
        try:
            stations.append(_azmet_to_station(raw).model_dump())
        except Exception:
            logger.debug("Failed to parse AZMet station: %s", raw, exc_info=True)

    result = {
        "source": "azmet",
        "network": "AZMet (University of Arizona)",
        "url": "https://api.azmet.arizona.edu/v1/",
        "count": len(stations),
        "stations": stations,
        "note": "Station list from /observations/daily enriched with metadata from azmet.arizona.edu.",
    }
    await cache.set(cache_key, result, ttl=config.CACHE_TTL_LONG)
    return result


@router.get(
    "/azmet/stations/{station_id}/data",
    summary="AZMet hourly observations for a station",
)
async def azmet_station_data(
    station_id: str,
    days: int = Query(
        3,
        ge=1,
        le=30,
        description="Number of days of hourly data to retrieve (default 3, max 30)",
    ),
):
    """Return hourly AZMet observations for the specified station.

    Data is returned in chronological order.  Parameters include air temperature
    (mean, max, min), relative humidity, wind speed and direction, precipitation,
    solar radiation, ET, and soil temperatures.

    ``station_id`` is the AZMet numeric station identifier
    (visible from GET /api/regional/azmet/stations).
    """
    cache_key = f"regional:azmet:station:{station_id}:days:{days}"
    cached = await cache.get(cache_key)
    if cached is not None:
        return cached

    now      = datetime.now(timezone.utc)
    start_dt = now - timedelta(days=days)
    start    = start_dt.strftime("%Y-%m-%d")
    end      = now.strftime("%Y-%m-%d")

    data = await _get_json(
        f"{_AZMET_BASE}/observations/hourly",
        params={"station": station_id, "start": start, "end": end},
        source_label="AZMet",
    )

    raw_list: list[dict] = data if isinstance(data, list) else data.get("data", [])
    if not raw_list:
        raise HTTPException(
            status_code=404,
            detail=(
                f"No AZMet hourly data found for station '{station_id}' "
                f"for the past {days} day(s).  Check that the station ID is correct."
            ),
        )

    readings: list[Reading] = []
    for obs in raw_list:
        readings.extend(_azmet_obs_to_readings(obs, station_id))
    readings.sort(key=lambda r: r.timestamp)

    # Build a minimal Station from the first observation's metadata
    first = raw_list[0]
    station_meta: Optional[dict] = None
    try:
        station_meta = _azmet_to_station(first).model_dump()
    except Exception:
        pass

    result = {
        "source": "azmet",
        "station_id": station_id,
        "days": days,
        "start": start,
        "end": end,
        "reading_count": len(readings),
        "station": station_meta,
        "readings": [r.model_dump() for r in readings],
    }
    await cache.set(cache_key, result, ttl=config.CACHE_TTL)
    return result


# ===========================================================================
# 4. Colorado DWR CDSS (Surface Water)
# ===========================================================================

# CDSS field → (canonical parameter name, unit)
_CDSS_VAR_MAP: dict[str, tuple[str, str]] = {
    "measValue":          ("streamflow",         "cfs"),
    "measCount":          ("measurement_count",  "count"),
    "dataValue":          ("streamflow",         "cfs"),
    "measDischarge":      ("discharge",          "cfs"),
    "measStage":          ("stage",              "ft"),
    "measStorage":        ("reservoir_storage",  "acre-ft"),
    "measDiversion":      ("diversion",          "cfs"),
}


def _cdss_station_to_model(raw: dict) -> Station:
    """Convert a CDSS surface-water station JSON object to the shared Station model."""
    abbrev = str(raw.get("abbrev", raw.get("stationNum", raw.get("wdid", ""))))
    lat  = _safe_float(raw.get("latitude")  or raw.get("latdecdeg"))
    lon  = _safe_float(raw.get("longitude") or raw.get("longdecdeg"))
    name = raw.get("stationName") or raw.get("gageName") or abbrev

    # Infer parameters from data source type
    data_source = (raw.get("dataSource") or raw.get("measureType") or "").lower()
    params: list[str] = []
    if "stream" in data_source or "gauge" in data_source or "flow" in data_source:
        params = ["streamflow", "stage"]
    elif "diversion" in data_source:
        params = ["diversion"]
    elif "reservoir" in data_source or "storage" in data_source:
        params = ["reservoir_storage"]
    else:
        params = ["streamflow"]

    # Parse last_update from period-of-record end date or measDate
    last_update = None
    for ts_field in ("porEnd", "measDate", "dataMeasDate", "modified"):
        ts_raw = raw.get(ts_field)
        if ts_raw:
            last_update = _parse_iso(str(ts_raw)) or _parse_mdyyyy(str(ts_raw))
            if last_update is not None:
                break

    return Station(
        id=abbrev,
        name=name,
        lat=lat,
        lon=lon,
        source="colorado_dwr",
        network="Colorado DWR CDSS",
        params=params,
        last_update=last_update,
        url=f"https://dwr.state.co.us/rest/get/api/v2/surfacewater/surfacewaterstations"
            f"?format=json&abbrev={abbrev}",
        extra={
            "county":       raw.get("county", ""),
            "water_district": raw.get("waterDistrict", raw.get("wd", "")),
            "water_division": raw.get("division", ""),
            "usgs_station_id": raw.get("usgsStationId", ""),
            "data_source":  raw.get("dataSource", ""),
            "structure_type": raw.get("structType", ""),
            "start_date":   raw.get("porStart", ""),
            "end_date":     raw.get("porEnd", ""),
        },
    )


def _cdss_ts_to_readings(rows: list[dict], abbrev: str) -> list[Reading]:
    """Convert CDSS time-series result rows to Reading objects."""
    readings: list[Reading] = []
    for row in rows:
        ts_raw = row.get("measDate") or row.get("dataMeasDate") or row.get("obsDate")
        ts = _parse_iso(ts_raw) or _parse_mdyyyy(ts_raw)
        if ts is None:
            continue

        # CDSS daily endpoint provides measValue or dataValue
        for field, (param_name, unit) in _CDSS_VAR_MAP.items():
            raw_val = row.get(field)
            if raw_val is None:
                continue
            try:
                readings.append(
                    Reading(
                        station_id=abbrev,
                        timestamp=ts,
                        parameter=param_name,
                        value=float(raw_val),
                        unit=unit,
                        quality=row.get("measType") or row.get("dataType") or None,
                        source="colorado_dwr",
                    )
                )
                break  # only one value field per row
            except (TypeError, ValueError):
                continue
    return readings


@router.get(
    "/colorado/stations",
    summary="List Colorado DWR CDSS surface-water stations",
)
async def colorado_stations(
    county: Optional[str] = Query(
        None,
        description=(
            "Filter by county name (e.g. 'Boulder', 'Larimer').  "
            "When omitted a statewide sample of 200 stations is returned."
        ),
    ),
):
    """Return Colorado DWR CDSS surface-water station metadata.

    The CDSS database contains 6,000+ surface-water monitoring stations covering
    streamflow gauges, irrigation diversions, and reservoir storage.  Provide a
    ``county`` filter to narrow results; without it a default sample is returned.

    Station abbreviations (``abbrev``) are used as the ``{abbrev}`` path parameter
    in the streamflow data endpoint.
    """
    cache_key = f"regional:colorado:stations:{county}"
    cached = await cache.get(cache_key)
    if cached is not None:
        return cached

    params: dict[str, Any] = {"format": "json", "pageSize": 200}
    if county:
        params["county"] = county.title()

    data = await _get_json(
        f"{_CDSS_BASE}/surfacewater/surfacewaterstations",
        params=params,
        source_label="Colorado DWR CDSS",
    )

    raw_list: list[dict] = _cdss_result_list(data)

    stations: list[dict] = []
    for raw in raw_list:
        try:
            stations.append(_cdss_station_to_model(raw).model_dump())
        except Exception:
            logger.debug("Failed to parse CDSS station: %s", raw, exc_info=True)

    result = {
        "source": "colorado_dwr",
        "network": "Colorado DWR CDSS Surface Water",
        "url": "https://dwr.state.co.us/rest/get/api/v2/",
        "county_filter": county,
        "count": len(stations),
        "stations": stations,
        "note": (
            "6,000+ stations statewide.  Use the county filter to narrow results.  "
            "Pass the 'abbrev' (station abbreviation) to "
            "GET /api/regional/colorado/stations/{abbrev}/data for time-series."
        ),
    }
    await cache.set(cache_key, result, ttl=config.CACHE_TTL_LONG)
    return result


@router.get(
    "/colorado/stations/{abbrev}/data",
    summary="Colorado DWR CDSS streamflow time-series for a station",
)
async def colorado_station_data(
    abbrev: str,
    days: int = Query(
        7,
        ge=1,
        le=365,
        description="Number of days of daily streamflow data to retrieve (default 7, max 365)",
    ),
):
    """Return daily streamflow (or diversion / storage) data for a CDSS station.

    ``abbrev`` is the CDSS station abbreviation returned by
    GET /api/regional/colorado/stations.

    Data comes from the CDSS ``surfacewatertsday`` (daily time-series) endpoint
    and is normalised to the shared Reading model with streamflow in CFS.

    The real-time ``surfacewaterts`` endpoint is also queried to provide the
    latest instantaneous observation as the first reading.
    """
    cache_key = f"regional:colorado:station:{abbrev}:days:{days}"
    cached = await cache.get(cache_key)
    if cached is not None:
        return cached

    now      = datetime.now(timezone.utc)
    start_dt = now - timedelta(days=days)
    start    = start_dt.strftime("%m/%d/%Y")
    end      = now.strftime("%m/%d/%Y")

    # Daily time-series
    daily_data = await _get_json(
        f"{_CDSS_BASE}/surfacewater/surfacewatertsday",
        params={
            "format": "json",
            "abbrev": abbrev.upper(),
            "startDate": start,
            "endDate": end,
        },
        source_label="Colorado DWR CDSS",
    )

    raw_rows: list[dict] = _cdss_result_list(daily_data)
    if not raw_rows:
        raise HTTPException(
            status_code=404,
            detail=(
                f"No CDSS data found for station abbreviation '{abbrev}' "
                f"in the past {days} day(s).  Verify the abbreviation using "
                "GET /api/regional/colorado/stations."
            ),
        )

    readings = _cdss_ts_to_readings(raw_rows, abbrev.upper())
    readings.sort(key=lambda r: r.timestamp)

    # Build a minimal station from the first row (CDSS embeds station meta)
    station_meta: Optional[dict] = None
    if raw_rows:
        try:
            station_meta = _cdss_station_to_model(raw_rows[0]).model_dump()
        except Exception:
            pass

    result = {
        "source": "colorado_dwr",
        "abbrev": abbrev.upper(),
        "days": days,
        "start": start,
        "end": end,
        "reading_count": len(readings),
        "station": station_meta,
        "readings": [r.model_dump() for r in readings],
    }
    await cache.set(cache_key, result, ttl=config.CACHE_TTL)
    return result


# ===========================================================================
# 5. TVA (Tennessee Valley Authority) Lake Levels
# ===========================================================================

# TVA JSON field → (canonical parameter name, unit)
# These match the field names in the RestApi/observed-data/{code}.json response.
_TVA_VAR_MAP: dict[str, tuple[str, str]] = {
    "ReservoirElevation":     ("pool_elevation",        "ft MSL"),
    "TailwaterElevation":     ("tailwater_elevation",   "ft MSL"),
    "AverageHourlyDischarge": ("discharge",             "cfs"),
    # Legacy field names from the old lake-levels-data.json feed (kept for
    # forward-compatibility if TVA ever restores the full feed).
    "PoolElevation":          ("pool_elevation",        "ft MSL"),
    "Inflow":                 ("inflow",                "kcfs"),
    "Outflow":                ("outflow",               "kcfs"),
    "NetInflow":              ("net_inflow",            "kcfs"),
    "PoolStorage":            ("reservoir_storage",     "1000 acre-ft"),
    "GatePosition":           ("gate_position",         "code"),
    "SpillwayDischarge":      ("spillway_discharge",    "kcfs"),
    "TurbineDischarge":       ("turbine_discharge",     "kcfs"),
}

# The 7 states served by TVA for context
_TVA_STATES = ["TN", "AL", "GA", "KY", "MS", "NC", "VA"]

# TVA RestApi observed-data base URL (per-reservoir hourly readings)
_TVA_OBSERVED_BASE = "https://www.tva.com/RestApi/observed-data"

# TVA reservoir metadata — LocationID codes confirmed working against the
# RestApi/observed-data/{code}.json endpoint, with name/lat/lon/state/river.
# Location codes sourced from RestApi/precipitation-data-location/.
_TVA_RESERVOIRS: dict[str, dict] = {
    "BRDG1": {"name": "Blue Ridge",          "lat": 34.868, "lon": -84.307, "state": "GA", "river": "Toccoa River"},
    "BOOT1": {"name": "Boone",               "lat": 36.413, "lon": -82.055, "state": "TN", "river": "South Fork Holston River"},
    "CCRA1": {"name": "Cedar Creek",         "lat": 34.544, "lon": -87.621, "state": "AL", "river": "Cedar Creek"},
    "CEHT1": {"name": "Center Hill",         "lat": 36.096, "lon": -85.809, "state": "TN", "river": "Caney Fork River"},
    "CHAN7": {"name": "Chatuge",              "lat": 34.850, "lon": -83.802, "state": "NC", "river": "Hiwassee River"},
    "CHPT1": {"name": "Cheatham",            "lat": 36.317, "lon": -87.218, "state": "TN", "river": "Cumberland River"},
    "CRKT1": {"name": "Cherokee",            "lat": 36.170, "lon": -83.498, "state": "TN", "river": "Holston River"},
    "CKDT1": {"name": "Chickamauga",         "lat": 35.103, "lon": -85.229, "state": "TN", "river": "Tennessee River"},
    "COHT1": {"name": "Cordell Hull",        "lat": 36.295, "lon": -85.951, "state": "TN", "river": "Cumberland River"},
    "DUGT1": {"name": "Douglas",             "lat": 35.963, "lon": -83.545, "state": "TN", "river": "French Broad River"},
    "FONN7": {"name": "Fontana",             "lat": 35.447, "lon": -83.808, "state": "NC", "river": "Little Tennessee River"},
    "FLDT1": {"name": "Fort Loudoun",        "lat": 35.798, "lon": -84.245, "state": "TN", "river": "Tennessee River"},
    "FPHT1": {"name": "Fort Patrick Henry",  "lat": 36.491, "lon": -82.484, "state": "TN", "river": "South Fork Holston River"},
    "GGRT1": {"name": "Great Falls",         "lat": 35.813, "lon": -85.716, "state": "TN", "river": "Caney Fork River"},
    "GVDA1": {"name": "Guntersville",        "lat": 34.427, "lon": -86.279, "state": "AL", "river": "Tennessee River"},
    "HIWN7": {"name": "Hiwassee",            "lat": 35.188, "lon": -84.124, "state": "NC", "river": "Hiwassee River"},
    "JPHT1": {"name": "J. Percy Priest",     "lat": 36.133, "lon": -86.620, "state": "TN", "river": "Stones River"},
    "KYDK2": {"name": "Kentucky",            "lat": 37.009, "lon": -88.274, "state": "KY", "river": "Tennessee River"},
    "MHDT1": {"name": "Melton Hill",         "lat": 35.883, "lon": -84.308, "state": "TN", "river": "Clinch River"},
    "NKJT1": {"name": "Nickajack",           "lat": 35.023, "lon": -85.636, "state": "TN", "river": "Tennessee River"},
    "NRMT1": {"name": "Normandy",            "lat": 35.437, "lon": -86.271, "state": "TN", "river": "Duck River"},
    "NRST1": {"name": "Norris",              "lat": 36.222, "lon": -84.090, "state": "TN", "river": "Clinch River"},
    "NOTG1": {"name": "Nottely",             "lat": 34.914, "lon": -84.069, "state": "GA", "river": "Nottely River"},
    "OHHT1": {"name": "Old Hickory",         "lat": 36.310, "lon": -86.654, "state": "TN", "river": "Cumberland River"},
    "PICT1": {"name": "Pickwick Landing",    "lat": 35.066, "lon": -88.254, "state": "TN", "river": "Tennessee River"},
    "SHDT1": {"name": "South Holston",       "lat": 36.530, "lon": -82.068, "state": "TN", "river": "South Fork Holston River"},
    "TMFT1": {"name": "Tims Ford",           "lat": 35.176, "lon": -86.314, "state": "TN", "river": "Elk River"},
    "WTGT1": {"name": "Watauga",             "lat": 36.333, "lon": -82.101, "state": "TN", "river": "Watauga River"},
    "WBOT1": {"name": "Watts Bar",           "lat": 35.609, "lon": -84.783, "state": "TN", "river": "Tennessee River"},
    "WHLA1": {"name": "Wheeler",             "lat": 34.582, "lon": -87.337, "state": "AL", "river": "Tennessee River"},
    "WLSA1": {"name": "Wilson",              "lat": 34.787, "lon": -87.627, "state": "AL", "river": "Tennessee River"},
}


def _tva_reservoir_to_station(raw: dict) -> Station:
    """Convert a TVA reservoir JSON object to the shared Station model."""
    res_id = str(raw.get("DamName") or raw.get("id") or raw.get("Name", "")).strip()
    # Use DamName as both ID and name; it's the stable identifier in TVA data
    name = raw.get("ReservoirName") or raw.get("damName") or res_id

    lat = _safe_float(raw.get("Latitude")  or raw.get("lat"))
    lon = _safe_float(raw.get("Longitude") or raw.get("lon"))

    params = [
        canonical
        for field, (canonical, _unit) in _TVA_VAR_MAP.items()
        if raw.get(field) is not None
    ]
    if not params:
        params = ["pool_elevation", "tailwater_elevation", "discharge"]

    return Station(
        id=res_id,
        name=name,
        lat=lat,
        lon=lon,
        source="tva",
        network="TVA Lake Levels",
        params=params,
        url="https://www.tva.com/environment/lake-levels",
        extra={
            "river":        raw.get("River", raw.get("river", "")),
            "state":        raw.get("State", raw.get("state", "")),
            "dam_number":   raw.get("DamNumber", ""),
            "normal_max_pool": _safe_float(raw.get("NormalMaxPool")),
            "normal_min_pool": _safe_float(raw.get("NormalMinPool")),
        },
    )


def _tva_obs_to_readings(raw: dict, reservoir_id: str) -> list[Reading]:
    """Extract Reading objects from a TVA reservoir JSON record."""
    # TVA timestamp fields vary; try several
    ts_raw = (
        raw.get("DateTimeStamp")
        or raw.get("DateTime")
        or raw.get("ObservationDate")
        or raw.get("date")
    )
    ts = _parse_iso(ts_raw) or datetime.now(timezone.utc)

    readings: list[Reading] = []
    for field, (param_name, unit) in _TVA_VAR_MAP.items():
        raw_val = raw.get(field)
        if raw_val is None:
            continue
        try:
            # TVA values may be strings with commas like "4,519"
            clean_val = str(raw_val).replace(",", "")
            readings.append(
                Reading(
                    station_id=reservoir_id,
                    timestamp=ts,
                    parameter=param_name,
                    value=float(clean_val),
                    unit=unit,
                    quality=None,
                    source="tva",
                )
            )
        except (TypeError, ValueError):
            continue
    return readings


async def _fetch_tva_reservoir(code: str, meta: dict) -> Optional[dict]:
    """Fetch observed data for a single TVA reservoir and return a merged dict."""
    url = f"{_TVA_OBSERVED_BASE}/{code}.json"
    try:
        async with _client() as client:
            resp = await client.get(url)
            resp.raise_for_status()
            records = resp.json()
    except Exception:
        logger.debug("TVA observed-data fetch failed for %s", code, exc_info=True)
        return None

    if not records or not isinstance(records, list):
        return None

    # Use the most recent observation (last in the list)
    latest = records[-1]

    # Parse timestamp from Day + Time fields (e.g. "02/18/2026" + "3 PM EST")
    ts = datetime.now(timezone.utc)
    day_str = latest.get("Day", "")
    time_str = latest.get("Time", "")
    ts_raw = f"{day_str} {time_str}".strip() if day_str else None
    if ts_raw:
        parsed = _parse_mdyyyy(day_str)
        if parsed:
            ts = parsed

    # Build readings from the latest observation
    readings: list[Reading] = []
    for field, (param_name, unit) in _TVA_VAR_MAP.items():
        raw_val = latest.get(field)
        if raw_val is None:
            continue
        try:
            clean_val = str(raw_val).replace(",", "")
            readings.append(
                Reading(
                    station_id=code,
                    timestamp=ts,
                    parameter=param_name,
                    value=float(clean_val),
                    unit=unit,
                    quality=None,
                    source="tva",
                )
            )
        except (TypeError, ValueError):
            continue

    station = Station(
        id=code,
        name=meta["name"],
        lat=meta.get("lat"),
        lon=meta.get("lon"),
        source="tva",
        network="TVA Lake Levels",
        params=[r.parameter for r in readings] or ["pool_elevation", "tailwater_elevation", "discharge"],
        url=f"https://www.tva.com/environment/lake-levels/{meta['name'].lower().replace(' ', '-').replace('.', '')}",
        last_update=ts,
        extra={
            "river": meta.get("river", ""),
            "state": meta.get("state", ""),
            "observation_count": len(records),
        },
    )

    return {
        **station.model_dump(),
        "latest_readings": [r.model_dump() for r in readings],
    }


@router.get(
    "/tva/reservoirs",
    summary="Current TVA reservoir levels for TVA-managed lakes",
)
async def tva_reservoirs():
    """Return current pool elevation, tailwater elevation, and discharge
    for TVA reservoirs across Tennessee, Alabama, Georgia, Kentucky,
    North Carolina, and Virginia.

    Data is sourced from the TVA public RestApi observed-data endpoint,
    which provides hourly readings for each reservoir.
    """
    cache_key = "regional:tva:reservoirs"
    cached = await cache.get(cache_key)
    if cached is not None:
        return cached

    # Fetch all reservoirs in parallel from TVA's RestApi observed-data endpoint
    tasks = [
        _fetch_tva_reservoir(code, meta)
        for code, meta in _TVA_RESERVOIRS.items()
    ]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    reservoirs_out: list[dict] = []
    for res in results:
        if isinstance(res, dict):
            reservoirs_out.append(res)
        elif isinstance(res, Exception):
            logger.debug("TVA reservoir fetch raised: %s", res)

    reservoirs_out.sort(key=lambda r: r.get("name", ""))

    if not reservoirs_out:
        raise HTTPException(
            status_code=502,
            detail="TVA lake levels: no reservoir data could be retrieved.  "
                   "The TVA RestApi may be temporarily unavailable.  "
                   "Check https://www.tva.com/environment/lake-levels for manual access.",
        )

    result = {
        "source": "tva",
        "network": "Tennessee Valley Authority",
        "states": _TVA_STATES,
        "reservoir_count": len(reservoirs_out),
        "data_url": f"{_TVA_OBSERVED_BASE}/{{code}}.json",
        "reservoirs": reservoirs_out,
    }
    await cache.set(cache_key, result, ttl=config.CACHE_TTL)
    return result


# ===========================================================================
# 6. Oklahoma Mesonet
# ===========================================================================

_OK_MESONET_SITEINFO_URL = (
    "https://www.mesonet.org/index.php/api/siteinfo"
    "/from_all_active_with_geo_fields/format/json/"
)
_OK_MESONET_CURRENT_URL = (
    "https://www.mesonet.org/data/public/mesonet/current/current.csv.txt"
)

_OK_MESONET_PARAMS = [
    "temperature", "dewpoint", "relative_humidity",
    "wind_speed", "wind_direction", "wind_gust",
    "pressure", "precipitation",
]


def _ok_to_station(stid: str, raw: dict) -> Station:
    """Convert an Oklahoma Mesonet siteinfo record to the shared Station model."""
    lat  = _safe_float(raw.get("nlat"))
    lon  = _safe_float(raw.get("elon"))
    name = raw.get("name") or raw.get("city") or stid

    return Station(
        id=stid,
        name=name,
        lat=lat,
        lon=lon,
        source="oklahoma_mesonet",
        network="Oklahoma Mesonet",
        params=_OK_MESONET_PARAMS,
        elevation=_safe_float(raw.get("elev")),
        url=f"https://www.mesonet.org/index.php/weather/station_info/{stid}",
        extra={
            "city":    raw.get("city", ""),
            "county":  raw.get("cnty", ""),
            "climate_division": raw.get("cdiv", ""),
            "class":   raw.get("clas", ""),
            "date_commissioned": raw.get("datc", ""),
        },
    )


@router.get(
    "/oklahoma/stations",
    summary="List all Oklahoma Mesonet stations",
)
async def oklahoma_stations():
    """Return the full list of Oklahoma Mesonet stations (~146 sites).

    Data is sourced from the Oklahoma Mesonet public siteinfo API, which returns
    station ID, name, city, county, lat/lon, elevation, and climate division.
    The Oklahoma Mesonet is operated by the Oklahoma Climatological Survey and
    measures temperature, humidity, wind, pressure, precipitation, and soil
    moisture across the state.
    """
    cache_key = "regional:oklahoma:stations"
    cached = await cache.get(cache_key)
    if cached is not None:
        return cached

    data = await _get_json(
        _OK_MESONET_SITEINFO_URL,
        source_label="Oklahoma Mesonet",
    )

    # Response is a dict keyed by STID: {stid, name, nlat, elon, elev, cnty, ...}
    if not isinstance(data, dict):
        raise HTTPException(status_code=502, detail="Oklahoma Mesonet returned unexpected data format")

    stations: list[dict] = []
    for stid, raw in data.items():
        try:
            stations.append(_ok_to_station(stid, raw).model_dump())
        except Exception:
            logger.debug("Failed to parse Oklahoma station %s: %s", stid, raw, exc_info=True)

    result = {
        "source": "oklahoma_mesonet",
        "network": "Oklahoma Mesonet (Oklahoma Climatological Survey)",
        "url": "https://www.mesonet.org/",
        "count": len(stations),
        "stations": stations,
    }
    await cache.set(cache_key, result, ttl=config.CACHE_TTL_LONG)
    return result


# ===========================================================================
# 7. Florida FAWN (Florida Automated Weather Network)
# ===========================================================================
#
# FAWN is operated by the University of Florida IFAS Extension.  It covers
# ~51 automated weather stations statewide with 15-minute observations.
#
# The real-time JSON feed is at:
#   https://fawn.ifas.ufl.edu/controller.php/latestobsjson/
#
# The response is gzip-compressed and includes lat/lon, temperature,
# humidity, wind, rainfall, solar radiation, soil temperature, and ET.
# No authentication required; the Accept-Encoding: gzip header is needed.
# ---------------------------------------------------------------------------

_FAWN_BASE_URL    = "https://fawn.ifas.ufl.edu"
_FAWN_LATEST_URL  = f"{_FAWN_BASE_URL}/controller.php/latestobsjson/"

# FAWN JSON field → (canonical parameter name, unit)
_FAWN_VAR_MAP: dict[str, tuple[str, str]] = {
    "temp2mF":        ("temperature",       "°F"),
    "temp60cmF":      ("temperature_60cm",  "°F"),
    "temp10mF":       ("temperature_10m",   "°F"),
    "soilTemp10cmF":  ("soil_temperature",  "°F"),
    "relHum2mPct":    ("relative_humidity", "%"),
    "dewPoint2mF":    ("dewpoint",          "°F"),
    "windSpeed10mMph":("wind_speed",        "mph"),
    "windDir10mDeg":  ("wind_direction",    "deg"),
    "rainFall2mInch": ("precipitation",     "in"),
    "totalRad2mWm2":  ("solar_radiation",   "W/m²"),
    "etInch":         ("evapotranspiration","in"),
    "bp2m":           ("pressure",          "hPa"),
}


def _fawn_obs_to_station(obs: dict) -> Station:
    """Convert a FAWN latestobsjson station record to the shared Station model."""
    sid  = str(obs.get("stnID", ""))
    name = obs.get("stnName", sid)
    lat  = _safe_float(obs.get("lat"))
    lon  = _safe_float(obs.get("lng"))
    elev = _safe_float(obs.get("elevation_feet"))

    # Parse dateTimes string: "2/18/2026 5:45 PM EST"
    # FAWN times are US Eastern (EST = UTC-5, EDT = UTC-4).
    last_update: Optional[datetime] = None
    date_str = obs.get("dateTimes", "")
    if date_str:
        from datetime import timedelta as _td
        is_edt = " EDT" in date_str
        offset = _td(hours=-4) if is_edt else _td(hours=-5)
        clean = date_str.replace(" EST", "").replace(" EDT", "").strip()
        for fmt in ("%m/%d/%Y %I:%M %p",):
            try:
                naive = datetime.strptime(clean, fmt)
                last_update = (naive - offset).replace(tzinfo=timezone.utc)
                break
            except ValueError:
                pass

    return Station(
        id=sid,
        name=name,
        lat=lat,
        lon=lon,
        source="fawn",
        network="Florida FAWN",
        params=list(_FAWN_VAR_MAP[k][0] for k in list(_FAWN_VAR_MAP)[:8]),
        elevation=elev,
        url=f"https://fawn.ifas.ufl.edu/data/reports/?rep=200&fawnsite={sid}",
        last_update=last_update,
        extra={
            "state":    "FL",
            "county":   obs.get("county", ""),
            "facility": obs.get("facility", ""),
            "is_fresh": obs.get("isFresh", 0),
        },
    )


def _fawn_obs_to_readings(obs: dict, ts: Optional[datetime]) -> list[Reading]:
    """Convert a FAWN latestobsjson station record to Reading objects."""
    sid = str(obs.get("stnID", ""))
    if ts is None:
        ts = datetime.now(timezone.utc)
    readings: list[Reading] = []
    for field, (param_name, unit) in _FAWN_VAR_MAP.items():
        raw = obs.get(field)
        if raw is None:
            continue
        # FAWN uses 9999 as the missing-value sentinel
        if raw == 9999 or raw == "9999":
            continue
        try:
            readings.append(
                Reading(
                    station_id=sid,
                    timestamp=ts,
                    parameter=param_name,
                    value=float(raw),
                    unit=unit,
                    quality=None,
                    source="fawn",
                )
            )
        except (TypeError, ValueError):
            continue
    return readings


@router.get(
    "/fawn/stations",
    summary="List all Florida FAWN stations with latest observations",
)
async def fawn_stations():
    """Return the latest observations from all FAWN stations.

    FAWN (Florida Automated Weather Network) is operated by the University
    of Florida IFAS Extension.  It covers ~51 automated stations statewide
    with 15-minute weather observations: temperature (2 m, 60 cm, 10 m),
    relative humidity, dewpoint, wind speed/direction, rainfall, solar
    radiation, soil temperature (10 cm), and evapotranspiration.

    Data is sourced from the FAWN real-time JSON feed; no authentication
    required.  Response is cached for the normal cache TTL.
    """
    cache_key = "regional:fawn:stations"
    cached = await cache.get(cache_key)
    if cached is not None:
        return cached

    # FAWN returns gzip-compressed JSON — httpx handles decompression automatically
    try:
        async with httpx.AsyncClient(
            headers={"User-Agent": config.USER_AGENT},
            timeout=config.HTTP_TIMEOUT,
            follow_redirects=True,
        ) as client:
            resp = await client.get(_FAWN_LATEST_URL)
            resp.raise_for_status()
            data = resp.json()
    except httpx.TimeoutException:
        raise HTTPException(status_code=504, detail="FAWN API timeout")
    except httpx.HTTPStatusError as exc:
        raise HTTPException(
            status_code=exc.response.status_code,
            detail=f"FAWN API error: {exc}",
        )
    except Exception as exc:
        logger.exception("Error fetching FAWN latest observations")
        raise HTTPException(status_code=502, detail=str(exc))

    raw_list: list[dict] = data.get("stnsWxData", [])
    fawn_time: str = data.get("fawnTime", "")

    stations: list[dict] = []
    readings_out: list[dict] = []

    for obs in raw_list:
        try:
            station = _fawn_obs_to_station(obs)
            stations.append(station.model_dump())
            readings = _fawn_obs_to_readings(obs, station.last_update)
            readings_out.extend(r.model_dump() for r in readings)
        except Exception:
            logger.debug("Failed to parse FAWN station: %s", obs.get("stnID"), exc_info=True)

    result = {
        "source": "fawn",
        "network": "Florida FAWN (University of Florida IFAS)",
        "url": "https://fawn.ifas.ufl.edu/",
        "fawn_time": fawn_time,
        "count": len(stations),
        "reading_count": len(readings_out),
        "stations": stations,
        "readings": readings_out,
    }
    await cache.set(cache_key, result, ttl=config.CACHE_TTL)
    return result


# ===========================================================================
# 8. Iowa ISU Soil Moisture Network (via IEM GeoJSON)
# ===========================================================================

_IEM_NETWORK_GEOJSON_URL = (
    "https://mesonet.agron.iastate.edu/geojson/network.php?network={network}"
)
_ISUSM_NETWORK = "ISUSM"

_ISUSM_PARAMS = [
    "temperature", "relative_humidity", "wind_speed", "wind_direction",
    "pressure", "precipitation", "solar_radiation",
    "soil_temperature", "soil_moisture",
]


def _iem_feature_to_station(feature: dict, network_label: str, source_id: str) -> Station:
    """Convert an IEM GeoJSON feature to the shared Station model."""
    props  = feature.get("properties", {})
    coords = feature.get("geometry", {}).get("coordinates", [None, None])
    sid    = feature.get("id", "")

    lon = _safe_float(coords[0]) if len(coords) > 0 else None
    lat = _safe_float(coords[1]) if len(coords) > 1 else None

    archive_begin = props.get("archive_begin") or ""
    archive_end   = props.get("archive_end")
    time_domain   = props.get("time_domain", "")
    online        = props.get("online", True)

    return Station(
        id=sid,
        name=props.get("sname") or sid,
        lat=lat,
        lon=lon,
        source=source_id,
        network=network_label,
        params=_ISUSM_PARAMS,
        elevation=_safe_float(props.get("elevation")),
        url=f"https://mesonet.agron.iastate.edu/sites/site.php?station={sid}&network={_ISUSM_NETWORK}",
        extra={
            "state":         props.get("state", ""),
            "county":        props.get("county", ""),
            "wfo":           props.get("wfo", ""),
            "online":        online,
            "archive_begin": archive_begin,
            "archive_end":   archive_end,
            "time_domain":   time_domain,
            "network":       props.get("network", _ISUSM_NETWORK),
        },
    )


@router.get(
    "/iowa/stations",
    summary="List all Iowa State University Soil Moisture Network stations",
)
async def iowa_stations():
    """Return the Iowa State University Soil Moisture (ISUSM) network stations.

    The ISUSM network consists of ~29 research-grade weather and soil moisture
    stations operated by Iowa State University and hosted via the Iowa
    Environmental Mesonet (IEM).  Stations measure temperature, humidity, wind,
    precipitation, solar radiation, and multi-depth soil temperature / moisture.

    Data is sourced from the IEM GeoJSON network endpoint.
    """
    cache_key = "regional:iowa_isusm:stations"
    cached = await cache.get(cache_key)
    if cached is not None:
        return cached

    url  = _IEM_NETWORK_GEOJSON_URL.format(network=_ISUSM_NETWORK)
    data = await _get_json(url, source_label="Iowa ISUSM (IEM)")

    features: list[dict] = data.get("features", []) if isinstance(data, dict) else []

    stations: list[dict] = []
    for feat in features:
        try:
            stations.append(
                _iem_feature_to_station(feat, "Iowa ISUSM", "iowa_isusm").model_dump()
            )
        except Exception:
            logger.debug("Failed to parse Iowa ISUSM station: %s", feat, exc_info=True)

    result = {
        "source": "iowa_isusm",
        "network": "Iowa State Univ Soil Moisture Network (via IEM)",
        "url": "https://mesonet.agron.iastate.edu/",
        "count": len(stations),
        "stations": stations,
    }
    await cache.set(cache_key, result, ttl=config.CACHE_TTL_LONG)
    return result


# ===========================================================================
# 9. California CIMIS (CA Irrigation Management Information System)
# ===========================================================================

_CIMIS_STATIONS_URL = "https://et.water.ca.gov/api/station"

_CIMIS_PARAMS = [
    "temperature", "temperature_max", "temperature_min",
    "relative_humidity", "wind_speed", "solar_radiation",
    "evapotranspiration", "precipitation", "dewpoint",
    "vapor_pressure",
]


def _cimis_parse_decimal_coord(hms_str: str) -> Optional[float]:
    """Extract the decimal coordinate from CIMIS HMS string.

    Example input: ``"36deg20min10N / 36.336222"``
    Returns ``36.336222`` as a float.
    """
    import re as _re
    m = _re.search(r"/\s*([-]?\d+\.\d+)", hms_str or "")
    if m:
        return _safe_float(m.group(1))
    return None


def _cimis_to_station(raw: dict) -> Station:
    """Convert a CIMIS station JSON record to the shared Station model."""
    sid  = str(raw.get("StationNbr", ""))
    name = raw.get("Name", sid)
    lat  = _cimis_parse_decimal_coord(raw.get("HmsLatitude", ""))
    lon  = _cimis_parse_decimal_coord(raw.get("HmsLongitude", ""))

    active = raw.get("IsActive", "True").strip().lower() == "true"

    return Station(
        id=sid,
        name=name,
        lat=lat,
        lon=lon,
        source="cimis",
        network="California CIMIS",
        params=_CIMIS_PARAMS,
        elevation=_safe_float(raw.get("Elevation")),
        url=f"https://et.water.ca.gov/Station/Details/{sid}",
        extra={
            "city":             raw.get("City", ""),
            "county":           raw.get("County", ""),
            "regional_office":  raw.get("RegionalOffice", ""),
            "ground_cover":     raw.get("GroundCover", ""),
            "is_active":        active,
            "is_eto_station":   raw.get("IsEtoStation", "False").lower() == "true",
            "connect_date":     raw.get("ConnectDate", ""),
            "disconnect_date":  raw.get("DisconnectDate", ""),
        },
    )


@router.get(
    "/cimis/stations",
    summary="List all California CIMIS weather stations",
)
async def cimis_stations(
    active_only: bool = True,
):
    """Return California CIMIS (CA Irrigation Management Information System) stations.

    CIMIS is operated by the CA Department of Water Resources and provides
    weather and reference evapotranspiration (ETo) data for irrigation
    management.  The network has ~145 active stations across California
    measuring temperature, humidity, wind, solar radiation, precipitation, and
    daily ETo.

    Pass ``active_only=false`` to include historical / decommissioned stations
    (273 total).
    """
    cache_key = f"regional:cimis:stations:active={active_only}"
    cached = await cache.get(cache_key)
    if cached is not None:
        return cached

    data = await _get_json(_CIMIS_STATIONS_URL, source_label="CIMIS")

    raw_list: list[dict] = (
        data.get("Stations", []) if isinstance(data, dict)
        else (data if isinstance(data, list) else [])
    )

    if active_only:
        raw_list = [s for s in raw_list if s.get("IsActive", "False").strip().lower() == "true"]

    stations: list[dict] = []
    for raw in raw_list:
        try:
            stations.append(_cimis_to_station(raw).model_dump())
        except Exception:
            logger.debug("Failed to parse CIMIS station: %s", raw, exc_info=True)

    result = {
        "source": "cimis",
        "network": "CIMIS (CA Dept. of Water Resources)",
        "url": "https://et.water.ca.gov/",
        "active_only": active_only,
        "count": len(stations),
        "stations": stations,
    }
    await cache.set(cache_key, result, ttl=config.CACHE_TTL_LONG)
    return result


# ===========================================================================
# 10. Generic IEM State Climate Network helper
# ===========================================================================
#
# The Iowa Environmental Mesonet (IEM) publishes GeoJSON station listings for
# hundreds of state-level COOP/ASOS/Climate observer networks via:
#   https://mesonet.agron.iastate.edu/geojson/network.php?network=<NETWORK_ID>
#
# We use this to expose state climate cooperative observer networks for
# Nebraska, Texas, Minnesota, Wisconsin, Missouri, Illinois, North/South
# Dakota, Tennessee, North Carolina, and Georgia — all free, no API key.
# ---------------------------------------------------------------------------

_IEM_STATE_PARAMS = [
    "temperature", "temperature_max", "temperature_min",
    "precipitation", "snow_depth", "relative_humidity",
    "wind_speed", "wind_direction",
]


async def _iem_state_stations(
    iem_network: str,
    network_label: str,
    source_id: str,
    state_abbr: str,
    cache_key: str,
    online_only: bool = False,
) -> dict:
    """Fetch and normalise an IEM state network GeoJSON to Station list."""
    cached = await cache.get(cache_key)
    if cached is not None:
        return cached

    url  = _IEM_NETWORK_GEOJSON_URL.format(network=iem_network)
    data = await _get_json(url, source_label=f"IEM {iem_network}")

    features: list[dict] = data.get("features", []) if isinstance(data, dict) else []
    if online_only:
        features = [f for f in features if f.get("properties", {}).get("online", True)]

    stations: list[dict] = []
    for feat in features:
        props  = feat.get("properties", {})
        coords = feat.get("geometry", {}).get("coordinates", [None, None])
        sid    = feat.get("id", "")
        lon    = _safe_float(coords[0]) if len(coords) > 0 else None
        lat    = _safe_float(coords[1]) if len(coords) > 1 else None
        try:
            station = Station(
                id=sid,
                name=props.get("sname") or sid,
                lat=lat,
                lon=lon,
                source=source_id,
                network=network_label,
                params=_IEM_STATE_PARAMS,
                elevation=_safe_float(props.get("elevation")),
                url=(
                    f"https://mesonet.agron.iastate.edu/sites/site.php"
                    f"?station={sid}&network={iem_network}"
                ),
                extra={
                    "state":         props.get("state", state_abbr),
                    "county":        props.get("county", ""),
                    "wfo":           props.get("wfo", ""),
                    "online":        props.get("online", True),
                    "archive_begin": props.get("archive_begin", ""),
                    "archive_end":   props.get("archive_end"),
                    "time_domain":   props.get("time_domain", ""),
                    "network":       props.get("network", iem_network),
                    "ncdc81":        props.get("ncdc81", ""),
                },
            )
            stations.append(station.model_dump())
        except Exception:
            logger.debug("Failed to parse IEM station %s/%s: %s", iem_network, sid, feat, exc_info=True)

    result = {
        "source": source_id,
        "network": network_label,
        "iem_network": iem_network,
        "url": f"https://mesonet.agron.iastate.edu/geojson/network.php?network={iem_network}",
        "count": len(stations),
        "stations": stations,
    }
    await cache.set(cache_key, result, ttl=config.CACHE_TTL_LONG)
    return result


# ---------------------------------------------------------------------------
# 10a. Nebraska Climate (NECLIMATE — COOP observer network, ~600 sites)
# ---------------------------------------------------------------------------

@router.get(
    "/nebraska/stations",
    summary="List Nebraska COOP climate observer stations (via IEM)",
)
async def nebraska_stations(online_only: bool = False):
    """Return Nebraska cooperative climate observer stations from the IEM
    NECLIMATE network (~609 total, ~141 currently active).

    Stations report daily temperature (max/min), precipitation, and snow depth.
    Use ``online_only=true`` to filter to only currently active stations.
    """
    return await _iem_state_stations(
        iem_network="NECLIMATE",
        network_label="Nebraska Climate Network (IEM/COOP)",
        source_id="nebraska_climate",
        state_abbr="NE",
        cache_key=f"regional:nebraska:stations:online={online_only}",
        online_only=online_only,
    )


# ---------------------------------------------------------------------------
# 10b. Texas Climate (TXCLIMATE — COOP observer network, ~1553 sites)
# ---------------------------------------------------------------------------

@router.get(
    "/texas/stations",
    summary="List Texas COOP climate observer stations (via IEM)",
)
async def texas_stations(online_only: bool = False):
    """Return Texas cooperative climate observer stations from the IEM
    TXCLIMATE network (~1553 total).

    Stations are operated by the National Weather Service COOP program and
    report daily temperature, precipitation, and snow depth across Texas.
    Use ``online_only=true`` to filter to only currently active stations.
    """
    return await _iem_state_stations(
        iem_network="TXCLIMATE",
        network_label="Texas Climate Network (IEM/COOP)",
        source_id="texas_climate",
        state_abbr="TX",
        cache_key=f"regional:texas:stations:online={online_only}",
        online_only=online_only,
    )


# ---------------------------------------------------------------------------
# 10c. Minnesota Climate (MNCLIMATE — COOP observer network, ~500 sites)
# ---------------------------------------------------------------------------

@router.get(
    "/minnesota/stations",
    summary="List Minnesota COOP climate observer stations (via IEM)",
)
async def minnesota_stations(online_only: bool = False):
    """Return Minnesota cooperative climate observer stations from the IEM
    MNCLIMATE network (~506 total).

    Stations report daily temperature, precipitation, and snow depth
    across Minnesota.  Use ``online_only=true`` for active stations only.
    """
    return await _iem_state_stations(
        iem_network="MNCLIMATE",
        network_label="Minnesota Climate Network (IEM/COOP)",
        source_id="minnesota_climate",
        state_abbr="MN",
        cache_key=f"regional:minnesota:stations:online={online_only}",
        online_only=online_only,
    )


# ---------------------------------------------------------------------------
# 10d. Wisconsin Climate (WICLIMATE — COOP observer network, ~1000 sites)
# ---------------------------------------------------------------------------

@router.get(
    "/wisconsin/stations",
    summary="List Wisconsin COOP climate observer stations (via IEM)",
)
async def wisconsin_stations(online_only: bool = False):
    """Return Wisconsin cooperative climate observer stations from the IEM
    WICLIMATE network (~1000+ total).

    Use ``online_only=true`` to filter to currently active stations only.
    """
    return await _iem_state_stations(
        iem_network="WICLIMATE",
        network_label="Wisconsin Climate Network (IEM/COOP)",
        source_id="wisconsin_climate",
        state_abbr="WI",
        cache_key=f"regional:wisconsin:stations:online={online_only}",
        online_only=online_only,
    )


# ---------------------------------------------------------------------------
# 10e. Missouri Climate (MOCLIMATE — COOP observer network, ~598 sites)
# ---------------------------------------------------------------------------

@router.get(
    "/missouri/stations",
    summary="List Missouri COOP climate observer stations (via IEM)",
)
async def missouri_stations(online_only: bool = False):
    """Return Missouri cooperative climate observer stations from the IEM
    MOCLIMATE network (~598 total).

    Use ``online_only=true`` to filter to currently active stations only.
    """
    return await _iem_state_stations(
        iem_network="MOCLIMATE",
        network_label="Missouri Climate Network (IEM/COOP)",
        source_id="missouri_climate",
        state_abbr="MO",
        cache_key=f"regional:missouri:stations:online={online_only}",
        online_only=online_only,
    )


# ---------------------------------------------------------------------------
# 10f. Illinois Climate (ILCLIMATE — COOP observer network, ~567 sites)
# ---------------------------------------------------------------------------

@router.get(
    "/illinois/stations",
    summary="List Illinois COOP climate observer stations (via IEM)",
)
async def illinois_stations(online_only: bool = False):
    """Return Illinois cooperative climate observer stations from the IEM
    ILCLIMATE network (~567 total).

    Use ``online_only=true`` to filter to currently active stations only.
    """
    return await _iem_state_stations(
        iem_network="ILCLIMATE",
        network_label="Illinois Climate Network (IEM/COOP)",
        source_id="illinois_climate",
        state_abbr="IL",
        cache_key=f"regional:illinois:stations:online={online_only}",
        online_only=online_only,
    )


# ---------------------------------------------------------------------------
# 10g. North Dakota Climate (NDCLIMATE — ~338 sites)
# ---------------------------------------------------------------------------

@router.get(
    "/north_dakota/stations",
    summary="List North Dakota COOP climate observer stations (via IEM)",
)
async def north_dakota_stations(online_only: bool = False):
    """Return North Dakota cooperative climate observer stations from the IEM
    NDCLIMATE network (~338 total).

    Use ``online_only=true`` to filter to currently active stations only.
    """
    return await _iem_state_stations(
        iem_network="NDCLIMATE",
        network_label="North Dakota Climate Network (IEM/COOP)",
        source_id="north_dakota_climate",
        state_abbr="ND",
        cache_key=f"regional:north_dakota:stations:online={online_only}",
        online_only=online_only,
    )


# ---------------------------------------------------------------------------
# 10h. South Dakota Climate (SDCLIMATE — ~426 sites)
# ---------------------------------------------------------------------------

@router.get(
    "/south_dakota/stations",
    summary="List South Dakota COOP climate observer stations (via IEM)",
)
async def south_dakota_stations(online_only: bool = False):
    """Return South Dakota cooperative climate observer stations from the IEM
    SDCLIMATE network (~426 total).

    Use ``online_only=true`` to filter to currently active stations only.
    """
    return await _iem_state_stations(
        iem_network="SDCLIMATE",
        network_label="South Dakota Climate Network (IEM/COOP)",
        source_id="south_dakota_climate",
        state_abbr="SD",
        cache_key=f"regional:south_dakota:stations:online={online_only}",
        online_only=online_only,
    )


# ---------------------------------------------------------------------------
# 10i. Tennessee Climate (TNCLIMATE — ~515 sites)
# ---------------------------------------------------------------------------

@router.get(
    "/tennessee/stations",
    summary="List Tennessee COOP climate observer stations (via IEM)",
)
async def tennessee_stations(online_only: bool = False):
    """Return Tennessee cooperative climate observer stations from the IEM
    TNCLIMATE network (~515 total).

    Use ``online_only=true`` to filter to currently active stations only.
    """
    return await _iem_state_stations(
        iem_network="TNCLIMATE",
        network_label="Tennessee Climate Network (IEM/COOP)",
        source_id="tennessee_climate",
        state_abbr="TN",
        cache_key=f"regional:tennessee:stations:online={online_only}",
        online_only=online_only,
    )


# ---------------------------------------------------------------------------
# 10j. North Carolina Climate (NCCLIMATE — ~502 sites)
# ---------------------------------------------------------------------------

@router.get(
    "/north_carolina/stations",
    summary="List North Carolina COOP climate observer stations (via IEM)",
)
async def north_carolina_stations(online_only: bool = False):
    """Return North Carolina cooperative climate observer stations from the IEM
    NCCLIMATE network (~502 total).

    Use ``online_only=true`` to filter to currently active stations only.
    """
    return await _iem_state_stations(
        iem_network="NCCLIMATE",
        network_label="North Carolina Climate Network (IEM/COOP)",
        source_id="north_carolina_climate",
        state_abbr="NC",
        cache_key=f"regional:north_carolina:stations:online={online_only}",
        online_only=online_only,
    )


# ---------------------------------------------------------------------------
# 10k. Georgia Climate (GACLIMATE — ~402 sites)
# ---------------------------------------------------------------------------

@router.get(
    "/georgia/stations",
    summary="List Georgia COOP climate observer stations (via IEM)",
)
async def georgia_stations(online_only: bool = False):
    """Return Georgia cooperative climate observer stations from the IEM
    GACLIMATE network (~402 total).

    Use ``online_only=true`` to filter to currently active stations only.
    """
    return await _iem_state_stations(
        iem_network="GACLIMATE",
        network_label="Georgia Climate Network (IEM/COOP)",
        source_id="georgia_climate",
        state_abbr="GA",
        cache_key=f"regional:georgia:stations:online={online_only}",
        online_only=online_only,
    )


# ---------------------------------------------------------------------------
# 10l. Kansas Climate (KSCLIMATE — ~756 sites)
# ---------------------------------------------------------------------------

@router.get(
    "/kansas_climate/stations",
    summary="List Kansas COOP climate observer stations (via IEM)",
)
async def kansas_climate_stations(online_only: bool = False):
    """Return Kansas cooperative climate observer stations from the IEM
    KSCLIMATE network (~756 total, complementing the KSU Kansas Mesonet).

    The COOP network covers a broader set of historical observer sites vs the
    KSU Mesonet automated stations.  Use ``online_only=true`` for active only.
    """
    return await _iem_state_stations(
        iem_network="KSCLIMATE",
        network_label="Kansas Climate Network (IEM/COOP)",
        source_id="kansas_climate",
        state_abbr="KS",
        cache_key=f"regional:kansas_climate:stations:online={online_only}",
        online_only=online_only,
    )


# ---------------------------------------------------------------------------
# 10m. Wyoming Climate (WYCLIMATE — ~498 sites)
# ---------------------------------------------------------------------------

@router.get(
    "/wyoming/stations",
    summary="List Wyoming COOP climate observer stations (via IEM)",
)
async def wyoming_stations(online_only: bool = False):
    """Return Wyoming cooperative climate observer stations from the IEM
    WYCLIMATE network (~498 total).

    Use ``online_only=true`` to filter to currently active stations only.
    """
    return await _iem_state_stations(
        iem_network="WYCLIMATE",
        network_label="Wyoming Climate Network (IEM/COOP)",
        source_id="wyoming_climate",
        state_abbr="WY",
        cache_key=f"regional:wyoming:stations:online={online_only}",
        online_only=online_only,
    )


# ---------------------------------------------------------------------------
# 10n. Oregon Climate (ORCLIMATE — ~677 sites)
# ---------------------------------------------------------------------------

@router.get(
    "/oregon/stations",
    summary="List Oregon COOP climate observer stations (via IEM)",
)
async def oregon_stations(online_only: bool = False):
    """Return Oregon cooperative climate observer stations from the IEM
    ORCLIMATE network (~677 total).

    Use ``online_only=true`` to filter to currently active stations only.
    """
    return await _iem_state_stations(
        iem_network="ORCLIMATE",
        network_label="Oregon Climate Network (IEM/COOP)",
        source_id="oregon_climate",
        state_abbr="OR",
        cache_key=f"regional:oregon:stations:online={online_only}",
        online_only=online_only,
    )


# ---------------------------------------------------------------------------
# 10o. Washington Climate (WACLIMATE — ~541 sites)
# ---------------------------------------------------------------------------

@router.get(
    "/washington/stations",
    summary="List Washington COOP climate observer stations (via IEM)",
)
async def washington_stations(online_only: bool = False):
    """Return Washington cooperative climate observer stations from the IEM
    WACLIMATE network (~541 total).

    Use ``online_only=true`` to filter to currently active stations only.
    """
    return await _iem_state_stations(
        iem_network="WACLIMATE",
        network_label="Washington Climate Network (IEM/COOP)",
        source_id="washington_climate",
        state_abbr="WA",
        cache_key=f"regional:washington:stations:online={online_only}",
        online_only=online_only,
    )


# ---------------------------------------------------------------------------
# 10p. Idaho Climate (IDCLIMATE — ~422 sites)
# ---------------------------------------------------------------------------

@router.get(
    "/idaho/stations",
    summary="List Idaho COOP climate observer stations (via IEM)",
)
async def idaho_stations(online_only: bool = False):
    """Return Idaho cooperative climate observer stations from the IEM
    IDCLIMATE network (~422 total).

    Use ``online_only=true`` to filter to currently active stations only.
    """
    return await _iem_state_stations(
        iem_network="IDCLIMATE",
        network_label="Idaho Climate Network (IEM/COOP)",
        source_id="idaho_climate",
        state_abbr="ID",
        cache_key=f"regional:idaho:stations:online={online_only}",
        online_only=online_only,
    )


# ---------------------------------------------------------------------------
# 10q. New Mexico Climate (NMCLIMATE — ~695 sites)
# ---------------------------------------------------------------------------

@router.get(
    "/new_mexico/stations",
    summary="List New Mexico COOP climate observer stations (via IEM)",
)
async def new_mexico_stations(online_only: bool = False):
    """Return New Mexico cooperative climate observer stations from the IEM
    NMCLIMATE network (~695 total).

    Use ``online_only=true`` to filter to currently active stations only.
    """
    return await _iem_state_stations(
        iem_network="NMCLIMATE",
        network_label="New Mexico Climate Network (IEM/COOP)",
        source_id="new_mexico_climate",
        state_abbr="NM",
        cache_key=f"regional:new_mexico:stations:online={online_only}",
        online_only=online_only,
    )


# ---------------------------------------------------------------------------
# 10r. Utah Climate (UTCLIMATE — ~498 sites)
# ---------------------------------------------------------------------------

@router.get(
    "/utah/stations",
    summary="List Utah COOP climate observer stations (via IEM)",
)
async def utah_stations(online_only: bool = False):
    """Return Utah cooperative climate observer stations from the IEM
    UTCLIMATE network (~498 total).

    Use ``online_only=true`` to filter to currently active stations only.
    """
    return await _iem_state_stations(
        iem_network="UTCLIMATE",
        network_label="Utah Climate Network (IEM/COOP)",
        source_id="utah_climate",
        state_abbr="UT",
        cache_key=f"regional:utah:stations:online={online_only}",
        online_only=online_only,
    )


# ---------------------------------------------------------------------------
# 10s. Virginia Climate (VACLIMATE — ~445 sites)
# ---------------------------------------------------------------------------

@router.get(
    "/virginia/stations",
    summary="List Virginia COOP climate observer stations (via IEM)",
)
async def virginia_stations(online_only: bool = False):
    """Return Virginia cooperative climate observer stations from the IEM
    VACLIMATE network (~445 total).

    Use ``online_only=true`` to filter to currently active stations only.
    """
    return await _iem_state_stations(
        iem_network="VACLIMATE",
        network_label="Virginia Climate Network (IEM/COOP)",
        source_id="virginia_climate",
        state_abbr="VA",
        cache_key=f"regional:virginia:stations:online={online_only}",
        online_only=online_only,
    )


# ---------------------------------------------------------------------------
# 10t. South Carolina Climate (SCCLIMATE — ~236 sites)
# ---------------------------------------------------------------------------

@router.get(
    "/south_carolina/stations",
    summary="List South Carolina COOP climate observer stations (via IEM)",
)
async def south_carolina_stations(online_only: bool = False):
    """Return South Carolina cooperative climate observer stations from the IEM
    SCCLIMATE network (~236 total).

    Use ``online_only=true`` to filter to currently active stations only.
    """
    return await _iem_state_stations(
        iem_network="SCCLIMATE",
        network_label="South Carolina Climate Network (IEM/COOP)",
        source_id="south_carolina_climate",
        state_abbr="SC",
        cache_key=f"regional:south_carolina:stations:online={online_only}",
        online_only=online_only,
    )


# ---------------------------------------------------------------------------
# 10u. Kentucky Climate (KYCLIMATE — ~469 sites)
# ---------------------------------------------------------------------------

@router.get(
    "/kentucky/stations",
    summary="List Kentucky COOP climate observer stations (via IEM)",
)
async def kentucky_stations(online_only: bool = False):
    """Return Kentucky cooperative climate observer stations from the IEM
    KYCLIMATE network (~469 total).

    Use ``online_only=true`` to filter to currently active stations only.
    """
    return await _iem_state_stations(
        iem_network="KYCLIMATE",
        network_label="Kentucky Climate Network (IEM/COOP)",
        source_id="kentucky_climate",
        state_abbr="KY",
        cache_key=f"regional:kentucky:stations:online={online_only}",
        online_only=online_only,
    )


# ---------------------------------------------------------------------------
# 10v. Alabama Climate (ALCLIMATE — ~369 sites)
# ---------------------------------------------------------------------------

@router.get(
    "/alabama/stations",
    summary="List Alabama COOP climate observer stations (via IEM)",
)
async def alabama_stations(online_only: bool = False):
    """Return Alabama cooperative climate observer stations from the IEM
    ALCLIMATE network (~369 total).

    Use ``online_only=true`` to filter to currently active stations only.
    """
    return await _iem_state_stations(
        iem_network="ALCLIMATE",
        network_label="Alabama Climate Network (IEM/COOP)",
        source_id="alabama_climate",
        state_abbr="AL",
        cache_key=f"regional:alabama:stations:online={online_only}",
        online_only=online_only,
    )


# ---------------------------------------------------------------------------
# 10w. Arkansas Climate (ARCLIMATE — ~381 sites)
# ---------------------------------------------------------------------------

@router.get(
    "/arkansas/stations",
    summary="List Arkansas COOP climate observer stations (via IEM)",
)
async def arkansas_stations(online_only: bool = False):
    """Return Arkansas cooperative climate observer stations from the IEM
    ARCLIMATE network (~381 total).

    Use ``online_only=true`` to filter to currently active stations only.
    """
    return await _iem_state_stations(
        iem_network="ARCLIMATE",
        network_label="Arkansas Climate Network (IEM/COOP)",
        source_id="arkansas_climate",
        state_abbr="AR",
        cache_key=f"regional:arkansas:stations:online={online_only}",
        online_only=online_only,
    )


# ---------------------------------------------------------------------------
# 10x. Florida Climate (FLCLIMATE — ~369 sites)
# Note: complements the FAWN network already implemented
# ---------------------------------------------------------------------------

@router.get(
    "/florida/stations",
    summary="List Florida COOP climate observer stations (via IEM)",
)
async def florida_stations(online_only: bool = False):
    """Return Florida cooperative climate observer stations from the IEM
    FLCLIMATE network (~369 total).

    This is the NWS COOP observer network and complements the Florida FAWN
    automated weather network already available at /api/regional/fawn/stations.
    Use ``online_only=true`` to filter to currently active stations only.
    """
    return await _iem_state_stations(
        iem_network="FLCLIMATE",
        network_label="Florida Climate Network (IEM/COOP)",
        source_id="florida_climate",
        state_abbr="FL",
        cache_key=f"regional:florida:stations:online={online_only}",
        online_only=online_only,
    )


# ---------------------------------------------------------------------------
# 10y. Hawaii Climate (HICLIMATE — ~540 sites)
# ---------------------------------------------------------------------------

@router.get(
    "/hawaii/stations",
    summary="List Hawaii COOP climate observer stations (via IEM)",
)
async def hawaii_stations(online_only: bool = False):
    """Return Hawaii cooperative climate observer stations from the IEM
    HICLIMATE network (~540 total).

    Use ``online_only=true`` to filter to currently active stations only.
    """
    return await _iem_state_stations(
        iem_network="HICLIMATE",
        network_label="Hawaii Climate Network (IEM/COOP)",
        source_id="hawaii_climate",
        state_abbr="HI",
        cache_key=f"regional:hawaii:stations:online={online_only}",
        online_only=online_only,
    )


# ---------------------------------------------------------------------------
# 10z. Alaska Climate (AKCLIMATE — ~670 sites)
# ---------------------------------------------------------------------------

@router.get(
    "/alaska/stations",
    summary="List Alaska COOP climate observer stations (via IEM)",
)
async def alaska_stations(online_only: bool = False):
    """Return Alaska cooperative climate observer stations from the IEM
    AKCLIMATE network (~670 total).

    Use ``online_only=true`` to filter to currently active stations only.
    """
    return await _iem_state_stations(
        iem_network="AKCLIMATE",
        network_label="Alaska Climate Network (IEM/COOP)",
        source_id="alaska_climate",
        state_abbr="AK",
        cache_key=f"regional:alaska:stations:online={online_only}",
        online_only=online_only,
    )


# ---------------------------------------------------------------------------
# 10aa. Louisiana Climate (LACLIMATE — ~478 sites)
# ---------------------------------------------------------------------------

@router.get(
    "/louisiana/stations",
    summary="List Louisiana COOP climate observer stations (via IEM)",
)
async def louisiana_stations(online_only: bool = False):
    """Return Louisiana cooperative climate observer stations from the IEM
    LACLIMATE network (~478 total).

    Use ``online_only=true`` to filter to currently active stations only.
    """
    return await _iem_state_stations(
        iem_network="LACLIMATE",
        network_label="Louisiana Climate Network (IEM/COOP)",
        source_id="louisiana_climate",
        state_abbr="LA",
        cache_key=f"regional:louisiana:stations:online={online_only}",
        online_only=online_only,
    )


# ---------------------------------------------------------------------------
# 10ab. Mississippi Climate (MSCLIMATE — ~350 sites)
# ---------------------------------------------------------------------------

@router.get(
    "/mississippi/stations",
    summary="List Mississippi COOP climate observer stations (via IEM)",
)
async def mississippi_stations(online_only: bool = False):
    """Return Mississippi cooperative climate observer stations from the IEM
    MSCLIMATE network (~350 total).

    Use ``online_only=true`` to filter to currently active stations only.
    """
    return await _iem_state_stations(
        iem_network="MSCLIMATE",
        network_label="Mississippi Climate Network (IEM/COOP)",
        source_id="mississippi_climate",
        state_abbr="MS",
        cache_key=f"regional:mississippi:stations:online={online_only}",
        online_only=online_only,
    )


# ---------------------------------------------------------------------------
# 10ac. Michigan Climate (MICLIMATE — ~632 sites)
# ---------------------------------------------------------------------------

@router.get(
    "/michigan/stations",
    summary="List Michigan COOP climate observer stations (via IEM)",
)
async def michigan_stations(online_only: bool = False):
    """Return Michigan cooperative climate observer stations from the IEM
    MICLIMATE network (~632 total).

    Use ``online_only=true`` to filter to currently active stations only.
    """
    return await _iem_state_stations(
        iem_network="MICLIMATE",
        network_label="Michigan Climate Network (IEM/COOP)",
        source_id="michigan_climate",
        state_abbr="MI",
        cache_key=f"regional:michigan:stations:online={online_only}",
        online_only=online_only,
    )


# ---------------------------------------------------------------------------
# 10ad. Indiana Climate (INCLIMATE — ~409 sites)
# ---------------------------------------------------------------------------

@router.get(
    "/indiana/stations",
    summary="List Indiana COOP climate observer stations (via IEM)",
)
async def indiana_stations(online_only: bool = False):
    """Return Indiana cooperative climate observer stations from the IEM
    INCLIMATE network (~409 total).

    Use ``online_only=true`` to filter to currently active stations only.
    """
    return await _iem_state_stations(
        iem_network="INCLIMATE",
        network_label="Indiana Climate Network (IEM/COOP)",
        source_id="indiana_climate",
        state_abbr="IN",
        cache_key=f"regional:indiana:stations:online={online_only}",
        online_only=online_only,
    )


# ---------------------------------------------------------------------------
# 10ae. Ohio Climate (OHCLIMATE — ~527 sites)
# ---------------------------------------------------------------------------

@router.get(
    "/ohio/stations",
    summary="List Ohio COOP climate observer stations (via IEM)",
)
async def ohio_stations(online_only: bool = False):
    """Return Ohio cooperative climate observer stations from the IEM
    OHCLIMATE network (~527 total).

    Use ``online_only=true`` to filter to currently active stations only.
    """
    return await _iem_state_stations(
        iem_network="OHCLIMATE",
        network_label="Ohio Climate Network (IEM/COOP)",
        source_id="ohio_climate",
        state_abbr="OH",
        cache_key=f"regional:ohio:stations:online={online_only}",
        online_only=online_only,
    )


# ---------------------------------------------------------------------------
# 10af. Pennsylvania Climate (PACLIMATE — ~734 sites)
# ---------------------------------------------------------------------------

@router.get(
    "/pennsylvania/stations",
    summary="List Pennsylvania COOP climate observer stations (via IEM)",
)
async def pennsylvania_stations(online_only: bool = False):
    """Return Pennsylvania cooperative climate observer stations from the IEM
    PACLIMATE network (~734 total).

    Use ``online_only=true`` to filter to currently active stations only.
    """
    return await _iem_state_stations(
        iem_network="PACLIMATE",
        network_label="Pennsylvania Climate Network (IEM/COOP)",
        source_id="pennsylvania_climate",
        state_abbr="PA",
        cache_key=f"regional:pennsylvania:stations:online={online_only}",
        online_only=online_only,
    )


# ---------------------------------------------------------------------------
# 10ag. New York Climate (NYCLIMATE — ~831 sites)
# ---------------------------------------------------------------------------

@router.get(
    "/new_york/stations",
    summary="List New York COOP climate observer stations (via IEM)",
)
async def new_york_stations(online_only: bool = False):
    """Return New York cooperative climate observer stations from the IEM
    NYCLIMATE network (~831 total).

    Use ``online_only=true`` to filter to currently active stations only.
    """
    return await _iem_state_stations(
        iem_network="NYCLIMATE",
        network_label="New York Climate Network (IEM/COOP)",
        source_id="new_york_climate",
        state_abbr="NY",
        cache_key=f"regional:new_york:stations:online={online_only}",
        online_only=online_only,
    )


# ---------------------------------------------------------------------------
# 10ah. New Jersey Climate (NJCLIMATE — ~169 sites)
# ---------------------------------------------------------------------------

@router.get(
    "/new_jersey/stations",
    summary="List New Jersey COOP climate observer stations (via IEM)",
)
async def new_jersey_stations(online_only: bool = False):
    """Return New Jersey cooperative climate observer stations from the IEM
    NJCLIMATE network (~169 total).

    Use ``online_only=true`` to filter to currently active stations only.
    """
    return await _iem_state_stations(
        iem_network="NJCLIMATE",
        network_label="New Jersey Climate Network (IEM/COOP)",
        source_id="new_jersey_climate",
        state_abbr="NJ",
        cache_key=f"regional:new_jersey:stations:online={online_only}",
        online_only=online_only,
    )


# ---------------------------------------------------------------------------
# 10ai. Connecticut Climate (CTCLIMATE — ~121 sites)
# ---------------------------------------------------------------------------

@router.get(
    "/connecticut/stations",
    summary="List Connecticut COOP climate observer stations (via IEM)",
)
async def connecticut_stations(online_only: bool = False):
    """Return Connecticut cooperative climate observer stations from the IEM
    CTCLIMATE network (~121 total).

    Use ``online_only=true`` to filter to currently active stations only.
    """
    return await _iem_state_stations(
        iem_network="CTCLIMATE",
        network_label="Connecticut Climate Network (IEM/COOP)",
        source_id="connecticut_climate",
        state_abbr="CT",
        cache_key=f"regional:connecticut:stations:online={online_only}",
        online_only=online_only,
    )


# ---------------------------------------------------------------------------
# 10aj. Massachusetts Climate (MACLIMATE — ~286 sites)
# ---------------------------------------------------------------------------

@router.get(
    "/massachusetts/stations",
    summary="List Massachusetts COOP climate observer stations (via IEM)",
)
async def massachusetts_stations(online_only: bool = False):
    """Return Massachusetts cooperative climate observer stations from the IEM
    MACLIMATE network (~286 total).

    Use ``online_only=true`` to filter to currently active stations only.
    """
    return await _iem_state_stations(
        iem_network="MACLIMATE",
        network_label="Massachusetts Climate Network (IEM/COOP)",
        source_id="massachusetts_climate",
        state_abbr="MA",
        cache_key=f"regional:massachusetts:stations:online={online_only}",
        online_only=online_only,
    )


# ---------------------------------------------------------------------------
# 10ak. Maine Climate (MECLIMATE — ~249 sites)
# ---------------------------------------------------------------------------

@router.get(
    "/maine/stations",
    summary="List Maine COOP climate observer stations (via IEM)",
)
async def maine_stations(online_only: bool = False):
    """Return Maine cooperative climate observer stations from the IEM
    MECLIMATE network (~249 total).

    Use ``online_only=true`` to filter to currently active stations only.
    """
    return await _iem_state_stations(
        iem_network="MECLIMATE",
        network_label="Maine Climate Network (IEM/COOP)",
        source_id="maine_climate",
        state_abbr="ME",
        cache_key=f"regional:maine:stations:online={online_only}",
        online_only=online_only,
    )


# ---------------------------------------------------------------------------
# 10al. New Hampshire Climate (NHCLIMATE — ~210 sites)
# ---------------------------------------------------------------------------

@router.get(
    "/new_hampshire/stations",
    summary="List New Hampshire COOP climate observer stations (via IEM)",
)
async def new_hampshire_stations(online_only: bool = False):
    """Return New Hampshire cooperative climate observer stations from the IEM
    NHCLIMATE network (~210 total).

    Use ``online_only=true`` to filter to currently active stations only.
    """
    return await _iem_state_stations(
        iem_network="NHCLIMATE",
        network_label="New Hampshire Climate Network (IEM/COOP)",
        source_id="new_hampshire_climate",
        state_abbr="NH",
        cache_key=f"regional:new_hampshire:stations:online={online_only}",
        online_only=online_only,
    )


# ---------------------------------------------------------------------------
# 10am. Vermont Climate (VTCLIMATE — ~166 sites)
# ---------------------------------------------------------------------------

@router.get(
    "/vermont/stations",
    summary="List Vermont COOP climate observer stations (via IEM)",
)
async def vermont_stations(online_only: bool = False):
    """Return Vermont cooperative climate observer stations from the IEM
    VTCLIMATE network (~166 total).

    Use ``online_only=true`` to filter to currently active stations only.
    """
    return await _iem_state_stations(
        iem_network="VTCLIMATE",
        network_label="Vermont Climate Network (IEM/COOP)",
        source_id="vermont_climate",
        state_abbr="VT",
        cache_key=f"regional:vermont:stations:online={online_only}",
        online_only=online_only,
    )


# ---------------------------------------------------------------------------
# 10an. Rhode Island Climate (RICLIMATE — ~37 sites)
# ---------------------------------------------------------------------------

@router.get(
    "/rhode_island/stations",
    summary="List Rhode Island COOP climate observer stations (via IEM)",
)
async def rhode_island_stations(online_only: bool = False):
    """Return Rhode Island cooperative climate observer stations from the IEM
    RICLIMATE network (~37 total).

    Use ``online_only=true`` to filter to currently active stations only.
    """
    return await _iem_state_stations(
        iem_network="RICLIMATE",
        network_label="Rhode Island Climate Network (IEM/COOP)",
        source_id="rhode_island_climate",
        state_abbr="RI",
        cache_key=f"regional:rhode_island:stations:online={online_only}",
        online_only=online_only,
    )


# ---------------------------------------------------------------------------
# 10ao. West Virginia Climate (WVCLIMATE — ~400 sites)
# ---------------------------------------------------------------------------

@router.get(
    "/west_virginia/stations",
    summary="List West Virginia COOP climate observer stations (via IEM)",
)
async def west_virginia_stations(online_only: bool = False):
    """Return West Virginia cooperative climate observer stations from the IEM
    WVCLIMATE network (~400 total).

    Use ``online_only=true`` to filter to currently active stations only.
    """
    return await _iem_state_stations(
        iem_network="WVCLIMATE",
        network_label="West Virginia Climate Network (IEM/COOP)",
        source_id="west_virginia_climate",
        state_abbr="WV",
        cache_key=f"regional:west_virginia:stations:online={online_only}",
        online_only=online_only,
    )


# ---------------------------------------------------------------------------
# 10ap. Delaware Climate (DECLIMATE — ~23 sites)
# ---------------------------------------------------------------------------

@router.get(
    "/delaware/stations",
    summary="List Delaware COOP climate observer stations (via IEM)",
)
async def delaware_stations(online_only: bool = False):
    """Return Delaware cooperative climate observer stations from the IEM
    DECLIMATE network (~23 total).

    Use ``online_only=true`` to filter to currently active stations only.
    """
    return await _iem_state_stations(
        iem_network="DECLIMATE",
        network_label="Delaware Climate Network (IEM/COOP)",
        source_id="delaware_climate",
        state_abbr="DE",
        cache_key=f"regional:delaware:stations:online={online_only}",
        online_only=online_only,
    )


# ---------------------------------------------------------------------------
# 10aq. Maryland Climate (MDCLIMATE — ~243 sites)
# ---------------------------------------------------------------------------

@router.get(
    "/maryland/stations",
    summary="List Maryland COOP climate observer stations (via IEM)",
)
async def maryland_stations(online_only: bool = False):
    """Return Maryland cooperative climate observer stations from the IEM
    MDCLIMATE network (~243 total).

    Use ``online_only=true`` to filter to currently active stations only.
    """
    return await _iem_state_stations(
        iem_network="MDCLIMATE",
        network_label="Maryland Climate Network (IEM/COOP)",
        source_id="maryland_climate",
        state_abbr="MD",
        cache_key=f"regional:maryland:stations:online={online_only}",
        online_only=online_only,
    )


# ---------------------------------------------------------------------------
# 10ar. Iowa Climate (IACLIMATE — ~541 sites)
# Note: Iowa ISUSM is already implemented separately
# ---------------------------------------------------------------------------

@router.get(
    "/iowa/climate/stations",
    summary="List Iowa COOP climate observer stations (via IEM)",
)
async def iowa_climate_stations(online_only: bool = False):
    """Return Iowa cooperative climate observer stations from the IEM
    IACLIMATE network (~541 total).

    This is the broader NWS COOP network, complementing the Iowa State
    University Soil Moisture (ISUSM) network at /api/regional/iowa/stations.
    Use ``online_only=true`` to filter to currently active stations only.
    """
    return await _iem_state_stations(
        iem_network="IACLIMATE",
        network_label="Iowa Climate Network (IEM/COOP)",
        source_id="iowa_climate",
        state_abbr="IA",
        cache_key=f"regional:iowa_climate:stations:online={online_only}",
        online_only=online_only,
    )


# ---------------------------------------------------------------------------
# 10as. Nevada Climate (NVCLIMATE — ~348 sites, 90 online)
# ---------------------------------------------------------------------------

@router.get(
    "/nevada/stations",
    summary="List Nevada COOP climate observer stations (via IEM)",
)
async def nevada_stations(online_only: bool = False):
    """Return Nevada cooperative climate observer stations from the IEM
    NVCLIMATE network (~348 total, ~90 currently active).

    Use ``online_only=true`` to filter to only currently active stations.
    """
    return await _iem_state_stations(
        iem_network="NVCLIMATE",
        network_label="Nevada Climate Network (IEM/COOP)",
        source_id="nevada_climate",
        state_abbr="NV",
        cache_key=f"regional:nevada:stations:online={online_only}",
        online_only=online_only,
    )


# ---------------------------------------------------------------------------
# 10at. Puerto Rico Climate (PRCLIMATE — ~147 sites)
# ---------------------------------------------------------------------------

@router.get(
    "/puerto_rico/stations",
    summary="List Puerto Rico COOP climate observer stations (via IEM)",
)
async def puerto_rico_stations(online_only: bool = False):
    """Return Puerto Rico cooperative climate observer stations from the IEM
    PRCLIMATE network (~147 total, ~12 currently active).

    Use ``online_only=true`` to filter to only currently active stations.
    """
    return await _iem_state_stations(
        iem_network="PRCLIMATE",
        network_label="Puerto Rico Climate Network (IEM/COOP)",
        source_id="puerto_rico_climate",
        state_abbr="PR",
        cache_key=f"regional:puerto_rico:stations:online={online_only}",
        online_only=online_only,
    )


# ---------------------------------------------------------------------------
# 10au. DC Climate (DCCLIMATE — ~6 sites)
# ---------------------------------------------------------------------------

@router.get(
    "/dc/stations",
    summary="List Washington DC COOP climate observer stations (via IEM)",
)
async def dc_stations(online_only: bool = False):
    """Return Washington DC cooperative climate observer stations from the IEM
    DCCLIMATE network (~6 total, ~2 currently active).

    Use ``online_only=true`` to filter to only currently active stations.
    """
    return await _iem_state_stations(
        iem_network="DCCLIMATE",
        network_label="DC Climate Network (IEM/COOP)",
        source_id="dc_climate",
        state_abbr="DC",
        cache_key=f"regional:dc:stations:online={online_only}",
        online_only=online_only,
    )


# ===========================================================================
# 11. USDA SCAN — Soil Climate Analysis Network (via IEM)
# ===========================================================================
#
# The USDA Natural Resources Conservation Service (NRCS) SCAN network is a
# national soil and climate monitoring network with ~208 stations across the
# contiguous US, Alaska, Hawaii, and Puerto Rico.  Stations measure air
# temperature, relative humidity, precipitation, solar radiation, wind speed,
# and multi-depth soil temperature / moisture.
#
# Data is served via the Iowa Environmental Mesonet (IEM) currents endpoint
# at no charge, no API key required.
#
# IEM network code: SCAN
# ---------------------------------------------------------------------------

_SCAN_PARAMS = [
    "temperature", "relative_humidity", "wind_speed", "wind_direction",
    "wind_gust", "precipitation", "solar_radiation",
    "soil_temperature", "soil_moisture",
]


@router.get(
    "/scan/stations",
    summary="List all USDA SCAN soil/climate network stations (via IEM)",
)
async def scan_stations():
    """Return the full list of USDA SCAN (Soil Climate Analysis Network) stations.

    SCAN is operated by the NRCS and consists of ~208 automated stations across
    the US.  Each station measures air temperature, humidity, wind, solar
    radiation, precipitation, and multi-depth soil temperature and moisture.

    Data is sourced from the Iowa Environmental Mesonet (IEM) GeoJSON network
    endpoint — free, no authentication required.
    """
    cache_key = "regional:scan:stations"
    cached = await cache.get(cache_key)
    if cached is not None:
        return cached

    url = _IEM_NETWORK_GEOJSON_URL.format(network="SCAN")
    data = await _get_json(url, source_label="SCAN (IEM)")

    features: list[dict] = data.get("features", []) if isinstance(data, dict) else []

    stations: list[dict] = []
    for feat in features:
        props  = feat.get("properties", {})
        coords = feat.get("geometry", {}).get("coordinates", [None, None])
        sid    = feat.get("id", "")
        lon    = _safe_float(coords[0]) if len(coords) > 0 else None
        lat    = _safe_float(coords[1]) if len(coords) > 1 else None
        try:
            station = Station(
                id=sid,
                name=props.get("sname") or sid,
                lat=lat,
                lon=lon,
                source="scan",
                network="USDA SCAN",
                params=_SCAN_PARAMS,
                elevation=_safe_float(props.get("elevation")),
                url=(
                    f"https://mesonet.agron.iastate.edu/sites/site.php"
                    f"?station={sid}&network=SCAN"
                ),
                extra={
                    "state":         props.get("state", ""),
                    "county":        props.get("county", ""),
                    "wfo":           props.get("wfo", ""),
                    "online":        props.get("online", True),
                    "archive_begin": props.get("archive_begin", ""),
                    "archive_end":   props.get("archive_end"),
                    "time_domain":   props.get("time_domain", ""),
                    "network":       "SCAN",
                    "climate_site":  props.get("climate_site", ""),
                },
            )
            stations.append(station.model_dump())
        except Exception:
            logger.debug("Failed to parse SCAN station %s: %s", sid, feat, exc_info=True)

    result = {
        "source": "scan",
        "network": "USDA SCAN — Soil Climate Analysis Network (via IEM)",
        "url": "https://www.nrcs.usda.gov/wps/portal/wcc/home/dataAccessHelp/webService/",
        "iem_network": "SCAN",
        "count": len(stations),
        "stations": stations,
    }
    await cache.set(cache_key, result, ttl=config.CACHE_TTL_LONG)
    return result


@router.get(
    "/scan/current",
    summary="Current conditions for all USDA SCAN stations (via IEM)",
)
async def scan_current(
    state: Optional[str] = Query(
        None,
        description="Optional 2-letter state code to filter stations (e.g. 'IA', 'TX')",
    ),
):
    """Return the latest observations from the USDA SCAN network via IEM.

    Returns all ~208 SCAN stations with the most recent temperature, humidity,
    wind, precipitation, solar radiation, and soil sensor readings.

    Optionally filter by state using the ``state`` query parameter.
    """
    cache_key = f"regional:scan:current:{state or 'all'}"
    cached = await cache.get(cache_key)
    if cached is not None:
        return cached

    try:
        async with _client() as client:
            resp = await client.get(
                "https://mesonet.agron.iastate.edu/api/1/currents.json",
                params={"network": "SCAN"},
            )
            resp.raise_for_status()
            data = resp.json()
    except httpx.TimeoutException:
        raise HTTPException(status_code=504, detail="IEM/SCAN API timeout")
    except httpx.HTTPStatusError as exc:
        raise HTTPException(
            status_code=exc.response.status_code,
            detail=f"IEM/SCAN API error: {exc}",
        )
    except Exception as exc:
        logger.exception("Error fetching SCAN current conditions")
        raise HTTPException(status_code=502, detail=str(exc))

    raw_obs = data.get("data", [])
    if state:
        state_upper = state.upper()
        raw_obs = [o for o in raw_obs if o.get("state", "").upper() == state_upper]

    stations_out: list[dict] = []
    readings_out: list[dict] = []

    for obs in raw_obs:
        sid = obs.get("station", "")
        lat = _safe_float(obs.get("lat"))
        lon = _safe_float(obs.get("lon"))

        # Parse observation timestamp for last_update
        valid_str = obs.get("utc_valid") or obs.get("valid")
        try:
            obs_ts = datetime.fromisoformat(
                (valid_str or "").replace("Z", "+00:00")
            )
        except (ValueError, AttributeError):
            obs_ts = None

        # Build a lightweight Station object
        try:
            station = Station(
                id=sid,
                name=obs.get("name") or sid,
                lat=lat,
                lon=lon,
                source="scan",
                network="USDA SCAN",
                params=_SCAN_PARAMS,
                elevation=None,
                url=f"https://mesonet.agron.iastate.edu/sites/site.php?station={sid}&network=SCAN",
                last_update=obs_ts,
                extra={
                    "state":   obs.get("state", ""),
                    "county":  obs.get("county", ""),
                    "network": "SCAN",
                },
            )
            stations_out.append(station.model_dump())
        except Exception:
            continue

        # Extract readings using the IEM variable map
        _ISUSM_IEM_VAR_MAP: dict[str, tuple[str, str]] = {
            "tmpf":   ("temperature", "°F"),
            "dwpf":   ("dewpoint", "°F"),
            "relh":   ("relative_humidity", "%"),
            "sknt":   ("wind_speed", "kt"),
            "drct":   ("wind_direction", "deg"),
            "gust":   ("wind_gust", "kt"),
            "srad":   ("solar_radiation", "W/m²"),
            "pday":   ("precipitation_today", "in"),
            "c1smv":  ("soil_moisture_5cm", "m³/m³"),
            "c2smv":  ("soil_moisture_20cm", "m³/m³"),
            "c3smv":  ("soil_moisture_50cm", "m³/m³"),
            "c4smv":  ("soil_moisture_100cm", "m³/m³"),
            "c1tmpf": ("soil_temp_5cm", "°F"),
            "c2tmpf": ("soil_temp_20cm", "°F"),
            "c3tmpf": ("soil_temp_50cm", "°F"),
            "c4tmpf": ("soil_temp_100cm", "°F"),
        }
        ts = obs_ts or datetime.now(timezone.utc)

        for key, (param_name, unit) in _ISUSM_IEM_VAR_MAP.items():
            raw_val = obs.get(key)
            if raw_val is None or raw_val == "M":
                continue
            try:
                readings_out.append(
                    Reading(
                        station_id=sid,
                        timestamp=ts,
                        parameter=param_name,
                        value=float(raw_val),
                        unit=unit,
                        quality=None,
                        source="scan",
                    ).model_dump()
                )
            except (TypeError, ValueError):
                continue

    result = {
        "source": "scan",
        "network": "USDA SCAN — Soil Climate Analysis Network",
        "state_filter": state,
        "station_count": len(stations_out),
        "reading_count": len(readings_out),
        "stations": stations_out,
        "readings": readings_out,
    }
    await cache.set(cache_key, result, ttl=config.CACHE_TTL)
    return result


# ===========================================================================
# 12. NDAWN — North Dakota Agricultural Weather Network
# ===========================================================================
#
# NDAWN is operated by North Dakota State University and covers North Dakota,
# Minnesota (64 stations), and a small portion of Montana (6 stations) — 253
# active stations total.
#
# Station metadata (lat/lon, city, county, state, elevation) is available
# from a clean JSON API at https://ndawn.ndsu.nodak.edu/api/stations/
#
# Real-time sensor data is only served via an HTML table at /current-data.html.
# We expose the station list with links to the NDAWN website for live data;
# no API key required.
# ---------------------------------------------------------------------------

_NDAWN_BASE = "https://ndawn.ndsu.nodak.edu"
_NDAWN_PARAMS = [
    "temperature", "dewpoint", "relative_humidity",
    "wind_speed", "wind_gust", "wind_direction",
    "solar_radiation", "precipitation",
    "soil_temperature",
]


@router.get(
    "/ndawn/stations",
    summary="List all NDAWN (North Dakota Ag Weather Network) stations",
)
async def ndawn_stations(
    state: Optional[str] = Query(
        None,
        description="Filter by 2-letter state code: 'ND', 'MN', or 'MT'",
    ),
):
    """Return the list of active NDAWN stations.

    NDAWN is operated by North Dakota State University.  It covers 253 active
    stations in North Dakota (183), Minnesota (64), and Montana (6).  Each
    station reports temperature, humidity, wind, solar radiation, precipitation,
    and soil temperature at 15-minute intervals.

    Station metadata (lat/lon, elevation, county) is sourced from the NDAWN
    REST API.  Link each station's ``url`` field for live current readings.
    """
    cache_key = f"regional:ndawn:stations:{state or 'all'}"
    cached = await cache.get(cache_key)
    if cached is not None:
        return cached

    # NDAWN returns 406 when Accept: application/json is sent, so use a
    # minimal-headers client that omits the Accept header.
    _ndawn_headers = {"User-Agent": config.USER_AGENT}
    try:
        async with httpx.AsyncClient(
            headers=_ndawn_headers,
            timeout=config.HTTP_TIMEOUT,
            follow_redirects=True,
        ) as client:
            resp = await client.get(f"{_NDAWN_BASE}/api/stations/")
            resp.raise_for_status()
            data = resp.json()
    except httpx.TimeoutException:
        raise HTTPException(status_code=504, detail="NDAWN API timeout")
    except httpx.HTTPStatusError as exc:
        raise HTTPException(
            status_code=exc.response.status_code,
            detail=f"NDAWN API error: {exc}",
        )
    except Exception as exc:
        logger.exception("Error fetching NDAWN station list")
        raise HTTPException(status_code=502, detail=str(exc))

    raw_list = data.get("data", [])
    # Filter to active stations (end_date is null) and optional state filter
    state_upper = state.upper() if state else None
    active = [
        s for s in raw_list
        if s.get("end_date") is None
        and (state_upper is None or s.get("state", "").upper() == state_upper)
    ]

    stations: list[dict] = []
    for raw in active:
        sid = str(raw.get("id", ""))
        city = raw.get("city", "")
        county = raw.get("county", "")
        st = raw.get("state", "")
        try:
            station = Station(
                id=f"ndawn_{sid}",
                name=f"{city}, {county} Co., {st}",
                lat=_safe_float(raw.get("latitude")),
                lon=_safe_float(raw.get("longitude")),
                source="ndawn",
                network="NDAWN",
                params=_NDAWN_PARAMS,
                elevation=_safe_float(raw.get("elevation")),
                url=f"{_NDAWN_BASE}/current/{city.lower().replace(' ', '-')}.html",
                extra={
                    "state":      st,
                    "county":     county,
                    "ndawn_id":   sid,
                    "begin_date": raw.get("begin_date", ""),
                },
            )
            stations.append(station.model_dump())
        except Exception:
            logger.debug("Failed to parse NDAWN station %s: %s", sid, raw, exc_info=True)

    result = {
        "source": "ndawn",
        "network": "NDAWN — North Dakota Agricultural Weather Network (NDSU)",
        "url": _NDAWN_BASE,
        "state_filter": state,
        "count": len(stations),
        "stations": stations,
        "note": (
            "Current sensor readings are available at each station's 'url' field "
            "(NDAWN website).  NDAWN serves real-time data via HTML only; "
            "15-minute interval data."
        ),
    }
    await cache.set(cache_key, result, ttl=config.CACHE_TTL_LONG)
    return result


# ===========================================================================
# 13. IEM State RWIS Networks — Road Weather Information Systems
# ===========================================================================
#
# The Iowa Environmental Mesonet (IEM) aggregates RWIS (Road Weather
# Information System) station data from state DOTs across the US.  These
# stations sit along highways and report pavement temperature, air
# temperature, visibility, wind, precipitation, and road surface conditions.
#
# Each state network is identified by the pattern <STATE>_RWIS in IEM.
# Station GeoJSON is served at:
#   https://mesonet.agron.iastate.edu/geojson/network.php?network=<STATE>_RWIS
#
# Parameters measured at RWIS stations (subset varies by station):
#   air temperature, pavement temperature, subsurface temperature, wind
#   speed/direction, visibility, precipitation intensity, relative humidity,
#   road surface condition, dew point.
#
# All endpoints are unauthenticated; no API key required.
# ---------------------------------------------------------------------------

_RWIS_PARAMS = [
    "temperature", "pavement_temperature", "wind_speed", "wind_direction",
    "wind_gust", "visibility", "precipitation", "relative_humidity",
    "dewpoint", "road_condition", "subsurface_temperature",
]

# Mapping of state abbreviation → (IEM network ID, approx station count)
_RWIS_NETWORKS: dict[str, tuple[str, int]] = {
    "AK": ("AK_RWIS", 125),
    "AR": ("AR_RWIS", 32),
    "AZ": ("AZ_RWIS", 20),
    "CA": ("CA_RWIS", 43),
    "CO": ("CO_RWIS", 190),
    "CT": ("CT_RWIS", 40),
    "DE": ("DE_RWIS", 43),
    "FL": ("FL_RWIS", 31),
    "GA": ("GA_RWIS", 60),
    "IA": ("IA_RWIS", 106),
    "ID": ("ID_RWIS", 140),
    "IL": ("IL_RWIS", 117),
    "IN": ("IN_RWIS", 64),
    "KS": ("KS_RWIS", 55),
    "KY": ("KY_RWIS", 39),
    "MA": ("MA_RWIS", 40),
    "MD": ("MD_RWIS", 155),
    "ME": ("ME_RWIS", 19),
    "MI": ("MI_RWIS", 190),
    "MN": ("MN_RWIS", 171),
    "MO": ("MO_RWIS", 40),
    "MT": ("MT_RWIS", 200),
    "ND": ("ND_RWIS", 48),
    "NE": ("NE_RWIS", 70),
    "NH": ("NH_RWIS", 30),
    "NJ": ("NJ_RWIS", 35),
    "NM": ("NM_RWIS", 12),
    "NV": ("NV_RWIS", 167),
    "NY": ("NY_RWIS", 24),
    "OH": ("OH_RWIS", 177),
    "OR": ("OR_RWIS", 212),
    "PA": ("PA_RWIS", 77),
    "SD": ("SD_RWIS", 187),
    "TN": ("TN_RWIS", 20),
    "UT": ("UT_RWIS", 342),
    "VA": ("VA_RWIS", 104),
    "VT": ("VT_RWIS", 41),
    "WA": ("WA_RWIS", 148),
    "WI": ("WI_RWIS", 83),
    "WV": ("WV_RWIS", 40),
    "WY": ("WY_RWIS", 129),
}


async def _rwis_state_stations(state_abbr: str) -> dict:
    """Fetch IEM RWIS station list for a single state and return normalised result."""
    net_id, approx = _RWIS_NETWORKS[state_abbr]
    cache_key = f"regional:rwis:{state_abbr.lower()}:stations"
    cached = await cache.get(cache_key)
    if cached is not None:
        return cached

    url  = _IEM_NETWORK_GEOJSON_URL.format(network=net_id)
    data = await _get_json(url, source_label=f"IEM {net_id}")

    features: list[dict] = data.get("features", []) if isinstance(data, dict) else []

    stations: list[dict] = []
    for feat in features:
        props  = feat.get("properties", {})
        coords = feat.get("geometry", {}).get("coordinates", [None, None])
        sid    = feat.get("id", "")
        lon    = _safe_float(coords[0]) if len(coords) > 0 else None
        lat    = _safe_float(coords[1]) if len(coords) > 1 else None
        try:
            station = Station(
                id=sid,
                name=props.get("sname") or sid,
                lat=lat,
                lon=lon,
                source=f"rwis_{state_abbr.lower()}",
                network=f"{state_abbr} DOT RWIS",
                params=_RWIS_PARAMS,
                elevation=_safe_float(props.get("elevation")),
                url=(
                    f"https://mesonet.agron.iastate.edu/sites/site.php"
                    f"?station={sid}&network={net_id}"
                ),
                extra={
                    "state":         props.get("state", state_abbr),
                    "county":        props.get("county", ""),
                    "wfo":           props.get("wfo", ""),
                    "online":        props.get("online", True),
                    "archive_begin": props.get("archive_begin", ""),
                    "archive_end":   props.get("archive_end"),
                    "time_domain":   props.get("time_domain", ""),
                    "iem_network":   net_id,
                },
            )
            stations.append(station.model_dump())
        except Exception:
            logger.debug("Failed to parse RWIS station %s/%s: %s", net_id, sid, feat, exc_info=True)

    result = {
        "source": f"rwis_{state_abbr.lower()}",
        "network": f"{state_abbr} DOT Road Weather Information System (RWIS)",
        "iem_network": net_id,
        "url": f"https://mesonet.agron.iastate.edu/geojson/network.php?network={net_id}",
        "count": len(stations),
        "stations": stations,
    }
    await cache.set(cache_key, result, ttl=config.CACHE_TTL_LONG)
    return result


# ---------------------------------------------------------------------------
# 13a. Alaska RWIS (~125 stations)
# ---------------------------------------------------------------------------
@router.get("/ak_rwis/stations", summary="Alaska DOT RWIS stations (via IEM)")
async def ak_rwis_stations():
    """Alaska Department of Transportation Road Weather Information System.
    ~125 stations across the state measuring air temperature, pavement
    temperature, wind, visibility, and road surface conditions.
    Data via IEM GeoJSON; no authentication required."""
    return await _rwis_state_stations("AK")


# ---------------------------------------------------------------------------
# 13b. Arkansas RWIS (~32 stations)
# ---------------------------------------------------------------------------
@router.get("/ar_rwis/stations", summary="Arkansas DOT RWIS stations (via IEM)")
async def ar_rwis_stations():
    """Arkansas Department of Transportation Road Weather Information System.
    ~32 stations measuring air and pavement temperature, wind, and road
    surface conditions.  Data via IEM GeoJSON; no authentication required."""
    return await _rwis_state_stations("AR")


# ---------------------------------------------------------------------------
# 13c. Arizona RWIS (~20 stations)
# ---------------------------------------------------------------------------
@router.get("/az_rwis/stations", summary="Arizona DOT RWIS stations (via IEM)")
async def az_rwis_stations():
    """Arizona Department of Transportation Road Weather Information System.
    ~20 stations at mountain passes and weather-sensitive corridors.
    Data via IEM GeoJSON; no authentication required."""
    return await _rwis_state_stations("AZ")


# ---------------------------------------------------------------------------
# 13d. California RWIS (~43 stations)
# ---------------------------------------------------------------------------
@router.get("/ca_rwis/stations", summary="California DOT RWIS stations (via IEM)")
async def ca_rwis_stations():
    """California Department of Transportation (Caltrans) Road Weather Information System.
    ~43 stations at mountain passes and weather-sensitive routes measuring air
    temperature, pavement temperature, wind, and road surface conditions.
    Data via IEM GeoJSON; no authentication required."""
    return await _rwis_state_stations("CA")


# ---------------------------------------------------------------------------
# 13e. Colorado RWIS (~190 stations)
# ---------------------------------------------------------------------------
@router.get("/co_rwis/stations", summary="Colorado DOT RWIS stations (via IEM)")
async def co_rwis_stations():
    """Colorado Department of Transportation Road Weather Information System.
    ~190 stations covering mountain passes (I-70, US-40, US-160 and others)
    and urban corridors, measuring air temperature, pavement temperature,
    wind, visibility, and precipitation.  Data via IEM GeoJSON."""
    return await _rwis_state_stations("CO")


# ---------------------------------------------------------------------------
# 13f. Connecticut RWIS (~40 stations)
# ---------------------------------------------------------------------------
@router.get("/ct_rwis/stations", summary="Connecticut DOT RWIS stations (via IEM)")
async def ct_rwis_stations():
    """Connecticut Department of Transportation Road Weather Information System.
    ~40 stations statewide measuring air and pavement temperature, wind,
    visibility, and road surface conditions.  Data via IEM GeoJSON."""
    return await _rwis_state_stations("CT")


# ---------------------------------------------------------------------------
# 13g. Delaware RWIS (~43 stations)
# ---------------------------------------------------------------------------
@router.get("/de_rwis/stations", summary="Delaware DOT RWIS stations (via IEM)")
async def de_rwis_stations():
    """Delaware Department of Transportation Road Weather Information System.
    ~43 stations measuring air and pavement temperature, wind, and road
    surface conditions.  Data via IEM GeoJSON; no authentication required."""
    return await _rwis_state_stations("DE")


# ---------------------------------------------------------------------------
# 13h. Florida RWIS (~31 stations)
# ---------------------------------------------------------------------------
@router.get("/fl_rwis/stations", summary="Florida DOT RWIS stations (via IEM)")
async def fl_rwis_stations():
    """Florida Department of Transportation Road Weather Information System.
    ~31 stations measuring air temperature, wind, visibility, and road
    surface conditions.  Data via IEM GeoJSON; no authentication required."""
    return await _rwis_state_stations("FL")


# ---------------------------------------------------------------------------
# 13i. Georgia RWIS (~60 stations)
# ---------------------------------------------------------------------------
@router.get("/ga_rwis/stations", summary="Georgia DOT RWIS stations (via IEM)")
async def ga_rwis_stations():
    """Georgia Department of Transportation Road Weather Information System.
    ~60 stations across the state measuring air and pavement temperature,
    wind, visibility, and road surface conditions.  Data via IEM GeoJSON."""
    return await _rwis_state_stations("GA")


# ---------------------------------------------------------------------------
# 13j. Iowa RWIS (~106 stations)
# ---------------------------------------------------------------------------
@router.get("/ia_rwis/stations", summary="Iowa DOT RWIS stations (via IEM)")
async def ia_rwis_stations():
    """Iowa Department of Transportation Road Weather Information System.
    ~106 stations statewide measuring air and pavement temperature, wind,
    visibility, precipitation, and road surface conditions.
    Data via IEM GeoJSON; no authentication required."""
    return await _rwis_state_stations("IA")


# ---------------------------------------------------------------------------
# 13k. Idaho RWIS (~140 stations)
# ---------------------------------------------------------------------------
@router.get("/id_rwis/stations", summary="Idaho DOT RWIS stations (via IEM)")
async def id_rwis_stations():
    """Idaho Transportation Department Road Weather Information System.
    ~140 stations at mountain passes and key highway corridors measuring
    air and pavement temperature, wind, visibility, and road conditions.
    Data via IEM GeoJSON; no authentication required."""
    return await _rwis_state_stations("ID")


# ---------------------------------------------------------------------------
# 13l. Illinois RWIS (~117 stations)
# ---------------------------------------------------------------------------
@router.get("/il_rwis/stations", summary="Illinois DOT RWIS stations (via IEM)")
async def il_rwis_stations():
    """Illinois Department of Transportation Road Weather Information System.
    ~117 stations across the state measuring air and pavement temperature,
    wind, visibility, precipitation, and road surface conditions.
    Data via IEM GeoJSON; no authentication required."""
    return await _rwis_state_stations("IL")


# ---------------------------------------------------------------------------
# 13m. Indiana RWIS (~64 stations)
# ---------------------------------------------------------------------------
@router.get("/in_rwis/stations", summary="Indiana DOT RWIS stations (via IEM)")
async def in_rwis_stations():
    """Indiana Department of Transportation Road Weather Information System.
    ~64 stations statewide measuring air and pavement temperature, wind,
    visibility, and road surface conditions.  Data via IEM GeoJSON."""
    return await _rwis_state_stations("IN")


# ---------------------------------------------------------------------------
# 13n. Kansas RWIS (~55 stations)
# ---------------------------------------------------------------------------
@router.get("/ks_rwis/stations", summary="Kansas DOT RWIS stations (via IEM)")
async def ks_rwis_stations():
    """Kansas Department of Transportation Road Weather Information System.
    ~55 stations measuring air and pavement temperature, wind, and road
    surface conditions.  Data via IEM GeoJSON; no authentication required."""
    return await _rwis_state_stations("KS")


# ---------------------------------------------------------------------------
# 13o. Kentucky RWIS (~39 stations)
# ---------------------------------------------------------------------------
@router.get("/ky_rwis/stations", summary="Kentucky DOT RWIS stations (via IEM)")
async def ky_rwis_stations():
    """Kentucky Transportation Cabinet Road Weather Information System.
    ~39 stations measuring air and pavement temperature, wind, visibility,
    and road surface conditions.  Data via IEM GeoJSON."""
    return await _rwis_state_stations("KY")


# ---------------------------------------------------------------------------
# 13p. Massachusetts RWIS (~40 stations)
# ---------------------------------------------------------------------------
@router.get("/ma_rwis/stations", summary="Massachusetts DOT RWIS stations (via IEM)")
async def ma_rwis_stations():
    """Massachusetts Department of Transportation Road Weather Information System.
    ~40 stations measuring air and pavement temperature, wind, visibility,
    and road surface conditions.  Data via IEM GeoJSON."""
    return await _rwis_state_stations("MA")


# ---------------------------------------------------------------------------
# 13q. Maryland RWIS (~155 stations)
# ---------------------------------------------------------------------------
@router.get("/md_rwis/stations", summary="Maryland DOT RWIS stations (via IEM)")
async def md_rwis_stations():
    """Maryland State Highway Administration Road Weather Information System.
    ~155 stations statewide measuring air and pavement temperature, wind,
    visibility, precipitation, and road surface conditions.
    Data via IEM GeoJSON; no authentication required."""
    return await _rwis_state_stations("MD")


# ---------------------------------------------------------------------------
# 13r. Maine RWIS (~19 stations)
# ---------------------------------------------------------------------------
@router.get("/me_rwis/stations", summary="Maine DOT RWIS stations (via IEM)")
async def me_rwis_stations():
    """Maine Department of Transportation Road Weather Information System.
    ~19 stations at weather-sensitive locations measuring air and pavement
    temperature, wind, and road surface conditions.  Data via IEM GeoJSON."""
    return await _rwis_state_stations("ME")


# ---------------------------------------------------------------------------
# 13s. Michigan RWIS (~190 stations)
# ---------------------------------------------------------------------------
@router.get("/mi_rwis/stations", summary="Michigan DOT RWIS stations (via IEM)")
async def mi_rwis_stations():
    """Michigan Department of Transportation Road Weather Information System.
    ~190 stations across the state measuring air and pavement temperature,
    wind, visibility, precipitation, and road surface conditions.
    Data via IEM GeoJSON; no authentication required."""
    return await _rwis_state_stations("MI")


# ---------------------------------------------------------------------------
# 13t. Minnesota RWIS (~171 stations)
# ---------------------------------------------------------------------------
@router.get("/mn_rwis/stations", summary="Minnesota DOT RWIS stations (via IEM)")
async def mn_rwis_stations():
    """Minnesota Department of Transportation Road Weather Information System.
    ~171 stations statewide measuring air and pavement temperature, wind,
    visibility, precipitation, and road surface conditions.
    Data via IEM GeoJSON; no authentication required."""
    return await _rwis_state_stations("MN")


# ---------------------------------------------------------------------------
# 13u. Missouri RWIS (~40 stations)
# ---------------------------------------------------------------------------
@router.get("/mo_rwis/stations", summary="Missouri DOT RWIS stations (via IEM)")
async def mo_rwis_stations():
    """Missouri Department of Transportation Road Weather Information System.
    ~40 stations measuring air and pavement temperature, wind, and road
    surface conditions.  Data via IEM GeoJSON; no authentication required."""
    return await _rwis_state_stations("MO")


# ---------------------------------------------------------------------------
# 13v. Montana RWIS (~200 stations)
# ---------------------------------------------------------------------------
@router.get("/mt_rwis/stations", summary="Montana DOT RWIS stations (via IEM)")
async def mt_rwis_stations():
    """Montana Department of Transportation Road Weather Information System.
    ~200 stations at mountain passes and highway corridors (I-90, I-15,
    US-2) measuring air and pavement temperature, wind, visibility, and
    road surface conditions.  Data via IEM GeoJSON."""
    return await _rwis_state_stations("MT")


# ---------------------------------------------------------------------------
# 13w. North Dakota RWIS (~48 stations)
# ---------------------------------------------------------------------------
@router.get("/nd_rwis/stations", summary="North Dakota DOT RWIS stations (via IEM)")
async def nd_rwis_stations():
    """North Dakota Department of Transportation Road Weather Information System.
    ~48 stations measuring air and pavement temperature, wind, visibility,
    and road surface conditions.  Data via IEM GeoJSON."""
    return await _rwis_state_stations("ND")


# ---------------------------------------------------------------------------
# 13x. Nebraska RWIS (~70 stations)
# ---------------------------------------------------------------------------
@router.get("/ne_rwis/stations", summary="Nebraska DOT RWIS stations (via IEM)")
async def ne_rwis_stations():
    """Nebraska Department of Transportation Road Weather Information System.
    ~70 stations measuring air and pavement temperature, wind, visibility,
    and road surface conditions.  Data via IEM GeoJSON."""
    return await _rwis_state_stations("NE")


# ---------------------------------------------------------------------------
# 13y. New Hampshire RWIS (~30 stations)
# ---------------------------------------------------------------------------
@router.get("/nh_rwis/stations", summary="New Hampshire DOT RWIS stations (via IEM)")
async def nh_rwis_stations():
    """New Hampshire Department of Transportation Road Weather Information System.
    ~30 stations at mountain passes and key corridors measuring air and
    pavement temperature, wind, and road surface conditions.
    Data via IEM GeoJSON; no authentication required."""
    return await _rwis_state_stations("NH")


# ---------------------------------------------------------------------------
# 13z. New Jersey RWIS (~35 stations)
# ---------------------------------------------------------------------------
@router.get("/nj_rwis/stations", summary="New Jersey DOT RWIS stations (via IEM)")
async def nj_rwis_stations():
    """New Jersey Department of Transportation Road Weather Information System.
    ~35 stations measuring air and pavement temperature, wind, visibility,
    and road surface conditions.  Data via IEM GeoJSON."""
    return await _rwis_state_stations("NJ")


# ---------------------------------------------------------------------------
# 13aa. New Mexico RWIS (~12 stations)
# ---------------------------------------------------------------------------
@router.get("/nm_rwis/stations", summary="New Mexico DOT RWIS stations (via IEM)")
async def nm_rwis_stations():
    """New Mexico Department of Transportation Road Weather Information System.
    ~12 stations at mountain passes measuring air and pavement temperature,
    wind, and road surface conditions.  Data via IEM GeoJSON."""
    return await _rwis_state_stations("NM")


# ---------------------------------------------------------------------------
# 13ab. Nevada RWIS (~167 stations)
# ---------------------------------------------------------------------------
@router.get("/nv_rwis/stations", summary="Nevada DOT RWIS stations (via IEM)")
async def nv_rwis_stations():
    """Nevada Department of Transportation Road Weather Information System.
    ~167 stations at mountain passes and highway corridors measuring air and
    pavement temperature, wind, visibility, precipitation, and road surface
    conditions.  Data via IEM GeoJSON; no authentication required."""
    return await _rwis_state_stations("NV")


# ---------------------------------------------------------------------------
# 13ac. New York RWIS (~24 stations)
# ---------------------------------------------------------------------------
@router.get("/ny_rwis/stations", summary="New York DOT RWIS stations (via IEM)")
async def ny_rwis_stations():
    """New York Department of Transportation Road Weather Information System.
    ~24 stations measuring air and pavement temperature, wind, visibility,
    and road surface conditions.  Data via IEM GeoJSON."""
    return await _rwis_state_stations("NY")


# ---------------------------------------------------------------------------
# 13ad. Ohio RWIS (~177 stations)
# ---------------------------------------------------------------------------
@router.get("/oh_rwis/stations", summary="Ohio DOT RWIS stations (via IEM)")
async def oh_rwis_stations():
    """Ohio Department of Transportation Road Weather Information System.
    ~177 stations statewide measuring air and pavement temperature, wind,
    visibility, precipitation, and road surface conditions.
    Data via IEM GeoJSON; no authentication required."""
    return await _rwis_state_stations("OH")


# ---------------------------------------------------------------------------
# 13ae. Oregon RWIS (~212 stations)
# ---------------------------------------------------------------------------
@router.get("/or_rwis/stations", summary="Oregon DOT RWIS stations (via IEM)")
async def or_rwis_stations():
    """Oregon Department of Transportation Road Weather Information System.
    ~212 stations at mountain passes and key corridors (US-97, US-20, I-84)
    measuring air and pavement temperature, wind, visibility, precipitation,
    and road surface conditions.  Data via IEM GeoJSON."""
    return await _rwis_state_stations("OR")


# ---------------------------------------------------------------------------
# 13af. Pennsylvania RWIS (~77 stations)
# ---------------------------------------------------------------------------
@router.get("/pa_rwis/stations", summary="Pennsylvania DOT RWIS stations (via IEM)")
async def pa_rwis_stations():
    """Pennsylvania Department of Transportation Road Weather Information System.
    ~77 stations measuring air and pavement temperature, wind, visibility,
    and road surface conditions.  Data via IEM GeoJSON."""
    return await _rwis_state_stations("PA")


# ---------------------------------------------------------------------------
# 13ag. South Dakota RWIS (~187 stations)
# ---------------------------------------------------------------------------
@router.get("/sd_rwis/stations", summary="South Dakota DOT RWIS stations (via IEM)")
async def sd_rwis_stations():
    """South Dakota Department of Transportation Road Weather Information System.
    ~187 stations measuring air and pavement temperature, wind, visibility,
    precipitation, and road surface conditions.  Data via IEM GeoJSON."""
    return await _rwis_state_stations("SD")


# ---------------------------------------------------------------------------
# 13ah. Tennessee RWIS (~20 stations)
# ---------------------------------------------------------------------------
@router.get("/tn_rwis/stations", summary="Tennessee DOT RWIS stations (via IEM)")
async def tn_rwis_stations():
    """Tennessee Department of Transportation Road Weather Information System.
    ~20 stations at mountain passes and weather-sensitive routes measuring
    air and pavement temperature, wind, and road surface conditions.
    Data via IEM GeoJSON; no authentication required."""
    return await _rwis_state_stations("TN")


# ---------------------------------------------------------------------------
# 13ai. Utah RWIS (~342 stations)
# ---------------------------------------------------------------------------
@router.get("/ut_rwis/stations", summary="Utah DOT RWIS stations (via IEM)")
async def ut_rwis_stations():
    """Utah Department of Transportation Road Weather Information System.
    ~342 stations — one of the densest RWIS networks in the US — covering
    mountain passes, interstates, and state routes, measuring air and
    pavement temperature, wind, visibility, precipitation, and road surface
    conditions.  Data via IEM GeoJSON; no authentication required."""
    return await _rwis_state_stations("UT")


# ---------------------------------------------------------------------------
# 13aj. Virginia RWIS (~104 stations)
# ---------------------------------------------------------------------------
@router.get("/va_rwis/stations", summary="Virginia DOT RWIS stations (via IEM)")
async def va_rwis_stations():
    """Virginia Department of Transportation Road Weather Information System.
    ~104 stations statewide measuring air and pavement temperature, wind,
    visibility, precipitation, and road surface conditions.
    Data via IEM GeoJSON; no authentication required."""
    return await _rwis_state_stations("VA")


# ---------------------------------------------------------------------------
# 13ak. Vermont RWIS (~41 stations)
# ---------------------------------------------------------------------------
@router.get("/vt_rwis/stations", summary="Vermont DOT RWIS stations (via IEM)")
async def vt_rwis_stations():
    """Vermont Agency of Transportation Road Weather Information System.
    ~41 stations at mountain passes and key corridors measuring air and
    pavement temperature, wind, visibility, and road surface conditions.
    Data via IEM GeoJSON; no authentication required."""
    return await _rwis_state_stations("VT")


# ---------------------------------------------------------------------------
# 13al. Washington RWIS (~148 stations)
# ---------------------------------------------------------------------------
@router.get("/wa_rwis/stations", summary="Washington DOT RWIS stations (via IEM)")
async def wa_rwis_stations():
    """Washington State Department of Transportation Road Weather Information System.
    ~148 stations at mountain passes (Snoqualmie, Stevens, White, North
    Cascades) and corridors measuring air and pavement temperature, wind,
    visibility, precipitation, and road surface conditions.
    Data via IEM GeoJSON; no authentication required."""
    return await _rwis_state_stations("WA")


# ---------------------------------------------------------------------------
# 13am. Wisconsin RWIS (~83 stations)
# ---------------------------------------------------------------------------
@router.get("/wi_rwis/stations", summary="Wisconsin DOT RWIS stations (via IEM)")
async def wi_rwis_stations():
    """Wisconsin Department of Transportation Road Weather Information System.
    ~83 stations measuring air and pavement temperature, wind, visibility,
    and road surface conditions.  Data via IEM GeoJSON."""
    return await _rwis_state_stations("WI")


# ---------------------------------------------------------------------------
# 13an. West Virginia RWIS (~40 stations)
# ---------------------------------------------------------------------------
@router.get("/wv_rwis/stations", summary="West Virginia DOT RWIS stations (via IEM)")
async def wv_rwis_stations():
    """West Virginia Division of Highways Road Weather Information System.
    ~40 stations at mountain passes and key routes measuring air and
    pavement temperature, wind, visibility, and road surface conditions.
    Data via IEM GeoJSON; no authentication required."""
    return await _rwis_state_stations("WV")


# ---------------------------------------------------------------------------
# 13ao. Wyoming RWIS (~129 stations)
# ---------------------------------------------------------------------------
@router.get("/wy_rwis/stations", summary="Wyoming DOT RWIS stations (via IEM)")
async def wy_rwis_stations():
    """Wyoming Department of Transportation Road Weather Information System.
    ~129 stations at mountain passes (I-80, I-25, US-26) and corridors
    measuring air and pavement temperature, wind, visibility, and road
    surface conditions.  Data via IEM GeoJSON; no authentication required."""
    return await _rwis_state_stations("WY")


# ===========================================================================
# 14. IEM State DCP Networks — RAWS Fire Weather Stations
# ===========================================================================
#
# IMPORTANT FINDING (2026-02-18):
# IEM does NOT maintain separate XX_RAWS network codes; querying e.g.
# "OR_RAWS" returns an empty FeatureCollection (count: 0).  Instead, RAWS
# (Remote Automated Weather Stations) operated by the USDA Forest Service,
# BLM, NPS, and state agencies are included inside each state's DCP (Data
# Collection Platform) network under the code XX_DCP.  Confirmed for CA, OR,
# MT, WA, ID, and other fire-prone states.
#
# This section provides per-state endpoints that fetch the XX_DCP network and
# filter to stations whose names contain "RAWS", making them discoverable
# separately from the broader DCP network.
#
# Station data available via IEM GeoJSON:
#   https://mesonet.agron.iastate.edu/geojson/network.php?network=XX_DCP
#
# Fire-weather parameters reported by RAWS (via WIMS / Synoptic):
#   air temperature, relative humidity, wind speed, wind direction, wind gust,
#   precipitation, solar radiation, fuel moisture (10-hr, 100-hr)
#
# All endpoints are unauthenticated; no API key required.
# ---------------------------------------------------------------------------

_RAWS_DCP_PARAMS = [
    "air_temp", "relative_humidity", "wind_speed", "wind_direction",
    "wind_gust", "precipitation", "solar_radiation",
    "fuel_moisture_10hr", "fuel_moisture_100hr",
]

# States with the most significant RAWS deployments (confirmed via IEM DCP)
_DCP_RAWS_STATES: dict[str, tuple[str, int]] = {
    "CA": ("CA_DCP", 500),   # California — highest fire risk, 500+ DCP, many RAWS
    "OR": ("OR_DCP", 400),   # Oregon — heavy Forest Service presence
    "WA": ("WA_DCP", 400),   # Washington — Cascades, eastern WA fire country
    "MT": ("MT_DCP", 500),   # Montana — major fire state, many RAWS
    "ID": ("ID_DCP", 400),   # Idaho — heavy BLM/Forest Service
    "NV": ("NV_DCP", 300),   # Nevada — BLM public lands
    "AZ": ("AZ_DCP", 300),   # Arizona — Sonoran / ponderosa fire zones
    "NM": ("NM_DCP", 300),   # New Mexico — Rio Grande corridor
    "CO": ("CO_DCP", 400),   # Colorado — Front Range / western slope
    "UT": ("UT_DCP", 300),   # Utah — Great Basin fire zone
    "WY": ("WY_DCP", 300),   # Wyoming — Bighorn / Yellowstone areas
}


async def _raws_dcp_stations(state_abbr: str) -> dict:
    """Fetch the IEM XX_DCP network and return only RAWS-named stations.

    IEM stores RAWS fire weather stations inside the state DCP (Data Collection
    Platform) networks, not in a separate RAWS network.  This helper fetches
    the full DCP GeoJSON and filters to features whose ``sname`` property
    contains the word "RAWS" (case-insensitive).

    Returns a dict matching the standard _iem_state_stations() response shape.
    """
    state_upper = state_abbr.upper()
    iem_network, _ = _DCP_RAWS_STATES.get(state_upper, (f"{state_upper}_DCP", 0))
    cache_key = f"regional:raws_dcp:{state_upper}"

    cached = await cache.get(cache_key)
    if cached is not None:
        return cached

    url = _IEM_NETWORK_GEOJSON_URL.format(network=iem_network)
    data = await _get_json(url, source_label=f"IEM DCP RAWS ({state_upper})")

    features: list[dict] = data.get("features", []) if isinstance(data, dict) else []

    # Filter to only RAWS-named stations
    raws_features = [
        f for f in features
        if "RAWS" in (f.get("properties", {}).get("sname") or "").upper()
    ]

    stations: list[dict] = []
    for feat in raws_features:
        props  = feat.get("properties", {})
        coords = feat.get("geometry", {}).get("coordinates", [None, None])
        sid    = feat.get("id", "")
        lon    = _safe_float(coords[0]) if len(coords) > 0 else None
        lat    = _safe_float(coords[1]) if len(coords) > 1 else None
        try:
            station = Station(
                id=sid,
                name=props.get("sname") or sid,
                lat=lat,
                lon=lon,
                source="raws_dcp",
                network=f"RAWS Fire Weather — {state_upper} (IEM DCP)",
                params=_RAWS_DCP_PARAMS,
                elevation=_safe_float(props.get("elevation")),
                url=(
                    f"https://mesonet.agron.iastate.edu/sites/site.php"
                    f"?station={sid}&network={iem_network}"
                ),
                extra={
                    "state":         state_upper,
                    "county":        props.get("county", ""),
                    "wfo":           props.get("wfo", ""),
                    "online":        props.get("online", True),
                    "archive_begin": props.get("archive_begin", ""),
                    "archive_end":   props.get("archive_end"),
                    "time_domain":   props.get("time_domain", ""),
                    "iem_network":   iem_network,
                    "climate_site":  props.get("climate_site", ""),
                    "raws_data_url": f"https://raws.nifc.gov/",
                    "synoptic_note": (
                        "Real-time RAWS observations via Synoptic (requires token): "
                        f"GET /api/synoptic/stations/latest?network=71&state={state_upper}"
                    ),
                },
            )
            stations.append(station.model_dump())
        except Exception:
            logger.debug(
                "Failed to parse RAWS DCP station %s/%s: %s",
                iem_network, sid, feat, exc_info=True,
            )

    result = {
        "source": "raws_dcp",
        "network": f"RAWS Fire Weather Stations — {state_upper}",
        "iem_network": iem_network,
        "url": f"https://mesonet.agron.iastate.edu/geojson/network.php?network={iem_network}",
        "note": (
            "RAWS stations are embedded in the IEM DCP network for each state. "
            "IEM does not maintain separate XX_RAWS networks (they return empty). "
            "This endpoint filters DCP stations by 'RAWS' in the station name. "
            "For real-time fire weather observations, use Synoptic network=71 "
            "(requires SYNOPTIC_TOKEN) or the NIFC ArcGIS feed at "
            "GET /api/specialty/raws/stations."
        ),
        "count": len(stations),
        "stations": stations,
    }
    await cache.set(cache_key, result, ttl=config.CACHE_TTL_LONG)
    return result


# ---------------------------------------------------------------------------
# 14a. Oregon RAWS Fire Weather (via IEM OR_DCP)
# ---------------------------------------------------------------------------

@router.get(
    "/oregon/raws",
    summary="Oregon RAWS fire weather stations (via IEM OR_DCP)",
)
async def oregon_raws():
    """Return Oregon RAWS (Remote Automated Weather Stations) from IEM.

    RAWS are operated by the USDA Forest Service, BLM, NPS, and Oregon
    Department of Forestry.  They report fire-weather parameters — air
    temperature, relative humidity, wind, precipitation, solar radiation,
    and fuel moisture — at 10-minute intervals.

    Data source: IEM OR_DCP network filtered to stations containing 'RAWS'
    in their name.  For real-time observations, use the Synoptic endpoint
    (GET /api/synoptic/stations/latest?network=71&state=OR) with a
    SYNOPTIC_TOKEN, or the NIFC ArcGIS feed at
    GET /api/specialty/raws/stations?state=OR.
    """
    return await _raws_dcp_stations("OR")


# ---------------------------------------------------------------------------
# 14b. California RAWS Fire Weather (via IEM CA_DCP)
# ---------------------------------------------------------------------------

@router.get(
    "/california/raws",
    summary="California RAWS fire weather stations (via IEM CA_DCP)",
)
async def california_raws():
    """Return California RAWS fire weather stations from IEM.

    California has one of the largest RAWS deployments in the US, managed by
    CAL FIRE, USDA Forest Service, BLM, and NPS.  Stations cover the Sierra
    Nevada, coastal ranges, and high desert.

    Data source: IEM CA_DCP network filtered to stations containing 'RAWS'
    in their name.  Station locations and metadata only; for real-time
    observations use Synoptic (network=71) or NIFC ArcGIS feed.
    """
    return await _raws_dcp_stations("CA")


# ---------------------------------------------------------------------------
# 14c. Montana RAWS Fire Weather (via IEM MT_DCP)
# ---------------------------------------------------------------------------

@router.get(
    "/montana/raws",
    summary="Montana RAWS fire weather stations (via IEM MT_DCP)",
)
async def montana_raws():
    """Return Montana RAWS fire weather stations from IEM.

    Montana experiences some of the largest wildland fires in the US.  RAWS
    are operated by the US Forest Service (Northern Region), BLM Montana/Dakotas
    State Office, and NPS.

    Data source: IEM MT_DCP network filtered to stations containing 'RAWS'
    in their name.
    """
    return await _raws_dcp_stations("MT")


# ---------------------------------------------------------------------------
# 14d. Washington RAWS Fire Weather (via IEM WA_DCP)
# ---------------------------------------------------------------------------

@router.get(
    "/washington/raws",
    summary="Washington RAWS fire weather stations (via IEM WA_DCP)",
)
async def washington_raws():
    """Return Washington State RAWS fire weather stations from IEM.

    Covers the Cascades, eastern Washington shrub-steppe, and Olympic
    Peninsula.  Operated by USDA Forest Service (Pacific Northwest Region),
    BLM, and WA DNR.

    Data source: IEM WA_DCP network filtered to stations containing 'RAWS'
    in their name.
    """
    return await _raws_dcp_stations("WA")


# ---------------------------------------------------------------------------
# 14e. Idaho RAWS Fire Weather (via IEM ID_DCP)
# ---------------------------------------------------------------------------

@router.get(
    "/idaho/raws",
    summary="Idaho RAWS fire weather stations (via IEM ID_DCP)",
)
async def idaho_raws():
    """Return Idaho RAWS fire weather stations from IEM.

    Idaho is home to some of the most remote fire terrain in the lower 48.
    Stations operated by USFS (Intermountain and Northern Regions), BLM, and
    Idaho Department of Lands.

    Data source: IEM ID_DCP network filtered to stations containing 'RAWS'
    in their name.
    """
    return await _raws_dcp_stations("ID")


# ---------------------------------------------------------------------------
# 14f. Nevada RAWS Fire Weather (via IEM NV_DCP)
# ---------------------------------------------------------------------------

@router.get(
    "/nevada/raws",
    summary="Nevada RAWS fire weather stations (via IEM NV_DCP)",
)
async def nevada_raws():
    """Return Nevada RAWS fire weather stations from IEM.

    Nevada is dominated by BLM public lands with significant Great Basin fire
    activity.  RAWS cover desert valleys, mountain ranges, and the Great Basin.

    Data source: IEM NV_DCP network filtered to stations containing 'RAWS'
    in their name.
    """
    return await _raws_dcp_stations("NV")


# ---------------------------------------------------------------------------
# 14g. Arizona RAWS Fire Weather (via IEM AZ_DCP)
# ---------------------------------------------------------------------------

@router.get(
    "/arizona/raws",
    summary="Arizona RAWS fire weather stations (via IEM AZ_DCP)",
)
async def arizona_raws():
    """Return Arizona RAWS fire weather stations from IEM.

    Arizona experiences severe fire seasons in the Sonoran Desert, Mogollon
    Rim, and ponderosa pine forests.  RAWS operated by USFS (Southwestern
    Region), BLM, NPS, and Arizona State Land Department.

    Data source: IEM AZ_DCP network filtered to stations containing 'RAWS'
    in their name.
    """
    return await _raws_dcp_stations("AZ")


# ---------------------------------------------------------------------------
# 14h. Colorado RAWS Fire Weather (via IEM CO_DCP)
# ---------------------------------------------------------------------------

@router.get(
    "/colorado/raws",
    summary="Colorado RAWS fire weather stations (via IEM CO_DCP)",
)
async def colorado_raws():
    """Return Colorado RAWS fire weather stations from IEM.

    Colorado's Front Range and western slope experience significant wildfire
    risk.  RAWS operated by USFS (Rocky Mountain Region), BLM Colorado,
    and Colorado State Forest Service.

    Data source: IEM CO_DCP network filtered to stations containing 'RAWS'
    in their name.
    """
    return await _raws_dcp_stations("CO")


# ---------------------------------------------------------------------------
# 14i. New Mexico RAWS Fire Weather (via IEM NM_DCP)
# ---------------------------------------------------------------------------

@router.get(
    "/new_mexico/raws",
    summary="New Mexico RAWS fire weather stations (via IEM NM_DCP)",
)
async def new_mexico_raws():
    """Return New Mexico RAWS fire weather stations from IEM.

    New Mexico has experienced several of the largest fires in US history
    (e.g., Hermits Peak/Calf Canyon 2022).  RAWS operated by USFS
    (Southwestern Region) and BLM New Mexico State Office.

    Data source: IEM NM_DCP network filtered to stations containing 'RAWS'
    in their name.
    """
    return await _raws_dcp_stations("NM")


# ---------------------------------------------------------------------------
# 14j. Utah RAWS Fire Weather (via IEM UT_DCP)
# ---------------------------------------------------------------------------

@router.get(
    "/utah/raws",
    summary="Utah RAWS fire weather stations (via IEM UT_DCP)",
)
async def utah_raws():
    """Return Utah RAWS fire weather stations from IEM.

    Utah's Great Basin, canyon country, and Wasatch Front all see significant
    fire activity.  RAWS operated by USFS (Intermountain Region), BLM Utah,
    and Utah Division of Forestry, Fire and State Lands.

    Data source: IEM UT_DCP network filtered to stations containing 'RAWS'
    in their name.
    """
    return await _raws_dcp_stations("UT")


# ===========================================================================
# Overview endpoint
# ===========================================================================

@router.get(
    "/overview",
    summary="Overview of all Regional Networks integrations",
)
async def regional_overview():
    """Return a summary of all five regional network integrations, their
    coverage areas, endpoints, and parameter sets.

    Useful for discovering what data is available without hitting each
    upstream API.
    """
    networks = [
        {
            "id": "montana_mesonet",
            "name": "Montana Mesonet",
            "operator": "University of Montana, Montana Climate Office",
            "coverage": "Montana statewide",
            "station_count_approx": 216,
            "auth_required": False,
            "update_frequency": "15 min – 1 hr",
            "params": list(_MONTANA_VAR_MAP[k][0] for k in _MONTANA_VAR_MAP),
            "endpoints": [
                "GET /api/regional/montana/stations",
                "GET /api/regional/montana/stations/{station_id}",
            ],
            "api_base": _MONTANA_BASE,
            "docs": "https://mesonet.climate.umt.edu/api/v2/docs",
        },
        {
            "id": "kansas_mesonet",
            "name": "Kansas Mesonet",
            "operator": "Kansas State University",
            "coverage": "Kansas statewide",
            "station_count_approx": 100,
            "auth_required": False,
            "update_frequency": "5 min",
            "params": list(_KANSAS_VAR_MAP[k][0] for k in _KANSAS_VAR_MAP),
            "endpoints": [
                "GET /api/regional/kansas/stations",
                "GET /api/regional/kansas/current",
            ],
            "api_base": _KANSAS_BASE,
            "docs": "https://mesonet.k-state.edu/",
        },
        {
            "id": "azmet",
            "name": "AZMet — Arizona Meteorological Network",
            "operator": "University of Arizona, CALS",
            "coverage": "Arizona statewide (agricultural zones)",
            "station_count_approx": 40,
            "auth_required": False,
            "update_frequency": "1 hr (hourly), 1 day (daily)",
            "params": list(_AZMET_VAR_MAP[k][0] for k in _AZMET_VAR_MAP),
            "endpoints": [
                "GET /api/regional/azmet/stations",
                "GET /api/regional/azmet/stations/{station_id}/data?days=3",
            ],
            "api_base": _AZMET_BASE,
            "docs": "https://api.azmet.arizona.edu/v1/docs",
        },
        {
            "id": "colorado_dwr",
            "name": "Colorado DWR CDSS — Surface Water",
            "operator": "Colorado Division of Water Resources",
            "coverage": "Colorado statewide",
            "station_count_approx": 6000,
            "auth_required": False,
            "update_frequency": "15 min (real-time), daily (archive)",
            "params": list(_CDSS_VAR_MAP[k][0] for k in _CDSS_VAR_MAP),
            "endpoints": [
                "GET /api/regional/colorado/stations?county=Boulder",
                "GET /api/regional/colorado/stations/{abbrev}/data?days=7",
            ],
            "api_base": _CDSS_BASE,
            "docs": "https://dwr.state.co.us/rest/get/help",
            "notes": (
                "Use the county query parameter to filter the 6,000+ station list.  "
                "The 'abbrev' field from /stations is the ID for /data."
            ),
        },
        {
            "id": "tva",
            "name": "TVA Lake Levels",
            "operator": "Tennessee Valley Authority",
            "coverage": "TN, AL, GA, KY, MS, NC, VA",
            "reservoir_count": len(_TVA_RESERVOIRS),
            "auth_required": False,
            "update_frequency": "Hourly (per-reservoir observed data)",
            "params": list(dict.fromkeys(v[0] for v in _TVA_VAR_MAP.values())),
            "endpoints": [
                "GET /api/regional/tva/reservoirs",
            ],
            "data_url": f"{_TVA_OBSERVED_BASE}/{{code}}.json",
            "docs": "https://www.tva.com/environment/lake-levels",
        },
    ]

    return {
        "module": "regional",
        "source_count": len(networks),
        "networks": networks,
        "variable_maps": {
            "montana_mesonet": {k: {"parameter": v[0], "unit": v[1]} for k, v in _MONTANA_VAR_MAP.items()},
            "kansas_mesonet":  {k: {"parameter": v[0], "unit": v[1]} for k, v in _KANSAS_VAR_MAP.items()},
            "azmet":           {k: {"parameter": v[0], "unit": v[1]} for k, v in _AZMET_VAR_MAP.items()},
            "colorado_dwr":    {k: {"parameter": v[0], "unit": v[1]} for k, v in _CDSS_VAR_MAP.items()},
            "tva":             {k: {"parameter": v[0], "unit": v[1]} for k, v in _TVA_VAR_MAP.items()},
        },
    }


# ===========================================================================
# Shared utility functions
# ===========================================================================

def _safe_float(val: Any) -> Optional[float]:
    """Convert val to float, returning None on failure."""
    if val is None:
        return None
    try:
        return float(val)
    except (TypeError, ValueError):
        return None


def _parse_iso(raw: Optional[str]) -> Optional[datetime]:
    """Parse an ISO-8601 string to an aware UTC datetime; return None on failure."""
    if not raw:
        return None
    try:
        return datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
    except (ValueError, AttributeError):
        pass
    # Try common date-only format (YYYY-MM-DD)
    try:
        return datetime.strptime(str(raw)[:10], "%Y-%m-%d").replace(tzinfo=timezone.utc)
    except ValueError:
        return None


def _parse_mdyyyy(raw: Optional[str]) -> Optional[datetime]:
    """Parse an MM/DD/YYYY date string to an aware UTC datetime; return None on failure."""
    if not raw:
        return None
    for fmt in ("%m/%d/%Y", "%m/%d/%Y %H:%M:%S", "%m/%d/%Y %H:%M"):
        try:
            return datetime.strptime(str(raw).strip(), fmt).replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    return None


def _latest_per_param(readings: list[Reading]) -> list[Reading]:
    """Return only the most-recent reading per (station_id, parameter) pair."""
    best: dict[tuple[str, str], Reading] = {}
    for r in readings:
        key = (r.station_id, r.parameter)
        if key not in best or r.timestamp > best[key].timestamp:
            best[key] = r
    return list(best.values())


def _cdss_result_list(data: Any) -> list[dict]:
    """Extract the result list from a CDSS API response envelope."""
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        # CDSS wraps results in {"ResultList": [...]} or {"result": [...]}
        return (
            data.get("ResultList")
            or data.get("result")
            or data.get("data")
            or data.get("stations")
            or []
        )
    return []


async def _gather_two(coro_a: Any, coro_b: Any):
    """Await two coroutines concurrently; re-raise the first exception encountered."""
    results = await asyncio.gather(coro_a, coro_b, return_exceptions=True)
    for r in results:
        if isinstance(r, Exception):
            raise r
    return results
