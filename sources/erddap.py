"""ERDDAP Universal Client — one integration for 53+ data servers worldwide.

ERDDAP (Environmental Research Division Data Access Program) is an open-source
data server developed by NOAA that provides a consistent REST interface to
oceanographic and environmental datasets.  Every ERDDAP server, regardless of
the institution operating it, exposes the same URL patterns:

  {base}/erddap/info/index.json           — list all datasets
  {base}/erddap/search/index.json         — full-text dataset search
  {base}/erddap/info/{id}/index.json      — single dataset metadata
  {base}/erddap/tabledap/{id}.json        — query tabular data
  {base}/erddap/griddap/{id}.json         — query gridded data

No authentication is required on any of the servers listed here.  The 25
servers in ERDDAP_SERVERS are the highest-priority nodes; the same module
transparently handles all 60+ servers in the global ERDDAP federation.

ERDDAP documentation: https://coastwatch.pfeg.noaa.gov/erddap/information.html
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
from models import Reading, Station

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Source registry metadata  (read by server.py /api/sources)
# ---------------------------------------------------------------------------

SOURCE_NAME = "ERDDAP Network"
SOURCE_DESCRIPTION = (
    "Universal ERDDAP client querying 25+ oceanographic and environmental data "
    "servers worldwide. All use identical REST patterns with no authentication."
)
SOURCE_META = {
    "url": "https://coastwatch.pfeg.noaa.gov/erddap/",
    "auth_required": False,
    "update_frequency": "varies by dataset (minutes to daily)",
    "coverage": "Global (focus on US coasts, Great Lakes, Pacific Islands)",
    "params": ["water_temperature", "salinity", "wave_height", "currents", "wind", "air_temperature"],
    "category": "water",
}

# ---------------------------------------------------------------------------
# Configured ERDDAP servers
# ---------------------------------------------------------------------------

ERDDAP_SERVERS: dict[str, str] = {
    "coastwatch":        "https://coastwatch.pfeg.noaa.gov",
    "osmc":              "https://osmc.noaa.gov",
    "ncei":              "https://www.ncei.noaa.gov",
    "ioos_sensors":      "https://erddap.sensors.ioos.us",
    "ioos_gliders":      "https://gliders.ioos.us",
    "pmel":              "https://data.pmel.noaa.gov/pmel",
    "glerl":             "https://apps.glerl.noaa.gov",
    "aoos":              "https://erddap.aoos.org",
    "pacioos":           "https://pae-paha.pacioos.hawaii.edu",
    "cencoos":           "https://erddap.axiomdatascience.com",
    "neracoos":          "https://www.neracoos.org",
    "secoora":           "https://erddap.secoora.org",
    "gcoos":             "https://erddap.gcoos.org",
    "nanoos":            "https://data.nanoos.org",
    "sccoos":            "https://erddap.sccoos.org",
    "caricoos":          "https://erddap.caricoos.org",
    "cdip":              "https://thredds.cdip.ucsd.edu",
    "bco_dmo":           "https://erddap.bco-dmo.org",
    "marine_institute_ie": "https://erddap.marine.ie",
    "smartatlantic":     "https://www.smartatlantic.ca",
    "gdp_drifters":      "https://erddap.aoml.noaa.gov/gdp",
    "emodnet_physics":   "https://erddap.emodnet-physics.eu",
    "uhslc":             "https://uhslc.soest.hawaii.edu",
    "samos":             "https://samos.coaps.fsu.edu",
    "neracoos_data":     "https://data.neracoos.org",
    "glos_seagull":      "https://seagull-erddap.glos.org",
}

# Human-readable descriptions for each server
_SERVER_DESCRIPTIONS: dict[str, str] = {
    "coastwatch":          "NOAA CoastWatch West Coast Node — satellite and in-situ data",
    "osmc":                "NOAA Observing System Monitoring Center — global surface marine obs",
    "ncei":                "NOAA National Centers for Environmental Information — climate archive",
    "ioos_sensors":        "IOOS Sensor Theme — integrated coastal sensor network",
    "ioos_gliders":        "IOOS Glider DAC — ocean glider mission data",
    "pmel":                "NOAA Pacific Marine Environmental Laboratory — Pacific/Arctic research",
    "glerl":               "NOAA CoastWatch Great Lakes — Great Lakes satellite and buoy data",
    "aoos":                "Alaska Ocean Observing System",
    "pacioos":             "Pacific Islands Ocean Observing System",
    "cencoos":             "Central & Northern California Ocean Observing System (Axiom)",
    "neracoos":            "Northeastern Regional Association of Coastal Ocean Observing Systems",
    "secoora":             "Southeast Coastal Ocean Observing Regional Association",
    "gcoos":               "Gulf Coast Ocean Observing System",
    "nanoos":              "Northwest Association of Networked Ocean Observing Systems",
    "sccoos":              "Southern California Coastal Ocean Observing System",
    "caricoos":            "Caribbean Coastal Ocean Observing System",
    "cdip":                "Coastal Data Information Program — wave buoy network (Scripps/USACE)",
    "bco_dmo":             "Biological and Chemical Oceanography Data Management Office",
    "marine_institute_ie": "Marine Institute Ireland — Irish coastal and ocean observations",
    "smartatlantic":       "SmartAtlantic Alliance — eastern Canadian coastal observations",
    "gdp_drifters":        "Global Drifter Program (AOML) — 1,500+ surface drifters, SST, currents",
    "emodnet_physics":     "EMODnet Physics — European marine observations (T, S, currents, waves)",
    "uhslc":               "University of Hawaii Sea Level Center — ~500 global tide gauges, hourly sea level",
    "samos":               "SAMOS — ~30 research vessel automated meteorological observations",
    "neracoos_data":       "NERACOOS Data Portal — Narragansett Bay, Gulf of Maine, NE coastal obs",
    "glos_seagull":        "GLOS Seagull — Great Lakes Observing System real-time and historical data",
}

# ---------------------------------------------------------------------------
# ERDDAP variable → canonical parameter name and unit (best-effort mapping)
# ---------------------------------------------------------------------------

_VAR_PARAM_MAP: dict[str, tuple[str, str]] = {
    "sea_surface_temperature":       ("sea_surface_temperature",       "°C"),
    "sea_water_temperature":         ("water_temperature",             "°C"),
    "water_temperature":             ("water_temperature",             "°C"),
    "temperature":                   ("temperature",                   "°C"),
    "air_temperature":               ("air_temperature",               "°C"),
    "sea_surface_salinity":          ("salinity",                      "PSU"),
    "sea_water_salinity":            ("salinity",                      "PSU"),
    "salinity":                      ("salinity",                      "PSU"),
    "significant_wave_height":       ("wave_height",                   "m"),
    "wave_height":                   ("wave_height",                   "m"),
    "sea_surface_wave_significant_height": ("wave_height",             "m"),
    "wind_speed":                    ("wind_speed",                    "m/s"),
    "wind_from_direction":           ("wind_direction",                "°"),
    "wind_direction":                ("wind_direction",                "°"),
    "eastward_sea_water_velocity":   ("current_u",                     "m/s"),
    "northward_sea_water_velocity":  ("current_v",                     "m/s"),
    "sea_water_speed":               ("current_speed",                 "m/s"),
    "sea_level_pressure":            ("pressure",                      "hPa"),
    "air_pressure_at_sea_level":     ("pressure",                      "hPa"),
    "relative_humidity":             ("relative_humidity",             "%"),
    "chlorophyll":                   ("chlorophyll",                   "mg/m³"),
    "chlorophyll_concentration":     ("chlorophyll",                   "mg/m³"),
    "dissolved_oxygen":              ("dissolved_oxygen",              "mg/L"),
    "oxygen":                        ("dissolved_oxygen",              "mg/L"),
    "turbidity":                     ("turbidity",                     "NTU"),
    "conductivity":                  ("conductivity",                  "S/m"),
    "depth":                         ("depth",                         "m"),
    "sea_floor_depth_below_sea_surface": ("depth",                    "m"),
    "precipitation":                 ("precipitation",                 "mm"),
}

# ---------------------------------------------------------------------------
# Router
# ---------------------------------------------------------------------------

router = APIRouter(prefix="/api/erddap", tags=["ERDDAP Network"])

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


def _erddap_base(server_id: str) -> str:
    """Return the validated ERDDAP base path for a server ID.

    Raises HTTPException(404) if the server ID is not recognised.
    """
    base = ERDDAP_SERVERS.get(server_id)
    if base is None:
        raise HTTPException(
            status_code=404,
            detail=(
                f"Unknown ERDDAP server '{server_id}'. "
                f"Valid IDs: {', '.join(sorted(ERDDAP_SERVERS))}."
            ),
        )
    return f"{base}/erddap"


async def _get_json(
    url: str,
    params: Optional[dict[str, Any]] = None,
    label: str = "ERDDAP",
    timeout_override: Optional[float] = None,
) -> Any:
    """Perform a GET request and return parsed JSON.

    Raises HTTPException on network or HTTP errors.
    """
    timeout = timeout_override if timeout_override is not None else config.HTTP_TIMEOUT
    try:
        async with httpx.AsyncClient(
            headers=_HTTP_HEADERS,
            timeout=timeout,
            follow_redirects=True,
        ) as client:
            resp = await client.get(url, params=params or {})
            resp.raise_for_status()
            return resp.json()
    except httpx.TimeoutException:
        raise HTTPException(status_code=504, detail=f"{label} API timeout ({url})")
    except httpx.HTTPStatusError as exc:
        raise HTTPException(
            status_code=exc.response.status_code,
            detail=f"{label} HTTP {exc.response.status_code}: {url}",
        )
    except Exception as exc:
        logger.exception("Unexpected error fetching %s", url)
        raise HTTPException(status_code=502, detail=f"Error fetching {label}: {exc}")


async def _probe_server(server_id: str, base_url: str) -> dict[str, Any]:
    """Check whether an ERDDAP server is reachable.

    Returns a dict with online status, dataset count (if available), and any
    error message.  Never raises — errors are captured into the return value.
    """
    url = f"{base_url}/erddap/info/index.json"
    try:
        async with httpx.AsyncClient(
            headers=_HTTP_HEADERS,
            timeout=10.0,
            follow_redirects=True,
        ) as client:
            resp = await client.get(url, params={"page": 1, "itemsPerPage": 1})
            resp.raise_for_status()
            data = resp.json()
            # ERDDAP info index returns table with rows
            rows = data.get("table", {}).get("rows", [])
            return {
                "server_id": server_id,
                "base_url": base_url,
                "erddap_url": f"{base_url}/erddap/",
                "description": _SERVER_DESCRIPTIONS.get(server_id, ""),
                "online": True,
                "dataset_count_sample": len(rows),
                "error": None,
            }
    except httpx.TimeoutException:
        return {
            "server_id": server_id,
            "base_url": base_url,
            "erddap_url": f"{base_url}/erddap/",
            "description": _SERVER_DESCRIPTIONS.get(server_id, ""),
            "online": False,
            "dataset_count_sample": None,
            "error": "timeout",
        }
    except Exception as exc:
        return {
            "server_id": server_id,
            "base_url": base_url,
            "erddap_url": f"{base_url}/erddap/",
            "description": _SERVER_DESCRIPTIONS.get(server_id, ""),
            "online": False,
            "dataset_count_sample": None,
            "error": str(exc),
        }

# ---------------------------------------------------------------------------
# ERDDAP response parsing helpers
# ---------------------------------------------------------------------------


def _parse_erddap_table(data: dict) -> list[dict[str, Any]]:
    """Convert an ERDDAP table-format JSON response into a list of row dicts.

    ERDDAP always returns data as::

        {
          "table": {
            "columnNames": ["col1", "col2", ...],
            "columnTypes": [...],
            "rows": [[val1, val2, ...], ...]
          }
        }

    This helper zips columnNames with each row to produce ordinary dicts.
    """
    table = data.get("table", {})
    cols = table.get("columnNames", [])
    rows = table.get("rows", [])
    if not cols:
        return []
    return [dict(zip(cols, row)) for row in rows]


def _parse_dataset_info(row: dict) -> dict[str, Any]:
    """Normalise a single ERDDAP dataset info row to a consistent shape."""
    return {
        "dataset_id":   row.get("Dataset ID", row.get("datasetID", "")),
        "title":        row.get("Title", row.get("title", "")),
        "summary":      row.get("Summary", row.get("summary", "")),
        "institution":  row.get("Institution", row.get("institution", "")),
        "data_type":    row.get("cdm_data_type", row.get("Data Type", "")),
        "min_time":     row.get("minTime (UTC)", row.get("min_time", "")),
        "max_time":     row.get("maxTime (UTC)", row.get("max_time", "")),
        "min_lon":      _safe_float(row.get("minLongitude (degrees_east)", row.get("min_lon"))),
        "max_lon":      _safe_float(row.get("maxLongitude (degrees_east)", row.get("max_lon"))),
        "min_lat":      _safe_float(row.get("minLatitude (degrees_north)", row.get("min_lat"))),
        "max_lat":      _safe_float(row.get("maxLatitude (degrees_north)", row.get("max_lat"))),
        "griddap_url":  row.get("griddap", ""),
        "tabledap_url": row.get("tabledap", ""),
    }


def _safe_float(val: Any) -> Optional[float]:
    """Convert a value to float, returning None on failure."""
    if val is None or val == "":
        return None
    try:
        return float(val)
    except (TypeError, ValueError):
        return None


def _parse_timestamp(val: Any) -> Optional[datetime]:
    """Parse an ERDDAP timestamp string (ISO-8601 or seconds since epoch)."""
    if val is None or val == "":
        return None
    # Numeric seconds since 1970-01-01
    if isinstance(val, (int, float)):
        try:
            return datetime.fromtimestamp(float(val), tz=timezone.utc)
        except (OSError, ValueError, OverflowError):
            return None
    # ISO string: "2024-01-15T12:00:00Z" or "2024-01-15T12:00:00+00:00"
    s = str(val).strip()
    for fmt in ("%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%S"):
        try:
            return datetime.strptime(s, fmt).replace(tzinfo=timezone.utc)
        except ValueError:
            pass
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except ValueError:
        return None


def _rows_to_stations_and_readings(
    rows: list[dict[str, Any]],
    server_id: str,
    dataset_id: str,
) -> tuple[list[Station], list[Reading]]:
    """Convert ERDDAP tabledap rows to Station and Reading objects.

    This best-effort parser handles common ERDDAP column naming conventions.
    Rows that lack valid lat/lon are returned as readings-only (station_id
    is synthesised from dataset_id).
    """
    stations_by_id: dict[str, Station] = {}
    readings: list[Reading] = []

    # Column name candidates (case-insensitive lookup done at row level)
    for row in rows:
        # Normalise keys to lowercase for matching
        low = {k.lower(): v for k, v in row.items()}

        lat = _safe_float(
            low.get("latitude") or low.get("lat") or low.get("latitude (degrees_north)")
        )
        lon = _safe_float(
            low.get("longitude") or low.get("lon") or low.get("longitude (degrees_east)")
        )

        ts_raw = (
            low.get("time")
            or low.get("time (utc)")
            or low.get("time (seconds since 1970-01-01t00:00:00z)")
        )
        ts = _parse_timestamp(ts_raw)
        if ts is None:
            continue

        # Build a station ID from station_id / station / platform / dataset
        raw_sid = (
            low.get("station")
            or low.get("station_id")
            or low.get("stationid")
            or low.get("platform")
            or low.get("wmo_platform_code")
            or low.get("mooring_site_desc")
            or ""
        )
        station_id = f"erddap:{server_id}:{dataset_id}"
        if raw_sid:
            station_id = f"erddap:{server_id}:{dataset_id}:{raw_sid}"

        # Register the station once
        if station_id not in stations_by_id and lat is not None and lon is not None:
            station_name = (
                str(low.get("station_name") or low.get("long_name") or raw_sid or dataset_id)
            )
            depth = _safe_float(low.get("depth") or low.get("depth (m)"))
            stations_by_id[station_id] = Station(
                id=station_id,
                name=station_name,
                lat=lat,
                lon=lon,
                source="erddap",
                network=server_id,
                params=[],
                elevation=-depth if depth is not None else None,
                url=f"{ERDDAP_SERVERS[server_id]}/erddap/tabledap/{dataset_id}.html"
                    if server_id in ERDDAP_SERVERS else None,
                extra={
                    "server_id": server_id,
                    "dataset_id": dataset_id,
                    "erddap_station": raw_sid,
                },
            )

        # Emit a Reading for each recognised variable in this row
        for col_raw, val in row.items():
            if val is None or val == "":
                continue
            col = col_raw.lower().split(" ")[0]  # strip "(unit)" suffix
            if col in ("latitude", "longitude", "time", "depth", "station",
                       "station_id", "platform", "wmo_platform_code"):
                continue
            param_name, unit = _VAR_PARAM_MAP.get(col, (col, ""))
            numeric = _safe_float(val)
            if numeric is None:
                continue
            readings.append(
                Reading(
                    station_id=station_id,
                    timestamp=ts,
                    parameter=param_name,
                    value=numeric,
                    unit=unit,
                    source="erddap",
                )
            )
            # Track which params the station measures
            if station_id in stations_by_id and param_name not in stations_by_id[station_id].params:
                stations_by_id[station_id].params.append(param_name)

    return list(stations_by_id.values()), readings


# ---------------------------------------------------------------------------
# Endpoint 1 — List configured servers with online status
# ---------------------------------------------------------------------------

@router.get(
    "/servers",
    summary="List all configured ERDDAP servers with online status",
)
async def list_servers(
    check_online: bool = Query(
        False,
        description=(
            "Probe each server to confirm it is reachable.  "
            "Takes several seconds; results are cached for 5 minutes."
        ),
    ),
):
    """Return all 25 configured ERDDAP servers.

    With `check_online=false` (default) returns the static server registry
    immediately.  With `check_online=true` fires concurrent HEAD-like probes
    to each server and annotates each entry with `online`, `error`, and a
    sample dataset count.  Results are cached for 5 minutes.
    """
    if not check_online:
        servers = [
            {
                "server_id":   sid,
                "base_url":    url,
                "erddap_url":  f"{url}/erddap/",
                "description": _SERVER_DESCRIPTIONS.get(sid, ""),
                "online":      None,
            }
            for sid, url in ERDDAP_SERVERS.items()
        ]
        return {
            "count": len(servers),
            "note": "Set check_online=true to probe each server's availability.",
            "servers": servers,
        }

    cache_key = "erddap:servers:online_status"
    cached = await cache.get(cache_key)
    if cached is not None:
        return cached

    # Fan out probe coroutines in parallel
    tasks = [_probe_server(sid, url) for sid, url in ERDDAP_SERVERS.items()]
    results = await asyncio.gather(*tasks, return_exceptions=False)

    online_count = sum(1 for r in results if r.get("online"))
    result = {
        "count": len(results),
        "online_count": online_count,
        "offline_count": len(results) - online_count,
        "servers": results,
    }
    await cache.set(cache_key, result, ttl=config.CACHE_TTL)
    return result


# ---------------------------------------------------------------------------
# Endpoint 2 — List / search datasets on a specific server
# ---------------------------------------------------------------------------

@router.get(
    "/{server_id}/datasets",
    summary="List or search datasets on a specific ERDDAP server",
)
async def list_datasets(
    server_id: str,
    search: Optional[str] = Query(
        None,
        description="Optional keyword to search for in dataset titles, summaries, etc.",
    ),
    limit: int = Query(
        100,
        ge=1,
        le=1000,
        description="Maximum number of datasets to return",
    ),
):
    """Return datasets available on the specified ERDDAP server.

    If `search` is provided, the ERDDAP full-text search index is used
    (`/erddap/search/index.json`).  Otherwise the full dataset catalogue
    (`/erddap/info/index.json`) is returned up to `limit` entries.
    Results are cached for 1 hour (dataset lists change infrequently).
    """
    erddap = _erddap_base(server_id)
    safe_search = (search or "").strip()
    cache_key = f"erddap:datasets:{server_id}:{safe_search}:{limit}"
    cached = await cache.get(cache_key)
    if cached is not None:
        return cached

    if safe_search:
        url = f"{erddap}/search/index.json"
        params: dict[str, Any] = {
            "searchFor":    safe_search,
            "page":         1,
            "itemsPerPage": limit,
        }
        label = f"ERDDAP search ({server_id})"
    else:
        url = f"{erddap}/info/index.json"
        params = {"page": 1, "itemsPerPage": limit}
        label = f"ERDDAP info index ({server_id})"

    try:
        data = await _get_json(url, params=params, label=label)
    except HTTPException:
        raise
    except Exception as exc:
        logger.exception("Unexpected error listing datasets on %s", server_id)
        raise HTTPException(status_code=502, detail=str(exc))

    rows = _parse_erddap_table(data)
    datasets = [_parse_dataset_info(r) for r in rows]

    result = {
        "server_id":  server_id,
        "erddap_url": f"{ERDDAP_SERVERS[server_id]}/erddap/",
        "search":     safe_search or None,
        "count":      len(datasets),
        "limit":      limit,
        "datasets":   datasets,
    }
    await cache.set(cache_key, result, ttl=config.CACHE_TTL_LONG)
    return result


# ---------------------------------------------------------------------------
# Endpoint 3 — Single dataset metadata
# ---------------------------------------------------------------------------

@router.get(
    "/{server_id}/dataset/{dataset_id}",
    summary="Metadata for a specific dataset on an ERDDAP server",
)
async def get_dataset_metadata(server_id: str, dataset_id: str):
    """Return full metadata for a single ERDDAP dataset.

    Fetches `/erddap/info/{dataset_id}/index.json`, which contains every
    attribute, variable, and dimension for the dataset.

    Results are cached for 1 hour.
    """
    erddap = _erddap_base(server_id)
    cache_key = f"erddap:meta:{server_id}:{dataset_id}"
    cached = await cache.get(cache_key)
    if cached is not None:
        return cached

    url = f"{erddap}/info/{dataset_id}/index.json"
    try:
        data = await _get_json(url, label=f"ERDDAP metadata ({server_id}/{dataset_id})")
    except HTTPException as exc:
        if exc.status_code == 404:
            raise HTTPException(
                status_code=404,
                detail=f"Dataset '{dataset_id}' not found on server '{server_id}'.",
            )
        raise

    rows = _parse_erddap_table(data)

    # Separate global attributes from variable metadata
    global_attrs: dict[str, Any] = {}
    variables: dict[str, dict[str, Any]] = {}

    for row in rows:
        row_type = row.get("Row Type", "")
        var_name  = row.get("Variable Name", "")
        attr_name = row.get("Attribute Name", "")
        value     = row.get("Value", "")

        if row_type == "attribute" and var_name == "NC_GLOBAL":
            global_attrs[attr_name] = value
        elif row_type == "variable":
            if var_name not in variables:
                variables[var_name] = {"name": var_name, "type": row.get("Data Type", ""), "attributes": {}}
        elif row_type == "attribute" and var_name:
            if var_name not in variables:
                variables[var_name] = {"name": var_name, "type": "", "attributes": {}}
            variables[var_name]["attributes"][attr_name] = value

    # Derive a few top-level convenience fields from global attributes
    result = {
        "server_id":   server_id,
        "dataset_id":  dataset_id,
        "tabledap_url": f"{ERDDAP_SERVERS[server_id]}/erddap/tabledap/{dataset_id}.html"
                        if dataset_id != "allDatasets" else None,
        "griddap_url":  f"{ERDDAP_SERVERS[server_id]}/erddap/griddap/{dataset_id}.html"
                        if dataset_id != "allDatasets" else None,
        "title":        global_attrs.get("title", ""),
        "summary":      global_attrs.get("summary", ""),
        "institution":  global_attrs.get("institution", ""),
        "license":      global_attrs.get("license", ""),
        "keywords":     global_attrs.get("keywords", ""),
        "cdm_data_type":global_attrs.get("cdm_data_type", ""),
        "time_coverage_start": global_attrs.get("time_coverage_start", ""),
        "time_coverage_end":   global_attrs.get("time_coverage_end", ""),
        "geospatial_lat_min":  _safe_float(global_attrs.get("geospatial_lat_min")),
        "geospatial_lat_max":  _safe_float(global_attrs.get("geospatial_lat_max")),
        "geospatial_lon_min":  _safe_float(global_attrs.get("geospatial_lon_min")),
        "geospatial_lon_max":  _safe_float(global_attrs.get("geospatial_lon_max")),
        "global_attributes":   global_attrs,
        "variables":           list(variables.values()),
        "variable_count":      len(variables),
    }
    await cache.set(cache_key, result, ttl=config.CACHE_TTL_LONG)
    return result


# ---------------------------------------------------------------------------
# Endpoint 4 — Query actual data from a tabledap dataset
# ---------------------------------------------------------------------------

@router.get(
    "/{server_id}/data/{dataset_id}",
    summary="Query data from an ERDDAP tabledap dataset",
)
async def get_dataset_data(
    server_id: str,
    dataset_id: str,
    variables: Optional[str] = Query(
        None,
        description=(
            "Comma-separated list of variables to retrieve, e.g. "
            "'latitude,longitude,time,sea_surface_temperature'.  "
            "If omitted, a sensible default set is requested."
        ),
    ),
    hours: int = Query(
        24,
        ge=1,
        le=8760,
        description="How many hours of data to look back from now (max 8760 = 1 year).",
    ),
    limit: int = Query(
        1000,
        ge=1,
        le=10000,
        description="Maximum number of rows to return.",
    ),
):
    """Query recent data from an ERDDAP tabledap dataset.

    Builds a tabledap request of the form::

        GET /erddap/tabledap/{dataset_id}.json?{vars}&time>={start}&orderByLimit("time,{limit}")

    The response is normalised into Station and Reading objects where
    recognised variable names are present (latitude, longitude, time plus at
    least one measured parameter).  The raw ERDDAP table is also returned so
    callers can work with all columns regardless of naming conventions.

    Results are cached for 5 minutes.
    """
    erddap = _erddap_base(server_id)
    cache_key = f"erddap:data:{server_id}:{dataset_id}:{variables}:{hours}:{limit}"
    cached = await cache.get(cache_key)
    if cached is not None:
        return cached

    # Build the time constraint
    start_dt = datetime.now(tz=timezone.utc) - timedelta(hours=hours)
    start_str = start_dt.strftime("%Y-%m-%dT%H:%M:%SZ")

    # Variable selection: caller-specified or a sensible broad default
    if variables and variables.strip():
        var_list = [v.strip() for v in variables.split(",") if v.strip()]
    else:
        # Request the most common oceanographic/met variables; ERDDAP silently
        # ignores variables that don't exist in the dataset.
        var_list = [
            "latitude", "longitude", "time", "depth",
            "station", "station_id", "platform",
            "sea_surface_temperature", "sea_water_temperature",
            "water_temperature", "air_temperature",
            "sea_surface_salinity", "sea_water_salinity",
            "significant_wave_height", "wind_speed", "wind_from_direction",
            "sea_level_pressure", "relative_humidity",
            "chlorophyll", "dissolved_oxygen",
        ]

    # ERDDAP tabledap query:  variables + constraints appended as query string
    # The ERDDAP URL format is:  /tabledap/{id}.json?var1,var2&constraint
    # httpx params= would double-encode the constraint operators, so we build
    # the query string component manually and pass it as part of the URL.
    var_str = ",".join(var_list)
    constraint = f"&time>={start_str}&orderByLimit(%22time,{limit}%22)"
    query_url = f"{erddap}/tabledap/{dataset_id}.json?{var_str}{constraint}"

    try:
        async with _make_client() as client:
            resp = await client.get(query_url)
            # ERDDAP returns 400 with an HTML/JSON error body if variables are
            # invalid for this dataset.  A 400 is recoverable — retry with the
            # minimal set of core variables.
            if resp.status_code == 400 and var_list != ["latitude", "longitude", "time"]:
                logger.debug(
                    "ERDDAP 400 on %s/%s with full var list — retrying with minimal vars",
                    server_id, dataset_id,
                )
                minimal_constraint = f"&time>={start_str}&orderByLimit(%22time,{limit}%22)"
                query_url = (
                    f"{erddap}/tabledap/{dataset_id}.json?"
                    f"latitude,longitude,time{minimal_constraint}"
                )
                resp = await client.get(query_url)
            resp.raise_for_status()
            data = resp.json()
    except httpx.TimeoutException:
        raise HTTPException(status_code=504, detail=f"ERDDAP tabledap timeout ({server_id}/{dataset_id})")
    except httpx.HTTPStatusError as exc:
        status = exc.response.status_code
        if status == 404:
            raise HTTPException(
                status_code=404,
                detail=f"Dataset '{dataset_id}' not found on server '{server_id}'.",
            )
        raise HTTPException(
            status_code=status,
            detail=f"ERDDAP tabledap error {status} for {server_id}/{dataset_id}",
        )
    except HTTPException:
        raise
    except Exception as exc:
        logger.exception("Unexpected error querying data from %s/%s", server_id, dataset_id)
        raise HTTPException(status_code=502, detail=str(exc))

    rows = _parse_erddap_table(data)
    stations, readings = _rows_to_stations_and_readings(rows, server_id, dataset_id)

    result = {
        "server_id":    server_id,
        "dataset_id":   dataset_id,
        "hours":        hours,
        "limit":        limit,
        "variables_requested": var_list,
        "row_count":    len(rows),
        "station_count": len(stations),
        "reading_count": len(readings),
        "stations":     [s.model_dump() for s in stations],
        "readings":     [r.model_dump() for r in readings],
        "raw_columns":  data.get("table", {}).get("columnNames", []),
        "raw_rows":     rows[:limit],  # echo back parsed rows for flexibility
    }
    await cache.set(cache_key, result, ttl=config.CACHE_TTL)
    return result


# ---------------------------------------------------------------------------
# Endpoint 5 — Cross-server dataset search
# ---------------------------------------------------------------------------

async def _search_one_server(
    server_id: str,
    base_url: str,
    query: str,
    limit: int,
) -> dict[str, Any]:
    """Search a single ERDDAP server; return a result dict, never raises."""
    url = f"{base_url}/erddap/search/index.json"
    params: dict[str, Any] = {
        "searchFor":    query,
        "page":         1,
        "itemsPerPage": limit,
    }
    try:
        async with httpx.AsyncClient(
            headers=_HTTP_HEADERS,
            timeout=15.0,
            follow_redirects=True,
        ) as client:
            resp = await client.get(url, params=params)
            resp.raise_for_status()
            data = resp.json()

        rows = _parse_erddap_table(data)
        datasets = [_parse_dataset_info(r) for r in rows]
        return {
            "server_id":   server_id,
            "erddap_url":  f"{base_url}/erddap/",
            "description": _SERVER_DESCRIPTIONS.get(server_id, ""),
            "count":       len(datasets),
            "datasets":    datasets,
            "error":       None,
        }
    except httpx.TimeoutException:
        logger.debug("ERDDAP search timeout on %s", server_id)
        return {
            "server_id": server_id, "erddap_url": f"{base_url}/erddap/",
            "description": _SERVER_DESCRIPTIONS.get(server_id, ""),
            "count": 0, "datasets": [], "error": "timeout",
        }
    except httpx.HTTPStatusError as exc:
        # 404 is normal for servers that return no results this way
        return {
            "server_id": server_id, "erddap_url": f"{base_url}/erddap/",
            "description": _SERVER_DESCRIPTIONS.get(server_id, ""),
            "count": 0, "datasets": [],
            "error": f"HTTP {exc.response.status_code}",
        }
    except Exception as exc:
        logger.debug("ERDDAP search error on %s: %s", server_id, exc)
        return {
            "server_id": server_id, "erddap_url": f"{base_url}/erddap/",
            "description": _SERVER_DESCRIPTIONS.get(server_id, ""),
            "count": 0, "datasets": [], "error": str(exc),
        }


@router.get(
    "/search",
    summary="Search for datasets matching a keyword across ALL configured ERDDAP servers",
)
async def search_all_servers(
    q: str = Query(..., min_length=2, description="Search term, e.g. 'sea_surface_temperature'"),
    limit: int = Query(
        20,
        ge=1,
        le=200,
        description="Maximum number of results to return per server.",
    ),
    servers: Optional[str] = Query(
        None,
        description=(
            "Comma-separated list of server IDs to restrict the search to.  "
            "If omitted all configured servers are queried in parallel."
        ),
    ),
):
    """Fan out a keyword search to all (or a subset of) ERDDAP servers in parallel.

    Results are aggregated and sorted by server.  Each server result includes
    the dataset list and an `error` field (null on success).

    Example searches:
    - `sea_surface_temperature` — SST datasets
    - `wave height` — wave observations
    - `glider` — all glider mission datasets
    - `salinity Oregon` — Oregon salinity datasets
    - `NDBC` — NOAA buoy data mirrored on ERDDAP

    Results are cached for 5 minutes.
    """
    q_clean = q.strip()
    cache_key = f"erddap:search:{q_clean}:{limit}:{servers}"
    cached_result = await cache.get(cache_key)
    if cached_result is not None:
        return cached_result

    # Determine which servers to query
    if servers and servers.strip():
        requested_ids = [s.strip() for s in servers.split(",") if s.strip()]
        unknown = [s for s in requested_ids if s not in ERDDAP_SERVERS]
        if unknown:
            raise HTTPException(
                status_code=400,
                detail=f"Unknown server IDs: {', '.join(unknown)}. "
                       f"Valid IDs: {', '.join(sorted(ERDDAP_SERVERS))}.",
            )
        target_servers = {sid: ERDDAP_SERVERS[sid] for sid in requested_ids}
    else:
        target_servers = ERDDAP_SERVERS

    # Fan out searches in parallel
    tasks = [
        _search_one_server(sid, url, q_clean, limit)
        for sid, url in target_servers.items()
    ]
    server_results: list[dict[str, Any]] = await asyncio.gather(*tasks)

    # Aggregate totals
    total_datasets = sum(r["count"] for r in server_results)
    servers_with_results = [r for r in server_results if r["count"] > 0]
    servers_with_errors  = [r["server_id"] for r in server_results if r["error"]]

    result = {
        "query":                 q_clean,
        "limit_per_server":      limit,
        "servers_queried":       len(server_results),
        "servers_with_results":  len(servers_with_results),
        "servers_with_errors":   len(servers_with_errors),
        "total_datasets_found":  total_datasets,
        "error_servers":         servers_with_errors,
        "results":               server_results,
    }
    await cache.set(cache_key, result, ttl=config.CACHE_TTL)
    return result
