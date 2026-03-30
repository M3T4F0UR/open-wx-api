"""East Coast & Southeast Weather/Water Networks.

Covers five regional data sources along the US East Coast and Southeast:

  1. Maryland CHART RWIS — Road weather stations via clean, no-auth JSON API.
  2. South Florida Water Management District (SFWMD) — Monitoring locations via
     ArcGIS REST FeatureServer.
  3. Charlotte-Mecklenburg FINS — Rain & stream gauges via USGS NWIS
     (Mecklenburg County, NC).
  4. NC FIMAN — North Carolina statewide precipitation gauges via USGS NWIS.
  5. Florida FAWN — UF/IFAS Automated Weather Network latest observations.

API references:
  Maryland CHART: https://chart.maryland.gov/LocalWeatherData/GetWeatherStationData
  SFWMD ArcGIS:   https://geoweb.sfwmd.gov/agsext2/rest/services/MonitoringLocations/
  USGS NWIS IV:   https://waterservices.usgs.gov/nwis/iv/
  FAWN:           https://fawn.ifas.ufl.edu/controller.php/lastObs/
"""
import logging
from datetime import datetime, timezone
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
SOURCE_NAME = "East Coast Networks"
SOURCE_DESCRIPTION = (
    "Aggregated East Coast and Southeast US weather and water data: Maryland CHART RWIS "
    "road weather stations, South Florida Water Management District (SFWMD) monitoring "
    "locations, Charlotte-Mecklenburg FINS gauges, NC FIMAN precipitation stations, and "
    "Florida FAWN automated agricultural weather stations."
)
SOURCE_META = {
    "url": "https://chart.maryland.gov/LocalWeatherData/",
    "auth_required": False,
    "update_frequency": "5–15 min depending on network",
    "coverage": "Maryland, North Carolina, South Carolina, Florida",
    "params": [
        "air_temperature", "dew_point", "wind_speed", "wind_direction",
        "visibility", "precipitation", "pavement_temperature", "pavement_condition",
        "discharge", "gage_height", "water_temperature",
    ],
    "category": "weather,water",
}

router = APIRouter(prefix="/api/eastcoast", tags=["East Coast Networks"])

# ---------------------------------------------------------------------------
# API base URLs
# ---------------------------------------------------------------------------
CHART_URL = "https://chart.maryland.gov/LocalWeatherData/GetWeatherStationData"

SFWMD_BASE = "https://geoweb.sfwmd.gov/agsext2/rest/services/MonitoringLocations"
SFWMD_WELLS_LAYER = f"{SFWMD_BASE}/DBHYDRO_Wells/FeatureServer/0/query"
SFWMD_RAIN_LAYER = f"{SFWMD_BASE}/DBHYDRO_Rain/FeatureServer/0/query"
SFWMD_STAGE_LAYER = f"{SFWMD_BASE}/DBHYDRO_Stage/FeatureServer/0/query"

NWIS_IV_BASE = "https://waterservices.usgs.gov/nwis/iv/"
# Mecklenburg County FIPS: 37119 (Charlotte, NC)
CHARLOTTE_COUNTY_CD = "37119"

FAWN_URL = "https://fawn.ifas.ufl.edu/controller.php/lastObs/"

# ---------------------------------------------------------------------------
# HTTP client helpers
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


# ===========================================================================
# 1. Maryland CHART RWIS
# ===========================================================================

def _parse_chart_station(raw: dict) -> tuple[Station, list[Reading]]:
    """Parse a single Maryland CHART station dict into Station + Readings."""
    station_id = str(raw.get("StationID", raw.get("stationId", "")))
    name = raw.get("StationName", raw.get("stationName", station_id))

    try:
        lat = float(raw.get("Latitude", raw.get("latitude", 0.0)))
    except (TypeError, ValueError):
        lat = 0.0
    try:
        lon = float(raw.get("Longitude", raw.get("longitude", 0.0)))
    except (TypeError, ValueError):
        lon = 0.0

    # Parse last-updated timestamp
    last_update: Optional[datetime] = None
    for ts_key in ("LastUpdated", "lastUpdated", "ObsTime", "obsTime", "DateTime"):
        ts_raw = raw.get(ts_key)
        if ts_raw:
            try:
                last_update = datetime.fromisoformat(str(ts_raw).replace("Z", "+00:00"))
            except ValueError:
                pass
            break

    # Build extra metadata dict — keep all scalar fields not parsed above
    skip_keys = {
        "StationID", "stationId", "StationName", "stationName",
        "Latitude", "latitude", "Longitude", "longitude",
        "LastUpdated", "lastUpdated", "ObsTime", "obsTime", "DateTime",
    }
    extra = {k: v for k, v in raw.items() if k not in skip_keys and not isinstance(v, (dict, list))}

    station = Station(
        id=f"md_chart_{station_id}",
        name=name,
        lat=lat,
        lon=lon,
        source="maryland_chart",
        network="MD CHART RWIS",
        url=f"https://chart.maryland.gov/LocalWeatherData/",
        last_update=last_update,
        extra=extra,
    )

    # Build Readings from known CHART observation fields
    readings: list[Reading] = []
    timestamp = last_update or datetime.now(timezone.utc)
    source_tag = "maryland_chart"

    # Map of (raw_key, parameter_name, unit)
    obs_fields: list[tuple[str, str, str]] = [
        # Air temperature — CHART typically returns Fahrenheit
        ("AirTemperature",          "air_temperature",        "°F"),
        ("airTemperature",          "air_temperature",        "°F"),
        ("AirTemp",                 "air_temperature",        "°F"),
        # Dew point
        ("DewPoint",                "dew_point",              "°F"),
        ("dewPoint",                "dew_point",              "°F"),
        # Relative humidity
        ("RelativeHumidity",        "relative_humidity",      "%"),
        ("relativeHumidity",        "relative_humidity",      "%"),
        # Wind speed
        ("WindSpeed",               "wind_speed",             "mph"),
        ("windSpeed",               "wind_speed",             "mph"),
        # Wind direction (degrees)
        ("WindDirection",           "wind_direction",         "deg"),
        ("windDirection",           "wind_direction",         "deg"),
        # Wind gust
        ("WindGust",                "wind_gust",              "mph"),
        ("windGust",                "wind_gust",              "mph"),
        # Visibility
        ("Visibility",              "visibility",             "mi"),
        ("visibility",              "visibility",             "mi"),
        # Precipitation
        ("PrecipRate",              "precipitation_rate",     "in/hr"),
        ("precipRate",              "precipitation_rate",     "in/hr"),
        ("Precipitation",           "precipitation",          "in"),
        ("precipitation",           "precipitation",          "in"),
        # Pavement temperature
        ("PavementTemperature",     "pavement_temperature",   "°F"),
        ("pavementTemperature",     "pavement_temperature",   "°F"),
        ("SurfaceTemperature",      "pavement_temperature",   "°F"),
        # Barometric pressure
        ("Pressure",                "pressure",               "mb"),
        ("pressure",                "pressure",               "mb"),
        ("BarometricPressure",      "pressure",               "mb"),
    ]

    seen_params: set[str] = set()
    for raw_key, param_name, unit in obs_fields:
        if param_name in seen_params:
            continue
        raw_val = raw.get(raw_key)
        if raw_val is None:
            continue
        try:
            value = float(raw_val)
        except (TypeError, ValueError):
            continue
        seen_params.add(param_name)
        readings.append(
            Reading(
                station_id=station.id,
                timestamp=timestamp,
                parameter=param_name,
                value=value,
                unit=unit,
                source=source_tag,
            )
        )

    station.params = list(seen_params)
    return station, readings


@router.get("/maryland_chart/stations", summary="All Maryland CHART RWIS weather stations")
async def maryland_chart_stations():
    """Return all Maryland CHART Road Weather Information System (RWIS) stations
    with their latest observations.

    The CHART API returns a single JSON payload with all active stations in one
    call — air temperature, dew point, wind speed/direction/gust, visibility,
    precipitation rate, and pavement temperature/condition.

    Source: https://chart.maryland.gov/LocalWeatherData/GetWeatherStationData
    """
    cache_key = "eastcoast:maryland_chart:stations"
    cached_result = await cache.get(cache_key)
    if cached_result is not None:
        return cached_result

    try:
        async with _client() as client:
            resp = await client.get(CHART_URL)
            resp.raise_for_status()
            data = resp.json()
    except httpx.TimeoutException:
        raise HTTPException(status_code=504, detail="Maryland CHART API timeout")
    except httpx.HTTPStatusError as exc:
        raise HTTPException(
            status_code=exc.response.status_code,
            detail=f"Maryland CHART API error: {exc}",
        )
    except Exception as exc:
        logger.exception("Unexpected error fetching Maryland CHART stations")
        raise HTTPException(status_code=500, detail=str(exc))

    # CHART returns either a list directly or wraps it in a key
    if isinstance(data, list):
        raw_stations = data
    elif isinstance(data, dict):
        # Try common wrapper keys
        for key in ("data", "stations", "WeatherStations", "features", "results"):
            if key in data and isinstance(data[key], list):
                raw_stations = data[key]
                break
        else:
            # Fallback: treat the dict values as a list
            raw_stations = list(data.values()) if data else []
    else:
        raw_stations = []

    stations = []
    for raw in raw_stations:
        if not isinstance(raw, dict):
            continue
        try:
            station, readings = _parse_chart_station(raw)
            entry = station.model_dump()
            entry["latest_readings"] = [r.model_dump() for r in readings]
            stations.append(entry)
        except Exception:
            logger.debug("Failed to parse CHART station: %s", raw, exc_info=True)
            continue

    result = {
        "source": "maryland_chart",
        "network": "MD CHART RWIS",
        "description": "Maryland CHART Road Weather Information System",
        "count": len(stations),
        "stations": stations,
    }
    await cache.set(cache_key, result, ttl=config.CACHE_TTL)
    return result


@router.get("/maryland_chart/stations/{station_id}", summary="Single Maryland CHART RWIS station")
async def maryland_chart_station(station_id: str):
    """Return metadata and latest observations for a single Maryland CHART station.

    The station_id should be the numeric ID returned by the list endpoint
    (without the 'md_chart_' prefix), or the full 'md_chart_XXX' form.
    """
    # Accept both bare numeric ID and prefixed form
    bare_id = station_id.removeprefix("md_chart_")

    cache_key = f"eastcoast:maryland_chart:station:{bare_id}"
    cached_result = await cache.get(cache_key)
    if cached_result is not None:
        return cached_result

    try:
        async with _client() as client:
            resp = await client.get(CHART_URL)
            resp.raise_for_status()
            data = resp.json()
    except httpx.TimeoutException:
        raise HTTPException(status_code=504, detail="Maryland CHART API timeout")
    except httpx.HTTPStatusError as exc:
        raise HTTPException(
            status_code=exc.response.status_code,
            detail=f"Maryland CHART API error: {exc}",
        )
    except Exception as exc:
        logger.exception("Error fetching Maryland CHART data for station %s", station_id)
        raise HTTPException(status_code=500, detail=str(exc))

    if isinstance(data, list):
        raw_stations = data
    elif isinstance(data, dict):
        for key in ("data", "stations", "WeatherStations", "features", "results"):
            if key in data and isinstance(data[key], list):
                raw_stations = data[key]
                break
        else:
            raw_stations = list(data.values()) if data else []
    else:
        raw_stations = []

    # Find the matching station by ID
    target: Optional[dict] = None
    for raw in raw_stations:
        if not isinstance(raw, dict):
            continue
        raw_id = str(raw.get("StationID", raw.get("stationId", "")))
        if raw_id == bare_id:
            target = raw
            break

    if target is None:
        raise HTTPException(
            status_code=404,
            detail=f"Maryland CHART station '{station_id}' not found",
        )

    station, readings = _parse_chart_station(target)
    result = {
        "station": station.model_dump(),
        "readings": [r.model_dump() for r in readings],
    }
    await cache.set(cache_key, result, ttl=config.CACHE_TTL)
    return result


# ===========================================================================
# 2. South Florida Water Management District (SFWMD)
# ===========================================================================

def _arcgis_query_params(where: str = "1=1", out_fields: str = "*", result_offset: int = 0) -> dict:
    """Build standard ArcGIS FeatureServer query parameters."""
    return {
        "where": where,
        "outFields": out_fields,
        "f": "json",
        "resultOffset": result_offset,
        "resultRecordCount": 1000,
        "returnGeometry": "true",
        "spatialRel": "esriSpatialRelIntersects",
    }


def _parse_sfwmd_feature(feature: dict, layer_name: str) -> Optional[Station]:
    """Convert an ArcGIS feature dict to a Station model."""
    attrs = feature.get("attributes", {})
    geom = feature.get("geometry", {})

    # Try common SFWMD station ID field names
    station_id = None
    for id_key in ("SITE", "STATION", "DBKEY", "SITE_ID", "OBJECTID", "FID"):
        val = attrs.get(id_key)
        if val is not None:
            station_id = str(val)
            break
    if station_id is None:
        return None

    # Station name
    name = None
    for name_key in ("SITE_NAME", "STATION_NAME", "NAME", "DESCRIPTION", "DESC"):
        val = attrs.get(name_key)
        if val and str(val).strip():
            name = str(val).strip()
            break
    name = name or station_id

    # Coordinates — ArcGIS geometry for geographic SR is x=lon, y=lat
    lat = 0.0
    lon = 0.0
    if geom:
        try:
            lon = float(geom.get("x", 0.0))
            lat = float(geom.get("y", 0.0))
        except (TypeError, ValueError):
            pass

    # Elevation
    elev: Optional[float] = None
    for elev_key in ("ELEVATION", "ELEV", "ALT"):
        val = attrs.get(elev_key)
        if val is not None:
            try:
                elev = float(val)
                break
            except (TypeError, ValueError):
                pass

    # Parse last_update from SFWMD period-of-record end timestamps.
    # ArcGIS date fields are epoch milliseconds.  Try several field names
    # in priority order — END_TS_POR (time-series end in Wells layer),
    # END_SAMPLE_POR (water-quality sampling end in Wells layer),
    # END_DATE (data period end in SiteStation layer).
    last_update: Optional[datetime] = None
    for ts_key in ("END_TS_POR", "END_SAMPLE_POR", "END_DATE"):
        ts_val = attrs.get(ts_key)
        if ts_val is not None:
            try:
                last_update = datetime.fromtimestamp(int(ts_val) / 1000, tz=timezone.utc)
                break
            except (TypeError, ValueError, OSError):
                pass

    # Build extra from remaining attributes
    extra = {k: v for k, v in attrs.items() if v is not None and k not in
             {"SITE", "STATION", "DBKEY", "SITE_ID", "OBJECTID", "FID",
              "SITE_NAME", "STATION_NAME", "NAME", "DESCRIPTION", "DESC",
              "ELEVATION", "ELEV", "ALT"}}

    return Station(
        id=f"sfwmd_{layer_name}_{station_id}",
        name=name,
        lat=lat,
        lon=lon,
        source="sfwmd",
        network=f"SFWMD {layer_name.replace('_', ' ').title()}",
        elevation=elev,
        last_update=last_update,
        url=f"https://www.sfwmd.gov/weather-radar/realtime-gauges",
        extra=extra,
    )


async def _fetch_sfwmd_layer(layer_url: str, layer_name: str) -> list[Station]:
    """Fetch all features from one SFWMD ArcGIS FeatureServer layer."""
    stations: list[Station] = []
    offset = 0

    try:
        async with _client() as client:
            while True:
                params = _arcgis_query_params(result_offset=offset)
                resp = await client.get(layer_url, params=params)
                resp.raise_for_status()
                data = resp.json()

                features = data.get("features", [])
                if not features:
                    break

                for feat in features:
                    station = _parse_sfwmd_feature(feat, layer_name)
                    if station:
                        stations.append(station)

                # ArcGIS signals more pages via exceededTransferLimit
                if not data.get("exceededTransferLimit", False):
                    break
                offset += len(features)

                # Safety cap — avoid infinite loops
                if offset > 10000:
                    logger.warning("SFWMD layer %s: offset cap hit at %d", layer_name, offset)
                    break

    except httpx.TimeoutException:
        logger.warning("SFWMD layer %s fetch timed out at offset %d", layer_name, offset)
    except httpx.HTTPStatusError as exc:
        logger.warning("SFWMD layer %s HTTP error: %s", layer_name, exc)
    except Exception:
        logger.exception("Unexpected error fetching SFWMD layer %s", layer_name)

    return stations


@router.get("/sfwmd/stations", summary="SFWMD monitoring stations (wells, rain, stage)")
async def sfwmd_stations(
    layer: str = Query(
        "wells",
        description="Which SFWMD layer to query: 'wells', 'rain', or 'stage'",
        pattern="^(wells|rain|stage)$",
    ),
):
    """Return South Florida Water Management District monitoring station locations.

    Queries the SFWMD ArcGIS REST FeatureServer. Three layers are available:
      - **wells** — groundwater monitoring wells (DBHYDRO_Wells)
      - **rain**  — rainfall gauges (DBHYDRO_Rain)
      - **stage** — water stage/level gauges (DBHYDRO_Stage)

    Source: https://geoweb.sfwmd.gov/agsext2/rest/services/MonitoringLocations/
    """
    layer_map = {
        "wells": (SFWMD_WELLS_LAYER, "wells"),
        "rain": (SFWMD_RAIN_LAYER, "rain"),
        "stage": (SFWMD_STAGE_LAYER, "stage"),
    }
    layer_url, layer_name = layer_map[layer]

    cache_key = f"eastcoast:sfwmd:stations:{layer}"
    cached_result = await cache.get(cache_key)
    if cached_result is not None:
        return cached_result

    stations = await _fetch_sfwmd_layer(layer_url, layer_name)

    result = {
        "source": "sfwmd",
        "layer": layer,
        "network": "South Florida Water Management District",
        "description": f"SFWMD DBHYDRO {layer} monitoring locations",
        "count": len(stations),
        "stations": [s.model_dump() for s in stations],
    }
    await cache.set(cache_key, result, ttl=config.CACHE_TTL_LONG)
    return result


@router.get("/sfwmd/stations/{station_id}", summary="Single SFWMD monitoring station detail")
async def sfwmd_station(station_id: str):
    """Return metadata for a single SFWMD monitoring station.

    The station_id should be the full id as returned by the list endpoint
    (e.g. 'sfwmd_wells_G-1234').

    Because SFWMD's ArcGIS layer does not support single-feature lookups
    without a known OBJECTID, this endpoint fetches the full layer and
    filters the result, relying on the cache to keep this efficient.
    """
    # Determine layer from id prefix
    layer_name = "wells"
    if "_rain_" in station_id:
        layer_name = "rain"
    elif "_stage_" in station_id:
        layer_name = "stage"

    # Re-use the cached station list
    list_cache_key = f"eastcoast:sfwmd:stations:{layer_name}"
    cached_list = await cache.get(list_cache_key)

    stations_raw: Optional[list] = None
    if cached_list is not None:
        stations_raw = cached_list.get("stations")

    if stations_raw is None:
        layer_map = {
            "wells": (SFWMD_WELLS_LAYER, "wells"),
            "rain": (SFWMD_RAIN_LAYER, "rain"),
            "stage": (SFWMD_STAGE_LAYER, "stage"),
        }
        layer_url, ln = layer_map[layer_name]
        fetched = await _fetch_sfwmd_layer(layer_url, ln)
        stations_raw = [s.model_dump() for s in fetched]
        # Populate the list cache opportunistically
        await cache.set(
            list_cache_key,
            {
                "source": "sfwmd",
                "layer": layer_name,
                "network": "South Florida Water Management District",
                "description": f"SFWMD DBHYDRO {layer_name} monitoring locations",
                "count": len(stations_raw),
                "stations": stations_raw,
            },
            ttl=config.CACHE_TTL_LONG,
        )

    match = next((s for s in stations_raw if s.get("id") == station_id), None)
    if match is None:
        raise HTTPException(
            status_code=404,
            detail=f"SFWMD station '{station_id}' not found in layer '{layer_name}'",
        )

    return {"station": match, "readings": []}


# ===========================================================================
# 3 & 4. USGS NWIS helpers (shared by Charlotte and NC FIMAN)
# ===========================================================================

_USGS_PARAM_MAP: dict[str, tuple[str, str]] = {
    "00045": ("precipitation",       "in"),
    "00060": ("discharge",           "ft³/s"),
    "00065": ("gage_height",         "ft"),
    "00010": ("water_temperature",   "°C"),
    "00300": ("dissolved_oxygen",    "mg/L"),
    "00400": ("pH",                  "std units"),
    "00095": ("specific_conductance","µS/cm"),
    "72019": ("depth_to_water",      "ft"),
}


def _usgs_param_info(code: str) -> tuple[str, str]:
    return _USGS_PARAM_MAP.get(code, (code, "unknown"))


def _parse_usgs_response(data: dict, source_tag: str, network: str) -> tuple[list[Station], dict[str, list[Reading]]]:
    """Parse a USGS NWIS IV JSON response into Station and Reading lists."""
    stations: list[Station] = []
    readings_map: dict[str, list[Reading]] = {}

    ts_by_site: dict[str, list[dict]] = {}
    for ts in data.get("value", {}).get("timeSeries", []):
        site_id = ts.get("sourceInfo", {}).get("siteCode", [{}])[0].get("value", "")
        if site_id:
            ts_by_site.setdefault(site_id, []).append(ts)

    for site_id, ts_list in ts_by_site.items():
        src_info = ts_list[0].get("sourceInfo", {})
        geo = src_info.get("geoLocation", {}).get("geogLocation", {})
        try:
            lat = float(geo.get("latitude", 0.0))
            lon = float(geo.get("longitude", 0.0))
        except (TypeError, ValueError):
            lat, lon = 0.0, 0.0

        elev_raw = src_info.get("elevation")
        try:
            elev: Optional[float] = float(elev_raw) if elev_raw is not None else None
        except (TypeError, ValueError):
            elev = None

        huc_list = src_info.get("hucCd", [{}])
        huc = huc_list[0].get("value", "") if huc_list else ""
        site_type_list = src_info.get("siteType", [{}])
        site_type = site_type_list[0].get("value", "") if site_type_list else ""

        station = Station(
            id=site_id,
            name=src_info.get("siteName", site_id),
            lat=lat,
            lon=lon,
            source=source_tag,
            network=network,
            elevation=elev,
            url=f"https://waterdata.usgs.gov/nwis/uv?site_no={site_id}",
            extra={"huc": huc, "site_type": site_type},
        )

        readings: list[Reading] = []
        params: list[str] = []

        for ts in ts_list:
            variable = ts.get("variable", {})
            p_code = variable.get("variableCode", [{}])[0].get("value", "")
            p_name, p_unit = _usgs_param_info(p_code)
            unit = variable.get("unit", {}).get("unitCode", p_unit)

            if p_name not in params:
                params.append(p_name)

            for entry in ts.get("values", [{}])[0].get("value", []):
                raw_val = entry.get("value")
                if raw_val in (None, "", "-999999"):
                    continue
                try:
                    value = float(raw_val)
                except (TypeError, ValueError):
                    continue
                dt_str = entry.get("dateTime", "")
                try:
                    ts_dt = datetime.fromisoformat(dt_str)
                except ValueError:
                    continue
                qualifiers = entry.get("qualifiers", [])
                quality = ",".join(qualifiers) if qualifiers else None
                readings.append(
                    Reading(
                        station_id=site_id,
                        timestamp=ts_dt,
                        parameter=p_name,
                        value=value,
                        unit=unit,
                        quality=quality,
                        source=source_tag,
                    )
                )

        station.params = params
        stations.append(station)
        readings_map[site_id] = readings

    return stations, readings_map


# ===========================================================================
# 3. Charlotte-Mecklenburg FINS (via USGS NWIS)
# ===========================================================================

@router.get("/charlotte/stations", summary="Charlotte-Mecklenburg rain & stream gauges (USGS NWIS)")
async def charlotte_stations(
    param: str = Query(
        "00045,00065",
        description="USGS parameter codes, comma-separated. Default: precip (00045) + gage height (00065)",
    ),
):
    """Return active USGS monitoring stations in Mecklenburg County, NC.

    Data is sourced from the USGS National Water Information System (NWIS)
    Instantaneous Values API, filtered to Mecklenburg County (FIPS 37119).
    Stations include the Charlotte-Mecklenburg FINS rain and stream gauge network.

    Source: https://waterservices.usgs.gov/nwis/iv/
    """
    cache_key = f"eastcoast:charlotte:stations:{param}"
    cached_result = await cache.get(cache_key)
    if cached_result is not None:
        return cached_result

    query_params = {
        "format": "json",
        "countyCd": CHARLOTTE_COUNTY_CD,
        "parameterCd": param,
        "siteStatus": "active",
    }

    try:
        async with _client() as client:
            resp = await client.get(NWIS_IV_BASE, params=query_params)
            resp.raise_for_status()
            data = resp.json()
    except httpx.TimeoutException:
        raise HTTPException(status_code=504, detail="USGS NWIS API timeout (Charlotte)")
    except httpx.HTTPStatusError as exc:
        raise HTTPException(
            status_code=exc.response.status_code,
            detail=f"USGS NWIS error (Charlotte): {exc}",
        )
    except Exception as exc:
        logger.exception("Error fetching Charlotte USGS stations")
        raise HTTPException(status_code=500, detail=str(exc))

    stations, readings_map = _parse_usgs_response(data, "usgs_charlotte", "Charlotte-Mecklenburg FINS")

    # Attach only the most recent reading per parameter to each station entry
    station_list = []
    for s in stations:
        latest: dict[str, Reading] = {}
        for r in readings_map.get(s.id, []):
            if r.parameter not in latest or r.timestamp > latest[r.parameter].timestamp:
                latest[r.parameter] = r
        entry = s.model_dump()
        entry["latest_readings"] = [r.model_dump() for r in latest.values()]
        station_list.append(entry)

    result = {
        "source": "usgs_charlotte",
        "network": "Charlotte-Mecklenburg FINS",
        "county": "Mecklenburg County, NC",
        "county_fips": CHARLOTTE_COUNTY_CD,
        "description": "Charlotte-Mecklenburg FINS rain and stream gauges via USGS NWIS",
        "count": len(station_list),
        "stations": station_list,
    }
    await cache.set(cache_key, result, ttl=config.CACHE_TTL_LONG)
    return result


@router.get(
    "/charlotte/stations/{site_id}/readings",
    summary="Time-series readings for a Charlotte-Mecklenburg USGS gauge",
)
async def charlotte_readings(
    site_id: str,
    hours: int = Query(24, ge=1, le=2160, description="Hours of history (max 90 days)"),
    param: str = Query(
        "00045,00065",
        description="USGS parameter codes, comma-separated",
    ),
):
    """Return time-series observations for a Charlotte-Mecklenburg USGS gauge.

    Uses the USGS NWIS Instantaneous Values API. Default parameters are
    precipitation (00045) and gage height (00065).

    Source: https://waterservices.usgs.gov/nwis/iv/
    """
    cache_key = f"eastcoast:charlotte:readings:{site_id}:{hours}:{param}"
    cached_result = await cache.get(cache_key)
    if cached_result is not None:
        return cached_result

    query_params = {
        "format": "json",
        "sites": site_id,
        "parameterCd": param,
        "period": f"PT{hours}H",
        "siteStatus": "all",
    }

    try:
        async with _client() as client:
            resp = await client.get(NWIS_IV_BASE, params=query_params)
            resp.raise_for_status()
            data = resp.json()
    except httpx.TimeoutException:
        raise HTTPException(status_code=504, detail="USGS NWIS API timeout (Charlotte readings)")
    except httpx.HTTPStatusError as exc:
        raise HTTPException(
            status_code=exc.response.status_code,
            detail=f"USGS NWIS error: {exc}",
        )
    except Exception as exc:
        logger.exception("Error fetching Charlotte readings for site %s", site_id)
        raise HTTPException(status_code=500, detail=str(exc))

    stations, readings_map = _parse_usgs_response(data, "usgs_charlotte", "Charlotte-Mecklenburg FINS")
    if not stations:
        raise HTTPException(
            status_code=404,
            detail=f"USGS site '{site_id}' not found or has no data for the requested parameters",
        )

    readings = sorted(readings_map.get(site_id, []), key=lambda r: r.timestamp)

    result = {
        "source": "usgs_charlotte",
        "station_id": site_id,
        "hours": hours,
        "params": param,
        "count": len(readings),
        "readings": [r.model_dump() for r in readings],
    }
    await cache.set(cache_key, result, ttl=config.CACHE_TTL)
    return result


# ===========================================================================
# 4. NC FIMAN — NC statewide precipitation via USGS NWIS
# ===========================================================================

@router.get("/nc_rain/stations", summary="North Carolina precipitation stations (USGS NWIS / NC FIMAN)")
async def nc_rain_stations(
    limit: int = Query(500, ge=1, le=5000, description="Maximum number of stations to return"),
):
    """Return active precipitation-monitoring USGS sites in North Carolina.

    These stations form the backbone of the NC FIMAN (Flood Inundation Mapping
    and Alert Network) real-time rainfall dataset. Data is served through the
    USGS NWIS Instantaneous Values API filtered to NC and parameter 00045
    (precipitation accumulation).

    Source: https://waterservices.usgs.gov/nwis/iv/
    """
    cache_key = f"eastcoast:nc_rain:stations:{limit}"
    cached_result = await cache.get(cache_key)
    if cached_result is not None:
        return cached_result

    query_params = {
        "format": "json",
        "stateCd": "NC",
        "parameterCd": "00045",
        "siteStatus": "active",
    }

    try:
        async with _client() as client:
            resp = await client.get(NWIS_IV_BASE, params=query_params)
            resp.raise_for_status()
            data = resp.json()
    except httpx.TimeoutException:
        raise HTTPException(status_code=504, detail="USGS NWIS API timeout (NC rain)")
    except httpx.HTTPStatusError as exc:
        raise HTTPException(
            status_code=exc.response.status_code,
            detail=f"USGS NWIS error (NC rain): {exc}",
        )
    except Exception as exc:
        logger.exception("Error fetching NC rain stations")
        raise HTTPException(status_code=500, detail=str(exc))

    stations, readings_map = _parse_usgs_response(data, "usgs_nc_rain", "NC FIMAN")
    stations = stations[:limit]

    station_list = []
    for s in stations:
        latest: dict[str, Reading] = {}
        for r in readings_map.get(s.id, []):
            if r.parameter not in latest or r.timestamp > latest[r.parameter].timestamp:
                latest[r.parameter] = r
        entry = s.model_dump()
        entry["latest_readings"] = [r.model_dump() for r in latest.values()]
        station_list.append(entry)

    result = {
        "source": "usgs_nc_rain",
        "network": "NC FIMAN",
        "state": "North Carolina",
        "description": "NC statewide precipitation gauges (USGS NWIS parameter 00045) — NC FIMAN backbone",
        "count": len(station_list),
        "stations": station_list,
    }
    await cache.set(cache_key, result, ttl=config.CACHE_TTL_LONG)
    return result


# ===========================================================================
# 5. Florida FAWN (Automated Weather Network)
# ===========================================================================

# FAWN parameter field name → (human name, unit)
_FAWN_FIELD_MAP: dict[str, tuple[str, str]] = {
    "AirTemp1": ("air_temperature_1m",      "°F"),
    "AirTemp2": ("air_temperature_2m",      "°F"),
    "AirTemp9": ("air_temperature_9m",      "°F"),
    "AirTemp": ("air_temperature",          "°F"),
    "DewPoint": ("dew_point",               "°F"),
    "RelHum": ("relative_humidity",         "%"),
    "WndSpd2": ("wind_speed_2m",            "mph"),
    "WndSpd10": ("wind_speed_10m",          "mph"),
    "WndSpd": ("wind_speed",                "mph"),
    "WndDir": ("wind_direction",            "deg"),
    "WndGust": ("wind_gust",                "mph"),
    "SoilTmp4": ("soil_temperature_4in",    "°F"),
    "SoilTmp8": ("soil_temperature_8in",    "°F"),
    "SoilTmp20": ("soil_temperature_20in",  "°F"),
    "SoilTmp40": ("soil_temperature_40in",  "°F"),
    "SoilTmp": ("soil_temperature",         "°F"),
    "Rain": ("precipitation",               "in"),
    "SR": ("solar_radiation",               "W/m²"),
    "ETo": ("evapotranspiration",           "in"),
    "NetRad": ("net_radiation",             "W/m²"),
    "Pressure": ("pressure",                "mb"),
    "BatVolt": ("battery_voltage",          "V"),
}

# Known FAWN station IDs that occasionally appear in the response.
# The API returns all stations in one payload — we just parse whatever arrives.

def _parse_fawn_observation(obs: dict) -> tuple[Optional[Station], list[Reading]]:
    """Parse a single FAWN observation record into a Station and Readings."""
    station_id = str(obs.get("StationId", obs.get("stationId", obs.get("id", ""))))
    if not station_id:
        return None, []

    name = str(obs.get("StationName", obs.get("stationName", obs.get("name", station_id)))).strip()

    try:
        lat = float(obs.get("Latitude", obs.get("latitude", 0.0)))
    except (TypeError, ValueError):
        lat = 0.0
    try:
        lon = float(obs.get("Longitude", obs.get("longitude", 0.0)))
    except (TypeError, ValueError):
        lon = 0.0

    # Parse observation timestamp
    timestamp: Optional[datetime] = None
    for ts_key in ("DateTime", "dateTime", "ObsDateTime", "obsDateTime", "Timestamp"):
        ts_raw = obs.get(ts_key)
        if ts_raw:
            try:
                timestamp = datetime.fromisoformat(str(ts_raw).replace("Z", "+00:00"))
            except ValueError:
                pass
            break
    if timestamp is None:
        timestamp = datetime.now(timezone.utc)

    # Extra metadata
    skip_keys = {
        "StationId", "stationId", "id", "StationName", "stationName", "name",
        "Latitude", "latitude", "Longitude", "longitude",
        "DateTime", "dateTime", "ObsDateTime", "obsDateTime", "Timestamp",
    }

    extra = {}
    for k, v in obs.items():
        if k not in skip_keys and not isinstance(v, (dict, list)):
            # Keep non-measurement scalar fields
            try:
                float(v)  # If it floats, it's probably a measurement — skip here
            except (TypeError, ValueError):
                extra[k] = v

    readings: list[Reading] = []
    params: list[str] = []
    seen: set[str] = set()

    for field, (param_name, unit) in _FAWN_FIELD_MAP.items():
        raw_val = obs.get(field)
        if raw_val is None:
            continue
        try:
            value = float(raw_val)
        except (TypeError, ValueError):
            continue
        # Skip obviously invalid sentinel values
        if value in (-9999.0, -999.0, 9999.0):
            continue
        if param_name not in seen:
            seen.add(param_name)
            params.append(param_name)
        readings.append(
            Reading(
                station_id=f"fawn_{station_id}",
                timestamp=timestamp,
                parameter=param_name,
                value=value,
                unit=unit,
                source="fawn",
            )
        )

    station = Station(
        id=f"fawn_{station_id}",
        name=f"FAWN {name}",
        lat=lat,
        lon=lon,
        source="fawn",
        network="Florida FAWN",
        params=params,
        last_update=timestamp,
        url=f"https://fawn.ifas.ufl.edu/data/reports/?report=hourly&station={station_id}",
        extra=extra,
    )
    return station, readings


@router.get("/fawn/stations", summary="Florida FAWN automated weather stations with latest observations")
async def fawn_stations():
    """Return all Florida FAWN (Automated Weather Network) stations with latest observations.

    The FAWN network consists of 42 stations across Florida operated by the
    University of Florida IFAS. Each station reports air temperature at multiple
    heights, dew point, relative humidity, wind speed/direction/gust,
    soil temperature, solar radiation, precipitation, and evapotranspiration.

    Source: https://fawn.ifas.ufl.edu/controller.php/lastObs/
    """
    cache_key = "eastcoast:fawn:stations"
    cached_result = await cache.get(cache_key)
    if cached_result is not None:
        return cached_result

    try:
        async with _client() as client:
            resp = await client.get(FAWN_URL)
            resp.raise_for_status()
            data = resp.json()
    except httpx.TimeoutException:
        raise HTTPException(status_code=504, detail="Florida FAWN API timeout")
    except httpx.HTTPStatusError as exc:
        raise HTTPException(
            status_code=exc.response.status_code,
            detail=f"Florida FAWN API error: {exc}",
        )
    except Exception as exc:
        logger.exception("Unexpected error fetching FAWN stations")
        raise HTTPException(status_code=500, detail=str(exc))

    # FAWN returns either a list of observations or a dict with a data wrapper
    if isinstance(data, list):
        obs_list = data
    elif isinstance(data, dict):
        for key in ("data", "observations", "stations", "results", "lastObs"):
            if key in data and isinstance(data[key], list):
                obs_list = data[key]
                break
        else:
            # Try treating dict values as records
            vals = list(data.values())
            if vals and isinstance(vals[0], dict):
                obs_list = vals
            else:
                obs_list = []
    else:
        obs_list = []

    station_list = []
    for obs in obs_list:
        if not isinstance(obs, dict):
            continue
        try:
            station, readings = _parse_fawn_observation(obs)
        except Exception:
            logger.debug("Failed to parse FAWN obs: %s", obs, exc_info=True)
            continue
        if station is None:
            continue
        entry = station.model_dump()
        entry["latest_readings"] = [r.model_dump() for r in readings]
        station_list.append(entry)

    result = {
        "source": "fawn",
        "network": "Florida FAWN",
        "operator": "University of Florida IFAS",
        "description": "UF/IFAS Florida Automated Weather Network — 42 stations statewide",
        "count": len(station_list),
        "stations": station_list,
    }
    await cache.set(cache_key, result, ttl=config.CACHE_TTL)
    return result


# ===========================================================================
# 6. Overview
# ===========================================================================

@router.get("/overview", summary="Summary of all East Coast network sources")
async def overview():
    """Return a descriptive summary of all East Coast and Southeast data sources
    available through this router.

    Each entry includes the network name, geographic coverage, what parameters
    are measured, update frequency, authentication requirements, and the
    relevant API endpoints.
    """
    return {
        "router": "eastcoast",
        "description": SOURCE_DESCRIPTION,
        "networks": [
            {
                "id": "maryland_chart",
                "name": "Maryland CHART RWIS",
                "operator": "Maryland State Highway Administration",
                "coverage": "Maryland statewide",
                "auth_required": False,
                "update_frequency": "~5 min",
                "params": [
                    "air_temperature", "dew_point", "relative_humidity",
                    "wind_speed", "wind_direction", "wind_gust",
                    "visibility", "precipitation_rate", "pavement_temperature",
                    "pressure",
                ],
                "category": "road_weather",
                "upstream_url": CHART_URL,
                "endpoints": {
                    "stations": "/api/eastcoast/maryland_chart/stations",
                    "station_detail": "/api/eastcoast/maryland_chart/stations/{station_id}",
                },
                "notes": "Clean, no-auth JSON API. Returns all stations in one call.",
            },
            {
                "id": "sfwmd",
                "name": "South Florida Water Management District (SFWMD)",
                "operator": "SFWMD",
                "coverage": "South Florida (16 counties)",
                "auth_required": False,
                "update_frequency": "15–60 min depending on gauge type",
                "params": ["water_level", "stage", "groundwater_depth", "precipitation"],
                "category": "water",
                "upstream_url": SFWMD_BASE,
                "endpoints": {
                    "stations": "/api/eastcoast/sfwmd/stations?layer=wells|rain|stage",
                    "station_detail": "/api/eastcoast/sfwmd/stations/{station_id}",
                },
                "notes": (
                    "Served via ArcGIS REST FeatureServer. Three layers: wells, rain, stage. "
                    "Paginated automatically."
                ),
            },
            {
                "id": "charlotte",
                "name": "Charlotte-Mecklenburg FINS",
                "operator": "Charlotte-Mecklenburg Storm Water Services / USGS",
                "coverage": "Mecklenburg County, NC",
                "auth_required": False,
                "update_frequency": "15 min",
                "params": ["precipitation", "gage_height", "discharge"],
                "category": "water",
                "upstream_url": NWIS_IV_BASE,
                "endpoints": {
                    "stations": "/api/eastcoast/charlotte/stations",
                    "readings": "/api/eastcoast/charlotte/stations/{site_id}/readings",
                },
                "notes": "Queries USGS NWIS for Mecklenburg County FIPS 37119.",
            },
            {
                "id": "nc_rain",
                "name": "NC FIMAN Precipitation Gauges",
                "operator": "NC Emergency Management / USGS",
                "coverage": "North Carolina statewide",
                "auth_required": False,
                "update_frequency": "15 min",
                "params": ["precipitation"],
                "category": "water",
                "upstream_url": NWIS_IV_BASE,
                "endpoints": {
                    "stations": "/api/eastcoast/nc_rain/stations",
                },
                "notes": (
                    "USGS NWIS stateCd=NC parameterCd=00045. Forms the backbone "
                    "of the NC FIMAN flood alert network."
                ),
            },
            {
                "id": "fawn",
                "name": "Florida FAWN",
                "operator": "University of Florida IFAS",
                "coverage": "Florida statewide (42 stations)",
                "auth_required": False,
                "update_frequency": "1 hour",
                "params": [
                    "air_temperature", "dew_point", "relative_humidity",
                    "wind_speed", "wind_direction", "wind_gust",
                    "soil_temperature", "solar_radiation", "precipitation",
                    "evapotranspiration", "net_radiation", "pressure",
                ],
                "category": "weather,agriculture",
                "upstream_url": FAWN_URL,
                "endpoints": {
                    "stations": "/api/eastcoast/fawn/stations",
                },
                "notes": (
                    "Single-call JSON API returning latest obs for all 42 stations. "
                    "Primarily designed for agricultural users."
                ),
            },
        ],
    }
