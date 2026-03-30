"""Real-Time ArcGIS Monitoring — curated live sensor feeds, no auth required.

Wraps the best publicly accessible ArcGIS FeatureServer endpoints discovered
through systematic research.  All are unauthenticated, return GeoJSON-style
features, and update on sub-hourly cadences (typically 5–30 minutes).

Networks included:

1. **NC FIMAN** — 876 real-time flood gauges across North Carolina (67 fields,
   5-min updates).  Rain totals (1/6/12/24 hr), condition, trend, county, WFO.
2. **SARA San Antonio Telemetry** — 41 dam/reservoir stations with water level,
   percent filled, and rainfall accumulations (1/3/6 hr).
3. **SARA Bexar County Flood** — 220 real-time flood sensors in the San Antonio
   River Authority network.
4. **Fort Collins Rain Gauges** — ~30 tipping-bucket rain gauges with 7 time-
   window accumulations (5-min to 24-hr totals).
5. **Fort Collins Stream Gauges** — 41 stations with stage, flow, and alarm
   level data for Poudre River tributaries.
6. **Illinois DOT RWIS** — 37 road weather stations, live atmospheric and
   pavement data.
7. **Georgia DOT RWIS** — 57 stations, 30-min updates, full road weather suite.
8. **OC Public Works Rainfall** — 66 rain gauges in Orange County CA, updating
   every 8–12 minutes.
9. **Salt Lake County Stream Gauges** — 25 stations with stage/flow for Jordan
   River and mountain-front drainages.

All endpoints are unauthenticated.
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Optional

from fastapi import APIRouter, HTTPException, Query

import cache
import http_client
from models import Reading, Station, StationDetail

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Source registry metadata  (read by server.py /api/sources)
# ---------------------------------------------------------------------------

SOURCE_NAME = "Real-Time ArcGIS Monitoring"
SOURCE_DESCRIPTION = (
    "Curated collection of live, unauthenticated ArcGIS FeatureServer sensor "
    "feeds: NC FIMAN flood gauges (876 stations), SARA San Antonio telemetry "
    "(41 dams + 220 flood sensors), Fort Collins rain/stream gauges, Illinois "
    "& Georgia DOT RWIS, Orange County CA rainfall, and Salt Lake County "
    "stream gauges.  Update frequencies range from 5 to 30 minutes."
)
SOURCE_META = {
    "url": "https://espwebapps.com/arcgis/rest/services/NCFIMAN",
    "auth_required": False,
    "update_frequency": "5–30 min",
    "coverage": "NC, TX (San Antonio), CO (Fort Collins), IL, GA, CA (Orange County), UT (Salt Lake County)",
    "params": [
        "precipitation", "rainfall_1hr", "rainfall_6hr", "rainfall_12hr",
        "rainfall_24hr", "stage", "flow", "water_level", "percent_filled",
        "air_temperature", "relative_humidity", "wind_speed", "wind_direction",
        "surface_temperature", "visibility",
    ],
    "category": "water",
}

# ---------------------------------------------------------------------------
# Router
# ---------------------------------------------------------------------------

router = APIRouter(prefix="/api/rt_arcgis", tags=["Real-Time ArcGIS Monitoring"])

# ---------------------------------------------------------------------------
# Constants — upstream ArcGIS FeatureServer URLs
# ---------------------------------------------------------------------------

_SOURCE = "arcgis_realtime"

_NC_FIMAN_URL = (
    "https://espwebapps.com/arcgis/rest/services/NCFIMAN/"
    "GAGES_ALL/FeatureServer/0/query"
)
_SARA_TELEMETRY_URL = (
    "https://gis.sara-tx.org/ags1/rest/services/Flood/"
    "Telemetry_Stations/FeatureServer/0/query"
)
_SARA_FLOOD_URL = (
    "https://gis.sara-tx.org/ags1/rest/services/Flood/"
    "vwBexarFlood/FeatureServer/0/query"
)
_FORTCOLLINS_RAIN_URL = (
    "https://gisweb.fcgov.com/arcgis/rest/services/"
    "RainWarning/FeatureServer/7/query"
)
_FORTCOLLINS_STREAMS_URL = (
    "https://gisweb.fcgov.com/arcgis/rest/services/"
    "StreamWarning/MapServer/0/query"
)
_IDOT_RWIS_URL = (
    "https://services2.arcgis.com/aIrBD8yn1TDTEXoz/arcgis/rest/services/"
    "RWIS/FeatureServer/0/query"
)
_GDOT_RWIS_URL = (
    "https://services1.arcgis.com/2iUE8l8JKrP2tygQ/arcgis/rest/services/"
    "GDOT_Road_Weather_Stations/FeatureServer/0/query"
)
_OC_RAIN_URL = (
    "https://www.ocgis.com/arcpub/rest/services/Environmental_Resources/"
    "Current_Rainfall_Totals/FeatureServer/0/query"
)
_SLC_STREAMS_URL = (
    "https://apps.saltlakecounty.gov/slcogis/rest/services/Engineering/"
    "FloodControl/MapServer/3/query"
)

# ---------------------------------------------------------------------------
# Field mappings: upstream field name -> (normalised param, unit)
# ---------------------------------------------------------------------------

_NC_FIMAN_FIELDS: dict[str, tuple[str, str]] = {
    "CURRENT_RAIN":  ("precipitation",   "in"),
    "RAIN_1HR":      ("rainfall_1hr",    "in"),
    "RAIN_6HR":      ("rainfall_6hr",    "in"),
    "RAIN_12HR":     ("rainfall_12hr",   "in"),
    "RAIN_24HR":     ("rainfall_24hr",   "in"),
    "CONDITION":     ("condition",        ""),
    "TREND":         ("trend",            ""),
}

_SARA_TELEMETRY_FIELDS: dict[str, tuple[str, str]] = {
    "WaterLevel":              ("water_level",      "ft"),
    "PercentFilled":           ("percent_filled",   "%"),
    "AccumulationOneHour":     ("rainfall_1hr",     "in"),
    "AccumulationThreeHour":   ("rainfall_3hr",     "in"),
    "AccumulationSixHour":     ("rainfall_6hr",     "in"),
}

_SARA_FLOOD_FIELDS: dict[str, tuple[str, str]] = {
    "WaterLevel":    ("water_level",    "ft"),
    "RainFall":      ("precipitation",  "in"),
}

_FC_RAIN_FIELDS: dict[str, tuple[str, str]] = {
    "Rain5min":   ("rainfall_5min",  "in"),
    "Rain15min":  ("rainfall_15min", "in"),
    "Rain1hr":    ("rainfall_1hr",   "in"),
    "Rain3hr":    ("rainfall_3hr",   "in"),
    "Rain6hr":    ("rainfall_6hr",   "in"),
    "Rain12hr":   ("rainfall_12hr",  "in"),
    "Rain24hr":   ("rainfall_24hr",  "in"),
}

_FC_STREAM_FIELDS: dict[str, tuple[str, str]] = {
    "Stage":      ("stage",      "ft"),
    "Flow":       ("flow",       "cfs"),
    "AlarmLevel": ("alarm_level", ""),
}

_IDOT_RWIS_FIELDS: dict[str, tuple[str, str]] = {
    "AirTemp":            ("air_temperature",     "°F"),
    "RelativeHumidity":   ("relative_humidity",   "%"),
    "DewPoint":           ("dewpoint",            "°F"),
    "WindSpeed":          ("wind_speed",          "mph"),
    "WindDirection":      ("wind_direction",      "°"),
    "SurfaceTemp":        ("surface_temperature", "°F"),
    "Visibility":         ("visibility",          "mi"),
}

_GDOT_RWIS_FIELDS: dict[str, tuple[str, str]] = {
    "AirTemp":            ("air_temperature",     "°F"),
    "RelativeHumidity":   ("relative_humidity",   "%"),
    "DewPoint":           ("dewpoint",            "°F"),
    "WindSpeed":          ("wind_speed",          "mph"),
    "WindDirection":      ("wind_direction",      "°"),
    "SurfaceTemp":        ("surface_temperature", "°F"),
    "Visibility":         ("visibility",          "mi"),
    "PrecipType":         ("precipitation_type",  ""),
}

_OC_RAIN_FIELDS: dict[str, tuple[str, str]] = {
    "Rain1Hr":    ("rainfall_1hr",   "in"),
    "Rain3Hr":    ("rainfall_3hr",   "in"),
    "Rain6Hr":    ("rainfall_6hr",   "in"),
    "Rain12Hr":   ("rainfall_12hr",  "in"),
    "Rain24Hr":   ("rainfall_24hr",  "in"),
    "RainTotal":  ("precipitation",  "in"),
}

_SLC_STREAM_FIELDS: dict[str, tuple[str, str]] = {
    "Stage":       ("stage",  "ft"),
    "Flow":        ("flow",   "cfs"),
}

# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

async def _arcgis_query(
    url: str,
    params: Optional[dict] = None,
    result_record_count: int = 2000,
) -> Any:
    """Shared helper — fetch JSON from an ArcGIS FeatureServer query endpoint.

    Applies standard defaults (where=1=1, outFields=*, f=json) and merges any
    caller-supplied params on top.
    """
    default_params = {
        "where": "1=1",
        "outFields": "*",
        "resultRecordCount": str(result_record_count),
        "f": "json",
    }
    if params:
        default_params.update(params)
    return await http_client.fetch_json(url, params=default_params, source=_SOURCE)


def _parse_esri_ts(raw: Any) -> Optional[datetime]:
    """Parse an Esri epoch-millis timestamp (or ISO string) to UTC datetime."""
    if raw is None:
        return None
    if isinstance(raw, (int, float)):
        try:
            return datetime.fromtimestamp(raw / 1000, tz=timezone.utc)
        except (OSError, ValueError, OverflowError):
            return None
    if isinstance(raw, str):
        for fmt in ("%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%d %H:%M:%S"):
            try:
                dt = datetime.strptime(raw, fmt)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                return dt
            except ValueError:
                continue
    return None


def _extract_coords(feature: dict) -> tuple[float, float]:
    """Extract (lat, lon) from an ArcGIS feature, checking geometry and attributes."""
    geom = feature.get("geometry") or {}
    lat = geom.get("y") or geom.get("lat") or 0
    lon = geom.get("x") or geom.get("lon") or 0
    if lat == 0 and lon == 0:
        attrs = feature.get("attributes") or {}
        for lat_key in ("LATITUDE", "Latitude", "latitude", "LAT", "lat", "Y", "y"):
            if lat_key in attrs and attrs[lat_key] is not None:
                lat = float(attrs[lat_key])
                break
        for lon_key in ("LONGITUDE", "Longitude", "longitude", "LON", "lon", "X", "x"):
            if lon_key in attrs and attrs[lon_key] is not None:
                lon = float(attrs[lon_key])
                break
    return float(lat), float(lon)


def _build_station(
    feature: dict,
    id_field: str,
    name_field: str,
    network: str,
    field_map: dict[str, tuple[str, str]],
    ts_field: Optional[str] = None,
) -> Station:
    """Build a Station model from an ArcGIS feature."""
    attrs = feature.get("attributes") or {}
    lat, lon = _extract_coords(feature)

    station_id = str(attrs.get(id_field) or attrs.get("OBJECTID") or "")
    name = str(attrs.get(name_field) or station_id)

    params: list[str] = []
    for raw_key, (param_name, _unit) in field_map.items():
        val = attrs.get(raw_key)
        if val is not None and str(val).strip() not in ("", "None", "null"):
            params.append(param_name)

    last_update = None
    if ts_field:
        last_update = _parse_esri_ts(attrs.get(ts_field))

    return Station(
        id=station_id,
        name=name,
        lat=lat,
        lon=lon,
        source=_SOURCE,
        network=network,
        params=params,
        last_update=last_update,
        extra={k: v for k, v in attrs.items() if v is not None},
    )


def _build_readings(
    feature: dict,
    station_id: str,
    field_map: dict[str, tuple[str, str]],
    ts_field: Optional[str] = None,
) -> list[Reading]:
    """Build Reading models from an ArcGIS feature's attributes."""
    attrs = feature.get("attributes") or {}
    ts = None
    if ts_field:
        ts = _parse_esri_ts(attrs.get(ts_field))
    if ts is None:
        ts = datetime.now(timezone.utc)

    readings: list[Reading] = []
    for raw_key, (param_name, unit) in field_map.items():
        raw_val = attrs.get(raw_key)
        if raw_val is None:
            continue
        try:
            value = float(raw_val)
        except (TypeError, ValueError):
            readings.append(Reading(
                station_id=station_id,
                timestamp=ts,
                parameter=param_name,
                value=0.0,
                unit=unit,
                quality=str(raw_val),
                source=_SOURCE,
            ))
            continue
        readings.append(Reading(
            station_id=station_id,
            timestamp=ts,
            parameter=param_name,
            value=value,
            unit=unit,
            source=_SOURCE,
        ))
    return readings


async def _fetch_and_parse(
    url: str,
    id_field: str,
    name_field: str,
    network: str,
    field_map: dict[str, tuple[str, str]],
    ts_field: Optional[str] = None,
    cache_key: Optional[str] = None,
    cache_ttl: int = 300,
    result_record_count: int = 2000,
) -> list[StationDetail]:
    """Fetch an ArcGIS FeatureServer layer, parse into StationDetail list."""
    if cache_key:
        cached_val = await cache.get(cache_key)
        if cached_val is not None:
            return cached_val

    data = await _arcgis_query(url, result_record_count=result_record_count)

    features = data.get("features") or []
    if not features:
        logger.warning("No features returned from %s", url)
        return []

    results: list[StationDetail] = []
    for feat in features:
        station = _build_station(feat, id_field, name_field, network, field_map, ts_field)
        readings = _build_readings(feat, station.id, field_map, ts_field)
        results.append(StationDetail(station=station, readings=readings))

    if cache_key:
        await cache.set(cache_key, results, ttl=cache_ttl)

    return results


def _response(network: str, results: list[StationDetail]) -> dict:
    """Standard response envelope."""
    return {
        "source": _SOURCE,
        "network": network,
        "station_count": len(results),
        "stations": [r.station.model_dump() for r in results],
        "readings": [rdg.model_dump() for sd in results for rdg in sd.readings],
    }


# ---------------------------------------------------------------------------
# Endpoints — NC FIMAN
# ---------------------------------------------------------------------------

@router.get("/nc_fiman")
async def nc_fiman(
    limit: int = Query(2000, ge=1, le=5000, description="Max features to return"),
):
    """NC FIMAN — 876 real-time flood gauges across North Carolina.  67 fields
    including rain totals (1/6/12/24 hr), condition, trend, county, WFO.
    Updates every ~5 minutes.  No auth required."""
    results = await _fetch_and_parse(
        url=_NC_FIMAN_URL,
        id_field="GAGE_ID",
        name_field="GAGE_NAME",
        network="nc_fiman",
        field_map=_NC_FIMAN_FIELDS,
        ts_field="LAST_UPDATED",
        cache_key="rt_arcgis:nc_fiman",
        cache_ttl=300,
        result_record_count=limit,
    )
    return _response("nc_fiman", results)


# ---------------------------------------------------------------------------
# Endpoints — SARA (San Antonio River Authority)
# ---------------------------------------------------------------------------

@router.get("/sara_telemetry")
async def sara_telemetry(
    limit: int = Query(2000, ge=1, le=5000, description="Max features to return"),
):
    """SARA San Antonio Telemetry — 41 real-time dam/reservoir stations with
    water level, percent filled, and rainfall accumulations (1/3/6 hr).
    No auth required."""
    results = await _fetch_and_parse(
        url=_SARA_TELEMETRY_URL,
        id_field="name",
        name_field="name",
        network="sara_telemetry",
        field_map=_SARA_TELEMETRY_FIELDS,
        ts_field="LastUpdate",
        cache_key="rt_arcgis:sara_telemetry",
        cache_ttl=300,
        result_record_count=limit,
    )
    return _response("sara_telemetry", results)


@router.get("/sara_flood")
async def sara_flood(
    limit: int = Query(2000, ge=1, le=5000, description="Max features to return"),
):
    """SARA Bexar County Flood — 220 real-time flood sensors in the San Antonio
    River Authority network.  No auth required."""
    results = await _fetch_and_parse(
        url=_SARA_FLOOD_URL,
        id_field="SiteName",
        name_field="SiteName",
        network="sara_flood",
        field_map=_SARA_FLOOD_FIELDS,
        ts_field="LastUpdate",
        cache_key="rt_arcgis:sara_flood",
        cache_ttl=300,
        result_record_count=limit,
    )
    return _response("sara_flood", results)


# ---------------------------------------------------------------------------
# Endpoints — Fort Collins
# ---------------------------------------------------------------------------

@router.get("/fortcollins/rain")
async def fortcollins_rain(
    limit: int = Query(2000, ge=1, le=5000, description="Max features to return"),
):
    """Fort Collins Rain Gauges — ~30 tipping-bucket sensors with 7 time-window
    accumulations (5-min through 24-hr totals).  No auth required."""
    results = await _fetch_and_parse(
        url=_FORTCOLLINS_RAIN_URL,
        id_field="StationName",
        name_field="StationName",
        network="fortcollins_rain",
        field_map=_FC_RAIN_FIELDS,
        ts_field="LastUpdate",
        cache_key="rt_arcgis:fc_rain",
        cache_ttl=300,
        result_record_count=limit,
    )
    return _response("fortcollins_rain", results)


@router.get("/fortcollins/streams")
async def fortcollins_streams(
    limit: int = Query(2000, ge=1, le=5000, description="Max features to return"),
):
    """Fort Collins Stream Gauges — 41 stations with stage, flow, and alarm
    level data for Poudre River tributaries.  No auth required."""
    results = await _fetch_and_parse(
        url=_FORTCOLLINS_STREAMS_URL,
        id_field="StationName",
        name_field="StationName",
        network="fortcollins_streams",
        field_map=_FC_STREAM_FIELDS,
        ts_field="LastUpdate",
        cache_key="rt_arcgis:fc_streams",
        cache_ttl=300,
        result_record_count=limit,
    )
    return _response("fortcollins_streams", results)


# ---------------------------------------------------------------------------
# Endpoints — State DOT RWIS
# ---------------------------------------------------------------------------

@router.get("/idot_rwis")
async def idot_rwis(
    limit: int = Query(2000, ge=1, le=5000, description="Max features to return"),
):
    """Illinois DOT RWIS — 37 road weather stations with live atmospheric and
    pavement data.  No auth required."""
    results = await _fetch_and_parse(
        url=_IDOT_RWIS_URL,
        id_field="StationId",
        name_field="StationName",
        network="idot_rwis",
        field_map=_IDOT_RWIS_FIELDS,
        ts_field="ObservationTime",
        cache_key="rt_arcgis:idot_rwis",
        cache_ttl=300,
        result_record_count=limit,
    )
    return _response("idot_rwis", results)


@router.get("/gdot_rwis")
async def gdot_rwis(
    limit: int = Query(2000, ge=1, le=5000, description="Max features to return"),
):
    """Georgia DOT RWIS — 57 road weather stations, 30-min updates, full road
    weather suite including precipitation type.  No auth required."""
    results = await _fetch_and_parse(
        url=_GDOT_RWIS_URL,
        id_field="StationId",
        name_field="StationName",
        network="gdot_rwis",
        field_map=_GDOT_RWIS_FIELDS,
        ts_field="ObservationTime",
        cache_key="rt_arcgis:gdot_rwis",
        cache_ttl=300,
        result_record_count=limit,
    )
    return _response("gdot_rwis", results)


# ---------------------------------------------------------------------------
# Endpoints — Orange County CA
# ---------------------------------------------------------------------------

@router.get("/oc_rain")
async def oc_rain(
    limit: int = Query(2000, ge=1, le=5000, description="Max features to return"),
):
    """OC Public Works Rainfall — 66 rain gauges in Orange County CA, updating
    every 8–12 minutes with multi-window accumulations.  No auth required."""
    results = await _fetch_and_parse(
        url=_OC_RAIN_URL,
        id_field="StationName",
        name_field="StationName",
        network="oc_rain",
        field_map=_OC_RAIN_FIELDS,
        ts_field="LastUpdate",
        cache_key="rt_arcgis:oc_rain",
        cache_ttl=300,
        result_record_count=limit,
    )
    return _response("oc_rain", results)


# ---------------------------------------------------------------------------
# Endpoints — Salt Lake County
# ---------------------------------------------------------------------------

@router.get("/slc_streams")
async def slc_streams(
    limit: int = Query(2000, ge=1, le=5000, description="Max features to return"),
):
    """Salt Lake County Stream Gauges — 25 stations with stage and flow for
    Jordan River and mountain-front drainages.  No auth required."""
    results = await _fetch_and_parse(
        url=_SLC_STREAMS_URL,
        id_field="StationName",
        name_field="StationName",
        network="slc_streams",
        field_map=_SLC_STREAM_FIELDS,
        ts_field="LastUpdate",
        cache_key="rt_arcgis:slc_streams",
        cache_ttl=300,
        result_record_count=limit,
    )
    return _response("slc_streams", results)


# ---------------------------------------------------------------------------
# Overview
# ---------------------------------------------------------------------------

@router.get("/overview")
async def overview():
    """Return metadata about all real-time ArcGIS monitoring sources, including
    available networks, coverage, parameters, and endpoint documentation."""
    return {
        "source": _SOURCE,
        "name": SOURCE_NAME,
        "description": SOURCE_DESCRIPTION,
        "auth_required": False,
        "update_frequency": "5–30 min",
        "coverage": {
            "nc_fiman": "North Carolina (876 flood gauges, 5-min updates)",
            "sara_telemetry": "San Antonio TX (41 dam/reservoir stations)",
            "sara_flood": "Bexar County TX (220 flood sensors)",
            "fortcollins_rain": "Fort Collins CO (~30 rain gauges, 7 time windows)",
            "fortcollins_streams": "Fort Collins CO (41 stream gauges, stage/flow/alarm)",
            "idot_rwis": "Illinois (37 road weather stations)",
            "gdot_rwis": "Georgia (57 road weather stations, 30-min)",
            "oc_rain": "Orange County CA (66 rain gauges, 8-12 min)",
            "slc_streams": "Salt Lake County UT (25 stream gauges, stage/flow)",
        },
        "parameters": SOURCE_META["params"],
        "endpoints": {
            "nc_fiman": "/api/rt_arcgis/nc_fiman",
            "sara_telemetry": "/api/rt_arcgis/sara_telemetry",
            "sara_flood": "/api/rt_arcgis/sara_flood",
            "fortcollins_rain": "/api/rt_arcgis/fortcollins/rain",
            "fortcollins_streams": "/api/rt_arcgis/fortcollins/streams",
            "idot_rwis": "/api/rt_arcgis/idot_rwis",
            "gdot_rwis": "/api/rt_arcgis/gdot_rwis",
            "oc_rain": "/api/rt_arcgis/oc_rain",
            "slc_streams": "/api/rt_arcgis/slc_streams",
            "overview": "/api/rt_arcgis/overview",
        },
        "upstream_apis": {
            "nc_fiman": _NC_FIMAN_URL,
            "sara_telemetry": _SARA_TELEMETRY_URL,
            "sara_flood": _SARA_FLOOD_URL,
            "fortcollins_rain": _FORTCOLLINS_RAIN_URL,
            "fortcollins_streams": _FORTCOLLINS_STREAMS_URL,
            "idot_rwis": _IDOT_RWIS_URL,
            "gdot_rwis": _GDOT_RWIS_URL,
            "oc_rain": _OC_RAIN_URL,
            "slc_streams": _SLC_STREAMS_URL,
        },
    }
