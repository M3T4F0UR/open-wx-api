"""State DOT Road Weather Information Systems (RWIS) via ArcGIS / public REST APIs.

Multiple state DOTs publish real-time road weather data through ArcGIS
FeatureServer endpoints or similar public REST APIs.  This module aggregates:

1. **Iowa DOT RWIS Atmospheric** — 10-min air weather (temp, humidity, dewpoint,
   wind, precipitation type, visibility) from ~100 RWIS stations.
2. **Iowa DOT RWIS Surface** — 10-min pavement conditions (surface temp, surface
   condition, ice percentage, freeze temp).
3. **Iowa DOT AWOS** — Airport weather observations (~41 fields).
4. **Maryland DOT RWIS** — Air + pavement + road condition data via Azure-hosted
   Koop service.
5. **WSDOT Weather** — Washington State DOT weather stations (~10-min updates).

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

SOURCE_NAME = "State DOT Road Weather (ArcGIS)"
SOURCE_DESCRIPTION = (
    "Road Weather Information System (RWIS) data from multiple state DOTs "
    "published via ArcGIS FeatureServer endpoints.  Includes Iowa DOT "
    "(atmospheric + pavement + AWOS), Maryland SHA RWIS, and WSDOT weather "
    "stations.  10-minute update frequency, no authentication required."
)
SOURCE_META = {
    "url": "https://services.arcgis.com/8lRhdTsQyJpO52F1/arcgis/rest/services",
    "auth_required": False,
    "update_frequency": "10 min",
    "coverage": "Iowa, Maryland, Washington State",
    "params": [
        "air_temperature", "relative_humidity", "dewpoint",
        "wind_speed", "wind_direction", "precipitation_type", "visibility",
        "surface_temperature", "surface_condition", "ice_percentage",
        "freeze_temperature",
    ],
    "category": "weather",
}

# ---------------------------------------------------------------------------
# Router
# ---------------------------------------------------------------------------

router = APIRouter(prefix="/api/dot_rwis", tags=["State DOT Road Weather (ArcGIS)"])

# ---------------------------------------------------------------------------
# Constants — upstream ArcGIS FeatureServer URLs
# ---------------------------------------------------------------------------

_IOWA_ATMOS_URL = (
    "https://services.arcgis.com/8lRhdTsQyJpO52F1/arcgis/rest/services/"
    "RWIS_Atmospheric_Data_View/FeatureServer/0/query"
)
_IOWA_SURFACE_URL = (
    "https://services.arcgis.com/8lRhdTsQyJpO52F1/arcgis/rest/services/"
    "RWIS_Surface_Data_View/FeatureServer/0/query"
)
_IOWA_AWOS_URL = (
    "https://services.arcgis.com/8lRhdTsQyJpO52F1/arcgis/rest/services/"
    "AWOS_Data_View/FeatureServer/0/query"
)
_MARYLAND_URL = (
    "https://app-storm-sha-prod.azurewebsites.net/koop/rwis/rest/services/"
    "rwis/FeatureServer/0/query"
)
_WSDOT_URL = (
    "https://data.wsdot.wa.gov/arcgis/rest/services/"
    "TravelInformation/TravelInfoCamerasWeather/FeatureServer/1/query"
)

_SOURCE = "dot_rwis_arcgis"

# ---------------------------------------------------------------------------
# Field mappings: upstream field name -> (normalised param, unit)
# ---------------------------------------------------------------------------

_IOWA_ATMOS_FIELDS: dict[str, tuple[str, str]] = {
    "AIR_TEMP":            ("air_temperature",    "°F"),
    "RELATIVE_HUMIDITY":   ("relative_humidity",  "%"),
    "DEW_POINT":           ("dewpoint",           "°F"),
    "AVG_WINDSPEED_MPH":   ("wind_speed",         "mph"),
    "MAX_WINDSPEED_MPH":   ("wind_gust",          "mph"),
    "WIND_DIRECTION_DEG":  ("wind_direction",     "°"),
    "PRECIPITATION_TYPE":  ("precipitation_type", ""),
    "VISIBILITY":          ("visibility",         "mi"),
}

_IOWA_SURFACE_FIELDS: dict[str, tuple[str, str]] = {
    "SURFACE_TEMP":      ("surface_temperature",  "°F"),
    "SURFACE_CONDITION":  ("surface_condition",    ""),
    "ICE_PERCENTAGE":     ("ice_percentage",       "%"),
    "FREEZE_TEMP":        ("freeze_temperature",   "°F"),
}

# AWOS field mapping — Iowa DOT AWOS uses slightly different field names
_IOWA_AWOS_FIELDS: dict[str, tuple[str, str]] = {
    "AIR_TEMP":            ("air_temperature",    "°F"),
    "DEW_POINT":           ("dewpoint",           "°F"),
    "PRESSURE":            ("pressure",           "inHg"),
    "WIND_SPEED_MPH":      ("wind_speed",         "mph"),
    "AVG_WINDSPEED_MPH":   ("wind_speed_avg",     "mph"),
    "WIND_DIRECTION_DEG":  ("wind_direction",     "°"),
    "VISIBILITY":          ("visibility",          "mi"),
    "CEILING":             ("ceiling",             "ft"),
    "FLIGHT_CATEGORY":     ("flight_category",     ""),
    "PRECIPITATION_TYPE":  ("precipitation_type",  ""),
}

_MARYLAND_FIELDS: dict[str, tuple[str, str]] = {
    "AirTemperature":        ("air_temperature",      "°F"),
    "RelativeHumidity":      ("relative_humidity",    "%"),
    "DewPoint":              ("dewpoint",             "°F"),
    "AvgWindSpeed":          ("wind_speed",           "mph"),
    "WindGust":              ("wind_gust",            "mph"),
    "WindDirection":         ("wind_direction",       "°"),
    "MaxSurfaceTemperature": ("surface_temperature",  "°F"),
    "RoadCondition":         ("surface_condition",    ""),
    "Visibility":            ("visibility",           "mi"),
    "PrecipiationType":      ("precipitation_type",   ""),
}

_WSDOT_FIELDS: dict[str, tuple[str, str]] = {
    "TemperatureFarhenheit":    ("air_temperature",    "°F"),
    "TemperatureCelcius":       ("air_temperature_c",  "°C"),
    "SurfaceTemperature":       ("surface_temperature",""),
    "WindSpeed":                ("wind_speed",         ""),
    "CardinalCompassDirection": ("wind_direction",     ""),
    "Visibility":               ("visibility",         "mi"),
}


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

async def _fetch_arcgis(url: str, params: Optional[dict] = None) -> Any:
    """Fetch JSON from an ArcGIS FeatureServer query endpoint."""
    default_params = {
        "where": "1=1",
        "outFields": "*",
        "f": "json",
        "outSR": "4326",          # Always request WGS84 coordinates
        "returnGeometry": "true",
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

    # Determine which parameters have values
    params: list[str] = []
    for raw_key, (param_name, _unit) in field_map.items():
        val = attrs.get(raw_key)
        if val is not None and str(val).strip() not in ("", "None", "null"):
            params.append(param_name)

    last_update = None
    if ts_field:
        last_update = _parse_esri_ts(attrs.get(ts_field))
    # Fallback: try common ArcGIS timestamp fields if primary didn't work
    if last_update is None:
        for fallback_key in ("DATA_LAST_UPDATED", "DATE_UPDATED",
                             "ReportTime", "ObservationTime",
                             "WeatherReportDateTime", "ReadingDateTime",
                             "COLLECTION_TIME", "REST_LAST_UPDATED"):
            val = attrs.get(fallback_key)
            if val is not None:
                last_update = _parse_esri_ts(val)
                if last_update is not None:
                    break

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
        # Try numeric conversion; keep strings for categorical fields (e.g. surface_condition)
        try:
            value = float(raw_val)
        except (TypeError, ValueError):
            # For non-numeric fields, store as value=0 with quality holding the text
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
    result_offset: int = 0,
    result_record_count: int = 5000,
) -> list[StationDetail]:
    """Fetch an ArcGIS FeatureServer layer, parse into StationDetail list."""
    if cache_key:
        cached_val = await cache.get(cache_key)
        if cached_val is not None:
            return cached_val

    params = {
        "where": "1=1",
        "outFields": "*",
        "f": "json",
        "resultOffset": str(result_offset),
        "resultRecordCount": str(result_record_count),
    }
    data = await _fetch_arcgis(url, params)

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


# ---------------------------------------------------------------------------
# Endpoints — Iowa DOT
# ---------------------------------------------------------------------------

@router.get("/iowa/atmospheric")
async def iowa_atmospheric(
    limit: int = Query(5000, ge=1, le=10000, description="Max features to return"),
):
    """Iowa DOT RWIS atmospheric data — air temp, humidity, dewpoint, wind,
    precipitation type, visibility.  Updates every ~10 minutes."""
    results = await _fetch_and_parse(
        url=_IOWA_ATMOS_URL,
        id_field="RPUID",
        name_field="RPUID_NAME",
        network="iowa_rwis_atmospheric",
        field_map=_IOWA_ATMOS_FIELDS,
        ts_field="DATA_LAST_UPDATED",
        cache_key="dot_rwis:iowa:atmospheric",
        cache_ttl=300,
        result_record_count=limit,
    )
    return {
        "source": _SOURCE,
        "network": "iowa_rwis_atmospheric",
        "station_count": len(results),
        "stations": [r.station.model_dump() for r in results],
        "readings": [rdg.model_dump() for sd in results for rdg in sd.readings],
    }


@router.get("/iowa/surface")
async def iowa_surface(
    limit: int = Query(5000, ge=1, le=10000, description="Max features to return"),
):
    """Iowa DOT RWIS surface/pavement data — surface temp, condition, ice
    percentage, freeze temp.  Updates every ~10 minutes."""
    results = await _fetch_and_parse(
        url=_IOWA_SURFACE_URL,
        id_field="RPUID",
        name_field="RPUID_NAME",
        network="iowa_rwis_surface",
        field_map=_IOWA_SURFACE_FIELDS,
        ts_field="DATA_LAST_UPDATED",
        cache_key="dot_rwis:iowa:surface",
        cache_ttl=300,
        result_record_count=limit,
    )
    return {
        "source": _SOURCE,
        "network": "iowa_rwis_surface",
        "station_count": len(results),
        "stations": [r.station.model_dump() for r in results],
        "readings": [rdg.model_dump() for sd in results for rdg in sd.readings],
    }


@router.get("/iowa/awos")
async def iowa_awos(
    limit: int = Query(5000, ge=1, le=10000, description="Max features to return"),
):
    """Iowa DOT AWOS airport weather stations — air temp, humidity, dewpoint,
    wind, visibility, altimeter, ceiling, weather conditions."""
    results = await _fetch_and_parse(
        url=_IOWA_AWOS_URL,
        id_field="AWOS_ID",
        name_field="AIRPORT_NAME",
        network="iowa_awos",
        field_map=_IOWA_AWOS_FIELDS,
        ts_field="DATA_LAST_UPDATED",
        cache_key="dot_rwis:iowa:awos",
        cache_ttl=300,
        result_record_count=limit,
    )
    return {
        "source": _SOURCE,
        "network": "iowa_awos",
        "station_count": len(results),
        "stations": [r.station.model_dump() for r in results],
        "readings": [rdg.model_dump() for sd in results for rdg in sd.readings],
    }


# ---------------------------------------------------------------------------
# Endpoints — Maryland DOT
# ---------------------------------------------------------------------------

@router.get("/maryland")
async def maryland(
    limit: int = Query(5000, ge=1, le=10000, description="Max features to return"),
):
    """Maryland SHA RWIS — air + pavement + road condition data from the
    Maryland State Highway Administration's Koop-based REST API."""
    results = await _fetch_and_parse(
        url=_MARYLAND_URL,
        id_field="station_id",
        name_field="NAME",
        network="maryland_rwis",
        field_map=_MARYLAND_FIELDS,
        ts_field="ReportTime",
        cache_key="dot_rwis:maryland",
        cache_ttl=300,
        result_record_count=limit,
    )
    return {
        "source": _SOURCE,
        "network": "maryland_rwis",
        "station_count": len(results),
        "stations": [r.station.model_dump() for r in results],
        "readings": [rdg.model_dump() for sd in results for rdg in sd.readings],
    }


# ---------------------------------------------------------------------------
# Endpoints — WSDOT (Washington)
# ---------------------------------------------------------------------------

@router.get("/washington")
async def washington(
    limit: int = Query(5000, ge=1, le=10000, description="Max features to return"),
):
    """WSDOT weather stations — temperature, humidity, wind, pressure,
    visibility, precipitation.  Updates every ~10 minutes."""
    results = await _fetch_and_parse(
        url=_WSDOT_URL,
        id_field="OBJECTID",
        name_field="WeatherStationDescription",
        network="wsdot_weather",
        field_map=_WSDOT_FIELDS,
        ts_field="WeatherReportDateTime",
        cache_key="dot_rwis:washington",
        cache_ttl=300,
        result_record_count=limit,
    )
    return {
        "source": _SOURCE,
        "network": "wsdot_weather",
        "station_count": len(results),
        "stations": [r.station.model_dump() for r in results],
        "readings": [rdg.model_dump() for sd in results for rdg in sd.readings],
    }


# ---------------------------------------------------------------------------
# Overview
# ---------------------------------------------------------------------------

@router.get("/overview")
async def overview():
    """Return metadata about this data source, including available networks,
    coverage, parameters, and endpoint documentation."""
    return {
        "source": _SOURCE,
        "name": SOURCE_NAME,
        "description": SOURCE_DESCRIPTION,
        "auth_required": False,
        "update_frequency": "~10 min",
        "coverage": {
            "iowa_rwis_atmospheric": "Iowa (~100 RWIS stations, air weather)",
            "iowa_rwis_surface": "Iowa (~100 RWIS stations, pavement conditions)",
            "iowa_awos": "Iowa (AWOS airport weather stations)",
            "maryland_rwis": "Maryland (SHA RWIS road weather)",
            "wsdot_weather": "Washington State (DOT weather stations)",
        },
        "parameters": SOURCE_META["params"],
        "endpoints": {
            "iowa_atmospheric": "/api/dot_rwis/iowa/atmospheric",
            "iowa_surface": "/api/dot_rwis/iowa/surface",
            "iowa_awos": "/api/dot_rwis/iowa/awos",
            "maryland": "/api/dot_rwis/maryland",
            "washington": "/api/dot_rwis/washington",
            "overview": "/api/dot_rwis/overview",
        },
        "upstream_apis": {
            "iowa_atmospheric": _IOWA_ATMOS_URL,
            "iowa_surface": _IOWA_SURFACE_URL,
            "iowa_awos": _IOWA_AWOS_URL,
            "maryland": _MARYLAND_URL,
            "wsdot": _WSDOT_URL,
        },
        "notes": (
            "All upstream APIs are unauthenticated ArcGIS FeatureServer or "
            "compatible REST endpoints.  Iowa DOT fields are in imperial units.  "
            "Maryland data is served through a Koop/Azure intermediary.  "
            "WSDOT data comes from the TravelInformation service."
        ),
    }
