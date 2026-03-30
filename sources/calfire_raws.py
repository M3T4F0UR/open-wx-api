"""CAL FIRE & NIFC RAWS — Fire Weather Observation Networks.

Two complementary ArcGIS FeatureServer sources, both free / no-auth:

1. **CAL FIRE RAWS Wind 2-D Public View** — 4,483 RAWS stations reporting
   10-minute observations of wind speed/direction/gust, air temperature,
   humidity, dew point, wet bulb, and peak wind.

2. **NIFC RAWS Fire Danger** — 946 stations with daily Energy Release
   Component (ERC) and Burning Index (BI) percentiles and trends.

ArcGIS caps ``resultRecordCount`` at 5,000 per request; this module
paginates with ``resultOffset`` to pull every record.
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Optional

import httpx
from fastapi import APIRouter, HTTPException, Path, Query

import cache
import config
from models import Reading, Station, StationDetail

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Source registry metadata  (read by server.py /api/sources)
# ---------------------------------------------------------------------------

SOURCE_NAME = "CAL FIRE & NIFC RAWS"
SOURCE_DESCRIPTION = (
    "CAL FIRE RAWS 10-minute wind/weather observations (~4,483 stations) "
    "and NIFC RAWS daily fire danger percentiles (ERC/BI, ~946 stations).  "
    "Both are free ArcGIS FeatureServer endpoints — no authentication required."
)
SOURCE_META = {
    "category": "fire_weather",
    "coverage": "United States (~5,400 stations combined)",
    "auth_required": False,
    "update_frequency": "10 min (obs) / daily (fire danger)",
    "params": [
        "wind_speed", "wind_direction", "wind_gust", "peak_wind",
        "air_temp", "relative_humidity", "dew_point", "wet_bulb",
        "erc", "erc_percentile", "bi", "bi_percentile",
    ],
    "urls": {
        "calfire_raws": "https://services5.arcgis.com/bz1uwWPKUInZBK94/arcgis/rest/services/RAWS_Wind_2D_Public_View/FeatureServer/0",
        "nifc_fire_danger": "https://services3.arcgis.com/T4QMspbfLg3qTGWY/arcgis/rest/services/RAWS_ERC_and_BI_Percentiles_and_Trends/FeatureServer/0",
    },
    "station_count": 5400,
}

# ---------------------------------------------------------------------------
# Router
# ---------------------------------------------------------------------------

router = APIRouter(prefix="/api/fire_weather", tags=["CAL FIRE & NIFC RAWS"])

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_CALFIRE_URL = (
    "https://services5.arcgis.com/bz1uwWPKUInZBK94/arcgis/rest/services/"
    "RAWS_Wind_2D_Public_View/FeatureServer/0/query"
)

_NIFC_URL = (
    "https://services3.arcgis.com/T4QMspbfLg3qTGWY/arcgis/rest/services/"
    "RAWS_ERC_and_BI_Percentiles_and_Trends/FeatureServer/0/query"
)

_PAGE_SIZE = 5000  # ArcGIS maximum resultRecordCount

# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------

_HEADERS = {
    "User-Agent": config.USER_AGENT,
    "Accept": "application/json",
}


def _client() -> httpx.AsyncClient:
    return httpx.AsyncClient(
        timeout=config.HTTP_TIMEOUT,
        headers=_HEADERS,
        follow_redirects=True,
    )


async def _arcgis_query(
    url: str,
    where: str = "1=1",
    out_fields: str = "*",
    extra_params: dict[str, Any] | None = None,
    max_records: int | None = None,
) -> list[dict]:
    """Paginate an ArcGIS FeatureServer query endpoint.

    Fetches pages of ``_PAGE_SIZE`` records using ``resultOffset`` until
    all features are returned (or *max_records* is reached).
    """
    all_features: list[dict] = []
    offset = 0
    params_base: dict[str, Any] = {
        "where": where,
        "outFields": out_fields,
        "resultRecordCount": _PAGE_SIZE,
        "f": "json",
    }
    if extra_params:
        params_base.update(extra_params)

    async with _client() as client:
        while True:
            params = {**params_base, "resultOffset": offset}
            resp = await client.get(url, params=params)
            resp.raise_for_status()
            body = resp.json()

            if "error" in body:
                raise HTTPException(
                    status_code=502,
                    detail=f"ArcGIS error: {body['error'].get('message', body['error'])}",
                )

            features = body.get("features", [])
            if not features:
                break

            all_features.extend(features)

            if max_records and len(all_features) >= max_records:
                all_features = all_features[:max_records]
                break

            # If we got fewer than a full page, we've reached the end
            if len(features) < _PAGE_SIZE:
                break

            offset += len(features)

    return all_features


# ---------------------------------------------------------------------------
# Parsing helpers — CAL FIRE RAWS
# ---------------------------------------------------------------------------

def _epoch_ms_to_dt(ms: Any) -> datetime | None:
    """Convert ArcGIS epoch-milliseconds to a UTC datetime, or None."""
    if ms is None:
        return None
    try:
        return datetime.fromtimestamp(int(ms) / 1000, tz=timezone.utc)
    except (ValueError, TypeError, OSError):
        return None


def _safe_float(val: Any) -> float | None:
    if val is None:
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


def _parse_calfire_station(feat: dict) -> StationDetail | None:
    """Parse one CAL FIRE ArcGIS feature into Station + Readings."""
    attr = feat.get("attributes", {})
    geom = feat.get("geometry", {})

    station_name = attr.get("StationName") or attr.get("station_name") or "Unknown"
    station_id = str(attr.get("StationID") or attr.get("station_id") or attr.get("OBJECTID", ""))
    if not station_id:
        return None

    lat = _safe_float(geom.get("y") or attr.get("latitude"))
    lon = _safe_float(geom.get("x") or attr.get("longitude"))
    if lat is None or lon is None:
        return None

    # ArcGIS field for observation time — try multiple field name conventions
    obs_time = _epoch_ms_to_dt(
        attr.get("last_updated")
        or attr.get("DateTime")
        or attr.get("date_time")
    )

    # Build readings from available fields
    readings: list[Reading] = []
    ts = obs_time or datetime.now(timezone.utc)

    # Map covers both the current ArcGIS field names (snake_case with units)
    # and legacy CamelCase names for forward compatibility
    _FIELD_MAP: list[tuple[list[str], str, str]] = [
        (["wind_speed_mph", "WindSpeed", "wind_speed"],           "wind_speed",     "mph"),
        (["wind_direction_deg", "WindDirection", "wind_direction"], "wind_direction", "deg"),
        (["wind_gust_mph", "WindGust", "wind_gust"],              "wind_gust",      "mph"),
        (["air_temp_f", "AirTemperature", "air_temperature", "AirTemp"], "air_temp", "F"),
        (["relative_humidity_pct", "RelativeHumidity", "relative_humidity", "RH"], "relative_humidity", "%"),
        (["dew_point_temp_f", "DewPoint", "dew_point"],           "dew_point",      "F"),
        (["wet_bulb_temp_f", "WetBulb", "wet_bulb"],              "wet_bulb",       "F"),
        (["peak_wind_speed_mph", "PeakWindSpeed", "peak_wind_speed"],  "peak_wind",      "mph"),
        (["peak_wind_direction_deg", "PeakWindDirection", "peak_wind_dir"], "peak_wind_direction", "deg"),
    ]

    for keys, param, unit in _FIELD_MAP:
        for key in keys:
            val = _safe_float(attr.get(key))
            if val is not None:
                readings.append(Reading(
                    station_id=station_id,
                    timestamp=ts,
                    parameter=param,
                    value=val,
                    unit=unit,
                    source="calfire_raws",
                ))
                break

    state = attr.get("State") or attr.get("state") or ""
    county = attr.get("County") or attr.get("county") or ""

    station = Station(
        id=station_id,
        name=station_name,
        lat=lat,
        lon=lon,
        source="calfire_raws",
        network="CAL FIRE RAWS",
        params=[r.parameter for r in readings],
        elevation=_safe_float(attr.get("Elevation") or attr.get("elevation")),
        last_update=obs_time,
        extra={
            k: v for k, v in {
                "state": state,
                "county": county,
                "nfdr_station_id": attr.get("NFDRStationID"),
                "agency": attr.get("Agency") or attr.get("agency"),
            }.items() if v
        },
    )

    return StationDetail(station=station, readings=readings)


# ---------------------------------------------------------------------------
# Parsing helpers — NIFC Fire Danger
# ---------------------------------------------------------------------------

def _parse_nifc_station(feat: dict) -> StationDetail | None:
    """Parse one NIFC fire-danger ArcGIS feature into Station + Readings."""
    attr = feat.get("attributes", {})
    geom = feat.get("geometry", {})

    station_name = attr.get("Station_Name") or attr.get("station_name") or "Unknown"
    station_id = str(attr.get("StationID") or attr.get("station_id") or attr.get("OBJECTID", ""))
    if not station_id:
        return None

    lat = _safe_float(geom.get("y") or attr.get("latitude"))
    lon = _safe_float(geom.get("x") or attr.get("longitude"))
    if lat is None or lon is None:
        return None

    obs_time = _epoch_ms_to_dt(attr.get("DateTime") or attr.get("date_time"))
    ts = obs_time or datetime.now(timezone.utc)

    readings: list[Reading] = []

    _DANGER_FIELDS: list[tuple[str, str, str]] = [
        ("ERC",             "erc",             ""),
        ("erc_percentile",  "erc_percentile",  "%"),
        ("BI",              "bi",              ""),
        ("bi_percentile",   "bi_percentile",   "%"),
    ]

    for key, param, unit in _DANGER_FIELDS:
        val = _safe_float(attr.get(key))
        if val is not None:
            readings.append(Reading(
                station_id=station_id,
                timestamp=ts,
                parameter=param,
                value=val,
                unit=unit,
                source="nifc_raws",
            ))

    state = attr.get("State") or attr.get("state") or ""
    erc_trend = attr.get("erc_trend") or attr.get("ERC_Trend") or ""

    station = Station(
        id=station_id,
        name=station_name,
        lat=lat,
        lon=lon,
        source="nifc_raws",
        network="NIFC RAWS Fire Danger",
        params=[r.parameter for r in readings],
        elevation=_safe_float(attr.get("Elevation") or attr.get("elevation")),
        last_update=obs_time,
        extra={
            k: v for k, v in {
                "state": state,
                "erc_trend": erc_trend,
                "gacc": attr.get("GACC") or attr.get("gacc"),
            }.items() if v
        },
    )

    return StationDetail(station=station, readings=readings)


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@router.get("/raws/current")
@cache.cached(ttl=config.CACHE_TTL, prefix="calfire_raws_current")
async def raws_current(
    limit: int = Query(5000, ge=1, le=10000, description="Max stations to return"),
):
    """All CAL FIRE RAWS current observations (paginated from ArcGIS)."""
    features = await _arcgis_query(
        _CALFIRE_URL,
        where="1=1",
        out_fields="*",
        extra_params={"returnGeometry": "true", "outSR": "4326"},
        max_records=limit,
    )

    results: list[dict] = []
    for feat in features:
        detail = _parse_calfire_station(feat)
        if detail:
            results.append(detail.model_dump(mode="json"))

    return {
        "source": "calfire_raws",
        "network": "CAL FIRE RAWS Wind 2-D Public View",
        "station_count": len(results),
        "stations": results,
    }


@router.get("/raws/state/{state}")
@cache.cached(ttl=config.CACHE_TTL, prefix="calfire_raws_state")
async def raws_by_state(
    state: str = Path(..., min_length=2, max_length=2, description="Two-letter state code (e.g. CA, OR)"),
):
    """CAL FIRE RAWS observations filtered by state."""
    state_upper = state.upper()
    # ArcGIS SQL where clause — try common field names
    where = f"State = '{state_upper}' OR state = '{state_upper}'"

    features = await _arcgis_query(
        _CALFIRE_URL,
        where=where,
        out_fields="*",
        extra_params={"returnGeometry": "true", "outSR": "4326"},
    )

    results: list[dict] = []
    for feat in features:
        detail = _parse_calfire_station(feat)
        if detail:
            results.append(detail.model_dump(mode="json"))

    return {
        "source": "calfire_raws",
        "state": state_upper,
        "station_count": len(results),
        "stations": results,
    }


@router.get("/danger")
@cache.cached(ttl=config.CACHE_TTL_LONG, prefix="nifc_danger")
async def fire_danger():
    """NIFC RAWS fire danger percentiles (ERC/BI) for all stations."""
    features = await _arcgis_query(
        _NIFC_URL,
        where="1=1",
        out_fields="*",
        extra_params={"returnGeometry": "true", "outSR": "4326"},
    )

    results: list[dict] = []
    for feat in features:
        detail = _parse_nifc_station(feat)
        if detail:
            results.append(detail.model_dump(mode="json"))

    return {
        "source": "nifc_raws",
        "network": "NIFC RAWS ERC & BI Percentiles and Trends",
        "station_count": len(results),
        "stations": results,
    }


@router.get("/overview")
@cache.cached(ttl=config.CACHE_TTL, prefix="fire_weather_overview")
async def fire_weather_overview():
    """Combined overview: CAL FIRE RAWS obs count + NIFC fire danger summary."""
    # Fetch counts from both services (lightweight — just get record count)
    calfire_features = await _arcgis_query(
        _CALFIRE_URL,
        where="1=1",
        out_fields="OBJECTID",
        extra_params={"returnGeometry": "false", "returnCountOnly": "false"},
    )

    nifc_features = await _arcgis_query(
        _NIFC_URL,
        where="1=1",
        out_fields="OBJECTID,ERC,erc_percentile,BI,bi_percentile,State",
        extra_params={"returnGeometry": "false"},
    )

    # Compute NIFC summary stats
    erc_vals = []
    bi_vals = []
    erc_pct_vals = []
    bi_pct_vals = []
    states: set[str] = set()

    for feat in nifc_features:
        attr = feat.get("attributes", {})
        erc = _safe_float(attr.get("ERC"))
        bi = _safe_float(attr.get("BI"))
        erc_p = _safe_float(attr.get("erc_percentile"))
        bi_p = _safe_float(attr.get("bi_percentile"))
        if erc is not None:
            erc_vals.append(erc)
        if bi is not None:
            bi_vals.append(bi)
        if erc_p is not None:
            erc_pct_vals.append(erc_p)
        if bi_p is not None:
            bi_pct_vals.append(bi_p)
        st = attr.get("State") or attr.get("state")
        if st:
            states.add(str(st).upper())

    def _stats(vals: list[float]) -> dict[str, float | None]:
        if not vals:
            return {"min": None, "max": None, "mean": None, "count": 0}
        return {
            "min": round(min(vals), 1),
            "max": round(max(vals), 1),
            "mean": round(sum(vals) / len(vals), 1),
            "count": len(vals),
        }

    # Count critical-percentile stations (>= 90th)
    critical_erc = sum(1 for v in erc_pct_vals if v >= 90)
    critical_bi = sum(1 for v in bi_pct_vals if v >= 90)

    return {
        "sources": {
            "calfire_raws": {
                "name": "CAL FIRE RAWS Wind 2-D Public View",
                "station_count": len(calfire_features),
                "update_frequency": "10 min",
            },
            "nifc_raws": {
                "name": "NIFC RAWS ERC & BI Percentiles and Trends",
                "station_count": len(nifc_features),
                "update_frequency": "daily",
                "states_reporting": sorted(states),
            },
        },
        "fire_danger_summary": {
            "erc": _stats(erc_vals),
            "erc_percentile": _stats(erc_pct_vals),
            "bi": _stats(bi_vals),
            "bi_percentile": _stats(bi_pct_vals),
            "critical_stations": {
                "erc_above_90th": critical_erc,
                "bi_above_90th": critical_bi,
            },
        },
    }
