"""Harris County Flood Warning System — rain gauges, stream gauges, and weather.

The largest county flood monitoring network in the US, operated by the
Harris County Flood Control District (HCFCD).  ~290 monitoring sites across
Harris County, Texas (Houston metro), including:

  - Rain gauges at nearly every site (hourly accumulations)
  - Stream/bayou elevation gauges (~230 sites) with flood thresholds
  - Weather stations (~15 sites) with temp, humidity, pressure, wind

Data is sourced two ways:

  1. ArcGIS FeatureServer (Friendswood subset, 9 sites) — station metadata
     with coordinates, sensor capability flags.
  2. HCFCD GageDetail pages — full network live data, embedded as JSON in
     the server-rendered ``refreshAll()`` call.  Parsed via regex extraction.

All endpoints are unauthenticated.  Data updates every 5-15 minutes.

References:
  - https://www.harriscountyfws.org/
  - https://www.hcfcd.org/
"""
from __future__ import annotations

import json
import logging
import re
from datetime import datetime, timedelta, timezone
from typing import Optional

from fastapi import APIRouter, HTTPException, Query

import cache
import http_client

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Module-level metadata (consumed by server.py /api/sources)
# ---------------------------------------------------------------------------

SOURCE_NAME = "Harris County Flood Warning"
SOURCE_DESCRIPTION = (
    "Harris County Flood Warning System: ~290 rain/stream/weather monitoring "
    "sites across Harris County (Houston), Texas.  Operated by the Harris "
    "County Flood Control District.  No auth required."
)
SOURCE_META = {
    "coverage": "Harris County, Texas, USA",
    "category": "water",
    "auth_required": False,
    "params": [
        "precipitation", "stage", "stream_elevation",
        "temperature", "humidity", "pressure",
        "wind_speed", "wind_direction", "wind_gust",
    ],
    "update_frequency": "5-15 min",
    "url": "https://www.harriscountyfws.org/",
}

# ---------------------------------------------------------------------------
# Router
# ---------------------------------------------------------------------------

router = APIRouter(
    prefix="/api/harris_flood",
    tags=["Harris County Flood Warning"],
)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_SOURCE = "harris_county_fws"

# ArcGIS FeatureServer (Friendswood / FWD subset — 9 sites with coordinates)
_ARCGIS_BASE = (
    "https://services6.arcgis.com/ghs8JYGzwZM6Zl6x/arcgis/rest/services/"
    "Harris_County_Flood_Warning_System_FWD/FeatureServer"
)
_ARCGIS_SITES_URL = f"{_ARCGIS_BASE}/0/query"

# HCFCD GageDetail page — contains full JSON payload in refreshAll() call
_GAGE_DETAIL_URL = "https://www.harriscountyfws.org/GageDetail/Index/{site_id}"

# Central time zone (Harris County)
_CT = timezone(timedelta(hours=-5))  # CDT; use -6 for CST if needed

# Regex to extract the refreshAll() JSON blob from the page HTML
_REFRESH_ALL_RE = re.compile(
    r"refreshAll\((\{.*?\})\)\s*;?\s*(?:</script>|$)",
    re.DOTALL,
)

# SensorClass codes used in the Weather.LastWeatherReading array
_WEATHER_SENSOR_MAP: dict[int, tuple[str, str]] = {
    30: ("temperature", "degF"),
    50: ("humidity", "%"),
    53: ("pressure", "inHg"),
    40: ("wind_speed", "mph"),
    41: ("wind_gust", "mph"),
    44: ("wind_direction", "deg"),
}


# ---------------------------------------------------------------------------
# Helpers — ArcGIS query
# ---------------------------------------------------------------------------

async def _query_arcgis_sites() -> list[dict]:
    """Fetch all sites from the ArcGIS FeatureServer (FWD subset).

    Returns raw feature dicts with properties and geometry.
    """
    params = {
        "where": "1=1",
        "outFields": "*",
        "f": "geojson",
        "resultRecordCount": 500,
    }
    try:
        data = await http_client.fetch_json(
            _ARCGIS_SITES_URL, params=params, source=_SOURCE,
        )
        return data.get("features", [])
    except Exception as exc:
        log.warning("ArcGIS site query failed: %s", exc)
        return []


# ---------------------------------------------------------------------------
# Helpers — GageDetail page scraping
# ---------------------------------------------------------------------------

async def _fetch_gage_detail(site_id: int) -> Optional[dict]:
    """Fetch and parse the refreshAll() JSON from a GageDetail page.

    Returns the parsed JSON dict, or None on failure.
    """
    url = _GAGE_DETAIL_URL.format(site_id=site_id)
    try:
        html = await http_client.fetch_text(url, source=_SOURCE, timeout=20)
    except Exception as exc:
        log.debug("GageDetail fetch failed for site %s: %s", site_id, exc)
        return None

    m = _REFRESH_ALL_RE.search(html)
    if not m:
        log.debug("No refreshAll() JSON found for site %s", site_id)
        return None

    try:
        return json.loads(m.group(1))
    except json.JSONDecodeError as exc:
        log.debug("JSON parse failed for site %s: %s", site_id, exc)
        return None


def _parse_timestamp(ts_str: Optional[str]) -> Optional[str]:
    """Parse an HCFCD timestamp string to ISO 8601.

    HCFCD uses formats like '2026-03-20T04:06:30' (local CDT, no offset).
    We assume Central Time and return a UTC ISO string.
    """
    if not ts_str or ts_str.startswith("0001-"):
        return None
    try:
        # Try parsing with offset first (some fields include it)
        if "+" in ts_str or ts_str.endswith("Z"):
            dt = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
        else:
            # Strip any fractional / timezone suffix before parsing
            clean = ts_str.split("-05:00")[0].split("-06:00")[0]
            # Remove trailing sub-second info if present
            if "." in clean:
                clean = clean.split(".")[0]
            dt = datetime.strptime(clean, "%Y-%m-%dT%H:%M:%S")
            dt = dt.replace(tzinfo=_CT)
        return dt.astimezone(timezone.utc).isoformat()
    except (ValueError, TypeError):
        return ts_str


def _extract_sites_list(data: dict) -> list[dict]:
    """Extract the Sites array from the refreshAll() JSON.

    Returns list of {Site_Id, Location} dicts, excluding the placeholder.
    """
    sites = data.get("Sites", [])
    return [
        s for s in sites
        if isinstance(s, dict) and s.get("Site_Id", 0) > 0
    ]


# ---------------------------------------------------------------------------
# Helpers — reading extraction
# ---------------------------------------------------------------------------

def _extract_stream_readings(site_id: int, se: dict) -> dict:
    """Extract stream elevation info from the StreamElevation sub-object."""
    if not se or not se.get("HasData"):
        return {}

    current = se.get("CurrentLevel")
    ts = _parse_timestamp(se.get("CurrentReadingDate"))
    tob = se.get("TOB", 0)
    boc = se.get("BOC", 0)
    fl_indicator = se.get("FLLevelIndicator", 0)

    channel = se.get("ChannelInfo") or {}
    flood_level = channel.get("FloodLevelIndicator", fl_indicator)

    # Recent time-series from CumulativeGridData
    grid = se.get("CumulativeGridData") or []
    time_series = []
    for entry in grid[:48]:  # Last 48 readings
        time_series.append({
            "timestamp": _parse_timestamp(entry.get("DataTime")),
            "value": entry.get("DataValue"),
        })

    return {
        "has_data": True,
        "current_level_ft": round(float(current), 2) if current else None,
        "timestamp": ts,
        "top_of_bank_ft": tob,
        "bottom_of_channel_ft": boc,
        "flood_level_indicator_ft": flood_level,
        "site_type": se.get("SiteType") or channel.get("SiteType"),
        "usgs_url": channel.get("USGSUrl"),
        "nws_url": channel.get("NWSUrl"),
        "recent_readings": time_series,
    }


def _extract_rainfall(site_id: int, rain: dict) -> dict:
    """Extract rainfall info from the Rainfall sub-object."""
    if not rain or not rain.get("HasData"):
        return {}

    total = rain.get("TotalRainfall", 0)
    reporting = rain.get("ReportingTime", "")

    # Hourly grid data
    grid = rain.get("RainfallGridData") or []
    hourly = []
    for entry in grid[:48]:
        hourly.append({
            "start": _parse_timestamp(entry.get("DateStart")),
            "end": _parse_timestamp(entry.get("DateEnd")),
            "value_in": entry.get("DataValue", 0),
        })

    return {
        "has_data": True,
        "total_rainfall_in": total,
        "reporting_period": reporting,
        "hourly_data": hourly,
    }


def _extract_weather(site_id: int, wx: dict) -> list[dict]:
    """Extract weather readings from the Weather sub-object."""
    if not wx or not wx.get("HasData"):
        return []

    readings = []
    for sensor in wx.get("LastWeatherReading", []):
        sensor_class = sensor.get("SensorClass", 0)
        mapped = _WEATHER_SENSOR_MAP.get(sensor_class)
        if not mapped:
            # Use description as fallback
            param = (sensor.get("Description") or "unknown").lower().replace(" ", "_")
            unit = (sensor.get("UnitSymbol") or "").strip()
        else:
            param, unit = mapped

        readings.append({
            "parameter": param,
            "value": sensor.get("LastReading"),
            "unit": unit,
            "timestamp": _parse_timestamp(sensor.get("LastTime")),
            "description": sensor.get("Description"),
            "sensor_id": sensor.get("SensorId"),
        })

    return readings


# ---------------------------------------------------------------------------
# Cached fetchers
# ---------------------------------------------------------------------------

@cache.cached(ttl=300, prefix="harris_sites:")
async def _fetch_sites_list() -> list[dict]:
    """Fetch the master sites list by scraping a single GageDetail page.

    We use site 1000 (Houston Transtar) as the canonical page — every page
    returns the same Sites array in the refreshAll() payload.
    """
    data = await _fetch_gage_detail(1000)
    if not data:
        # Fallback: try site 520 (White Oak Bayou)
        data = await _fetch_gage_detail(520)
    if not data:
        return []
    return _extract_sites_list(data)


@cache.cached(ttl=600, prefix="harris_arcgis_sites:")
async def _fetch_arcgis_sites() -> dict[int, dict]:
    """Fetch ArcGIS site metadata, keyed by Site_Num.

    Returns {site_num: {lat, lon, stream_elev, rainfall, weather}}.
    """
    features = await _query_arcgis_sites()
    result: dict[int, dict] = {}
    for feat in features:
        props = feat.get("properties", {})
        geom = feat.get("geometry", {})
        coords = geom.get("coordinates", [])
        if len(coords) < 2:
            continue
        site_num = props.get("Site_Num")
        if site_num is None:
            continue
        result[int(site_num)] = {
            "lat": props.get("Lat", coords[1]),
            "lon": props.get("Long", coords[0]),
            "stream_elev": props.get("Stream_Elev") == "Y",
            "rainfall": props.get("Rainfall") == "Y",
            "weather": props.get("Weather") == "Y",
            "owner": props.get("Owner"),
        }
    return result


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@router.get("/rain", summary="Current rainfall from Harris County FWS gauges")
async def harris_rain(
    site_id: Optional[int] = Query(
        None,
        description="Specific site ID (e.g. 520). If omitted, returns a "
        "summary from a selection of representative sites.",
    ),
) -> dict:
    """Return current rainfall data from Harris County Flood Warning System.

    If ``site_id`` is provided, returns detailed hourly rainfall for that
    one site.  Otherwise, returns data from a curated set of representative
    sites across the county (to avoid scraping all 290 pages).
    """
    if site_id is not None:
        return await _rain_single(site_id)

    # Representative sites spanning Harris County watersheds
    representative = [
        100, 340, 420, 520, 710, 1000, 1050, 1120, 1600, 2020,
        2110, 2220, 460, 830, 1320, 1720, 780, 1940,
    ]
    return await _rain_batch(representative)


async def _rain_single(site_id: int) -> dict:
    """Fetch rainfall data for a single site."""
    cache_key = f"harris_rain:{site_id}"
    cached_val = await cache.get(cache_key)
    if cached_val is not None:
        return cached_val

    data = await _fetch_gage_detail(site_id)
    if not data:
        raise HTTPException(502, f"Could not fetch data for site {site_id}")

    rain = _extract_rainfall(site_id, data.get("Rainfall", {}))
    if not rain:
        raise HTTPException(404, f"No rainfall data for site {site_id}")

    result = {
        "source": _SOURCE,
        "site_id": site_id,
        "network": "rain",
        "total_rainfall_in": rain.get("total_rainfall_in", 0),
        "reporting_period": rain.get("reporting_period", ""),
        "hourly_data": rain.get("hourly_data", []),
    }
    await cache.set(cache_key, result, ttl=300)
    return result


async def _rain_batch(site_ids: list[int]) -> dict:
    """Fetch rainfall summaries for multiple sites."""
    cache_key = "harris_rain:batch"
    cached_val = await cache.get(cache_key)
    if cached_val is not None:
        return cached_val

    sites_meta = await _fetch_arcgis_sites()
    sites_list = await _fetch_sites_list()
    site_name_map = {s["Site_Id"]: s["Location"] for s in sites_list}

    gauges: list[dict] = []
    for sid in site_ids:
        data = await _fetch_gage_detail(sid)
        if not data:
            continue

        rain = _extract_rainfall(sid, data.get("Rainfall", {}))
        if not rain.get("has_data"):
            continue

        meta = sites_meta.get(sid, {})
        gauges.append({
            "site_id": sid,
            "name": site_name_map.get(sid, f"Site {sid}"),
            "lat": meta.get("lat"),
            "lon": meta.get("lon"),
            "total_rainfall_in": rain.get("total_rainfall_in", 0),
            "reporting_period": rain.get("reporting_period", ""),
        })

    result = {
        "source": _SOURCE,
        "network": "rain",
        "gauge_count": len(gauges),
        "gauges": gauges,
    }
    await cache.set(cache_key, result, ttl=300)
    return result


@router.get("/streams", summary="Stream levels with flood thresholds")
async def harris_streams(
    site_id: Optional[int] = Query(
        None,
        description="Specific site ID (e.g. 520). If omitted, returns a "
        "summary from representative stream gauge sites.",
    ),
) -> dict:
    """Return current stream/bayou elevation data with flood thresholds.

    Each stream gauge reports:
    - Current water level (ft above datum)
    - Top of Bank (TOB) and Bottom of Channel (BOC) elevations
    - Flood Level Indicator — the level at which flooding is expected
    - Recent time-series readings (up to 48 observations)
    """
    if site_id is not None:
        return await _stream_single(site_id)

    # Representative stream gauges across major watersheds
    representative = [
        110, 340, 370, 420, 460, 520, 550, 710, 750, 1050,
        1120, 1160, 1600, 1640, 1720, 2020, 2040, 2110, 2220,
        2260, 830, 780, 1460, 1940,
    ]
    return await _stream_batch(representative)


async def _stream_single(site_id: int) -> dict:
    """Fetch stream elevation data for a single site."""
    cache_key = f"harris_stream:{site_id}"
    cached_val = await cache.get(cache_key)
    if cached_val is not None:
        return cached_val

    data = await _fetch_gage_detail(site_id)
    if not data:
        raise HTTPException(502, f"Could not fetch data for site {site_id}")

    stream = _extract_stream_readings(site_id, data.get("StreamElevation", {}))
    if not stream:
        raise HTTPException(404, f"No stream data for site {site_id}")

    rain = _extract_rainfall(site_id, data.get("Rainfall", {}))

    result = {
        "source": _SOURCE,
        "site_id": site_id,
        "network": "stream",
        **stream,
        "rainfall": {
            "total_in": rain.get("total_rainfall_in", 0),
            "period": rain.get("reporting_period", ""),
        } if rain else None,
    }
    await cache.set(cache_key, result, ttl=300)
    return result


async def _stream_batch(site_ids: list[int]) -> dict:
    """Fetch stream level summaries for multiple sites."""
    cache_key = "harris_stream:batch"
    cached_val = await cache.get(cache_key)
    if cached_val is not None:
        return cached_val

    sites_meta = await _fetch_arcgis_sites()
    sites_list = await _fetch_sites_list()
    site_name_map = {s["Site_Id"]: s["Location"] for s in sites_list}

    gauges: list[dict] = []
    for sid in site_ids:
        data = await _fetch_gage_detail(sid)
        if not data:
            continue

        stream = _extract_stream_readings(sid, data.get("StreamElevation", {}))
        if not stream.get("has_data"):
            continue

        meta = sites_meta.get(sid, {})
        flood_level = stream.get("flood_level_indicator_ft", 0)
        current = stream.get("current_level_ft")
        tob = stream.get("top_of_bank_ft", 0)

        # Compute flood status
        if current is not None and flood_level and flood_level > 0:
            if current >= tob and tob > 0:
                status = "flooding_likely"
            elif current >= flood_level:
                status = "flooding_possible"
            else:
                pct = (current / flood_level * 100) if flood_level else 0
                status = f"normal ({pct:.0f}% of flood level)"
        else:
            status = "unknown"

        gauges.append({
            "site_id": sid,
            "name": site_name_map.get(sid, f"Site {sid}"),
            "lat": meta.get("lat"),
            "lon": meta.get("lon"),
            "current_level_ft": current,
            "top_of_bank_ft": tob,
            "bottom_of_channel_ft": stream.get("bottom_of_channel_ft", 0),
            "flood_level_indicator_ft": flood_level,
            "flood_status": status,
            "timestamp": stream.get("timestamp"),
            "site_type": stream.get("site_type"),
        })

    result = {
        "source": _SOURCE,
        "network": "stream",
        "gauge_count": len(gauges),
        "gauges": gauges,
    }
    await cache.set(cache_key, result, ttl=300)
    return result


@router.get("/overview", summary="Harris County Flood Warning System overview")
async def harris_overview() -> dict:
    """Summary of the Harris County FWS and available endpoints."""
    # Fetch site list for an accurate count
    sites = await _fetch_sites_list()

    return {
        "source": _SOURCE,
        "name": SOURCE_NAME,
        "description": SOURCE_DESCRIPTION,
        "operator": "Harris County Flood Control District (HCFCD)",
        "coverage": "Harris County, Texas (~1,777 sq mi, Houston metro)",
        "auth_required": False,
        "update_frequency": "5-15 min",
        "total_sites": len(sites),
        "networks": [
            {
                "id": "rain",
                "name": "Rain Gauges",
                "description": (
                    "Tipping-bucket precipitation gauges at nearly all "
                    "monitoring sites.  Reports hourly accumulations."
                ),
                "station_count_approx": 280,
                "params": ["precipitation"],
                "endpoint": "/api/harris_flood/rain",
            },
            {
                "id": "stream",
                "name": "Stream / Bayou Gauges",
                "description": (
                    "Pressure-transducer stream elevation gauges on bayous, "
                    "creeks, rivers, dams, and drainage channels.  Includes "
                    "Top of Bank, Bottom of Channel, and Flood Level "
                    "Indicator thresholds."
                ),
                "station_count_approx": 230,
                "params": ["stream_elevation", "stage"],
                "endpoint": "/api/harris_flood/streams",
            },
            {
                "id": "weather",
                "name": "Weather Stations",
                "description": (
                    "Full weather stations at select sites reporting "
                    "temperature, humidity, barometric pressure, wind "
                    "speed/gust/direction."
                ),
                "station_count_approx": 15,
                "params": [
                    "temperature", "humidity", "pressure",
                    "wind_speed", "wind_gust", "wind_direction",
                ],
                "endpoint": "/api/harris_flood/streams?site_id=1000",
                "notes": "Weather data included in per-site detail queries",
            },
        ],
        "endpoints": [
            "/api/harris_flood/rain",
            "/api/harris_flood/rain?site_id=520",
            "/api/harris_flood/streams",
            "/api/harris_flood/streams?site_id=520",
            "/api/harris_flood/overview",
        ],
        "data_urls": {
            "main": "https://www.harriscountyfws.org/",
            "hcfcd": "https://www.hcfcd.org/",
            "gage_detail_example": _GAGE_DETAIL_URL.format(site_id=520),
            "arcgis_fwd": f"{_ARCGIS_SITES_URL}?where=1%3D1&outFields=*&f=geojson",
        },
        "notes": (
            "The rain and streams endpoints return data from a curated set "
            "of representative sites by default.  Pass site_id to query any "
            "of the ~290 individual monitoring locations.  Full site list is "
            "available in the refreshAll() payload of any GageDetail page."
        ),
    }
