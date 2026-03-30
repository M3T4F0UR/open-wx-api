"""Earthquake Monitoring — USGS, EMSC SeismicPortal, GeoNet NZ.

Three seismological data sources are integrated here:

1. USGS Earthquake Hazards Program
   Pre-built GeoJSON feeds updated every minute plus a flexible query API.
   Covers worldwide seismicity with focus on the US.
   Feed URL: https://earthquake.usgs.gov/earthquakes/feed/v1.0/
   Query API: https://earthquake.usgs.gov/fdsnws/event/1/

2. EMSC SeismicPortal (European-Mediterranean Seismological Centre)
   FDSN-compatible event query API covering global seismicity with
   European/Mediterranean focus.
   Base URL: http://www.seismicportal.eu/fdsnws/event/1/

3. GeoNet NZ (GNS Science, New Zealand)
   Real-time earthquake data for the New Zealand region filtered by
   Modified Mercalli Intensity (MMI).
   Base URL: https://api.geonet.org.nz/

All APIs are free with no authentication required.
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Optional

from fastapi import APIRouter, HTTPException, Query

import cache
import config
import http_client
from models import Reading, Station, StationDetail

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Source registry metadata
# ---------------------------------------------------------------------------
SOURCE_NAME = "Earthquake Monitoring"
SOURCE_DESCRIPTION = (
    "Real-time earthquake data from USGS Earthquake Hazards Program, "
    "EMSC SeismicPortal, and GeoNet NZ — magnitude, depth, location, "
    "and impact information for worldwide seismicity."
)
SOURCE_META = {
    "url": "https://earthquake.usgs.gov/",
    "auth_required": False,
    "update_frequency": "1 min (feeds), real-time (query)",
    "coverage": "Global (USGS), Europe/Mediterranean (EMSC), New Zealand (GeoNet)",
    "params": ["magnitude", "depth", "intensity"],
    "category": "seismology",
}

router = APIRouter(prefix="/api/earthquakes", tags=["Earthquake Monitoring"])

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
USGS_FEED_BASE = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary"
USGS_QUERY_BASE = "https://earthquake.usgs.gov/fdsnws/event/1/query"
EMSC_QUERY_BASE = "http://www.seismicportal.eu/fdsnws/event/1/query"
GEONET_BASE = "https://api.geonet.org.nz"

# Map period + magnitude threshold to USGS feed filenames
_USGS_FEEDS: dict[str, dict[str, str]] = {
    "hour": {
        "all": f"{USGS_FEED_BASE}/all_hour.geojson",
        "1.0": f"{USGS_FEED_BASE}/1.0_hour.geojson",
        "2.5": f"{USGS_FEED_BASE}/2.5_hour.geojson",
        "4.5": f"{USGS_FEED_BASE}/4.5_hour.geojson",
        "significant": f"{USGS_FEED_BASE}/significant_hour.geojson",
    },
    "day": {
        "all": f"{USGS_FEED_BASE}/all_day.geojson",
        "1.0": f"{USGS_FEED_BASE}/1.0_day.geojson",
        "2.5": f"{USGS_FEED_BASE}/2.5_day.geojson",
        "4.5": f"{USGS_FEED_BASE}/4.5_day.geojson",
        "significant": f"{USGS_FEED_BASE}/significant_day.geojson",
    },
    "week": {
        "all": f"{USGS_FEED_BASE}/all_week.geojson",
        "1.0": f"{USGS_FEED_BASE}/1.0_week.geojson",
        "2.5": f"{USGS_FEED_BASE}/2.5_week.geojson",
        "4.5": f"{USGS_FEED_BASE}/4.5_week.geojson",
        "significant": f"{USGS_FEED_BASE}/significant_week.geojson",
    },
    "month": {
        "all": f"{USGS_FEED_BASE}/all_month.geojson",
        "1.0": f"{USGS_FEED_BASE}/1.0_month.geojson",
        "2.5": f"{USGS_FEED_BASE}/2.5_month.geojson",
        "4.5": f"{USGS_FEED_BASE}/4.5_month.geojson",
        "significant": f"{USGS_FEED_BASE}/significant_month.geojson",
    },
}


# ---------------------------------------------------------------------------
# Helper utilities
# ---------------------------------------------------------------------------


def _pick_feed_url(period: str, min_magnitude: Optional[float]) -> str:
    """Select the best USGS pre-built feed URL for the given period and magnitude."""
    period = period.lower()
    if period not in _USGS_FEEDS:
        raise ValueError(f"Invalid period '{period}'. Use hour, day, week, or month.")

    feeds = _USGS_FEEDS[period]
    if min_magnitude is None:
        return feeds["all"]
    if min_magnitude >= 4.5:
        return feeds["4.5"]
    if min_magnitude >= 2.5:
        return feeds["2.5"]
    if min_magnitude >= 1.0:
        return feeds["1.0"]
    return feeds["all"]


def _parse_usgs_feature(feature: dict) -> Station:
    """Convert a USGS GeoJSON feature to a Station model."""
    props = feature.get("properties", {})
    coords = feature.get("geometry", {}).get("coordinates", [0, 0, 0])
    lon, lat = coords[0], coords[1]
    depth_km = coords[2] if len(coords) > 2 else None

    event_id = feature.get("id", props.get("code", "unknown"))
    mag = props.get("mag")
    place = props.get("place", "Unknown location")
    event_time = props.get("time")

    ts = None
    if event_time is not None:
        try:
            ts = datetime.fromtimestamp(event_time / 1000, tz=timezone.utc)
        except (OSError, ValueError):
            pass

    return Station(
        id=event_id,
        name=f"M{mag:.1f} — {place}" if mag is not None else place,
        lat=lat,
        lon=lon,
        source="usgs_earthquake",
        network="USGS",
        params=["magnitude", "depth"],
        elevation=None,
        url=props.get("url"),
        last_update=ts,
        extra={
            "magnitude": mag,
            "mag_type": props.get("magType"),
            "depth_km": depth_km,
            "place": place,
            "status": props.get("status"),
            "tsunami": props.get("tsunami"),
            "felt": props.get("felt"),
            "cdi": props.get("cdi"),
            "mmi": props.get("mmi"),
            "alert": props.get("alert"),
            "sig": props.get("sig"),
            "event_type": props.get("type"),
            "title": props.get("title"),
        },
    )


def _parse_usgs_features(data: dict) -> list[Station]:
    """Parse all features from a USGS GeoJSON response."""
    features = data.get("features", [])
    stations = []
    for f in features:
        try:
            stations.append(_parse_usgs_feature(f))
        except Exception as exc:
            logger.debug("Skipping malformed USGS feature: %s", exc)
    return stations


def _parse_emsc_event(event: dict) -> Station:
    """Convert an EMSC SeismicPortal event to a Station model."""
    props = event.get("properties", {})
    coords = event.get("geometry", {}).get("coordinates", [0, 0, 0])
    lon, lat = coords[0], coords[1]
    depth_km = coords[2] if len(coords) > 2 else None

    event_id = props.get("source_id", props.get("unid", "unknown"))
    mag = props.get("mag")
    region = props.get("flynn_region", "Unknown region")

    ts = None
    time_str = props.get("time")
    if time_str:
        try:
            ts = datetime.fromisoformat(time_str.replace("Z", "+00:00"))
        except (ValueError, AttributeError):
            pass

    return Station(
        id=f"emsc_{event_id}",
        name=f"M{mag:.1f} — {region}" if mag is not None else region,
        lat=lat,
        lon=lon,
        source="emsc_earthquake",
        network="EMSC",
        params=["magnitude", "depth"],
        url=f"https://www.seismicportal.eu/eventdetails.html?unid={event_id}",
        last_update=ts,
        extra={
            "magnitude": mag,
            "mag_type": props.get("magtype"),
            "depth_km": depth_km,
            "region": region,
            "auth": props.get("auth"),
            "source_id": event_id,
        },
    )


def _parse_geonet_quake(quake: dict) -> Station:
    """Convert a GeoNet NZ quake object to a Station model."""
    props = quake.get("properties", {})
    coords = quake.get("geometry", {}).get("coordinates", [0, 0, 0])
    lon, lat = coords[0], coords[1]
    depth_km = coords[2] if len(coords) > 2 else None

    event_id = props.get("publicID", "unknown")
    mag = props.get("magnitude")
    locality = props.get("locality", "Unknown location")

    ts = None
    time_str = props.get("time")
    if time_str:
        try:
            ts = datetime.fromisoformat(time_str.replace("Z", "+00:00"))
        except (ValueError, AttributeError):
            pass

    return Station(
        id=f"geonet_{event_id}",
        name=f"M{mag:.1f} — {locality}" if mag is not None else locality,
        lat=lat,
        lon=lon,
        source="geonet_earthquake",
        network="GeoNet NZ",
        params=["magnitude", "depth"],
        url=f"https://www.geonet.org.nz/earthquake/{event_id}",
        last_update=ts,
        extra={
            "magnitude": mag,
            "depth_km": depth_km,
            "locality": locality,
            "mmi": props.get("mmi"),
            "quality": props.get("quality"),
        },
    )


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------


@router.get("/recent")
async def recent_earthquakes(
    period: str = Query("hour", description="Time period: hour, day, week, or month"),
    min_magnitude: Optional[float] = Query(
        None, alias="min_magnitude", description="Minimum magnitude filter (e.g. 2.5)"
    ),
):
    """Recent earthquakes from USGS pre-built feeds.

    Uses the USGS GeoJSON summary feeds which are updated every minute.
    Returns earthquake locations as Station objects with magnitude/depth in extras.
    """
    try:
        feed_url = _pick_feed_url(period, min_magnitude)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))

    cache_key = f"earthquakes:recent:{period}:{min_magnitude}"
    cached = await cache.get(cache_key)
    if cached is not None:
        return cached

    try:
        data = await http_client.fetch_json(feed_url, source="usgs_earthquake")
    except Exception as exc:
        logger.error("USGS earthquake feed failed: %s", exc)
        raise HTTPException(status_code=502, detail=f"USGS feed unavailable: {exc}")

    stations = _parse_usgs_features(data)

    # Apply client-side magnitude filter for feeds that include events below threshold
    if min_magnitude is not None:
        stations = [
            s for s in stations
            if s.extra.get("magnitude") is not None
            and s.extra["magnitude"] >= min_magnitude
        ]

    metadata = data.get("metadata", {})
    result = {
        "count": len(stations),
        "period": period,
        "min_magnitude": min_magnitude,
        "generated": metadata.get("generated"),
        "title": metadata.get("title"),
        "earthquakes": [s.model_dump(mode="json") for s in stations],
    }

    await cache.set(cache_key, result, ttl=60)  # 1-min TTL matches feed update rate
    return result


@router.get("/query")
async def query_earthquakes(
    start: str = Query(..., description="Start date (ISO 8601, e.g. 2026-03-18)"),
    end: str = Query(..., description="End date (ISO 8601, e.g. 2026-03-19)"),
    min_mag: float = Query(0, description="Minimum magnitude"),
    max_mag: Optional[float] = Query(None, description="Maximum magnitude"),
    lat: Optional[float] = Query(None, description="Latitude for radius search"),
    lon: Optional[float] = Query(None, description="Longitude for radius search"),
    radius_km: Optional[float] = Query(None, description="Search radius in km (requires lat/lon)"),
    limit: int = Query(500, description="Maximum number of results (max 20000)"),
):
    """Custom earthquake query via the USGS FDSN event API.

    Supports time range, magnitude range, and geographic radius filtering.
    """
    params: dict = {
        "format": "geojson",
        "starttime": start,
        "endtime": end,
        "minmagnitude": min_mag,
        "limit": min(limit, 20000),
        "orderby": "time",
    }
    if max_mag is not None:
        params["maxmagnitude"] = max_mag
    if lat is not None and lon is not None and radius_km is not None:
        params["latitude"] = lat
        params["longitude"] = lon
        params["maxradiuskm"] = radius_km

    cache_key = f"earthquakes:query:{start}:{end}:{min_mag}:{max_mag}:{lat}:{lon}:{radius_km}:{limit}"
    cached = await cache.get(cache_key)
    if cached is not None:
        return cached

    try:
        data = await http_client.fetch_json(
            USGS_QUERY_BASE, params=params, source="usgs_earthquake"
        )
    except Exception as exc:
        logger.error("USGS earthquake query failed: %s", exc)
        raise HTTPException(status_code=502, detail=f"USGS query API unavailable: {exc}")

    stations = _parse_usgs_features(data)
    metadata = data.get("metadata", {})

    result = {
        "count": len(stations),
        "query": {
            "start": start,
            "end": end,
            "min_mag": min_mag,
            "max_mag": max_mag,
            "lat": lat,
            "lon": lon,
            "radius_km": radius_km,
        },
        "title": metadata.get("title"),
        "earthquakes": [s.model_dump(mode="json") for s in stations],
    }

    await cache.set(cache_key, result, ttl=120)
    return result


@router.get("/significant")
async def significant_earthquakes():
    """Significant earthquakes from the past week (USGS curated feed).

    The USGS 'significant' feed includes events that are notable due to
    magnitude, PAGER alert level, felt reports, or tsunami potential.
    """
    cache_key = "earthquakes:significant_week"
    cached = await cache.get(cache_key)
    if cached is not None:
        return cached

    feed_url = _USGS_FEEDS["week"]["significant"]
    try:
        data = await http_client.fetch_json(feed_url, source="usgs_earthquake")
    except Exception as exc:
        logger.error("USGS significant feed failed: %s", exc)
        raise HTTPException(status_code=502, detail=f"USGS feed unavailable: {exc}")

    stations = _parse_usgs_features(data)
    metadata = data.get("metadata", {})

    result = {
        "count": len(stations),
        "period": "week",
        "generated": metadata.get("generated"),
        "title": metadata.get("title"),
        "earthquakes": [s.model_dump(mode="json") for s in stations],
    }

    await cache.set(cache_key, result, ttl=60)
    return result


@router.get("/emsc")
async def emsc_earthquakes(
    min_mag: float = Query(5.0, description="Minimum magnitude"),
    limit: int = Query(20, description="Number of results (max 500)"),
):
    """European-Mediterranean seismicity from EMSC SeismicPortal.

    Returns recent earthquakes from the EMSC FDSN event API, useful for
    global and European/Mediterranean coverage as a complement to USGS.
    """
    cache_key = f"earthquakes:emsc:{min_mag}:{limit}"
    cached = await cache.get(cache_key)
    if cached is not None:
        return cached

    params = {
        "limit": min(limit, 500),
        "format": "json",
        "minmag": min_mag,
    }

    try:
        data = await http_client.fetch_json(
            EMSC_QUERY_BASE, params=params, source="emsc_earthquake"
        )
    except Exception as exc:
        logger.error("EMSC SeismicPortal query failed: %s", exc)
        raise HTTPException(status_code=502, detail=f"EMSC unavailable: {exc}")

    # EMSC returns a FeatureCollection-like structure
    features = data.get("features", [])
    stations = []
    for f in features:
        try:
            stations.append(_parse_emsc_event(f))
        except Exception as exc:
            logger.debug("Skipping malformed EMSC event: %s", exc)

    result = {
        "count": len(stations),
        "source": "EMSC SeismicPortal",
        "min_mag": min_mag,
        "earthquakes": [s.model_dump(mode="json") for s in stations],
    }

    await cache.set(cache_key, result, ttl=120)
    return result


@router.get("/nz")
async def geonet_nz_earthquakes(
    mmi: int = Query(3, description="Minimum Modified Mercalli Intensity (MMI), -1 to 8"),
):
    """New Zealand earthquakes from GeoNet.

    Returns recent earthquakes filtered by Modified Mercalli Intensity.
    GeoNet is operated by GNS Science and provides authoritative NZ seismic data.
    """
    cache_key = f"earthquakes:geonet_nz:{mmi}"
    cached = await cache.get(cache_key)
    if cached is not None:
        return cached

    url = f"{GEONET_BASE}/quake"
    params = {"MMI": mmi}

    try:
        data = await http_client.fetch_json(url, params=params, source="geonet_earthquake")
    except Exception as exc:
        logger.error("GeoNet NZ query failed: %s", exc)
        raise HTTPException(status_code=502, detail=f"GeoNet NZ unavailable: {exc}")

    features = data.get("features", [])
    stations = []
    for f in features:
        try:
            stations.append(_parse_geonet_quake(f))
        except Exception as exc:
            logger.debug("Skipping malformed GeoNet quake: %s", exc)

    result = {
        "count": len(stations),
        "source": "GeoNet NZ",
        "mmi": mmi,
        "earthquakes": [s.model_dump(mode="json") for s in stations],
    }

    await cache.set(cache_key, result, ttl=120)
    return result
