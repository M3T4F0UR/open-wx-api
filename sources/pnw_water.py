"""Pacific NW Water Monitoring — Oregon, Washington, Idaho state networks.

Aggregates water monitoring data from state-operated networks across the
Pacific Northwest that are NOT already covered by USGS or NOAA:

1. Oregon OWRD — State-operated stream gauges (beyond USGS network)
   Portal: https://apps.wrd.state.or.us/apps/sw/hydro_near_real_time/

2. Washington Ecology Freshwater DataStream — Continuous streamflow + WQ
   Portal: https://apps.ecology.wa.gov/continuousflowandwq/
   Params: temperature, dissolved_oxygen, pH, conductivity, turbidity
   15–30 min intervals via GOES satellite

3. Idaho DOT RWIS — Road weather stations (ArcGIS)
   REST: https://gis.itd.idaho.gov/arcgisprod/rest/services/

4. Idaho SNOTEL Stations — NRCS SNOTEL metadata via Idaho IDWR (ArcGIS)
   REST: https://gis.idwr.idaho.gov/hosting/rest/services/

5. Puget Sound Vital Signs — 25 ecosystem indicators
   Portal: https://vitalsigns.pugetsoundinfo.wa.gov/

All sources are free with no authentication required.
"""
from __future__ import annotations

import logging
import re
from datetime import datetime, timezone
from typing import Any, Optional

import httpx
from fastapi import APIRouter, HTTPException, Query

import cache
import config
from models import Station

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Source registry metadata  (read by server.py /api/sources)
# ---------------------------------------------------------------------------

SOURCE_NAME = "Pacific NW Water Monitoring"
SOURCE_DESCRIPTION = (
    "Pacific Northwest state water monitoring networks — Oregon OWRD stream "
    "gauges, Washington Ecology freshwater data (streamflow + water quality), "
    "Idaho DOT RWIS road weather, Idaho SNOTEL metadata, and Puget Sound "
    "Vital Signs ecosystem indicators.  No authentication required."
)
SOURCE_META = {
    "url": "https://apps.wrd.state.or.us/apps/sw/hydro_near_real_time/",
    "auth_required": False,
    "update_frequency": "15 min – 1 hr",
    "coverage": "Oregon, Washington, Idaho (Pacific Northwest US)",
    "params": [
        "streamflow", "stream_stage", "water_temperature",
        "dissolved_oxygen", "pH", "conductivity", "turbidity",
        "road_weather", "snow_water_equivalent",
    ],
    "category": "water",
}

# ---------------------------------------------------------------------------
# Router
# ---------------------------------------------------------------------------

router = APIRouter(prefix="/api/pnw_water", tags=["Pacific NW Water Monitoring"])

# ---------------------------------------------------------------------------
# API base URLs
# ---------------------------------------------------------------------------

# 1. Oregon OWRD — hydro near real time
OWRD_BASE = "https://apps.wrd.state.or.us/apps/sw/hydro_near_real_time"

# 2. Washington Ecology — continuous flow and WQ
WA_ECO_BASE = "https://apps.ecology.wa.gov/continuousflowandwq"

# 3. Idaho DOT RWIS (ArcGIS)
IDOT_RWIS_URL = (
    "https://gis.itd.idaho.gov/arcgisprod/rest/services/ArcGISOnline"
    "/IdahoTransportationLayersForOpenData/MapServer/141/query"
)

# 4. Idaho SNOTEL (ArcGIS via IDWR)
ID_SNOTEL_URL = (
    "https://gis.idwr.idaho.gov/hosting/rest/services/Reference"
    "/NrcsSnotelStations/MapServer/0/query"
)

# 5. Puget Sound Vital Signs
PUGET_SOUND_BASE = "https://vitalsigns.pugetsoundinfo.wa.gov"

# ---------------------------------------------------------------------------
# HTTP helpers
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


def _arcgis_query_params(
    where: str = "1=1",
    out_fields: str = "*",
    result_offset: int = 0,
    result_record_count: int = 1000,
) -> dict:
    """Build standard ArcGIS REST query parameters."""
    return {
        "where": where,
        "outFields": out_fields,
        "f": "json",
        "resultOffset": result_offset,
        "resultRecordCount": result_record_count,
        "returnGeometry": "true",
        "spatialRel": "esriSpatialRelIntersects",
    }


def _parse_arcgis_feature(
    feature: dict,
    source_key: str,
    network_label: str,
) -> Optional[Station]:
    """Convert an ArcGIS feature dict into a Station model."""
    attrs = feature.get("attributes", {})
    geom = feature.get("geometry", {})

    # Station ID — try common field names
    station_id = None
    for id_key in (
        "STATION_ID", "StationID", "Station_ID", "SITE_ID", "SiteID",
        "SITE", "STATION", "station_nm", "Name", "OBJECTID", "FID",
    ):
        val = attrs.get(id_key)
        if val is not None and str(val).strip():
            station_id = str(val).strip()
            break
    if station_id is None:
        return None

    # Station name
    name = None
    for name_key in (
        "STATION_NAME", "StationName", "Station_Name",
        "SITE_NAME", "SiteName", "Site_Name",
        "NAME", "Name", "DESCRIPTION", "DESC", "station_nm",
    ):
        val = attrs.get(name_key)
        if val and str(val).strip():
            name = str(val).strip()
            break
    name = name or station_id

    # Coordinates
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
    for elev_key in ("ELEVATION", "ELEV", "Elevation", "elev_ft", "ALT"):
        val = attrs.get(elev_key)
        if val is not None:
            try:
                elev = float(val)
                break
            except (TypeError, ValueError):
                pass

    # Extra metadata — keep non-ID/name/geometry attributes
    _skip = {
        "STATION_ID", "StationID", "Station_ID", "SITE_ID", "SiteID",
        "SITE", "STATION", "station_nm", "OBJECTID", "FID",
        "STATION_NAME", "StationName", "Station_Name", "SITE_NAME",
        "SiteName", "Site_Name", "NAME", "Name", "DESCRIPTION", "DESC",
        "ELEVATION", "ELEV", "Elevation", "elev_ft", "ALT",
    }
    extra = {k: v for k, v in attrs.items() if v is not None and k not in _skip}

    return Station(
        id=f"{source_key}_{station_id}",
        name=name,
        lat=lat,
        lon=lon,
        source="pnw_water",
        network=network_label,
        elevation=elev,
        extra=extra,
    )


async def _fetch_arcgis_layer(
    layer_url: str,
    source_key: str,
    network_label: str,
    max_records: int = 10000,
) -> list[Station]:
    """Fetch all features from an ArcGIS query endpoint with pagination."""
    stations: list[Station] = []
    offset = 0

    try:
        async with _client() as client:
            while True:
                params = _arcgis_query_params(result_offset=offset)
                resp = await client.get(layer_url, params=params)
                resp.raise_for_status()
                data = resp.json()

                if "error" in data:
                    logger.warning(
                        "%s ArcGIS error: %s",
                        source_key,
                        data["error"].get("message", data["error"]),
                    )
                    break

                features = data.get("features", [])
                if not features:
                    break

                for feat in features:
                    station = _parse_arcgis_feature(feat, source_key, network_label)
                    if station:
                        stations.append(station)

                if not data.get("exceededTransferLimit", False):
                    break
                offset += len(features)

                if offset > max_records:
                    logger.warning(
                        "%s: pagination cap hit at %d records",
                        source_key,
                        offset,
                    )
                    break

    except httpx.TimeoutException:
        logger.warning("%s fetch timed out at offset %d", source_key, offset)
    except httpx.HTTPStatusError as exc:
        logger.warning("%s HTTP error: %s", source_key, exc)
    except Exception:
        logger.exception("Unexpected error fetching %s", source_key)

    return stations


# ===========================================================================
# 1. Oregon OWRD Stream Gauges
# ===========================================================================

@router.get("/owrd/stations", summary="Oregon OWRD stream gauge stations")
async def owrd_stations():
    """Return Oregon Water Resources Department stream gauge stations.

    The OWRD hydro near real-time network includes state-operated gauges
    that are NOT part of the USGS network.  These provide streamflow,
    stage, and water temperature across Oregon.

    Source: https://apps.wrd.state.or.us/apps/sw/hydro_near_real_time/
    """
    cache_key = "pnw_water:owrd:stations"
    cached = await cache.get(cache_key)
    if cached is not None:
        return cached

    stations: list[dict] = []
    try:
        async with _client() as client:
            # OWRD provides a station listing page; we scrape the JSON feed
            # Try the data API endpoint first
            resp = await client.get(
                f"{OWRD_BASE}/hydro_download.aspx",
                params={
                    "dataset": "StationList",
                    "format": "json",
                },
            )
            if resp.status_code == 200:
                try:
                    data = resp.json()
                    if isinstance(data, list):
                        for rec in data:
                            sid = str(
                                rec.get("station_nbr")
                                or rec.get("StationNumber")
                                or rec.get("station_id", "")
                            ).strip()
                            if not sid:
                                continue
                            stations.append({
                                "id": f"owrd_{sid}",
                                "name": rec.get("station_name", rec.get("StationName", sid)),
                                "lat": _safe_float(rec.get("latitude", rec.get("Latitude"))),
                                "lon": _safe_float(rec.get("longitude", rec.get("Longitude"))),
                                "source": "pnw_water",
                                "network": "Oregon OWRD",
                                "params": ["streamflow", "stream_stage"],
                                "extra": {
                                    k: v for k, v in rec.items()
                                    if k not in {
                                        "station_nbr", "StationNumber", "station_id",
                                        "station_name", "StationName",
                                        "latitude", "Latitude",
                                        "longitude", "Longitude",
                                    }
                                    and v is not None
                                },
                            })
                except Exception:
                    logger.debug("OWRD JSON parse failed, trying HTML fallback")

            # Fallback: scrape the station listing page
            if not stations:
                resp = await client.get(f"{OWRD_BASE}/")
                if resp.status_code == 200:
                    # Extract station IDs from the HTML page
                    text = resp.text
                    # Look for station links in the page
                    ids = re.findall(
                        r'station_nbr=(\d+)', text
                    )
                    for sid in sorted(set(ids)):
                        stations.append({
                            "id": f"owrd_{sid}",
                            "name": f"OWRD Station {sid}",
                            "lat": 0.0,
                            "lon": 0.0,
                            "source": "pnw_water",
                            "network": "Oregon OWRD",
                            "params": ["streamflow", "stream_stage"],
                        })

    except httpx.TimeoutException:
        logger.warning("OWRD station list fetch timed out")
    except Exception:
        logger.exception("Error fetching OWRD stations")

    result = {
        "source": "owrd",
        "name": "Oregon Water Resources Department",
        "description": (
            "State-operated stream gauges beyond the USGS network — "
            "streamflow, stage, and water temperature across Oregon"
        ),
        "count": len(stations),
        "stations": stations,
    }
    await cache.set(cache_key, result, ttl=config.CACHE_TTL_LONG)
    return result


# ===========================================================================
# 2. Washington Ecology Freshwater DataStream
# ===========================================================================

@router.get(
    "/wa_ecology/stations",
    summary="Washington Ecology freshwater monitoring stations",
)
async def wa_ecology_stations():
    """Return Washington Department of Ecology continuous flow + WQ stations.

    The Freshwater DataStream provides 15–30 minute interval data via GOES
    satellite for streamflow, water temperature, dissolved oxygen, pH,
    conductivity, and turbidity.

    Source: https://apps.ecology.wa.gov/continuousflowandwq/
    """
    cache_key = "pnw_water:wa_ecology:stations"
    cached = await cache.get(cache_key)
    if cached is not None:
        return cached

    stations: list[dict] = []
    try:
        async with _client() as client:
            # WA Ecology provides a station data feed
            resp = await client.get(
                f"{WA_ECO_BASE}/StationList",
                params={"format": "json"},
            )
            if resp.status_code == 200:
                try:
                    data = resp.json()
                    records = data if isinstance(data, list) else data.get("stations", data.get("features", []))
                    for rec in records:
                        sid = str(
                            rec.get("Station")
                            or rec.get("StationCode")
                            or rec.get("station_id", "")
                        ).strip()
                        if not sid:
                            continue
                        stations.append({
                            "id": f"wa_eco_{sid}",
                            "name": rec.get("StationName", rec.get("Name", sid)),
                            "lat": _safe_float(rec.get("Latitude", rec.get("lat"))),
                            "lon": _safe_float(rec.get("Longitude", rec.get("lon"))),
                            "source": "pnw_water",
                            "network": "WA Ecology Freshwater",
                            "params": [
                                "streamflow", "water_temperature",
                                "dissolved_oxygen", "pH",
                                "conductivity", "turbidity",
                            ],
                            "extra": {
                                k: v for k, v in rec.items()
                                if k not in {
                                    "Station", "StationCode", "station_id",
                                    "StationName", "Name",
                                    "Latitude", "lat", "Longitude", "lon",
                                }
                                and v is not None
                            },
                        })
                except Exception:
                    logger.debug("WA Ecology JSON parse failed")

            # Fallback: scrape station page
            if not stations:
                resp = await client.get(f"{WA_ECO_BASE}/")
                if resp.status_code == 200:
                    ids = re.findall(
                        r'(?:Station|station)[=:][\s"]*([A-Z0-9]+)', resp.text
                    )
                    for sid in sorted(set(ids)):
                        stations.append({
                            "id": f"wa_eco_{sid}",
                            "name": f"WA Ecology Station {sid}",
                            "lat": 0.0,
                            "lon": 0.0,
                            "source": "pnw_water",
                            "network": "WA Ecology Freshwater",
                            "params": [
                                "streamflow", "water_temperature",
                                "dissolved_oxygen", "pH",
                                "conductivity", "turbidity",
                            ],
                        })

    except httpx.TimeoutException:
        logger.warning("WA Ecology station list fetch timed out")
    except Exception:
        logger.exception("Error fetching WA Ecology stations")

    result = {
        "source": "wa_ecology",
        "name": "Washington Dept of Ecology — Freshwater DataStream",
        "description": (
            "Continuous streamflow and water quality monitoring — temperature, "
            "dissolved oxygen, pH, conductivity, turbidity — at 15–30 min "
            "intervals via GOES satellite"
        ),
        "count": len(stations),
        "stations": stations,
    }
    await cache.set(cache_key, result, ttl=config.CACHE_TTL_LONG)
    return result


# ===========================================================================
# 3. Idaho DOT RWIS
# ===========================================================================

@router.get("/idot_rwis/stations", summary="Idaho DOT RWIS road weather stations")
async def idot_rwis_stations():
    """Return Idaho DOT Road Weather Information System (RWIS) stations.

    Provides road surface temperature, air temperature, wind, precipitation,
    and visibility data from road weather stations across Idaho.

    Source: ArcGIS — Idaho Transportation Layers for Open Data
    """
    cache_key = "pnw_water:idot_rwis:stations"
    cached = await cache.get(cache_key)
    if cached is not None:
        return cached

    stations = await _fetch_arcgis_layer(
        layer_url=IDOT_RWIS_URL,
        source_key="idot_rwis",
        network_label="Idaho DOT RWIS",
    )

    result = {
        "source": "idot_rwis",
        "name": "Idaho DOT Road Weather Information System",
        "description": (
            "Road weather stations across Idaho — surface temperature, "
            "air temperature, wind, precipitation, visibility"
        ),
        "count": len(stations),
        "stations": [s.model_dump() for s in stations],
    }
    await cache.set(cache_key, result, ttl=config.CACHE_TTL_LONG)
    return result


# ===========================================================================
# 4. Idaho SNOTEL Stations (via IDWR ArcGIS)
# ===========================================================================

@router.get(
    "/id_snotel/stations",
    summary="Idaho SNOTEL station metadata via IDWR",
)
async def id_snotel_stations():
    """Return NRCS SNOTEL station metadata from Idaho IDWR ArcGIS service.

    Provides station locations and metadata for SNOTEL sites in Idaho.
    This is the IDWR-hosted metadata layer; actual observations come from
    NRCS directly.

    Source: https://gis.idwr.idaho.gov/hosting/rest/services/Reference/NrcsSnotelStations/
    """
    cache_key = "pnw_water:id_snotel:stations"
    cached = await cache.get(cache_key)
    if cached is not None:
        return cached

    stations = await _fetch_arcgis_layer(
        layer_url=ID_SNOTEL_URL,
        source_key="id_snotel",
        network_label="Idaho SNOTEL (IDWR)",
    )

    result = {
        "source": "id_snotel",
        "name": "NRCS SNOTEL Stations — Idaho (via IDWR)",
        "description": (
            "SNOTEL station metadata from Idaho Dept of Water Resources — "
            "snow water equivalent, snow depth, precipitation, temperature"
        ),
        "count": len(stations),
        "stations": [s.model_dump() for s in stations],
    }
    await cache.set(cache_key, result, ttl=config.CACHE_TTL_LONG)
    return result


# ===========================================================================
# 5. Puget Sound Vital Signs
# ===========================================================================

# Known indicator keys from the Vital Signs portal
PUGET_SOUND_INDICATORS = [
    {"key": "chinook_salmon", "name": "Chinook Salmon", "category": "species_and_food_web"},
    {"key": "orcas", "name": "Orcas (Southern Resident Killer Whales)", "category": "species_and_food_web"},
    {"key": "birds", "name": "Birds", "category": "species_and_food_web"},
    {"key": "pacific_herring", "name": "Pacific Herring", "category": "species_and_food_web"},
    {"key": "marine_water_quality", "name": "Marine Water Quality", "category": "water_quality"},
    {"key": "freshwater_quality", "name": "Freshwater Quality", "category": "water_quality"},
    {"key": "marine_sediment_quality", "name": "Marine Sediment Quality", "category": "water_quality"},
    {"key": "toxics_in_fish", "name": "Toxics in Fish", "category": "water_quality"},
    {"key": "shellfish_beds", "name": "Shellfish Beds", "category": "water_quality"},
    {"key": "drinking_water", "name": "Drinking Water", "category": "water_quantity"},
    {"key": "stream_flow", "name": "Summer Stream Flows", "category": "water_quantity"},
    {"key": "estuaries", "name": "Estuaries", "category": "habitats"},
    {"key": "floodplains", "name": "Floodplains", "category": "habitats"},
    {"key": "forests", "name": "Forest Cover", "category": "habitats"},
    {"key": "land_development", "name": "Land Development Pressures", "category": "habitats"},
    {"key": "eelgrass", "name": "Eelgrass", "category": "habitats"},
    {"key": "shoreline_armoring", "name": "Shoreline Armoring", "category": "habitats"},
    {"key": "air_quality", "name": "Air Quality", "category": "human_health"},
    {"key": "economic_vitality", "name": "Economic Vitality", "category": "human_wellbeing"},
    {"key": "cultural_wellbeing", "name": "Cultural Wellbeing", "category": "human_wellbeing"},
    {"key": "outdoor_activity", "name": "Outdoor Activity", "category": "human_wellbeing"},
    {"key": "sound_stewardship", "name": "Sound Stewardship", "category": "human_wellbeing"},
    {"key": "local_foods", "name": "Local Foods", "category": "human_wellbeing"},
    {"key": "marine_survival", "name": "Marine Survival of Salmon", "category": "species_and_food_web"},
    {"key": "swimming_beaches", "name": "Swimming Beaches", "category": "human_health"},
]


@router.get(
    "/puget_sound/indicators",
    summary="Puget Sound Vital Signs — ecosystem indicators",
)
async def puget_sound_indicators():
    """Return Puget Sound Vital Signs ecosystem indicator catalog.

    The Puget Sound Partnership tracks 25 ecosystem indicators covering
    species, water quality, water quantity, habitats, and human wellbeing.

    Source: https://vitalsigns.pugetsoundinfo.wa.gov/
    """
    cache_key = "pnw_water:puget_sound:indicators"
    cached = await cache.get(cache_key)
    if cached is not None:
        return cached

    # Try to fetch live indicator data from the Vital Signs API
    indicators = list(PUGET_SOUND_INDICATORS)  # fallback to static catalog
    try:
        async with _client() as client:
            resp = await client.get(
                f"{PUGET_SOUND_BASE}/api/VitalSignIndicator",
            )
            if resp.status_code == 200:
                try:
                    data = resp.json()
                    if isinstance(data, list) and len(data) > 0:
                        indicators = []
                        for item in data:
                            indicators.append({
                                "key": item.get("key", item.get("id", "")),
                                "name": item.get("name", item.get("title", "")),
                                "category": item.get("category", ""),
                                "description": item.get("description", ""),
                                "status": item.get("status", ""),
                                "trend": item.get("trend", ""),
                            })
                except Exception:
                    logger.debug("Puget Sound indicators JSON parse failed")
    except httpx.TimeoutException:
        logger.warning("Puget Sound Vital Signs fetch timed out")
    except Exception:
        logger.exception("Error fetching Puget Sound indicators")

    result = {
        "source": "puget_sound",
        "name": "Puget Sound Vital Signs",
        "description": (
            "25 ecosystem indicators tracking the health of Puget Sound — "
            "species, water quality, habitats, and human wellbeing"
        ),
        "count": len(indicators),
        "indicators": indicators,
    }
    await cache.set(cache_key, result, ttl=config.CACHE_TTL_LONG)
    return result


# ===========================================================================
# Overview endpoint
# ===========================================================================

@router.get("/overview", summary="Pacific NW Water Monitoring — all sources overview")
async def pnw_water_overview():
    """Return a summary of all Pacific NW water monitoring sources.

    Provides counts and status for each sub-source without fetching full
    station lists (uses cached counts where available).
    """
    sources = [
        {
            "key": "owrd",
            "name": "Oregon OWRD Stream Gauges",
            "state": "OR",
            "url": OWRD_BASE,
            "endpoint": "/api/pnw_water/owrd/stations",
            "params": ["streamflow", "stream_stage", "water_temperature"],
            "description": "State-operated gauges beyond USGS network",
        },
        {
            "key": "wa_ecology",
            "name": "Washington Ecology Freshwater DataStream",
            "state": "WA",
            "url": WA_ECO_BASE,
            "endpoint": "/api/pnw_water/wa_ecology/stations",
            "params": [
                "streamflow", "water_temperature",
                "dissolved_oxygen", "pH", "conductivity", "turbidity",
            ],
            "description": (
                "Continuous flow + water quality at 15–30 min intervals "
                "via GOES satellite"
            ),
        },
        {
            "key": "idot_rwis",
            "name": "Idaho DOT RWIS",
            "state": "ID",
            "url": "https://gis.itd.idaho.gov/",
            "endpoint": "/api/pnw_water/idot_rwis/stations",
            "params": [
                "road_surface_temperature", "air_temperature",
                "wind", "precipitation", "visibility",
            ],
            "description": "Road weather stations across Idaho",
        },
        {
            "key": "id_snotel",
            "name": "Idaho SNOTEL Stations (IDWR)",
            "state": "ID",
            "url": "https://gis.idwr.idaho.gov/",
            "endpoint": "/api/pnw_water/id_snotel/stations",
            "params": [
                "snow_water_equivalent", "snow_depth",
                "precipitation", "temperature",
            ],
            "description": "SNOTEL station metadata from Idaho IDWR",
        },
        {
            "key": "puget_sound",
            "name": "Puget Sound Vital Signs",
            "state": "WA",
            "url": PUGET_SOUND_BASE,
            "endpoint": "/api/pnw_water/puget_sound/indicators",
            "params": [
                "water_quality", "species", "habitats",
                "human_wellbeing",
            ],
            "description": "25 ecosystem indicators for Puget Sound",
        },
    ]

    # Attach cached counts where available
    for src in sources:
        cached = await cache.get(f"pnw_water:{src['key']}:stations")
        if cached is None:
            cached = await cache.get(f"pnw_water:{src['key']}:indicators")
        if cached is not None:
            src["cached_count"] = cached.get("count", 0)

    return {
        "source": "pnw_water",
        "name": SOURCE_NAME,
        "description": SOURCE_DESCRIPTION,
        "sub_sources": sources,
        "total_sub_sources": len(sources),
    }


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _safe_float(val: Any, default: float = 0.0) -> float:
    """Convert a value to float, returning *default* on failure."""
    if val is None:
        return default
    try:
        return float(val)
    except (TypeError, ValueError):
        return default
