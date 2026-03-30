"""Esri Living Atlas — real-time feeds from ArcGIS Online public services.

All services are hosted on ArcGIS Online (services9.arcgis.com) with **no
authentication required**.  They are curated by Esri's Living Atlas team and
aggregate authoritative feeds from NOAA, USGS, NWS, EPA, and others into a
uniform ArcGIS FeatureServer query interface.

Implemented services:
  1. Live Stream Gauges (36K+ stations)
  2. NOAA METAR Wind (6K+ stations)
  3. USGS Seismic Data (real-time earthquakes)
  4. Monitored Global Volcanoes (161 volcanoes)
  5. VIIRS Thermal Hotspots / Fire Activity (747K+ detections)
  6. USA Wildfires (current incidents + perimeters)
  7. NWS Watches & Warnings
  8. US Drought Intensity
  9. Active Hurricanes
 10. NOAA Storm Reports
 11. Air Quality PM2.5 (OpenAQ, 7.9K+ stations)
 12. CAP Alerts Feed (global hazard alerts)

All endpoints share a common ArcGIS REST query pattern with automatic
pagination via ``exceededTransferLimit``.
"""
from __future__ import annotations

import logging
from typing import Optional

from fastapi import APIRouter, HTTPException, Query

import cache
import http_client
from models import Station

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Source registry metadata
# ---------------------------------------------------------------------------
SOURCE_NAME = "Esri Living Atlas"
SOURCE_DESCRIPTION = (
    "Real-time environmental feeds from Esri Living Atlas — stream gauges, "
    "METAR wind, earthquakes, volcanoes, fire hotspots, wildfires, NWS warnings, "
    "drought, hurricanes, storm reports, air quality, and global CAP alerts.  "
    "All public, no auth required."
)
SOURCE_META = {
    "url": "https://livingatlas.arcgis.com/",
    "auth_required": False,
    "update_frequency": "1–60 min (varies by service)",
    "coverage": "Global / US",
    "params": [
        "streamflow", "stage", "wind", "temperature", "magnitude",
        "fire", "drought", "air_quality", "alerts",
    ],
    "category": "multi-hazard",
}

router = APIRouter(prefix="/api/living_atlas", tags=["Esri Living Atlas"])

# ---------------------------------------------------------------------------
# Service URLs (all on services9.arcgis.com)
# ---------------------------------------------------------------------------
_BASE = "https://services9.arcgis.com/RHVPKKiFTONKtxq3/arcgis/rest/services"

URLS = {
    "stream_gauges": f"{_BASE}/Live_Stream_Gauges_v1/FeatureServer/0/query",
    "metar":         f"{_BASE}/NOAA_METAR_current_wind_speed_direction_v1/FeatureServer/0/query",
    "earthquakes":   f"{_BASE}/USGS_Seismic_Data_v1/FeatureServer/0/query",
    "volcanoes":     f"{_BASE}/Monitored_Global_USGS_Volcanos/FeatureServer/0/query",
    "fire_hotspots": f"{_BASE}/Satellite_VIIRS_Thermal_Hotspots_and_Fire_Activity/FeatureServer/0/query",
    "wildfires":     f"{_BASE}/USA_Wildfires_v1/FeatureServer/0/query",
    "warnings":      f"{_BASE}/NWS_Watches_Warnings_v1/FeatureServer/1/query",
    "drought":       f"{_BASE}/US_Drought_Intensity_v1/FeatureServer/3/query",
    "hurricanes":    f"{_BASE}/Active_Hurricanes_v1/FeatureServer/0/query",
    "storm_reports": f"{_BASE}/NOAA_storm_reports_v1/FeatureServer/4/query",
    "air_quality":   f"{_BASE}/Air_Quality_PM25_Latest_Results/FeatureServer/0/query",
    "global_alerts": f"{_BASE}/CAP_Alerts_Feed/FeatureServer/0/query",
}

# Default record counts per service (ArcGIS caps at 2000 per request)
_DEFAULT_LIMIT = 2000
_ARCGIS_MAX_PAGE = 2000


# ---------------------------------------------------------------------------
# Shared ArcGIS query helper with pagination
# ---------------------------------------------------------------------------
async def _arcgis_query(
    url: str,
    *,
    where: str = "1=1",
    out_fields: str = "*",
    limit: int = _DEFAULT_LIMIT,
    geometry: Optional[str] = None,
    geometry_type: str = "esriGeometryEnvelope",
    in_sr: int = 4326,
    spatial_rel: str = "esriSpatialRelIntersects",
    order_by: Optional[str] = None,
    source_label: str = "esri_living_atlas",
) -> list[dict]:
    """Query an ArcGIS FeatureServer with automatic pagination.

    Returns the raw list of feature dicts (each has ``attributes`` and
    optionally ``geometry``).
    """
    all_features: list[dict] = []
    offset = 0
    page_size = min(limit, _ARCGIS_MAX_PAGE)

    while len(all_features) < limit:
        remaining = limit - len(all_features)
        count = min(remaining, page_size)

        params: dict = {
            "where": where,
            "outFields": out_fields,
            "resultRecordCount": count,
            "resultOffset": offset,
            "f": "json",
            "returnGeometry": "true",
            "outSR": "4326",
        }
        if geometry:
            params["geometry"] = geometry
            params["geometryType"] = geometry_type
            params["inSR"] = str(in_sr)
            params["spatialRel"] = spatial_rel
        if order_by:
            params["orderByFields"] = order_by

        try:
            data = await http_client.fetch_json(url, params=params, source=source_label)
        except Exception as exc:
            logger.error("ArcGIS query failed (%s): %s", url.split("/services/")[1][:50], exc)
            raise HTTPException(status_code=502, detail=f"ArcGIS service unavailable: {exc}")

        if "error" in data:
            err = data["error"]
            raise HTTPException(
                status_code=502,
                detail=f"ArcGIS error {err.get('code')}: {err.get('message')}",
            )

        features = data.get("features", [])
        all_features.extend(features)

        if not data.get("exceededTransferLimit", False):
            break
        if len(features) == 0:
            break
        offset += len(features)

    return all_features[:limit]


def _attr(feat: dict) -> dict:
    """Shortcut to get feature attributes."""
    return feat.get("attributes", {})


def _geom_xy(feat: dict) -> tuple[float, float]:
    """Extract (lat, lon) from a feature geometry point."""
    geom = feat.get("geometry", {})
    return geom.get("y", 0.0), geom.get("x", 0.0)


# ---------------------------------------------------------------------------
# 1. Live Stream Gauges
# ---------------------------------------------------------------------------
@router.get("/stream_gauges")
async def stream_gauges(
    limit: int = Query(2000, le=10000, description="Max records"),
    state: Optional[str] = Query(None, description="US state abbreviation (e.g. CA)"),
):
    """Live stream gauge observations — stage (ft) and flow (cfs) from 36K+ stations."""
    where = "1=1"
    if state:
        where = f"state = '{state.upper()}'"

    ck = f"la:stream_gauges:{state}:{limit}"
    cached = await cache.get(ck)
    if cached is not None:
        return cached

    features = await _arcgis_query(
        URLS["stream_gauges"], where=where, limit=limit,
        out_fields="stationid,name,stage_ft,flow_cfs,status,lastupdate,state,url",
        source_label="esri_stream_gauges",
    )

    stations = []
    for f in features:
        a = _attr(f)
        lat, lon = _geom_xy(f)
        stations.append(Station(
            id=str(a.get("stationid", "")),
            name=a.get("name", "Unknown"),
            lat=lat,
            lon=lon,
            source="esri_stream_gauges",
            network="USGS via Esri",
            params=["stage_ft", "flow_cfs"],
            extra={
                "stage_ft": a.get("stage_ft"),
                "flow_cfs": a.get("flow_cfs"),
                "status": a.get("status"),
                "lastupdate": a.get("lastupdate"),
                "state": a.get("state"),
            },
        ).model_dump(mode="json"))

    result = {"count": len(stations), "service": "Live Stream Gauges", "stations": stations}
    await cache.set(ck, result, ttl=120)
    return result


# ---------------------------------------------------------------------------
# 2. NOAA METAR Wind
# ---------------------------------------------------------------------------
@router.get("/metar")
async def metar_wind(
    limit: int = Query(2000, le=10000, description="Max records"),
):
    """Current METAR wind speed/direction plus temperature and pressure from 6K+ stations."""
    ck = f"la:metar:{limit}"
    cached = await cache.get(ck)
    if cached is not None:
        return cached

    features = await _arcgis_query(
        URLS["metar"], limit=limit,
        out_fields="ICAO,STATION_NAME,TEMP,DEW_POINT,R_HUMIDITY,WIND_DIRECT,WIND_SPEED,PRESSURE",
        source_label="esri_metar",
    )

    stations = []
    for f in features:
        a = _attr(f)
        lat, lon = _geom_xy(f)
        stations.append(Station(
            id=a.get("ICAO", ""),
            name=a.get("STATION_NAME", "Unknown"),
            lat=lat,
            lon=lon,
            source="esri_metar",
            network="NOAA METAR via Esri",
            params=["temperature", "wind_speed", "wind_direction", "pressure"],
            extra={
                "icao": a.get("ICAO"),
                "temp": a.get("TEMP"),
                "dew_point": a.get("DEW_POINT"),
                "humidity": a.get("R_HUMIDITY"),
                "wind_direction": a.get("WIND_DIRECT"),
                "wind_speed": a.get("WIND_SPEED"),
                "pressure": a.get("PRESSURE"),
            },
        ).model_dump(mode="json"))

    result = {"count": len(stations), "service": "NOAA METAR Wind", "stations": stations}
    await cache.set(ck, result, ttl=120)
    return result


# ---------------------------------------------------------------------------
# 3. USGS Seismic Data
# ---------------------------------------------------------------------------
@router.get("/earthquakes")
async def earthquakes(
    min_mag: Optional[float] = Query(None, description="Minimum magnitude filter"),
    period: str = Query("day", description="Time period hint (not server-enforced)"),
    limit: int = Query(1000, le=10000, description="Max records"),
):
    """Real-time earthquake data from the USGS Seismic feed on ArcGIS."""
    where = "1=1"
    if min_mag is not None:
        where = f"mag >= {min_mag}"

    ck = f"la:earthquakes:{min_mag}:{period}:{limit}"
    cached = await cache.get(ck)
    if cached is not None:
        return cached

    features = await _arcgis_query(
        URLS["earthquakes"], where=where, limit=limit,
        order_by="time DESC",
        source_label="esri_seismic",
    )

    events = []
    for f in features:
        a = _attr(f)
        lat, lon = _geom_xy(f)
        events.append({
            "id": a.get("eventid") or a.get("OBJECTID"),
            "lat": lat,
            "lon": lon,
            "magnitude": a.get("mag"),
            "depth_km": a.get("depth"),
            "place": a.get("place"),
            "time": a.get("time"),
            "mag_type": a.get("magType"),
            "status": a.get("status"),
            "tsunami": a.get("tsunami"),
            "felt": a.get("felt"),
            "alert": a.get("alert"),
        })

    result = {"count": len(events), "service": "USGS Seismic Data", "earthquakes": events}
    await cache.set(ck, result, ttl=60)
    return result


# ---------------------------------------------------------------------------
# 4. Monitored Volcanoes
# ---------------------------------------------------------------------------
@router.get("/volcanoes")
async def volcanoes():
    """Monitored global volcanoes with USGS alert levels (161 volcanoes)."""
    ck = "la:volcanoes"
    cached = await cache.get(ck)
    if cached is not None:
        return cached

    features = await _arcgis_query(
        URLS["volcanoes"], limit=500, source_label="esri_volcanoes",
    )

    items = []
    for f in features:
        a = _attr(f)
        lat, lon = _geom_xy(f)
        items.append({
            "name": a.get("Volcano_Name") or a.get("name"),
            "lat": lat,
            "lon": lon,
            "alert_level": a.get("Alert_Level") or a.get("alert_level"),
            "color_code": a.get("Color_Code") or a.get("color_code"),
            "country": a.get("Country"),
            "elevation_m": a.get("Elevation") or a.get("elev"),
            "type": a.get("Type") or a.get("type"),
            "last_eruption": a.get("Last_Eruption") or a.get("last_eruption"),
            **{k: v for k, v in a.items() if k not in (
                "Volcano_Name", "name", "Alert_Level", "alert_level",
                "Color_Code", "color_code", "Country", "Elevation", "elev",
                "Type", "type", "Last_Eruption", "last_eruption", "OBJECTID",
            )},
        })

    result = {"count": len(items), "service": "Monitored Volcanoes", "volcanoes": items}
    await cache.set(ck, result, ttl=300)
    return result


# ---------------------------------------------------------------------------
# 5. VIIRS Thermal Hotspots
# ---------------------------------------------------------------------------
@router.get("/fire_hotspots")
async def fire_hotspots(
    bbox: Optional[str] = Query(
        None, description="Bounding box: west,south,east,north (e.g. -125,24,-66,50)"
    ),
    limit: int = Query(1000, le=10000, description="Max records"),
):
    """VIIRS satellite thermal hotspots and fire activity detections."""
    geometry = None
    if bbox:
        parts = bbox.split(",")
        if len(parts) == 4:
            geometry = f"{parts[0]},{parts[1]},{parts[2]},{parts[3]}"

    ck = f"la:fire_hotspots:{bbox}:{limit}"
    cached = await cache.get(ck)
    if cached is not None:
        return cached

    features = await _arcgis_query(
        URLS["fire_hotspots"], limit=limit, geometry=geometry,
        source_label="esri_viirs_hotspots",
    )

    items = []
    for f in features:
        a = _attr(f)
        lat, lon = _geom_xy(f)
        items.append({
            "lat": lat,
            "lon": lon,
            "brightness": a.get("brightness") or a.get("bright_ti4"),
            "confidence": a.get("confidence"),
            "frp": a.get("frp"),
            "acq_date": a.get("acq_date"),
            "acq_time": a.get("acq_time"),
            "satellite": a.get("satellite"),
            "daynight": a.get("daynight"),
            **{k: v for k, v in a.items() if k not in (
                "brightness", "bright_ti4", "confidence", "frp",
                "acq_date", "acq_time", "satellite", "daynight", "OBJECTID",
            )},
        })

    result = {"count": len(items), "service": "VIIRS Thermal Hotspots", "hotspots": items}
    await cache.set(ck, result, ttl=120)
    return result


# ---------------------------------------------------------------------------
# 6. USA Wildfires
# ---------------------------------------------------------------------------
@router.get("/wildfires")
async def wildfires(
    limit: int = Query(500, le=5000, description="Max records"),
):
    """Current US wildfire incidents from the Living Atlas USA Wildfires feed."""
    ck = f"la:wildfires:{limit}"
    cached = await cache.get(ck)
    if cached is not None:
        return cached

    features = await _arcgis_query(
        URLS["wildfires"], limit=limit, source_label="esri_wildfires",
    )

    items = []
    for f in features:
        a = _attr(f)
        lat, lon = _geom_xy(f)
        items.append({
            "name": a.get("IncidentName") or a.get("poly_IncidentName") or a.get("name"),
            "lat": lat,
            "lon": lon,
            "acres": a.get("GISAcres") or a.get("DailyAcres") or a.get("acres"),
            "contained": a.get("PercentContained") or a.get("percent_contained"),
            "fire_type": a.get("IncidentTypeCategory") or a.get("type"),
            "state": a.get("POOState") or a.get("state"),
            "discovered": a.get("FireDiscoveryDateTime") or a.get("discovered"),
            "cause": a.get("FireCause") or a.get("cause"),
            **{k: v for k, v in a.items() if k not in (
                "IncidentName", "poly_IncidentName", "name", "GISAcres",
                "DailyAcres", "acres", "PercentContained", "percent_contained",
                "IncidentTypeCategory", "type", "POOState", "state",
                "FireDiscoveryDateTime", "discovered", "FireCause", "cause",
                "OBJECTID",
            )},
        })

    result = {"count": len(items), "service": "USA Wildfires", "wildfires": items}
    await cache.set(ck, result, ttl=120)
    return result


# ---------------------------------------------------------------------------
# 7. NWS Watches & Warnings
# ---------------------------------------------------------------------------
@router.get("/warnings")
async def warnings(
    state: Optional[str] = Query(None, description="US state abbreviation (e.g. OR)"),
    limit: int = Query(1000, le=5000, description="Max records"),
):
    """Active NWS watches, warnings, and advisories."""
    where = "1=1"
    if state:
        # NWS warnings typically have a state field or can be filtered by zone
        where = f"state = '{state.upper()}' OR cap_areaDesc LIKE '%{state.upper()}%'"

    ck = f"la:warnings:{state}:{limit}"
    cached = await cache.get(ck)
    if cached is not None:
        return cached

    features = await _arcgis_query(
        URLS["warnings"], where=where, limit=limit,
        source_label="esri_nws_warnings",
    )

    items = []
    for f in features:
        a = _attr(f)
        items.append({
            "event": a.get("Event") or a.get("event") or a.get("prod_type"),
            "severity": a.get("Severity") or a.get("severity"),
            "urgency": a.get("Urgency") or a.get("urgency"),
            "headline": a.get("Headline") or a.get("headline"),
            "area": a.get("cap_areaDesc") or a.get("area"),
            "onset": a.get("Onset") or a.get("onset"),
            "expires": a.get("Expires") or a.get("expires") or a.get("expiration"),
            "status": a.get("Status") or a.get("status"),
            "msg_type": a.get("MsgType") or a.get("msg_type"),
            "wfo": a.get("WFO") or a.get("wfo"),
            **{k: v for k, v in a.items() if k not in (
                "Event", "event", "prod_type", "Severity", "severity",
                "Urgency", "urgency", "Headline", "headline",
                "cap_areaDesc", "area", "Onset", "onset", "Expires",
                "expires", "expiration", "Status", "status", "MsgType",
                "msg_type", "WFO", "wfo", "OBJECTID",
            )},
        })

    result = {"count": len(items), "service": "NWS Watches & Warnings", "warnings": items}
    await cache.set(ck, result, ttl=60)
    return result


# ---------------------------------------------------------------------------
# 8. US Drought Intensity
# ---------------------------------------------------------------------------
@router.get("/drought")
async def drought(
    limit: int = Query(500, le=5000, description="Max records"),
):
    """US Drought Monitor intensity polygons."""
    ck = f"la:drought:{limit}"
    cached = await cache.get(ck)
    if cached is not None:
        return cached

    features = await _arcgis_query(
        URLS["drought"], limit=limit, source_label="esri_drought",
    )

    items = []
    for f in features:
        a = _attr(f)
        items.append({
            "dm": a.get("DM") or a.get("dm"),
            "label": a.get("Label") or a.get("label"),
            "start_date": a.get("Start_Date") or a.get("start_date"),
            "end_date": a.get("End_Date") or a.get("end_date"),
            "state": a.get("State") or a.get("state"),
            **{k: v for k, v in a.items() if k not in (
                "DM", "dm", "Label", "label", "Start_Date", "start_date",
                "End_Date", "end_date", "State", "state", "OBJECTID",
            )},
        })

    result = {"count": len(items), "service": "US Drought Intensity", "drought": items}
    await cache.set(ck, result, ttl=600)
    return result


# ---------------------------------------------------------------------------
# 9. Active Hurricanes
# ---------------------------------------------------------------------------
@router.get("/hurricanes")
async def hurricanes():
    """Active hurricanes, tropical storms, and disturbances (NHC)."""
    ck = "la:hurricanes"
    cached = await cache.get(ck)
    if cached is not None:
        return cached

    features = await _arcgis_query(
        URLS["hurricanes"], limit=500, source_label="esri_hurricanes",
    )

    items = []
    for f in features:
        a = _attr(f)
        lat, lon = _geom_xy(f)
        items.append({
            "name": a.get("STORMNAME") or a.get("name"),
            "lat": lat,
            "lon": lon,
            "wind_kts": a.get("MAXWIND") or a.get("max_wind"),
            "pressure_mb": a.get("MSLP") or a.get("pressure"),
            "category": a.get("STORMTYPE") or a.get("storm_type"),
            "basin": a.get("BASIN") or a.get("basin"),
            "movement": a.get("MOVEMENT") or a.get("movement"),
            "timestamp": a.get("DTG") or a.get("timestamp"),
            **{k: v for k, v in a.items() if k not in (
                "STORMNAME", "name", "MAXWIND", "max_wind", "MSLP",
                "pressure", "STORMTYPE", "storm_type", "BASIN", "basin",
                "MOVEMENT", "movement", "DTG", "timestamp", "OBJECTID",
            )},
        })

    result = {"count": len(items), "service": "Active Hurricanes", "hurricanes": items}
    await cache.set(ck, result, ttl=120)
    return result


# ---------------------------------------------------------------------------
# 10. NOAA Storm Reports
# ---------------------------------------------------------------------------
@router.get("/storm_reports")
async def storm_reports(
    limit: int = Query(1000, le=10000, description="Max records"),
):
    """Recent NOAA storm reports (tornado, hail, wind, etc.)."""
    ck = f"la:storm_reports:{limit}"
    cached = await cache.get(ck)
    if cached is not None:
        return cached

    features = await _arcgis_query(
        URLS["storm_reports"], limit=limit,
        order_by="timestamp DESC",
        source_label="esri_storm_reports",
    )

    items = []
    for f in features:
        a = _attr(f)
        lat, lon = _geom_xy(f)
        items.append({
            "lat": lat,
            "lon": lon,
            "event_type": a.get("Event") or a.get("event_type") or a.get("type"),
            "location": a.get("Location") or a.get("location"),
            "state": a.get("State") or a.get("state"),
            "timestamp": a.get("timestamp") or a.get("Date"),
            "comments": a.get("Comments") or a.get("comments"),
            "magnitude": a.get("Magnitude") or a.get("magnitude"),
            "source": a.get("Source") or a.get("source"),
            **{k: v for k, v in a.items() if k not in (
                "Event", "event_type", "type", "Location", "location",
                "State", "state", "timestamp", "Date", "Comments",
                "comments", "Magnitude", "magnitude", "Source", "source",
                "OBJECTID",
            )},
        })

    result = {"count": len(items), "service": "NOAA Storm Reports", "storm_reports": items}
    await cache.set(ck, result, ttl=120)
    return result


# ---------------------------------------------------------------------------
# 11. Air Quality PM2.5
# ---------------------------------------------------------------------------
@router.get("/air_quality")
async def air_quality(
    limit: int = Query(2000, le=10000, description="Max records"),
):
    """Latest PM2.5 air quality readings from OpenAQ stations on ArcGIS."""
    ck = f"la:air_quality:{limit}"
    cached = await cache.get(ck)
    if cached is not None:
        return cached

    features = await _arcgis_query(
        URLS["air_quality"], limit=limit, source_label="esri_air_quality",
    )

    stations = []
    for f in features:
        a = _attr(f)
        lat, lon = _geom_xy(f)
        stations.append(Station(
            id=str(a.get("location") or a.get("OBJECTID", "")),
            name=a.get("location") or a.get("city") or "Unknown",
            lat=lat,
            lon=lon,
            source="esri_air_quality",
            network="OpenAQ via Esri",
            params=["pm25"],
            extra={
                "pm25": a.get("value") or a.get("pm25"),
                "unit": a.get("unit", "ug/m3"),
                "country": a.get("country"),
                "city": a.get("city"),
                "last_updated": a.get("lastUpdated") or a.get("date_utc"),
            },
        ).model_dump(mode="json"))

    result = {"count": len(stations), "service": "Air Quality PM2.5", "stations": stations}
    await cache.set(ck, result, ttl=300)
    return result


# ---------------------------------------------------------------------------
# 12. CAP Alerts Feed (Global)
# ---------------------------------------------------------------------------
@router.get("/global_alerts")
async def global_alerts(
    severity: Optional[str] = Query(None, description="Filter by severity (e.g. Extreme, Severe)"),
    limit: int = Query(1000, le=10000, description="Max records"),
):
    """Global Common Alerting Protocol (CAP) hazard alerts."""
    where = "1=1"
    if severity:
        where = f"severity = '{severity}'"

    ck = f"la:global_alerts:{severity}:{limit}"
    cached = await cache.get(ck)
    if cached is not None:
        return cached

    features = await _arcgis_query(
        URLS["global_alerts"], where=where, limit=limit,
        source_label="esri_cap_alerts",
    )

    items = []
    for f in features:
        a = _attr(f)
        lat, lon = _geom_xy(f)
        items.append({
            "lat": lat,
            "lon": lon,
            "event": a.get("event") or a.get("Event"),
            "severity": a.get("severity") or a.get("Severity"),
            "urgency": a.get("urgency") or a.get("Urgency"),
            "certainty": a.get("certainty") or a.get("Certainty"),
            "headline": a.get("headline") or a.get("Headline"),
            "description": a.get("description") or a.get("Description"),
            "sender": a.get("senderName") or a.get("sender"),
            "effective": a.get("effective") or a.get("Effective"),
            "expires": a.get("expires") or a.get("Expires"),
            "area": a.get("areaDesc") or a.get("area"),
            **{k: v for k, v in a.items() if k not in (
                "event", "Event", "severity", "Severity", "urgency", "Urgency",
                "certainty", "Certainty", "headline", "Headline", "description",
                "Description", "senderName", "sender", "effective", "Effective",
                "expires", "Expires", "areaDesc", "area", "OBJECTID",
            )},
        })

    result = {"count": len(items), "service": "CAP Alerts Feed", "alerts": items}
    await cache.set(ck, result, ttl=60)
    return result


# ---------------------------------------------------------------------------
# 13. Overview — list all available services
# ---------------------------------------------------------------------------
@router.get("/overview")
async def overview():
    """List all available Esri Living Atlas services with their endpoints and URLs."""
    services = [
        {
            "name": "Live Stream Gauges",
            "endpoint": "/api/living_atlas/stream_gauges",
            "url": URLS["stream_gauges"].replace("/query", ""),
            "description": "36K+ USGS stream gauge stations — stage (ft) and flow (cfs)",
            "params": "limit, state",
        },
        {
            "name": "NOAA METAR Wind",
            "endpoint": "/api/living_atlas/metar",
            "url": URLS["metar"].replace("/query", ""),
            "description": "6K+ METAR stations — wind, temperature, pressure, humidity",
            "params": "limit",
        },
        {
            "name": "USGS Seismic Data",
            "endpoint": "/api/living_atlas/earthquakes",
            "url": URLS["earthquakes"].replace("/query", ""),
            "description": "Real-time earthquakes — magnitude, depth, location",
            "params": "min_mag, period, limit",
        },
        {
            "name": "Monitored Volcanoes",
            "endpoint": "/api/living_atlas/volcanoes",
            "url": URLS["volcanoes"].replace("/query", ""),
            "description": "161 USGS-monitored volcanoes with alert levels",
            "params": "(none)",
        },
        {
            "name": "VIIRS Thermal Hotspots",
            "endpoint": "/api/living_atlas/fire_hotspots",
            "url": URLS["fire_hotspots"].replace("/query", ""),
            "description": "747K+ satellite fire detections — brightness, FRP, confidence",
            "params": "bbox, limit",
        },
        {
            "name": "USA Wildfires",
            "endpoint": "/api/living_atlas/wildfires",
            "url": URLS["wildfires"].replace("/query", ""),
            "description": "Current wildfire incidents — acres, containment, cause",
            "params": "limit",
        },
        {
            "name": "NWS Watches & Warnings",
            "endpoint": "/api/living_atlas/warnings",
            "url": URLS["warnings"].replace("/query", ""),
            "description": "Active NWS watches, warnings, and advisories",
            "params": "state, limit",
        },
        {
            "name": "US Drought Intensity",
            "endpoint": "/api/living_atlas/drought",
            "url": URLS["drought"].replace("/query", ""),
            "description": "US Drought Monitor intensity polygons",
            "params": "limit",
        },
        {
            "name": "Active Hurricanes",
            "endpoint": "/api/living_atlas/hurricanes",
            "url": URLS["hurricanes"].replace("/query", ""),
            "description": "Active hurricanes, tropical storms, and disturbances",
            "params": "(none)",
        },
        {
            "name": "NOAA Storm Reports",
            "endpoint": "/api/living_atlas/storm_reports",
            "url": URLS["storm_reports"].replace("/query", ""),
            "description": "Recent tornado, hail, and wind damage reports",
            "params": "limit",
        },
        {
            "name": "Air Quality PM2.5",
            "endpoint": "/api/living_atlas/air_quality",
            "url": URLS["air_quality"].replace("/query", ""),
            "description": "7.9K+ OpenAQ PM2.5 monitoring stations",
            "params": "limit",
        },
        {
            "name": "CAP Alerts Feed",
            "endpoint": "/api/living_atlas/global_alerts",
            "url": URLS["global_alerts"].replace("/query", ""),
            "description": "6.4K+ global Common Alerting Protocol hazard alerts",
            "params": "severity, limit",
        },
    ]

    return {
        "source": SOURCE_NAME,
        "base": _BASE,
        "auth_required": False,
        "service_count": len(services),
        "services": services,
    }
