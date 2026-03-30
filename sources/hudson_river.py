"""Northeast River Monitoring — HRECOS + NH DES Real-Time Portal.

Two no-auth data sources covering the Hudson/Mohawk rivers and New Hampshire
lakes, rivers, and dams:

1. HRECOS (Hudson River Environmental Conditions Observing System)
   Water quality and weather at 15-min intervals along the Hudson and Mohawk
   rivers.  Post-2019 data available via USGS Water Services API (stations
   have USGS site numbers).  Legacy data hub at https://hrecos.org/
   Parameters: DO, temperature, salinity, turbidity, pH, conductivity,
   wind speed/direction, air temperature, barometric pressure, precipitation.

2. NH DES Real-Time Portal
   New Hampshire Dept. of Environmental Services real-time lake/river levels
   and dam bureau monitoring network.
   Portal: https://nhdes.rtiamanzi.org/

Both APIs are free with no authentication required.
"""
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
SOURCE_NAME = "Northeast River Monitoring"
SOURCE_DESCRIPTION = (
    "Hudson River Environmental Conditions Observing System (HRECOS) water "
    "quality and weather stations along the Hudson and Mohawk rivers, plus "
    "New Hampshire DES real-time lake/river levels and dam bureau network."
)
SOURCE_META = {
    "url": "https://hrecos.org/",
    "auth_required": False,
    "update_frequency": "15 min (HRECOS) / varies (NH DES)",
    "coverage": "Hudson River (NY), Mohawk River (NY), New Hampshire lakes and rivers",
    "params": [
        "dissolved_oxygen", "water_temperature", "salinity", "turbidity",
        "ph", "conductivity", "wind_speed", "wind_direction",
        "air_temperature", "barometric_pressure", "precipitation",
        "water_level",
    ],
    "category": "water,weather",
}

router = APIRouter(prefix="/api/ne_rivers", tags=["Northeast River Monitoring"])

# ---------------------------------------------------------------------------
# Constants — HRECOS via USGS Water Services
# ---------------------------------------------------------------------------
USGS_IV_BASE = "https://waterservices.usgs.gov/nwis/iv/"

# HRECOS stations with their USGS site numbers.
# Each entry: (usgs_site_no, name, lat, lon, river)
HRECOS_STATIONS: list[tuple[str, str, float, float, str]] = [
    ("01359165", "Albany",                  42.6437, -73.7517, "Hudson River"),
    ("01358000", "Coxsackie",              42.3533, -73.7936, "Hudson River"),
    ("01355001", "Schodack Island",         42.5047, -73.7742, "Hudson River"),
    ("01372058", "Norrie Point",            41.8312, -73.9397, "Hudson River"),
    ("01374019", "West Point",              41.3912, -73.9579, "Hudson River"),
    ("01374525", "Piermont",                41.0410, -73.9030, "Hudson River"),
    ("01376520", "George Washington Bridge", 40.8517, -73.9437, "Hudson River"),
    ("01359139", "Mohawk at Cohoes",        42.7794, -73.7061, "Mohawk River"),
    ("01335754", "Mohawk at Lock 8",        42.9322, -74.1961, "Mohawk River"),
]

# USGS parameter codes for HRECOS water quality + weather parameters
_USGS_PARAM_CODES = {
    "00010": ("water_temperature", "°C"),
    "00300": ("dissolved_oxygen", "mg/L"),
    "00095": ("conductivity", "µS/cm"),
    "00400": ("ph", "pH"),
    "63680": ("turbidity", "NTU"),
    "00480": ("salinity", "ppt"),
    "00035": ("wind_speed", "m/s"),
    "00036": ("wind_direction", "°"),
    "00020": ("air_temperature", "°C"),
    "00025": ("barometric_pressure", "mmHg"),
    "00045": ("precipitation", "mm"),
    "00065": ("water_level", "ft"),
}

# All parameter codes as a comma-separated string for USGS queries
_USGS_PARAM_LIST = ",".join(_USGS_PARAM_CODES.keys())

# ---------------------------------------------------------------------------
# Constants — NH DES Real-Time Portal
# ---------------------------------------------------------------------------
NHDES_BASE = "https://nhdes.rtiamanzi.org"
NHDES_API_STATIONS = f"{NHDES_BASE}/api/stations"
NHDES_API_LATEST = f"{NHDES_BASE}/api/latest"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _parse_usgs_iv(data: dict, source_tag: str = "hrecos") -> list[StationDetail]:
    """Parse USGS instantaneous-values JSON response into StationDetail list."""
    results: dict[str, StationDetail] = {}

    ts_list = data.get("value", {}).get("timeSeries", [])
    for ts in ts_list:
        site_info = ts.get("sourceInfo", {})
        site_code = site_info.get("siteCode", [{}])[0].get("value", "")
        site_name = site_info.get("siteName", site_code)
        geo = site_info.get("geoLocation", {}).get("geogLocation", {})
        lat = geo.get("latitude", 0.0)
        lon = geo.get("longitude", 0.0)

        # Variable info
        var_info = ts.get("variable", {})
        var_code = var_info.get("variableCode", [{}])[0].get("value", "")
        param_name, unit = _USGS_PARAM_CODES.get(var_code, (var_code, ""))

        # Parse values
        values = ts.get("values", [{}])[0].get("value", [])
        readings: list[Reading] = []
        for v in values:
            try:
                val = float(v.get("value", ""))
            except (TypeError, ValueError):
                continue
            # Skip USGS nodata sentinels
            if val == -999999.0:
                continue
            try:
                timestamp = datetime.fromisoformat(
                    v.get("dateTime", "").replace("Z", "+00:00")
                )
            except (TypeError, ValueError):
                continue
            readings.append(
                Reading(
                    station_id=site_code,
                    timestamp=timestamp,
                    parameter=param_name,
                    value=val,
                    unit=unit,
                    quality=v.get("qualifiers", [{}])[0].get("qualifierCode")
                    if v.get("qualifiers") else None,
                    source=source_tag,
                )
            )

        if site_code not in results:
            results[site_code] = StationDetail(
                station=Station(
                    id=site_code,
                    name=site_name,
                    lat=lat,
                    lon=lon,
                    source=source_tag,
                    network="HRECOS",
                    params=[],
                    url=f"https://waterdata.usgs.gov/nwis/uv?site_no={site_code}",
                ),
                readings=[],
            )

        detail = results[site_code]
        detail.readings.extend(readings)
        if param_name not in detail.station.params:
            detail.station.params.append(param_name)

    return list(results.values())


# ===========================================================================
# 1. HRECOS — Hudson/Mohawk stations
# ===========================================================================

@router.get(
    "/hudson/stations",
    summary="HRECOS station list",
)
async def hrecos_stations():
    """Return the list of HRECOS monitoring stations along the Hudson and
    Mohawk rivers with their USGS site numbers and coordinates.
    """
    cache_key = "ne_rivers:hrecos:stations"
    cached = await cache.get(cache_key)
    if cached:
        return cached

    stations = []
    for site_no, name, lat, lon, river in HRECOS_STATIONS:
        stations.append(
            Station(
                id=site_no,
                name=f"HRECOS {name}",
                lat=lat,
                lon=lon,
                source="hrecos",
                network="HRECOS",
                params=list(_USGS_PARAM_CODES.values()),
                url=f"https://waterdata.usgs.gov/nwis/uv?site_no={site_no}",
                extra={"river": river},
            ).model_dump()
        )

    result = {"stations": stations, "count": len(stations)}
    await cache.set(cache_key, result, ttl=config.CACHE_TTL_LONG)
    return result


@router.get(
    "/hudson/current",
    summary="HRECOS latest readings",
)
async def hrecos_current(
    site: Optional[str] = Query(
        None,
        description="USGS site number to filter (e.g. 01359165). If omitted, returns all HRECOS stations.",
    ),
):
    """Fetch the most recent observations from HRECOS stations via the USGS
    Water Services instantaneous-values API.  Returns water quality (DO, temp,
    salinity, turbidity, pH, conductivity) and weather (wind, air temp,
    pressure, precip) readings at 15-minute intervals.
    """
    cache_key = f"ne_rivers:hrecos:current:{site or 'all'}"
    cached = await cache.get(cache_key)
    if cached:
        return cached

    # Build site list
    if site:
        sites_param = site
    else:
        sites_param = ",".join(s[0] for s in HRECOS_STATIONS)

    params = {
        "format": "json",
        "sites": sites_param,
        "parameterCd": _USGS_PARAM_LIST,
        "siteStatus": "active",
    }

    try:
        data = await http_client.fetch_json(
            USGS_IV_BASE, params=params, source="hrecos", timeout=20
        )
    except Exception as exc:
        logger.error("HRECOS USGS fetch failed: %s", exc)
        raise HTTPException(status_code=502, detail=f"USGS API error: {exc}")

    details = _parse_usgs_iv(data, source_tag="hrecos")

    # Return only the latest reading per parameter per station
    result_stations = []
    for detail in details:
        latest: dict[str, Reading] = {}
        for r in detail.readings:
            if r.parameter not in latest or r.timestamp > latest[r.parameter].timestamp:
                latest[r.parameter] = r
        result_stations.append({
            "station": detail.station.model_dump(),
            "readings": [r.model_dump() for r in latest.values()],
            "reading_count": len(latest),
        })

    result = {
        "stations": result_stations,
        "count": len(result_stations),
        "retrieved_at": _now_utc().isoformat(),
    }
    await cache.set(cache_key, result, ttl=config.CACHE_TTL)
    return result


# ===========================================================================
# 2. NH DES Real-Time Portal
# ===========================================================================

@router.get(
    "/nh/levels",
    summary="NH DES lake/river levels",
)
async def nh_des_levels(
    station_id: Optional[str] = Query(
        None, description="Filter by NH DES station ID. If omitted, returns all stations."
    ),
):
    """Fetch real-time lake and river levels from the New Hampshire DES
    monitoring portal (dam bureau network and environmental monitoring).
    Data is served from https://nhdes.rtiamanzi.org/ with no authentication.
    """
    cache_key = f"ne_rivers:nh_des:levels:{station_id or 'all'}"
    cached = await cache.get(cache_key)
    if cached:
        return cached

    # Fetch station list
    try:
        stations_data = await http_client.fetch_json(
            NHDES_API_STATIONS, source="nh_des", timeout=15
        )
    except Exception as exc:
        logger.error("NH DES stations fetch failed: %s", exc)
        raise HTTPException(status_code=502, detail=f"NH DES API error: {exc}")

    # Fetch latest readings
    try:
        latest_data = await http_client.fetch_json(
            NHDES_API_LATEST, source="nh_des", timeout=15
        )
    except Exception as exc:
        logger.warning("NH DES latest readings fetch failed: %s", exc)
        latest_data = []

    # Build readings index (station_id -> list of readings)
    readings_by_station: dict[str, list[dict]] = {}
    if isinstance(latest_data, list):
        for item in latest_data:
            sid = str(item.get("stationId", item.get("station_id", "")))
            if sid:
                readings_by_station.setdefault(sid, []).append(item)
    elif isinstance(latest_data, dict):
        # Some portals return {stationId: [readings]}
        for sid, readings in latest_data.items():
            if isinstance(readings, list):
                readings_by_station[str(sid)] = readings

    # Parse stations
    result_stations = []
    station_list = stations_data if isinstance(stations_data, list) else stations_data.get("stations", stations_data.get("data", []))

    for stn in station_list:
        sid = str(stn.get("stationId", stn.get("station_id", stn.get("id", ""))))
        if not sid:
            continue
        if station_id and sid != station_id:
            continue

        try:
            lat = float(stn.get("latitude", stn.get("lat", 0.0)))
            lon = float(stn.get("longitude", stn.get("lon", stn.get("lng", 0.0))))
        except (TypeError, ValueError):
            lat, lon = 0.0, 0.0

        name = stn.get("name", stn.get("stationName", sid))

        # Build readings
        readings: list[Reading] = []
        for raw_r in readings_by_station.get(sid, []):
            param = raw_r.get("parameter", raw_r.get("param", "water_level"))
            unit = raw_r.get("unit", raw_r.get("units", "ft"))
            try:
                value = float(raw_r.get("value", raw_r.get("reading", 0)))
            except (TypeError, ValueError):
                continue

            ts_str = raw_r.get("timestamp", raw_r.get("dateTime", raw_r.get("date", "")))
            try:
                if ts_str:
                    ts = datetime.fromisoformat(str(ts_str).replace("Z", "+00:00"))
                else:
                    ts = _now_utc()
            except (TypeError, ValueError):
                ts = _now_utc()

            readings.append(
                Reading(
                    station_id=sid,
                    timestamp=ts,
                    parameter=str(param),
                    value=value,
                    unit=str(unit),
                    source="nh_des",
                )
            )

        station = Station(
            id=sid,
            name=name,
            lat=lat,
            lon=lon,
            source="nh_des",
            network="NH DES",
            params=[r.parameter for r in readings] if readings else ["water_level"],
            url=f"{NHDES_BASE}/station/{sid}",
        )

        result_stations.append({
            "station": station.model_dump(),
            "readings": [r.model_dump() for r in readings],
        })

    result = {
        "stations": result_stations,
        "count": len(result_stations),
        "retrieved_at": _now_utc().isoformat(),
    }
    await cache.set(cache_key, result, ttl=config.CACHE_TTL)
    return result


# ===========================================================================
# 3. Overview
# ===========================================================================

@router.get("/overview", summary="Northeast River Monitoring — source overview")
async def overview():
    """Return summary information about the Northeast River Monitoring sources."""
    return {
        "module": SOURCE_NAME,
        "description": SOURCE_DESCRIPTION,
        "sources": [
            {
                "id": "hrecos",
                "name": "HRECOS — Hudson River Environmental Conditions Observing System",
                "description": (
                    "Water quality and weather monitoring at 15-minute intervals "
                    "along the Hudson and Mohawk rivers.  9 stations reporting DO, "
                    "temperature, salinity, turbidity, pH, conductivity, wind, "
                    "air temperature, pressure, and precipitation.  Post-2019 data "
                    "served via USGS Water Services API."
                ),
                "url": "https://hrecos.org/",
                "auth_required": False,
                "update_frequency": "15 min",
                "coverage": "Hudson River (NY) and Mohawk River (NY)",
                "station_count": len(HRECOS_STATIONS),
                "params": [
                    "dissolved_oxygen", "water_temperature", "salinity",
                    "turbidity", "ph", "conductivity", "wind_speed",
                    "wind_direction", "air_temperature",
                    "barometric_pressure", "precipitation", "water_level",
                ],
                "endpoints": [
                    "GET /api/ne_rivers/hudson/stations",
                    "GET /api/ne_rivers/hudson/current?site={usgs_site_no}",
                ],
            },
            {
                "id": "nh_des",
                "name": "NH DES Real-Time Portal",
                "description": (
                    "New Hampshire Department of Environmental Services real-time "
                    "monitoring portal.  Lake and river levels, dam bureau network "
                    "data.  Documented API available on the portal homepage."
                ),
                "url": "https://nhdes.rtiamanzi.org/",
                "auth_required": False,
                "update_frequency": "varies",
                "coverage": "New Hampshire — lakes, rivers, and dams",
                "params": ["water_level"],
                "endpoints": [
                    "GET /api/ne_rivers/nh/levels?station_id={id}",
                ],
            },
        ],
        "meta": SOURCE_META,
    }
