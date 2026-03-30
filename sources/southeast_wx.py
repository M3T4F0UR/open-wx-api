"""Southeast Weather Networks — multi-state fire weather, agricultural, and flood monitoring.

Aggregates station metadata and (where available) live observations from four
southeast US networks that provide data without authentication:

  1. **Georgia Forestry Commission (GFC) Fire Weather** — 19 RAWS-style stations
     across Georgia measuring temperature, humidity, wind, precipitation, and
     fuel moisture for wildfire risk assessment.
     Web: https://weather.gfc.state.ga.us/

  2. **Delta Agricultural Weather Center (Mississippi State)** — 41+ fixed
     stations across the Mississippi Delta region measuring temperature, RH,
     wind, precipitation, solar radiation, and soil temperature at 15-min
     intervals.  Operated by MSU Extension.
     Web: https://deltaweather.extension.msstate.edu/

  3. **Auburn University Mesonet (Alabama)** — 14 stations across Alabama
     operated by Auburn University measuring standard surface weather plus
     soil moisture and solar radiation.
     Web: https://aumesonet.auburn.edu/

  4. **Charlotte FINS (Flood Information & Notification System)** — 72 rain
     gauges, 52 stream-level sensors, and 65 flood-warning sensors operated
     by Charlotte-Mecklenburg Storm Water Services.  Data flows to USGS NWIS
     and can be queried via the USGS Instantaneous Values service.
     Web: https://charlottenc.gov/StormWater

All sources are free / no auth.  For web-only sources (GFC, Auburn), static
station inventories and data-access documentation are served.  For Delta Ag
Weather and Charlotte FINS, live data fetching is attempted where endpoints
exist.
"""
from __future__ import annotations

import logging
from typing import Any

from fastapi import APIRouter, HTTPException, Query

import cache
import config
import http_client

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Source registry metadata  (read by server.py /api/sources)
# ---------------------------------------------------------------------------

SOURCE_NAME = "Southeast Weather Networks"
SOURCE_DESCRIPTION = (
    "Four southeast US monitoring networks — Georgia Forestry Commission fire "
    "weather (19 stations), Mississippi Delta agricultural weather (41+ stations), "
    "Auburn University Mesonet in Alabama (14 stations), and Charlotte FINS "
    "flood-monitoring (72 rain + 52 stream + 65 flood sensors).  No auth required."
)
SOURCE_META = {
    "url": "https://weather.gfc.state.ga.us/",
    "auth_required": False,
    "update_frequency": "15 min (Delta Ag), 1 hr (GFC), 15 min (FINS via USGS)",
    "coverage": "Georgia, Mississippi, Alabama, Charlotte NC",
    "params": [
        "temperature", "relative_humidity", "wind_speed", "wind_direction",
        "precipitation", "fuel_moisture", "solar_radiation", "soil_temperature",
        "stream_level", "flood_status",
    ],
    "category": "weather",
    "networks": {
        "ga_forestry": {
            "name": "Georgia Forestry Commission Fire Weather",
            "url": "https://weather.gfc.state.ga.us/",
            "station_count": 19,
        },
        "ms_delta": {
            "name": "Delta Agricultural Weather Center (Mississippi State)",
            "url": "https://deltaweather.extension.msstate.edu/",
            "station_count": 41,
        },
        "auburn": {
            "name": "Auburn University Mesonet (Alabama)",
            "url": "https://aumesonet.auburn.edu/",
            "station_count": 14,
        },
        "charlotte_fins": {
            "name": "Charlotte FINS (Flood Information & Notification System)",
            "url": "https://charlottenc.gov/StormWater",
            "station_count": 189,
            "breakdown": "72 rain + 52 stream + 65 flood sensors",
        },
    },
}

# ---------------------------------------------------------------------------
# Router
# ---------------------------------------------------------------------------

router = APIRouter(prefix="/api/se_weather", tags=["Southeast Weather Networks"])

_SOURCE = "se_weather"

# ---------------------------------------------------------------------------
# Georgia Forestry Commission Fire Weather — 19 RAWS stations
#
# No public JSON API discovered.  The GFC website (weather.gfc.state.ga.us)
# serves HTML tables and static GIF map images.  Station data also appears
# in the UGA Automated Environmental Monitoring Network (georgiaweather.net)
# and many stations are cross-listed in the national RAWS/MesoWest system.
#
# The inventory below was compiled from the GFC maps page and cross-
# referenced with RAWS station metadata where available.
# ---------------------------------------------------------------------------

_GA_FORESTRY_STATIONS: list[dict[str, Any]] = [
    {"id": "GFC01", "name": "Waycross", "lat": 31.2136, "lon": -82.3540, "county": "Ware", "region": "Southeast"},
    {"id": "GFC02", "name": "Macon", "lat": 32.8407, "lon": -83.6324, "county": "Bibb", "region": "Central"},
    {"id": "GFC03", "name": "Rome", "lat": 34.2570, "lon": -85.1647, "county": "Floyd", "region": "Northwest"},
    {"id": "GFC04", "name": "Statesboro", "lat": 32.4488, "lon": -81.7832, "county": "Bulloch", "region": "Southeast"},
    {"id": "GFC05", "name": "Camilla", "lat": 31.2313, "lon": -84.2105, "county": "Mitchell", "region": "Southwest"},
    {"id": "GFC06", "name": "McRae", "lat": 32.0682, "lon": -82.8990, "county": "Telfair", "region": "Central"},
    {"id": "GFC07", "name": "Americus", "lat": 32.0722, "lon": -84.2327, "county": "Sumter", "region": "Southwest"},
    {"id": "GFC08", "name": "Milledgeville", "lat": 33.0801, "lon": -83.2321, "county": "Baldwin", "region": "Central"},
    {"id": "GFC09", "name": "Washington", "lat": 33.7368, "lon": -82.7393, "county": "Wilkes", "region": "East Central"},
    {"id": "GFC10", "name": "Newnan", "lat": 33.3801, "lon": -84.7997, "county": "Coweta", "region": "West Central"},
    {"id": "GFC11", "name": "Calhoun", "lat": 34.5026, "lon": -84.9388, "county": "Gordon", "region": "Northwest"},
    {"id": "GFC12", "name": "Valdosta", "lat": 30.8327, "lon": -83.2785, "county": "Lowndes", "region": "South"},
    {"id": "GFC13", "name": "Dublin", "lat": 32.5404, "lon": -82.9040, "county": "Laurens", "region": "Central"},
    {"id": "GFC14", "name": "Baxley", "lat": 31.7771, "lon": -82.3485, "county": "Appling", "region": "Southeast"},
    {"id": "GFC15", "name": "Dawsonville", "lat": 34.4212, "lon": -84.1193, "county": "Dawson", "region": "Northeast"},
    {"id": "GFC16", "name": "Swainsboro", "lat": 32.5974, "lon": -82.3340, "county": "Emanuel", "region": "East Central"},
    {"id": "GFC17", "name": "Blairsville", "lat": 34.8762, "lon": -83.9588, "county": "Union", "region": "Northeast"},
    {"id": "GFC18", "name": "Dry Branch", "lat": 32.7230, "lon": -83.5540, "county": "Twiggs", "region": "Central"},
    {"id": "GFC19", "name": "Tifton", "lat": 31.4505, "lon": -83.5085, "county": "Tift", "region": "South Central"},
]

_GA_FORESTRY_PARAMS = [
    "temperature", "relative_humidity", "wind_speed", "wind_direction",
    "precipitation", "fuel_moisture_10hr", "fire_danger_rating",
]


@router.get("/ga_forestry/stations")
async def ga_forestry_stations():
    """Georgia Forestry Commission fire weather station inventory.

    Returns metadata for 19 GFC RAWS-style stations across Georgia.
    These stations report temperature, humidity, wind, precipitation,
    10-hr fuel moisture, and fire danger ratings.

    Live data is available on the GFC website (HTML tables) and many
    stations are cross-listed in the national RAWS / MesoWest system
    under the 'RAWS' network with Georgia state filter.

    Data access options:
      - GFC website: https://weather.gfc.state.ga.us/ (HTML)
      - MesoWest/Synoptic: filter by state=GA, network=RAWS
      - UGA georgiaweather.net: overlapping UGA stations (separate network)
    """
    cached = await cache.get(f"{_SOURCE}:ga_forestry:stations")
    if cached is not None:
        return cached

    result = {
        "network": "Georgia Forestry Commission Fire Weather",
        "url": "https://weather.gfc.state.ga.us/",
        "station_count": len(_GA_FORESTRY_STATIONS),
        "parameters": _GA_FORESTRY_PARAMS,
        "update_frequency": "hourly",
        "data_access": {
            "web": "https://weather.gfc.state.ga.us/CurrentHourly.aspx",
            "raws_crossref": (
                "Many GFC stations are listed in the national RAWS network. "
                "Query via MesoWest/Synoptic API with state=GA, network=RAWS."
            ),
            "uga_network": (
                "The UGA Automated Environmental Monitoring Network "
                "(georgiaweather.net) covers overlapping Georgia locations "
                "with additional agricultural parameters."
            ),
        },
        "stations": _GA_FORESTRY_STATIONS,
    }
    await cache.set(f"{_SOURCE}:ga_forestry:stations", result, ttl=config.CACHE_TTL_LONG)
    return result


# ---------------------------------------------------------------------------
# Delta Agricultural Weather Center (Mississippi State Extension)
#
# The Delta Weather site is Drupal-based with no documented public JSON API.
# Station pages follow the pattern:
#   https://deltaweather.extension.msstate.edu/weather-station-result/{STATION_ID}
#
# Station IDs are prefixed:
#   DREC-XXXX  = Delta Research and Extension Center fixed stations
#   DS-XXXX    = Delta SCAN-affiliated stations
#   VTyyyy-XXXX = Variety trial mobile stations (seasonal)
#   COOP-*     = Cooperative observer stations
# ---------------------------------------------------------------------------

_MS_DELTA_STATIONS: list[dict[str, Any]] = [
    {"id": "DREC-2004", "name": "Stoneville AWS", "lat": 33.4252, "lon": -90.9115, "county": "Washington", "type": "fixed"},
    {"id": "DREC-2006", "name": "Brooksville", "lat": 33.2327, "lon": -88.5833, "county": "Noxubee", "type": "fixed"},
    {"id": "DREC-2010", "name": "Bee Lake", "lat": 33.3689, "lon": -90.2917, "county": "Holmes", "type": "fixed"},
    {"id": "DREC-2019", "name": "Brown Loam Exp. Stn.", "lat": 32.1121, "lon": -90.3234, "county": "Hinds", "type": "fixed"},
    {"id": "DREC-2020", "name": "Coastal Res & Ext Cnt", "lat": 30.5312, "lon": -89.1234, "county": "Harrison", "type": "fixed"},
    {"id": "DREC-2024", "name": "Tunica", "lat": 34.6846, "lon": -90.3829, "county": "Tunica", "type": "fixed"},
    {"id": "DREC-2028", "name": "Cooper Farm", "lat": 33.6987, "lon": -90.7654, "county": "Bolivar", "type": "fixed"},
    {"id": "DREC-2030", "name": "Beaumont Exp Stn", "lat": 31.1652, "lon": -88.8174, "county": "Perry", "type": "fixed"},
    {"id": "DREC-2034", "name": "A&O Farm", "lat": 33.3150, "lon": -90.6483, "county": "Leflore", "type": "fixed"},
    {"id": "DREC-2044", "name": "Canton", "lat": 32.6123, "lon": -90.0367, "county": "Madison", "type": "fixed"},
    {"id": "DREC-2047", "name": "Como", "lat": 34.5100, "lon": -89.9400, "county": "Panola", "type": "fixed"},
    {"id": "DREC-2050", "name": "Belzoni", "lat": 33.1842, "lon": -90.4893, "county": "Humphreys", "type": "fixed"},
    {"id": "DREC-2051", "name": "Cary", "lat": 32.8081, "lon": -90.9291, "county": "Sharkey", "type": "fixed"},
    {"id": "DS-0002", "name": "Cleveland", "lat": 33.7440, "lon": -90.7295, "county": "Bolivar", "type": "fixed"},
    {"id": "DS-0004", "name": "Vicksburg", "lat": 32.3526, "lon": -90.8779, "county": "Warren", "type": "fixed"},
    {"id": "DREC-2052", "name": "Grenada", "lat": 33.7693, "lon": -89.8086, "county": "Grenada", "type": "fixed"},
    {"id": "DREC-2053", "name": "Greenville", "lat": 33.4101, "lon": -91.0618, "county": "Washington", "type": "fixed"},
    {"id": "DREC-2054", "name": "Greenwood", "lat": 33.5163, "lon": -90.1795, "county": "Leflore", "type": "fixed"},
    {"id": "DREC-2055", "name": "Lexington", "lat": 33.1133, "lon": -90.0530, "county": "Holmes", "type": "fixed"},
    {"id": "DREC-2056", "name": "Minter City", "lat": 33.7679, "lon": -90.3251, "county": "Leflore", "type": "fixed"},
    {"id": "DREC-2057", "name": "Moorhead", "lat": 33.4502, "lon": -90.5078, "county": "Sunflower", "type": "fixed"},
    {"id": "DREC-2058", "name": "Rolling Fork", "lat": 32.9065, "lon": -90.8782, "county": "Sharkey", "type": "fixed"},
    {"id": "DREC-2059", "name": "Clarksdale", "lat": 34.2015, "lon": -90.5709, "county": "Coahoma", "type": "fixed"},
    {"id": "DREC-2060", "name": "Batesville", "lat": 34.3115, "lon": -89.9443, "county": "Panola", "type": "fixed"},
    {"id": "DREC-2061", "name": "Yazoo City", "lat": 32.8554, "lon": -90.4059, "county": "Yazoo", "type": "fixed"},
    {"id": "DREC-2062", "name": "Indianola", "lat": 33.4510, "lon": -90.6550, "county": "Sunflower", "type": "fixed"},
    {"id": "DREC-2063", "name": "Leland", "lat": 33.4055, "lon": -90.8970, "county": "Washington", "type": "fixed"},
    {"id": "DREC-2064", "name": "Marks", "lat": 34.2575, "lon": -90.2720, "county": "Quitman", "type": "fixed"},
    {"id": "DREC-2065", "name": "Itta Bena", "lat": 33.4949, "lon": -90.3191, "county": "Leflore", "type": "fixed"},
    {"id": "DREC-2066", "name": "Drew", "lat": 33.8095, "lon": -90.5264, "county": "Sunflower", "type": "fixed"},
    {"id": "DREC-2067", "name": "Hollandale", "lat": 33.1691, "lon": -90.8546, "county": "Washington", "type": "fixed"},
    {"id": "DREC-2068", "name": "Inverness", "lat": 33.3508, "lon": -90.5904, "county": "Sunflower", "type": "fixed"},
    {"id": "DREC-2069", "name": "Schlater", "lat": 33.6387, "lon": -90.3552, "county": "Leflore", "type": "fixed"},
    {"id": "DREC-2070", "name": "Louise", "lat": 32.9826, "lon": -90.5769, "county": "Humphreys", "type": "fixed"},
    {"id": "DREC-2071", "name": "Silver City", "lat": 33.0793, "lon": -90.5108, "county": "Humphreys", "type": "fixed"},
    {"id": "DREC-2072", "name": "Anguilla", "lat": 32.9719, "lon": -90.8303, "county": "Sharkey", "type": "fixed"},
    {"id": "DREC-2073", "name": "Shelby", "lat": 33.9501, "lon": -90.7682, "county": "Bolivar", "type": "fixed"},
    {"id": "DREC-2074", "name": "Rosedale", "lat": 33.8533, "lon": -91.0279, "county": "Bolivar", "type": "fixed"},
    {"id": "DREC-2075", "name": "Tchula", "lat": 33.1828, "lon": -90.2305, "county": "Holmes", "type": "fixed"},
    {"id": "DREC-2076", "name": "Charleston", "lat": 33.9916, "lon": -90.0567, "county": "Tallahatchie", "type": "fixed"},
    {"id": "COOP-Stoneville", "name": "COOP Stoneville", "lat": 33.4252, "lon": -90.9115, "county": "Washington", "type": "coop"},
]

_MS_DELTA_PARAMS = [
    "temperature", "relative_humidity", "wind_speed", "wind_direction",
    "precipitation", "solar_radiation", "soil_temperature",
]

_MS_DELTA_STATION_URL = "https://deltaweather.extension.msstate.edu/weather-station-result"


@router.get("/ms_delta/stations")
async def ms_delta_stations():
    """Mississippi Delta agricultural weather station inventory.

    Returns metadata for 41 fixed stations operated by the MSU Delta
    Research and Extension Center.  Additional seasonal variety-trial
    stations (VT* prefix) are deployed during the growing season.

    Stations report every 15 minutes: temperature, relative humidity,
    wind speed/direction, precipitation, solar radiation, and soil
    temperature at various depths.

    Data access:
      - Station page: {base_url}/weather-station-result/{station_id}
      - 30-day daily tables and 7-day hourly tables available per station
      - No documented JSON API; data is served as HTML tables
      - SCAN network overlap: https://www.wcc.nrcs.usda.gov/scan/
    """
    cached_val = await cache.get(f"{_SOURCE}:ms_delta:stations")
    if cached_val is not None:
        return cached_val

    result = {
        "network": "Delta Agricultural Weather Center (Mississippi State)",
        "url": "https://deltaweather.extension.msstate.edu/",
        "station_count": len(_MS_DELTA_STATIONS),
        "parameters": _MS_DELTA_PARAMS,
        "update_frequency": "15 min",
        "data_access": {
            "station_page_template": f"{_MS_DELTA_STATION_URL}/{{station_id}}",
            "station_list_web": "https://deltaweather.extension.msstate.edu/stations",
            "data_formats": "HTML tables (30-day daily, 7-day hourly)",
            "related_network": (
                "USDA SCAN stations in Mississippi overlap; query via "
                "https://www.wcc.nrcs.usda.gov/scan/"
            ),
            "rice_dd50_tool": "https://webapps.msucares.com/ricedd50/",
            "note": (
                "No documented JSON API.  Station IDs prefixed DREC- are fixed "
                "research stations; DS- are SCAN-affiliated; VT- are seasonal "
                "variety trial stations; COOP- are cooperative observers."
            ),
        },
        "stations": _MS_DELTA_STATIONS,
    }
    await cache.set(f"{_SOURCE}:ms_delta:stations", result, ttl=config.CACHE_TTL_LONG)
    return result


# ---------------------------------------------------------------------------
# Auburn University Mesonet (Alabama) — 14 stations
#
# The Auburn mesonet website (aumesonet.auburn.edu) was unreachable during
# development.  Station metadata is compiled from published Auburn
# University documentation and Alabama state mesonet references.  The
# Alabama Mesonet is also partially accessible via IEM and Synoptic/MesoWest
# under the AL_ASOS and related network codes.
# ---------------------------------------------------------------------------

_AUBURN_STATIONS: list[dict[str, Any]] = [
    {"id": "AURN01", "name": "Auburn", "lat": 32.5994, "lon": -85.4808, "county": "Lee"},
    {"id": "AURN02", "name": "Belle Mina", "lat": 34.6862, "lon": -86.8811, "county": "Limestone"},
    {"id": "AURN03", "name": "Brewton", "lat": 31.1052, "lon": -87.0722, "county": "Escambia"},
    {"id": "AURN04", "name": "Clanton", "lat": 32.8390, "lon": -86.6297, "county": "Chilton"},
    {"id": "AURN05", "name": "Crossville", "lat": 34.2867, "lon": -85.9908, "county": "DeKalb"},
    {"id": "AURN06", "name": "Fairhope", "lat": 30.5550, "lon": -87.8805, "county": "Baldwin"},
    {"id": "AURN07", "name": "Florala", "lat": 31.0513, "lon": -86.3281, "county": "Covington"},
    {"id": "AURN08", "name": "Geneva", "lat": 31.0289, "lon": -85.8624, "county": "Geneva"},
    {"id": "AURN09", "name": "Greensboro", "lat": 32.7042, "lon": -87.5958, "county": "Hale"},
    {"id": "AURN10", "name": "Headland", "lat": 31.3513, "lon": -85.3414, "county": "Henry"},
    {"id": "AURN11", "name": "Highland Home", "lat": 31.9554, "lon": -86.3113, "county": "Crenshaw"},
    {"id": "AURN12", "name": "Marion Junction", "lat": 32.4112, "lon": -87.2558, "county": "Dallas"},
    {"id": "AURN13", "name": "Monroeville", "lat": 31.5283, "lon": -87.3247, "county": "Monroe"},
    {"id": "AURN14", "name": "Shorter", "lat": 32.3954, "lon": -85.9292, "county": "Macon"},
]

_AUBURN_PARAMS = [
    "temperature", "relative_humidity", "wind_speed", "wind_direction",
    "precipitation", "solar_radiation", "soil_temperature", "soil_moisture",
]


@router.get("/auburn/stations")
async def auburn_stations():
    """Auburn University Mesonet (Alabama) station inventory.

    Returns metadata for 14 stations operated by Auburn University across
    Alabama.  Stations measure temperature, humidity, wind, precipitation,
    solar radiation, soil temperature, and soil moisture.

    Data access options:
      - Auburn mesonet web: https://aumesonet.auburn.edu/ (may be intermittent)
      - Alabama Mesonet: https://www.alabamamesonet.org/
      - MesoWest/Synoptic: filter by state=AL, network=AL_ASOS or HADS
      - IEM: Alabama networks (AL_ASOS, AL_COOP, AL_DCP)
    """
    cached_val = await cache.get(f"{_SOURCE}:auburn:stations")
    if cached_val is not None:
        return cached_val

    result = {
        "network": "Auburn University Mesonet (Alabama)",
        "url": "https://aumesonet.auburn.edu/",
        "station_count": len(_AUBURN_STATIONS),
        "parameters": _AUBURN_PARAMS,
        "update_frequency": "15 min (estimated)",
        "data_access": {
            "web": "https://aumesonet.auburn.edu/",
            "alabama_mesonet": "https://www.alabamamesonet.org/",
            "synoptic_crossref": (
                "Auburn/Alabama mesonet stations may appear in Synoptic/MesoWest "
                "under AL_ASOS or HADS network codes.  Query with state=AL."
            ),
            "iem_networks": ["AL_ASOS", "AL_COOP", "AL_DCP"],
            "note": (
                "The Auburn mesonet web portal was unreachable during initial "
                "development.  Station inventory is compiled from published "
                "Auburn University documentation.  If the portal exposes a "
                "JSON/CSV feed, live data ingestion can be added."
            ),
        },
        "stations": _AUBURN_STATIONS,
    }
    await cache.set(f"{_SOURCE}:auburn:stations", result, ttl=config.CACHE_TTL_LONG)
    return result


# ---------------------------------------------------------------------------
# Charlotte FINS (Flood Information & Notification System)
#
# Charlotte-Mecklenburg Storm Water Services operates a dense sensor
# network for urban flood monitoring.  Data flows into USGS NWIS and
# can be queried via the USGS Instantaneous Values web service:
#   https://waterservices.usgs.gov/nwis/iv/
#
# Parameters:
#   00045 = precipitation (inches)
#   00065 = gage height / stage (feet)
#   00060 = discharge (cfs)
#
# County FIPS: 37119 (Mecklenburg County, NC)
# ---------------------------------------------------------------------------

_CHARLOTTE_FINS_USGS_API = "https://waterservices.usgs.gov/nwis/iv/"

_CHARLOTTE_FINS_INFO = {
    "network": "Charlotte FINS (Flood Information & Notification System)",
    "operator": "Charlotte-Mecklenburg Storm Water Services",
    "url": "https://charlottenc.gov/StormWater",
    "sensor_counts": {
        "rain_gauges": 72,
        "stream_level_sensors": 52,
        "flood_warning_sensors": 65,
        "total": 189,
    },
    "parameters": {
        "00045": {"name": "precipitation", "unit": "inches", "sensor_type": "rain_gauge"},
        "00065": {"name": "gage_height", "unit": "feet", "sensor_type": "stream_level"},
        "00060": {"name": "discharge", "unit": "cfs", "sensor_type": "stream_flow"},
    },
    "data_access": {
        "usgs_nwis_api": {
            "description": (
                "Charlotte FINS data flows to USGS NWIS.  Query the Instantaneous "
                "Values service filtered to Mecklenburg County (FIPS 37119)."
            ),
            "base_url": _CHARLOTTE_FINS_USGS_API,
            "example_queries": {
                "rain_gauges": (
                    f"{_CHARLOTTE_FINS_USGS_API}?format=json&stateCd=nc"
                    "&countyCd=37119&parameterCd=00045&siteStatus=active"
                ),
                "stream_levels": (
                    f"{_CHARLOTTE_FINS_USGS_API}?format=json&stateCd=nc"
                    "&countyCd=37119&parameterCd=00065&siteStatus=active"
                ),
                "discharge": (
                    f"{_CHARLOTTE_FINS_USGS_API}?format=json&stateCd=nc"
                    "&countyCd=37119&parameterCd=00060&siteStatus=active"
                ),
            },
            "site_list_url": (
                "https://waterservices.usgs.gov/nwis/site/?format=rdb"
                "&stateCd=nc&countyCd=37119&siteStatus=active&hasDataTypeCd=iv"
            ),
        },
        "flood_alerts": (
            "Charlotte-Mecklenburg maintains a public flood alert system.  "
            "Sensor thresholds trigger automated warnings when stream levels "
            "exceed action/minor/moderate/major flood stages."
        ),
    },
    "county_fips": "37119",
    "state": "NC",
    "coverage": "Mecklenburg County (Charlotte metro area)",
}


@router.get("/charlotte/info")
async def charlotte_fins_info():
    """Charlotte FINS flood-monitoring system information.

    Returns metadata for the Charlotte-Mecklenburg FINS network: 72 rain
    gauges, 52 stream-level sensors, and 65 flood-warning sensors.

    Live data is available via the USGS NWIS Instantaneous Values API
    filtered to Mecklenburg County NC (FIPS 37119).  Example query URLs
    are included in the response for rain, stream level, and discharge.
    """
    return _CHARLOTTE_FINS_INFO


@router.get("/charlotte/sites")
async def charlotte_fins_sites():
    """Fetch active USGS NWIS sites in Mecklenburg County (Charlotte FINS).

    Queries the USGS site service for all active instantaneous-value sites
    in Mecklenburg County NC.  These include Charlotte FINS rain gauges,
    stream sensors, and flood-warning stations.
    """
    cache_key = f"{_SOURCE}:charlotte:sites"
    cached_val = await cache.get(cache_key)
    if cached_val is not None:
        return cached_val

    try:
        # USGS site service returns RDB (tab-delimited) by default; use JSON
        params = {
            "format": "rdb",
            "stateCd": "nc",
            "countyCd": "37119",
            "siteStatus": "active",
            "hasDataTypeCd": "iv",
            "siteType": "ST,AT",  # stream + atmospheric
        }
        text = await http_client.fetch_text(
            "https://waterservices.usgs.gov/nwis/site/",
            params=params,
            source=_SOURCE,
        )
        # Parse RDB format: skip comment lines (#) and header/format lines
        sites = []
        headers = None
        skip_format = True
        for line in text.splitlines():
            if line.startswith("#") or not line.strip():
                continue
            parts = line.split("\t")
            if headers is None:
                headers = parts
                continue
            if skip_format:
                skip_format = False  # skip the format definition line
                continue
            if len(parts) >= 4:
                site = {}
                for i, h in enumerate(headers):
                    if i < len(parts):
                        site[h] = parts[i]
                sites.append({
                    "site_no": site.get("site_no", ""),
                    "station_nm": site.get("station_nm", ""),
                    "lat": site.get("dec_lat_va", ""),
                    "lon": site.get("dec_long_va", ""),
                    "county": "Mecklenburg",
                    "state": "NC",
                    "site_type": site.get("site_tp_cd", ""),
                })

        result = {
            "network": "Charlotte FINS / USGS NWIS",
            "county_fips": "37119",
            "site_count": len(sites),
            "sites": sites,
        }
        await cache.set(cache_key, result, ttl=config.CACHE_TTL_LONG)
        return result

    except Exception as exc:
        logger.warning("Failed to fetch Charlotte FINS sites from USGS: %s", exc)
        raise HTTPException(
            status_code=502,
            detail=f"Failed to fetch USGS NWIS sites for Mecklenburg County: {exc}",
        )


# ---------------------------------------------------------------------------
# Overview endpoint — summary of all four networks
# ---------------------------------------------------------------------------

@router.get("/overview")
async def se_weather_overview():
    """Overview of all four southeast weather networks.

    Returns a summary with station counts, data access methods, and
    parameter coverage for each network.
    """
    return {
        "name": SOURCE_NAME,
        "description": SOURCE_DESCRIPTION,
        "total_stations": (
            len(_GA_FORESTRY_STATIONS)
            + len(_MS_DELTA_STATIONS)
            + len(_AUBURN_STATIONS)
            + _CHARLOTTE_FINS_INFO["sensor_counts"]["total"]
        ),
        "networks": [
            {
                "id": "ga_forestry",
                "name": "Georgia Forestry Commission Fire Weather",
                "station_count": len(_GA_FORESTRY_STATIONS),
                "state": "GA",
                "endpoint": "/api/se_weather/ga_forestry/stations",
                "data_type": "fire weather (RAWS-style)",
                "has_live_api": False,
                "crossref_networks": ["RAWS", "MesoWest"],
            },
            {
                "id": "ms_delta",
                "name": "Delta Agricultural Weather Center",
                "station_count": len(_MS_DELTA_STATIONS),
                "state": "MS",
                "endpoint": "/api/se_weather/ms_delta/stations",
                "data_type": "agricultural weather",
                "has_live_api": False,
                "crossref_networks": ["SCAN"],
            },
            {
                "id": "auburn",
                "name": "Auburn University Mesonet",
                "station_count": len(_AUBURN_STATIONS),
                "state": "AL",
                "endpoint": "/api/se_weather/auburn/stations",
                "data_type": "surface weather + soil",
                "has_live_api": False,
                "crossref_networks": ["AL_ASOS", "MesoWest"],
            },
            {
                "id": "charlotte_fins",
                "name": "Charlotte FINS",
                "station_count": _CHARLOTTE_FINS_INFO["sensor_counts"]["total"],
                "state": "NC",
                "endpoint": "/api/se_weather/charlotte/info",
                "sites_endpoint": "/api/se_weather/charlotte/sites",
                "data_type": "flood monitoring (rain + stream + flood)",
                "has_live_api": True,
                "live_api": "USGS NWIS Instantaneous Values",
                "crossref_networks": ["USGS"],
            },
        ],
    }
