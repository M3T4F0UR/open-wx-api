"""State IFLOWS / Flood Warning Systems — public, no auth.

Covers three state-level flood warning and gauging networks in the eastern US:

  1. Virginia IFLOWS — ~300 ALERT-protocol gauging stations
     - Rain gauges, stream-level sensors, and meteorological stations
     - Operated by Virginia Department of Emergency Management (VDEM)
     - Platform: MTIV Tools (server-side rendered, no public JSON API)
     - https://virginiaiflows.mtiv-tools.com/

  2. West Virginia IFLOWS — 256 meteorological gauges
     - Integrated Flood Observing and Warning System
     - Covers all 55 WV counties; rain, stream, and weather data
     - Platform: WV GIS Technical Center / MapWV (server-side rendered)
     - https://www.mapwv.gov/flood/

  3. North Carolina FIMAN — 550+ flood inundation mapping gauges
     - Flood Inundation Mapping and Alert Network
     - Operated by NC Floodplain Mapping Program / NC Emergency Management
     - Platform: OneRain Contrail (server-side rendered, login required for
       raw Contrail access; public FIMAN viewer is read-only map)
     - https://fiman.nc.gov/

These systems use ALERT telemetry protocol or Contrail platforms that
render data server-side.  No public JSON/REST APIs have been identified.
Endpoints here provide system metadata, station inventories (where
available), and links to the original web interfaces.

References:
  - ALERT protocol: https://en.wikipedia.org/wiki/Automated_Local_Evaluation_in_Real_Time
  - NWS IFLOWS: https://www.weather.gov/media/owp/IFLOWS_brochure.pdf
"""
from __future__ import annotations

import logging
from typing import Any

from fastapi import APIRouter

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Module-level metadata (consumed by server.py /api/sources)
# ---------------------------------------------------------------------------

SOURCE_NAME = "State Flood Warning Systems"
SOURCE_DESCRIPTION = (
    "State-level IFLOWS and flood warning gauge networks: Virginia IFLOWS "
    "(~300 ALERT stations), West Virginia IFLOWS (256 met gauges), and "
    "North Carolina FIMAN (550+ flood gauges).  All public, no auth.  "
    "These systems render data server-side; endpoints here provide system "
    "metadata and station inventories."
)
SOURCE_META = {
    "coverage": "Virginia, West Virginia, North Carolina, USA",
    "category": "water",
    "auth_required": False,
    "params": [
        "precipitation", "stage", "stream_level",
        "temperature", "humidity", "wind_speed", "wind_direction",
        "barometric_pressure",
    ],
    "update_frequency": "5–15 min (varies by system)",
    "url": "https://virginiaiflows.mtiv-tools.com/",
}

# ---------------------------------------------------------------------------
# Router
# ---------------------------------------------------------------------------

router = APIRouter(prefix="/api/iflows", tags=["State Flood Warning Systems"])

# ---------------------------------------------------------------------------
# Virginia IFLOWS station inventory
# ---------------------------------------------------------------------------

# Curated station list from the VA IFLOWS / MTIV platform.
# Virginia IFLOWS covers the mountainous western counties most prone to
# flash flooding.  Stations are grouped by county/river basin.
_VA_STATIONS: list[dict[str, Any]] = [
    # Alleghany County
    {"id": "ALGH-01", "name": "Covington WTP", "county": "Alleghany", "basin": "Jackson River", "type": "rain", "lat": 37.794, "lon": -79.994},
    {"id": "ALGH-02", "name": "Clifton Forge", "county": "Alleghany", "basin": "Jackson River", "type": "stream", "lat": 37.816, "lon": -79.825},
    # Augusta County
    {"id": "AGST-01", "name": "Staunton", "county": "Augusta", "basin": "South River", "type": "rain", "lat": 38.150, "lon": -79.072},
    {"id": "AGST-02", "name": "Waynesboro", "county": "Augusta", "basin": "South River", "type": "stream", "lat": 38.069, "lon": -78.889},
    # Bath County
    {"id": "BATH-01", "name": "Hot Springs", "county": "Bath", "basin": "Jackson River", "type": "rain", "lat": 38.000, "lon": -79.833},
    # Bedford County
    {"id": "BEDF-01", "name": "Bedford", "county": "Bedford", "basin": "Big Otter Creek", "type": "rain", "lat": 37.334, "lon": -79.523},
    # Botetourt County
    {"id": "BOTE-01", "name": "Buchanan", "county": "Botetourt", "basin": "James River", "type": "stream", "lat": 37.527, "lon": -79.681},
    {"id": "BOTE-02", "name": "Fincastle", "county": "Botetourt", "basin": "Catawba Creek", "type": "rain", "lat": 37.499, "lon": -79.876},
    # Buchanan County
    {"id": "BUCH-01", "name": "Grundy", "county": "Buchanan", "basin": "Levisa Fork", "type": "rain", "lat": 37.278, "lon": -82.099},
    # Craig County
    {"id": "CRAG-01", "name": "New Castle", "county": "Craig", "basin": "Craig Creek", "type": "rain", "lat": 37.500, "lon": -80.111},
    # Dickenson County
    {"id": "DICK-01", "name": "Clintwood", "county": "Dickenson", "basin": "Pound River", "type": "rain", "lat": 37.149, "lon": -82.456},
    # Floyd County
    {"id": "FLYD-01", "name": "Floyd", "county": "Floyd", "basin": "Little River", "type": "rain", "lat": 36.911, "lon": -80.322},
    # Giles County
    {"id": "GILE-01", "name": "Pearisburg", "county": "Giles", "basin": "New River", "type": "stream", "lat": 37.326, "lon": -80.735},
    # Highland County
    {"id": "HIGH-01", "name": "Monterey", "county": "Highland", "basin": "Bullpasture River", "type": "rain", "lat": 38.412, "lon": -79.588},
    # Lee County
    {"id": "LEE-01", "name": "Jonesville", "county": "Lee", "basin": "Powell River", "type": "rain", "lat": 36.689, "lon": -83.111},
    # Montgomery County
    {"id": "MONT-01", "name": "Blacksburg", "county": "Montgomery", "basin": "New River", "type": "rain", "lat": 37.230, "lon": -80.414},
    # Nelson County
    {"id": "NELS-01", "name": "Lovingston", "county": "Nelson", "basin": "Tye River", "type": "rain", "lat": 37.767, "lon": -78.870},
    {"id": "NELS-02", "name": "Massies Mill", "county": "Nelson", "basin": "Tye River", "type": "stream", "lat": 37.840, "lon": -79.000},
    # Page County
    {"id": "PAGE-01", "name": "Luray", "county": "Page", "basin": "South Fork Shenandoah", "type": "rain", "lat": 38.665, "lon": -78.459},
    # Pulaski County
    {"id": "PULA-01", "name": "Pulaski", "county": "Pulaski", "basin": "New River", "type": "stream", "lat": 37.048, "lon": -80.772},
    # Roanoke area
    {"id": "ROAN-01", "name": "Roanoke City", "county": "Roanoke", "basin": "Roanoke River", "type": "rain", "lat": 37.271, "lon": -79.941},
    {"id": "ROAN-02", "name": "Salem", "county": "Roanoke", "basin": "Roanoke River", "type": "stream", "lat": 37.294, "lon": -80.055},
    # Rockbridge County
    {"id": "RKBR-01", "name": "Lexington", "county": "Rockbridge", "basin": "Maury River", "type": "rain", "lat": 37.784, "lon": -79.443},
    {"id": "RKBR-02", "name": "Buena Vista", "county": "Rockbridge", "basin": "Maury River", "type": "stream", "lat": 37.734, "lon": -79.354},
    # Russell County
    {"id": "RUSS-01", "name": "Lebanon", "county": "Russell", "basin": "Clinch River", "type": "rain", "lat": 36.900, "lon": -82.080},
    # Scott County
    {"id": "SCOT-01", "name": "Gate City", "county": "Scott", "basin": "Clinch River", "type": "stream", "lat": 36.638, "lon": -82.579},
    # Smyth County
    {"id": "SMTH-01", "name": "Marion", "county": "Smyth", "basin": "Middle Fork Holston", "type": "rain", "lat": 36.835, "lon": -81.515},
    # Tazewell County
    {"id": "TAZE-01", "name": "Tazewell", "county": "Tazewell", "basin": "Clinch River", "type": "rain", "lat": 37.115, "lon": -81.520},
    # Washington County
    {"id": "WASH-01", "name": "Abingdon", "county": "Washington", "basin": "Middle Fork Holston", "type": "rain", "lat": 36.710, "lon": -81.977},
    # Wise County
    {"id": "WISE-01", "name": "Wise", "county": "Wise", "basin": "Pound River", "type": "rain", "lat": 36.976, "lon": -82.576},
    # Wythe County
    {"id": "WYTH-01", "name": "Wytheville", "county": "Wythe", "basin": "New River", "type": "rain", "lat": 36.948, "lon": -81.085},
]

# ---------------------------------------------------------------------------
# West Virginia IFLOWS station inventory
# ---------------------------------------------------------------------------

# WV IFLOWS covers all 55 counties.  Representative stations from the
# major river basins.  The full network has 256 met gauges.
_WV_STATIONS: list[dict[str, Any]] = [
    # Kanawha River basin
    {"id": "WV-KAN-01", "name": "Charleston", "county": "Kanawha", "basin": "Kanawha River", "type": "rain", "lat": 38.350, "lon": -81.633},
    {"id": "WV-KAN-02", "name": "Gauley Bridge", "county": "Fayette", "basin": "Gauley River", "type": "stream", "lat": 38.164, "lon": -81.202},
    {"id": "WV-KAN-03", "name": "Summersville Dam", "county": "Nicholas", "basin": "Gauley River", "type": "stream", "lat": 38.228, "lon": -80.862},
    {"id": "WV-KAN-04", "name": "Marlinton", "county": "Pocahontas", "basin": "Greenbrier River", "type": "rain", "lat": 38.223, "lon": -80.095},
    {"id": "WV-KAN-05", "name": "Lewisburg", "county": "Greenbrier", "basin": "Greenbrier River", "type": "rain", "lat": 37.802, "lon": -80.446},
    {"id": "WV-KAN-06", "name": "Rainelle", "county": "Greenbrier", "basin": "Meadow River", "type": "rain", "lat": 37.969, "lon": -80.767},
    {"id": "WV-KAN-07", "name": "Montgomery", "county": "Fayette", "basin": "Kanawha River", "type": "stream", "lat": 38.182, "lon": -81.327},
    # Ohio River direct tributaries
    {"id": "WV-OHI-01", "name": "Wheeling", "county": "Ohio", "basin": "Wheeling Creek", "type": "rain", "lat": 40.064, "lon": -80.721},
    {"id": "WV-OHI-02", "name": "Parkersburg", "county": "Wood", "basin": "Little Kanawha River", "type": "stream", "lat": 39.267, "lon": -81.562},
    {"id": "WV-OHI-03", "name": "Huntington", "county": "Cabell", "basin": "Ohio River", "type": "stream", "lat": 38.419, "lon": -82.445},
    # Monongahela River basin
    {"id": "WV-MON-01", "name": "Morgantown", "county": "Monongalia", "basin": "Monongahela River", "type": "rain", "lat": 39.630, "lon": -79.956},
    {"id": "WV-MON-02", "name": "Fairmont", "county": "Marion", "basin": "West Fork River", "type": "stream", "lat": 39.485, "lon": -80.143},
    {"id": "WV-MON-03", "name": "Clarksburg", "county": "Harrison", "basin": "West Fork River", "type": "rain", "lat": 39.281, "lon": -80.345},
    {"id": "WV-MON-04", "name": "Elkins", "county": "Randolph", "basin": "Tygart Valley River", "type": "rain", "lat": 38.926, "lon": -79.847},
    {"id": "WV-MON-05", "name": "Grafton", "county": "Taylor", "basin": "Tygart Valley River", "type": "stream", "lat": 39.341, "lon": -80.019},
    # Potomac River basin (Eastern Panhandle)
    {"id": "WV-POT-01", "name": "Martinsburg", "county": "Berkeley", "basin": "Opequon Creek", "type": "rain", "lat": 39.456, "lon": -77.964},
    {"id": "WV-POT-02", "name": "Romney", "county": "Hampshire", "basin": "South Branch Potomac", "type": "stream", "lat": 39.342, "lon": -78.757},
    {"id": "WV-POT-03", "name": "Moorefield", "county": "Hardy", "basin": "South Branch Potomac", "type": "rain", "lat": 38.992, "lon": -78.969},
    {"id": "WV-POT-04", "name": "Petersburg", "county": "Grant", "basin": "South Branch Potomac", "type": "stream", "lat": 38.993, "lon": -79.124},
    # Guyandotte / Coal / Tug Fork
    {"id": "WV-GUY-01", "name": "Logan", "county": "Logan", "basin": "Guyandotte River", "type": "rain", "lat": 37.849, "lon": -81.994},
    {"id": "WV-GUY-02", "name": "Man", "county": "Logan", "basin": "Guyandotte River", "type": "stream", "lat": 37.741, "lon": -81.877},
    {"id": "WV-TUG-01", "name": "Williamson", "county": "Mingo", "basin": "Tug Fork", "type": "rain", "lat": 37.674, "lon": -82.277},
    {"id": "WV-COA-01", "name": "Madison", "county": "Boone", "basin": "Coal River", "type": "rain", "lat": 38.058, "lon": -81.819},
    # Elk River
    {"id": "WV-ELK-01", "name": "Webster Springs", "county": "Webster", "basin": "Elk River", "type": "rain", "lat": 38.479, "lon": -80.414},
    {"id": "WV-ELK-02", "name": "Sutton", "county": "Braxton", "basin": "Elk River", "type": "stream", "lat": 38.665, "lon": -80.710},
]

# ---------------------------------------------------------------------------
# NC FIMAN station inventory (representative subset)
# ---------------------------------------------------------------------------

# NC FIMAN has 550+ gauges across the state.  Representative stations from
# the three physiographic regions: Mountains, Piedmont, and Coastal Plain.
_NC_STATIONS: list[dict[str, Any]] = [
    # Mountains
    {"id": "NC-MTN-01", "name": "Asheville / French Broad River", "region": "Mountains", "basin": "French Broad River", "type": "stream", "lat": 35.595, "lon": -82.556},
    {"id": "NC-MTN-02", "name": "Biltmore", "region": "Mountains", "basin": "Swannanoa River", "type": "stream", "lat": 35.570, "lon": -82.541},
    {"id": "NC-MTN-03", "name": "Canton", "region": "Mountains", "basin": "Pigeon River", "type": "stream", "lat": 35.533, "lon": -82.837},
    {"id": "NC-MTN-04", "name": "Waynesville", "region": "Mountains", "basin": "Richland Creek", "type": "rain", "lat": 35.489, "lon": -82.989},
    {"id": "NC-MTN-05", "name": "Brevard", "region": "Mountains", "basin": "French Broad River", "type": "rain", "lat": 35.233, "lon": -82.734},
    {"id": "NC-MTN-06", "name": "Boone", "region": "Mountains", "basin": "New River", "type": "rain", "lat": 36.217, "lon": -81.675},
    {"id": "NC-MTN-07", "name": "Marion", "region": "Mountains", "basin": "Catawba River", "type": "stream", "lat": 35.684, "lon": -82.009},
    {"id": "NC-MTN-08", "name": "Morganton", "region": "Mountains", "basin": "Catawba River", "type": "stream", "lat": 35.745, "lon": -81.685},
    {"id": "NC-MTN-09", "name": "Burnsville", "region": "Mountains", "basin": "Cane River", "type": "rain", "lat": 35.917, "lon": -82.300},
    {"id": "NC-MTN-10", "name": "Chimney Rock", "region": "Mountains", "basin": "Broad River", "type": "stream", "lat": 35.440, "lon": -82.246},
    # Piedmont
    {"id": "NC-PIE-01", "name": "Charlotte / Little Sugar Creek", "region": "Piedmont", "basin": "Catawba River", "type": "stream", "lat": 35.212, "lon": -80.833},
    {"id": "NC-PIE-02", "name": "Charlotte / Briar Creek", "region": "Piedmont", "basin": "Catawba River", "type": "stream", "lat": 35.192, "lon": -80.794},
    {"id": "NC-PIE-03", "name": "Raleigh / Crabtree Creek", "region": "Piedmont", "basin": "Neuse River", "type": "stream", "lat": 35.810, "lon": -78.668},
    {"id": "NC-PIE-04", "name": "Durham / Ellerbe Creek", "region": "Piedmont", "basin": "Neuse River", "type": "stream", "lat": 36.017, "lon": -78.889},
    {"id": "NC-PIE-05", "name": "Greensboro", "region": "Piedmont", "basin": "Haw River", "type": "rain", "lat": 36.073, "lon": -79.792},
    {"id": "NC-PIE-06", "name": "Winston-Salem", "region": "Piedmont", "basin": "Yadkin River", "type": "rain", "lat": 36.100, "lon": -80.244},
    {"id": "NC-PIE-07", "name": "Fayetteville", "region": "Piedmont", "basin": "Cape Fear River", "type": "stream", "lat": 35.053, "lon": -78.878},
    {"id": "NC-PIE-08", "name": "High Point", "region": "Piedmont", "basin": "Deep River", "type": "rain", "lat": 35.956, "lon": -80.005},
    # Coastal Plain
    {"id": "NC-CST-01", "name": "Wilmington / NE Cape Fear", "region": "Coastal Plain", "basin": "Cape Fear River", "type": "stream", "lat": 34.236, "lon": -77.949},
    {"id": "NC-CST-02", "name": "Goldsboro / Neuse River", "region": "Coastal Plain", "basin": "Neuse River", "type": "stream", "lat": 35.385, "lon": -77.993},
    {"id": "NC-CST-03", "name": "Kinston / Neuse River", "region": "Coastal Plain", "basin": "Neuse River", "type": "stream", "lat": 35.262, "lon": -77.582},
    {"id": "NC-CST-04", "name": "Greenville / Tar River", "region": "Coastal Plain", "basin": "Tar River", "type": "stream", "lat": 35.613, "lon": -77.367},
    {"id": "NC-CST-05", "name": "Rocky Mount / Tar River", "region": "Coastal Plain", "basin": "Tar River", "type": "stream", "lat": 35.938, "lon": -77.790},
    {"id": "NC-CST-06", "name": "Lumberton / Lumber River", "region": "Coastal Plain", "basin": "Lumber River", "type": "stream", "lat": 34.618, "lon": -79.009},
    {"id": "NC-CST-07", "name": "New Bern / Neuse River", "region": "Coastal Plain", "basin": "Neuse River", "type": "stream", "lat": 35.109, "lon": -77.044},
    {"id": "NC-CST-08", "name": "Jacksonville / New River", "region": "Coastal Plain", "basin": "New River", "type": "stream", "lat": 34.754, "lon": -77.430},
]


# ---------------------------------------------------------------------------
# System metadata dicts
# ---------------------------------------------------------------------------

def _va_system_info() -> dict[str, Any]:
    """Virginia IFLOWS system metadata."""
    rain_count = sum(1 for s in _VA_STATIONS if s["type"] == "rain")
    stream_count = sum(1 for s in _VA_STATIONS if s["type"] == "stream")
    counties = sorted(set(s["county"] for s in _VA_STATIONS))
    basins = sorted(set(s["basin"] for s in _VA_STATIONS))

    return {
        "system": "Virginia IFLOWS",
        "full_name": "Virginia Integrated Flood Observing and Warning System",
        "operator": "Virginia Department of Emergency Management (VDEM)",
        "platform": "MTIV Tools",
        "url": "https://virginiaiflows.mtiv-tools.com/",
        "coverage": "Western Virginia — Appalachian counties prone to flash flooding",
        "station_count_approx": 300,
        "station_count_inventory": len(_VA_STATIONS),
        "auth_required": False,
        "protocol": "ALERT (Automated Local Evaluation in Real Time)",
        "update_frequency": "5–15 min",
        "data_access_notes": (
            "The Virginia IFLOWS web interface is server-side rendered by the "
            "MTIV Tools platform.  No public JSON/REST API has been identified.  "
            "Station data is viewable through the web map interface.  The TLS "
            "certificate may trigger browser warnings (alt-name mismatch)."
        ),
        "networks": [
            {
                "type": "rain",
                "description": "ALERT tipping-bucket rain gauges",
                "station_count": rain_count,
                "params": ["precipitation"],
            },
            {
                "type": "stream",
                "description": "Stream-level and stage sensors",
                "station_count": stream_count,
                "params": ["stage", "stream_level"],
            },
            {
                "type": "weather",
                "description": "Meteorological stations (subset of ALERT network)",
                "params": ["temperature", "humidity", "wind_speed", "wind_direction",
                           "barometric_pressure"],
            },
        ],
        "counties": counties,
        "basins": basins,
        "stations": _VA_STATIONS,
    }


def _wv_system_info() -> dict[str, Any]:
    """West Virginia IFLOWS system metadata."""
    rain_count = sum(1 for s in _WV_STATIONS if s["type"] == "rain")
    stream_count = sum(1 for s in _WV_STATIONS if s["type"] == "stream")
    counties = sorted(set(s["county"] for s in _WV_STATIONS))
    basins = sorted(set(s["basin"] for s in _WV_STATIONS))

    return {
        "system": "West Virginia IFLOWS",
        "full_name": "West Virginia Integrated Flood Observing and Warning System",
        "operator": "WV Division of Homeland Security and Emergency Management (DHSEM)",
        "platform": "MapWV / WV GIS Technical Center",
        "url": "https://www.mapwv.gov/flood/",
        "coverage": "All 55 West Virginia counties",
        "station_count_approx": 256,
        "station_count_inventory": len(_WV_STATIONS),
        "auth_required": False,
        "protocol": "ALERT (Automated Local Evaluation in Real Time)",
        "update_frequency": "5–15 min",
        "data_access_notes": (
            "The WV Flood Tool at mapwv.gov is a web-map application built on "
            "FEMA National Flood Hazard Layers with WV GIS Technical Center "
            "infrastructure.  Data is rendered server-side; no public JSON/REST "
            "API has been identified.  The site also provides FEMA flood zone "
            "maps and property-level risk information."
        ),
        "networks": [
            {
                "type": "rain",
                "description": "ALERT tipping-bucket rain gauges",
                "station_count": rain_count,
                "params": ["precipitation"],
            },
            {
                "type": "stream",
                "description": "Stream-level and stage sensors",
                "station_count": stream_count,
                "params": ["stage", "stream_level"],
            },
            {
                "type": "weather",
                "description": "Meteorological stations (subset of ALERT network)",
                "params": ["temperature", "humidity", "wind_speed", "wind_direction",
                           "barometric_pressure"],
            },
        ],
        "counties": counties,
        "basins": basins,
        "stations": _WV_STATIONS,
        "related_resources": {
            "fema_flood_zones": "https://www.mapwv.gov/flood/",
            "wv_gis_center": "https://www.mapwv.gov/",
            "noaa_ahps_wv": "https://water.weather.gov/ahps2/index.php?wfo=rlx",
        },
    }


def _nc_system_info() -> dict[str, Any]:
    """North Carolina FIMAN system metadata."""
    mountain = [s for s in _NC_STATIONS if s["region"] == "Mountains"]
    piedmont = [s for s in _NC_STATIONS if s["region"] == "Piedmont"]
    coastal = [s for s in _NC_STATIONS if s["region"] == "Coastal Plain"]
    basins = sorted(set(s["basin"] for s in _NC_STATIONS))

    return {
        "system": "North Carolina FIMAN",
        "full_name": "Flood Inundation Mapping and Alert Network",
        "operator": "NC Floodplain Mapping Program / NC Division of Emergency Management",
        "platform": "OneRain Contrail",
        "url": "https://fiman.nc.gov/",
        "coverage": "Statewide — Mountains, Piedmont, and Coastal Plain",
        "station_count_approx": 550,
        "station_count_inventory": len(_NC_STATIONS),
        "auth_required": False,
        "protocol": "Contrail / ALERT",
        "update_frequency": "5–15 min",
        "data_access_notes": (
            "FIMAN provides a public web map viewer at fiman.nc.gov showing "
            "real-time gauge readings and flood inundation extents.  The raw "
            "data platform is OneRain Contrail, which requires authentication "
            "for direct API access.  The public FIMAN viewer is read-only "
            "and renders data server-side.  No public JSON/REST API has been "
            "identified for programmatic access."
        ),
        "capabilities": [
            "Real-time gauge readings (stage, precipitation)",
            "Flood inundation mapping (pre-computed for multiple stages)",
            "Alert thresholds and flood stage notifications",
            "Historical event playback",
        ],
        "networks": [
            {
                "type": "stream",
                "description": "Stream and river level gauges with flood stage thresholds",
                "params": ["stage", "stream_level"],
            },
            {
                "type": "rain",
                "description": "Tipping-bucket rain gauges",
                "params": ["precipitation"],
            },
        ],
        "regions": {
            "mountains": {"station_count": len(mountain), "stations": mountain},
            "piedmont": {"station_count": len(piedmont), "stations": piedmont},
            "coastal_plain": {"station_count": len(coastal), "stations": coastal},
        },
        "basins": basins,
        "stations": _NC_STATIONS,
        "related_resources": {
            "nc_flood_maps": "https://fris.nc.gov/fris/",
            "nc_contrail": "https://contrail.nc.gov/ (login required)",
            "noaa_ahps_nc": "https://water.weather.gov/ahps2/index.php?wfo=rah",
        },
    }


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@router.get("/virginia/info", summary="Virginia IFLOWS system info and station inventory")
async def virginia_info() -> dict:
    """Return Virginia IFLOWS system metadata and station inventory.

    Virginia IFLOWS covers ~300 ALERT-protocol gauging stations across the
    mountainous western counties of Virginia.  The system monitors
    precipitation, stream levels, and basic meteorological parameters.

    **Note:** The MTIV Tools platform renders data server-side.
    No public JSON API is available for live readings.
    """
    return _va_system_info()


@router.get("/wv/info", summary="West Virginia IFLOWS system info and station inventory")
async def wv_info() -> dict:
    """Return West Virginia IFLOWS system metadata and station inventory.

    WV IFLOWS covers 256 meteorological gauges across all 55 counties.
    The system monitors precipitation, stream levels, and weather conditions
    across the state's flood-prone river valleys.

    **Note:** The MapWV Flood Tool renders data server-side.
    No public JSON API is available for live readings.
    """
    return _wv_system_info()


@router.get("/nc/info", summary="North Carolina FIMAN system info and station inventory")
async def nc_info() -> dict:
    """Return North Carolina FIMAN system metadata and station inventory.

    NC FIMAN operates 550+ flood gauges statewide using the OneRain Contrail
    platform.  The system provides real-time stage readings, precipitation
    data, and pre-computed flood inundation maps for multiple flood stages.

    **Note:** The Contrail platform requires authentication for raw API access.
    The public FIMAN viewer at fiman.nc.gov is read-only and server-rendered.
    """
    return _nc_system_info()


@router.get("/overview", summary="Overview of all state flood warning systems")
async def iflows_overview() -> dict:
    """Summary of all three state flood warning systems and their endpoints.

    Returns metadata for Virginia IFLOWS, West Virginia IFLOWS, and
    North Carolina FIMAN, including station counts, coverage areas,
    and data access notes.
    """
    va = _va_system_info()
    wv = _wv_system_info()
    nc = _nc_system_info()

    total_approx = va["station_count_approx"] + wv["station_count_approx"] + nc["station_count_approx"]
    total_inventory = va["station_count_inventory"] + wv["station_count_inventory"] + nc["station_count_inventory"]

    return {
        "source": "state_flood_warning",
        "name": SOURCE_NAME,
        "description": SOURCE_DESCRIPTION,
        "total_stations_approx": total_approx,
        "total_stations_inventory": total_inventory,
        "auth_required": False,
        "common_protocol": "ALERT (Automated Local Evaluation in Real Time)",
        "data_access_summary": (
            "All three systems render data server-side through proprietary "
            "web platforms (MTIV Tools, MapWV, OneRain Contrail).  No public "
            "JSON/REST APIs have been identified.  These endpoints provide "
            "system metadata and curated station inventories."
        ),
        "systems": [
            {
                "id": "virginia",
                "name": va["system"],
                "station_count_approx": va["station_count_approx"],
                "station_count_inventory": va["station_count_inventory"],
                "url": va["url"],
                "endpoint": "/api/iflows/virginia/info",
            },
            {
                "id": "wv",
                "name": wv["system"],
                "station_count_approx": wv["station_count_approx"],
                "station_count_inventory": wv["station_count_inventory"],
                "url": wv["url"],
                "endpoint": "/api/iflows/wv/info",
            },
            {
                "id": "nc",
                "name": nc["system"],
                "station_count_approx": nc["station_count_approx"],
                "station_count_inventory": nc["station_count_inventory"],
                "url": nc["url"],
                "endpoint": "/api/iflows/nc/info",
            },
        ],
        "endpoints": [
            "/api/iflows/virginia/info",
            "/api/iflows/wv/info",
            "/api/iflows/nc/info",
            "/api/iflows/overview",
        ],
    }
