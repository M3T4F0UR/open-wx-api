"""Specialty environmental monitoring networks — no authentication required.

Six specialty networks covering precipitation chemistry, aerosol/visibility,
rural air quality, UV-B radiation, and research-site hydroclimatology:

1. TVA Rain Gauge Network   — 345 gauges across the Tennessee Valley
2. CASTNET                  — 90 rural/remote air-quality monitoring sites
3. IMPROVE                  — 160 national park aerosol/visibility sites
4. NADP NTN                 — 250 precipitation chemistry (acid rain) sites
5. USDA UV-B Monitoring     — 30+ agricultural UV-B stations
6. ClimDB/HydroDB           — 45 LTER + USFS long-term research sites

All networks provide publicly available data via CSV download or web query
interfaces.  Where direct API access is available, live data endpoints are
provided.  For web-only / FTP-only sources, metadata endpoints document the
access path and station inventory.

TVA Lake Info:    https://lakeinfo.tva.gov/
CASTNET:          https://www.epa.gov/castnet
IMPROVE:          https://vista.cira.colostate.edu/Improve/
NADP NTN:         https://nadp.slh.wisc.edu/
USDA UV-B:        https://carhenge.nrel.colostate.edu/UVB/
ClimDB/HydroDB:   https://climhy.lternet.edu/
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Optional

from fastapi import APIRouter, HTTPException, Query

import cache
import http_client

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Source registry metadata  (read by server.py /api/sources)
# ---------------------------------------------------------------------------

SOURCE_NAME = "Specialty Environmental Networks"
SOURCE_DESCRIPTION = (
    "Six specialty environmental monitoring networks: "
    "(1) TVA Rain Gauge Network — 345 precipitation gauges across the Tennessee Valley; "
    "(2) CASTNET — 90 rural/remote air-quality sites (weekly chemistry + met); "
    "(3) IMPROVE — 160 national park aerosol and visibility monitoring sites; "
    "(4) NADP NTN — 250 precipitation chemistry (acid rain) sites; "
    "(5) USDA UV-B — 30+ agricultural UV-B radiation stations; "
    "(6) ClimDB/HydroDB — 45 LTER and USFS long-term research watersheds.  "
    "All data is publicly available; no authentication required."
)
SOURCE_META = {
    "url": "https://www.epa.gov/castnet",
    "auth_required": False,
    "update_frequency": "varies (15 min – weekly, see per-network docs)",
    "coverage": "Continental US (TVA: Tennessee Valley; others: national)",
    "params": [
        "precipitation", "rainfall",
        "ozone", "sulfate", "nitrate", "ammonium",
        "aerosol_mass", "light_extinction", "visibility",
        "wet_deposition", "acid_rain", "ph",
        "uv_b_irradiance", "erythemal_dose",
        "streamflow", "air_temperature", "precipitation_research",
    ],
    "category": "environmental",
}

router = APIRouter(prefix="/api/specialty_env", tags=["Specialty Environmental Networks"])

SOURCE = "specialty_env"

# Cache TTLs (seconds)
TTL_OVERVIEW = 3600       # 1 hr  — mostly static metadata
TTL_STATIONS = 86400      # 24 hr — station lists rarely change
TTL_DATA = 900            # 15 min — for live-ish data where available

# ═══════════════════════════════════════════════════════════════════════════
# 1.  TVA Rain Gauge Network
# ═══════════════════════════════════════════════════════════════════════════

TVA_PRECIP_URL = "https://lakeinfo.tva.gov/web/precip.htm"
TVA_LAKE_INFO_URL = "https://lakeinfo.tva.gov"

# Representative TVA rain gauge stations (subset — full inventory is 345+)
# Coordinates from TVA public lake-levels documentation.
TVA_GAUGE_REGIONS: dict[str, dict[str, Any]] = {
    "upper_tennessee": {
        "name": "Upper Tennessee River Basin",
        "states": ["TN", "NC", "VA"],
        "approx_gauges": 95,
        "description": "Headwaters region — French Broad, Holston, Nolichucky drainages",
    },
    "middle_tennessee": {
        "name": "Middle Tennessee River Basin",
        "states": ["TN", "AL"],
        "approx_gauges": 85,
        "description": "Watts Bar to Pickwick — Clinch, Elk, Duck River drainages",
    },
    "lower_tennessee": {
        "name": "Lower Tennessee / Kentucky Lake",
        "states": ["TN", "KY", "MS", "AL"],
        "approx_gauges": 65,
        "description": "Kentucky Lake and downstream tributaries",
    },
    "cumberland": {
        "name": "Cumberland River Basin",
        "states": ["TN", "KY"],
        "approx_gauges": 60,
        "description": "Cumberland drainage — TVA cooperative gauges",
    },
    "tributary": {
        "name": "Tributary / Small Watershed Gauges",
        "states": ["TN", "NC", "GA", "AL", "KY"],
        "approx_gauges": 40,
        "description": "Small-basin and headwater monitoring gauges",
    },
}


@router.get("/tva/info")
async def tva_info():
    """TVA Rain Gauge Network — access information and regional inventory.

    The TVA operates 345+ rain gauges across the Tennessee Valley for
    reservoir operations and flood forecasting.  Data is available through
    a web query interface; there is no public REST API.

    Returns regional breakdown, access URLs, and data format notes.
    """
    return {
        "network": "TVA Rain Gauge Network",
        "operator": "Tennessee Valley Authority",
        "description": (
            "345+ tipping-bucket rain gauges across the Tennessee Valley, "
            "operated for reservoir management and flood forecasting.  "
            "Data is available through the TVA Lake Info web interface."
        ),
        "total_gauges": 345,
        "data_access": {
            "web_interface": TVA_PRECIP_URL,
            "lake_info_portal": TVA_LAKE_INFO_URL,
            "rainfall_page": "https://www.tva.com/environment/lake-levels/rainfall-gauge-data",
            "format": "HTML table (requires parsing); no public JSON/CSV API",
            "notes": (
                "The precip.htm page provides a web query form for gauge "
                "selection by date range and region.  Results are rendered "
                "as HTML tables.  Automated access requires HTML scraping."
            ),
        },
        "update_frequency": "15-minute (tipping-bucket) to hourly summaries",
        "auth_required": False,
        "regions": TVA_GAUGE_REGIONS,
        "parameters": ["precipitation_incremental", "precipitation_accumulated"],
    }


@router.get("/tva/precip")
async def tva_precip_page():
    """Fetch and summarize the TVA precipitation data page.

    Attempts to retrieve the TVA precip web page and returns basic
    availability information.  Full parsing of the interactive query
    form is not implemented — this endpoint confirms reachability.
    """
    cache_key = f"{SOURCE}:tva:precip_check"
    cached = await cache.get(cache_key)
    if cached is not None:
        return cached

    try:
        text = await http_client.fetch_text(
            TVA_PRECIP_URL, source=SOURCE, timeout=15.0,
        )
        available = text is not None and len(text) > 500
        result = {
            "source": "TVA Rain Gauge Network",
            "url": TVA_PRECIP_URL,
            "reachable": available,
            "page_size_bytes": len(text) if text else 0,
            "note": (
                "TVA precipitation data is served through an interactive "
                "web form.  Use the web interface directly for gauge queries."
            ),
            "checked_utc": datetime.now(timezone.utc).isoformat(),
        }
    except Exception as exc:
        logger.warning("TVA precip page check failed: %s", exc)
        result = {
            "source": "TVA Rain Gauge Network",
            "url": TVA_PRECIP_URL,
            "reachable": False,
            "error": str(exc),
            "checked_utc": datetime.now(timezone.utc).isoformat(),
        }

    await cache.set(cache_key, result, ttl=TTL_DATA)
    return result


# ═══════════════════════════════════════════════════════════════════════════
# 2.  CASTNET — Clean Air Status and Trends Network
# ═══════════════════════════════════════════════════════════════════════════

CASTNET_BASE = "https://www.epa.gov/castnet"
CASTNET_DATA_URL = "https://www.epa.gov/castnet/download-data"

# CASTNET sites are categorized by EPA region.  This is a curated subset
# of the ~90 active monitoring locations.
CASTNET_SITES: list[dict[str, Any]] = [
    {"site_id": "ABT147", "name": "Abington", "state": "CT", "lat": 41.84, "lon": -72.01},
    {"site_id": "ACA416", "name": "Acadia NP", "state": "ME", "lat": 44.37, "lon": -68.26},
    {"site_id": "ANA115", "name": "Ann Arbor", "state": "MI", "lat": 42.42, "lon": -83.90},
    {"site_id": "BBE401", "name": "Big Bend NP", "state": "TX", "lat": 29.30, "lon": -103.18},
    {"site_id": "BEL116", "name": "Beltsville", "state": "MD", "lat": 39.03, "lon": -76.82},
    {"site_id": "BWR139", "name": "Blackwater NWR", "state": "MD", "lat": 38.44, "lon": -76.11},
    {"site_id": "CAD150", "name": "Caddo Valley", "state": "AR", "lat": 34.18, "lon": -93.10},
    {"site_id": "CAN407", "name": "Canyonlands NP", "state": "UT", "lat": 38.46, "lon": -109.82},
    {"site_id": "CAT175", "name": "Catoctin Mountain", "state": "MD", "lat": 39.63, "lon": -77.49},
    {"site_id": "CDR119", "name": "Cedar Creek", "state": "MN", "lat": 45.40, "lon": -93.20},
    {"site_id": "CHE185", "name": "Cherokee Nation", "state": "OK", "lat": 36.77, "lon": -94.67},
    {"site_id": "COW137", "name": "Coweeta", "state": "NC", "lat": 35.06, "lon": -83.43},
    {"site_id": "DEN417", "name": "Denali NP", "state": "AK", "lat": 63.72, "lon": -148.97},
    {"site_id": "EVE419", "name": "Everglades NP", "state": "FL", "lat": 25.39, "lon": -80.68},
    {"site_id": "GLA486", "name": "Glacier NP", "state": "MT", "lat": 48.51, "lon": -113.00},
    {"site_id": "GLR468", "name": "Great Lakes Research", "state": "WI", "lat": 45.57, "lon": -88.62},
    {"site_id": "GRS420", "name": "Great Smoky Mountains", "state": "TN", "lat": 35.63, "lon": -83.94},
    {"site_id": "HOW132", "name": "Howland", "state": "ME", "lat": 45.20, "lon": -68.73},
    {"site_id": "JOT403", "name": "Joshua Tree NP", "state": "CA", "lat": 34.07, "lon": -116.39},
    {"site_id": "KEF112", "name": "Kane Experimental Forest", "state": "PA", "lat": 41.60, "lon": -78.77},
    {"site_id": "MCK131", "name": "McKinney", "state": "IN", "lat": 39.23, "lon": -86.45},
    {"site_id": "MEV405", "name": "Mesa Verde NP", "state": "CO", "lat": 37.20, "lon": -108.49},
    {"site_id": "MKG113", "name": "M.K. Goddard", "state": "PA", "lat": 41.43, "lon": -80.14},
    {"site_id": "OLY421", "name": "Olympic NP", "state": "WA", "lat": 48.10, "lon": -123.43},
    {"site_id": "PED108", "name": "Pedernales Falls", "state": "TX", "lat": 30.31, "lon": -98.31},
    {"site_id": "PIN414", "name": "Pinnacles NM", "state": "CA", "lat": 36.49, "lon": -121.16},
    {"site_id": "PNF126", "name": "Penn Nursery", "state": "PA", "lat": 40.72, "lon": -77.95},
    {"site_id": "ROM406", "name": "Rocky Mountain NP", "state": "CO", "lat": 40.28, "lon": -105.55},
    {"site_id": "SEK430", "name": "Sequoia NP", "state": "CA", "lat": 36.49, "lon": -118.83},
    {"site_id": "SHN418", "name": "Shenandoah NP", "state": "VA", "lat": 38.52, "lon": -78.43},
    {"site_id": "SPD111", "name": "Speedwell", "state": "TN", "lat": 36.47, "lon": -83.83},
    {"site_id": "VOY413", "name": "Voyageurs NP", "state": "MN", "lat": 48.41, "lon": -92.83},
    {"site_id": "WSP144", "name": "Washington's Crossing", "state": "NJ", "lat": 40.31, "lon": -74.87},
    {"site_id": "YEL408", "name": "Yellowstone NP", "state": "WY", "lat": 44.56, "lon": -110.40},
    {"site_id": "YOS404", "name": "Yosemite NP", "state": "CA", "lat": 37.71, "lon": -119.71},
]


@router.get("/castnet/info")
async def castnet_info():
    """CASTNET network metadata and data access details.

    Returns the station inventory, monitored parameters, data download
    URLs, and format information for EPA's Clean Air Status and Trends
    Network.
    """
    return {
        "network": "CASTNET — Clean Air Status and Trends Network",
        "operator": "US EPA",
        "description": (
            "~90 rural and remote monitoring sites measuring ambient air "
            "quality (ozone, sulfur, nitrogen compounds) and meteorological "
            "conditions.  Weekly filter-pack chemistry plus continuous "
            "ozone and met at select sites."
        ),
        "total_sites": 90,
        "data_access": {
            "download_page": CASTNET_DATA_URL,
            "main_site": CASTNET_BASE,
            "format": "CSV download via EPA query wizard",
            "datasets": [
                "Weekly dry deposition filter-pack chemistry",
                "Hourly ozone concentrations",
                "Hourly meteorological data (temperature, RH, wind, solar)",
                "Annual and seasonal summaries",
            ],
            "notes": (
                "Data is freely available via CSV download from the EPA "
                "CASTNET data page.  No API key or account required.  "
                "Select site(s), date range, and dataset type on the download form."
            ),
        },
        "update_frequency": "Weekly (filter chemistry); hourly (ozone, met)",
        "auth_required": False,
        "parameters": [
            "ozone_concentration",
            "sulfate", "nitrate", "ammonium", "calcium", "magnesium",
            "sodium", "potassium", "chloride",
            "sulfur_dioxide",
            "air_temperature", "relative_humidity", "wind_speed",
            "wind_direction", "solar_radiation", "precipitation",
        ],
    }


@router.get("/castnet/stations")
async def castnet_stations():
    """CASTNET station list with coordinates.

    Returns a curated inventory of CASTNET monitoring sites including
    site IDs, names, state, and lat/lon coordinates.
    """
    cache_key = f"{SOURCE}:castnet:stations"
    cached = await cache.get(cache_key)
    if cached is not None:
        return cached

    result = {
        "network": "CASTNET",
        "station_count": len(CASTNET_SITES),
        "note": (
            "Curated subset of ~90 active CASTNET sites.  For the complete "
            "inventory, visit https://www.epa.gov/castnet/download-data"
        ),
        "stations": CASTNET_SITES,
        "generated_utc": datetime.now(timezone.utc).isoformat(),
    }
    await cache.set(cache_key, result, ttl=TTL_STATIONS)
    return result


# ═══════════════════════════════════════════════════════════════════════════
# 3.  IMPROVE — Interagency Monitoring of Protected Visual Environments
# ═══════════════════════════════════════════════════════════════════════════

IMPROVE_BASE = "https://vista.cira.colostate.edu/Improve"
IMPROVE_DATA_URL = "https://vista.cira.colostate.edu/Improve/improve-data/"
IMPROVE_FED_QUERY = "https://views.cira.colostate.edu/fed/QueryWizard/Default.aspx"

# Representative IMPROVE sites across the national park system
IMPROVE_SITES: list[dict[str, Any]] = [
    {"site_id": "ACAD1", "name": "Acadia NP", "state": "ME", "lat": 44.377, "lon": -68.261},
    {"site_id": "BIBE1", "name": "Big Bend NP", "state": "TX", "lat": 29.303, "lon": -103.178},
    {"site_id": "BRCA1", "name": "Bryce Canyon NP", "state": "UT", "lat": 37.618, "lon": -112.174},
    {"site_id": "CANY1", "name": "Canyonlands NP", "state": "UT", "lat": 38.459, "lon": -109.821},
    {"site_id": "CRLA1", "name": "Crater Lake NP", "state": "OR", "lat": 42.896, "lon": -122.136},
    {"site_id": "DENA1", "name": "Denali NP", "state": "AK", "lat": 63.723, "lon": -148.968},
    {"site_id": "EVER1", "name": "Everglades NP", "state": "FL", "lat": 25.391, "lon": -80.681},
    {"site_id": "GLAC1", "name": "Glacier NP", "state": "MT", "lat": 48.510, "lon": -113.997},
    {"site_id": "GRBA1", "name": "Great Basin NP", "state": "NV", "lat": 39.005, "lon": -114.216},
    {"site_id": "GRCA2", "name": "Grand Canyon NP", "state": "AZ", "lat": 36.055, "lon": -112.184},
    {"site_id": "GRSA1", "name": "Great Sand Dunes NP", "state": "CO", "lat": 37.723, "lon": -105.519},
    {"site_id": "GRSM1", "name": "Great Smoky Mountains", "state": "TN", "lat": 35.633, "lon": -83.941},
    {"site_id": "GUMO1", "name": "Guadalupe Mountains NP", "state": "TX", "lat": 31.831, "lon": -104.809},
    {"site_id": "HAVO1", "name": "Hawaii Volcanoes NP", "state": "HI", "lat": 19.431, "lon": -155.258},
    {"site_id": "ISLE1", "name": "Isle Royale NP", "state": "MI", "lat": 47.917, "lon": -89.150},
    {"site_id": "JOSH1", "name": "Joshua Tree NP", "state": "CA", "lat": 34.070, "lon": -116.389},
    {"site_id": "LAVO1", "name": "Lassen Volcanic NP", "state": "CA", "lat": 40.540, "lon": -121.577},
    {"site_id": "MACA1", "name": "Mammoth Cave NP", "state": "KY", "lat": 37.132, "lon": -86.148},
    {"site_id": "MEVE1", "name": "Mesa Verde NP", "state": "CO", "lat": 37.198, "lon": -108.491},
    {"site_id": "MORA1", "name": "Mount Rainier NP", "state": "WA", "lat": 46.758, "lon": -122.124},
    {"site_id": "OLYM1", "name": "Olympic NP", "state": "WA", "lat": 48.098, "lon": -123.431},
    {"site_id": "PEFO1", "name": "Petrified Forest NP", "state": "AZ", "lat": 34.821, "lon": -109.892},
    {"site_id": "PINN1", "name": "Pinnacles NP", "state": "CA", "lat": 36.491, "lon": -121.157},
    {"site_id": "REDW1", "name": "Redwood NP", "state": "CA", "lat": 41.561, "lon": -124.084},
    {"site_id": "ROMO1", "name": "Rocky Mountain NP", "state": "CO", "lat": 40.278, "lon": -105.546},
    {"site_id": "SEQU1", "name": "Sequoia NP", "state": "CA", "lat": 36.489, "lon": -118.829},
    {"site_id": "SHEN1", "name": "Shenandoah NP", "state": "VA", "lat": 38.523, "lon": -78.435},
    {"site_id": "VIIS1", "name": "Virgin Islands NP", "state": "VI", "lat": 18.336, "lon": -64.796},
    {"site_id": "YELL2", "name": "Yellowstone NP", "state": "WY", "lat": 44.565, "lon": -110.400},
    {"site_id": "YOSE1", "name": "Yosemite NP", "state": "CA", "lat": 37.713, "lon": -119.706},
    {"site_id": "ZION1", "name": "Zion NP", "state": "UT", "lat": 37.198, "lon": -113.150},
]


@router.get("/improve/info")
async def improve_info():
    """IMPROVE network metadata and data access details.

    Returns station inventory, monitored parameters, and instructions
    for accessing the FED Query Wizard for data download.
    """
    return {
        "network": "IMPROVE — Interagency Monitoring of Protected Visual Environments",
        "operator": "NPS / EPA / USFS / cooperative",
        "description": (
            "~160 monitoring sites (primarily in national parks and wilderness "
            "areas) measuring fine particulate matter composition, aerosol "
            "optical properties, and visibility.  Operated since 1988 to track "
            "progress under the Regional Haze Rule."
        ),
        "total_sites": 160,
        "data_access": {
            "main_site": IMPROVE_BASE,
            "data_page": IMPROVE_DATA_URL,
            "fed_query_wizard": IMPROVE_FED_QUERY,
            "format": "CSV download via FED Query Wizard (web form)",
            "datasets": [
                "Aerosol composition (sulfate, nitrate, OC, EC, soil, sea salt)",
                "Fine mass (PM2.5) and coarse mass (PM10-PM2.5)",
                "Light extinction (reconstructed and measured)",
                "Deciview (visibility index)",
            ],
            "notes": (
                "Data is freely available via the FED Query Wizard at CIRA.  "
                "Select IMPROVE dataset, site(s), date range, and parameters.  "
                "Results download as CSV.  No authentication required."
            ),
        },
        "update_frequency": "Every-third-day sampling; data released quarterly",
        "auth_required": False,
        "parameters": [
            "sulfate_fine", "nitrate_fine", "organic_carbon", "elemental_carbon",
            "fine_soil", "sea_salt", "pm25_mass", "pm10_mass",
            "light_extinction_reconstructed", "light_extinction_measured",
            "deciview", "light_scattering", "light_absorption",
        ],
    }


@router.get("/improve/stations")
async def improve_stations():
    """IMPROVE station list with coordinates.

    Returns a curated inventory of IMPROVE sites at national parks
    and wilderness areas, including site IDs and lat/lon coordinates.
    """
    cache_key = f"{SOURCE}:improve:stations"
    cached = await cache.get(cache_key)
    if cached is not None:
        return cached

    result = {
        "network": "IMPROVE",
        "station_count": len(IMPROVE_SITES),
        "note": (
            "Curated subset of ~160 IMPROVE sites.  For the complete inventory, "
            "visit https://vista.cira.colostate.edu/Improve/improve-data/"
        ),
        "stations": IMPROVE_SITES,
        "generated_utc": datetime.now(timezone.utc).isoformat(),
    }
    await cache.set(cache_key, result, ttl=TTL_STATIONS)
    return result


# ═══════════════════════════════════════════════════════════════════════════
# 4.  NADP NTN — National Atmospheric Deposition Program / National Trends
# ═══════════════════════════════════════════════════════════════════════════

NADP_BASE = "https://nadp.slh.wisc.edu"
NADP_MAPS_DATA = "https://nadp.slh.wisc.edu/maps-data/"
NADP_NTN_DATA = "https://nadp.slh.wisc.edu/networks/national-trends-network/"

# Representative NTN sites across the US
NADP_NTN_SITES: list[dict[str, Any]] = [
    {"site_id": "AL10", "name": "Black Belt Research", "state": "AL", "lat": 32.45, "lon": -87.24},
    {"site_id": "AZ03", "name": "Grand Canyon NP", "state": "AZ", "lat": 36.06, "lon": -112.18},
    {"site_id": "CA45", "name": "Hopland", "state": "CA", "lat": 39.00, "lon": -123.08},
    {"site_id": "CA67", "name": "Joshua Tree NP", "state": "CA", "lat": 34.07, "lon": -116.39},
    {"site_id": "CO19", "name": "Rocky Mountain NP — Beaver Meadows", "state": "CO", "lat": 40.36, "lon": -105.58},
    {"site_id": "CO98", "name": "Rocky Mountain NP — Loch Vale", "state": "CO", "lat": 40.29, "lon": -105.66},
    {"site_id": "FL11", "name": "Everglades NP", "state": "FL", "lat": 25.39, "lon": -80.68},
    {"site_id": "GA41", "name": "Georgia Station", "state": "GA", "lat": 33.18, "lon": -84.41},
    {"site_id": "IL11", "name": "Bondville", "state": "IL", "lat": 40.05, "lon": -88.37},
    {"site_id": "IN34", "name": "Indiana Dunes NL", "state": "IN", "lat": 41.63, "lon": -87.09},
    {"site_id": "KS31", "name": "Konza Prairie", "state": "KS", "lat": 39.10, "lon": -96.61},
    {"site_id": "ME02", "name": "Acadia NP — McFarland Hill", "state": "ME", "lat": 44.38, "lon": -68.26},
    {"site_id": "ME09", "name": "Greenville", "state": "ME", "lat": 45.49, "lon": -69.67},
    {"site_id": "MN16", "name": "Marcell Experimental Forest", "state": "MN", "lat": 47.53, "lon": -93.47},
    {"site_id": "NC25", "name": "Coweeta", "state": "NC", "lat": 35.06, "lon": -83.43},
    {"site_id": "NH02", "name": "Hubbard Brook", "state": "NH", "lat": 43.95, "lon": -71.70},
    {"site_id": "NY20", "name": "Huntington Wildlife Forest", "state": "NY", "lat": 43.97, "lon": -74.22},
    {"site_id": "OH09", "name": "Oxford", "state": "OH", "lat": 39.53, "lon": -84.72},
    {"site_id": "OR10", "name": "H.J. Andrews Experimental Forest", "state": "OR", "lat": 44.21, "lon": -122.26},
    {"site_id": "PA29", "name": "Kane Experimental Forest", "state": "PA", "lat": 41.60, "lon": -78.77},
    {"site_id": "TN11", "name": "Great Smoky Mountains NP — Elkmont", "state": "TN", "lat": 35.66, "lon": -83.59},
    {"site_id": "TX04", "name": "Big Bend NP", "state": "TX", "lat": 29.30, "lon": -103.18},
    {"site_id": "VA13", "name": "Shenandoah NP — Big Meadows", "state": "VA", "lat": 38.52, "lon": -78.44},
    {"site_id": "VT01", "name": "Bennington", "state": "VT", "lat": 42.88, "lon": -73.15},
    {"site_id": "WA14", "name": "Olympic NP — Hoh", "state": "WA", "lat": 47.86, "lon": -123.93},
    {"site_id": "WI37", "name": "Trout Lake", "state": "WI", "lat": 46.05, "lon": -89.65},
    {"site_id": "WV04", "name": "Parsons", "state": "WV", "lat": 39.09, "lon": -79.66},
    {"site_id": "WY06", "name": "Pinedale", "state": "WY", "lat": 42.93, "lon": -109.79},
    {"site_id": "WY08", "name": "Yellowstone NP — Tower Falls", "state": "WY", "lat": 44.92, "lon": -110.42},
]


@router.get("/nadp/info")
async def nadp_info():
    """NADP National Trends Network — metadata and data access.

    Returns station inventory, monitored parameters, and access URLs for
    the national precipitation chemistry (acid rain) monitoring network.
    """
    return {
        "network": "NADP NTN — National Atmospheric Deposition Program / National Trends Network",
        "operator": "NADP Program Office (University of Wisconsin–Madison)",
        "description": (
            "~250 sites collecting weekly precipitation samples for chemical "
            "analysis.  The longest-running precipitation chemistry network "
            "in the US, tracking acid rain trends since 1978."
        ),
        "total_sites": 250,
        "data_access": {
            "main_site": NADP_BASE,
            "maps_and_data": NADP_MAPS_DATA,
            "ntn_network_page": NADP_NTN_DATA,
            "format": "CSV download via NADP Maps & Data interface",
            "datasets": [
                "Weekly precipitation chemistry (pH, conductance, ions)",
                "Annual/seasonal weighted-mean concentrations",
                "Wet deposition totals (kg/ha)",
                "Isopleth maps (concentration and deposition)",
            ],
            "notes": (
                "All data is freely available from the NADP website.  "
                "Use the Maps & Data interface to select sites, date ranges, "
                "and parameters.  CSV and PDF exports available.  No login required."
            ),
        },
        "update_frequency": "Weekly sampling; data released ~3 months after collection",
        "auth_required": False,
        "parameters": [
            "ph", "conductance",
            "sulfate_concentration", "nitrate_concentration",
            "ammonium_concentration", "calcium_concentration",
            "magnesium_concentration", "sodium_concentration",
            "potassium_concentration", "chloride_concentration",
            "sulfate_deposition", "nitrate_deposition",
            "ammonium_deposition", "precipitation_depth",
        ],
    }


@router.get("/nadp/stations")
async def nadp_stations():
    """NADP NTN station list with coordinates.

    Returns a curated inventory of NTN precipitation chemistry sites.
    """
    cache_key = f"{SOURCE}:nadp:stations"
    cached = await cache.get(cache_key)
    if cached is not None:
        return cached

    result = {
        "network": "NADP NTN",
        "station_count": len(NADP_NTN_SITES),
        "note": (
            "Curated subset of ~250 NTN sites.  For the complete inventory, "
            "visit https://nadp.slh.wisc.edu/maps-data/"
        ),
        "stations": NADP_NTN_SITES,
        "generated_utc": datetime.now(timezone.utc).isoformat(),
    }
    await cache.set(cache_key, result, ttl=TTL_STATIONS)
    return result


# ═══════════════════════════════════════════════════════════════════════════
# 5.  USDA UV-B Monitoring and Research Program
# ═══════════════════════════════════════════════════════════════════════════

USDA_UVB_BASE = "https://carhenge.nrel.colostate.edu/UVB/"

# USDA UV-B station network — active agricultural UV monitoring sites
USDA_UVB_SITES: list[dict[str, Any]] = [
    {"site_id": "AL02", "name": "Auburn", "state": "AL", "lat": 32.60, "lon": -85.50},
    {"site_id": "AZ01", "name": "Maricopa", "state": "AZ", "lat": 33.07, "lon": -111.97},
    {"site_id": "CA01", "name": "Davis", "state": "CA", "lat": 38.53, "lon": -121.78},
    {"site_id": "CO01", "name": "Nunn", "state": "CO", "lat": 40.81, "lon": -104.76},
    {"site_id": "FL01", "name": "Homestead", "state": "FL", "lat": 25.51, "lon": -80.50},
    {"site_id": "GA01", "name": "Griffin", "state": "GA", "lat": 33.26, "lon": -84.29},
    {"site_id": "HI01", "name": "Mauna Loa", "state": "HI", "lat": 19.54, "lon": -155.58},
    {"site_id": "IA01", "name": "Ames", "state": "IA", "lat": 42.02, "lon": -93.77},
    {"site_id": "IL02", "name": "Bondville", "state": "IL", "lat": 40.05, "lon": -88.37},
    {"site_id": "IN01", "name": "West Lafayette", "state": "IN", "lat": 40.47, "lon": -86.99},
    {"site_id": "KS01", "name": "Manhattan", "state": "KS", "lat": 39.10, "lon": -96.61},
    {"site_id": "LA01", "name": "Baton Rouge", "state": "LA", "lat": 30.36, "lon": -91.17},
    {"site_id": "MD01", "name": "Beltsville", "state": "MD", "lat": 39.03, "lon": -76.85},
    {"site_id": "MI01", "name": "East Lansing", "state": "MI", "lat": 42.71, "lon": -84.47},
    {"site_id": "MN01", "name": "St. Paul", "state": "MN", "lat": 44.99, "lon": -93.18},
    {"site_id": "MS02", "name": "Stoneville", "state": "MS", "lat": 33.44, "lon": -90.88},
    {"site_id": "MT01", "name": "Bozeman", "state": "MT", "lat": 45.66, "lon": -111.05},
    {"site_id": "NC01", "name": "Raleigh", "state": "NC", "lat": 35.73, "lon": -78.68},
    {"site_id": "NE01", "name": "Mead", "state": "NE", "lat": 41.15, "lon": -96.49},
    {"site_id": "NM01", "name": "Las Cruces", "state": "NM", "lat": 32.62, "lon": -106.74},
    {"site_id": "NY01", "name": "Geneva", "state": "NY", "lat": 42.88, "lon": -77.03},
    {"site_id": "OH01", "name": "Columbus", "state": "OH", "lat": 40.01, "lon": -83.04},
    {"site_id": "OK01", "name": "Goodwell", "state": "OK", "lat": 36.60, "lon": -101.60},
    {"site_id": "OR01", "name": "Corvallis", "state": "OR", "lat": 44.63, "lon": -123.19},
    {"site_id": "PA01", "name": "University Park", "state": "PA", "lat": 40.72, "lon": -77.93},
    {"site_id": "SC01", "name": "Florence", "state": "SC", "lat": 34.24, "lon": -79.81},
    {"site_id": "TX01", "name": "Bushland", "state": "TX", "lat": 35.19, "lon": -102.09},
    {"site_id": "UT01", "name": "Logan", "state": "UT", "lat": 41.67, "lon": -111.89},
    {"site_id": "VT01", "name": "Burlington", "state": "VT", "lat": 44.44, "lon": -73.21},
    {"site_id": "WA01", "name": "Pullman", "state": "WA", "lat": 46.76, "lon": -117.19},
    {"site_id": "WI01", "name": "Madison", "state": "WI", "lat": 43.13, "lon": -89.35},
]


@router.get("/uvb/info")
async def uvb_info():
    """USDA UV-B Monitoring and Research Program — metadata and access.

    Returns station inventory, monitored parameters, and data access
    details for the USDA UV-B agricultural monitoring network.
    """
    return {
        "network": "USDA UV-B Monitoring and Research Program",
        "operator": "USDA / Colorado State University (NREL)",
        "description": (
            "30+ stations across the US measuring UV-B radiation for "
            "agricultural and environmental research.  Instruments include "
            "broadband UV-B radiometers, UV multi-filter rotating shadow-band "
            "radiometers, and PAR sensors.  Operated since 1992."
        ),
        "total_sites": 31,
        "data_access": {
            "main_site": USDA_UVB_BASE,
            "format": "Data accessible via web interface; daily/hourly files",
            "datasets": [
                "Broadband UV-B irradiance (280–320 nm)",
                "Erythemal UV dose rate",
                "UV-A irradiance (320–400 nm)",
                "Photosynthetically Active Radiation (PAR)",
                "Total solar irradiance",
                "Ancillary met (temperature, humidity)",
            ],
            "notes": (
                "Data is publicly available from the USDA UV-B program website "
                "at Colorado State University.  The web interface provides "
                "site selection and date range queries.  No authentication required."
            ),
        },
        "update_frequency": "Hourly (logged every 3 minutes, hourly summaries)",
        "auth_required": False,
        "parameters": [
            "uvb_irradiance", "erythemal_dose_rate", "uva_irradiance",
            "par", "total_solar_irradiance",
            "air_temperature", "relative_humidity",
        ],
    }


@router.get("/uvb/stations")
async def uvb_stations():
    """USDA UV-B station list with coordinates.

    Returns the full inventory of active USDA UV-B monitoring sites.
    """
    cache_key = f"{SOURCE}:uvb:stations"
    cached = await cache.get(cache_key)
    if cached is not None:
        return cached

    result = {
        "network": "USDA UV-B",
        "station_count": len(USDA_UVB_SITES),
        "stations": USDA_UVB_SITES,
        "generated_utc": datetime.now(timezone.utc).isoformat(),
    }
    await cache.set(cache_key, result, ttl=TTL_STATIONS)
    return result


# ═══════════════════════════════════════════════════════════════════════════
# 6.  ClimDB/HydroDB — LTER + USFS Long-Term Research Sites
# ═══════════════════════════════════════════════════════════════════════════

CLIMDB_BASE = "https://climhy.lternet.edu/"

# Long-term ecological and hydrological research sites in the ClimDB/HydroDB network
CLIMDB_SITES: list[dict[str, Any]] = [
    {"site_id": "AND", "name": "H.J. Andrews LTER", "state": "OR", "lat": 44.21, "lon": -122.26, "type": "LTER"},
    {"site_id": "ARC", "name": "Arctic LTER", "state": "AK", "lat": 68.63, "lon": -149.60, "type": "LTER"},
    {"site_id": "BES", "name": "Baltimore Ecosystem Study LTER", "state": "MD", "lat": 39.28, "lon": -76.61, "type": "LTER"},
    {"site_id": "BNZ", "name": "Bonanza Creek LTER", "state": "AK", "lat": 64.70, "lon": -148.32, "type": "LTER"},
    {"site_id": "CDR", "name": "Cedar Creek LTER", "state": "MN", "lat": 45.40, "lon": -93.20, "type": "LTER"},
    {"site_id": "CWT", "name": "Coweeta LTER", "state": "NC", "lat": 35.06, "lon": -83.43, "type": "LTER"},
    {"site_id": "FCE", "name": "Florida Coastal Everglades LTER", "state": "FL", "lat": 25.47, "lon": -80.85, "type": "LTER"},
    {"site_id": "HBR", "name": "Hubbard Brook LTER", "state": "NH", "lat": 43.94, "lon": -71.75, "type": "LTER"},
    {"site_id": "HFR", "name": "Harvard Forest LTER", "state": "MA", "lat": 42.53, "lon": -72.19, "type": "LTER"},
    {"site_id": "JRN", "name": "Jornada Basin LTER", "state": "NM", "lat": 32.62, "lon": -106.74, "type": "LTER"},
    {"site_id": "KBS", "name": "Kellogg Biological Station LTER", "state": "MI", "lat": 42.40, "lon": -85.40, "type": "LTER"},
    {"site_id": "KNZ", "name": "Konza Prairie LTER", "state": "KS", "lat": 39.10, "lon": -96.61, "type": "LTER"},
    {"site_id": "LUQ", "name": "Luquillo LTER", "state": "PR", "lat": 18.32, "lon": -65.82, "type": "LTER"},
    {"site_id": "MCM", "name": "McMurdo Dry Valleys LTER", "state": "AQ", "lat": -77.62, "lon": 163.05, "type": "LTER"},
    {"site_id": "NTL", "name": "North Temperate Lakes LTER", "state": "WI", "lat": 46.01, "lon": -89.67, "type": "LTER"},
    {"site_id": "NWT", "name": "Niwot Ridge LTER", "state": "CO", "lat": 40.05, "lon": -105.59, "type": "LTER"},
    {"site_id": "PAL", "name": "Palmer LTER", "state": "AQ", "lat": -64.77, "lon": -64.05, "type": "LTER"},
    {"site_id": "PIE", "name": "Plum Island Ecosystems LTER", "state": "MA", "lat": 42.76, "lon": -70.89, "type": "LTER"},
    {"site_id": "SEV", "name": "Sevilleta LTER", "state": "NM", "lat": 34.35, "lon": -106.88, "type": "LTER"},
    {"site_id": "SGS", "name": "Shortgrass Steppe LTER", "state": "CO", "lat": 40.82, "lon": -104.77, "type": "LTER"},
    {"site_id": "VCR", "name": "Virginia Coast Reserve LTER", "state": "VA", "lat": 37.28, "lon": -75.91, "type": "LTER"},
    # USFS Experimental Forests
    {"site_id": "CHL", "name": "Coweeta Hydrologic Lab", "state": "NC", "lat": 35.06, "lon": -83.43, "type": "USFS"},
    {"site_id": "FEF", "name": "Fernow Experimental Forest", "state": "WV", "lat": 39.05, "lon": -79.67, "type": "USFS"},
    {"site_id": "FRA", "name": "Fraser Experimental Forest", "state": "CO", "lat": 39.89, "lon": -105.88, "type": "USFS"},
    {"site_id": "HJA", "name": "H.J. Andrews Experimental Forest", "state": "OR", "lat": 44.23, "lon": -122.17, "type": "USFS"},
    {"site_id": "HUB", "name": "Hubbard Brook Experimental Forest", "state": "NH", "lat": 43.94, "lon": -71.73, "type": "USFS"},
    {"site_id": "LEF", "name": "Luquillo Experimental Forest", "state": "PR", "lat": 18.32, "lon": -65.82, "type": "USFS"},
    {"site_id": "MAR", "name": "Marcell Experimental Forest", "state": "MN", "lat": 47.53, "lon": -93.47, "type": "USFS"},
    {"site_id": "PSW", "name": "Pacific Southwest Research Station", "state": "CA", "lat": 37.07, "lon": -119.19, "type": "USFS"},
    {"site_id": "SFK", "name": "San Dimas Experimental Forest", "state": "CA", "lat": 34.20, "lon": -117.77, "type": "USFS"},
    {"site_id": "TDR", "name": "Tenderfoot Creek Experimental Forest", "state": "MT", "lat": 46.92, "lon": -110.87, "type": "USFS"},
]


@router.get("/climdb/info")
async def climdb_info():
    """ClimDB/HydroDB — metadata and data access details.

    Returns site inventory and access information for the LTER and USFS
    long-term research site climate and hydrology database.
    """
    lter_count = sum(1 for s in CLIMDB_SITES if s["type"] == "LTER")
    usfs_count = sum(1 for s in CLIMDB_SITES if s["type"] == "USFS")

    return {
        "network": "ClimDB/HydroDB — Long-Term Ecological & Hydrological Research",
        "operator": "LTER Network / USFS",
        "description": (
            "~45 long-term research sites providing daily climate and hydrology "
            "data from LTER (Long Term Ecological Research) network sites and "
            "USFS experimental forests and ranges.  Data spans decades at most "
            "sites, supporting long-term trend analysis."
        ),
        "total_sites": len(CLIMDB_SITES),
        "lter_sites": lter_count,
        "usfs_sites": usfs_count,
        "data_access": {
            "main_site": CLIMDB_BASE,
            "format": "Web query interface; CSV download",
            "datasets": [
                "Daily air temperature (min, max, mean)",
                "Daily precipitation",
                "Daily streamflow / discharge",
                "Snow depth and SWE (select sites)",
                "Relative humidity, wind, radiation (select sites)",
            ],
            "notes": (
                "ClimDB/HydroDB provides a unified query interface for daily "
                "climate and hydrology data from research sites.  Select site(s), "
                "variable(s), and date range.  Output is CSV.  No login required.  "
                "Note: some sites have data back to the 1950s–1960s."
            ),
        },
        "update_frequency": "Daily (data availability varies by site; some lag months–years)",
        "auth_required": False,
        "parameters": [
            "air_temperature_min", "air_temperature_max", "air_temperature_mean",
            "precipitation", "streamflow", "discharge",
            "snow_depth", "snow_water_equivalent",
        ],
    }


@router.get("/climdb/stations")
async def climdb_stations(
    site_type: Optional[str] = Query(
        None, description="Filter by type: 'LTER' or 'USFS'",
    ),
):
    """ClimDB/HydroDB site list with coordinates.

    Returns the inventory of LTER and USFS research sites.
    Optionally filter by site_type ('LTER' or 'USFS').
    """
    cache_key = f"{SOURCE}:climdb:stations:{site_type or 'all'}"
    cached = await cache.get(cache_key)
    if cached is not None:
        return cached

    sites = CLIMDB_SITES
    if site_type:
        st = site_type.upper()
        if st not in ("LTER", "USFS"):
            raise HTTPException(status_code=400, detail="site_type must be 'LTER' or 'USFS'")
        sites = [s for s in CLIMDB_SITES if s["type"] == st]

    result = {
        "network": "ClimDB/HydroDB",
        "filter": site_type or "all",
        "station_count": len(sites),
        "stations": sites,
        "generated_utc": datetime.now(timezone.utc).isoformat(),
    }
    await cache.set(cache_key, result, ttl=TTL_STATIONS)
    return result


# ═══════════════════════════════════════════════════════════════════════════
# Overview endpoint — all six networks at a glance
# ═══════════════════════════════════════════════════════════════════════════

@router.get("/overview")
async def overview():
    """Specialty Environmental Networks — overview of all six networks.

    Returns a summary of each network including station counts, parameters,
    data access URLs, and update frequencies.
    """
    cache_key = f"{SOURCE}:overview"
    cached = await cache.get(cache_key)
    if cached is not None:
        return cached

    result = {
        "source": SOURCE_NAME,
        "generated_utc": datetime.now(timezone.utc).isoformat(),
        "total_networks": 6,
        "networks": {
            "tva_rain_gauges": {
                "name": "TVA Rain Gauge Network",
                "operator": "Tennessee Valley Authority",
                "site_count": 345,
                "coverage": "Tennessee Valley (TN, AL, KY, NC, VA, GA, MS)",
                "parameters": ["precipitation"],
                "data_url": TVA_PRECIP_URL,
                "update_frequency": "15 min – hourly",
                "access_method": "Web query (HTML parsing required)",
            },
            "castnet": {
                "name": "CASTNET — Clean Air Status and Trends Network",
                "operator": "US EPA",
                "site_count": 90,
                "coverage": "Continental US + Alaska",
                "parameters": ["ozone", "sulfate", "nitrate", "ammonium", "meteorology"],
                "data_url": CASTNET_DATA_URL,
                "update_frequency": "Weekly (chemistry), hourly (ozone/met)",
                "access_method": "CSV download via EPA query wizard",
            },
            "improve": {
                "name": "IMPROVE — Aerosol and Visibility Monitoring",
                "operator": "NPS / EPA / USFS",
                "site_count": 160,
                "coverage": "National parks and wilderness areas, US-wide",
                "parameters": ["pm25", "aerosol_composition", "visibility", "light_extinction"],
                "data_url": IMPROVE_DATA_URL,
                "update_frequency": "Every-third-day sampling; quarterly release",
                "access_method": "CSV download via FED Query Wizard",
            },
            "nadp_ntn": {
                "name": "NADP NTN — Precipitation Chemistry",
                "operator": "NADP / University of Wisconsin",
                "site_count": 250,
                "coverage": "Continental US + Alaska, Hawaii, Puerto Rico, Virgin Islands",
                "parameters": ["ph", "sulfate", "nitrate", "ammonium", "wet_deposition"],
                "data_url": NADP_MAPS_DATA,
                "update_frequency": "Weekly sampling; ~3-month data lag",
                "access_method": "CSV download via NADP Maps & Data",
            },
            "usda_uvb": {
                "name": "USDA UV-B Monitoring Program",
                "operator": "USDA / Colorado State University",
                "site_count": 31,
                "coverage": "Continental US + Hawaii",
                "parameters": ["uvb_irradiance", "erythemal_dose", "par", "solar_radiation"],
                "data_url": USDA_UVB_BASE,
                "update_frequency": "Hourly (3-min logging)",
                "access_method": "Web interface data query",
            },
            "climdb_hydrodb": {
                "name": "ClimDB/HydroDB — LTER + USFS Research Sites",
                "operator": "LTER Network / USFS",
                "site_count": len(CLIMDB_SITES),
                "coverage": "US-wide + Antarctica + Puerto Rico (research sites)",
                "parameters": ["temperature", "precipitation", "streamflow"],
                "data_url": CLIMDB_BASE,
                "update_frequency": "Daily (availability lags vary)",
                "access_method": "Web query interface, CSV download",
            },
        },
    }
    await cache.set(cache_key, result, ttl=TTL_OVERVIEW)
    return result
