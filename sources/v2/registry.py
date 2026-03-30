"""v2 API category registry — maps every source module to a semantic category.

The CATEGORY_REGISTRY is the single source of truth for v2 URL routing.
infer_v2_category() resolves a source name (and optional query params)
to a category string used in /api/v2/{category}/... URLs.
"""

from __future__ import annotations

from typing import Optional


# ── Category definitions ────────────────────────────────────────────────────

CATEGORIES: dict[str, dict] = {
    "weather": {
        "description": (
            "Surface weather observations from airports, mesonets, fire weather, "
            "road weather, and international met services"
        ),
        "sources": [
            "noaa_weather", "faa_weather", "mesonets", "iem_dcp", "regional",
            "coagmet", "montana_mesonet_api", "infrastructure", "dot_rwis_arcgis",
            "fawn", "nc_econet", "southeast_wx", "ag_networks",
            "wsu_agweathernet", "sdge_weather", "madis_public",
            "hong_kong", "austria", "poland", "iceland", "baltic", "argentina",
            "australia", "chile", "colombia", "mexico", "asia_east",
            "europe_nordic", "europe_west", "international", "waggle_sage",
            "citizen", "arcgis_realtime", "noaa_arcgis", "calfire_raws",
            "space_weather", "africa", "federal_facilities", "specialty",
        ],
    },
    "water": {
        "description": "Streamflow, water levels, tides, flood monitoring, and reservoir data",
        "sources": [
            "usgs", "noaa_water", "federal_water", "usace_cda", "california",
            "california_extended", "eastcoast", "florida_water", "texas_water",
            "pnw_water", "chesapeake", "harris_county", "king_county",
            "hudson_river", "columbia_river", "iowa_flood", "iflows",
            "southwest_flood", "maricopa_flood", "midwest_stormwater",
            "nm_water", "brazil_hydro", "aquarius", "misc_monitoring",
            "water_quality",
        ],
    },
    "air_quality": {
        "description": (
            "Air quality monitoring from regulatory networks, citizen sensors, "
            "and satellite"
        ),
        "sources": [
            "openaq", "airnow", "epa_arcgis", "sensor_community", "state_aq",
            "citizen_extended",
        ],
    },
    "fire": {
        "description": "Fire weather stations, satellite hotspots, wildfire incidents and perimeters",
        "sources": [
            "calfire_raws", "esri_living_atlas",
        ],
    },
    "seismic": {
        "description": "Earthquake monitoring, volcanic activity, and geophysical hazards",
        "sources": [
            "earthquakes",
        ],
    },
    "ocean": {
        "description": "Ocean buoys, coastal monitoring, marine weather, and sea level",
        "sources": [
            "chesapeake_buoys", "ocean_extended", "erddap",
        ],
    },
    "climate": {
        "description": "Climate observations, snowpack, solar radiation, and gridded products",
        "sources": [
            "climate", "rcc_acis", "nasa_power", "agriculture_ext",
            "satellite_products", "nrel_midc", "radiation",
            "specialty_networks",
        ],
    },
    "infrastructure": {
        "description": "Dams, groundwater wells, water rights, and water quality archives",
        "sources": [
            "dams_wells", "state_wells", "state_water_quality", "nyc_data",
        ],
    },
}


# ── Derived lookup tables ───────────────────────────────────────────────────

# source_name → category (first category wins for non-ambiguous)
_SOURCE_TO_CATEGORY: dict[str, str] = {}
# source_name → list of categories (for multi-category sources)
_SOURCE_TO_CATEGORIES: dict[str, list[str]] = {}

for _cat, _meta in CATEGORIES.items():
    for _src in _meta["sources"]:
        _SOURCE_TO_CATEGORIES.setdefault(_src, []).append(_cat)
        if _src not in _SOURCE_TO_CATEGORY:
            _SOURCE_TO_CATEGORY[_src] = _cat


def infer_v2_category(
    source_name: str,
    params: Optional[dict] = None,
) -> str:
    """Resolve a source module name to its v2 category.

    1. Check the explicit registry first.
    2. If a source appears in multiple categories, use *params* to
       disambiguate (e.g. calfire_raws → "fire" when param
       ``category=fire`` is present).
    3. Default to ``"weather"`` for unknown sources.
    """
    cats = _SOURCE_TO_CATEGORIES.get(source_name)
    if cats is None:
        return "weather"

    if len(cats) == 1:
        return cats[0]

    # Multi-category source — try to disambiguate via params
    if params:
        requested = params.get("category") or params.get("cat")
        if requested and requested in cats:
            return requested

    # Fallback: first registered category
    return cats[0]
