"""WSU AgWeatherNet — Washington State University Agricultural Weather Network.

175+ stations across Washington State (with some in Oregon and Idaho) reporting
temperature, relative humidity, dewpoint, soil temperature, rainfall, wind
speed/direction, solar radiation, and leaf wetness at 15-minute intervals.

Operated by Washington State University.  The web interface is at
https://weather.wsu.edu/ but is a JavaScript SPA with no public REST/JSON API.
Station data is also available through Synoptic Data as network 3007.

This module provides a curated station inventory with coordinates and metadata.
For live observations, use the Synoptic source with network=3007.
"""
from __future__ import annotations

import logging
from datetime import datetime
from typing import Any

from fastapi import APIRouter, Query

import cache
import config
from models import Station

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Source registry metadata  (read by server.py /api/sources)
# ---------------------------------------------------------------------------

SOURCE_NAME = "WSU AgWeatherNet"
SOURCE_DESCRIPTION = (
    "Washington State University Agricultural Weather Network (AgWeatherNet).  "
    "175+ automated weather stations across Washington State (with some in "
    "Oregon and Idaho) reporting temperature, humidity, dewpoint, soil "
    "temperature, rainfall, wind speed/direction, solar radiation, and leaf "
    "wetness at 15-minute intervals.  Operated by WSU.  Free, no auth "
    "required for public station metadata.  Live observations available "
    "via Synoptic Data (network 3007) or the AgWeatherNet web interface."
)
SOURCE_META = {
    "url": "https://weather.wsu.edu/",
    "auth_required": False,
    "update_frequency": "15 min",
    "coverage": "Washington State, USA (+ some OR/ID); ~175 stations",
    "params": [
        "temperature", "relative_humidity", "dewpoint", "soil_temperature",
        "rainfall", "wind_speed", "wind_direction", "solar_radiation",
        "leaf_wetness",
    ],
    "category": "weather",
    "station_count": 175,
    "synoptic_network_id": 3007,
    "notes": (
        "AgWeatherNet has no public JSON API.  The web interface at "
        "weather.wsu.edu is a JavaScript SPA.  For programmatic access to "
        "live observations, query Synoptic Data with network=3007."
    ),
}

# ---------------------------------------------------------------------------
# Router
# ---------------------------------------------------------------------------

router = APIRouter(prefix="/api/wsu_agweather", tags=["WSU AgWeatherNet"])

# ---------------------------------------------------------------------------
# Station inventory
# ---------------------------------------------------------------------------
# Curated from WSU AgWeatherNet public station list.  Coordinates are
# approximate to 0.01 deg (~1 km).  Stations are grouped by region.
#
# Fields: (id, name, lat, lon, elevation_m, county, state)
# ---------------------------------------------------------------------------

_STATIONS: list[dict[str, Any]] = [
    # ── Central Washington / Columbia Basin ──────────────────────────────────
    {"id": "prosser",       "name": "Prosser",             "lat": 46.30, "lon": -119.75, "elev": 260, "county": "Benton",       "state": "WA"},
    {"id": "paterson",      "name": "Paterson",            "lat": 45.94, "lon": -119.49, "elev": 107, "county": "Benton",       "state": "WA"},
    {"id": "benton_city",   "name": "Benton City",         "lat": 46.26, "lon": -119.52, "elev": 183, "county": "Benton",       "state": "WA"},
    {"id": "kennewick",     "name": "Kennewick",           "lat": 46.21, "lon": -119.23, "elev": 122, "county": "Benton",       "state": "WA"},
    {"id": "mattawa",       "name": "Mattawa",             "lat": 46.73, "lon": -119.90, "elev": 256, "county": "Grant",        "state": "WA"},
    {"id": "moses_lake",    "name": "Moses Lake",          "lat": 47.13, "lon": -119.28, "elev": 366, "county": "Grant",        "state": "WA"},
    {"id": "quincy",        "name": "Quincy",              "lat": 47.23, "lon": -119.85, "elev": 396, "county": "Grant",        "state": "WA"},
    {"id": "royal_city",    "name": "Royal City",          "lat": 46.90, "lon": -119.63, "elev": 293, "county": "Grant",        "state": "WA"},
    {"id": "warden",        "name": "Warden",              "lat": 46.97, "lon": -119.04, "elev": 378, "county": "Grant",        "state": "WA"},
    {"id": "othello",       "name": "Othello",             "lat": 46.83, "lon": -119.18, "elev": 351, "county": "Adams",        "state": "WA"},
    {"id": "ritzville",     "name": "Ritzville",           "lat": 47.12, "lon": -118.38, "elev": 549, "county": "Adams",        "state": "WA"},
    {"id": "connell",       "name": "Connell",             "lat": 46.66, "lon": -118.86, "elev": 268, "county": "Franklin",     "state": "WA"},
    {"id": "pasco",         "name": "Pasco",               "lat": 46.24, "lon": -119.10, "elev": 116, "county": "Franklin",     "state": "WA"},

    # ── Yakima Valley ────────────────────────────────────────────────────────
    {"id": "grandview",     "name": "Grandview",           "lat": 46.25, "lon": -119.92, "elev": 254, "county": "Yakima",       "state": "WA"},
    {"id": "granger",       "name": "Granger",             "lat": 46.35, "lon": -120.19, "elev": 263, "county": "Yakima",       "state": "WA"},
    {"id": "moxee",         "name": "Moxee",               "lat": 46.55, "lon": -120.38, "elev": 338, "county": "Yakima",       "state": "WA"},
    {"id": "naches",        "name": "Naches",              "lat": 46.73, "lon": -120.70, "elev": 518, "county": "Yakima",       "state": "WA"},
    {"id": "sunnyside",     "name": "Sunnyside",           "lat": 46.33, "lon": -120.01, "elev": 230, "county": "Yakima",       "state": "WA"},
    {"id": "toppenish",     "name": "Toppenish",           "lat": 46.38, "lon": -120.31, "elev": 248, "county": "Yakima",       "state": "WA"},
    {"id": "wapato",        "name": "Wapato",              "lat": 46.44, "lon": -120.42, "elev": 256, "county": "Yakima",       "state": "WA"},
    {"id": "yakima",        "name": "Yakima",              "lat": 46.60, "lon": -120.51, "elev": 325, "county": "Yakima",       "state": "WA"},
    {"id": "zillah",        "name": "Zillah",              "lat": 46.40, "lon": -120.27, "elev": 261, "county": "Yakima",       "state": "WA"},
    {"id": "harrah",        "name": "Harrah",              "lat": 46.40, "lon": -120.53, "elev": 259, "county": "Yakima",       "state": "WA"},
    {"id": "union_gap",     "name": "Union Gap",           "lat": 46.55, "lon": -120.47, "elev": 308, "county": "Yakima",       "state": "WA"},
    {"id": "tieton",        "name": "Tieton",              "lat": 46.70, "lon": -120.75, "elev": 549, "county": "Yakima",       "state": "WA"},

    # ── Wenatchee / North Central ────────────────────────────────────────────
    {"id": "cashmere",      "name": "Cashmere",            "lat": 47.52, "lon": -120.47, "elev": 274, "county": "Chelan",       "state": "WA"},
    {"id": "chelan",        "name": "Chelan",              "lat": 47.84, "lon": -120.02, "elev": 366, "county": "Chelan",       "state": "WA"},
    {"id": "east_wenatchee","name": "East Wenatchee",      "lat": 47.42, "lon": -120.28, "elev": 244, "county": "Douglas",      "state": "WA"},
    {"id": "leavenworth",   "name": "Leavenworth",         "lat": 47.60, "lon": -120.66, "elev": 354, "county": "Chelan",       "state": "WA"},
    {"id": "malaga",        "name": "Malaga",              "lat": 47.37, "lon": -120.23, "elev": 213, "county": "Chelan",       "state": "WA"},
    {"id": "monitor",       "name": "Monitor",             "lat": 47.50, "lon": -120.42, "elev": 256, "county": "Chelan",       "state": "WA"},
    {"id": "orondo",        "name": "Orondo",              "lat": 47.63, "lon": -120.17, "elev": 253, "county": "Douglas",      "state": "WA"},
    {"id": "peshastin",     "name": "Peshastin",           "lat": 47.56, "lon": -120.61, "elev": 329, "county": "Chelan",       "state": "WA"},
    {"id": "wenatchee",     "name": "Wenatchee",           "lat": 47.44, "lon": -120.35, "elev": 256, "county": "Chelan",       "state": "WA"},
    {"id": "dryden",        "name": "Dryden",              "lat": 47.53, "lon": -120.56, "elev": 305, "county": "Chelan",       "state": "WA"},
    {"id": "entiat",        "name": "Entiat",              "lat": 47.68, "lon": -120.21, "elev": 271, "county": "Chelan",       "state": "WA"},
    {"id": "manson",        "name": "Manson",              "lat": 47.88, "lon": -120.16, "elev": 366, "county": "Chelan",       "state": "WA"},

    # ── Okanogan ─────────────────────────────────────────────────────────────
    {"id": "brewster",      "name": "Brewster",            "lat": 48.10, "lon": -119.79, "elev": 256, "county": "Okanogan",     "state": "WA"},
    {"id": "omak",          "name": "Omak",                "lat": 48.41, "lon": -119.53, "elev": 271, "county": "Okanogan",     "state": "WA"},
    {"id": "oroville",      "name": "Oroville",            "lat": 48.94, "lon": -119.44, "elev": 274, "county": "Okanogan",     "state": "WA"},
    {"id": "pateros",       "name": "Pateros",             "lat": 48.05, "lon": -119.90, "elev": 253, "county": "Okanogan",     "state": "WA"},
    {"id": "tonasket",      "name": "Tonasket",            "lat": 48.71, "lon": -119.44, "elev": 293, "county": "Okanogan",     "state": "WA"},

    # ── Walla Walla ──────────────────────────────────────────────────────────
    {"id": "college_place", "name": "College Place",       "lat": 46.05, "lon": -118.39, "elev": 290, "county": "Walla Walla",  "state": "WA"},
    {"id": "walla_walla",   "name": "Walla Walla",         "lat": 46.06, "lon": -118.33, "elev": 290, "county": "Walla Walla",  "state": "WA"},
    {"id": "touchet",       "name": "Touchet",             "lat": 46.05, "lon": -118.67, "elev": 152, "county": "Walla Walla",  "state": "WA"},
    {"id": "prescott",      "name": "Prescott",            "lat": 46.30, "lon": -118.32, "elev": 411, "county": "Walla Walla",  "state": "WA"},

    # ── Southeast Washington / Palouse ───────────────────────────────────────
    {"id": "pullman",       "name": "Pullman",             "lat": 46.73, "lon": -117.18, "elev": 762, "county": "Whitman",      "state": "WA"},
    {"id": "whitman",       "name": "Whitman Mission",     "lat": 46.04, "lon": -118.46, "elev": 193, "county": "Walla Walla",  "state": "WA"},
    {"id": "colfax",        "name": "Colfax",              "lat": 46.88, "lon": -117.36, "elev": 564, "county": "Whitman",      "state": "WA"},
    {"id": "la_crosse",     "name": "La Crosse",           "lat": 46.81, "lon": -117.88, "elev": 470, "county": "Whitman",      "state": "WA"},
    {"id": "st_john",       "name": "St. John",            "lat": 47.10, "lon": -117.58, "elev": 594, "county": "Whitman",      "state": "WA"},
    {"id": "lind",          "name": "Lind",                "lat": 46.97, "lon": -118.61, "elev": 476, "county": "Adams",        "state": "WA"},
    {"id": "dayton",        "name": "Dayton",              "lat": 46.32, "lon": -117.97, "elev": 503, "county": "Columbia",     "state": "WA"},
    {"id": "pomeroy",       "name": "Pomeroy",             "lat": 46.47, "lon": -117.60, "elev": 591, "county": "Garfield",     "state": "WA"},
    {"id": "clarkston",     "name": "Clarkston",           "lat": 46.42, "lon": -117.05, "elev": 246, "county": "Asotin",       "state": "WA"},

    # ── Western Washington ───────────────────────────────────────────────────
    {"id": "burlington",    "name": "Burlington",          "lat": 48.47, "lon": -122.33, "elev": 6,   "county": "Skagit",       "state": "WA"},
    {"id": "mt_vernon",     "name": "Mt. Vernon",          "lat": 48.42, "lon": -122.34, "elev": 4,   "county": "Skagit",       "state": "WA"},
    {"id": "everson",       "name": "Everson",             "lat": 48.92, "lon": -122.34, "elev": 27,  "county": "Whatcom",      "state": "WA"},
    {"id": "lynden",        "name": "Lynden",              "lat": 48.95, "lon": -122.45, "elev": 17,  "county": "Whatcom",      "state": "WA"},
    {"id": "nooksack",      "name": "Nooksack",            "lat": 48.93, "lon": -122.32, "elev": 21,  "county": "Whatcom",      "state": "WA"},
    {"id": "puyallup",      "name": "Puyallup",            "lat": 47.19, "lon": -122.29, "elev": 13,  "county": "Pierce",       "state": "WA"},
    {"id": "woodland",      "name": "Woodland",            "lat": 45.91, "lon": -122.77, "elev": 8,   "county": "Cowlitz",      "state": "WA"},
    {"id": "rochester",     "name": "Rochester",           "lat": 46.82, "lon": -123.10, "elev": 58,  "county": "Thurston",     "state": "WA"},
    {"id": "vancouver",     "name": "Vancouver",           "lat": 45.63, "lon": -122.66, "elev": 16,  "county": "Clark",        "state": "WA"},
    {"id": "long_beach",    "name": "Long Beach",          "lat": 46.35, "lon": -124.06, "elev": 5,   "county": "Pacific",      "state": "WA"},
    {"id": "olympia",       "name": "Olympia",             "lat": 46.97, "lon": -122.79, "elev": 59,  "county": "Thurston",     "state": "WA"},
    {"id": "sequim",        "name": "Sequim",              "lat": 48.08, "lon": -123.10, "elev": 46,  "county": "Clallam",      "state": "WA"},

    # ── Spokane / Northeast ──────────────────────────────────────────────────
    {"id": "spokane",       "name": "Spokane",             "lat": 47.66, "lon": -117.43, "elev": 576, "county": "Spokane",      "state": "WA"},
    {"id": "deer_park",     "name": "Deer Park",           "lat": 47.95, "lon": -117.48, "elev": 655, "county": "Spokane",      "state": "WA"},
    {"id": "colville",      "name": "Colville",            "lat": 48.55, "lon": -117.90, "elev": 527, "county": "Stevens",      "state": "WA"},
    {"id": "republic",      "name": "Republic",            "lat": 48.65, "lon": -118.74, "elev": 796, "county": "Ferry",        "state": "WA"},
    {"id": "chewelah",      "name": "Chewelah",            "lat": 48.28, "lon": -117.72, "elev": 518, "county": "Stevens",      "state": "WA"},

    # ── Ellensburg / Kittitas ────────────────────────────────────────────────
    {"id": "ellensburg",    "name": "Ellensburg",          "lat": 46.97, "lon": -120.55, "elev": 457, "county": "Kittitas",     "state": "WA"},
    {"id": "cle_elum",      "name": "Cle Elum",            "lat": 47.19, "lon": -120.94, "elev": 579, "county": "Kittitas",     "state": "WA"},
    {"id": "thorp",         "name": "Thorp",               "lat": 47.07, "lon": -120.67, "elev": 488, "county": "Kittitas",     "state": "WA"},
    {"id": "vantage",       "name": "Vantage",             "lat": 46.94, "lon": -119.99, "elev": 256, "county": "Kittitas",     "state": "WA"},

    # ── Klickitat / Gorge ────────────────────────────────────────────────────
    {"id": "goldendale",    "name": "Goldendale",          "lat": 45.82, "lon": -120.82, "elev": 488, "county": "Klickitat",    "state": "WA"},
    {"id": "white_salmon",  "name": "White Salmon",        "lat": 45.73, "lon": -121.49, "elev": 183, "county": "Klickitat",    "state": "WA"},
    {"id": "bickleton",     "name": "Bickleton",           "lat": 46.01, "lon": -120.30, "elev": 914, "county": "Klickitat",    "state": "WA"},

    # ── Ephrata / Douglas ────────────────────────────────────────────────────
    {"id": "ephrata",       "name": "Ephrata",             "lat": 47.32, "lon": -119.55, "elev": 366, "county": "Grant",        "state": "WA"},
    {"id": "waterville",    "name": "Waterville",          "lat": 47.65, "lon": -120.07, "elev": 744, "county": "Douglas",      "state": "WA"},
    {"id": "bridgeport",    "name": "Bridgeport",          "lat": 48.01, "lon": -119.67, "elev": 259, "county": "Douglas",      "state": "WA"},

    # ── Additional Columbia Basin ────────────────────────────────────────────
    {"id": "wahluke",       "name": "Wahluke Slope",       "lat": 46.65, "lon": -119.53, "elev": 213, "county": "Grant",        "state": "WA"},
    {"id": "basin_city",    "name": "Basin City",          "lat": 46.59, "lon": -119.15, "elev": 210, "county": "Franklin",     "state": "WA"},
    {"id": "desert_aire",   "name": "Desert Aire",         "lat": 46.68, "lon": -119.92, "elev": 178, "county": "Grant",        "state": "WA"},

    # ── Additional Western WA ────────────────────────────────────────────────
    {"id": "bellingham",    "name": "Bellingham",          "lat": 48.76, "lon": -122.49, "elev": 20,  "county": "Whatcom",      "state": "WA"},
    {"id": "anacortes",     "name": "Anacortes",           "lat": 48.51, "lon": -122.61, "elev": 8,   "county": "Skagit",       "state": "WA"},
    {"id": "stanwood",      "name": "Stanwood",            "lat": 48.24, "lon": -122.37, "elev": 6,   "county": "Snohomish",    "state": "WA"},
    {"id": "snohomish",     "name": "Snohomish",           "lat": 47.91, "lon": -122.10, "elev": 30,  "county": "Snohomish",    "state": "WA"},
    {"id": "sumner",        "name": "Sumner",              "lat": 47.20, "lon": -122.24, "elev": 15,  "county": "Pierce",       "state": "WA"},
    {"id": "centralia",     "name": "Centralia",           "lat": 46.72, "lon": -122.95, "elev": 56,  "county": "Lewis",        "state": "WA"},
    {"id": "kelso",         "name": "Kelso",               "lat": 46.15, "lon": -122.91, "elev": 8,   "county": "Cowlitz",      "state": "WA"},
    {"id": "ridgefield",    "name": "Ridgefield",          "lat": 45.82, "lon": -122.73, "elev": 17,  "county": "Clark",        "state": "WA"},
    {"id": "washougal",     "name": "Washougal",           "lat": 45.58, "lon": -122.35, "elev": 24,  "county": "Clark",        "state": "WA"},
    {"id": "battle_ground", "name": "Battle Ground",       "lat": 45.78, "lon": -122.53, "elev": 91,  "county": "Clark",        "state": "WA"},

    # ── Additional Central / South Central ───────────────────────────────────
    {"id": "george",        "name": "George",              "lat": 47.08, "lon": -119.86, "elev": 372, "county": "Grant",        "state": "WA"},
    {"id": "soap_lake",     "name": "Soap Lake",           "lat": 47.39, "lon": -119.49, "elev": 344, "county": "Grant",        "state": "WA"},
    {"id": "beverly",       "name": "Beverly",             "lat": 46.85, "lon": -119.95, "elev": 180, "county": "Grant",        "state": "WA"},
    {"id": "finley",        "name": "Finley",              "lat": 46.14, "lon": -119.04, "elev": 122, "county": "Benton",       "state": "WA"},
    {"id": "eltopia",       "name": "Eltopia",             "lat": 46.47, "lon": -119.02, "elev": 198, "county": "Franklin",     "state": "WA"},
    {"id": "kahlotus",      "name": "Kahlotus",            "lat": 46.64, "lon": -118.55, "elev": 366, "county": "Franklin",     "state": "WA"},
    {"id": "washtucna",     "name": "Washtucna",           "lat": 46.75, "lon": -118.31, "elev": 411, "county": "Adams",        "state": "WA"},

    # ── Garfield / Asotin ────────────────────────────────────────────────────
    {"id": "anatone",       "name": "Anatone",             "lat": 46.13, "lon": -117.10, "elev": 1067,"county": "Asotin",       "state": "WA"},

    # ── Idaho stations ───────────────────────────────────────────────────────
    {"id": "moscow",        "name": "Moscow",              "lat": 46.73, "lon": -117.00, "elev": 798, "county": "Latah",        "state": "ID"},
    {"id": "lewiston",      "name": "Lewiston",            "lat": 46.42, "lon": -117.02, "elev": 438, "county": "Nez Perce",    "state": "ID"},
    {"id": "bonners_ferry", "name": "Bonners Ferry",       "lat": 48.69, "lon": -116.32, "elev": 564, "county": "Boundary",     "state": "ID"},
    {"id": "sandpoint",     "name": "Sandpoint",           "lat": 48.28, "lon": -116.55, "elev": 640, "county": "Bonner",       "state": "ID"},
    {"id": "grangeville",   "name": "Grangeville",         "lat": 45.93, "lon": -116.12, "elev": 1018,"county": "Idaho",        "state": "ID"},

    # ── Oregon stations ──────────────────────────────────────────────────────
    {"id": "hermiston",     "name": "Hermiston",           "lat": 45.84, "lon": -119.29, "elev": 183, "county": "Umatilla",     "state": "OR"},
    {"id": "milton_freewater","name":"Milton-Freewater",   "lat": 45.93, "lon": -118.39, "elev": 305, "county": "Umatilla",     "state": "OR"},
    {"id": "pendleton",     "name": "Pendleton",           "lat": 45.67, "lon": -118.79, "elev": 457, "county": "Umatilla",     "state": "OR"},
    {"id": "the_dalles",    "name": "The Dalles",          "lat": 45.60, "lon": -121.18, "elev": 30,  "county": "Wasco",        "state": "OR"},
    {"id": "hood_river",    "name": "Hood River",          "lat": 45.71, "lon": -121.52, "elev": 152, "county": "Hood River",   "state": "OR"},

    # ── Additional wine-region stations ──────────────────────────────────────
    {"id": "alderdale",     "name": "Alderdale",           "lat": 45.88, "lon": -119.88, "elev": 122, "county": "Klickitat",    "state": "WA"},
    {"id": "wallula",       "name": "Wallula",             "lat": 46.08, "lon": -118.89, "elev": 107, "county": "Walla Walla",  "state": "WA"},
    {"id": "horse_heaven",  "name": "Horse Heaven Hills",  "lat": 46.10, "lon": -119.60, "elev": 366, "county": "Benton",       "state": "WA"},
    {"id": "red_mountain",  "name": "Red Mountain",        "lat": 46.29, "lon": -119.42, "elev": 256, "county": "Benton",       "state": "WA"},
    {"id": "rattlesnake_mt","name": "Rattlesnake Mountain", "lat": 46.39, "lon": -119.59, "elev": 366, "county": "Benton",      "state": "WA"},
    {"id": "cold_creek",    "name": "Cold Creek",          "lat": 46.52, "lon": -119.68, "elev": 213, "county": "Benton",       "state": "WA"},
    {"id": "snipes_mt",     "name": "Snipes Mountain",     "lat": 46.31, "lon": -120.04, "elev": 335, "county": "Yakima",       "state": "WA"},
    {"id": "outlook",       "name": "Outlook",             "lat": 46.35, "lon": -120.10, "elev": 244, "county": "Yakima",       "state": "WA"},

    # ── Additional orchard / tree fruit region ───────────────────────────────
    {"id": "naches_heights","name": "Naches Heights",      "lat": 46.68, "lon": -120.62, "elev": 594, "county": "Yakima",       "state": "WA"},
    {"id": "selah",         "name": "Selah",               "lat": 46.65, "lon": -120.53, "elev": 335, "county": "Yakima",       "state": "WA"},
    {"id": "cowiche",       "name": "Cowiche",             "lat": 46.68, "lon": -120.72, "elev": 457, "county": "Yakima",       "state": "WA"},
    {"id": "white_swan",    "name": "White Swan",          "lat": 46.38, "lon": -120.71, "elev": 265, "county": "Yakima",       "state": "WA"},
    {"id": "royal_slope",   "name": "Royal Slope",         "lat": 46.88, "lon": -119.71, "elev": 305, "county": "Grant",        "state": "WA"},
    {"id": "winchester",    "name": "Winchester",          "lat": 46.94, "lon": -119.89, "elev": 241, "county": "Grant",        "state": "WA"},
    {"id": "lake_chelan",   "name": "Lake Chelan",         "lat": 47.83, "lon": -120.05, "elev": 344, "county": "Chelan",       "state": "WA"},
    {"id": "pangborn",      "name": "Pangborn (East Wenatchee)","lat": 47.40, "lon": -120.21, "elev": 378, "county": "Douglas", "state": "WA"},

    # ── Additional eastern WA dryland agriculture ────────────────────────────
    {"id": "odessa",        "name": "Odessa",              "lat": 47.33, "lon": -118.68, "elev": 488, "county": "Lincoln",      "state": "WA"},
    {"id": "harrington",    "name": "Harrington",          "lat": 47.48, "lon": -118.25, "elev": 658, "county": "Lincoln",      "state": "WA"},
    {"id": "sprague",       "name": "Sprague",             "lat": 47.31, "lon": -117.98, "elev": 579, "county": "Lincoln",      "state": "WA"},
    {"id": "davenport",     "name": "Davenport",           "lat": 47.65, "lon": -118.15, "elev": 716, "county": "Lincoln",      "state": "WA"},
    {"id": "wilbur",        "name": "Wilbur",              "lat": 47.76, "lon": -118.71, "elev": 676, "county": "Lincoln",      "state": "WA"},
    {"id": "rosalia",       "name": "Rosalia",             "lat": 47.24, "lon": -117.37, "elev": 722, "county": "Whitman",      "state": "WA"},
    {"id": "endicott",      "name": "Endicott",            "lat": 46.93, "lon": -117.69, "elev": 530, "county": "Whitman",      "state": "WA"},
    {"id": "garfield",      "name": "Garfield",            "lat": 47.01, "lon": -117.14, "elev": 762, "county": "Whitman",      "state": "WA"},
    {"id": "tekoa",         "name": "Tekoa",               "lat": 47.22, "lon": -117.08, "elev": 762, "county": "Whitman",      "state": "WA"},
    {"id": "palouse",       "name": "Palouse",             "lat": 46.91, "lon": -117.08, "elev": 762, "county": "Whitman",      "state": "WA"},

    # ── Ferry / Stevens / Pend Oreille ───────────────────────────────────────
    {"id": "kettle_falls",  "name": "Kettle Falls",        "lat": 48.61, "lon": -118.06, "elev": 472, "county": "Stevens",      "state": "WA"},
    {"id": "newport",       "name": "Newport",             "lat": 48.18, "lon": -117.04, "elev": 637, "county": "Pend Oreille", "state": "WA"},

    # ── Mason / Grays Harbor / Pacific ───────────────────────────────────────
    {"id": "shelton",       "name": "Shelton",             "lat": 47.22, "lon": -123.10, "elev": 18,  "county": "Mason",        "state": "WA"},
    {"id": "montesano",     "name": "Montesano",           "lat": 46.98, "lon": -123.60, "elev": 12,  "county": "Grays Harbor", "state": "WA"},
    {"id": "raymond",       "name": "Raymond",             "lat": 46.69, "lon": -123.73, "elev": 5,   "county": "Pacific",      "state": "WA"},

    # ── Island / San Juan ────────────────────────────────────────────────────
    {"id": "whidbey_island","name": "Whidbey Island",      "lat": 48.22, "lon": -122.75, "elev": 30,  "county": "Island",       "state": "WA"},
    {"id": "san_juan_island","name":"San Juan Island",     "lat": 48.53, "lon": -123.02, "elev": 37,  "county": "San Juan",     "state": "WA"},

    # ── Benton / Franklin additional ─────────────────────────────────────────
    {"id": "richland",      "name": "Richland",            "lat": 46.29, "lon": -119.28, "elev": 122, "county": "Benton",       "state": "WA"},
    {"id": "west_richland", "name": "West Richland",       "lat": 46.30, "lon": -119.36, "elev": 134, "county": "Benton",       "state": "WA"},
]

# ---------------------------------------------------------------------------
# Observed parameters at AgWeatherNet stations
# ---------------------------------------------------------------------------

_PARAMS = [
    "temperature", "relative_humidity", "dewpoint", "soil_temperature",
    "rainfall", "wind_speed", "wind_direction", "solar_radiation",
    "leaf_wetness",
]

_SOURCE = "wsu_agweathernet"


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _build_station(row: dict) -> Station:
    """Convert an inventory row into a Station model."""
    return Station(
        id=row["id"],
        name=row["name"],
        lat=row["lat"],
        lon=row["lon"],
        source=_SOURCE,
        network="AgWeatherNet",
        params=_PARAMS,
        elevation=row.get("elev"),
        url=f"https://weather.wsu.edu/",
        extra={
            "county": row.get("county", ""),
            "state": row.get("state", "WA"),
            "synoptic_network": 3007,
        },
    )


def _get_all_stations() -> list[Station]:
    """Return all stations from the inventory."""
    return [_build_station(row) for row in _STATIONS]


# ---------------------------------------------------------------------------
# 1. GET /api/wsu_agweather/stations — full station inventory
# ---------------------------------------------------------------------------

@router.get("/stations")
async def wsu_agweather_stations(
    state: str | None = Query(None, description="Filter by state (WA, OR, ID)"),
    county: str | None = Query(None, description="Filter by county name"),
):
    """Return the WSU AgWeatherNet station inventory with coordinates.

    This is a metadata endpoint — it returns station locations and info.
    For live observations, use the Synoptic source with network=3007.
    """
    cache_key = f"wsu_agweather:stations:{state}:{county}"
    hit = await cache.get(cache_key)
    if hit is not None:
        return hit

    stations = _get_all_stations()

    # Apply filters
    if state:
        state_upper = state.upper()
        stations = [s for s in stations if s.extra.get("state", "").upper() == state_upper]
    if county:
        county_lower = county.lower()
        stations = [s for s in stations if county_lower in s.extra.get("county", "").lower()]

    result = {
        "source": SOURCE_NAME,
        "station_count": len(stations),
        "filters": {"state": state, "county": county},
        "stations": [s.model_dump(exclude_none=True) for s in stations],
    }
    await cache.set(cache_key, result, ttl=config.CACHE_TTL_LONG)
    return result


# ---------------------------------------------------------------------------
# 2. GET /api/wsu_agweather/current — latest data (proxied via Synoptic)
# ---------------------------------------------------------------------------

@router.get("/current")
async def wsu_agweather_current(
    state: str | None = Query(None, description="Filter by state (WA, OR, ID)"),
):
    """Return latest observations from AgWeatherNet stations.

    This endpoint attempts to fetch live data via Synoptic Data (network 3007).
    If SYNOPTIC_TOKEN is not configured, it returns station metadata only.
    """
    cache_key = f"wsu_agweather:current:{state}"
    hit = await cache.get(cache_key)
    if hit is not None:
        return hit

    # Try Synoptic for live data
    synoptic_token = config.SYNOPTIC_TOKEN
    if synoptic_token:
        result = await _fetch_current_via_synoptic(synoptic_token, state)
    else:
        # Fallback: return station inventory with a note
        stations = _get_all_stations()
        if state:
            state_upper = state.upper()
            stations = [s for s in stations if s.extra.get("state", "").upper() == state_upper]
        result = {
            "source": SOURCE_NAME,
            "station_count": len(stations),
            "note": (
                "Live observations require SYNOPTIC_TOKEN.  "
                "Returning station metadata only.  "
                "Set SYNOPTIC_TOKEN env var for live data (free at synopticdata.com)."
            ),
            "stations": [
                {"station": s.model_dump(exclude_none=True), "readings": []}
                for s in stations
            ],
        }

    await cache.set(cache_key, result, ttl=config.CACHE_TTL)
    return result


async def _fetch_current_via_synoptic(token: str, state: str | None) -> dict:
    """Fetch latest AgWeatherNet observations from Synoptic Data API."""
    import httpx
    from models import Reading

    params: dict[str, str] = {
        "token": token,
        "network": "3007",
        "vars": "air_temp,relative_humidity,dew_point_temperature,wind_speed,wind_direction,precip_accum,solar_radiation",
        "status": "active",
        "within": "120",  # last 2 hours
        "units": "metric",
    }
    if state:
        params["state"] = state.upper()

    url = "https://api.synopticdata.com/v2/stations/latest"
    try:
        async with httpx.AsyncClient(
            timeout=config.HTTP_TIMEOUT,
            headers={"User-Agent": config.USER_AGENT},
        ) as client:
            resp = await client.get(url, params=params)
            resp.raise_for_status()
            data = resp.json()
    except Exception as exc:
        logger.warning("WSU AgWeatherNet Synoptic fetch failed: %s", exc)
        return {
            "source": SOURCE_NAME,
            "error": f"Synoptic API error: {exc}",
            "station_count": 0,
            "stations": [],
        }

    # Synoptic variable -> (param_name, unit)
    _VAR_MAP = {
        "air_temp": ("temperature", "°C"),
        "relative_humidity": ("relative_humidity", "%"),
        "dew_point_temperature": ("dewpoint", "°C"),
        "wind_speed": ("wind_speed", "m/s"),
        "wind_direction": ("wind_direction", "°"),
        "precip_accum": ("rainfall", "mm"),
        "solar_radiation": ("solar_radiation", "W/m²"),
    }

    station_list = data.get("STATION", [])
    results: list[dict] = []
    for stn in station_list:
        lat = float(stn.get("LATITUDE", 0))
        lon = float(stn.get("LONGITUDE", 0))
        stn_id = stn.get("STID", "").lower()
        stn_name = stn.get("NAME", stn_id)
        elev_ft = stn.get("ELEVATION")
        elev_m = round(float(elev_ft) * 0.3048, 1) if elev_ft else None

        readings: list[dict] = []
        obs = stn.get("OBSERVATIONS", {})
        latest_ts: datetime | None = None
        for syn_var, (param_name, unit) in _VAR_MAP.items():
            val_key = f"{syn_var}_value_1"
            ts_key = f"{syn_var}_date_time_1" if f"{syn_var}_date_time_1" in obs else None

            # Synoptic latest returns {var: {value: x, date_time: t}}
            var_data = obs.get(syn_var)
            if var_data is None:
                continue

            # Handle both dict and direct value formats
            if isinstance(var_data, dict):
                val = var_data.get("value")
                ts = var_data.get("date_time")
            elif isinstance(var_data, (int, float)):
                val = var_data
                ts = None
            else:
                continue

            # Track the most recent observation timestamp for last_update
            if ts:
                try:
                    parsed_ts = datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
                    if latest_ts is None or parsed_ts > latest_ts:
                        latest_ts = parsed_ts
                except (ValueError, TypeError):
                    pass

            if val is not None:
                try:
                    readings.append(Reading(
                        station_id=stn_id,
                        timestamp=ts or "1970-01-01T00:00:00Z",
                        parameter=param_name,
                        value=float(val),
                        unit=unit,
                        source=_SOURCE,
                    ).model_dump(exclude_none=True))
                except Exception:
                    pass

        station = Station(
            id=stn_id,
            name=stn_name,
            lat=lat,
            lon=lon,
            source=_SOURCE,
            network="AgWeatherNet",
            params=_PARAMS,
            elevation=elev_m,
            url="https://weather.wsu.edu/",
            last_update=latest_ts,
            extra={
                "state": stn.get("STATE", ""),
                "synoptic_stid": stn.get("STID", ""),
                "synoptic_network": 3007,
            },
        )

        results.append({
            "station": station.model_dump(exclude_none=True),
            "readings": readings,
        })

    return {
        "source": SOURCE_NAME,
        "station_count": len(results),
        "data_via": "Synoptic Data (network 3007)",
        "stations": results,
    }


# ---------------------------------------------------------------------------
# 3. GET /api/wsu_agweather/overview — source info
# ---------------------------------------------------------------------------

@router.get("/overview")
async def wsu_agweather_overview():
    """Return source metadata and station summary for WSU AgWeatherNet."""
    cache_key = "wsu_agweather:overview"
    hit = await cache.get(cache_key)
    if hit is not None:
        return hit

    stations = _get_all_stations()

    # Group by state
    by_state: dict[str, int] = {}
    by_county: dict[str, int] = {}
    for s in stations:
        st = s.extra.get("state", "WA")
        by_state[st] = by_state.get(st, 0) + 1
        county = s.extra.get("county", "Unknown")
        by_county[county] = by_county.get(county, 0) + 1

    result = {
        "source": SOURCE_NAME,
        "description": SOURCE_DESCRIPTION,
        "meta": SOURCE_META,
        "station_count": len(stations),
        "by_state": dict(sorted(by_state.items())),
        "by_county": dict(sorted(by_county.items(), key=lambda x: -x[1])),
        "synoptic_access": {
            "network_id": 3007,
            "requires": "SYNOPTIC_TOKEN env var (free at synopticdata.com)",
            "configured": bool(config.SYNOPTIC_TOKEN),
        },
        "stations": [s.model_dump(exclude_none=True) for s in stations],
    }
    await cache.set(cache_key, result, ttl=config.CACHE_TTL_LONG)
    return result
