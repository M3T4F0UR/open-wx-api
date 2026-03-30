"""Southwest County Flood Control Districts — rain, stream, and weather data.

Covers two county-level ALERT telemetry networks in the Desert Southwest:

  1. Clark County Regional Flood Control District (CCRFCD), Nevada
     - 200+ rain gauges, 150+ water-level stations, 15+ weather stations
     - Covers Las Vegas metro, Lake Mead NRA, Moapa, and surrounding desert
     - Data via ASP.NET server-rendered pages (no JSON API exposed)
     - Update frequency: 5 minutes

  2. Pima County Regional Flood Control District (RFCD), Arizona
     - ~80 rain gauges, ~60 stream gauges across 12 watersheds
     - Covers Tucson metro, Rillito Creek, Santa Cruz River, Canada Del Oro
     - Data via PHP-rendered HTML tables (scraped to structured JSON)
     - Update frequency: 5–15 minutes

All endpoints are unauthenticated.

References:
  - https://gustfront.ccrfcd.org/gagemap/gagemap.html
  - https://gustfront.ccrfcd.org/stations/default.aspx
  - https://alertmap.rfcd.pima.gov/gmap/gmap.html
"""
from __future__ import annotations

import logging
import re
from datetime import datetime, timedelta, timezone
from html.parser import HTMLParser
from typing import Any, Optional

import httpx
from fastapi import APIRouter, HTTPException, Query

import config
from cache import cached

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Module-level metadata (consumed by server.py /api/sources)
# ---------------------------------------------------------------------------

SOURCE_NAME = "Southwest County Flood Control"
SOURCE_DESCRIPTION = (
    "Clark County NV (Las Vegas) and Pima County AZ (Tucson) flood control "
    "district ALERT networks: 200+ rain gauges, 150+ water-level stations, "
    "15+ weather stations (Clark); ~80 rain gauges and ~60 stream gauges "
    "(Pima).  Data updates every 5–15 minutes.  No auth required."
)
SOURCE_META = {
    "coverage": "Clark County NV (Las Vegas) and Pima County AZ (Tucson)",
    "category": "water",
    "auth_required": False,
    "params": [
        "precipitation", "stage", "discharge",
        "temperature", "humidity", "dewpoint",
        "wind_speed", "wind_direction",
    ],
    "update_frequency": "5–15 min",
    "url": "https://gustfront.ccrfcd.org/gagemap/gagemap.html",
}

# ---------------------------------------------------------------------------
# Router
# ---------------------------------------------------------------------------

router = APIRouter(prefix="/api/sw_flood", tags=["Southwest County Flood Control"])

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_SOURCE_CLARK = "clark_county_flood"
_SOURCE_PIMA = "pima_county_flood"

# ── Clark County (CCRFCD) endpoints ──────────────────────────────────────
_CLARK_STATION_LIST = "https://gustfront.ccrfcd.org/stations/default.aspx"
_CLARK_GAGE_DATA_LIST = "https://gustfront.ccrfcd.org/gagedatalist/default.aspx"
_CLARK_GAGE_MAP = "https://gustfront.ccrfcd.org/gagemap/gagemap.html"
_CLARK_STATIC_MAP = "https://gustfront.ccrfcd.org/gageMapStatic/default.aspx"

# ── Pima County (RFCD) PHP table endpoints ───────────────────────────────
_PIMA_RAIN_URL = "https://alertmap.rfcd.pima.gov/gmap/gmap_files/php/Tables/pgi.php"
_PIMA_STREAM_URL = "https://alertmap.rfcd.pima.gov/gmap/gmap_files/php/Tables/sgi.php"
_PIMA_MAP_URL = "https://alertmap.rfcd.pima.gov/gmap/gmap.html"

# Arizona MST (no DST)
_AZ_TZ = timezone(timedelta(hours=-7))
# Pacific Time (Clark County NV is PT)
_PT_OFFSET = timezone(timedelta(hours=-8))   # PST; PDT is -7

# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------

_HEADERS = {"User-Agent": config.USER_AGENT, "Accept": "*/*"}


def _client() -> httpx.AsyncClient:
    return httpx.AsyncClient(
        timeout=config.HTTP_TIMEOUT,
        headers=_HEADERS,
        follow_redirects=True,
    )


# ===========================================================================
#
#  PIMA COUNTY (Tucson) — HTML table scraping
#
# ===========================================================================

# ---------------------------------------------------------------------------
# Pima rain-gauge HTML parser
# ---------------------------------------------------------------------------

class _PimaRainParser(HTMLParser):
    """Parse the Pima County precipitation gauge HTML table (pgi.php).

    The page has region headers in <strong> tags followed by HTML tables.
    Each data row has columns:
        Alert Sensor ID | Station Name | 15min | 30min | 1hr | 3hr | 6hr | 12hr | 24hr
    """

    _PRECIP_COLS = [
        ("precip_15min", "in"),
        ("precip_30min", "in"),
        ("precip_1hr", "in"),
        ("precip_3hr", "in"),
        ("precip_6hr", "in"),
        ("precip_12hr", "in"),
        ("precip_24hr", "in"),
    ]

    def __init__(self) -> None:
        super().__init__()
        self.stations: list[dict] = []
        self.readings: list[dict] = []
        self.update_time: Optional[str] = None
        self._region = ""
        self._in_table = False
        self._in_row = False
        self._in_cell = False
        self._in_header_row = False
        self._cells: list[str] = []
        self._cell_text = ""
        self._in_strong = False
        self._strong_text = ""
        self._row_count = 0
        self._full_text_parts: list[str] = []

    def handle_starttag(self, tag: str, attrs: list) -> None:
        if tag == "table":
            self._in_table = True
            self._row_count = 0
        elif tag == "tr" and self._in_table:
            self._in_row = True
            self._cells = []
        elif tag in ("td", "th") and self._in_row:
            self._in_cell = True
            self._cell_text = ""
        elif tag == "strong":
            self._in_strong = True
            self._strong_text = ""

    def handle_endtag(self, tag: str) -> None:
        if tag == "strong" and self._in_strong:
            self._in_strong = False
            text = self._strong_text.strip()
            # Region headers are <strong> tags outside table rows
            if not self._in_cell and text and not text.startswith("Alert") and not text.isdigit():
                self._region = text
        if tag in ("td", "th") and self._in_cell:
            self._in_cell = False
            self._cells.append(self._cell_text.strip())
        elif tag == "tr" and self._in_row:
            self._in_row = False
            self._row_count += 1
            # Skip header row (first row of each table)
            if self._row_count == 1:
                return
            self._process_rain_row(self._cells)
        elif tag == "table":
            self._in_table = False

    def handle_data(self, data: str) -> None:
        self._full_text_parts.append(data)
        if self._in_cell:
            self._cell_text += data
        if self._in_strong:
            self._strong_text += data
        # Look for update timestamp
        if "Updated:" in data:
            m = re.search(r"Updated:\s*(\d{2}/\d{2}/\d{4}\s+\d{2}:\d{2}:\d{2}\s*[AP]M)", data)
            if m:
                self.update_time = m.group(1).strip()

    def _process_rain_row(self, cells: list[str]) -> None:
        """Process a single rain-gauge data row."""
        if len(cells) < 9:
            return
        sensor_id = cells[0].strip()
        if not sensor_id or not sensor_id.isdigit():
            return
        name = cells[1].strip()

        self.stations.append({
            "id": sensor_id,
            "name": name,
            "source": _SOURCE_PIMA,
            "network": "rain",
            "region": self._region,
        })

        for i, (param, unit) in enumerate(self._PRECIP_COLS):
            try:
                val = float(cells[2 + i])
            except (ValueError, IndexError):
                continue
            self.readings.append({
                "station_id": sensor_id,
                "parameter": param,
                "value": round(val, 3),
                "unit": unit,
                "source": _SOURCE_PIMA,
            })


# ---------------------------------------------------------------------------
# Pima stream-gauge HTML parser
# ---------------------------------------------------------------------------

class _PimaStreamParser(HTMLParser):
    """Parse the Pima County stream gauge HTML table (sgi.php).

    Each watershed section has a header, then a table with rows:
        Device ID | Station Name | Time | Stage (ft) | Discharge (cfs)
    """

    def __init__(self) -> None:
        super().__init__()
        self.stations: list[dict] = []
        self.readings: list[dict] = []
        self.update_time: Optional[str] = None
        self._watershed = ""
        self._in_table = False
        self._in_row = False
        self._in_cell = False
        self._in_header_row = False
        self._cells: list[str] = []
        self._cell_text = ""
        self._in_strong = False
        self._strong_text = ""
        self._row_count = 0

    def handle_starttag(self, tag: str, attrs: list) -> None:
        if tag == "table":
            self._in_table = True
            self._row_count = 0
        elif tag == "tr" and self._in_table:
            self._in_row = True
            self._cells = []
        elif tag in ("td", "th") and self._in_row:
            self._in_cell = True
            self._cell_text = ""
        elif tag == "strong":
            self._in_strong = True
            self._strong_text = ""

    def handle_endtag(self, tag: str) -> None:
        if tag == "strong" and self._in_strong:
            self._in_strong = False
            text = self._strong_text.strip()
            if not self._in_cell and text and not text.startswith("Device"):
                self._watershed = text
        if tag in ("td", "th") and self._in_cell:
            self._in_cell = False
            self._cells.append(self._cell_text.strip())
        elif tag == "tr" and self._in_row:
            self._in_row = False
            self._row_count += 1
            if self._row_count == 1:
                return
            self._process_stream_row(self._cells)
        elif tag == "table":
            self._in_table = False

    def handle_data(self, data: str) -> None:
        if self._in_cell:
            self._cell_text += data
        if self._in_strong:
            self._strong_text += data
        if "Updated:" in data:
            m = re.search(r"Updated:\s*(\d{2}/\d{2}/\d{4}\s+\d{2}:\d{2}:\d{2}\s*[AP]M)", data)
            if m:
                self.update_time = m.group(1).strip()

    def _process_stream_row(self, cells: list[str]) -> None:
        """Process a single stream-gauge data row."""
        if len(cells) < 4:
            return
        device_id = cells[0].strip()
        if not device_id or not device_id.isdigit():
            return
        name = cells[1].strip()

        # Timestamp may be in cell 2
        ts_str = cells[2].strip() if len(cells) > 2 else ""
        ts_iso = None
        if ts_str:
            # Format: HH:MM:SS MM/DD/YYYY
            m = re.match(r"(\d{2}:\d{2}:\d{2})\s+(\d{2}/\d{2}/\d{4})", ts_str)
            if m:
                try:
                    dt = datetime.strptime(
                        f"{m.group(2)} {m.group(1)}", "%m/%d/%Y %H:%M:%S"
                    ).replace(tzinfo=_AZ_TZ)
                    ts_iso = dt.isoformat()
                except ValueError:
                    pass

        self.stations.append({
            "id": device_id,
            "name": name,
            "source": _SOURCE_PIMA,
            "network": "stream",
            "watershed": self._watershed,
            "timestamp": ts_iso,
        })

        # Stage (ft) — column 3
        if len(cells) > 3:
            try:
                stage = float(cells[3])
                self.readings.append({
                    "station_id": device_id,
                    "parameter": "stage",
                    "value": round(stage, 2),
                    "unit": "ft",
                    "timestamp": ts_iso,
                    "source": _SOURCE_PIMA,
                })
            except ValueError:
                pass

        # Discharge (cfs) — column 4
        if len(cells) > 4:
            try:
                discharge = float(cells[4])
                self.readings.append({
                    "station_id": device_id,
                    "parameter": "discharge",
                    "value": round(discharge, 1),
                    "unit": "cfs",
                    "timestamp": ts_iso,
                    "source": _SOURCE_PIMA,
                })
            except ValueError:
                pass


# ---------------------------------------------------------------------------
# Pima County fetch + cache helpers
# ---------------------------------------------------------------------------

@cached(ttl=300, prefix="pima_rain:")
async def _fetch_pima_rain_html() -> str:
    """Fetch the Pima County precipitation gauge HTML page."""
    async with _client() as client:
        resp = await client.get(_PIMA_RAIN_URL)
        resp.raise_for_status()
        return resp.text


@cached(ttl=300, prefix="pima_stream:")
async def _fetch_pima_stream_html() -> str:
    """Fetch the Pima County stream gauge HTML page."""
    async with _client() as client:
        resp = await client.get(_PIMA_STREAM_URL)
        resp.raise_for_status()
        return resp.text


def _parse_pima_rain(html: str) -> dict:
    """Parse Pima rain gauge HTML into structured data."""
    parser = _PimaRainParser()
    parser.feed(html)
    return {
        "stations": parser.stations,
        "readings": parser.readings,
        "update_time": parser.update_time,
    }


def _parse_pima_stream(html: str) -> dict:
    """Parse Pima stream gauge HTML into structured data."""
    parser = _PimaStreamParser()
    parser.feed(html)
    return {
        "stations": parser.stations,
        "readings": parser.readings,
        "update_time": parser.update_time,
    }


# ---------------------------------------------------------------------------
# Pima County endpoints
# ---------------------------------------------------------------------------

@router.get("/pima/rain", summary="Pima County rain gauge data (scraped)")
async def pima_rain(
    min_precip_1hr: float = Query(0.0, description="Only return gauges with >= this 1-hour total (inches)"),
    region: Optional[str] = Query(None, description="Filter by region name (case-insensitive substring)"),
) -> dict:
    """Return current precipitation accumulations from ~80 Pima County rain gauges.

    Each gauge reports 15-min through 24-hour accumulations.
    Data is scraped from the RFCD ALERT system PHP page and cached for 5 minutes.
    """
    html = await _fetch_pima_rain_html()
    data = _parse_pima_rain(html)

    stations = data["stations"]
    readings = data["readings"]

    # Apply region filter
    if region:
        region_lower = region.lower()
        matching_ids = {
            s["id"] for s in stations
            if region_lower in s.get("region", "").lower()
        }
        stations = [s for s in stations if s["id"] in matching_ids]
        readings = [r for r in readings if r["station_id"] in matching_ids]

    # Apply minimum precip filter
    if min_precip_1hr > 0:
        precip_1hr = {
            r["station_id"]: r["value"]
            for r in readings
            if r["parameter"] == "precip_1hr"
        }
        matching_ids = {
            sid for sid, val in precip_1hr.items() if val >= min_precip_1hr
        }
        stations = [s for s in stations if s["id"] in matching_ids]
        readings = [r for r in readings if r["station_id"] in matching_ids]

    return {
        "source": _SOURCE_PIMA,
        "network": "rain",
        "station_count": len(stations),
        "reading_count": len(readings),
        "update_time": data["update_time"],
        "stations": stations,
        "readings": readings,
    }


@router.get("/pima/stream", summary="Pima County stream gauge data (scraped)")
async def pima_stream(
    min_stage: float = Query(0.0, description="Only return gauges with stage >= this value (ft)"),
    watershed: Optional[str] = Query(None, description="Filter by watershed name (case-insensitive substring)"),
) -> dict:
    """Return current stage and discharge from ~60 Pima County stream gauges.

    Data is scraped from the RFCD ALERT system PHP page and cached for 5 minutes.
    Gauges span 12 major watersheds in the Tucson metro area.
    """
    html = await _fetch_pima_stream_html()
    data = _parse_pima_stream(html)

    stations = data["stations"]
    readings = data["readings"]

    # Apply watershed filter
    if watershed:
        ws_lower = watershed.lower()
        matching_ids = {
            s["id"] for s in stations
            if ws_lower in s.get("watershed", "").lower()
        }
        stations = [s for s in stations if s["id"] in matching_ids]
        readings = [r for r in readings if r["station_id"] in matching_ids]

    # Apply minimum stage filter
    if min_stage > 0:
        stage_vals = {
            r["station_id"]: r["value"]
            for r in readings
            if r["parameter"] == "stage"
        }
        matching_ids = {
            sid for sid, val in stage_vals.items() if val >= min_stage
        }
        stations = [s for s in stations if s["id"] in matching_ids]
        readings = [r for r in readings if r["station_id"] in matching_ids]

    return {
        "source": _SOURCE_PIMA,
        "network": "stream",
        "station_count": len(stations),
        "reading_count": len(readings),
        "update_time": data["update_time"],
        "stations": stations,
        "readings": readings,
    }


# ===========================================================================
#
#  CLARK COUNTY (Las Vegas) — metadata + station registry
#
#  The CCRFCD ALERT system uses an ASP.NET application with server-rendered
#  HTML pages.  No JSON API has been identified.  The station list page
#  provides a comprehensive registry of gauge IDs and names but no real-time
#  data in a scrapable format.  We expose a curated station registry and
#  metadata endpoints; users can follow the provided URLs to access the
#  interactive map for real-time readings.
#
# ===========================================================================

# Curated subset of Clark County rain gauge stations (Las Vegas area).
# Full list has 200+ stations; this covers major drainage basins.
_CLARK_RAIN_STATIONS: list[dict[str, Any]] = [
    # ── Las Vegas Wash basin ──
    {"id": "4004", "name": "Las Vegas Wash at Nellis", "basin": "Las Vegas Wash"},
    {"id": "4024", "name": "Upper Las Vegas Wash DB", "basin": "Las Vegas Wash"},
    {"id": "4064", "name": "Lower Las Vegas Wash DB", "basin": "Las Vegas Wash"},
    {"id": "4069", "name": "Las Vegas Wash at Craig Rd", "basin": "Las Vegas Wash"},
    {"id": "4074", "name": "Las Vegas Wash at Lake Mead", "basin": "Las Vegas Wash"},
    {"id": "4084", "name": "Las Vegas Wash above Flamingo Wash", "basin": "Las Vegas Wash"},
    {"id": "4089", "name": "Las Vegas Wash below Vegas Valley Dr (USGS)", "basin": "Las Vegas Wash"},
    {"id": "4284", "name": "Las Vegas Wash at Lamb", "basin": "Las Vegas Wash"},
    {"id": "4529", "name": "Las Vegas Wash at LV Blvd", "basin": "Las Vegas Wash"},
    {"id": "4534", "name": "Upper Las Vegas Wash 1", "basin": "Las Vegas Wash"},
    {"id": "4544", "name": "Las Vegas Wash at Pabco Rd", "basin": "Las Vegas Wash"},
    {"id": "4599", "name": "Las Vegas Wash at The Club at Sunrise (USGS)", "basin": "Las Vegas Wash"},
    # ── Flamingo Wash ──
    {"id": "4334", "name": "Upper Flamingo 1", "basin": "Flamingo Wash"},
    {"id": "4349", "name": "Upper Flamingo DB", "basin": "Flamingo Wash"},
    {"id": "4364", "name": "Flamingo Wash at Torrey Pines Dr", "basin": "Flamingo Wash"},
    {"id": "4374", "name": "Flamingo Wash at Eastern Ave", "basin": "Flamingo Wash"},
    {"id": "4394", "name": "Flamingo Wash at Nellis Blvd (USGS)", "basin": "Flamingo Wash"},
    {"id": "4399", "name": "Flamingo Wash at Mojave", "basin": "Flamingo Wash"},
    {"id": "4574", "name": "Flamingo Wash at Spencer", "basin": "Flamingo Wash"},
    # ── Duck Creek ──
    {"id": "4619", "name": "Lower Duck Creek DB", "basin": "Duck Creek"},
    {"id": "4624", "name": "Duck Creek at Paradise", "basin": "Duck Creek"},
    {"id": "4634", "name": "Duck Creek at Eastern", "basin": "Duck Creek"},
    {"id": "4659", "name": "Duck Creek above Sunset", "basin": "Duck Creek"},
    {"id": "4674", "name": "Duck Creek near Tomiyasu", "basin": "Duck Creek"},
    {"id": "4694", "name": "Duck Creek at Broadbent", "basin": "Duck Creek"},
    # ── Sloan Channel ──
    {"id": "4114", "name": "Sloan Channel at Las Vegas Blvd", "basin": "Sloan Channel"},
    {"id": "4174", "name": "Sloan Channel at Charleston (USGS)", "basin": "Sloan Channel"},
    {"id": "4199", "name": "Sloan Channel below Sahara", "basin": "Sloan Channel"},
    {"id": "4704", "name": "Sloan at I-15", "basin": "Sloan Channel"},
    # ── Pittman Wash ──
    {"id": "4714", "name": "Pittman Pecos", "basin": "Pittman Wash"},
    {"id": "4734", "name": "Pittman Wash at Wigwam", "basin": "Pittman Wash"},
    {"id": "4749", "name": "Pittman Wash at Stephanie", "basin": "Pittman Wash"},
    {"id": "4759", "name": "Pittman Park DB", "basin": "Pittman Wash"},
    # ── C-1 Channel ──
    {"id": "4549", "name": "NE C-1 DB", "basin": "C-1 Channel"},
    {"id": "4554", "name": "Lower C-1 Channel (USGS)", "basin": "C-1 Channel"},
    {"id": "4779", "name": "C-1 Channel near Boulder Hwy", "basin": "C-1 Channel"},
    {"id": "4784", "name": "C-1 Channel at Warm Springs (USGS)", "basin": "C-1 Channel"},
    # ── Central Las Vegas ──
    {"id": "4274", "name": "Downtown Las Vegas", "basin": "Central LV"},
    {"id": "4244", "name": "Oakey at Rainbow", "basin": "Central LV"},
    {"id": "4289", "name": "Freeway Channel near Charleston", "basin": "Central LV"},
    {"id": "4309", "name": "Desert Inn DB", "basin": "Central LV"},
    # ── Summerlin / Red Rock ──
    {"id": "4204", "name": "Summerlin West", "basin": "Summerlin"},
    {"id": "4209", "name": "Summerlin NW", "basin": "Summerlin"},
    {"id": "4324", "name": "Red Rock Canyon", "basin": "Red Rock"},
    {"id": "4329", "name": "Brownstone Canyon", "basin": "Red Rock"},
    {"id": "4344", "name": "Red Rock DB", "basin": "Red Rock"},
    # ── Henderson / Boulder City ──
    {"id": "4724", "name": "Anthem DB", "basin": "Henderson"},
    {"id": "4729", "name": "McCullough Hills DB", "basin": "Henderson"},
    {"id": "4789", "name": "East Henderson 2", "basin": "Henderson"},
    {"id": "4834", "name": "Boulder City SW", "basin": "Boulder City"},
    {"id": "4844", "name": "Bootleg Canyon DB", "basin": "Boulder City"},
    {"id": "4854", "name": "Boulder City", "basin": "Boulder City"},
    # ── North Las Vegas ──
    {"id": "4009", "name": "Floyd Lamb North EEA", "basin": "North LV"},
    {"id": "4034", "name": "North Valley 1", "basin": "North LV"},
    {"id": "4039", "name": "North Valley 2", "basin": "North LV"},
    {"id": "4054", "name": "Lone Mtn at Hualapai", "basin": "North LV"},
    {"id": "4104", "name": "Nellis AFB North", "basin": "North LV"},
    {"id": "4504", "name": "Beltway Channel at Cheyenne", "basin": "North LV"},
    {"id": "4539", "name": "Beltway Channel at Aliante", "basin": "North LV"},
    {"id": "4579", "name": "Skye Canyon 1 DB", "basin": "North LV"},
    # ── Kyle Canyon / Mt Charleston ──
    {"id": "4029", "name": "Kyle Canyon DB", "basin": "Kyle Canyon"},
    {"id": "4044", "name": "Castle Rock", "basin": "Kyle Canyon"},
    # ── Outlying / NPS ──
    {"id": "1", "name": "Willow Beach 1 (NPS)", "basin": "Lake Mead NRA"},
    {"id": "4", "name": "Malpais Flattop Mesa (NPS)", "basin": "Lake Mead NRA"},
    {"id": "7", "name": "Eldorado (NPS)", "basin": "Lake Mead NRA"},
    {"id": "14", "name": "Cottonwood Cove West (NPS)", "basin": "Lake Mead NRA"},
    {"id": "4814", "name": "Nelson", "basin": "Outlying"},
    {"id": "4144", "name": "Apex", "basin": "Outlying"},
    {"id": "4614", "name": "Arden", "basin": "Outlying"},
]

# Curated Clark County weather stations with sensor types
_CLARK_WX_STATIONS: list[dict[str, Any]] = [
    {"id": "4010", "name": "Fossil Ridge", "basin": "North LV",
     "sensors": ["wind_direction", "wind_speed", "peak_wind", "temperature", "humidity", "dewpoint", "battery"]},
    {"id": "4030", "name": "North Valley 1", "basin": "North LV",
     "sensors": ["wind_direction", "wind_speed", "peak_wind", "temperature", "humidity", "dewpoint", "battery"]},
    {"id": "4100", "name": "Nellis AFB North", "basin": "North LV",
     "sensors": ["wind_direction", "wind_speed", "peak_wind", "temperature", "humidity", "dewpoint", "battery"]},
    {"id": "4197", "name": "Summerlin West", "basin": "Summerlin",
     "sensors": ["wind_direction", "wind_speed", "peak_wind", "temperature", "humidity", "dewpoint", "battery"]},
    {"id": "4220", "name": "Angel Park West", "basin": "Central LV",
     "sensors": ["wind_direction", "wind_speed", "peak_wind", "temperature", "humidity", "dewpoint", "battery"]},
    {"id": "4270", "name": "Downtown Las Vegas", "basin": "Central LV",
     "sensors": ["wind_direction", "wind_speed", "peak_wind", "temperature", "humidity", "dewpoint", "battery"]},
    {"id": "4300", "name": "Blue Diamond Ridge South", "basin": "Red Rock",
     "sensors": ["wind_direction", "wind_speed", "peak_wind", "temperature", "humidity", "dewpoint", "battery"]},
    {"id": "4350", "name": "The Lakes", "basin": "Central LV",
     "sensors": ["wind_direction", "wind_speed", "peak_wind", "temperature", "humidity", "dewpoint", "battery"]},
    {"id": "4640", "name": "NWS Offices", "basin": "Central LV",
     "sensors": ["wind_direction", "wind_speed", "peak_wind", "temperature", "humidity", "dewpoint", "battery"]},
    {"id": "4700", "name": "Sloan at I-15", "basin": "Sloan Channel",
     "sensors": ["wind_direction", "wind_speed", "peak_wind", "temperature", "humidity", "dewpoint", "battery"]},
    {"id": "4767", "name": "Borman", "basin": "Henderson",
     "sensors": ["wind_direction", "wind_speed", "peak_wind", "temperature", "humidity", "dewpoint", "battery"]},
    {"id": "4810", "name": "Nelson", "basin": "Outlying",
     "sensors": ["wind_direction", "wind_speed", "peak_wind", "temperature", "humidity", "dewpoint", "battery"]},
    {"id": "4829", "name": "Boulder City SW", "basin": "Boulder City",
     "sensors": ["wind_direction", "wind_speed", "peak_wind", "temperature", "humidity", "dewpoint", "battery"]},
    {"id": "4850", "name": "Boulder City", "basin": "Boulder City",
     "sensors": ["wind_direction", "wind_speed", "peak_wind", "temperature", "humidity", "dewpoint", "battery"]},
    {"id": "4139", "name": "Apex", "basin": "Outlying",
     "sensors": ["wind_direction", "wind_speed", "peak_wind", "temperature", "humidity", "dewpoint", "battery"]},
]

# Curated Clark County water-level (WL) stations for major channels
_CLARK_WL_STATIONS: list[dict[str, Any]] = [
    {"id": "4003", "name": "Las Vegas Wash at Nellis", "basin": "Las Vegas Wash"},
    {"id": "4023", "name": "Upper Las Vegas Wash DB", "basin": "Las Vegas Wash"},
    {"id": "4063", "name": "Lower Las Vegas Wash DB", "basin": "Las Vegas Wash"},
    {"id": "4073", "name": "Las Vegas Wash at Lake Mead", "basin": "Las Vegas Wash"},
    {"id": "4083", "name": "Las Vegas Wash above Flamingo Wash", "basin": "Las Vegas Wash"},
    {"id": "4113", "name": "Sloan Channel at Las Vegas Blvd", "basin": "Sloan Channel"},
    {"id": "4173", "name": "Sloan Channel at Charleston (USGS)", "basin": "Sloan Channel"},
    {"id": "4198", "name": "Sloan Channel below Sahara", "basin": "Sloan Channel"},
    {"id": "4283", "name": "Las Vegas Wash at Lamb", "basin": "Las Vegas Wash"},
    {"id": "4363", "name": "Flamingo Wash at Torrey Pines Dr", "basin": "Flamingo Wash"},
    {"id": "4373", "name": "Flamingo Wash at Eastern Ave", "basin": "Flamingo Wash"},
    {"id": "4393", "name": "Flamingo Wash at Nellis Blvd (USGS)", "basin": "Flamingo Wash"},
    {"id": "4398", "name": "Flamingo Wash at Mojave", "basin": "Flamingo Wash"},
    {"id": "4528", "name": "Las Vegas Wash at LV Blvd", "basin": "Las Vegas Wash"},
    {"id": "4553", "name": "Lower C-1 Channel (USGS)", "basin": "C-1 Channel"},
    {"id": "4618", "name": "Lower Duck Creek DB", "basin": "Duck Creek"},
    {"id": "4623", "name": "Duck Creek at Paradise", "basin": "Duck Creek"},
    {"id": "4633", "name": "Duck Creek at Eastern", "basin": "Duck Creek"},
    {"id": "4708", "name": "Headworks DB", "basin": "Pittman Wash"},
    {"id": "4733", "name": "Pittman Wash at Wigwam", "basin": "Pittman Wash"},
    {"id": "4748", "name": "Pittman Wash at Stephanie", "basin": "Pittman Wash"},
    {"id": "4783", "name": "C-1 Channel at Warm Springs (USGS)", "basin": "C-1 Channel"},
]

# ---------------------------------------------------------------------------
# Clark County endpoints (metadata-only — no JSON API available)
# ---------------------------------------------------------------------------

@router.get("/clark/stations", summary="Clark County CCRFCD station registry")
async def clark_stations(
    network: Optional[str] = Query(None, description="Filter: rain, water_level, weather"),
    basin: Optional[str] = Query(None, description="Filter by drainage basin (case-insensitive substring)"),
) -> dict:
    """Return the curated Clark County CCRFCD station registry.

    The CCRFCD ALERT system has 200+ rain gauges, 150+ water-level stations,
    and 15+ weather stations across the Las Vegas valley and surrounding areas.

    **Note:** This is a metadata-only endpoint.  The CCRFCD system uses ASP.NET
    server-rendered pages with no exposed JSON API.  For real-time readings,
    visit the interactive gauge map or station list pages linked in the response.
    """
    stations: list[dict] = []

    if network is None or network == "rain":
        for s in _CLARK_RAIN_STATIONS:
            stations.append({**s, "network": "rain", "source": _SOURCE_CLARK})
    if network is None or network == "water_level":
        for s in _CLARK_WL_STATIONS:
            stations.append({**s, "network": "water_level", "source": _SOURCE_CLARK})
    if network is None or network == "weather":
        for s in _CLARK_WX_STATIONS:
            stations.append({**s, "network": "weather", "source": _SOURCE_CLARK})

    if basin:
        basin_lower = basin.lower()
        stations = [s for s in stations if basin_lower in s.get("basin", "").lower()]

    return {
        "source": _SOURCE_CLARK,
        "station_count": len(stations),
        "note": (
            "Metadata-only: the CCRFCD ALERT system does not expose a JSON API. "
            "Use the interactive map or station list pages for real-time readings."
        ),
        "data_urls": {
            "interactive_map": _CLARK_GAGE_MAP,
            "static_map": _CLARK_STATIC_MAP,
            "station_list": _CLARK_STATION_LIST,
            "gage_data_list": _CLARK_GAGE_DATA_LIST,
        },
        "stations": stations,
    }


# ===========================================================================
#
#  Combined overview
#
# ===========================================================================

@router.get("/overview", summary="Southwest County Flood Control overview")
async def sw_flood_overview() -> dict:
    """Summary of both flood control district ALERT systems and available endpoints."""
    return {
        "source": "sw_flood",
        "name": SOURCE_NAME,
        "description": SOURCE_DESCRIPTION,
        "districts": [
            {
                "id": _SOURCE_CLARK,
                "name": "Clark County Regional Flood Control District (CCRFCD)",
                "state": "Nevada",
                "coverage": "Clark County, NV — Las Vegas valley, Lake Mead NRA, Moapa",
                "auth_required": False,
                "update_frequency": "5 min",
                "data_access": "metadata-only (ASP.NET server-rendered, no JSON API)",
                "networks": [
                    {
                        "id": "rain",
                        "name": "Rain Gauges",
                        "station_count_approx": 200,
                        "params": ["precipitation"],
                        "endpoint": "/api/sw_flood/clark/stations?network=rain",
                    },
                    {
                        "id": "water_level",
                        "name": "Water Level Stations",
                        "station_count_approx": 150,
                        "params": ["stage"],
                        "endpoint": "/api/sw_flood/clark/stations?network=water_level",
                    },
                    {
                        "id": "weather",
                        "name": "Weather Stations",
                        "station_count_approx": 15,
                        "params": [
                            "temperature", "humidity", "dewpoint",
                            "wind_speed", "wind_direction", "peak_wind",
                        ],
                        "endpoint": "/api/sw_flood/clark/stations?network=weather",
                    },
                ],
                "data_urls": {
                    "interactive_map": _CLARK_GAGE_MAP,
                    "static_map": _CLARK_STATIC_MAP,
                    "station_list": _CLARK_STATION_LIST,
                    "gage_data_list": _CLARK_GAGE_DATA_LIST,
                },
            },
            {
                "id": _SOURCE_PIMA,
                "name": "Pima County Regional Flood Control District (RFCD)",
                "state": "Arizona",
                "coverage": "Pima County, AZ — Tucson metro, Rillito, Santa Cruz, Canada Del Oro",
                "auth_required": False,
                "update_frequency": "5–15 min",
                "data_access": "HTML table scraping (PHP pages → structured JSON)",
                "networks": [
                    {
                        "id": "rain",
                        "name": "ALERT Rain Gauges",
                        "description": "Tipping-bucket precipitation gauges across 12 regions",
                        "station_count_approx": 80,
                        "params": [
                            "precip_15min", "precip_30min", "precip_1hr",
                            "precip_3hr", "precip_6hr", "precip_12hr", "precip_24hr",
                        ],
                        "data_source": "HTML scraper (pgi.php)",
                        "endpoint": "/api/sw_flood/pima/rain",
                    },
                    {
                        "id": "stream",
                        "name": "ALERT Stream Gauges",
                        "description": "Stage and discharge sensors on washes and rivers",
                        "station_count_approx": 60,
                        "params": ["stage", "discharge"],
                        "data_source": "HTML scraper (sgi.php)",
                        "endpoint": "/api/sw_flood/pima/stream",
                    },
                ],
                "data_urls": {
                    "interactive_map": _PIMA_MAP_URL,
                    "rain_table": _PIMA_RAIN_URL,
                    "stream_table": _PIMA_STREAM_URL,
                    "mobile_map": "https://pima.jefulleralert.com/jefmap/",
                },
            },
        ],
        "endpoints": [
            "/api/sw_flood/pima/rain",
            "/api/sw_flood/pima/stream",
            "/api/sw_flood/clark/stations",
            "/api/sw_flood/overview",
        ],
    }
