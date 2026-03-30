"""King County WA Hydrology — stream and rain gauge monitoring.

King County's Hydrologic Information Center provides public access to
rainfall, streamflow, and other hydrologic data collected at ~500+
gauge sites across King County, Washington.  Data includes 15-minute
rainfall and streamflow, daily summaries, water quality, and
multi-depth thermistor readings.

No authentication required.  The backend is ASP.NET with form-based
data access, so this module scrapes the HTML pages.

Main portal:   https://green2.kingcounty.gov/hydrology/
Marine data:   https://green2.kingcounty.gov/marine/
Data download: https://green2.kingcounty.gov/hydrology/Data.aspx
"""
from __future__ import annotations

import logging
import re
from typing import Any, Optional
from datetime import datetime, timezone

from fastapi import APIRouter, HTTPException, Query

import cache
import http_client

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Source registry metadata  (read by server.py /api/sources)
# ---------------------------------------------------------------------------

SOURCE_NAME = "King County WA Hydrology"
SOURCE_DESCRIPTION = (
    "King County (WA) Hydrologic Information Center — ~500+ stream and rain "
    "gauges across King County watersheds.  Provides 15-minute rainfall and "
    "streamflow data, daily summaries, water quality, and marine monitoring.  "
    "No authentication required."
)
SOURCE_META = {
    "url": "https://green2.kingcounty.gov/hydrology/",
    "auth_required": False,
    "update_frequency": "15 min (real-time gauges)",
    "coverage": "King County, Washington",
    "params": [
        "rainfall", "streamflow", "stream_stage", "water_temperature",
        "conductivity", "dissolved_oxygen", "turbidity", "barometric_pressure",
    ],
    "category": "water",
    "station_count": "~500+",
}

# ---------------------------------------------------------------------------
# Router
# ---------------------------------------------------------------------------

router = APIRouter(prefix="/api/king_county", tags=["King County WA Hydrology"])

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_HYDRO_BASE = "https://green2.kingcounty.gov/hydrology"
_MARINE_BASE = "https://green2.kingcounty.gov/marine"
_SOURCE = "king_county"

# ASP.NET pages
_GAUGE_LIST_URL = f"{_HYDRO_BASE}/GaugeListSearch.aspx"
_DATA_DOWNLOAD_URL = f"{_HYDRO_BASE}/DataDownload.aspx"
_GAUGE_DETAIL_URL = f"{_HYDRO_BASE}/GaugeDetail.aspx"

# Water Resource Inventory Areas in King County
_WRIAS = {
    "07": "Snohomish",
    "08": "Cedar-Sammamish",
    "09": "Duwamish-Green",
    "10": "Puyallup-White",
    "15": "Kitsap",
}

# Known parameter types from the data download page
_PARAMETERS = [
    "Air Temperature",
    "Barometric Pressure",
    "Conductivity",
    "Dissolved Oxygen",
    "pH",
    "Precipitation",
    "Rainfall",
    "Relative Humidity",
    "Solar Radiation",
    "Stream Discharge",
    "Stream Stage",
    "Turbidity",
    "Water Level",
    "Water Temperature",
    "Wind Direction",
    "Wind Speed",
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _parse_gauge_table(html: str) -> list[dict]:
    """Parse gauge list from the ASP.NET GridView HTML table.

    The GaugeListSearch page renders an HTML table with columns like:
    Site Code | Site Name | Parameters | WRIA | Status
    """
    gauges: list[dict] = []

    # Find table rows — the GridView uses <tr> tags inside the main table
    # Skip header row, parse data rows
    row_pattern = re.compile(
        r"<tr[^>]*>\s*<td[^>]*>(.*?)</td>\s*<td[^>]*>(.*?)</td>"
        r"(?:\s*<td[^>]*>(.*?)</td>)?(?:\s*<td[^>]*>(.*?)</td>)?",
        re.DOTALL | re.IGNORECASE,
    )

    for match in row_pattern.finditer(html):
        site_code = _strip_html(match.group(1)).strip()
        site_name = _strip_html(match.group(2)).strip()
        params_raw = _strip_html(match.group(3) or "").strip()
        wria = _strip_html(match.group(4) or "").strip()

        if not site_code or site_code.lower() in ("site code", "site", "code"):
            continue  # skip header

        gauge: dict[str, Any] = {
            "id": site_code,
            "name": site_name or site_code,
            "source": _SOURCE,
            "url": f"{_GAUGE_DETAIL_URL}?id={site_code}",
        }
        if params_raw:
            gauge["params"] = [p.strip() for p in params_raw.split(",") if p.strip()]
        if wria:
            gauge["wria"] = wria

        gauges.append(gauge)

    return gauges


def _strip_html(text: str) -> str:
    """Remove HTML tags from a string."""
    return re.sub(r"<[^>]+>", "", text) if text else ""


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@router.get("/gauges", summary="List King County hydrological gauges")
async def list_gauges(
    wria: Optional[str] = Query(
        None,
        description="Filter by WRIA number (07=Snohomish, 08=Cedar-Sammamish, "
                    "09=Duwamish-Green, 10=Puyallup-White, 15=Kitsap)",
    ),
    active_only: bool = Query(True, description="Only return active gauges"),
):
    """List hydrological gauge sites in King County, WA.

    King County operates ~500+ stream and rain gauges across the major
    watersheds.  Data includes precipitation, streamflow, water quality,
    and environmental parameters at 15-minute intervals.

    The gauge list is scraped from King County's GaugeListSearch page.
    Results are cached for 1 hour.
    """
    cache_key = f"king_county:gauges:{wria}:{active_only}"
    cached = await cache.get(cache_key)
    if cached is not None:
        return cached

    # Fetch the gauge list page — we request the "browse all" view
    try:
        html = await http_client.fetch_text(
            _GAUGE_LIST_URL, source=_SOURCE,
        )
    except Exception as exc:
        logger.error("King County gauge list fetch failed: %s", exc)
        raise HTTPException(
            status_code=502,
            detail=f"King County hydrology site unavailable: {exc}",
        )

    gauges = _parse_gauge_table(html)

    # Apply filters
    if wria:
        gauges = [g for g in gauges if g.get("wria", "").startswith(wria)]

    result = {
        "source": _SOURCE,
        "count": len(gauges),
        "wria_filter": wria,
        "wrias": _WRIAS,
        "gauges": gauges,
        "notes": (
            "Gauge list is scraped from the King County GaugeListSearch page.  "
            "If count is 0, the ASP.NET page may require a postback to populate "
            "results.  Use the overview endpoint for direct links to browse "
            "gauges on the King County site."
        ),
    }
    await cache.set(cache_key, result, ttl=3600)
    return result


@router.get("/overview", summary="King County WA Hydrology overview")
async def king_county_overview() -> dict:
    """Summary of King County hydrological monitoring and available endpoints."""
    return {
        "source": _SOURCE,
        "name": SOURCE_NAME,
        "description": SOURCE_DESCRIPTION,
        "operator": "King County Department of Natural Resources and Parks",
        "coverage": "King County, Washington (2,307 sq mi)",
        "auth_required": False,
        "update_frequency": "15-minute (real-time gauges)",
        "data_types": [
            {
                "type": "rainfall",
                "description": "15-minute and daily rainfall totals from tipping-bucket gauges",
                "interval": "15 min / daily",
            },
            {
                "type": "streamflow",
                "description": "Stream discharge and stage at bridge/channel sites",
                "interval": "15 min / daily",
            },
            {
                "type": "water_quality",
                "description": "Conductivity, dissolved oxygen, pH, turbidity, temperature",
                "interval": "15 min (continuous) or discrete sampling",
            },
            {
                "type": "marine",
                "description": "Puget Sound marine water quality and sediment monitoring",
                "url": _MARINE_BASE,
            },
        ],
        "wrias": [
            {"id": k, "name": v} for k, v in _WRIAS.items()
        ],
        "parameters": _PARAMETERS,
        "data_access": {
            "portal": f"{_HYDRO_BASE}/",
            "data_download": _DATA_DOWNLOAD_URL,
            "gauge_list": _GAUGE_LIST_URL,
            "gauge_map": f"{_HYDRO_BASE}/GaugeMap.aspx",
            "gauge_search": f"{_HYDRO_BASE}/GaugeTextSearch.aspx",
            "multi_site_download": f"{_HYDRO_BASE}/MultiSiteDataDownload.aspx",
            "summary_tables": f"{_HYDRO_BASE}/SummaryDataTables.aspx",
            "marine_monitoring": _MARINE_BASE,
        },
        "download_pages": [
            {
                "name": "Multi-Parameter Data",
                "url": f"{_HYDRO_BASE}/DataDownload.aspx",
                "description": "Single-site data with multiple parameters and intervals",
            },
            {
                "name": "Multi-Site Data",
                "url": f"{_HYDRO_BASE}/MultiSiteDataDownload.aspx",
                "description": "Same parameter across multiple sites",
            },
            {
                "name": "Summary Data",
                "url": f"{_HYDRO_BASE}/SummaryDataTables.aspx",
                "description": "Monthly/annual summary statistics",
            },
            {
                "name": "Crest/Stage Data",
                "url": f"{_HYDRO_BASE}/WaterQualityDataDownload.aspx",
                "description": "Flood crest and stage records",
            },
            {
                "name": "Field Measurements",
                "url": f"{_HYDRO_BASE}/FieldObservationReport.aspx",
                "description": "Discrete field observation reports",
            },
            {
                "name": "Multi-Depth Data",
                "url": f"{_HYDRO_BASE}/ThermistorData.aspx",
                "description": "Thermistor depth profile data",
            },
        ],
        "notes": (
            "King County's hydrology data is served through ASP.NET pages with "
            "form-based postback.  There is no public REST/JSON API.  The gauges "
            "endpoint scrapes the gauge list page; for full data downloads, use "
            "the direct links above or the King County portal."
        ),
        "contact": "Brendan Grant, 206-477-4698",
        "endpoints": [
            "/api/king_county/gauges",
            "/api/king_county/overview",
        ],
    }
