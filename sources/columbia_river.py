"""Columbia River DART — Data Access in Real Time.

River environment data for the Columbia and Snake River systems, including
outflow, spill, elevation, dissolved gas, and water temperature at major dams.
Data available since 1949 via the Columbia Basin Research group at the
University of Washington.

Also queries the USACE Northwestern Division Dataquery for supplemental
real-time conditions.

Sources:
  - DART: https://www.cbr.washington.edu/dart
  - USACE NWD: https://www.nwd-wc.usace.army.mil/dd/common/dataquery/www/

No authentication required.  DART returns plain-text / CSV responses.
"""

import logging
from datetime import datetime, timezone
from typing import Optional

import httpx
from fastapi import APIRouter, HTTPException, Query

import cache
import config
from models import Reading, Station

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Source registry metadata  (read by server.py /api/sources)
# ---------------------------------------------------------------------------

SOURCE_NAME = "Columbia River DART"
SOURCE_DESCRIPTION = (
    "Columbia Basin Research DART (Data Access in Real Time) — outflow, spill, "
    "elevation, dissolved gas, and water temperature at Columbia and Snake River "
    "dams.  Data available since 1949.  No authentication required."
)
SOURCE_META = {
    "url": "https://www.cbr.washington.edu/dart",
    "auth_required": False,
    "update_frequency": "hourly",
    "coverage": "Columbia and Snake River basins (Pacific Northwest US)",
    "params": ["outflow", "spill", "elevation", "dissolved_gas", "temperature"],
    "category": "water",
}

router = APIRouter(prefix="/api/columbia_river", tags=["Columbia River DART"])

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

DART_BASE = "https://www.cbr.washington.edu/dart/query/river_graph_text"
USACE_NWD_BASE = "https://www.nwd-wc.usace.army.mil/dd/common/dataquery/www/"

_HEADERS = {
    "User-Agent": config.USER_AGENT,
    "Accept": "text/plain, text/csv, */*",
}

# Columbia and Snake River dams tracked by DART.
# Keys are the standard DART 3-letter abbreviations.
DAMS: dict[str, dict] = {
    "BON": {
        "name": "Bonneville",
        "river": "Columbia",
        "lat": 45.6444,
        "lon": -121.9406,
        "river_mile": 146.1,
    },
    "TDA": {
        "name": "The Dalles",
        "river": "Columbia",
        "lat": 45.6131,
        "lon": -121.1347,
        "river_mile": 191.5,
    },
    "JDA": {
        "name": "John Day",
        "river": "Columbia",
        "lat": 45.7156,
        "lon": -120.6936,
        "river_mile": 215.6,
    },
    "MCN": {
        "name": "McNary",
        "river": "Columbia",
        "lat": 45.9358,
        "lon": -119.2981,
        "river_mile": 292.0,
    },
    "PRD": {
        "name": "Priest Rapids",
        "river": "Columbia",
        "lat": 46.6431,
        "lon": -119.9128,
        "river_mile": 397.1,
    },
    "WAN": {
        "name": "Wanapum",
        "river": "Columbia",
        "lat": 46.8733,
        "lon": -119.9625,
        "river_mile": 415.8,
    },
    "RIS": {
        "name": "Rock Island",
        "river": "Columbia",
        "lat": 47.3461,
        "lon": -120.0886,
        "river_mile": 453.4,
    },
    "RRH": {
        "name": "Rocky Reach",
        "river": "Columbia",
        "lat": 47.4975,
        "lon": -120.2978,
        "river_mile": 473.7,
    },
    "WEL": {
        "name": "Wells",
        "river": "Columbia",
        "lat": 47.9497,
        "lon": -119.8706,
        "river_mile": 515.6,
    },
    "CHJ": {
        "name": "Chief Joseph",
        "river": "Columbia",
        "lat": 47.9953,
        "lon": -119.6350,
        "river_mile": 545.1,
    },
    "GCL": {
        "name": "Grand Coulee",
        "river": "Columbia",
        "lat": 47.9553,
        "lon": -118.9814,
        "river_mile": 596.6,
    },
    "IHR": {
        "name": "Ice Harbor",
        "river": "Snake",
        "lat": 46.2503,
        "lon": -118.8781,
        "river_mile": 9.7,
    },
    "LMN": {
        "name": "Lower Monumental",
        "river": "Snake",
        "lat": 46.5614,
        "lon": -118.5375,
        "river_mile": 41.6,
    },
    "LGS": {
        "name": "Little Goose",
        "river": "Snake",
        "lat": 46.5858,
        "lon": -118.0375,
        "river_mile": 70.3,
    },
    "LWG": {
        "name": "Lower Granite",
        "river": "Snake",
        "lat": 46.6608,
        "lon": -117.4283,
        "river_mile": 107.5,
    },
    "DWR": {
        "name": "Dworshak",
        "river": "Clearwater",
        "lat": 46.5131,
        "lon": -116.2983,
        "river_mile": 1.9,
    },
}

# DART query parameter names for the river_graph_text form.
DART_PARAMS: dict[str, str] = {
    "outflow": "Outflow (kcfs)",
    "spill": "Spill (kcfs)",
    "elevation": "Forebay Elev. (ft)",
    "dissolved_gas": "Dissolved Gas (% sat)",
    "temperature": "Temperature (°C)",
}

DART_UNITS: dict[str, str] = {
    "outflow": "kcfs",
    "spill": "kcfs",
    "elevation": "ft",
    "dissolved_gas": "% sat",
    "temperature": "°C",
}


# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------

def _client() -> httpx.AsyncClient:
    return httpx.AsyncClient(
        headers=_HEADERS,
        timeout=config.HTTP_TIMEOUT,
        follow_redirects=True,
    )


def _safe_float(val: str, default: float | None = None) -> float | None:
    """Convert a string to float, returning *default* on failure."""
    if val is None:
        return default
    val = val.strip()
    if not val or val == "--" or val == "NA":
        return default
    try:
        return float(val)
    except (TypeError, ValueError):
        return default


async def _dart_fetch(dam: str, param: str) -> list[dict]:
    """Fetch data from DART river_graph_text for a single dam and parameter.

    DART's form-based API expects POST-style query parameters.  The response
    is plain text with whitespace-separated columns (date, value, ...).
    Returns a list of {datetime, value} dicts for the most recent data.
    """
    params = {
        "proj": dam.upper(),
        "param": param,
        "year": str(datetime.now(timezone.utc).year),
        "startday": "1",
        "endday": "365",
    }

    try:
        async with _client() as c:
            resp = await c.get(DART_BASE, params=params)
            resp.raise_for_status()
            return _parse_dart_text(resp.text, param)
    except httpx.TimeoutException:
        logger.warning("DART timeout for dam=%s param=%s", dam, param)
        return []
    except httpx.HTTPStatusError as exc:
        logger.warning(
            "DART HTTP %s for dam=%s param=%s",
            exc.response.status_code, dam, param,
        )
        return []
    except Exception:
        logger.exception("DART error for dam=%s param=%s", dam, param)
        return []


def _parse_dart_text(text: str, param: str) -> list[dict]:
    """Parse DART plain-text response into a list of observation dicts.

    DART text output has header/comment lines starting with '#' or containing
    column labels, followed by whitespace-delimited data rows.
    Expected columns: date (MM/DD/YYYY or YYYY-MM-DD), value.
    """
    records: list[dict] = []
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("#") or line.startswith("Date"):
            continue
        parts = line.split()
        if len(parts) < 2:
            continue

        date_str = parts[0]
        val = _safe_float(parts[1])
        if val is None:
            continue

        # Try parsing date
        dt = None
        for fmt in ("%m/%d/%Y", "%Y-%m-%d"):
            try:
                dt = datetime.strptime(date_str, fmt).replace(tzinfo=timezone.utc)
                break
            except ValueError:
                continue

        if dt is None:
            continue

        records.append({
            "datetime": dt.isoformat(),
            "value": val,
            "unit": DART_UNITS.get(param, ""),
            "param": param,
        })

    return records


def _dam_to_station(code: str) -> dict:
    """Convert a dam entry to a Station-compatible dict."""
    info = DAMS[code]
    return Station(
        id=code,
        name=info["name"],
        lat=info["lat"],
        lon=info["lon"],
        source="columbia_dart",
        network=f"{info['river']} River",
        params=list(DART_PARAMS.keys()),
        elevation=None,
        url=f"https://www.cbr.washington.edu/dart/query/river_graph_text?proj={code}",
        extra={
            "river": info["river"],
            "river_mile": info["river_mile"],
        },
    ).model_dump()


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@router.get("/conditions", summary="Current river conditions at a dam")
async def conditions(
    dam: str = Query(
        ...,
        description="Dam abbreviation code, e.g. 'BON' for Bonneville (see /dams for full list)",
    ),
):
    """Return the latest available river conditions (outflow, spill, elevation,
    dissolved gas, temperature) for a single dam on the Columbia or Snake River.
    """
    dam_upper = dam.upper()
    if dam_upper not in DAMS:
        raise HTTPException(
            status_code=404,
            detail=f"Unknown dam '{dam}'. Use /api/columbia_river/dams for valid codes.",
        )

    cache_key = f"columbia_dart:conditions:{dam_upper}"
    cached = await cache.get(cache_key)
    if cached is not None:
        return cached

    dam_info = DAMS[dam_upper]
    readings: list[dict] = []

    # Fetch each parameter from DART
    for param_key in DART_PARAMS:
        records = await _dart_fetch(dam_upper, param_key)
        if records:
            # Take the most recent record
            latest = records[-1]
            readings.append(Reading(
                station_id=dam_upper,
                timestamp=latest["datetime"],
                param=param_key,
                value=latest["value"],
                unit=latest["unit"],
                source="columbia_dart",
            ).model_dump())

    result = {
        "source": "columbia_dart",
        "dam": dam_upper,
        "name": dam_info["name"],
        "river": dam_info["river"],
        "lat": dam_info["lat"],
        "lon": dam_info["lon"],
        "river_mile": dam_info["river_mile"],
        "readings": readings,
        "reading_count": len(readings),
    }

    await cache.set(cache_key, result, ttl=config.CACHE_TTL)
    return result


@router.get("/dams", summary="List all monitored dams")
async def dams():
    """Return metadata for all Columbia and Snake River dams tracked by DART."""
    cache_key = "columbia_dart:dams"
    cached = await cache.get(cache_key)
    if cached is not None:
        return cached

    dam_list = [_dam_to_station(code) for code in DAMS]

    result = {
        "source": "columbia_dart",
        "count": len(dam_list),
        "dams": dam_list,
    }
    await cache.set(cache_key, result, ttl=config.CACHE_TTL_LONG)
    return result


@router.get("/overview", summary="Columbia River DART — source overview")
async def overview():
    """Return summary information about the Columbia River DART data source."""
    return {
        "module": SOURCE_NAME,
        "description": SOURCE_DESCRIPTION,
        "sources": [
            {
                "id": "columbia_dart",
                "name": "Columbia River DART",
                "description": (
                    "Columbia Basin Research DART — river environment data at "
                    "Columbia and Snake River dams including outflow, spill, "
                    "elevation, dissolved gas, and water temperature.  Data "
                    "available since 1949.  Published by the University of "
                    "Washington School of Aquatic & Fishery Sciences."
                ),
                "url": "https://www.cbr.washington.edu/dart",
                "auth_required": False,
                "update_frequency": "hourly",
                "coverage": "Columbia and Snake River basins (Pacific Northwest US)",
                "dam_count": len(DAMS),
                "params": SOURCE_META["params"],
                "endpoints": [
                    "GET /api/columbia_river/conditions?dam=BON",
                    "GET /api/columbia_river/dams",
                    "GET /api/columbia_river/overview",
                ],
            },
            {
                "id": "usace_nwd",
                "name": "USACE NWD Dataquery",
                "description": (
                    "Supplemental real-time data from the US Army Corps of "
                    "Engineers Northwestern Division Dataquery system."
                ),
                "url": USACE_NWD_BASE,
                "auth_required": False,
            },
        ],
    }
