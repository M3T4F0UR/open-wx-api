"""Iowa Flood & Water Quality — IFIS bridge sensors + AQuIA monitoring.

Combines two Iowa-specific data APIs:

1. Iowa Flood Information System (IFIS)
   ~300 bridge-mounted stream sensors providing real-time depth and
   elevation data across Iowa's watersheds.
   API: https://ifis.iowafloodcenter.org/ifis/ws/

2. Iowa DNR AQuIA (Ambient Water Quality)
   Water-quality monitoring sites operated by Iowa DNR, providing
   nutrient, bacteria, and chemical sampling data.
   API: https://programs.iowadnr.gov/aquia/api/v1/

Both APIs are free with no authentication required.
"""
import logging
from datetime import datetime, timezone
from typing import Optional

from fastapi import APIRouter, HTTPException, Query

import cache
import http_client
from models import Reading, Station, StationDetail

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Source registry metadata
# ---------------------------------------------------------------------------
SOURCE_NAME = "Iowa Flood & Water Quality"
SOURCE_DESCRIPTION = (
    "Iowa Flood Information System (IFIS) bridge stream sensors (~300 sites) "
    "and Iowa DNR AQuIA ambient water-quality monitoring network."
)
SOURCE_META = {
    "url": "https://ifis.iowafloodcenter.org/ifis/",
    "auth_required": False,
    "update_frequency": "15 min (IFIS) / varies (AQuIA)",
    "coverage": "Iowa",
    "params": ["stream_depth", "stream_elevation", "water_quality"],
    "category": "water",
}

router = APIRouter(prefix="/api/iowa_flood", tags=["Iowa Flood & Water Quality"])

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
IFIS_BASE = "https://ifis.iowafloodcenter.org/ifis/ws"
AQUIA_BASE = "https://programs.iowadnr.gov/aquia/api/v1"


# ---------------------------------------------------------------------------
# Helper: parse IFIS tab-delimited response
# ---------------------------------------------------------------------------

def _parse_ifis_tab(text: str, site_id: str, parameter: str, unit: str) -> list[Reading]:
    """Parse IFIS tab-delimited time-series text into Reading objects.

    Expected format (tab-separated, first line is header):
        datetime\tvalue
        2026-03-20 12:00:00\t3.45
    """
    readings: list[Reading] = []
    lines = text.strip().splitlines()
    for line in lines[1:]:  # skip header
        parts = line.split("\t")
        if len(parts) < 2:
            continue
        dt_str, raw_val = parts[0].strip(), parts[1].strip()
        if not raw_val or raw_val in ("-999", "-999.0", ""):
            continue
        try:
            value = float(raw_val)
        except (TypeError, ValueError):
            continue
        try:
            ts = datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S").replace(
                tzinfo=timezone.utc
            )
        except ValueError:
            # Try alternate format
            try:
                ts = datetime.fromisoformat(dt_str)
            except ValueError:
                continue
        readings.append(
            Reading(
                station_id=site_id,
                timestamp=ts,
                parameter=parameter,
                value=value,
                unit=unit,
                source="iowa_ifis",
            )
        )
    return readings


def _parse_ifis_sites(text: str) -> list[Station]:
    """Parse IFIS sites list (tab-delimited) into Station objects.

    Expected format:
        site_id\tsite_name\tlat\tlon\t...
    The exact column layout may vary; we handle common variations.
    """
    stations: list[Station] = []
    lines = text.strip().splitlines()
    if not lines:
        return stations

    # First line is header — detect column positions
    header = lines[0].lower().split("\t")
    col_map: dict[str, int] = {}
    for i, col in enumerate(header):
        col = col.strip()
        if "id" in col or col == "site":
            col_map.setdefault("id", i)
        elif "name" in col or "description" in col:
            col_map.setdefault("name", i)
        elif "lat" in col:
            col_map.setdefault("lat", i)
        elif "lon" in col or "lng" in col:
            col_map.setdefault("lon", i)

    id_col = col_map.get("id", 0)
    name_col = col_map.get("name", 1)
    lat_col = col_map.get("lat", 2)
    lon_col = col_map.get("lon", 3)

    for line in lines[1:]:
        parts = line.split("\t")
        if len(parts) <= max(id_col, name_col, lat_col, lon_col):
            continue
        site_id = parts[id_col].strip()
        if not site_id:
            continue
        try:
            lat = float(parts[lat_col].strip())
            lon = float(parts[lon_col].strip())
        except (ValueError, IndexError):
            continue
        stations.append(
            Station(
                id=site_id,
                name=parts[name_col].strip() or site_id,
                lat=lat,
                lon=lon,
                source="iowa_ifis",
                network="IFIS",
                params=["stream_depth", "stream_elevation"],
                url=f"https://ifis.iowafloodcenter.org/ifis/app/?site={site_id}",
            )
        )
    return stations


def _parse_ifis_sites_json(data) -> list[Station]:
    """Parse IFIS sites JSON response into Station objects.

    Handles the case where the API returns JSON instead of tab-delimited.
    """
    stations: list[Station] = []
    items = data if isinstance(data, list) else data.get("sites", data.get("data", []))
    for item in items:
        if not isinstance(item, dict):
            continue
        site_id = str(item.get("id", item.get("site_id", item.get("site", "")))).strip()
        if not site_id:
            continue
        try:
            lat = float(item.get("lat", item.get("latitude", 0)))
            lon = float(item.get("lon", item.get("longitude", item.get("lng", 0))))
        except (TypeError, ValueError):
            continue
        name = item.get("name", item.get("site_name", item.get("description", site_id)))
        stations.append(
            Station(
                id=site_id,
                name=str(name),
                lat=lat,
                lon=lon,
                source="iowa_ifis",
                network="IFIS",
                params=["stream_depth", "stream_elevation"],
                url=f"https://ifis.iowafloodcenter.org/ifis/app/?site={site_id}",
            )
        )
    return stations


# ---------------------------------------------------------------------------
# Endpoints — IFIS stream sensors
# ---------------------------------------------------------------------------

@router.get("/sites", summary="List IFIS stream sensor sites")
async def list_ifis_sites():
    """Return all ~300 Iowa Flood Information System bridge sensor sites."""
    cache_key = "iowa_ifis:sites"
    cached = await cache.get(cache_key)
    if cached is not None:
        return cached

    url = f"{IFIS_BASE}/sites.php"
    # IFIS may return tab-delimited text or JSON depending on params
    try:
        data = await http_client.fetch_json(url, source="iowa_ifis")
        stations = _parse_ifis_sites_json(data)
    except Exception:
        # Fall back to text parsing
        try:
            text = await http_client.fetch_text(url, source="iowa_ifis")
            stations = _parse_ifis_sites(text)
        except Exception as exc:
            logger.error("Failed to fetch IFIS sites: %s", exc)
            raise HTTPException(status_code=502, detail=f"IFIS sites unavailable: {exc}")

    result = {
        "source": "iowa_ifis",
        "count": len(stations),
        "stations": [s.model_dump() for s in stations],
    }
    await cache.set(cache_key, result, ttl=600)
    return result


@router.get("/depth/{site_id}", summary="Stream depth time series")
async def get_stream_depth(
    site_id: str,
    hours: int = Query(24, ge=1, le=168, description="Hours of data to retrieve (max 168)"),
):
    """Return stream depth readings for an IFIS sensor site."""
    cache_key = f"iowa_ifis:depth:{site_id}:{hours}"
    cached = await cache.get(cache_key)
    if cached is not None:
        return cached

    url = f"{IFIS_BASE}/sites.php"
    params = {"site": site_id, "period": str(hours), "format": "tab"}
    try:
        text = await http_client.fetch_text(url, params=params, source="iowa_ifis")
    except Exception as exc:
        logger.error("IFIS depth fetch failed for %s: %s", site_id, exc)
        raise HTTPException(status_code=502, detail=f"IFIS depth unavailable: {exc}")

    readings = _parse_ifis_tab(text, site_id, parameter="stream_depth", unit="ft")
    result = {
        "source": "iowa_ifis",
        "site_id": site_id,
        "parameter": "stream_depth",
        "hours": hours,
        "count": len(readings),
        "readings": [r.model_dump() for r in readings],
    }
    await cache.set(cache_key, result, ttl=300)
    return result


@router.get("/elevation/{site_id}", summary="Stream elevation time series")
async def get_stream_elevation(
    site_id: str,
    hours: int = Query(24, ge=1, le=168, description="Hours of data to retrieve (max 168)"),
):
    """Return stream elevation readings for an IFIS sensor site."""
    cache_key = f"iowa_ifis:elev:{site_id}:{hours}"
    cached = await cache.get(cache_key)
    if cached is not None:
        return cached

    url = f"{IFIS_BASE}/elev_sites.php"
    params = {"site": site_id, "period": str(hours), "format": "tab"}
    try:
        text = await http_client.fetch_text(url, params=params, source="iowa_ifis")
    except Exception as exc:
        logger.error("IFIS elevation fetch failed for %s: %s", site_id, exc)
        raise HTTPException(status_code=502, detail=f"IFIS elevation unavailable: {exc}")

    readings = _parse_ifis_tab(text, site_id, parameter="stream_elevation", unit="ft")
    result = {
        "source": "iowa_ifis",
        "site_id": site_id,
        "parameter": "stream_elevation",
        "hours": hours,
        "count": len(readings),
        "readings": [r.model_dump() for r in readings],
    }
    await cache.set(cache_key, result, ttl=300)
    return result


# ---------------------------------------------------------------------------
# Endpoints — AQuIA water quality
# ---------------------------------------------------------------------------

@router.get("/water_quality/sites", summary="List AQuIA water-quality monitoring sites")
async def list_aquia_sites():
    """Return Iowa DNR AQuIA ambient water-quality monitoring sites."""
    cache_key = "iowa_aquia:sites"
    cached = await cache.get(cache_key)
    if cached is not None:
        return cached

    url = f"{AQUIA_BASE}/sites"
    try:
        data = await http_client.fetch_json(url, source="iowa_aquia")
    except Exception as exc:
        logger.error("AQuIA sites fetch failed: %s", exc)
        raise HTTPException(status_code=502, detail=f"AQuIA sites unavailable: {exc}")

    # Normalize into Station objects
    items = data if isinstance(data, list) else data.get("data", data.get("sites", []))
    stations: list[Station] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        site_id = str(
            item.get("id", item.get("siteId", item.get("site_id", "")))
        ).strip()
        if not site_id:
            continue
        try:
            lat = float(item.get("latitude", item.get("lat", 0)))
            lon = float(item.get("longitude", item.get("lon", item.get("lng", 0))))
        except (TypeError, ValueError):
            continue
        name = item.get("name", item.get("siteName", item.get("description", site_id)))
        stations.append(
            Station(
                id=site_id,
                name=str(name),
                lat=lat,
                lon=lon,
                source="iowa_aquia",
                network="AQuIA",
                params=item.get("parameters", []),
                url=f"https://programs.iowadnr.gov/aquia/Site/{site_id}",
                extra={
                    k: v
                    for k, v in item.items()
                    if k not in ("id", "siteId", "site_id", "latitude", "longitude",
                                 "lat", "lon", "lng", "name", "siteName", "description",
                                 "parameters")
                },
            )
        )

    result = {
        "source": "iowa_aquia",
        "count": len(stations),
        "stations": [s.model_dump() for s in stations],
    }
    await cache.set(cache_key, result, ttl=900)
    return result


@router.get("/water_quality/sites/{site_id}", summary="AQuIA site detail")
async def get_aquia_site_detail(site_id: str):
    """Return detailed information for a single AQuIA monitoring site."""
    cache_key = f"iowa_aquia:site:{site_id}"
    cached = await cache.get(cache_key)
    if cached is not None:
        return cached

    url = f"{AQUIA_BASE}/sites/{site_id}"
    try:
        data = await http_client.fetch_json(url, source="iowa_aquia")
    except Exception as exc:
        logger.error("AQuIA site detail failed for %s: %s", site_id, exc)
        raise HTTPException(status_code=502, detail=f"AQuIA site detail unavailable: {exc}")

    if not data:
        raise HTTPException(status_code=404, detail=f"AQuIA site {site_id} not found")

    result = {
        "source": "iowa_aquia",
        "site_id": site_id,
        "data": data,
    }
    await cache.set(cache_key, result, ttl=900)
    return result
