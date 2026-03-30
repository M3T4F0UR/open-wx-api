"""RCC-ACIS (Applied Climate Information System) — daily climate observations.

Provides daily max/min temperature, precipitation, snowfall, snow depth, and
average temperature from ~10,000 COOP/NWS stations via the Regional Climate
Centers' ACIS web services.  No authentication required.

API reference: https://www.rcc-acis.org/docs_webservices.html
"""
import logging
from datetime import datetime, timezone
from typing import Optional

import httpx
from fastapi import APIRouter, HTTPException, Query

import cache
import config
import http_client
from models import Reading, Station, StationDetail

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Source registry metadata
# ---------------------------------------------------------------------------
SOURCE_NAME = "RCC-ACIS"
SOURCE_DESCRIPTION = (
    "RCC Applied Climate Information System — daily temperature, precipitation, "
    "snowfall, and snow depth observations from ~10,000 COOP and NWS stations "
    "across the United States.  Free, no authentication required."
)
SOURCE_META = {
    "url": "https://www.rcc-acis.org/",
    "auth_required": False,
    "update_frequency": "daily",
    "coverage": "United States",
    "params": ["max_temp", "min_temp", "precipitation", "snowfall", "snow_depth", "avg_temp"],
    "category": "climate",
}

router = APIRouter(prefix="/api/acis_v2", tags=["RCC-ACIS"])

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
ACIS_BASE = "https://data.rcc-acis.org/"

# Element short names → human-readable labels and units
ELEM_MAP: dict[str, dict[str, str]] = {
    "maxt":  {"name": "Max Temperature",   "unit": "°F"},
    "mint":  {"name": "Min Temperature",   "unit": "°F"},
    "pcpn":  {"name": "Precipitation",     "unit": "in"},
    "snow":  {"name": "Snowfall",          "unit": "in"},
    "snwd":  {"name": "Snow Depth",        "unit": "in"},
    "obst":  {"name": "Obs Time Temp",     "unit": "°F"},
    "avgt":  {"name": "Avg Temperature",   "unit": "°F"},
}

DEFAULT_ELEMS = "maxt,mint,pcpn"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

async def _acis_post(endpoint: str, body: dict) -> dict:
    """POST JSON to an RCC-ACIS endpoint and return parsed response.

    Uses the shared http_client connection pool with circuit breaker tracking.
    """
    url = f"{ACIS_BASE}{endpoint}"
    key = "rcc_acis"

    await http_client._check_circuit(key)

    client = await http_client.get_client()
    last_exc: Optional[Exception] = None
    backoff = [0.5, 1.0, 2.0]

    import asyncio

    for attempt in range(3):
        try:
            resp = await client.post(
                url,
                json=body,
                headers={"Accept": "application/json"},
                timeout=config.HTTP_TIMEOUT,
            )
            resp.raise_for_status()
            data = resp.json()
            await http_client._record_success(key)
            return data
        except Exception as exc:
            last_exc = exc
            retryable = isinstance(exc, (httpx.TimeoutException, httpx.ConnectError))
            if isinstance(exc, httpx.HTTPStatusError):
                retryable = exc.response.status_code in {502, 503, 504}
            if not retryable or attempt >= 2:
                await http_client._record_failure(key)
                raise
            delay = backoff[attempt]
            logger.warning(
                "[rcc_acis] Retry %d/2 after %s (%.1fs): %s",
                attempt + 1, type(exc).__name__, delay, url,
            )
            await asyncio.sleep(delay)

    await http_client._record_failure(key)
    raise last_exc  # type: ignore[misc]


def _parse_station_meta(meta: dict) -> Station:
    """Convert an ACIS meta block to a Station model."""
    ll = meta.get("ll", [0.0, 0.0])
    lon = float(ll[0]) if len(ll) > 0 else 0.0
    lat = float(ll[1]) if len(ll) > 1 else 0.0

    elev_raw = meta.get("elev")
    try:
        elevation = float(elev_raw) if elev_raw is not None and elev_raw != -999 else None
    except (TypeError, ValueError):
        elevation = None

    # Pick the best station ID from sids (prefer USW/USC COOP IDs)
    sids = meta.get("sids", [])
    station_id = str(meta.get("uid", ""))
    network = ""
    for sid_entry in sids:
        sid_str = sid_entry if isinstance(sid_entry, str) else str(sid_entry)
        parts = sid_str.split(" ")
        sid_code = parts[0]
        # Network codes: 1=WBAN, 2=COOP, 4=FAA, 5=ICAO, 6=GHCN, 7=NWSLI, etc.
        if len(parts) > 1:
            net_code = parts[1]
            if net_code == "2":  # COOP — preferred
                station_id = sid_code
                network = "COOP"
                break
            elif net_code == "1":
                station_id = sid_code
                network = "WBAN"
            elif net_code == "5" and not network:
                station_id = sid_code
                network = "ICAO"
    if not network:
        # Fallback: use uid as ID, first sid's network
        station_id = str(meta.get("uid", sids[0].split(" ")[0] if sids else "unknown"))
        network = "ACIS"

    return Station(
        id=station_id,
        name=meta.get("name", station_id),
        lat=lat,
        lon=lon,
        source="rcc_acis",
        network=network,
        elevation=elevation,
        url=f"https://xmacis.rcc-acis.org/StnMeta?sid={station_id}",
        extra={
            "uid": meta.get("uid"),
            "sids": sids,
        },
    )


def _parse_readings_from_data(
    station_id: str,
    data_rows: list,
    elems: list[str],
    date_field: str = "date",
) -> list[Reading]:
    """Parse ACIS data rows into Reading objects.

    Each row is [date, val1, val2, ...] for StnData or
    a dict with 'date' and element values for MultiStnData.
    """
    readings: list[Reading] = []

    for row in data_rows:
        if isinstance(row, list):
            # StnData format: [date_str, val1, val2, ...]
            if len(row) < 2:
                continue
            date_str = row[0]
            values = row[1:]
        elif isinstance(row, dict):
            date_str = row.get(date_field, "")
            values = [row.get(e, "M") for e in elems]
        else:
            continue

        try:
            dt = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        except (ValueError, TypeError):
            continue

        for i, elem_name in enumerate(elems):
            if i >= len(values):
                break
            raw = values[i]
            # ACIS uses "M" for missing, "T" for trace, "S" for special
            if raw in (None, "M", "S", "", " "):
                continue
            if raw == "T":
                value = 0.0  # trace
            else:
                try:
                    value = float(raw)
                except (TypeError, ValueError):
                    continue

            info = ELEM_MAP.get(elem_name, {"name": elem_name, "unit": "unknown"})
            readings.append(
                Reading(
                    station_id=station_id,
                    timestamp=dt,
                    parameter=info["name"],
                    value=value,
                    unit=info["unit"],
                    quality="T" if raw == "T" else None,
                    source="rcc_acis",
                )
            )

    return readings


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@router.get("/stations", summary="List RCC-ACIS climate stations")
async def list_stations(
    state: Optional[str] = Query(
        None,
        description="Two-letter US state code (e.g. 'OR', 'CA')",
        min_length=2,
        max_length=2,
    ),
    bbox: Optional[str] = Query(
        None,
        description="Bounding box as west,south,east,north (decimal degrees)",
    ),
    elems: str = Query(
        DEFAULT_ELEMS,
        description="Comma-separated element names (maxt,mint,pcpn,snow,snwd,obst,avgt)",
    ),
    limit: int = Query(500, ge=1, le=10000, description="Maximum stations to return"),
):
    """List climate stations from the RCC-ACIS station metadata service.

    Filter by state abbreviation OR bounding box.  At least one spatial
    filter is required to avoid excessively large responses.
    """
    if not state and not bbox:
        raise HTTPException(
            status_code=400,
            detail="Provide at least one of: state (2-letter code) or bbox (west,south,east,north)",
        )

    cache_key = f"rcc_acis:stations:{state}:{bbox}:{elems}:{limit}"
    cached = await cache.get(cache_key)
    if cached is not None:
        return cached

    body: dict = {
        "meta": "name,ll,elev,uid,sids",
        "elems": elems,
    }
    if state:
        body["state"] = state.upper()
    elif bbox:
        parts = [p.strip() for p in bbox.split(",")]
        if len(parts) != 4:
            raise HTTPException(status_code=400, detail="bbox must be west,south,east,north")
        body["bbox"] = ",".join(parts)

    try:
        data = await _acis_post("StnMeta", body)
    except http_client.CircuitOpenError as exc:
        raise HTTPException(status_code=503, detail=str(exc))
    except httpx.TimeoutException:
        raise HTTPException(status_code=504, detail="RCC-ACIS API timeout")
    except httpx.HTTPStatusError as exc:
        raise HTTPException(status_code=exc.response.status_code, detail=f"RCC-ACIS API error: {exc}")
    except Exception as exc:
        logger.exception("Unexpected error fetching RCC-ACIS station list")
        raise HTTPException(status_code=500, detail=str(exc))

    meta_list = data.get("meta", [])
    stations = [_parse_station_meta(m) for m in meta_list[:limit]]

    result = {"stations": [s.model_dump() for s in stations], "count": len(stations)}
    await cache.set(cache_key, result, ttl=config.CACHE_TTL_LONG)
    return result


@router.get("/current", summary="Current-day observations for all stations in a state/bbox")
async def current_observations(
    state: Optional[str] = Query(
        None,
        description="Two-letter US state code (e.g. 'OR')",
        min_length=2,
        max_length=2,
    ),
    bbox: Optional[str] = Query(
        None,
        description="Bounding box as west,south,east,north",
    ),
    date: Optional[str] = Query(
        None,
        description="Date in YYYY-MM-DD format (default: today)",
    ),
    elems: str = Query(
        DEFAULT_ELEMS,
        description="Comma-separated element names",
    ),
    limit: int = Query(500, ge=1, le=10000, description="Maximum stations to return"),
):
    """Fetch the latest daily observations for all stations in a state or bbox.

    Uses the ACIS MultiStnData endpoint.  Returns station metadata alongside
    the daily observation values for the requested date.
    """
    if not state and not bbox:
        raise HTTPException(
            status_code=400,
            detail="Provide at least one of: state or bbox",
        )

    target_date = date or datetime.now(timezone.utc).strftime("%Y-%m-%d")
    cache_key = f"rcc_acis:current:{state}:{bbox}:{target_date}:{elems}:{limit}"
    cached = await cache.get(cache_key)
    if cached is not None:
        return cached

    body: dict = {
        "date": target_date,
        "elems": elems,
        "meta": "name,ll,elev",
    }
    if state:
        body["state"] = state.upper()
    elif bbox:
        parts = [p.strip() for p in bbox.split(",")]
        if len(parts) != 4:
            raise HTTPException(status_code=400, detail="bbox must be west,south,east,north")
        body["bbox"] = ",".join(parts)

    try:
        data = await _acis_post("MultiStnData", body)
    except http_client.CircuitOpenError as exc:
        raise HTTPException(status_code=503, detail=str(exc))
    except httpx.TimeoutException:
        raise HTTPException(status_code=504, detail="RCC-ACIS API timeout")
    except httpx.HTTPStatusError as exc:
        raise HTTPException(status_code=exc.response.status_code, detail=f"RCC-ACIS API error: {exc}")
    except Exception as exc:
        logger.exception("Error fetching RCC-ACIS multi-station data")
        raise HTTPException(status_code=500, detail=str(exc))

    elem_list = [e.strip() for e in elems.split(",")]
    results = []

    for entry in data.get("data", [])[:limit]:
        meta = entry.get("meta", {})
        station = _parse_station_meta(meta)

        # MultiStnData returns values as a flat list matching elem order
        raw_data = entry.get("data", [])
        obs: dict = {"station": station.model_dump(), "date": target_date, "values": {}}

        for i, elem_name in enumerate(elem_list):
            if i >= len(raw_data):
                break
            raw = raw_data[i]
            info = ELEM_MAP.get(elem_name, {"name": elem_name, "unit": "unknown"})
            if raw in (None, "M", "S", "", " "):
                obs["values"][info["name"]] = {"value": None, "unit": info["unit"]}
            elif raw == "T":
                obs["values"][info["name"]] = {"value": 0.0, "unit": info["unit"], "quality": "trace"}
            else:
                try:
                    obs["values"][info["name"]] = {"value": float(raw), "unit": info["unit"]}
                except (TypeError, ValueError):
                    obs["values"][info["name"]] = {"value": None, "unit": info["unit"]}

        results.append(obs)

    result = {"date": target_date, "count": len(results), "data": results}
    await cache.set(cache_key, result, ttl=config.CACHE_TTL)
    return result


@router.get("/station/{sid}/data", summary="Time-series data for a single ACIS station")
async def station_data(
    sid: str,
    start: str = Query(..., description="Start date YYYY-MM-DD"),
    end: str = Query(..., description="End date YYYY-MM-DD"),
    elems: str = Query(
        DEFAULT_ELEMS,
        description="Comma-separated element names",
    ),
):
    """Fetch daily time-series data for a single station.

    Uses the ACIS StnData endpoint.  The ``sid`` can be a COOP ID, WBAN,
    ICAO, or ACIS UID.
    """
    # Validate dates
    try:
        datetime.strptime(start, "%Y-%m-%d")
        datetime.strptime(end, "%Y-%m-%d")
    except ValueError:
        raise HTTPException(status_code=400, detail="Dates must be in YYYY-MM-DD format")

    cache_key = f"rcc_acis:stndata:{sid}:{start}:{end}:{elems}"
    cached = await cache.get(cache_key)
    if cached is not None:
        return cached

    body: dict = {
        "sid": sid,
        "sdate": start,
        "edate": end,
        "elems": elems,
        "meta": "name,ll,elev,uid,sids",
    }

    try:
        data = await _acis_post("StnData", body)
    except http_client.CircuitOpenError as exc:
        raise HTTPException(status_code=503, detail=str(exc))
    except httpx.TimeoutException:
        raise HTTPException(status_code=504, detail="RCC-ACIS API timeout")
    except httpx.HTTPStatusError as exc:
        raise HTTPException(status_code=exc.response.status_code, detail=f"RCC-ACIS API error: {exc}")
    except Exception as exc:
        logger.exception("Error fetching RCC-ACIS station data for %s", sid)
        raise HTTPException(status_code=500, detail=str(exc))

    meta = data.get("meta", {})
    station = _parse_station_meta(meta)

    elem_list = [e.strip() for e in elems.split(",")]
    raw_rows = data.get("data", [])
    readings = _parse_readings_from_data(station.id, raw_rows, elem_list)
    readings.sort(key=lambda r: r.timestamp)

    result = {
        "station": station.model_dump(),
        "start": start,
        "end": end,
        "elems": elems,
        "count": len(readings),
        "readings": [r.model_dump() for r in readings],
    }
    await cache.set(cache_key, result, ttl=config.CACHE_TTL)
    return result


@router.get("/overview", summary="RCC-ACIS source information")
async def overview():
    """Return metadata about the RCC-ACIS data source."""
    return {
        "source": "rcc_acis",
        "name": SOURCE_NAME,
        "description": SOURCE_DESCRIPTION,
        **SOURCE_META,
        "endpoints": {
            "stations": "/api/acis_v2/stations?state=OR",
            "current": "/api/acis_v2/current?state=OR&date=2026-03-19",
            "station_data": "/api/acis_v2/station/{sid}/data?start=2026-03-01&end=2026-03-19",
            "overview": "/api/acis_v2/overview",
        },
        "elements": {k: v for k, v in ELEM_MAP.items()},
        "notes": [
            "All data is daily resolution (not sub-hourly).",
            "Missing values appear as 'M', trace precipitation as 'T' (mapped to 0.0).",
            "Station IDs can be COOP, WBAN, ICAO, or ACIS UID.",
            "No authentication required.",
        ],
    }
