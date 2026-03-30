"""Climate & Snowpack Data — ACIS/RCC + NRCS SNOTEL/SCAN.

Integrates two complementary climate data APIs:

1. ACIS (Applied Climate Information System) / RCC-ACIS
   Applied Climate Information System operated by the Regional Climate Centers.
   Provides access to historical and recent climate data from 30,000+ stations
   (COOP, ASOS, AWOS, GHCN) across the entire United States.
   API: https://data.rcc-acis.org/
   No authentication required; POST JSON bodies.

2. NRCS SNOTEL / AWDB (Air-Water Database)
   Natural Resources Conservation Service Snow Telemetry network.
   ~900 SNOTEL stations in western US mountains measuring snow water equivalent,
   snow depth, precipitation, and air temperature.  Also includes ~3,100 SCAN
   (Soil Climate Analysis Network) stations measuring soil moisture and temperature.
   API: https://wcc.sc.egov.usda.gov/reportGenerator/ (CSV reports, no auth)
"""
from __future__ import annotations

import csv
import io
import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

import httpx
from fastapi import APIRouter, HTTPException, Query

import cache
import config
from models import Reading, Station, StationDetail

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Source registry metadata  (read by server.py /api/sources)
# ---------------------------------------------------------------------------

SOURCE_NAME = "Climate & Snowpack (ACIS + SNOTEL)"
SOURCE_DESCRIPTION = (
    "ACIS/RCC climate data (30,000+ stations, temp/precip/wind) and "
    "NRCS SNOTEL/SCAN network (~4,000 western US mountain stations, "
    "snow water equivalent, snow depth, soil moisture)."
)
SOURCE_META = {
    "url": "https://data.rcc-acis.org/",
    "auth_required": False,
    "update_frequency": "Daily (ACIS), Hourly (SNOTEL)",
    "coverage": "United States",
    "params": ["temperature", "precipitation", "snow_water_equivalent", "snow_depth", "soil_moisture"],
    "category": "climate",
}

router = APIRouter(prefix="/api/climate", tags=["Climate & Snowpack"])

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

ACIS_BASE = "https://data.rcc-acis.org"

# NRCS report generator base — produces on-the-fly CSV reports (single-station)
SNOTEL_REPORT_BASE = "https://wcc.sc.egov.usda.gov/reportGenerator/view_csv"

# NRCS AWDB REST API — used for station metadata lists (report generator
# multi-station filter queries are broken as of early 2026)
AWDB_REST_BASE = "https://wcc.sc.egov.usda.gov/awdbRestApi/services/v1"

# ACIS element codes → (human name, unit)
ACIS_ELEMENTS: dict[str, tuple[str, str]] = {
    "1":  ("Max Temperature",   "°F"),
    "2":  ("Min Temperature",   "°F"),
    "4":  ("Precipitation",     "in"),
    "7":  ("Pan Evaporation",   "in"),
    "10": ("Avg Temperature",   "°F"),
    "11": ("Wind Movement",     "mi"),
    "43": ("Max Temperature",   "°F"),   # element 43 = MaxTemp with time-of-obs
}

# Default element string for most ACIS queries (MaxTemp, MinTemp, Precip)
ACIS_DEFAULT_ELEMS = "1,2,4"

# SNOTEL parameter codes available in the report generator CSV
# These are the column names used in the report URL path fragment and header row.
SNOTEL_PARAMS: dict[str, tuple[str, str]] = {
    "WTEQ": ("Snow Water Equivalent", "in"),
    "SNWD": ("Snow Depth",            "in"),
    "PREC": ("Precipitation",         "in"),
    "TOBS": ("Air Temperature",       "°F"),
    "TMAX": ("Max Air Temperature",   "°F"),
    "TMIN": ("Min Air Temperature",   "°F"),
    "SMS":  ("Soil Moisture",         "%"),
    "STO":  ("Soil Temperature",      "°F"),
}

# Default SNOTEL parameters (most common snowpack indicators)
SNOTEL_DEFAULT_PARAMS = "WTEQ::value,SNWD::value,PREC::value,TOBS::value"

_HTTP_HEADERS = {
    "User-Agent": config.USER_AGENT,
    "Accept": "application/json, text/csv, */*",
}


# ---------------------------------------------------------------------------
# Shared HTTP helpers
# ---------------------------------------------------------------------------

def _make_client() -> httpx.AsyncClient:
    """Return a configured async HTTP client."""
    return httpx.AsyncClient(
        headers=_HTTP_HEADERS,
        timeout=config.HTTP_TIMEOUT,
        follow_redirects=True,
    )


# ---------------------------------------------------------------------------
# ACIS data-normalisation helpers
# ---------------------------------------------------------------------------

def _acis_station_to_model(raw: dict) -> Station:
    """Convert an ACIS StnMeta station dict to the shared Station model.

    ACIS returns coordinates in the ``ll`` field as [lon, lat].  Station IDs
    are stored in the ``sids`` array; we use the first entry (often a COOP or
    ASOS identifier) as the canonical ``id``.
    """
    sids: list[str] = raw.get("sids", [])
    # sids items may be "USW00024229 1" (id + space + network_type_code)
    # Strip the trailing type code to get a clean ID.
    canonical_sid = sids[0].split()[0] if sids else raw.get("uid", "")

    name = raw.get("name", canonical_sid)
    ll = raw.get("ll", [0.0, 0.0])
    try:
        lon = float(ll[0])
        lat = float(ll[1])
    except (IndexError, TypeError, ValueError):
        lon, lat = 0.0, 0.0

    try:
        elev = float(raw.get("elev", 0.0)) if raw.get("elev") not in (None, "", "M") else None
    except (TypeError, ValueError):
        elev = None

    state = raw.get("state", "")

    # Determine which parameters this station reports from valid_daterange or
    # the requested elements — keep it generic since ACIS covers all COOP vars.
    params = ["temperature", "precipitation"]
    if raw.get("elems"):
        elem_str = str(raw.get("elems", ""))
        if "7" in elem_str.split(","):
            params.append("evaporation")
        if "11" in elem_str.split(","):
            params.append("wind")

    # Build a list of all SIDs for the extra block (useful for cross-referencing)
    all_sids = [s.split()[0] for s in sids]

    return Station(
        id=canonical_sid,
        name=name,
        lat=lat,
        lon=lon,
        source="climate",
        network="ACIS",
        params=params,
        elevation=elev,
        url=f"https://www.rcc-acis.org/stationmap.html?state={state}",
        extra={
            "state": state,
            "sids": all_sids,
            "uid": raw.get("uid", ""),
        },
    )


def _acis_parse_stndata(data: dict, sid: str) -> list[Reading]:
    """Parse ACIS StnData response into Reading objects.

    ACIS StnData returns::

        {
          "meta": {...},
          "data": [
            ["2024-01-01", "42", "28", "0.12"],  // date + one value per elem
            ...
          ],
          "smry": [...]   // optional summary rows
        }

    The element order in each row mirrors the ``elems`` field sent in the
    request body.  We receive the elem codes as a comma-separated string and
    map each position to its human name and unit.
    """
    readings: list[Reading] = []
    meta = data.get("meta", {})
    elems_raw: Any = meta.get("elems", data.get("elems", ACIS_DEFAULT_ELEMS))

    # Normalise the element list — may be a str "1,2,4" or a list of dicts
    if isinstance(elems_raw, str):
        elem_codes = [e.strip() for e in elems_raw.split(",") if e.strip()]
    elif isinstance(elems_raw, list):
        elem_codes = []
        for e in elems_raw:
            if isinstance(e, dict):
                elem_codes.append(str(e.get("name", e.get("id", ""))))
            else:
                elem_codes.append(str(e))
    else:
        elem_codes = []

    for row in data.get("data", []):
        if not row:
            continue
        date_str = row[0] if row else ""
        try:
            ts = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        except (ValueError, TypeError):
            continue

        for i, raw_val in enumerate(row[1:], start=0):
            if i >= len(elem_codes):
                break
            code = elem_codes[i]
            human_name, unit = ACIS_ELEMENTS.get(code, (f"Element {code}", ""))

            # ACIS uses "M" for missing, "T" for trace, "S" for suspect
            if raw_val in (None, "M", "", "S"):
                continue
            quality: Optional[str] = None
            if raw_val == "T":
                # Trace precipitation — store as 0.001 with quality flag
                numeric = 0.001
                quality = "trace"
            else:
                try:
                    numeric = float(str(raw_val).rstrip("ABCS"))   # strip flag chars
                    # Detect appended quality flags: value may be "12.3A"
                    flag_chars = "".join(c for c in str(raw_val) if c.isalpha())
                    if flag_chars:
                        quality = flag_chars
                except (TypeError, ValueError):
                    continue

            readings.append(
                Reading(
                    station_id=sid,
                    timestamp=ts,
                    parameter=human_name,
                    value=numeric,
                    unit=unit,
                    quality=quality,
                    source="climate",
                )
            )

    return readings


def _acis_parse_multistn(data: dict) -> list[dict]:
    """Parse ACIS MultiStnData response into a list of station+readings dicts.

    MultiStnData returns::

        {
          "data": [
            {
              "meta": {"name": "...", "sids": [...], "ll": [lon, lat], ...},
              "data": [["date", val1, val2, ...]]
            },
            ...
          ]
        }
    """
    results = []
    elems_meta = data.get("params", {}).get("elems", ACIS_DEFAULT_ELEMS)

    for entry in data.get("data", []):
        meta_block = entry.get("meta", {})
        # Inject the elems context into the meta block so _acis_parse_stndata
        # can decode column positions.
        synthetic_data = {
            "meta": {**meta_block, "elems": elems_meta},
            "data": entry.get("data", []),
        }

        sids = meta_block.get("sids", [])
        sid = sids[0].split()[0] if sids else meta_block.get("uid", "unknown")

        station = _acis_station_to_model(meta_block)
        readings = _acis_parse_stndata(synthetic_data, sid)

        # Set last_update from the most recent reading timestamp
        if readings:
            station.last_update = max(r.timestamp for r in readings)

        results.append(
            {
                "station": station.model_dump(),
                "readings": [r.model_dump() for r in readings],
            }
        )

    return results


# ---------------------------------------------------------------------------
# SNOTEL data-normalisation helpers
# ---------------------------------------------------------------------------

def _build_snotel_station_url(state: str, network: str) -> str:
    """Build the reportGenerator URL to list all stations for a state/network."""
    # The report generator accepts a filter expression in the URL path.
    # Format: customMultipleStationReport/daily/state="OR" AND network="SNTL"/-1,0/...
    # We URL-encode the double quotes as %22.
    state_upper = state.upper()
    net_upper = network.upper()
    filter_expr = f'state=%22{state_upper}%22%20AND%20network=%22{net_upper}%22'
    cols = "name,stationId,state,networkCode,latitude,longitude,elevation,WTEQ::value,SNWD::value,PREC::value,TOBS::value"
    return f"{SNOTEL_REPORT_BASE}/customMultipleStationReport/daily/{filter_expr}/-1,0/{cols}"


def _build_snotel_data_url(triplet: str, days: int, params: str) -> str:
    """Build the reportGenerator URL for a single SNOTEL station time series.

    ``triplet`` is in the form ``302:OR:SNTL`` (stationId:state:network).
    The date range fragment ``-{days},0`` means "from N days ago through today".
    """
    # Encode the triplet pipe-separated per the report generator convention.
    encoded_triplet = f"{triplet}|id=%22%22|name"
    return (
        f"{SNOTEL_REPORT_BASE}/customSingleStationReport/daily/"
        f"{encoded_triplet}/-{days},0/{params}"
    )


def _build_snotel_all_url(network: str = "SNTL") -> str:
    """Build the reportGenerator URL to fetch all SNOTEL or SCAN stations (US-wide)."""
    net_upper = network.upper()
    filter_expr = f'network=%22{net_upper}%22'
    cols = "name,stationId,state,networkCode,latitude,longitude,elevation"
    return f"{SNOTEL_REPORT_BASE}/customMultipleStationReport/daily/{filter_expr}/-1,0/{cols}"


def _parse_snotel_csv_stations(text: str, network: str) -> list[Station]:
    """Parse a NRCS reportGenerator CSV response listing multiple stations.

    The report generator prefixes several comment lines (starting with ``#``)
    before the header row.  We skip those and hand the rest to the csv module.

    Expected columns (may vary slightly):
        Station Name, Station Id, State Code, Network Code,
        Latitude, Longitude, Elevation (ft)
    Optionally followed by data columns (WTEQ Value, SNWD Value, …).
    """
    stations: list[Station] = []
    if not text or not text.strip():
        return stations

    # Strip comment/metadata lines produced by the report generator
    lines = [line for line in text.splitlines() if not line.startswith("#")]
    if not lines:
        return stations

    reader = csv.DictReader(io.StringIO("\n".join(lines)))
    for row in reader:
        try:
            # The report generator uses friendly column names; try several variants
            name = (
                row.get("Station Name")
                or row.get("name")
                or row.get("Site Name")
                or ""
            ).strip()

            raw_sid = (
                row.get("Station Id")
                or row.get("stationId")
                or row.get("Site Id")
                or ""
            ).strip()
            if not raw_sid:
                continue

            state = (
                row.get("State Code")
                or row.get("state")
                or row.get("State")
                or ""
            ).strip().upper()

            net_code = (
                row.get("Network Code")
                or row.get("networkCode")
                or row.get("Network")
                or network.upper()
            ).strip().upper()

            # Triplet: stationId:state:network — canonical SNOTEL station key
            triplet = f"{raw_sid}:{state}:{net_code}"

            try:
                lat = float(row.get("Latitude") or row.get("latitude") or 0.0)
            except (TypeError, ValueError):
                lat = 0.0
            try:
                lon = float(row.get("Longitude") or row.get("longitude") or 0.0)
            except (TypeError, ValueError):
                lon = 0.0
            try:
                elev_raw = row.get("Elevation (ft)") or row.get("elevation") or row.get("Elevation") or None
                elev = float(elev_raw) if elev_raw not in (None, "", "N/A") else None
            except (TypeError, ValueError):
                elev = None

            # Determine parameter coverage
            params: list[str] = []
            if "WTEQ" in ",".join(row.keys()):
                params.append("snow_water_equivalent")
            if "SNWD" in ",".join(row.keys()):
                params.append("snow_depth")
            if "PREC" in ",".join(row.keys()):
                params.append("precipitation")
            if "TOBS" in ",".join(row.keys()):
                params.append("temperature")
            if net_code == "SCAN":
                params += ["soil_moisture", "soil_temperature"]
            if not params:
                params = (
                    ["snow_water_equivalent", "snow_depth", "precipitation", "temperature"]
                    if net_code == "SNTL"
                    else ["soil_moisture", "soil_temperature", "precipitation", "temperature"]
                )

            src = "snotel" if net_code == "SNTL" else ("scan" if net_code == "SCAN" else "climate")
            stations.append(
                Station(
                    id=triplet,
                    name=name or triplet,
                    lat=lat,
                    lon=lon,
                    source=src,
                    network=net_code,
                    params=params,
                    elevation=elev,
                    url=(
                        f"https://wcc.sc.egov.usda.gov/nwcc/site?sitenum={raw_sid}"
                    ),
                    extra={
                        "triplet": triplet,
                        "station_id": raw_sid,
                        "state": state,
                        "network": net_code,
                    },
                )
            )
        except Exception:
            # Skip malformed rows rather than aborting the entire list
            continue

    return stations


def _parse_snotel_csv_data(text: str, triplet: str) -> list[Reading]:
    """Parse a NRCS reportGenerator single-station CSV response into Readings.

    The NRCS report generator can emit two distinct header styles depending on
    how the URL is constructed:

    1. Code-in-header (when the URL uses e.g. ``WTEQ::value``):
         ``WTEQ Value (in)``, ``SNWD Value (in)``, …

    2. Verbose description (when the URL omits the code prefix):
         ``Snow Water Equivalent (in) Start of Day Values``,
         ``Snow Depth (in) Start of Day Values``, …

    Both formats are handled.  The first match wins — we first check whether
    the uppercased column name contains the SNOTEL parameter code directly
    (e.g. ``WTEQ``), and if not, we fall back to matching against a table of
    verbose English keywords.

    Dates are in ``YYYY-MM-DD`` format.  Missing values are represented by
    empty strings or ``-99.9`` (NRCS sentinel).
    """
    # Fallback: verbose English phrases → (code, unit) used by the report generator
    # when the URL does not embed the parameter code in the column header.
    _VERBOSE_KEYWORDS: dict[str, tuple[str, str]] = {
        "SNOW WATER EQUIVALENT": ("Snow Water Equivalent", "in"),
        "SNOW DEPTH":            ("Snow Depth",            "in"),
        "PRECIPITATION ACCUM":   ("Precipitation",         "in"),
        "AIR TEMPERATURE OBS":   ("Air Temperature",       "°F"),
        "AIR TEMPERATURE MAX":   ("Max Air Temperature",   "°F"),
        "AIR TEMPERATURE MIN":   ("Min Air Temperature",   "°F"),
        "SOIL MOISTURE":         ("Soil Moisture",         "%"),
        "SOIL TEMPERATURE":      ("Soil Temperature",      "°F"),
    }

    readings: list[Reading] = []
    if not text or not text.strip():
        return readings

    lines = [line for line in text.splitlines() if not line.startswith("#")]
    if not lines:
        return readings

    reader = csv.DictReader(io.StringIO("\n".join(lines)))

    # Build a mapping from header column name → (parameter, unit).
    # Strategy:
    #   1. Check whether a SNOTEL_PARAMS code (e.g. "WTEQ") appears literally
    #      in the uppercased column name.
    #   2. If not, scan the verbose keyword table for a matching substring.
    header_map: dict[str, tuple[str, str]] = {}
    if reader.fieldnames:
        for col in reader.fieldnames:
            col_upper = col.upper()
            matched = False
            # Pass 1 — code-in-header (e.g. "WTEQ Value (in)")
            for code, (param, unit) in SNOTEL_PARAMS.items():
                if code in col_upper:
                    header_map[col] = (param, unit)
                    matched = True
                    break
            if not matched:
                # Pass 2 — verbose English description
                for keyword, (param, unit) in _VERBOSE_KEYWORDS.items():
                    if keyword in col_upper:
                        header_map[col] = (param, unit)
                        break

    for row in reader:
        date_str = (row.get("Date") or row.get("date") or "").strip()
        if not date_str:
            continue
        try:
            ts = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        except ValueError:
            continue

        for col, (param, unit) in header_map.items():
            raw_val = row.get(col, "").strip()
            if not raw_val or raw_val in ("", "N/A", "--"):
                continue
            try:
                value = float(raw_val)
            except (TypeError, ValueError):
                continue

            # -99.9 is a NRCS sentinel for missing data
            if value <= -99.0:
                continue

            readings.append(
                Reading(
                    station_id=triplet,
                    timestamp=ts,
                    parameter=param,
                    value=value,
                    unit=unit,
                    quality=None,
                    source="climate",
                )
            )

    return readings


# ---------------------------------------------------------------------------
# ACIS Endpoints
# ---------------------------------------------------------------------------

@router.get(
    "/acis/stations",
    summary="Search ACIS climate stations by state or bounding box",
)
async def acis_list_stations(
    state: Optional[str] = Query(
        None,
        description=(
            "Two-letter state code (e.g. 'OR').  "
            "Returns all ACIS stations in the state."
        ),
    ),
    bbox: Optional[str] = Query(
        None,
        description=(
            "Bounding box as west,south,east,north decimal degrees "
            "(e.g. '-125,42,-117,46' for Oregon)."
        ),
    ),
    limit: int = Query(500, ge=1, le=5000, description="Maximum number of stations to return"),
):
    """Return ACIS climate station metadata filtered by state or bounding box.

    ACIS covers 30,000+ active US stations from the COOP, ASOS, AWOS, and
    GHCN-Daily networks.  At least one of ``state`` or ``bbox`` must be
    provided.

    Station IDs (``sid``) are ACIS composite identifiers.  The most common
    formats are:
    - ``USW00024229``  — NWS/WBAN ID
    - ``350454``       — COOP ID
    - ``KPDX``         — ICAO code
    """
    if not state and not bbox:
        raise HTTPException(
            status_code=400,
            detail="Provide at least one of: state (e.g. ?state=OR) or bbox (west,south,east,north)",
        )

    cache_key = f"climate:acis:stations:{state}:{bbox}:{limit}"
    cached = await cache.get(cache_key)
    if cached is not None:
        return cached

    body: dict[str, Any] = {
        # Note: "valid_daterange" causes ACIS to return an empty result set when
        # combined with a state filter — omit it.
        "meta": "name,state,sids,ll,elev,uid",
    }
    if state:
        body["state"] = state.upper()
    if bbox:
        parts = [p.strip() for p in bbox.split(",")]
        if len(parts) != 4:
            raise HTTPException(
                status_code=400,
                detail="bbox must be four comma-separated values: west,south,east,north",
            )
        body["bbox"] = ",".join(parts)

    url = f"{ACIS_BASE}/StnMeta"
    try:
        async with _make_client() as client:
            resp = await client.post(url, json=body)
            resp.raise_for_status()
            data = resp.json()
    except httpx.TimeoutException:
        raise HTTPException(status_code=504, detail="ACIS API timeout")
    except httpx.HTTPStatusError as exc:
        raise HTTPException(
            status_code=exc.response.status_code,
            detail=f"ACIS API error: {exc}",
        )
    except Exception as exc:
        logger.exception("Error fetching ACIS station list")
        raise HTTPException(status_code=502, detail=str(exc))

    raw_stations = data.get("meta", [])
    if not raw_stations:
        logger.warning("ACIS StnMeta returned no stations for body: %s", body)

    # Apply limit
    raw_stations = raw_stations[:limit]
    stations = [_acis_station_to_model(s) for s in raw_stations]

    result = {
        "source": "acis",
        "filters": {"state": state, "bbox": bbox},
        "count": len(stations),
        "stations": [s.model_dump() for s in stations],
    }
    await cache.set(cache_key, result, ttl=config.CACHE_TTL_LONG)
    return result


@router.get(
    "/acis/stations/{sid}/data",
    summary="Time-series climate data for one ACIS station",
)
async def acis_station_data(
    sid: str,
    days: int = Query(7, ge=1, le=3650, description="Days of history to retrieve (max 10 years)"),
    elems: str = Query(
        ACIS_DEFAULT_ELEMS,
        description=(
            "Comma-separated ACIS element codes. "
            "1=MaxTemp, 2=MinTemp, 4=Precip, 10=AvgTemp, 7=PanEvap, 11=Wind. "
            "Default: '1,2,4' (MaxTemp, MinTemp, Precip)."
        ),
    ),
):
    """Return daily climate time-series for a single ACIS station.

    Data is returned as a list of Reading objects, one per day per element.
    Missing days or values are omitted.

    Example station IDs:
    - ``USW00024229`` — Portland International Airport (KPDX)
    - ``350454``      — Corvallis, OR COOP station
    """
    cache_key = f"climate:acis:stndata:{sid}:{days}:{elems}"
    cached = await cache.get(cache_key)
    if cached is not None:
        return cached

    now = datetime.now(tz=timezone.utc)
    edate = now.strftime("%Y-%m-%d")
    sdate = (now - timedelta(days=days)).strftime("%Y-%m-%d")

    body: dict[str, Any] = {
        "sid": sid,
        "sdate": sdate,
        "edate": edate,
        "elems": elems,
        "meta": "name,state,sids,ll,elev,uid",
    }

    url = f"{ACIS_BASE}/StnData"
    try:
        async with _make_client() as client:
            resp = await client.post(url, json=body)
            resp.raise_for_status()
            data = resp.json()
    except httpx.TimeoutException:
        raise HTTPException(status_code=504, detail="ACIS API timeout")
    except httpx.HTTPStatusError as exc:
        if exc.response.status_code == 400:
            raise HTTPException(
                status_code=400,
                detail=f"ACIS rejected the request — check station ID '{sid}' and element codes '{elems}'",
            )
        raise HTTPException(
            status_code=exc.response.status_code,
            detail=f"ACIS API error: {exc}",
        )
    except Exception as exc:
        logger.exception("Error fetching ACIS data for station %s", sid)
        raise HTTPException(status_code=502, detail=str(exc))

    if "error" in data:
        raise HTTPException(
            status_code=422,
            detail=f"ACIS API error for station '{sid}': {data['error']}",
        )

    meta_block = data.get("meta", {})
    if not meta_block:
        raise HTTPException(
            status_code=404,
            detail=f"ACIS station '{sid}' not found",
        )

    # Inject the requested elems into the meta block so the parser can map columns
    meta_block["elems"] = elems
    station = _acis_station_to_model(meta_block)
    readings = _acis_parse_stndata(data, sid)
    readings.sort(key=lambda r: (r.timestamp, r.parameter))

    # Set last_update from the most recent reading timestamp
    if readings:
        station.last_update = max(r.timestamp for r in readings)

    result = {
        "source": "acis",
        "station": station.model_dump(),
        "sdate": sdate,
        "edate": edate,
        "elems": elems,
        "days": days,
        "count": len(readings),
        "readings": [r.model_dump() for r in readings],
    }
    await cache.set(cache_key, result, ttl=config.CACHE_TTL)
    return result


@router.get(
    "/acis/multi",
    summary="Latest climate data for multiple ACIS stations in a state",
)
async def acis_multi_station(
    state: str = Query(..., description="Two-letter state code (e.g. 'OR')"),
    date: Optional[str] = Query(
        None,
        description=(
            "Date in YYYY-MM-DD format.  Defaults to today.  "
            "ACIS returns the observation for that calendar day."
        ),
    ),
    elems: str = Query(
        ACIS_DEFAULT_ELEMS,
        description=(
            "Comma-separated ACIS element codes. "
            "1=MaxTemp, 2=MinTemp, 4=Precip, 10=AvgTemp. "
            "Default: '1,2,4'."
        ),
    ),
):
    """Return a single day of climate data for all ACIS stations in a state.

    Uses the ACIS MultiStnData endpoint which returns one data row per station
    for the requested date.  Useful for creating state-wide snapshots.
    """
    if date is None:
        date = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d")
    else:
        # Validate date format
        try:
            datetime.strptime(date, "%Y-%m-%d")
        except ValueError:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid date '{date}' — expected YYYY-MM-DD format",
            )

    cache_key = f"climate:acis:multi:{state}:{date}:{elems}"
    cached = await cache.get(cache_key)
    if cached is not None:
        return cached

    body: dict[str, Any] = {
        "state": state.upper(),
        "date": date,
        "elems": elems,
        "meta": "name,state,sids,ll,elev,uid",
    }

    url = f"{ACIS_BASE}/MultiStnData"
    try:
        async with _make_client() as client:
            resp = await client.post(url, json=body)
            resp.raise_for_status()
            data = resp.json()
    except httpx.TimeoutException:
        raise HTTPException(status_code=504, detail="ACIS API timeout")
    except httpx.HTTPStatusError as exc:
        raise HTTPException(
            status_code=exc.response.status_code,
            detail=f"ACIS API error: {exc}",
        )
    except Exception as exc:
        logger.exception("Error fetching ACIS multi-station data for state %s", state)
        raise HTTPException(status_code=502, detail=str(exc))

    if "error" in data:
        raise HTTPException(
            status_code=422,
            detail=f"ACIS MultiStnData error: {data['error']}",
        )

    # Attach elems context to the top-level params so _acis_parse_multistn
    # can pass it down to the per-station parser.
    data["params"] = data.get("params", {})
    data["params"]["elems"] = elems

    station_results = _acis_parse_multistn(data)

    result = {
        "source": "acis",
        "state": state.upper(),
        "date": date,
        "elems": elems,
        "count": len(station_results),
        "stations": station_results,
    }
    await cache.set(cache_key, result, ttl=config.CACHE_TTL)
    return result


# ---------------------------------------------------------------------------
# SNOTEL Endpoints
# ---------------------------------------------------------------------------

@router.get(
    "/snotel/stations",
    summary="List NRCS SNOTEL or SCAN stations",
)
async def snotel_list_stations(
    state: Optional[str] = Query(
        None,
        description=(
            "Two-letter state code to filter stations (e.g. 'OR').  "
            "Omit to list all stations for the selected network."
        ),
    ),
    network: str = Query(
        "SNTL",
        description="Network code: SNTL (SNOTEL, default) or SCAN (Soil Climate Analysis Network)",
    ),
):
    """Return NRCS SNOTEL or SCAN station locations.

    SNOTEL stations are located in western US mountains and measure snow water
    equivalent, snow depth, precipitation, and air temperature.  SCAN stations
    are nationwide and focus on soil moisture and temperature.

    Station IDs are returned as ``triplet`` values in the format
    ``{stationId}:{state}:{network}`` (e.g. ``302:OR:SNTL``).  These triplets
    are required for the data endpoint.
    """
    network = network.upper()
    if network not in ("SNTL", "SCAN"):
        raise HTTPException(
            status_code=400,
            detail="network must be one of: SNTL (SNOTEL) or SCAN",
        )

    cache_key = f"climate:snotel:stations:{state}:{network}"
    cached = await cache.get(cache_key)
    if cached is not None:
        return cached

    # Use the AWDB REST API — the reportGenerator CSV filter is broken for multi-station queries.
    # The REST API returns all ~4,400 stations; we filter client-side by networkCode and stateCode.
    awdb_url = f"{AWDB_REST_BASE}/stations"
    try:
        async with _make_client() as client:
            resp = await client.get(awdb_url, timeout=45)
            resp.raise_for_status()
            raw_stations_json = resp.json()
    except httpx.TimeoutException:
        raise HTTPException(status_code=504, detail="NRCS AWDB REST API timeout")
    except httpx.HTTPStatusError as exc:
        raise HTTPException(
            status_code=exc.response.status_code,
            detail=f"NRCS AWDB error: {exc}",
        )
    except Exception as exc:
        logger.exception("Error fetching AWDB station list (network=%s, state=%s)", network, state)
        raise HTTPException(status_code=502, detail=str(exc))

    # Filter to the requested network and optionally state
    state_upper = state.upper() if state else None
    raw_stations_json = [
        r for r in (raw_stations_json if isinstance(raw_stations_json, list) else [])
        if r.get("networkCode", "").upper() == network
        and (state_upper is None or r.get("stateCode", "").upper() == state_upper)
    ]

    # AWDB REST returns a flat list of station objects, each with:
    # stationTriplet, stationId, stateCode, networkCode, name,
    # latitude, longitude, elevation, beginDate, endDate, ...
    stations: list[Station] = []
    if isinstance(raw_stations_json, list):
        for raw in raw_stations_json:
            try:
                triplet = raw.get("stationTriplet", "")
                sid = raw.get("stationId", "")
                state_code = raw.get("stateCode", "")
                net_code = raw.get("networkCode", network)
                canonical_triplet = triplet or f"{sid}:{state_code}:{net_code}"

                try:
                    lat = float(raw.get("latitude") or 0.0)
                except (TypeError, ValueError):
                    lat = 0.0
                try:
                    lon = float(raw.get("longitude") or 0.0)
                except (TypeError, ValueError):
                    lon = 0.0
                try:
                    elev = float(raw.get("elevation") or 0.0) or None
                except (TypeError, ValueError):
                    elev = None

                params_list = (
                    ["snow_water_equivalent", "snow_depth", "precipitation", "temperature"]
                    if net_code == "SNTL"
                    else ["soil_moisture", "soil_temperature", "precipitation", "temperature"]
                )

                # Use network-specific source for correct quality_tier:
                #   SNTL → "snotel" (Tier 2), SCAN → "scan" (Tier 2)
                src = "snotel" if net_code == "SNTL" else "scan"

                stations.append(
                    Station(
                        id=canonical_triplet,
                        name=raw.get("name") or canonical_triplet,
                        lat=lat,
                        lon=lon,
                        source=src,
                        network=net_code,
                        params=params_list,
                        elevation=elev,
                        url=(
                            f"https://wcc.sc.egov.usda.gov/nwcc/site?sitenum={sid}"
                        ),
                        extra={
                            "triplet": canonical_triplet,
                            "station_id": sid,
                            "state": state_code,
                            "network": net_code,
                            "county": raw.get("countyName", ""),
                            "huc": raw.get("huc", ""),
                            "begin_date": raw.get("beginDate", ""),
                            "end_date": raw.get("endDate", ""),
                        },
                    )
                )
            except Exception:
                logger.debug("Failed to parse AWDB station: %s", raw, exc_info=True)

    result = {
        "source": "snotel",
        "network": network,
        "network_name": "SNOTEL" if network == "SNTL" else "SCAN",
        "state": state.upper() if state else None,
        "count": len(stations),
        "stations": [s.model_dump() for s in stations],
        "data_source": "NRCS AWDB REST API",
    }
    await cache.set(cache_key, result, ttl=config.CACHE_TTL_LONG)
    return result


@router.get(
    "/snotel/stations/{triplet:path}/data",
    summary="Time-series snowpack/climate data for one SNOTEL/SCAN station",
)
async def snotel_station_data(
    triplet: str,
    days: int = Query(7, ge=1, le=365, description="Days of history to retrieve (max 1 year)"),
    params: str = Query(
        SNOTEL_DEFAULT_PARAMS,
        description=(
            "Colon-separated parameter specifications for the report generator. "
            "Format: CODE::value (e.g. 'WTEQ::value,SNWD::value,PREC::value,TOBS::value'). "
            "Available codes: WTEQ (snow water equiv), SNWD (snow depth), "
            "PREC (precip), TOBS (air temp), TMAX, TMIN, SMS (soil moisture), STO (soil temp)."
        ),
    ),
):
    """Return daily snowpack and climate data for a single SNOTEL or SCAN station.

    ``triplet`` is the station's canonical identifier in the form
    ``{stationId}:{state}:{network}`` (e.g. ``302:OR:SNTL``).

    Data is sourced from the NRCS reportGenerator CSV endpoint and converted
    to the platform's standard Reading format.

    Example triplets:
    - ``302:OR:SNTL``  — Cascade Summit SNOTEL, Oregon
    - ``1008:WA:SNTL`` — Paradise SNOTEL, Mount Rainier, Washington
    - ``2001:TX:SCAN`` — Uvalde SCAN station, Texas
    """
    # Basic triplet format validation (id:state:network)
    parts = triplet.split(":")
    if len(parts) != 3:
        raise HTTPException(
            status_code=400,
            detail=(
                f"Invalid triplet format '{triplet}'.  "
                "Expected stationId:state:network, e.g. '302:OR:SNTL'"
            ),
        )
    station_num, state_code, net_code = parts
    net_code = net_code.upper()
    state_code = state_code.upper()
    canonical_triplet = f"{station_num}:{state_code}:{net_code}"

    cache_key = f"climate:snotel:data:{canonical_triplet}:{days}:{params}"
    cached = await cache.get(cache_key)
    if cached is not None:
        return cached

    url = _build_snotel_data_url(canonical_triplet, days, params)

    try:
        async with _make_client() as client:
            resp = await client.get(url)
            resp.raise_for_status()
            text = resp.text
    except httpx.TimeoutException:
        raise HTTPException(status_code=504, detail="NRCS report generator timeout")
    except httpx.HTTPStatusError as exc:
        if exc.response.status_code == 404:
            raise HTTPException(
                status_code=404,
                detail=f"SNOTEL station '{canonical_triplet}' not found",
            )
        raise HTTPException(
            status_code=exc.response.status_code,
            detail=f"NRCS report generator error: {exc}",
        )
    except Exception as exc:
        logger.exception("Error fetching SNOTEL data for triplet %s", canonical_triplet)
        raise HTTPException(status_code=502, detail=str(exc))

    if not text or not text.strip():
        raise HTTPException(
            status_code=404,
            detail=f"SNOTEL station '{canonical_triplet}' returned no data",
        )

    # Extract station name from comment lines (lines starting with #)
    station_name = canonical_triplet
    for line in text.splitlines():
        if line.startswith("#") and ":" in line:
            # Comment lines often contain "# Station: Name" or similar
            candidate = line.lstrip("#").strip()
            if candidate and not candidate.startswith("Date") and len(candidate) > 3:
                station_name = candidate
                break

    readings = _parse_snotel_csv_data(text, canonical_triplet)
    readings.sort(key=lambda r: (r.timestamp, r.parameter))

    # Derive last_update from the most recent reading timestamp
    snotel_last_update = max((r.timestamp for r in readings), default=None)

    # Build a minimal Station model from what we know
    station = Station(
        id=canonical_triplet,
        name=station_name,
        lat=0.0,
        lon=0.0,
        source="climate",
        network=net_code,
        params=list({r.parameter for r in readings}),
        url=f"https://wcc.sc.egov.usda.gov/nwcc/site?sitenum={station_num}",
        last_update=snotel_last_update,
        extra={
            "triplet": canonical_triplet,
            "station_id": station_num,
            "state": state_code,
            "network": net_code,
            "report_url": url,
        },
    )

    result = {
        "source": "snotel",
        "station": station.model_dump(),
        "triplet": canonical_triplet,
        "days": days,
        "params": params,
        "count": len(readings),
        "readings": [r.model_dump() for r in readings],
    }
    await cache.set(cache_key, result, ttl=config.CACHE_TTL)
    return result


# ---------------------------------------------------------------------------
# Overview / info endpoint
# ---------------------------------------------------------------------------

@router.get(
    "/info",
    summary="Climate & Snowpack source information",
)
async def climate_info():
    """Return metadata describing both integrated climate data sources.

    Includes endpoint inventory, parameter lists, station counts, and links
    to the upstream APIs.
    """
    return {
        "platform": "wxsection.com",
        "source": SOURCE_NAME,
        "description": SOURCE_DESCRIPTION,
        "generated_at": datetime.now(tz=timezone.utc).isoformat(),
        "networks": [
            {
                "id": "acis",
                "name": "ACIS / RCC-ACIS",
                "operator": "NOAA Regional Climate Centers (RCCs)",
                "description": (
                    "Applied Climate Information System — aggregates historical and recent "
                    "climate data from 30,000+ US stations including COOP, ASOS, AWOS, "
                    "and GHCN-Daily networks."
                ),
                "coverage": "United States",
                "station_count_approx": 30000,
                "auth_required": False,
                "update_frequency": "Daily",
                "params": [
                    "temperature_max", "temperature_min", "temperature_avg",
                    "precipitation", "pan_evaporation", "wind_movement",
                ],
                "elem_codes": {
                    k: {"name": v[0], "unit": v[1]}
                    for k, v in ACIS_ELEMENTS.items()
                },
                "endpoints": [
                    "GET /api/climate/acis/stations?state=OR",
                    "GET /api/climate/acis/stations?bbox=-125,42,-117,46",
                    "GET /api/climate/acis/stations/{sid}/data?days=14",
                    "GET /api/climate/acis/multi?state=OR&date=2024-01-15",
                ],
                "api_url": ACIS_BASE,
                "docs": "https://www.rcc-acis.org/docs_webservices.html",
            },
            {
                "id": "snotel",
                "name": "NRCS SNOTEL / SCAN",
                "operator": "USDA Natural Resources Conservation Service (NRCS)",
                "description": (
                    "SNOTEL (Snow Telemetry) — ~900 automated mountain stations in the "
                    "western US measuring snowpack, precipitation, and temperature.  "
                    "SCAN (Soil Climate Analysis Network) — ~3,100 nationwide stations "
                    "measuring soil moisture and temperature."
                ),
                "coverage": "Western United States (SNOTEL), nationwide (SCAN)",
                "station_count_approx": 4000,
                "auth_required": False,
                "update_frequency": "Hourly",
                "params": [
                    "snow_water_equivalent", "snow_depth", "precipitation",
                    "air_temperature", "soil_moisture", "soil_temperature",
                ],
                "param_codes": {
                    k: {"name": v[0], "unit": v[1]}
                    for k, v in SNOTEL_PARAMS.items()
                },
                "endpoints": [
                    "GET /api/climate/snotel/stations?state=OR&network=SNTL",
                    "GET /api/climate/snotel/stations?network=SCAN",
                    "GET /api/climate/snotel/stations/302:OR:SNTL/data?days=14",
                ],
                "api_url": "https://wcc.sc.egov.usda.gov/reportGenerator/",
                "docs": "https://www.nrcs.usda.gov/wps/portal/wcc/home/dataAccessHelp/",
            },
        ],
        **SOURCE_META,
    }
