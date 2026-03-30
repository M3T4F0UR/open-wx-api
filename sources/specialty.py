"""Specialty weather data sources — wxsection.com data platform.

Covers three niche but high-value networks:

  1. SNOTEL (NRCS)       — 900+ snowpack / water-supply monitoring sites
  2. RAWS  (NIFC/Synoptic) — Remote Automatic Weather Stations for fire weather
  3. Blitzortung           — Community lightning detection network

SNOTEL and RAWS are production-ready.  The Blitzortung integration provides
metadata and endpoint stubs; real-time strike streaming requires a WebSocket
client (see /api/specialty/lightning/info).

SNOTEL API reference: https://wcc.sc.egov.usda.gov/awdbRestApi/swagger-ui/index.html
NIFC RAWS (ArcGIS):   https://data-nifc.opendata.arcgis.com/
Blitzortung:          https://www.blitzortung.org/
"""
from __future__ import annotations

import logging
from datetime import date, datetime, timedelta, timezone
from typing import Any, Optional
from urllib.parse import quote

import httpx
from fastapi import APIRouter, HTTPException, Query

import cache
import config
from models import Reading, Station, StationDetail

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Source registry metadata  (read by server.py /api/sources)
# ---------------------------------------------------------------------------

SOURCE_NAME = "Specialty Networks (SNOTEL / RAWS / Lightning)"
SOURCE_DESCRIPTION = (
    "Three specialty observational networks: "
    "(1) NRCS SNOTEL — 900+ automated snowpack and water-supply stations across the western US; "
    "(2) RAWS — 2,000+ Remote Automatic Weather Stations managed by the USFS / BLM for fire weather; "
    "(3) Blitzortung — global community lightning detection network (WebSocket streaming)."
)
SOURCE_META = {
    "url": "https://www.nrcs.usda.gov/wps/portal/wcc/home/",
    "auth_required": False,
    "update_frequency": "1 hr (SNOTEL), 10 min (RAWS), real-time (lightning)",
    "coverage": "Western United States (SNOTEL/RAWS), Global (Lightning)",
    "params": [
        "snow_water_equivalent", "snow_depth", "precipitation",
        "air_temperature", "soil_moisture",
        "air_temp", "wind_speed", "wind_direction", "relative_humidity",
        "solar_radiation", "fuel_moisture",
        "lightning_strikes",
    ],
    "category": "weather",
}

# ---------------------------------------------------------------------------
# Sub-source constants
# ---------------------------------------------------------------------------

# SNOTEL / AWDB REST API (NRCS)
AWDB_BASE = "https://wcc.sc.egov.usda.gov/awdbRestApi/services/v1"
SNOTEL_NETWORK_CODE = "SNTL"

# SNOTEL element codes → (human name, unit)
SNOTEL_ELEMENTS: dict[str, tuple[str, str]] = {
    "WTEQ": ("Snow Water Equivalent",        "in"),
    "SNWD": ("Snow Depth",                   "in"),
    "PREC": ("Precipitation Accumulated",    "in"),
    "PRCPSA": ("Precipitation (Sensor A)",   "in"),
    "TAVG": ("Air Temperature (Average)",    "°F"),
    "TMAX": ("Air Temperature (Max)",        "°F"),
    "TMIN": ("Air Temperature (Min)",        "°F"),
    "SMS":  ("Soil Moisture",                "%"),
    "STO":  ("Soil Temperature",             "°F"),
    "SAL":  ("Snow Albedo",                  "%"),
    "WDIRV": ("Wind Direction",              "°"),
    "WSPDV": ("Wind Speed",                  "mph"),
    "WSPDX": ("Wind Gust",                   "mph"),
    "RHUM": ("Relative Humidity",            "%"),
    "SRAD": ("Solar Radiation",              "W/m²"),
    "BATT": ("Battery Voltage",              "V"),
}

DEFAULT_SNOTEL_ELEMENTS = "WTEQ,SNWD,PREC,TAVG"

# RAWS via ArcGIS FeatureServer — CAL FIRE Wind 2-D Public View
# (original NIFC IRWIN_RAWS_Active service is defunct / returns Invalid URL)
RAWS_ARCGIS_BASE = (
    "https://services5.arcgis.com/bz1uwWPKUInZBK94/arcgis/rest/services"
    "/RAWS_Wind_2D_Public_View/FeatureServer/0/query"
)
# Fallback: Synoptic also exposes RAWS as network 71
RAWS_SYNOPTIC_NETWORK = "71"

# Blitzortung
BLITZORTUNG_WS_ENDPOINT = "wss://ws1.blitzortung.org/"
BLITZORTUNG_DATA_BASE = "https://data.blitzortung.org/Strokes/"
BLITZORTUNG_DOCS = "https://www.blitzortung.org/en/live_lightning_maps.php"

# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------

_HTTP_HEADERS = {
    "User-Agent": config.USER_AGENT,
    "Accept": "application/json",
}


def _make_client() -> httpx.AsyncClient:
    return httpx.AsyncClient(
        headers=_HTTP_HEADERS,
        timeout=config.HTTP_TIMEOUT,
        follow_redirects=True,
    )


async def _get_json(url: str, params: Optional[dict] = None, label: str = "upstream") -> Any:
    """GET url, return parsed JSON; raise HTTPException on failure."""
    try:
        async with _make_client() as client:
            resp = await client.get(url, params=params or {})
            resp.raise_for_status()
            return resp.json()
    except httpx.TimeoutException:
        raise HTTPException(status_code=504, detail=f"{label} API timeout ({url})")
    except httpx.HTTPStatusError as exc:
        raise HTTPException(
            status_code=exc.response.status_code,
            detail=f"{label} API error: HTTP {exc.response.status_code} for {url}",
        )
    except Exception as exc:
        logger.exception("Unexpected error fetching %s", url)
        raise HTTPException(status_code=502, detail=f"Error fetching {label}: {exc}")


# ---------------------------------------------------------------------------
# SNOTEL parsing helpers
# ---------------------------------------------------------------------------

def _triplet_to_id(triplet: str) -> str:
    """Normalise a SNOTEL triplet: '301:OR:SNTL' → '301:OR:SNTL' (strip whitespace)."""
    return triplet.strip()


def _parse_snotel_station(raw: dict) -> Station:
    """Convert an AWDB station metadata record to the shared Station model."""
    triplet = raw.get("stationTriplet", "")
    name = raw.get("name", triplet)

    try:
        lat = float(raw.get("latitude", 0.0))
    except (TypeError, ValueError):
        lat = 0.0
    try:
        lon = float(raw.get("longitude", 0.0))
    except (TypeError, ValueError):
        lon = 0.0
    try:
        elev_ft = raw.get("elevation")
        elevation = float(elev_ft) * 0.3048 if elev_ft is not None else None  # ft → m
    except (TypeError, ValueError):
        elevation = None

    state = raw.get("stateCode", "")
    county = raw.get("countyName", "")
    huc = raw.get("huc", "")

    # Build station ID that is URL-safe
    safe_id = triplet.replace(":", "_")

    # The AWDB station listing uses endDate = "2100-01-01 00:00" as a sentinel
    # for active stations — it is NOT a real last-observation timestamp.
    # For active stations (the default query), use the current fetch time as
    # last_update since SNOTEL reports hourly.  For truly inactive stations
    # (endDate in the past), parse the real end date.
    last_update = None
    end_date_str = raw.get("endDate", "")
    if end_date_str:
        try:
            for fmt in ("%Y-%m-%d %H:%M", "%Y-%m-%d"):
                try:
                    parsed = datetime.strptime(end_date_str, fmt).replace(tzinfo=timezone.utc)
                    break
                except ValueError:
                    parsed = None
            if parsed is not None:
                # If endDate is a far-future sentinel (year >= 2099), this is an
                # active station — use current time as the freshness indicator.
                if parsed.year >= 2099:
                    last_update = datetime.now(timezone.utc)
                else:
                    # Genuinely closed station — use the real end date
                    last_update = parsed
        except (ValueError, TypeError):
            pass

    return Station(
        id=safe_id,
        name=name,
        lat=lat,
        lon=lon,
        source="snotel",
        network="NRCS SNOTEL",
        params=list(SNOTEL_ELEMENTS.keys()),
        elevation=elevation,
        last_update=last_update,
        url=(
            f"https://wcc.sc.egov.usda.gov/nwcc/site?sitenum={raw.get('stationId', '')}"
        ),
        extra={
            "triplet": triplet,
            "station_id": raw.get("stationId", ""),
            "state": state,
            "county": county,
            "huc": huc,
            "network_code": raw.get("networkCode", SNOTEL_NETWORK_CODE),
            "begin_date": raw.get("beginDate", ""),
            "end_date": raw.get("endDate", ""),
            "data_time_zone": raw.get("dataTimeZone", ""),
        },
    )


def _parse_awdb_data(raw_data: list[dict], station_id: str) -> list[Reading]:
    """Parse an AWDB data response (array of element data blocks) into Readings."""
    readings: list[Reading] = []

    for element_block in raw_data:
        element_code = element_block.get("stationElement", {}).get("elementCode", "")
        human_name, unit = SNOTEL_ELEMENTS.get(element_code, (element_code, ""))

        # Each element block has a "values" array: [{value, date, ...}, ...]
        values_list = element_block.get("values", []) or []
        for entry in values_list:
            raw_val = entry.get("value")
            raw_date = entry.get("date", "")

            if raw_val is None or raw_val == "" or raw_date == "":
                continue
            try:
                numeric = float(raw_val)
            except (TypeError, ValueError):
                continue

            # AWDB dates can be "YYYY-MM-DD" or "YYYY-MM-DD HH:mm"
            try:
                if "T" in raw_date or " " in raw_date:
                    dt_str = raw_date.replace(" ", "T")
                    if "+" not in dt_str and "Z" not in dt_str:
                        dt_str += "+00:00"
                    ts = datetime.fromisoformat(dt_str)
                else:
                    # Date-only — assume midnight UTC
                    ts = datetime.strptime(raw_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
            except (ValueError, AttributeError):
                continue

            quality = entry.get("qualityControlled")
            quality_str = "qc" if quality else None

            readings.append(
                Reading(
                    station_id=station_id,
                    timestamp=ts,
                    parameter=human_name if human_name else element_code,
                    value=numeric,
                    unit=unit,
                    quality=quality_str,
                    source="snotel",
                )
            )

    return readings


# ---------------------------------------------------------------------------
# RAWS parsing helpers
# ---------------------------------------------------------------------------

def _parse_raws_feature(feature: dict) -> Optional[Station]:
    """Convert a NIFC ArcGIS feature to a Station model."""
    attrs = feature.get("attributes", {})
    geom = feature.get("geometry") or {}

    # ArcGIS geometry uses x/y (lon/lat) in WGS84
    lon = geom.get("x") or attrs.get("Longitude") or attrs.get("longitude")
    lat = geom.get("y") or attrs.get("Latitude") or attrs.get("latitude")

    if lon is None or lat is None:
        return None

    try:
        lat = float(lat)
        lon = float(lon)
    except (TypeError, ValueError):
        return None

    # Various field name conventions in the NIFC / CAL FIRE dataset
    station_id = (
        attrs.get("StationID")
        or attrs.get("station_id")
        or attrs.get("stationId")
        or attrs.get("OBJECTID")
        or ""
    )
    name = (
        attrs.get("StationName")
        or attrs.get("station_name")
        or attrs.get("stationName")
        or attrs.get("NAME")
        or str(station_id)
    )
    state = attrs.get("State") or attrs.get("state") or attrs.get("StateCode") or ""

    try:
        elev_ft = attrs.get("Elevation") or attrs.get("elevation")
        elevation = float(elev_ft) * 0.3048 if elev_ft is not None else None
    except (TypeError, ValueError):
        elevation = None

    agency = attrs.get("Agency") or attrs.get("agency") or attrs.get("ManagingAgency") or ""
    nwszone = attrs.get("NWSZone") or attrs.get("nwsZone") or ""

    # Extract observation timestamp from ArcGIS attributes (epoch-ms or ISO string)
    # CAL FIRE uses "last_updated"; NIFC used "DateTime"/"date_time"
    last_update = None
    for ts_key in ("last_updated", "DateTime", "date_time", "LastUpdate",
                   "last_update", "ObservationTime"):
        ts_raw = attrs.get(ts_key)
        if ts_raw is not None:
            try:
                if isinstance(ts_raw, (int, float)):
                    last_update = datetime.fromtimestamp(int(ts_raw) / 1000, tz=timezone.utc)
                elif isinstance(ts_raw, str) and ts_raw.strip():
                    last_update = datetime.fromisoformat(ts_raw.replace("Z", "+00:00"))
            except (ValueError, TypeError, OSError):
                pass
            if last_update is not None:
                break

    return Station(
        id=f"raws:{station_id}",
        name=str(name),
        lat=lat,
        lon=lon,
        source="raws",
        network="NIFC RAWS",
        params=[
            "air_temp", "relative_humidity", "wind_speed",
            "wind_direction", "wind_gust", "precipitation",
            "fuel_moisture", "solar_radiation",
        ],
        elevation=elevation,
        last_update=last_update,
        url=f"https://raws.nifc.gov/station/{station_id}",
        extra={
            "state": state,
            "agency": agency,
            "nws_zone": nwszone,
            "raw_attrs": {k: v for k, v in attrs.items() if v is not None},
        },
    )


# ---------------------------------------------------------------------------
# Router
# ---------------------------------------------------------------------------

router = APIRouter(prefix="/api/specialty", tags=["Specialty (SNOTEL / RAWS / Lightning)"])


# ===========================================================================
# SNOTEL endpoints
# ===========================================================================

@router.get(
    "/snotel/stations",
    summary="List SNOTEL snowpack monitoring stations",
)
async def snotel_stations(
    state: Optional[str] = Query(
        None,
        description="Two-letter US state code (e.g. 'OR', 'CO').  Leave blank for all states.",
        min_length=2,
        max_length=2,
    ),
    network: str = Query(
        SNOTEL_NETWORK_CODE,
        description=(
            "AWDB network code.  'SNTL'=SNOTEL, 'SNOW'=snow courses, "
            "'SCAN'=soil climate, 'COOP'=co-op.  Default: SNTL"
        ),
    ),
    active_only: bool = Query(True, description="Return only currently active stations"),
):
    """Return all NRCS AWDB stations for the specified network.

    SNOTEL stations are concentrated in the western US mountain ranges and
    report hourly snow water equivalent, snow depth, precipitation,
    temperature, and soil moisture.  Results cached for 1 hour.

    Station IDs are 'stationTriplets' in the form `{number}:{state}:{network}`
    (e.g. `301:OR:SNTL`).  Use the URL-safe encoded form `301_OR_SNTL` in
    subsequent API calls to this platform.
    """
    cache_key = f"snotel:stations:{state}:{network}:{active_only}"
    cached_result = await cache.get(cache_key)
    if cached_result is not None:
        return cached_result

    params: dict[str, Any] = {"networkCodes": network}
    if state:
        params["stateCodes"] = state.upper()
    if active_only:
        params["activeStationsOnly"] = "true"

    raw = await _get_json(f"{AWDB_BASE}/stations", params=params, label="NRCS AWDB")

    # AWDB returns either a list or a dict with a data wrapper
    if isinstance(raw, dict):
        station_list = raw.get("data") or raw.get("stations") or raw.get("results") or []
    elif isinstance(raw, list):
        station_list = raw
    else:
        station_list = []

    stations = [_parse_snotel_station(s) for s in station_list if isinstance(s, dict)]

    result = {
        "source": "nrcs_snotel",
        "network": network,
        "state_filter": state,
        "count": len(stations),
        "stations": [s.model_dump() for s in stations],
    }
    await cache.set(cache_key, result, ttl=config.CACHE_TTL_LONG)
    return result


@router.get(
    "/snotel/stations/{triplet}/readings",
    summary="SNOTEL element data for a station",
)
async def snotel_readings(
    triplet: str,
    days: int = Query(
        7,
        ge=1,
        le=3650,
        description="Number of days of history to retrieve (max 10 years)",
    ),
    elements: str = Query(
        DEFAULT_SNOTEL_ELEMENTS,
        description=(
            "Comma-separated AWDB element codes.  "
            "WTEQ=snow water equiv, SNWD=snow depth, PREC=precipitation, TAVG=avg temp.  "
            "Full list: https://wcc.sc.egov.usda.gov/reportGenerator/."
        ),
    ),
):
    """Return time-series element readings for a SNOTEL (or other AWDB) station.

    The `triplet` path parameter uses underscores as separators to be URL-safe:
    `301_OR_SNTL` (maps to AWDB triplet `301:OR:SNTL`).

    AWDB reports data at the station's native interval, typically:
    - Instantaneous / hourly: most SNOTEL elements
    - Daily: snow courses, some co-op stations

    Results are cached for 5 minutes.
    """
    # Allow both underscore-encoded and colon-encoded triplets
    awdb_triplet = triplet.replace("_", ":").upper()
    safe_id = triplet.replace(":", "_").upper()

    cache_key = f"snotel:readings:{safe_id}:{days}:{elements}"
    cached_result = await cache.get(cache_key)
    if cached_result is not None:
        return cached_result

    # Compute date range
    end_dt = date.today()
    begin_dt = end_dt - timedelta(days=days)
    begin_str = begin_dt.strftime("%Y-%m-%d")
    end_str = end_dt.strftime("%Y-%m-%d")

    # Fetch element data from AWDB
    params: dict[str, Any] = {
        "stationTriplets": awdb_triplet,
        "elements": elements,
        "beginDate": begin_str,
        "endDate": end_str,
        "centralTendencyType": "None",
    }

    raw = await _get_json(f"{AWDB_BASE}/data", params=params, label="NRCS AWDB")

    # Fetch station metadata for coordinates etc.
    meta_raw = await _get_json(
        f"{AWDB_BASE}/stations",
        params={"stationTriplets": awdb_triplet},
        label="NRCS AWDB metadata",
    )

    # Parse metadata
    if isinstance(meta_raw, list) and meta_raw:
        station_meta = meta_raw[0]
    elif isinstance(meta_raw, dict):
        station_meta = meta_raw.get("data", [meta_raw])[0] if meta_raw.get("data") else meta_raw
    else:
        # Build minimal metadata from triplet
        parts = awdb_triplet.split(":")
        station_meta = {
            "stationTriplet": awdb_triplet,
            "name": awdb_triplet,
            "stateCode": parts[1] if len(parts) > 1 else "",
        }

    station = _parse_snotel_station(station_meta)
    station.id = safe_id  # ensure consistent ID format

    # Parse observations
    if isinstance(raw, list):
        data_blocks = raw
    elif isinstance(raw, dict):
        data_blocks = raw.get("data") or [raw]
    else:
        data_blocks = []

    readings = _parse_awdb_data(data_blocks, safe_id)
    readings.sort(key=lambda r: r.timestamp)

    result = {
        "station": station.model_dump(),
        "triplet": awdb_triplet,
        "begin_date": begin_str,
        "end_date": end_str,
        "days": days,
        "elements": elements,
        "count": len(readings),
        "readings": [r.model_dump() for r in readings],
    }
    await cache.set(cache_key, result, ttl=config.CACHE_TTL)
    return result


@router.get(
    "/snotel/stations/{triplet}",
    summary="SNOTEL station metadata + latest readings",
)
async def snotel_station_detail(triplet: str):
    """Return metadata and the most recent observation for each element at a SNOTEL station.

    Uses the last 2 days of data to guarantee at least one valid reading per element.
    """
    awdb_triplet = triplet.replace("_", ":").upper()
    safe_id = triplet.replace(":", "_").upper()

    cache_key = f"snotel:station_detail:{safe_id}"
    cached_result = await cache.get(cache_key)
    if cached_result is not None:
        return cached_result

    # Delegate to the readings endpoint with a short window
    data_result = await snotel_readings(
        triplet=triplet,
        days=2,
        elements=DEFAULT_SNOTEL_ELEMENTS + ",WDIRV,WSPDV,WSPDX,RHUM",
    )

    # Reduce to latest reading per element
    all_readings = [Reading(**r) for r in data_result["readings"]]
    latest: dict[str, Reading] = {}
    for r in all_readings:
        if r.parameter not in latest or r.timestamp > latest[r.parameter].timestamp:
            latest[r.parameter] = r

    station = Station(**data_result["station"])
    detail = StationDetail(station=station, readings=list(latest.values()))
    result = detail.model_dump()
    await cache.set(cache_key, result, ttl=config.CACHE_TTL)
    return result


@router.get(
    "/snotel/info",
    summary="SNOTEL source information and available element codes",
)
async def snotel_info():
    """Return information about the SNOTEL / AWDB data source."""
    return {
        "source": "NRCS SNOTEL / AWDB",
        "description": (
            "NRCS SNOTEL (SNOwpack TELemetry) — automated mountain snowpack monitoring "
            "network operated by the Natural Resources Conservation Service.  ~900 stations "
            "in the western US, primarily in high-elevation mountain basins."
        ),
        "api_base": AWDB_BASE,
        "api_docs": "https://wcc.sc.egov.usda.gov/awdbRestApi/swagger-ui/index.html",
        "auth_required": False,
        "update_frequency": "Instantaneous (hourly reports), daily official values",
        "coverage": "Western United States (ID, MT, WY, CO, UT, NV, CA, OR, WA, AK, and more)",
        "network_codes": {
            "SNTL": "SNOTEL — automated telemetry snowpack stations",
            "SNOW": "Snow Courses — manual monthly snowpack surveys",
            "SCAN": "Soil Climate Analysis Network — soil moisture & temperature",
            "COOP": "NWS Cooperative Observer stations (some managed by NRCS)",
        },
        "elements": {
            code: {"name": name, "unit": unit}
            for code, (name, unit) in SNOTEL_ELEMENTS.items()
        },
        "example_triplets": [
            "301:OR:SNTL (Crater Lake, OR)",
            "652:CO:SNTL (Berthoud Summit, CO)",
            "679:UT:SNTL (Alta, UT)",
            "1050:CA:SNTL (Mammoth Pass, CA)",
        ],
        "url": "https://www.nrcs.usda.gov/wps/portal/wcc/home/",
    }


# ===========================================================================
# RAWS endpoints
# ===========================================================================

@router.get(
    "/raws/stations",
    summary="NIFC RAWS fire weather stations",
)
async def raws_stations(
    state: Optional[str] = Query(
        None,
        description="Two-letter state code to filter (e.g. 'OR', 'CA')",
    ),
    bbox: Optional[str] = Query(
        None,
        description="Bounding box: west,south,east,north (decimal degrees)",
    ),
    limit: int = Query(
        200,
        ge=1,
        le=2000,
        description="Maximum number of stations to return",
    ),
):
    """Return active RAWS (Remote Automatic Weather Stations) from the NIFC dataset.

    RAWS are operated by the USDA Forest Service, BLM, NPS, and other land
    management agencies.  They report fire-weather parameters including
    temperature, relative humidity, wind, precipitation, fuel moisture, and
    solar radiation.

    Data is sourced from the NIFC ArcGIS FeatureServer.  Results cached for 1 hour.
    """
    cache_key = f"raws:stations:{state}:{bbox}:{limit}"
    cached_result = await cache.get(cache_key)
    if cached_result is not None:
        return cached_result

    # Build ArcGIS where clause
    where_parts: list[str] = ["1=1"]
    if state:
        where_parts.append(f"State='{state.upper()}'")

    # ArcGIS geometry filter for bbox
    arcgis_params: dict[str, Any] = {
        "where": " AND ".join(where_parts),
        "outFields": "*",
        "f": "json",
        "resultRecordCount": limit,
        "returnGeometry": "true",
        "outSR": "4326",
    }

    if bbox:
        parts = [p.strip() for p in bbox.split(",")]
        if len(parts) != 4:
            raise HTTPException(
                status_code=400,
                detail="bbox must be four comma-separated values: west,south,east,north",
            )
        west, south, east, north = parts
        arcgis_params["geometry"] = f"{west},{south},{east},{north}"
        arcgis_params["geometryType"] = "esriGeometryEnvelope"
        arcgis_params["inSR"] = "4326"
        arcgis_params["spatialRel"] = "esriSpatialRelIntersects"

    raw = await _get_json(RAWS_ARCGIS_BASE, params=arcgis_params, label="NIFC ArcGIS")

    # Check for ArcGIS error block
    if isinstance(raw, dict) and raw.get("error"):
        err = raw["error"]
        raise HTTPException(
            status_code=502,
            detail=f"NIFC ArcGIS error: {err.get('message', err)}",
        )

    features = raw.get("features", []) or []
    stations: list[Station] = []
    for feat in features:
        s = _parse_raws_feature(feat)
        if s is not None:
            stations.append(s)

    result = {
        "source": "nifc_raws",
        "count": len(stations),
        "filters": {"state": state, "bbox": bbox},
        "note": (
            "For real-time RAWS observations use the Synoptic endpoint: "
            "GET /api/synoptic/stations/latest?network=71&state={state}  "
            "(RAWS is Synoptic network 71)"
        ),
        "stations": [s.model_dump() for s in stations],
    }
    await cache.set(cache_key, result, ttl=config.CACHE_TTL_LONG)
    return result


@router.get(
    "/raws/info",
    summary="RAWS source information",
)
async def raws_info():
    """Return information about the RAWS fire weather network."""
    return {
        "source": "NIFC RAWS",
        "full_name": "Remote Automatic Weather Stations",
        "description": (
            "~2,100 automated weather stations managed by USDA Forest Service, BLM, "
            "NPS, BIA, and state agencies.  Provide core fire-weather parameters "
            "used in fire danger rating and dispatch decisions."
        ),
        "auth_required": False,
        "update_frequency": "10 minutes (some sites hourly)",
        "coverage": "United States and territories — concentrated in western states",
        "parameters": [
            "air_temperature", "relative_humidity",
            "wind_speed", "wind_direction", "wind_gust",
            "precipitation", "solar_radiation",
            "fuel_moisture_10hr", "fuel_moisture_100hr",
        ],
        "data_sources": {
            "nifc_arcgis": {
                "url": "https://data-nifc.opendata.arcgis.com/",
                "description": "NIFC Open Data — station locations and metadata via ArcGIS",
                "endpoint": RAWS_ARCGIS_BASE,
            },
            "synoptic": {
                "url": "https://synopticdata.com/",
                "description": "Real-time RAWS observations via Synoptic Data (network 71)",
                "note": "Requires SYNOPTIC_TOKEN.  Use GET /api/synoptic/stations/latest?network=71",
            },
            "famweb": {
                "url": "https://famweb.nwcg.gov/weatherstation/",
                "description": "NWCG Fire & Aviation Management weather station portal",
            },
        },
    }


# ===========================================================================
# Lightning (Blitzortung) endpoints
# ===========================================================================

@router.get(
    "/lightning/info",
    summary="Blitzortung lightning network — data access information",
)
async def lightning_info():
    """Return information about Blitzortung lightning data and how to access it.

    Blitzortung is a community-driven lightning detection network with ~2,000+
    volunteer-operated receivers worldwide.  It achieves 1–5 km accuracy for
    nearby strikes.

    Real-time strike data is available via WebSocket; this endpoint explains
    the integration architecture.
    """
    return {
        "source": "Blitzortung",
        "description": (
            "Community-operated global lightning detection network.  "
            "~2,000+ receiver stations worldwide contributed by volunteers.  "
            "Achieves 1–5 km accuracy for nearby strikes and millisecond-level "
            "timing via GPS-synchronized detectors."
        ),
        "coverage": "Global",
        "auth_required": False,
        "parameters": ["lightning_strike_lat", "lightning_strike_lon", "lightning_strike_time"],
        "real_time_websocket": {
            "endpoint": BLITZORTUNG_WS_ENDPOINT,
            "protocol": "WebSocket (WSS)",
            "note": (
                "Connect and send a region subscription message such as: "
                '{"west":-130,"east":-60,"north":55,"south":20} '
                "to receive real-time GeoJSON strike events for North America.  "
                "Strike objects include: {time (ns), lat, lon, alt, pol, mds, mcg}."
            ),
            "client_example": (
                "import websockets, json\n"
                "async with websockets.connect('wss://ws1.blitzortung.org/') as ws:\n"
                "    await ws.send(json.dumps({'west':-130,'east':-60,'north':55,'south':20}))\n"
                "    while True:\n"
                "        msg = await ws.recv()\n"
                "        strike = json.loads(msg)\n"
                "        print(strike)  # {'time':..., 'lat':..., 'lon':...}"
            ),
        },
        "historical_data": {
            "url": BLITZORTUNG_DATA_BASE,
            "format": "Compressed JSON stroke files, partitioned by time",
            "note": (
                "Historical bulk strike files are available at data.blitzortung.org. "
                "Files are organised by year/month/day/hour.  "
                "Each file is a gzip-compressed newline-delimited JSON stream."
            ),
        },
        "live_maps": BLITZORTUNG_DOCS,
        "third_party_apis": {
            "openweathermap": "https://openweathermap.org/api (includes lightning layer)",
            "tomorrow_io": "https://www.tomorrow.io/weather-api/ (lightning events)",
            "weatherbit": "https://www.weatherbit.io/api (lightning POI data)",
        },
        "integration_status": "WebSocket client not yet integrated — contributions welcome",
        "integration_note": (
            "Full WebSocket integration requires a persistent async task outside the "
            "request-response cycle.  The recommended pattern for this FastAPI project "
            "is a background asyncio task started in a lifespan handler that buffers "
            "strikes in a shared deque and exposes them via this REST API."
        ),
    }


@router.get(
    "/lightning/recent",
    summary="Recent lightning strikes (placeholder — WebSocket required)",
)
async def lightning_recent(
    lat: Optional[float] = Query(None, description="Center latitude for spatial filter"),
    lon: Optional[float] = Query(None, description="Center longitude for spatial filter"),
    radius_km: float = Query(
        200.0,
        ge=1.0,
        le=5000.0,
        description="Radius in km for spatial filter",
    ),
    minutes: int = Query(
        60,
        ge=1,
        le=1440,
        description="Window of recent strikes in minutes",
    ),
):
    """Placeholder endpoint for recent Blitzortung lightning strikes.

    Full real-time strike data requires a persistent WebSocket connection to
    `wss://ws1.blitzortung.org/`.  This endpoint returns a status object
    explaining the integration gap and how to implement it.

    When the WebSocket background task is running, this endpoint will return
    strikes buffered in memory from the live feed.
    """
    return {
        "status": "not_implemented",
        "source": "Blitzortung",
        "websocket_endpoint": BLITZORTUNG_WS_ENDPOINT,
        "query": {
            "lat": lat,
            "lon": lon,
            "radius_km": radius_km,
            "minutes": minutes,
        },
        "strikes": [],
        "count": 0,
        "message": (
            "Real-time strike data requires a persistent WebSocket connection to "
            f"{BLITZORTUNG_WS_ENDPOINT}.  "
            "Add a FastAPI lifespan background task that connects, buffers strikes "
            "in a collections.deque, and this endpoint can then query that buffer.  "
            "See GET /api/specialty/lightning/info for full integration guidance."
        ),
        "alternative": (
            "For immediate lightning data, consider: "
            "OpenWeatherMap (/api/onecall with thunderstorm alerts), "
            "Tomorrow.io (lightning events API), "
            "or NOAA SPC (https://www.spc.noaa.gov/climo/reports/) for severe reports."
        ),
    }


@router.get(
    "/lightning/websocket-sample",
    summary="Sample Blitzortung WebSocket message format",
)
async def lightning_websocket_sample():
    """Return an example of the Blitzortung WebSocket message format.

    Useful for building a client without needing a live connection.
    Strike times are nanoseconds since Unix epoch.
    """
    now_ns = int(datetime.now(tz=timezone.utc).timestamp() * 1e9)
    return {
        "source": "Blitzortung",
        "websocket_url": BLITZORTUNG_WS_ENDPOINT,
        "subscription_message": {
            "description": "Send this JSON to subscribe to a geographic region",
            "example": {"west": -130, "east": -60, "north": 55, "south": 20},
            "note": "Omit the bounding box to receive global feed (very high volume)",
        },
        "strike_message_format": {
            "time": {
                "description": "Strike time as nanoseconds since Unix epoch",
                "example": now_ns,
                "convert_python": "datetime.fromtimestamp(time / 1e9, tz=timezone.utc)",
            },
            "lat": {"description": "Latitude (decimal degrees)", "example": 45.1234},
            "lon": {"description": "Longitude (decimal degrees)", "example": -122.4567},
            "alt": {"description": "Altitude of detector (metres)", "example": 52},
            "pol": {"description": "Polarity: 0=negative CG, 1=positive CG", "example": 0},
            "mds": {"description": "Median deviation of arrival time (ns)", "example": 5000},
            "mcg": {"description": "Number of contributing stations", "example": 8},
            "status": {"description": "Detection status code", "example": 0},
            "region": {"description": "Geographic region code", "example": 3},
        },
        "sample_strikes": [
            {
                "time": now_ns - 120_000_000_000,
                "lat": 45.1234,
                "lon": -122.4567,
                "alt": 52,
                "pol": 0,
                "mds": 4800,
                "mcg": 9,
                "status": 0,
                "region": 3,
            },
            {
                "time": now_ns - 65_000_000_000,
                "lat": 44.8901,
                "lon": -121.3456,
                "alt": 48,
                "pol": 1,
                "mds": 7200,
                "mcg": 6,
                "status": 0,
                "region": 3,
            },
        ],
    }
