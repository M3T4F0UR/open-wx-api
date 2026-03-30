"""Western European Weather — Spain, Switzerland, France, Netherlands.

Aggregates surface observations from four Western European met services:

  1. AEMET (Spain)       — ~800 stations, free API key, two-step JSON fetch
  2. MeteoSwiss          — ~260 stations, no auth (opened May 2025), STAC → CSV
  3. Météo-France        — ~60 SYNOP stations via OpenDataSoft (no auth)
  4. KNMI (Netherlands)  — ~50 stations, free API key, 10-min data files

All services provide at minimum: temperature, humidity, wind, pressure.
"""
from __future__ import annotations

import csv
import io
import logging
from datetime import datetime, timezone
from typing import Any, Optional

import httpx
from fastapi import APIRouter, HTTPException

import cache
import config
from models import Reading, Station, StationDetail

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Source registry metadata  (read by server.py /api/sources)
# ---------------------------------------------------------------------------

SOURCE_NAME = "Western European Weather"
SOURCE_DESCRIPTION = (
    "Surface observations from four Western European national met services: "
    "AEMET (Spain, ~800 stations), MeteoSwiss (Switzerland, ~260 stations), "
    "Météo-France (~60 SYNOP stations via OpenDataSoft), and KNMI "
    "(Netherlands, ~50 stations with 10-minute data)."
)
SOURCE_META = {
    "url": "https://opendata.aemet.es/",
    "auth_required": True,
    "auth_note": "AEMET and KNMI require free API keys; MeteoSwiss and Météo-France (ODS mirror) need no auth",
    "update_frequency": "10 min – 1 hr (varies by service)",
    "coverage": "Spain, Switzerland, France, Netherlands",
    "params": [
        "temperature", "humidity", "wind_speed", "wind_direction",
        "pressure", "precipitation", "wind_gust",
    ],
    "category": "weather",
    "station_count": 1170,
    "services": {
        "aemet": {"country": "Spain", "auth": True, "stations": 800},
        "meteoswiss": {"country": "Switzerland", "auth": False, "stations": 260},
        "meteofrance": {"country": "France", "auth": False, "stations": 60},
        "knmi": {"country": "Netherlands", "auth": True, "stations": 50},
    },
}

# ---------------------------------------------------------------------------
# Router
# ---------------------------------------------------------------------------

router = APIRouter(prefix="/api/europe_west", tags=["Western Europe"])

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# AEMET (Spain)
AEMET_OBS_URL = "https://opendata.aemet.es/opendata/api/observacion/convencional/todas"

# MeteoSwiss (Switzerland)
METEOSWISS_STAC_URL = (
    "https://data.geo.admin.ch/api/stac/v1/collections/"
    "ch.meteoschweiz.ogd-smn/items"
)

# Météo-France via OpenDataSoft
METEOFRANCE_ODS_URL = (
    "https://public.opendatasoft.com/api/records/1.0/search/"
)
METEOFRANCE_DATASET = "donnees-synop-essentielles-omm"

# KNMI (Netherlands)
KNMI_DATASET_URL = (
    "https://api.dataplatform.knmi.nl/open-data/v1/datasets/"
    "Actuele10mindataKNMIstations/versions/2/files"
)

# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------

_HEADERS = {
    "User-Agent": config.USER_AGENT,
    "Accept": "application/json",
}


def _client(**kwargs: Any) -> httpx.AsyncClient:
    return httpx.AsyncClient(
        headers=_HEADERS,
        timeout=config.HTTP_TIMEOUT,
        follow_redirects=True,
        **kwargs,
    )


def _require_key(key_value: str, key_name: str, service: str) -> None:
    """Raise 503 if an API key is not configured."""
    if not key_value:
        raise HTTPException(
            status_code=503,
            detail=(
                f"{service} requires an API key. "
                f"Set the {key_name} environment variable to enable this endpoint."
            ),
        )


def _safe_float(val: Any) -> Optional[float]:
    if val is None or val == "":
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


# =========================================================================
# 1. AEMET (Spain) — two-step JSON fetch
# =========================================================================

def _parse_aemet_timestamp(item: dict) -> Optional[datetime]:
    """Parse AEMET 'fint' ISO timestamp."""
    fint = item.get("fint")
    if not fint:
        return None
    try:
        return datetime.fromisoformat(fint.replace("Z", "+00:00"))
    except (ValueError, TypeError):
        return None


def _parse_aemet_station(item: dict) -> StationDetail:
    station_id = item.get("idema", "")
    name = item.get("ubi", station_id)
    ts = _parse_aemet_timestamp(item)
    lat = _safe_float(item.get("lat"))
    lon = _safe_float(item.get("lon"))
    alt = _safe_float(item.get("alt"))

    station = Station(
        id=f"aemet_{station_id}",
        name=name,
        lat=lat or 0.0,
        lon=lon or 0.0,
        source="europe_west_aemet",
        network="AEMET",
        url=f"https://opendata.aemet.es/opendata/api/observacion/convencional/datos/estacion/{station_id}",
        last_update=ts,
        extra={"altitude_m": alt} if alt is not None else {},
    )

    # AEMET field -> (parameter, unit)
    field_map = {
        "ta":   ("temperature", "°C"),
        "hr":   ("humidity", "%"),
        "vv":   ("wind_speed", "m/s"),
        "dv":   ("wind_direction", "°"),
        "pres": ("pressure", "hPa"),
        "prec": ("precipitation", "mm"),
        "vmax": ("wind_gust", "m/s"),
        "tamin": ("temperature_min", "°C"),
        "tamax": ("temperature_max", "°C"),
    }

    readings: list[Reading] = []
    for field, (param, unit) in field_map.items():
        val = _safe_float(item.get(field))
        if val is not None and ts is not None:
            readings.append(Reading(
                station_id=f"aemet_{station_id}",
                timestamp=ts,
                parameter=param,
                value=val,
                unit=unit,
                source="europe_west_aemet",
            ))

    return StationDetail(station=station, readings=readings)


@router.get("/spain/current", summary="All AEMET stations (Spain)")
async def get_spain_current():
    """Current observations from all ~800 AEMET stations.

    Uses AEMET's two-step API: first request returns a URL containing the
    actual data, which is then fetched in a second request.
    """
    _require_key(config.AEMET_API_KEY, "AEMET_API_KEY", "AEMET (Spain)")

    cached = await cache.get("europe_west:spain:current")
    if cached is not None:
        return cached

    async with _client() as client:
        # Step 1: get the data URL
        resp1 = await client.get(
            AEMET_OBS_URL,
            params={"api_key": config.AEMET_API_KEY},
        )
        if resp1.status_code != 200:
            raise HTTPException(502, f"AEMET API returned {resp1.status_code}")
        meta = resp1.json()
        datos_url = meta.get("datos")
        if not datos_url:
            raise HTTPException(502, "AEMET response missing 'datos' URL")

        # Step 2: fetch actual observation data
        resp2 = await client.get(datos_url)
        if resp2.status_code != 200:
            raise HTTPException(502, f"AEMET data URL returned {resp2.status_code}")
        data = resp2.json()

    if not isinstance(data, list):
        raise HTTPException(502, "Unexpected AEMET response format")

    results = [_parse_aemet_station(item).model_dump() for item in data]
    await cache.set("europe_west:spain:current", results, ttl=config.CACHE_TTL)
    return results


# =========================================================================
# 2. MeteoSwiss (Switzerland) — STAC catalogue → CSV
# =========================================================================

def _parse_meteoswiss_csv(text: str) -> list[StationDetail]:
    """Parse MeteoSwiss SMN CSV into StationDetail objects."""
    results: list[StationDetail] = []
    reader = csv.DictReader(io.StringIO(text), delimiter=";")

    for row in reader:
        station_id = row.get("Station/Location", "").strip()
        if not station_id:
            continue

        ts_str = row.get("Date", "")
        ts: Optional[datetime] = None
        if ts_str:
            try:
                ts = datetime.strptime(ts_str.strip(), "%Y%m%d%H%M").replace(
                    tzinfo=timezone.utc
                )
            except ValueError:
                pass

        station = Station(
            id=f"meteoswiss_{station_id}",
            name=station_id,
            lat=0.0,
            lon=0.0,
            source="europe_west_meteoswiss",
            network="MeteoSwiss SMN",
            last_update=ts,
        )

        field_map = {
            "tre200s0": ("temperature", "°C"),
            "ure200s0": ("humidity", "%"),
            "fu3010z0": ("wind_speed", "m/s"),
            "dkl010z0": ("wind_direction", "°"),
            "prestas0": ("pressure", "hPa"),
            "rre150z0": ("precipitation", "mm"),
            "fkl010z1": ("wind_gust", "m/s"),
        }

        readings: list[Reading] = []
        for field, (param, unit) in field_map.items():
            val = _safe_float(row.get(field, "").strip())
            if val is not None and ts is not None:
                readings.append(Reading(
                    station_id=f"meteoswiss_{station_id}",
                    timestamp=ts,
                    parameter=param,
                    value=val,
                    unit=unit,
                    source="europe_west_meteoswiss",
                ))

        results.append(StationDetail(station=station, readings=readings))

    return results


@router.get("/switzerland/current", summary="MeteoSwiss latest (Switzerland)")
async def get_switzerland_current():
    """Current observations from ~260 MeteoSwiss automatic stations.

    Fetches the STAC catalogue to find the latest measurement CSV, then
    parses it.  No authentication required.
    """
    cached = await cache.get("europe_west:switzerland:current")
    if cached is not None:
        return cached

    async with _client() as client:
        # Query STAC items, sorted by newest first
        resp = await client.get(
            METEOSWISS_STAC_URL,
            params={"limit": 1, "sortby": "-datetime"},
        )
        if resp.status_code != 200:
            raise HTTPException(502, f"MeteoSwiss STAC returned {resp.status_code}")
        stac = resp.json()

        features = stac.get("features", [])
        if not features:
            raise HTTPException(502, "No items found in MeteoSwiss STAC catalogue")

        # Find the CSV asset URL
        assets = features[0].get("assets", {})
        csv_url: Optional[str] = None
        for _key, asset in assets.items():
            href = asset.get("href", "")
            if href.endswith(".csv"):
                csv_url = href
                break

        if not csv_url:
            raise HTTPException(502, "No CSV asset found in MeteoSwiss STAC item")

        # Fetch CSV
        csv_resp = await client.get(csv_url)
        if csv_resp.status_code != 200:
            raise HTTPException(502, f"MeteoSwiss CSV fetch returned {csv_resp.status_code}")

    results = [sd.model_dump() for sd in _parse_meteoswiss_csv(csv_resp.text)]
    await cache.set("europe_west:switzerland:current", results, ttl=config.CACHE_TTL)
    return results


# =========================================================================
# 3. Météo-France — via OpenDataSoft (no auth)
# =========================================================================

def _parse_meteofrance_record(rec: dict) -> StationDetail:
    """Parse a single OpenDataSoft record for Météo-France SYNOP."""
    fields = rec.get("fields", {})
    station_id = fields.get("numer_sta", "")
    name = fields.get("nom", station_id)
    lat = _safe_float(fields.get("latitude"))
    lon = _safe_float(fields.get("longitude"))
    alt = _safe_float(fields.get("altitude"))

    ts: Optional[datetime] = None
    date_str = fields.get("date")
    if date_str:
        try:
            ts = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
        except (ValueError, TypeError):
            pass

    station = Station(
        id=f"meteofrance_{station_id}",
        name=name,
        lat=lat or 0.0,
        lon=lon or 0.0,
        source="europe_west_meteofrance",
        network="Météo-France SYNOP",
        last_update=ts,
        extra={"altitude_m": alt} if alt is not None else {},
    )

    # OpenDataSoft field -> (parameter, unit)
    field_map = {
        "t":    ("temperature", "K"),       # SYNOP reports in Kelvin
        "u":    ("humidity", "%"),
        "ff":   ("wind_speed", "m/s"),
        "dd":   ("wind_direction", "°"),
        "pmer": ("pressure_msl", "Pa"),     # SYNOP reports in Pa
        "rr1":  ("precipitation_1h", "mm"),
        "rafper": ("wind_gust", "m/s"),
    }

    readings: list[Reading] = []
    for field, (param, unit) in field_map.items():
        val = _safe_float(fields.get(field))
        if val is not None and ts is not None:
            readings.append(Reading(
                station_id=f"meteofrance_{station_id}",
                timestamp=ts,
                parameter=param,
                value=val,
                unit=unit,
                source="europe_west_meteofrance",
            ))

    return StationDetail(station=station, readings=readings)


@router.get("/france/current", summary="Météo-France SYNOP (France)")
async def get_france_current():
    """Current SYNOP observations from ~60 French stations.

    Uses the OpenDataSoft public mirror — no authentication required.
    """
    cached = await cache.get("europe_west:france:current")
    if cached is not None:
        return cached

    async with _client() as client:
        resp = await client.get(
            METEOFRANCE_ODS_URL,
            params={
                "dataset": METEOFRANCE_DATASET,
                "rows": 100,
                "sort": "date",
            },
        )
        if resp.status_code != 200:
            raise HTTPException(502, f"OpenDataSoft API returned {resp.status_code}")
        data = resp.json()

    records = data.get("records", [])
    results = [_parse_meteofrance_record(r).model_dump() for r in records]
    await cache.set("europe_west:france:current", results, ttl=config.CACHE_TTL)
    return results


# =========================================================================
# 4. KNMI (Netherlands) — latest file listing
# =========================================================================

@router.get("/netherlands/latest", summary="KNMI latest file listing (Netherlands)")
async def get_netherlands_latest():
    """List the latest 10-minute observation data files from KNMI.

    Returns the file listing (names + timestamps) from the KNMI Open Data
    platform.  Each file covers all ~50 stations for one 10-minute interval.
    Requires ``KNMI_API_KEY`` environment variable.
    """
    _require_key(config.KNMI_API_KEY, "KNMI_API_KEY", "KNMI (Netherlands)")

    cached = await cache.get("europe_west:netherlands:latest")
    if cached is not None:
        return cached

    async with _client() as client:
        resp = await client.get(
            KNMI_DATASET_URL,
            headers={
                **_HEADERS,
                "Authorization": config.KNMI_API_KEY,
            },
            params={
                "maxKeys": 10,
                "orderBy": "lastModified",
                "sorting": "desc",
            },
        )
        if resp.status_code == 403:
            raise HTTPException(502, "KNMI API key rejected (403 Forbidden)")
        if resp.status_code != 200:
            raise HTTPException(502, f"KNMI API returned {resp.status_code}")
        data = resp.json()

    files = data.get("files", [])
    results = {
        "source": "KNMI",
        "dataset": "Actuele10mindataKNMIstations",
        "file_count": len(files),
        "files": [
            {
                "filename": f.get("filename"),
                "last_modified": f.get("lastModified"),
                "size": f.get("size"),
            }
            for f in files
        ],
    }
    await cache.set("europe_west:netherlands:latest", results, ttl=config.CACHE_TTL)
    return results


# =========================================================================
# 5. Info endpoint
# =========================================================================

@router.get("/info", summary="Source information")
async def europe_west_info():
    """Return information about the Western European Weather data source,
    including API key configuration status for services that require one."""
    return {
        "source": SOURCE_NAME,
        "description": SOURCE_DESCRIPTION,
        "services": {
            "aemet": {
                "country": "Spain",
                "stations": "~800",
                "auth_required": True,
                "key_configured": bool(config.AEMET_API_KEY),
                "api_docs": "https://opendata.aemet.es/dist/index.html",
            },
            "meteoswiss": {
                "country": "Switzerland",
                "stations": "~260",
                "auth_required": False,
                "api_docs": "https://data.geo.admin.ch/",
            },
            "meteofrance": {
                "country": "France",
                "stations": "~60 SYNOP",
                "auth_required": False,
                "note": "Uses OpenDataSoft public mirror",
                "api_docs": "https://public.opendatasoft.com/explore/dataset/donnees-synop-essentielles-omm/",
            },
            "knmi": {
                "country": "Netherlands",
                "stations": "~50",
                "auth_required": True,
                "key_configured": bool(config.KNMI_API_KEY),
                "api_docs": "https://developer.dataplatform.knmi.nl/",
            },
        },
        **SOURCE_META,
    }
