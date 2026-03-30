"""Brazil ANA Hidroweb + GEOGloWS ECMWF Streamflow — hydrology data.

Two data sources:

1. **Brazil ANA Hidroweb** — telemetric hydrological stations from Brazil's
   Agência Nacional de Águas e Saneamento Básico (ANA).  ~4,600 stations
   covering river levels, discharge, and precipitation across the Amazon,
   Paraná, and São Francisco basins.  Free registration; JSON/CSV responses.

   - Station info:  https://www.snirh.gov.br/hidroweb/rest/api/estacaotelemetrica?id={id}
   - Telemetric data: https://www.snirh.gov.br/hidroweb/rest/api/documento/gerarTelemetricas
   - SOAP station list: https://telemetriaws1.ana.gov.br/ServiceANA.asmx

2. **GEOGloWS ECMWF Streamflow** — global river forecasts from the GEOGloWS
   ECMWF Streamflow Service.  15-day ensemble forecasts and 40-year historical
   simulation for every river worldwide.  No authentication required; JSON/CSV.

   - Forecast stats: https://geoglows.ecmwf.int/api/ForecastStats/?reach_id=XXXXX
"""

import logging
from datetime import datetime, timezone
from typing import Any, Optional

import httpx
from fastapi import APIRouter, HTTPException, Query

import cache
import config

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Source registry metadata  (read by server.py /api/sources)
# ---------------------------------------------------------------------------

SOURCE_NAME = "Brazil Hydrology"
SOURCE_DESCRIPTION = (
    "Hydrological data from Brazil's ANA Hidroweb (~4,600 telemetric stations "
    "covering river levels, discharge, and precipitation in the Amazon, Paraná, "
    "and São Francisco basins) and GEOGloWS ECMWF Streamflow (15-day ensemble "
    "forecasts for every river globally).  ANA requires free registration; "
    "GEOGloWS requires no authentication."
)
SOURCE_META = {
    "url": "https://www.snirh.gov.br/hidroweb/",
    "auth_required": False,
    "update_frequency": "15 min (ANA telemetric), 12 hr (GEOGloWS)",
    "coverage": "Brazil (ANA) / Global rivers (GEOGloWS)",
    "params": [
        "water_level", "discharge", "precipitation",
        "streamflow_forecast",
    ],
    "category": "hydrology",
    "station_count": 4600,
}

router = APIRouter(prefix="/api/brazil_hydro", tags=["Brazil Hydrology"])

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_ANA_BASE = "https://www.snirh.gov.br/hidroweb/rest/api"
_ANA_STATION_URL = f"{_ANA_BASE}/estacaotelemetrica"
_ANA_DATA_URL = f"{_ANA_BASE}/documento/gerarTelemetricas"

_GEOGLOWS_BASE = "https://geoglows.ecmwf.int/api"

_HEADERS = {
    "User-Agent": config.USER_AGENT,
    "Accept": "application/json",
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


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


async def _ana_get(url: str, params: dict[str, Any] | None = None) -> Any:
    """Fetch JSON from the ANA Hidroweb REST API."""
    try:
        async with _client() as c:
            resp = await c.get(url, params=params)
            resp.raise_for_status()
            return resp.json()
    except httpx.TimeoutException:
        raise HTTPException(status_code=504, detail="ANA Hidroweb API timeout")
    except httpx.HTTPStatusError as exc:
        raise HTTPException(
            status_code=exc.response.status_code,
            detail=f"ANA Hidroweb error: {exc}",
        )
    except Exception as exc:
        logger.exception("Error querying ANA Hidroweb")
        raise HTTPException(status_code=502, detail=str(exc))


async def _geoglows_get(endpoint: str, params: dict[str, Any] | None = None) -> Any:
    """Fetch JSON from the GEOGloWS ECMWF Streamflow API."""
    url = f"{_GEOGLOWS_BASE}/{endpoint}"
    try:
        async with _client() as c:
            resp = await c.get(url, params=params)
            resp.raise_for_status()
            ct = resp.headers.get("content-type", "")
            if "text/csv" in ct or "text/plain" in ct:
                return _parse_geoglows_csv(resp.text)
            return resp.json()
    except httpx.TimeoutException:
        raise HTTPException(status_code=504, detail="GEOGloWS API timeout")
    except httpx.HTTPStatusError as exc:
        raise HTTPException(
            status_code=exc.response.status_code,
            detail=f"GEOGloWS error: {exc}",
        )
    except Exception as exc:
        logger.exception("Error querying GEOGloWS")
        raise HTTPException(status_code=502, detail=str(exc))


# ---------------------------------------------------------------------------
# Parsing helpers
# ---------------------------------------------------------------------------

def _safe_float(val: Any, default: float | None = None) -> float | None:
    """Convert a value to float, returning *default* on failure."""
    if val is None:
        return default
    try:
        return float(val)
    except (TypeError, ValueError):
        return default


def _parse_geoglows_csv(text: str) -> dict:
    """Parse GEOGloWS CSV response into a JSON-friendly structure.

    The CSV typically has a datetime index column and statistical columns
    (flow_avg, flow_max, flow_min, high_res, etc.).
    """
    lines = text.strip().splitlines()
    if len(lines) < 2:
        return {"columns": [], "data": []}

    headers = [h.strip() for h in lines[0].split(",")]
    rows = []
    for line in lines[1:]:
        if not line.strip():
            continue
        parts = [p.strip() for p in line.split(",")]
        row = {}
        for i, h in enumerate(headers):
            val = parts[i] if i < len(parts) else ""
            row[h] = val
        rows.append(row)

    return {"columns": headers, "data": rows}


def _parse_ana_station(raw: dict) -> dict:
    """Normalize an ANA telemetric station record."""
    return {
        "id": str(raw.get("Codigo", raw.get("codigo", ""))),
        "name": raw.get("Nome", raw.get("nome", "")),
        "lat": _safe_float(raw.get("Latitude", raw.get("latitude"))),
        "lon": _safe_float(raw.get("Longitude", raw.get("longitude"))),
        "state": raw.get("Estado", raw.get("estado", "")),
        "municipality": raw.get("Municipio", raw.get("municipio", "")),
        "river": raw.get("Rio", raw.get("rio", "")),
        "basin": raw.get("Bacia", raw.get("bacia", "")),
        "sub_basin": raw.get("SubBacia", raw.get("subbacia", "")),
        "operator": raw.get("Operadora", raw.get("operadora", "")),
        "responsible": raw.get("Responsavel", raw.get("responsavel", "")),
        "type": raw.get("Tipo", raw.get("tipo", "")),
        "status": raw.get("Situacao", raw.get("situacao", "")),
        "source": "brazil_ana",
    }


def _parse_ana_readings(raw: Any, station_id: str) -> list[dict]:
    """Normalize ANA telemetric reading records.

    The API can return either a list of records or a nested structure.
    Each record typically contains timestamp, nivel (water level),
    vazao (discharge), and chuva (precipitation).
    """
    records: list[dict] = []
    if isinstance(raw, dict):
        # May be nested under a key
        for v in raw.values():
            if isinstance(v, list):
                raw = v
                break
        else:
            return records

    if not isinstance(raw, list):
        return records

    for item in raw:
        ts_raw = item.get("DataHora", item.get("dataHora", ""))
        if not ts_raw:
            continue

        try:
            ts = datetime.fromisoformat(str(ts_raw).replace("Z", "+00:00"))
        except (ValueError, TypeError):
            ts_str = str(ts_raw)
            try:
                ts = datetime.strptime(ts_str[:19], "%Y-%m-%dT%H:%M:%S").replace(
                    tzinfo=timezone.utc
                )
            except ValueError:
                continue

        entry: dict[str, Any] = {
            "station_id": station_id,
            "timestamp": ts.isoformat(),
        }

        # Water level (nivel) — meters
        nivel = _safe_float(item.get("Nivel", item.get("nivel")))
        if nivel is not None:
            entry["water_level_m"] = nivel

        # Discharge (vazao) — m³/s
        vazao = _safe_float(item.get("Vazao", item.get("vazao")))
        if vazao is not None:
            entry["discharge_m3s"] = vazao

        # Precipitation (chuva) — mm
        chuva = _safe_float(item.get("Chuva", item.get("chuva")))
        if chuva is not None:
            entry["precipitation_mm"] = chuva

        records.append(entry)

    return records


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@router.get(
    "/stations/{station_id}",
    summary="ANA telemetric station info",
)
async def get_station(station_id: str):
    """Return metadata for a single ANA telemetric station.

    Example station IDs: 5669600, 19314239, 66600000.
    """
    cache_key = f"brazil_ana:station:{station_id}"
    cached = await cache.get(cache_key)
    if cached is not None:
        return cached

    data = await _ana_get(_ANA_STATION_URL, params={"id": station_id})

    if not data:
        raise HTTPException(status_code=404, detail=f"Station {station_id} not found")

    # API may return a single object or a list
    raw = data[0] if isinstance(data, list) else data
    result = _parse_ana_station(raw)

    await cache.set(cache_key, result, ttl=config.CACHE_TTL_LONG)
    return result


@router.get(
    "/readings/{station_id}",
    summary="ANA telemetric readings",
)
async def get_readings(
    station_id: str,
    start: str = Query(
        ...,
        description="Start date/time (ISO 8601, e.g. 2026-03-01 or 2026-03-01T00:00:00Z)",
    ),
    end: str = Query(
        ...,
        description="End date/time (ISO 8601, e.g. 2026-03-18 or 2026-03-18T23:59:59Z)",
    ),
):
    """Return telemetric readings (water level, discharge, precipitation) for a station.

    Time range is limited by ANA's data retention.  Typical telemetric data
    is available for the past ~90 days.
    """
    # Normalize dates to the format ANA expects
    try:
        dt_start = datetime.fromisoformat(start.replace("Z", "+00:00"))
    except ValueError:
        dt_start = datetime.strptime(start[:10], "%Y-%m-%d").replace(tzinfo=timezone.utc)

    try:
        dt_end = datetime.fromisoformat(end.replace("Z", "+00:00"))
    except ValueError:
        dt_end = datetime.strptime(end[:10], "%Y-%m-%d").replace(tzinfo=timezone.utc)

    # ANA expects ISO timestamps with milliseconds
    fmt_start = dt_start.strftime("%Y-%m-%dT%H:%M:%S.000Z")
    fmt_end = dt_end.strftime("%Y-%m-%dT%H:%M:%S.000Z")

    cache_key = f"brazil_ana:readings:{station_id}:{start}:{end}"
    cached = await cache.get(cache_key)
    if cached is not None:
        return cached

    params = {
        "codigosEstacoes": station_id,
        "tipoArquivo": "2",  # JSON
        "periodoInicial": fmt_start,
        "periodoFinal": fmt_end,
    }

    data = await _ana_get(_ANA_DATA_URL, params=params)
    readings = _parse_ana_readings(data, station_id)

    result = {
        "station_id": station_id,
        "start": fmt_start,
        "end": fmt_end,
        "count": len(readings),
        "readings": readings,
    }

    await cache.set(cache_key, result, ttl=config.CACHE_TTL)
    return result


@router.get(
    "/geoglows/forecast",
    summary="GEOGloWS ECMWF streamflow forecast",
)
async def geoglows_forecast(
    reach_id: str = Query(
        ...,
        description="GEOGloWS reach ID (numeric string identifying a river segment)",
    ),
    return_format: str = Query(
        "json",
        description="Response format: 'json' (default) or 'csv'",
    ),
):
    """Return a 15-day ensemble streamflow forecast from GEOGloWS ECMWF.

    The response includes statistical flow metrics (mean, max, min, high-res)
    for the specified river reach.  Reach IDs can be looked up from the
    GEOGloWS web viewer: https://geoglows.ecmwf.int/
    """
    cache_key = f"geoglows:forecast:{reach_id}"
    cached = await cache.get(cache_key)
    if cached is not None:
        return cached

    fmt = "csv" if return_format == "csv" else "json"
    params: dict[str, str] = {
        "reach_id": reach_id,
        "return_format": fmt,
    }

    data = await _geoglows_get("ForecastStats/", params=params)

    result = {
        "reach_id": reach_id,
        "source": "geoglows",
        "forecast": data,
        "retrieved_at": _now_utc().isoformat(),
    }

    await cache.set(cache_key, result, ttl=config.CACHE_TTL)
    return result


@router.get("/overview", summary="Brazil Hydrology — source overview")
async def overview():
    """Return summary information about the Brazil Hydrology data sources."""
    return {
        "module": SOURCE_NAME,
        "description": SOURCE_DESCRIPTION,
        "sources": [
            {
                "id": "brazil_ana",
                "name": "Brazil ANA Hidroweb",
                "description": (
                    "Telemetric hydrological stations from Brazil's Agência Nacional "
                    "de Águas (ANA).  ~4,600 stations reporting river levels, "
                    "discharge, and precipitation across the Amazon, Paraná, and "
                    "São Francisco basins."
                ),
                "url": "https://www.snirh.gov.br/hidroweb/",
                "auth_required": False,
                "update_frequency": "15 min (telemetric)",
                "coverage": "Brazil — major river basins",
                "station_count": "~4,600",
                "params": ["water_level", "discharge", "precipitation"],
                "endpoints": [
                    "GET /api/brazil_hydro/stations/{station_id}",
                    "GET /api/brazil_hydro/readings/{station_id}?start=...&end=...",
                ],
            },
            {
                "id": "geoglows",
                "name": "GEOGloWS ECMWF Streamflow",
                "description": (
                    "15-day ensemble streamflow forecasts and 40-year historical "
                    "simulation for every river globally, powered by ECMWF ERA-5 "
                    "and GFS forcing.  No authentication required."
                ),
                "url": "https://geoglows.ecmwf.int/",
                "auth_required": False,
                "update_frequency": "12 hr",
                "coverage": "Global (all rivers)",
                "params": ["streamflow_forecast"],
                "endpoints": [
                    "GET /api/brazil_hydro/geoglows/forecast?reach_id=XXXXX",
                ],
            },
        ],
    }
