"""Colombia IDEAM weather stations via Socrata (datos.gov.co).

Colombia's Institute of Hydrology, Meteorology and Environmental Studies (IDEAM)
publishes a station catalog of ~2,800 meteorological and hydrological stations
through the national open-data portal (datos.gov.co) using the Socrata API.

No authentication is required for basic queries.  The API supports SoQL
(Socrata Query Language) for filtering, pagination, and sorting.

API reference: https://dev.socrata.com/docs/queries/
Dataset:       https://www.datos.gov.co/resource/hp9r-jxuu.json
"""

import logging
from typing import Optional, Any

import httpx
from fastapi import APIRouter, HTTPException, Query

import cache
import config
from models import Station, StationDetail

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Source registry metadata  (read by server.py /api/sources)
# ---------------------------------------------------------------------------

SOURCE_NAME = "Colombia IDEAM"
SOURCE_DESCRIPTION = (
    "Station catalog from Colombia's Institute of Hydrology, Meteorology and "
    "Environmental Studies (IDEAM), served via the national open-data portal "
    "(datos.gov.co / Socrata).  Approximately 2,800 meteorological, hydrological, "
    "and environmental monitoring stations.  No authentication required."
)
SOURCE_META = {
    "url": "https://www.datos.gov.co/resource/hp9r-jxuu.json",
    "auth_required": False,
    "update_frequency": "catalog updated periodically",
    "coverage": "Colombia",
    "params": [
        "temperature", "precipitation", "humidity", "wind_speed",
        "wind_direction", "pressure", "solar_radiation", "evaporation",
        "streamflow", "water_level",
    ],
    "category": "weather",
    "station_count": 2800,
}

router = APIRouter(prefix="/api/colombia", tags=["Colombia"])

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

SOCRATA_BASE = "https://www.datos.gov.co/resource/hp9r-jxuu.json"

_HEADERS = {
    "User-Agent": config.USER_AGENT,
    "Accept": "application/json",
}

# Map of known IDEAM station categories to human-readable parameter lists
_CATEGORY_PARAMS: dict[str, list[str]] = {
    "Climatológica Ordinaria":  ["temperature", "precipitation", "humidity"],
    "Climatológica Principal":  ["temperature", "precipitation", "humidity", "wind_speed", "wind_direction", "pressure", "solar_radiation"],
    "Pluviométrica":            ["precipitation"],
    "Sinóptica Principal":      ["temperature", "precipitation", "humidity", "wind_speed", "wind_direction", "pressure"],
    "Sinóptica Secundaria":     ["temperature", "precipitation", "humidity", "wind_speed", "wind_direction"],
    "Agrometeorológica":        ["temperature", "precipitation", "humidity", "solar_radiation", "evaporation"],
    "Limnimétrica":             ["water_level"],
    "Limnigráfica":             ["water_level", "streamflow"],
    "Hidrológica":              ["water_level", "streamflow"],
    "Mareográfica":             ["water_level"],
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


async def _socrata_get(params: dict[str, Any]) -> list[dict]:
    """Query the Socrata dataset; return the JSON array of records."""
    try:
        async with _client() as c:
            resp = await c.get(SOCRATA_BASE, params=params)
            resp.raise_for_status()
            return resp.json()
    except httpx.TimeoutException:
        raise HTTPException(status_code=504, detail="datos.gov.co API timeout")
    except httpx.HTTPStatusError as exc:
        raise HTTPException(
            status_code=exc.response.status_code,
            detail=f"datos.gov.co error: {exc}",
        )
    except Exception as exc:
        logger.exception("Error querying datos.gov.co")
        raise HTTPException(status_code=502, detail=str(exc))


# ---------------------------------------------------------------------------
# Parsing helpers
# ---------------------------------------------------------------------------

def _safe_float(val: Any, default: float = 0.0) -> float:
    """Convert a value to float, returning *default* on failure."""
    if val is None:
        return default
    try:
        return float(val)
    except (TypeError, ValueError):
        return default


def _parse_station(raw: dict) -> Station:
    """Convert a Socrata IDEAM station record to the shared Station model."""
    # The Socrata dataset uses Spanish field names
    station_id = str(
        raw.get("codigo")
        or raw.get("codigoestacion")
        or raw.get("objectid")
        or ""
    )

    name = raw.get("nombre", raw.get("nombreestacion", station_id))
    lat = _safe_float(raw.get("latitud"))
    lon = _safe_float(raw.get("longitud"))
    elevation = _safe_float(raw.get("altitud")) if raw.get("altitud") is not None else None

    departamento = raw.get("departamento", "")
    municipio = raw.get("municipio", "")
    categoria = raw.get("categoria", "")
    estado = raw.get("estado", "")
    tecnologia = raw.get("tecnologia", "")

    # Derive parameter list from station category
    params = _CATEGORY_PARAMS.get(categoria, [])

    network = departamento if departamento else "IDEAM"

    return Station(
        id=station_id,
        name=name or station_id,
        lat=lat,
        lon=lon,
        source="colombia_ideam",
        network=network,
        params=params if params else ["unknown"],
        elevation=elevation,
        url=f"https://www.datos.gov.co/resource/hp9r-jxuu.json?codigo={station_id}",
        extra={
            "departamento": departamento,
            "municipio": municipio,
            "categoria": categoria,
            "estado": estado,
            "tecnologia": tecnologia,
        },
    )


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@router.get("/stations", summary="Colombia IDEAM — station catalog")
async def stations(
    state: Optional[str] = Query(
        None,
        description="Filter by station state: 'active' or 'inactive' (maps to estado='Activa'/'Suspendida')",
    ),
    departamento: Optional[str] = Query(
        None,
        description="Filter by Colombian department (e.g., 'ANTIOQUIA', 'CUNDINAMARCA')",
    ),
    categoria: Optional[str] = Query(
        None,
        description="Filter by station category (e.g., 'Climatológica Ordinaria', 'Pluviométrica')",
    ),
    limit: int = Query(500, ge=1, le=5000, description="Max number of stations to return"),
    offset: int = Query(0, ge=0, description="Pagination offset"),
):
    """Return IDEAM station catalog with optional filtering.

    Supports filtering by state (active/inactive), department, and category.
    Results are paginated using limit/offset.
    """
    cache_key = f"colombia:stations:{state}:{departamento}:{categoria}:{limit}:{offset}"
    cached = await cache.get(cache_key)
    if cached is not None:
        return cached

    # Build SoQL $where clauses
    where_parts: list[str] = []
    if state:
        estado_val = "Activa" if state.lower() == "active" else "Suspendida"
        where_parts.append(f"estado='{estado_val}'")
    if departamento:
        where_parts.append(f"departamento='{departamento.upper()}'")
    if categoria:
        where_parts.append(f"categoria='{categoria}'")

    params: dict[str, Any] = {
        "$limit": limit,
        "$offset": offset,
        "$order": "codigo ASC",
    }
    if where_parts:
        params["$where"] = " AND ".join(where_parts)

    rows = await _socrata_get(params)

    station_list = [_parse_station(row).model_dump() for row in rows]
    result = {
        "source": "colombia_ideam",
        "count": len(station_list),
        "limit": limit,
        "offset": offset,
        "stations": station_list,
    }
    await cache.set(cache_key, result, ttl=config.CACHE_TTL_LONG)
    return result


@router.get("/stations/{station_id}", summary="Colombia IDEAM — single station detail")
async def station_detail(station_id: str):
    """Return metadata for a single IDEAM station by its station code."""
    cache_key = f"colombia:station:{station_id}"
    cached = await cache.get(cache_key)
    if cached is not None:
        return cached

    rows = await _socrata_get({"codigo": station_id, "$limit": 1})

    if not rows:
        raise HTTPException(
            status_code=404,
            detail=f"Station '{station_id}' not found in IDEAM catalog",
        )

    station = _parse_station(rows[0])
    result = station.model_dump()
    await cache.set(cache_key, result, ttl=config.CACHE_TTL_LONG)
    return result


@router.get("/overview", summary="Colombia IDEAM — source overview")
async def overview():
    """Return summary information about the Colombia IDEAM data source."""
    return {
        "module": SOURCE_NAME,
        "description": SOURCE_DESCRIPTION,
        "sources": [
            {
                "id": "colombia_ideam",
                "name": "Colombia IDEAM",
                "description": (
                    "Station catalog from Colombia's IDEAM — ~2,800 meteorological, "
                    "hydrological, and environmental monitoring stations across all "
                    "32 departments.  Data served via Socrata (datos.gov.co)."
                ),
                "url": "https://www.datos.gov.co/resource/hp9r-jxuu.json",
                "auth_required": False,
                "update_frequency": "catalog updated periodically",
                "coverage": "Colombia (all 32 departments)",
                "station_count": "~2,800",
                "params": SOURCE_META["params"],
                "endpoints": [
                    "GET /api/colombia/stations?state=active&limit=500",
                    "GET /api/colombia/stations/{station_id}",
                    "GET /api/colombia/overview",
                ],
            }
        ],
    }
