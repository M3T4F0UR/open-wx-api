"""Bureau of Meteorology (BoM) Australia — real-time weather observations.

Provides temperature, humidity, wind, pressure, rainfall, cloud, and
visibility data from ~700 automatic weather stations across Australia.

No authentication required.  Data is served as JSON feeds from:
    http://www.bom.gov.au/fwo/{StateProductCode}/{StateProductCode}.{WMO_ID}.json

API reference: http://www.bom.gov.au/catalogue/data-feeds.shtml
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
SOURCE_NAME = "Australia BoM"
SOURCE_DESCRIPTION = (
    "Bureau of Meteorology (BoM) Australia — real-time weather observations "
    "from ~700 automatic weather stations.  Temperature, humidity, wind, "
    "pressure, rainfall, cloud, and visibility."
)
SOURCE_META = {
    "url": "http://www.bom.gov.au/",
    "auth_required": False,
    "update_frequency": "30 min",
    "coverage": "Australia",
    "params": [
        "air_temp", "rel_hum", "wind_speed", "wind_dir", "wind_gust",
        "pressure", "rainfall", "cloud", "visibility",
    ],
    "category": "weather",
}

router = APIRouter(prefix="/api/australia", tags=["Australia BoM"])

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
BOM_BASE = "http://www.bom.gov.au/fwo"

# State/territory product codes used by BoM JSON feeds
STATE_PRODUCT_CODES: dict[str, str] = {
    "NSW": "IDN60901",
    "VIC": "IDV60901",
    "QLD": "IDQ60901",
    "SA":  "IDS60901",
    "WA":  "IDW60901",
    "TAS": "IDT60901",
    "NT":  "IDD60901",
    "ACT": "IDC60901",
}

# Map BoM JSON field names to (parameter_name, unit) pairs
FIELD_MAP: dict[str, tuple[str, str]] = {
    "air_temp":       ("Air Temperature",      "°C"),
    "apparent_t":     ("Apparent Temperature",  "°C"),
    "dewpt":          ("Dew Point",             "°C"),
    "rel_hum":        ("Relative Humidity",     "%"),
    "wind_spd_kmh":   ("Wind Speed",            "km/h"),
    "wind_dir":       ("Wind Direction",        ""),
    "gust_kmh":       ("Wind Gust",             "km/h"),
    "press_msl":      ("Pressure (MSL)",        "hPa"),
    "press_qnh":      ("Pressure (QNH)",        "hPa"),
    "rain_trace":     ("Rainfall (since 9am)",  "mm"),
    "cloud":          ("Cloud",                 ""),
    "vis_km":         ("Visibility",            "km"),
}


# ---------------------------------------------------------------------------
# Helper utilities
# ---------------------------------------------------------------------------

def _build_url(product_code: str, wmo_id: Optional[str] = None) -> str:
    """Build BoM JSON feed URL for a state product code and optional WMO ID."""
    if wmo_id:
        return f"{BOM_BASE}/{product_code}/{product_code}.{wmo_id}.json"
    # Without a WMO ID the product-level feed includes all stations for the state
    return f"{BOM_BASE}/{product_code}/{product_code}.json"


def _parse_timestamp(obs: dict) -> Optional[datetime]:
    """Extract a UTC datetime from a BoM observation record.

    BoM provides ``aifstime_utc`` in the format ``"20260320143000"``
    (YYYYMMDDHHmmss) which we parse into a timezone-aware datetime.
    """
    raw = obs.get("aifstime_utc", "")
    if not raw:
        return None
    try:
        return datetime.strptime(raw, "%Y%m%d%H%M%S").replace(tzinfo=timezone.utc)
    except ValueError:
        return None


def _parse_readings(obs: dict, station_id: str) -> list[Reading]:
    """Convert a single BoM observation dict into a list of Reading objects."""
    ts = _parse_timestamp(obs)
    if ts is None:
        return []

    readings: list[Reading] = []
    for field, (param_name, unit) in FIELD_MAP.items():
        raw_val = obs.get(field)
        if raw_val is None or raw_val == "-":
            continue
        try:
            value = float(raw_val)
        except (TypeError, ValueError):
            # Some fields like wind_dir or cloud are strings — store as-is
            # by converting to a float if possible, otherwise skip numeric reading
            continue

        readings.append(
            Reading(
                station_id=station_id,
                timestamp=ts,
                parameter=param_name,
                value=value,
                unit=unit,
                source="australia_bom",
            )
        )

    # Handle rain_trace specially — it's a string like "0.0" or "Trace"
    rain_raw = obs.get("rain_trace", "")
    if rain_raw and rain_raw not in ("-", ""):
        try:
            rain_val = float(rain_raw)
            # Only add if not already captured by the numeric path above
            if not any(r.parameter == "Rainfall (since 9am)" for r in readings):
                readings.append(
                    Reading(
                        station_id=station_id,
                        timestamp=ts,
                        parameter="Rainfall (since 9am)",
                        value=rain_val,
                        unit="mm",
                        source="australia_bom",
                    )
                )
        except (TypeError, ValueError):
            pass

    return readings


def _parse_station(obs: dict) -> Optional[Station]:
    """Convert a BoM observation dict into a Station model.

    Each observation record contains the station metadata inline.
    """
    wmo_id = obs.get("wmo")
    name = obs.get("name", "")
    lat = obs.get("lat")
    lon = obs.get("lon")

    if not wmo_id or lat is None or lon is None:
        return None

    try:
        lat_f = float(lat)
        lon_f = float(lon)
    except (TypeError, ValueError):
        return None

    # Determine which params are present in this observation
    params: list[str] = []
    for field, (param_name, _) in FIELD_MAP.items():
        val = obs.get(field)
        if val is not None and val != "-":
            if param_name not in params:
                params.append(param_name)

    state = obs.get("state", "")
    ts = _parse_timestamp(obs)

    return Station(
        id=str(wmo_id),
        name=name,
        lat=lat_f,
        lon=lon_f,
        source="australia_bom",
        network=f"BoM {state}" if state else "BoM",
        params=params,
        last_update=ts,
        url=f"http://www.bom.gov.au/products/{STATE_PRODUCT_CODES.get(state, '')}.shtml",
        extra={
            "wmo_id": str(wmo_id),
            "state": state,
            "history_product": obs.get("history_product", ""),
        },
    )


async def _fetch_state_observations(product_code: str) -> list[dict]:
    """Fetch the observation JSON feed for a state and return the data array."""
    url = _build_url(product_code)
    try:
        data = await http_client.fetch_json(url, source="australia_bom")
    except http_client.CircuitOpenError as exc:
        raise HTTPException(status_code=503, detail=str(exc))
    except httpx.TimeoutException:
        raise HTTPException(status_code=504, detail="BoM API timeout")
    except httpx.HTTPStatusError as exc:
        raise HTTPException(
            status_code=exc.response.status_code,
            detail=f"BoM API error: {exc}",
        )
    except Exception as exc:
        logger.exception("Error fetching BoM observations for %s", product_code)
        raise HTTPException(status_code=500, detail=str(exc))

    # BoM JSON structure: {"observations": {"data": [...]}}
    obs_list = data.get("observations", {}).get("data", [])
    return obs_list


async def _fetch_station_observations(product_code: str, wmo_id: str) -> list[dict]:
    """Fetch observations for a single station by WMO ID."""
    url = _build_url(product_code, wmo_id)
    try:
        data = await http_client.fetch_json(url, source="australia_bom")
    except http_client.CircuitOpenError as exc:
        raise HTTPException(status_code=503, detail=str(exc))
    except httpx.TimeoutException:
        raise HTTPException(status_code=504, detail="BoM API timeout")
    except httpx.HTTPStatusError as exc:
        if exc.response.status_code == 404:
            raise HTTPException(
                status_code=404,
                detail=f"BoM station WMO {wmo_id} not found in {product_code}",
            )
        raise HTTPException(
            status_code=exc.response.status_code,
            detail=f"BoM API error: {exc}",
        )
    except Exception as exc:
        logger.exception("Error fetching BoM station %s/%s", product_code, wmo_id)
        raise HTTPException(status_code=500, detail=str(exc))

    return data.get("observations", {}).get("data", [])


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@router.get("/stations", summary="List BoM weather stations for an Australian state")
async def list_stations(
    state: str = Query(
        ...,
        description="Australian state/territory code: NSW, VIC, QLD, SA, WA, TAS, NT, ACT",
        min_length=2,
        max_length=3,
    ),
    limit: int = Query(500, ge=1, le=2000, description="Maximum number of stations"),
):
    """Return weather stations for the given Australian state/territory.

    Fetches the BoM JSON observations feed for the state and extracts unique
    stations from the most recent observation per site.
    """
    state_upper = state.upper()
    product_code = STATE_PRODUCT_CODES.get(state_upper)
    if not product_code:
        raise HTTPException(
            status_code=400,
            detail=f"Unknown state '{state}'. Valid: {', '.join(sorted(STATE_PRODUCT_CODES))}",
        )

    cache_key = f"australia_bom:stations:{state_upper}:{limit}"
    cached = await cache.get(cache_key)
    if cached is not None:
        return cached

    obs_list = await _fetch_state_observations(product_code)

    # Deduplicate stations — keep only the first (most recent) observation per WMO ID
    seen: dict[str, Station] = {}
    for obs in obs_list:
        wmo_id = obs.get("wmo")
        if not wmo_id or str(wmo_id) in seen:
            continue
        station = _parse_station(obs)
        if station:
            seen[station.id] = station

    stations = list(seen.values())[:limit]
    result = {"stations": [s.model_dump() for s in stations], "count": len(stations)}
    await cache.set(cache_key, result, ttl=config.CACHE_TTL_LONG)
    return result


@router.get(
    "/stations/{wmo_id}",
    summary="Single BoM station with latest readings",
)
async def get_station(
    wmo_id: str,
    state: str = Query(
        ...,
        description="State/territory code (needed to select the correct BoM product feed)",
        min_length=2,
        max_length=3,
    ),
):
    """Return metadata and the most recent readings for a BoM station by WMO ID.

    The ``state`` parameter is required because BoM organises feeds by
    state product code.
    """
    state_upper = state.upper()
    product_code = STATE_PRODUCT_CODES.get(state_upper)
    if not product_code:
        raise HTTPException(
            status_code=400,
            detail=f"Unknown state '{state}'. Valid: {', '.join(sorted(STATE_PRODUCT_CODES))}",
        )

    cache_key = f"australia_bom:station:{state_upper}:{wmo_id}"
    cached = await cache.get(cache_key)
    if cached is not None:
        return cached

    obs_list = await _fetch_station_observations(product_code, wmo_id)
    if not obs_list:
        raise HTTPException(status_code=404, detail=f"No observations for WMO {wmo_id}")

    # First record is the most recent observation
    latest_obs = obs_list[0]
    station = _parse_station(latest_obs)
    if not station:
        raise HTTPException(status_code=404, detail=f"Could not parse station WMO {wmo_id}")

    # Parse readings from the most recent observation
    readings = _parse_readings(latest_obs, station.id)

    detail = StationDetail(station=station, readings=readings)
    result = detail.model_dump()
    await cache.set(cache_key, result, ttl=config.CACHE_TTL)
    return result


@router.get("/overview", summary="Available Australian states and BoM product codes")
async def overview():
    """Return the list of supported Australian states/territories and their
    BoM product codes.  Useful for discovering valid ``state`` parameter values.
    """
    states = []
    for code, product in sorted(STATE_PRODUCT_CODES.items()):
        states.append({
            "state": code,
            "product_code": product,
            "feed_url": _build_url(product),
        })

    return {
        "source": "australia_bom",
        "description": SOURCE_DESCRIPTION,
        "states": states,
        "example_station_url": _build_url("IDN60901", "94767"),
        "note": "Pass ?state=NSW (etc.) to /api/australia/stations to list stations.",
    }
