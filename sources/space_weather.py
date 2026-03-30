"""Space Weather — NOAA SWPC + NASA DONKI.

Two data sources are integrated here:

1. NOAA Space Weather Prediction Center (SWPC)
   Real-time space weather data including planetary K-index, solar wind
   parameters, GOES X-ray and proton flux, aurora forecasts, and alerts.
   All endpoints return JSON arrays and require no authentication.
   Base URL: https://services.swpc.noaa.gov/

2. NASA DONKI (Space Weather Database Of Notifications, Knowledge, Information)
   Catalogued space weather events: coronal mass ejections, solar flares,
   and geomagnetic storms.  Free DEMO_KEY works for moderate usage.
   Base URL: https://api.nasa.gov/DONKI/

All APIs are free with no authentication required (NASA DEMO_KEY is embedded).
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

from fastapi import APIRouter, HTTPException, Query

import cache
import http_client

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Source registry metadata  (read by server.py /api/sources)
# ---------------------------------------------------------------------------

SOURCE_NAME = "Space Weather"
SOURCE_DESCRIPTION = (
    "Real-time space weather conditions from NOAA SWPC (K-index, solar wind, "
    "GOES X-ray/proton flux, aurora forecast, alerts) and NASA DONKI "
    "(coronal mass ejections, solar flares, geomagnetic storms)."
)
SOURCE_META = {
    "url": "https://www.swpc.noaa.gov/",
    "auth_required": False,
    "update_frequency": "1-5 min (SWPC real-time), daily (DONKI catalog)",
    "coverage": "Global / near-Earth space environment",
    "params": [
        "kp_index", "solar_wind_speed", "solar_wind_density",
        "magnetic_field", "xray_flux", "proton_flux",
        "aurora_probability", "solar_flares", "cme",
    ],
    "category": "space_weather",
}

router = APIRouter(prefix="/api/space_weather", tags=["Space Weather"])

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
SWPC_BASE = "https://services.swpc.noaa.gov"
NASA_DONKI_BASE = "https://api.nasa.gov/DONKI"
NASA_API_KEY = "DEMO_KEY"

# Cache TTLs (seconds)
TTL_KP = 300          # 5 min — Kp updates every 3 hours, but check frequently
TTL_SOLAR_WIND = 120  # 2 min — near-real-time
TTL_XRAY = 120        # 2 min
TTL_AURORA = 600      # 10 min
TTL_ALERTS = 300      # 5 min
TTL_FLARES = 900      # 15 min — DONKI updates infrequently
TTL_OVERVIEW = 180    # 3 min

SOURCE = "space_weather"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _parse_kp_entry(row: list[str]) -> dict[str, Any]:
    """Parse a row from the SWPC Kp-index JSON (list of strings)."""
    # Format: [time_tag, Kp, Kp_fraction, a_running, station_count]
    return {
        "time_tag": row[0],
        "kp": float(row[1]) if row[1] else None,
        "kp_fraction": row[2] if len(row) > 2 else None,
        "a_running": float(row[3]) if len(row) > 3 and row[3] else None,
        "station_count": int(row[4]) if len(row) > 4 and row[4] else None,
    }


def _classify_kp(kp: float | None) -> str:
    """Return a human-readable geomagnetic condition label from Kp."""
    if kp is None:
        return "unknown"
    if kp < 4:
        return "quiet"
    if kp < 5:
        return "unsettled"
    if kp < 6:
        return "minor_storm"  # G1
    if kp < 7:
        return "moderate_storm"  # G2
    if kp < 8:
        return "strong_storm"  # G3
    if kp < 9:
        return "severe_storm"  # G4
    return "extreme_storm"  # G5


def _classify_xray(flux: float | None) -> str:
    """Classify X-ray flux into solar flare class."""
    if flux is None:
        return "unknown"
    if flux < 1e-7:
        return "A"
    if flux < 1e-6:
        return "B"
    if flux < 1e-5:
        return "C"
    if flux < 1e-4:
        return "M"
    return "X"


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@router.get("/kp")
async def get_kp_index(
    limit: int = Query(24, ge=1, le=500, description="Number of most recent entries"),
):
    """Planetary K-index — recent geomagnetic activity measurements.

    The Kp index quantifies disturbances in Earth's magnetic field on a
    0-9 scale.  Values >= 5 indicate geomagnetic storms (G1+).
    """
    cached = await cache.get(f"{SOURCE}:kp")
    if cached is not None:
        entries = cached
    else:
        url = f"{SWPC_BASE}/products/noaa-planetary-k-index.json"
        try:
            raw = await http_client.fetch_json(url, source=SOURCE)
        except Exception as exc:
            logger.error("SWPC Kp fetch failed: %s", exc)
            raise HTTPException(502, f"SWPC Kp data unavailable: {exc}")

        # First row is header; skip it
        entries = [_parse_kp_entry(row) for row in raw[1:] if len(row) >= 2]
        await cache.set(f"{SOURCE}:kp", entries, TTL_KP)

    # Return most recent entries
    result = entries[-limit:] if limit < len(entries) else entries
    latest = result[-1] if result else None
    return {
        "source": "NOAA SWPC",
        "parameter": "planetary_kp_index",
        "count": len(result),
        "latest": latest,
        "geomagnetic_condition": _classify_kp(latest["kp"] if latest else None),
        "data": result,
    }


@router.get("/solar_wind")
async def get_solar_wind(
    limit: int = Query(60, ge=1, le=2000, description="Number of most recent entries"),
):
    """Solar wind plasma and interplanetary magnetic field (7-day archive).

    Returns merged plasma (speed, density, temperature) and magnetic field
    (Bz, Bt) measurements from the DSCOVR or ACE spacecraft at L1.
    """
    cache_key = f"{SOURCE}:solar_wind"
    cached_data = await cache.get(cache_key)
    if cached_data is not None:
        merged = cached_data
    else:
        plasma_url = f"{SWPC_BASE}/products/solar-wind/plasma-7-day.json"
        mag_url = f"{SWPC_BASE}/products/solar-wind/mag-7-day.json"

        try:
            plasma_raw = await http_client.fetch_json(plasma_url, source=SOURCE)
        except Exception as exc:
            logger.error("SWPC solar wind plasma fetch failed: %s", exc)
            raise HTTPException(502, f"Solar wind plasma unavailable: {exc}")

        try:
            mag_raw = await http_client.fetch_json(mag_url, source=SOURCE)
        except Exception as exc:
            logger.error("SWPC solar wind mag fetch failed: %s", exc)
            raise HTTPException(502, f"Solar wind mag field unavailable: {exc}")

        # Build lookup of mag data by time_tag
        # Both have header row at index 0: skip it
        mag_by_time: dict[str, dict] = {}
        for row in mag_raw[1:]:
            if len(row) >= 4:
                mag_by_time[row[0]] = {
                    "bx_gsm": _safe_float(row[1]),
                    "by_gsm": _safe_float(row[2]),
                    "bz_gsm": _safe_float(row[3]),
                    "bt": _safe_float(row[6]) if len(row) > 6 else None,
                }

        merged = []
        for row in plasma_raw[1:]:
            if len(row) >= 3:
                time_tag = row[0]
                entry: dict[str, Any] = {
                    "time_tag": time_tag,
                    "density": _safe_float(row[1]),     # p/cc
                    "speed": _safe_float(row[2]),        # km/s
                    "temperature": _safe_float(row[3]) if len(row) > 3 else None,  # K
                }
                mag = mag_by_time.get(time_tag, {})
                entry.update({
                    "bz_gsm": mag.get("bz_gsm"),
                    "bt": mag.get("bt"),
                })
                merged.append(entry)

        await cache.set(cache_key, merged, TTL_SOLAR_WIND)

    result = merged[-limit:] if limit < len(merged) else merged
    latest = result[-1] if result else None
    return {
        "source": "NOAA SWPC (DSCOVR/ACE at L1)",
        "parameter": "solar_wind",
        "count": len(result),
        "latest": latest,
        "units": {
            "density": "protons/cm³",
            "speed": "km/s",
            "temperature": "K",
            "bz_gsm": "nT",
            "bt": "nT",
        },
        "data": result,
    }


@router.get("/xray")
async def get_xray(
    limit: int = Query(60, ge=1, le=5000, description="Number of most recent entries"),
):
    """GOES X-ray flux — primary solar flare indicator.

    Returns 1-minute X-ray flux from the primary GOES satellite in
    the 0.1-0.8 nm (long) channel.  Flux >= 1e-5 W/m² = C-class flare.
    """
    cache_key = f"{SOURCE}:xray"
    cached_data = await cache.get(cache_key)
    if cached_data is not None:
        entries = cached_data
    else:
        url = f"{SWPC_BASE}/json/goes/primary/xrays-7-day.json"
        try:
            raw = await http_client.fetch_json(url, source=SOURCE)
        except Exception as exc:
            logger.error("SWPC GOES X-ray fetch failed: %s", exc)
            raise HTTPException(502, f"GOES X-ray data unavailable: {exc}")

        entries = []
        for item in raw:
            if isinstance(item, dict):
                entries.append({
                    "time_tag": item.get("time_tag"),
                    "satellite": item.get("satellite"),
                    "energy": item.get("energy"),
                    "flux": item.get("flux"),
                    "flare_class": _classify_xray(item.get("flux")),
                })
        await cache.set(cache_key, entries, TTL_XRAY)

    result = entries[-limit:] if limit < len(entries) else entries
    latest = result[-1] if result else None
    return {
        "source": "NOAA SWPC (GOES primary)",
        "parameter": "xray_flux",
        "count": len(result),
        "latest": latest,
        "units": {"flux": "W/m²"},
        "data": result,
    }


@router.get("/aurora")
async def get_aurora():
    """Aurora forecast — OVATION model latest prediction.

    Returns the OVATION aurora probability map: an array of
    [longitude, latitude, probability] entries covering the globe.
    """
    cache_key = f"{SOURCE}:aurora"
    cached_data = await cache.get(cache_key)
    if cached_data is not None:
        return cached_data

    url = f"{SWPC_BASE}/json/ovation_aurora_latest.json"
    try:
        raw = await http_client.fetch_json(url, source=SOURCE)
    except Exception as exc:
        logger.error("SWPC aurora fetch failed: %s", exc)
        raise HTTPException(502, f"Aurora forecast unavailable: {exc}")

    # The response is a dict with "Forecast Date", "Data Issue Date",
    # "coordinates" (array of [lon, lat, prob])
    forecast_time = raw.get("Forecast Date")
    observation_time = raw.get("Data Issue Date")
    coords = raw.get("coordinates", [])

    # Find maximum aurora probability
    max_prob = 0
    max_location = None
    for entry in coords:
        if len(entry) >= 3 and entry[2] > max_prob:
            max_prob = entry[2]
            max_location = {"longitude": entry[0], "latitude": entry[1]}

    result = {
        "source": "NOAA SWPC (OVATION model)",
        "parameter": "aurora_forecast",
        "forecast_time": forecast_time,
        "observation_time": observation_time,
        "total_grid_points": len(coords),
        "max_probability": max_prob,
        "max_probability_location": max_location,
        "coordinates": coords,
    }
    await cache.set(cache_key, result, TTL_AURORA)
    return result


@router.get("/alerts")
async def get_alerts():
    """Active space weather alerts, warnings, and watches from SWPC.

    Includes geomagnetic storm warnings, solar radiation storms,
    radio blackout alerts, and other advisories.
    """
    cache_key = f"{SOURCE}:alerts"
    cached_data = await cache.get(cache_key)
    if cached_data is not None:
        return cached_data

    url = f"{SWPC_BASE}/products/alerts.json"
    try:
        raw = await http_client.fetch_json(url, source=SOURCE)
    except Exception as exc:
        logger.error("SWPC alerts fetch failed: %s", exc)
        raise HTTPException(502, f"Space weather alerts unavailable: {exc}")

    alerts = []
    for item in raw:
        if isinstance(item, dict):
            alerts.append({
                "product_id": item.get("product_id"),
                "issue_datetime": item.get("issue_datetime"),
                "message": item.get("message"),
            })

    result = {
        "source": "NOAA SWPC",
        "parameter": "space_weather_alerts",
        "count": len(alerts),
        "alerts": alerts,
    }
    await cache.set(cache_key, result, TTL_ALERTS)
    return result


@router.get("/flares")
async def get_flares(
    days: int = Query(7, ge=1, le=30, description="Look-back period in days"),
):
    """Recent solar flares from NASA DONKI catalog.

    Returns catalogued solar flare events including class, peak time,
    source location, and linked CME/SEP events.
    """
    cache_key = f"{SOURCE}:flares:{days}"
    cached_data = await cache.get(cache_key)
    if cached_data is not None:
        return cached_data

    end = datetime.now(timezone.utc)
    start = end - timedelta(days=days)
    url = f"{NASA_DONKI_BASE}/FLR"
    params = {
        "startDate": start.strftime("%Y-%m-%d"),
        "endDate": end.strftime("%Y-%m-%d"),
        "api_key": NASA_API_KEY,
    }

    try:
        raw = await http_client.fetch_json(url, params=params, source=SOURCE)
    except Exception as exc:
        logger.error("NASA DONKI flares fetch failed: %s", exc)
        raise HTTPException(502, f"NASA DONKI solar flares unavailable: {exc}")

    # DONKI returns an empty list or list of flare objects
    flares = []
    if isinstance(raw, list):
        for item in raw:
            flares.append({
                "flr_id": item.get("flrID"),
                "begin_time": item.get("beginTime"),
                "peak_time": item.get("peakTime"),
                "end_time": item.get("endTime"),
                "class_type": item.get("classType"),
                "source_location": item.get("sourceLocation"),
                "active_region": item.get("activeRegionNum"),
                "linked_events": item.get("linkedEvents"),
            })

    result = {
        "source": "NASA DONKI",
        "parameter": "solar_flares",
        "days": days,
        "count": len(flares),
        "flares": flares,
    }
    await cache.set(cache_key, result, TTL_FLARES)
    return result


@router.get("/overview")
async def get_overview():
    """Dashboard summary of current space weather conditions.

    Aggregates the latest Kp index, solar wind, X-ray flux, and alert
    count into a single response for quick situational awareness.
    """
    cache_key = f"{SOURCE}:overview"
    cached_data = await cache.get(cache_key)
    if cached_data is not None:
        return cached_data

    overview: dict[str, Any] = {
        "source": "NOAA SWPC + NASA DONKI",
        "generated_utc": datetime.now(timezone.utc).isoformat(),
    }

    # --- Kp index ---
    try:
        kp_data = await http_client.fetch_json(
            f"{SWPC_BASE}/products/noaa-planetary-k-index.json", source=SOURCE
        )
        if len(kp_data) > 1:
            last_row = kp_data[-1]
            kp_val = float(last_row[1]) if last_row[1] else None
            overview["kp_index"] = {
                "value": kp_val,
                "time_tag": last_row[0],
                "condition": _classify_kp(kp_val),
            }
    except Exception as exc:
        logger.warning("Overview: Kp fetch failed: %s", exc)
        overview["kp_index"] = {"error": str(exc)}

    # --- Solar wind (latest) ---
    try:
        plasma = await http_client.fetch_json(
            f"{SWPC_BASE}/products/solar-wind/plasma-7-day.json", source=SOURCE
        )
        if len(plasma) > 1:
            last = plasma[-1]
            overview["solar_wind"] = {
                "speed_km_s": _safe_float(last[2]),
                "density_p_cc": _safe_float(last[1]),
                "time_tag": last[0],
            }
    except Exception as exc:
        logger.warning("Overview: solar wind fetch failed: %s", exc)
        overview["solar_wind"] = {"error": str(exc)}

    # --- X-ray flux (latest) ---
    try:
        xray = await http_client.fetch_json(
            f"{SWPC_BASE}/json/goes/primary/xrays-7-day.json", source=SOURCE
        )
        if xray:
            # Find most recent long-channel (0.1-0.8nm) entry
            latest_xray = None
            for item in reversed(xray):
                if isinstance(item, dict) and item.get("energy") == "0.1-0.8nm":
                    latest_xray = item
                    break
            if latest_xray is None and xray:
                latest_xray = xray[-1] if isinstance(xray[-1], dict) else None
            if latest_xray:
                flux = latest_xray.get("flux")
                overview["xray"] = {
                    "flux": flux,
                    "flare_class": _classify_xray(flux),
                    "time_tag": latest_xray.get("time_tag"),
                }
    except Exception as exc:
        logger.warning("Overview: X-ray fetch failed: %s", exc)
        overview["xray"] = {"error": str(exc)}

    # --- Alert count ---
    try:
        alerts = await http_client.fetch_json(
            f"{SWPC_BASE}/products/alerts.json", source=SOURCE
        )
        overview["active_alerts"] = len(alerts) if isinstance(alerts, list) else 0
    except Exception as exc:
        logger.warning("Overview: alerts fetch failed: %s", exc)
        overview["active_alerts"] = {"error": str(exc)}

    # --- Determine overall risk level ---
    kp_val = None
    if isinstance(overview.get("kp_index"), dict):
        kp_val = overview["kp_index"].get("value")

    xray_class = "A"
    if isinstance(overview.get("xray"), dict):
        xray_class = overview["xray"].get("flare_class", "A")

    if kp_val is not None and kp_val >= 7:
        risk = "severe"
    elif kp_val is not None and kp_val >= 5:
        risk = "elevated"
    elif xray_class in ("M", "X"):
        risk = "elevated"
    elif kp_val is not None and kp_val >= 4:
        risk = "minor"
    else:
        risk = "nominal"

    overview["overall_risk"] = risk

    await cache.set(cache_key, overview, TTL_OVERVIEW)
    return overview


# ---------------------------------------------------------------------------
# Utility
# ---------------------------------------------------------------------------

def _safe_float(val: Any) -> Optional[float]:
    """Convert a value to float, returning None on failure."""
    if val is None or val == "":
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None
