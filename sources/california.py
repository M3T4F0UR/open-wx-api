"""California Rain/Flood/Water Networks — wxsection.com source module.

Covers:
  1. CDEC  — California Data Exchange Center (DWR)
  2. LA County DPW — Rainfall gauge network (ArcGIS REST)
  3. Orange County PWD — Hydrology Data Hub (ArcGIS feature service)
  4. Santa Clara Valley Water — ALERT telemetry API
"""

import asyncio
import csv
import io
import logging
import re
from datetime import datetime, timedelta, timezone
from typing import Optional

import httpx
from fastapi import APIRouter, HTTPException, Query

import config
from cache import cached
from models import Reading, Station

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Module-level metadata (consumed by server.py /api/sources)
# ---------------------------------------------------------------------------

SOURCE_NAME = "California Networks"
SOURCE_DESCRIPTION = (
    "California rain, flood, and water monitoring networks: CDEC (DWR), "
    "LA County DPW rainfall gauges, Orange County PWD hydrology, "
    "and Santa Clara Valley Water ALERT telemetry."
)
SOURCE_META = {
    "coverage": "California, USA",
    "category": "water",
    "auth_required": False,
    "params": ["precipitation", "stage", "flow", "reservoir_storage", "water_temp", "air_temp"],
    "update_frequency": "near-real-time (15 min – hourly)",
    "url": "https://cdec.water.ca.gov",
}

# ---------------------------------------------------------------------------
# Router
# ---------------------------------------------------------------------------

router = APIRouter(prefix="/api/california", tags=["California Networks"])

# ---------------------------------------------------------------------------
# Shared HTTP helpers
# ---------------------------------------------------------------------------

_HEADERS = {"User-Agent": config.USER_AGENT, "Accept": "*/*"}


def _client() -> httpx.AsyncClient:
    return httpx.AsyncClient(
        timeout=config.HTTP_TIMEOUT,
        headers=_HEADERS,
        follow_redirects=True,
    )


# ---------------------------------------------------------------------------
# ── 1. CDEC  (California Data Exchange Center) ─────────────────────────────
# ---------------------------------------------------------------------------

_CDEC_BASE = "https://cdec.water.ca.gov"

# Well-known CDEC sensor codes
CDEC_SENSORS: dict[int, tuple[str, str]] = {
    1:  ("stage",             "ft"),
    2:  ("precipitation",     "in"),       # event / tipping-bucket
    6:  ("reservoir_storage", "acre-ft"),
    15: ("reservoir_storage_pct", "%"),
    20: ("flow",              "cfs"),
    25: ("water_temp",        "°F"),
    30: ("air_temp",          "°F"),
    45: ("precipitation",     "in"),       # incremental / standard precip
    62: ("evapotranspiration","in"),
    76: ("wind_speed",        "mph"),
}

# Curated list of high-value CDEC stations (rain/flood/hydro).
# Covers the major watersheds and urban flood-prone areas statewide.
_CDEC_KNOWN_STATIONS: list[dict] = [
    # Los Angeles / Southern CA
    {"id": "LAX", "name": "Los Angeles Airport",               "lat": 33.9425, "lon": -118.4081, "sensors": [45, 30]},
    {"id": "MTC", "name": "Matilija Creek (Ventura)",          "lat": 34.5152, "lon": -119.2664, "sensors": [20, 1]},
    {"id": "STN", "name": "Stony Creek (Ventura)",             "lat": 34.4919, "lon": -119.1811, "sensors": [45, 20]},
    {"id": "VEN", "name": "Ventura River",                     "lat": 34.2775, "lon": -119.2950, "sensors": [20, 1]},
    {"id": "SCR", "name": "Santa Clara River (Fillmore)",      "lat": 34.3997, "lon": -118.9203, "sensors": [20, 1]},
    # San Francisco Bay Area
    {"id": "SMF", "name": "Sacramento Metro Airport",          "lat": 38.6955, "lon": -121.5908, "sensors": [45, 30]},
    {"id": "SJC", "name": "San Jose Airport",                  "lat": 37.3626, "lon": -121.9289, "sensors": [45, 30]},
    {"id": "SFO", "name": "San Francisco Airport",             "lat": 37.6213, "lon": -122.3790, "sensors": [45, 30]},
    {"id": "NAP", "name": "Napa River at Napa",                "lat": 38.2955, "lon": -122.2859, "sensors": [20, 1, 45]},
    {"id": "PEL", "name": "Petaluma River",                    "lat": 38.2328, "lon": -122.6375, "sensors": [20, 1]},
    # Sacramento Valley / Delta
    {"id": "SAC", "name": "Sacramento River at Sacramento",    "lat": 38.5816, "lon": -121.4944, "sensors": [20, 1]},
    {"id": "FRE", "name": "Fresno Airport",                    "lat": 36.7762, "lon": -119.7193, "sensors": [45, 30]},
    {"id": "ORO", "name": "Oroville Dam (Feather River)",      "lat": 39.5369, "lon": -121.4875, "sensors": [6, 20]},
    {"id": "SHA", "name": "Shasta Dam",                        "lat": 40.7196, "lon": -122.4144, "sensors": [6, 20]},
    {"id": "FOL", "name": "Folsom Dam",                        "lat": 38.7132, "lon": -121.1607, "sensors": [6, 20]},
    {"id": "NML", "name": "New Melones Dam",                   "lat": 37.9549, "lon": -120.5313, "sensors": [6, 20]},
    # Sierra Nevada rain/snow
    {"id": "BLU", "name": "Blue Canyon (Sierra)",              "lat": 39.2832, "lon": -120.7099, "sensors": [45, 30]},
    {"id": "CSS", "name": "Calaveras Big Trees",               "lat": 38.2716, "lon": -120.3177, "sensors": [45, 2]},
    # Central Coast
    {"id": "SBA", "name": "Santa Barbara Airport",             "lat": 34.4263, "lon": -119.8403, "sensors": [45, 30]},
    {"id": "SLO", "name": "San Luis Obispo (Cal Poly)",        "lat": 35.3050, "lon": -120.6625, "sensors": [45, 30]},
    # North Coast
    {"id": "EKA", "name": "Eureka Airport",                    "lat": 40.8021, "lon": -124.1627, "sensors": [45, 30]},
    {"id": "RDD", "name": "Redding Airport",                   "lat": 40.5090, "lon": -122.2930, "sensors": [45, 30]},
    # San Diego
    {"id": "SAN", "name": "San Diego Airport",                 "lat": 32.7338, "lon": -117.1933, "sensors": [45, 30]},
    {"id": "STO", "name": "Sweetwater Reservoir (San Diego)",  "lat": 32.6949, "lon": -116.9775, "sensors": [6, 45]},
]


def _cdec_station_url(station_id: str) -> str:
    return f"{_CDEC_BASE}/dynamicapp/staMeta?station_id={station_id.upper()}"


def _cdec_csv_url(station_id: str, sensor: int, duration: str, start: str, end: str) -> str:
    return (
        f"{_CDEC_BASE}/dynamicapp/req/CSVDataServlet"
        f"?Stations={station_id.upper()}"
        f"&SensorNums={sensor}"
        f"&dur_code={duration.upper()}"
        f"&Start={start}"
        f"&End={end}"
    )


def _parse_cdec_csv(text: str, station_id: str) -> list[Reading]:
    """Parse CDEC CSV response into Reading objects.

    Expected header:
        STATION_ID,DURATION,SENSOR_NUMBER,SENSOR_TYPE,DATE TIME,OBS DATE,VALUE,DATA_FLAG,UNITS
    """
    readings: list[Reading] = []
    if not text or not text.strip():
        return readings

    reader = csv.DictReader(io.StringIO(text.strip()))
    for row in reader:
        try:
            raw_val = row.get("VALUE", "").strip()
            if not raw_val or raw_val in ("---", "ART", "BRT", ""):
                continue
            value = float(raw_val)

            # Parse timestamp — CDEC uses "MM/DD/YYYY HH:MM" or "YYYY-MM-DD HH:MM"
            raw_dt = (row.get("DATE TIME") or row.get("OBS DATE") or "").strip()
            if not raw_dt:
                continue
            ts = _parse_cdec_dt(raw_dt)
            if ts is None:
                continue

            sensor_num_str = (row.get("SENSOR_NUMBER") or row.get("SENSOR NUMBER") or "").strip()
            try:
                sensor_num = int(sensor_num_str)
            except ValueError:
                sensor_num = 45  # default to precip

            param, unit = CDEC_SENSORS.get(sensor_num, ("observation", ""))
            # Prefer the unit from the CSV if present
            csv_unit = row.get("UNITS", "").strip()
            if csv_unit:
                unit = csv_unit

            quality = (row.get("DATA_FLAG") or row.get("DATA FLAG") or "").strip() or None

            readings.append(Reading(
                station_id=station_id.upper(),
                timestamp=ts,
                parameter=param,
                value=value,
                unit=unit,
                quality=quality,
                source="cdec",
            ))
        except Exception:
            continue

    return readings


def _parse_cdec_dt(raw: str) -> Optional[datetime]:
    """Try several CDEC datetime formats and return an aware UTC datetime."""
    formats = [
        "%m/%d/%Y %H%M",   # e.g. 01/15/2025 1400
        "%m/%d/%Y %H:%M",  # e.g. 01/15/2025 14:00
        "%Y-%m-%d %H:%M",  # e.g. 2025-01-15 14:00
        "%Y%m%d %H%M",
    ]
    for fmt in formats:
        try:
            dt = datetime.strptime(raw.strip(), fmt)
            # CDEC timestamps are Pacific time; treat as-is and mark UTC for
            # consistency (CDEC does not publish TZ offsets in CSV).
            return dt.replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    return None


@router.get("/cdec/stations", summary="List CDEC stations")
@cached(ttl=config.CACHE_TTL_LONG, prefix="ca:")
async def cdec_stations(
    sensor: Optional[int] = Query(None, description="Filter by sensor number (e.g. 45 for precip)"),
) -> dict:
    """Return the curated list of major CDEC monitoring stations.

    Optionally filter to stations that carry a particular sensor number.
    """
    stations = []
    for s in _CDEC_KNOWN_STATIONS:
        if sensor is not None and sensor not in s.get("sensors", []):
            continue
        param_list = [CDEC_SENSORS[sn][0] for sn in s.get("sensors", []) if sn in CDEC_SENSORS]
        stations.append(Station(
            id=s["id"],
            name=s["name"],
            lat=s["lat"],
            lon=s["lon"],
            source="cdec",
            network="cdec",
            params=list(dict.fromkeys(param_list)),  # deduplicate, preserve order
            url=_cdec_station_url(s["id"]),
            extra={"sensors": s.get("sensors", [])},
        ))

    return {
        "source": "cdec",
        "network": "California Data Exchange Center",
        "count": len(stations),
        "stations": [st.model_dump() for st in stations],
    }


@router.get("/cdec/stations/{station_id}", summary="CDEC station metadata")
@cached(ttl=config.CACHE_TTL_LONG, prefix="ca:")
async def cdec_station_info(station_id: str) -> dict:
    """Fetch live metadata for a single CDEC station by scraping staMeta."""
    sid = station_id.upper()
    url = _cdec_station_url(sid)

    # Check known list first
    known = next((s for s in _CDEC_KNOWN_STATIONS if s["id"] == sid), None)

    try:
        async with _client() as client:
            resp = await client.get(url)
            resp.raise_for_status()
            html = resp.text
    except httpx.TimeoutException:
        if known:
            return _cdec_known_to_station(known).model_dump()
        raise HTTPException(504, f"CDEC metadata request timed out for station {sid}")
    except httpx.HTTPStatusError as exc:
        raise HTTPException(exc.response.status_code, f"CDEC returned HTTP {exc.response.status_code}")

    # Parse minimal info from the HTML response
    extra: dict = {"raw_meta_url": url}

    # Pull the station name from <title> or table heading
    name = sid
    if "<title>" in html.lower():
        try:
            start = html.lower().index("<title>") + 7
            end = html.lower().index("</title>")
            name = html[start:end].strip()
        except ValueError:
            pass

    lat = known["lat"] if known else 0.0
    lon = known["lon"] if known else 0.0

    station = Station(
        id=sid,
        name=name,
        lat=lat,
        lon=lon,
        source="cdec",
        network="cdec",
        params=[CDEC_SENSORS[sn][0] for sn in (known or {}).get("sensors", []) if sn in CDEC_SENSORS],
        url=url,
        extra=extra,
    )
    return station.model_dump()


def _cdec_known_to_station(s: dict) -> Station:
    param_list = [CDEC_SENSORS[sn][0] for sn in s.get("sensors", []) if sn in CDEC_SENSORS]
    return Station(
        id=s["id"],
        name=s["name"],
        lat=s["lat"],
        lon=s["lon"],
        source="cdec",
        network="cdec",
        params=list(dict.fromkeys(param_list)),
        url=_cdec_station_url(s["id"]),
        extra={"sensors": s.get("sensors", [])},
    )


@router.get("/cdec/stations/{station_id}/readings", summary="CDEC time-series readings")
@cached(ttl=config.CACHE_TTL, prefix="ca:")
async def cdec_readings(
    station_id: str,
    sensor: int = Query(45, description="Sensor number (default 45 = incremental precip)"),
    duration: str = Query("H", description="Duration code: E=event, H=hourly, D=daily"),
    hours: int = Query(24, ge=1, le=8760, description="Hours of data to retrieve (max 8760)"),
) -> dict:
    """Return time-series readings from a CDEC station."""
    sid = station_id.upper()
    dur = duration.upper()
    if dur not in ("E", "H", "D"):
        raise HTTPException(400, "duration must be one of E, H, D")

    now = datetime.now(timezone.utc)
    start_dt = now - timedelta(hours=hours)
    start_str = start_dt.strftime("%Y-%m-%dT%H:%M:%S")
    end_str = now.strftime("%Y-%m-%dT%H:%M:%S")

    url = _cdec_csv_url(sid, sensor, dur, start_str, end_str)

    try:
        async with _client() as client:
            resp = await client.get(url)
            resp.raise_for_status()
            text = resp.text
    except httpx.TimeoutException:
        raise HTTPException(504, f"CDEC CSV request timed out for {sid} sensor {sensor}")
    except httpx.HTTPStatusError as exc:
        raise HTTPException(exc.response.status_code, f"CDEC returned HTTP {exc.response.status_code}")
    except Exception as exc:
        log.exception("CDEC CSV fetch error")
        raise HTTPException(502, f"CDEC fetch error: {exc}")

    readings = _parse_cdec_csv(text, sid)
    param_name, unit = CDEC_SENSORS.get(sensor, ("observation", ""))

    return {
        "source": "cdec",
        "station_id": sid,
        "sensor": sensor,
        "parameter": param_name,
        "unit": unit,
        "duration": dur,
        "hours_requested": hours,
        "count": len(readings),
        "readings": [r.model_dump() for r in readings],
        "data_url": url,
    }


async def _cdec_fetch_latest(client: httpx.AsyncClient, station: dict) -> Optional[datetime]:
    """Fetch the most recent timestamp for a CDEC station (lightweight 2-hr window)."""
    sid = station["id"]
    # Use the first sensor code for this station
    sensors = station.get("sensors", [])
    sensor = sensors[0] if sensors else 45
    now = datetime.now(timezone.utc)
    start_dt = now - timedelta(hours=6)
    url = _cdec_csv_url(sid, sensor, "H", start_dt.strftime("%Y-%m-%d"), now.strftime("%Y-%m-%d"))
    try:
        resp = await client.get(url)
        resp.raise_for_status()
        text = resp.text
    except Exception:
        return None
    # Parse CSV to find the most recent timestamp
    if not text or not text.strip():
        return None
    reader = csv.DictReader(io.StringIO(text.strip()))
    latest: Optional[datetime] = None
    for row in reader:
        raw_dt = (row.get("DATE TIME") or row.get("OBS DATE") or "").strip()
        if not raw_dt:
            continue
        ts = _parse_cdec_dt(raw_dt)
        if ts is not None and (latest is None or ts > latest):
            latest = ts
    return latest


@router.get("/cdec/current", summary="CDEC stations with current timestamps")
@cached(ttl=config.CACHE_TTL, prefix="ca:")
async def cdec_current() -> dict:
    """Return CDEC stations with last_update timestamps from recent observations.

    Fetches the last 6 hours of hourly data for each curated CDEC station in
    parallel and extracts the most recent observation timestamp.
    """
    async with _client() as client:
        tasks = [_cdec_fetch_latest(client, s) for s in _CDEC_KNOWN_STATIONS]
        results = await asyncio.gather(*tasks, return_exceptions=True)

    stations = []
    for s, latest_ts in zip(_CDEC_KNOWN_STATIONS, results):
        if isinstance(latest_ts, Exception):
            latest_ts = None
        param_list = [CDEC_SENSORS[sn][0] for sn in s.get("sensors", []) if sn in CDEC_SENSORS]
        stations.append(Station(
            id=s["id"],
            name=s["name"],
            lat=s["lat"],
            lon=s["lon"],
            source="cdec",
            network="cdec",
            params=list(dict.fromkeys(param_list)),
            last_update=latest_ts,
            url=_cdec_station_url(s["id"]),
            extra={"sensors": s.get("sensors", [])},
        ).model_dump())

    return {
        "source": "cdec",
        "network": "California Data Exchange Center",
        "count": len(stations),
        "stations": stations,
    }


# ---------------------------------------------------------------------------
# ── 2. LA County DPW Rainfall (ALERT real-time rain gauges) ─────────────────
# ---------------------------------------------------------------------------

# NOTE: The original ArcGIS REST layer at
#   dpw.gis.lacounty.gov/dpw/rest/services/Rainfall/MapServer/0
# has been repurposed to serve Major_Channel polylines instead of rain gauge
# point features.  We now scrape station + rainfall data from the DPW
# real-time precipitation map page, which embeds station coordinates,
# names, and current readings in its JavaScript.

_LACOUNTY_RAINFALL_URL = "https://dpw.lacounty.gov/wrd/precip/alert_rain/index.cfm"

# Duration code → (canonical parameter name, description)
_LA_DURATION_MAP: dict[str, tuple[str, str]] = {
    "15":  ("precipitation_15min",   "in"),
    "30":  ("precipitation_30min",   "in"),
    "1":   ("precipitation_1hr",     "in"),
    "3":   ("precipitation_3hr",     "in"),
    "6":   ("precipitation_6hr",     "in"),
    "12":  ("precipitation_12hr",    "in"),
    "24":  ("precipitation_24hr",    "in"),
    "2":   ("precipitation_48hr",    "in"),
    "33":  ("precipitation_30day",   "in"),
    "4":   ("precipitation_3day",    "in"),
    "7":   ("precipitation_7day",    "in"),
    "99":  ("precipitation_season_avg", "in"),
    "100": ("precipitation_season",  "in"),
}


def _parse_lacounty_rainfall_page(html: str) -> list[dict]:
    """Parse the LA County DPW real-time precipitation page JavaScript.

    The page embeds station data in DrawWRDRainFall_*() functions with patterns:
      var markerDisplayText = ["Station Name "];
      if (num == <dur_code>) hrNum += ["<value>"];
      ...
      var viewDurationURL = goToUrl = "Home/ViewDuration?id=" + "<station_id>" + ...
      var Marker = setRainFallMarker1(map, [<lat>], [<lon>], ..., ["<full_name>"]);
    """
    # Extract station IDs (ALERT IDs like "2550400")
    id_re = re.compile(
        r'"Home/ViewDuration\?id="\s*\+\s*"(\d+)"'
    )
    ids = id_re.findall(html)

    # Extract marker coordinates and names
    marker_re = re.compile(
        r'setRainFallMarker\d?\(map,\s*\[([^\]]+)\],\s*\[([^\]]+)\],'
        r'\s*markerDisplayText,\s*viewDurationURL,\s*\["([^"]+)"\]\)'
    )
    markers = marker_re.findall(html)

    if len(ids) != len(markers):
        log.warning(
            "LA County DPW: ID count (%d) != marker count (%d); using min",
            len(ids), len(markers),
        )

    # Extract per-station rainfall values by splitting on station blocks.
    # Each station block starts with `var markerDisplayText = ["<name>"];`
    # and contains `if (num == <code>) hrNum += ["<value>"];` lines.
    value_re = re.compile(
        r'if\s*\(num\s*==\s*(\d+)\)\s*\n?\s*hrNum\s*\+=\s*\["([^"]*)"\]'
    )
    # Split HTML into station blocks at each markerDisplayText
    block_re = re.compile(r'var markerDisplayText\s*=\s*\[')
    blocks = block_re.split(html)
    # First element is before any station block
    station_blocks = blocks[1:] if len(blocks) > 1 else []

    parsed: list[dict] = []
    count = min(len(ids), len(markers))
    for i in range(count):
        lat_str, lon_str, full_name = markers[i]
        try:
            lat = float(lat_str.strip())
            lon = float(lon_str.strip())
        except (ValueError, TypeError):
            continue

        name = full_name.replace(" Precip", "").strip()
        alert_id = ids[i]

        # Extract rainfall values from the corresponding block
        rainfall: dict[str, float] = {}
        if i < len(station_blocks):
            for dur_code, val_str in value_re.findall(station_blocks[i]):
                try:
                    rainfall[dur_code] = float(val_str)
                except (ValueError, TypeError):
                    pass

        parsed.append({
            "alert_id": alert_id,
            "name": name,
            "lat": lat,
            "lon": lon,
            "rainfall": rainfall,
        })

    return parsed


def _la_parsed_to_station(raw: dict, fetch_time: Optional[datetime] = None) -> Station:
    """Convert a parsed LA County DPW rain gauge to a Station model."""
    sid = raw["alert_id"]
    params = ["precipitation"]
    # Use fetch_time as last_update since data is live at scrape time
    last_ts = fetch_time or datetime.now(timezone.utc)
    return Station(
        id=f"lacounty_{sid}",
        name=raw["name"],
        lat=raw["lat"],
        lon=raw["lon"],
        source="lacounty_dpw",
        network="la_rainfall",
        params=params,
        last_update=last_ts,
        url="https://dpw.lacounty.gov/wrd/precip/",
        extra={"alert_id": sid},
    )


def _la_parsed_to_readings(raw: dict) -> list[Reading]:
    """Extract rainfall readings from parsed station data."""
    readings: list[Reading] = []
    sid = f"lacounty_{raw['alert_id']}"
    ts = datetime.now(timezone.utc)

    for dur_code, value in raw.get("rainfall", {}).items():
        mapping = _LA_DURATION_MAP.get(dur_code)
        if mapping is None:
            continue
        param, unit = mapping
        try:
            readings.append(Reading(
                station_id=sid,
                timestamp=ts,
                parameter=param,
                value=value,
                unit=unit,
                quality="realtime",
                source="lacounty_dpw",
            ))
        except (TypeError, ValueError):
            pass

    return readings


@router.get("/lacounty/stations", summary="LA County DPW rain gauges")
@cached(ttl=config.CACHE_TTL, prefix="ca:")
async def lacounty_stations() -> dict:
    """Return all LA County DPW rainfall gauge stations with current readings.

    Data is scraped from the LA County DPW near-real-time precipitation map
    which displays ALERT rain gauge data for 130+ stations across the county.
    """
    try:
        # LA County DPW uses a self-signed cert in its chain; disable
        # verification for this specific request.
        async with httpx.AsyncClient(
            timeout=config.HTTP_TIMEOUT,
            headers=_HEADERS,
            follow_redirects=True,
            verify=False,
        ) as client:
            resp = await client.get(_LACOUNTY_RAINFALL_URL)
            resp.raise_for_status()
            html = resp.text
    except httpx.TimeoutException:
        raise HTTPException(504, "LA County DPW precipitation page request timed out")
    except httpx.HTTPStatusError as exc:
        raise HTTPException(exc.response.status_code, f"LA County DPW returned HTTP {exc.response.status_code}")
    except Exception as exc:
        log.exception("LA County DPW fetch error")
        raise HTTPException(502, f"LA County DPW fetch error: {exc}")

    parsed = _parse_lacounty_rainfall_page(html)
    if not parsed:
        return {"source": "lacounty_dpw", "count": 0, "stations": []}

    fetch_time = datetime.now(timezone.utc)
    stations = []
    for raw in parsed:
        try:
            st = _la_parsed_to_station(raw, fetch_time=fetch_time)
            stations.append(st.model_dump())
        except Exception:
            log.debug("Failed to parse LA County station: %s", raw, exc_info=True)

    return {
        "source": "lacounty_dpw",
        "network": "LA County DPW Rainfall",
        "count": len(stations),
        "stations": stations,
    }


@router.get("/lacounty/stations/{station_id}", summary="LA County DPW single gauge readings")
@cached(ttl=config.CACHE_TTL, prefix="ca:")
async def lacounty_station(station_id: str) -> dict:
    """Return station info and current rainfall readings for a single LA County gauge.

    station_id should be the ALERT ID (numeric string) or the full
    prefixed ID returned by /lacounty/stations (e.g. "lacounty_2550400").
    """
    raw_id = station_id.removeprefix("lacounty_")

    # Fetch all stations and filter (the data comes from a single page scrape)
    try:
        async with httpx.AsyncClient(
            timeout=config.HTTP_TIMEOUT,
            headers=_HEADERS,
            follow_redirects=True,
            verify=False,
        ) as client:
            resp = await client.get(_LACOUNTY_RAINFALL_URL)
            resp.raise_for_status()
            html = resp.text
    except httpx.TimeoutException:
        raise HTTPException(504, "LA County DPW precipitation page request timed out")
    except httpx.HTTPStatusError as exc:
        raise HTTPException(exc.response.status_code, f"LA County DPW returned HTTP {exc.response.status_code}")

    parsed = _parse_lacounty_rainfall_page(html)
    match = next((p for p in parsed if p["alert_id"] == raw_id), None)
    if match is None:
        raise HTTPException(404, f"Station '{station_id}' not found in LA County DPW")

    station = _la_parsed_to_station(match, fetch_time=datetime.now(timezone.utc))
    readings = _la_parsed_to_readings(match)

    return {
        "source": "lacounty_dpw",
        "station": station.model_dump(),
        "readings": [r.model_dump() for r in readings],
    }


# ---------------------------------------------------------------------------
# ── 3. Orange County PWD — Hydrology Data Hub (ArcGIS) ─────────────────────
# ---------------------------------------------------------------------------

# OC PWD publishes its ALERT gauge network via their ArcGIS Online Hub.
# The feature service URL below is the publicly accessible REST endpoint.
_OC_FEATURE_URL = (
    "https://services.arcgis.com/UXmFoWC7yDHcDN5Q/arcgis/rest/services/"
    "OCHydstraFloodStations/FeatureServer/0/query"
    "?where=1%3D1&outFields=*&f=json&returnGeometry=true&outSR=4326"
)


def _oc_feat_to_station(feat: dict) -> Optional[Station]:
    try:
        attrs = feat.get("attributes", {})
        geo = feat.get("geometry", {})

        sid = (
            attrs.get("StationID")
            or attrs.get("STATION_ID")
            or attrs.get("station_id")
            or attrs.get("OCID")
            or attrs.get("HSC")
            or attrs.get("OBJECTID")
        )
        if sid is None:
            return None

        name = (
            attrs.get("StationName")
            or attrs.get("STATION_NAME")
            or attrs.get("HydstraName")
            or attrs.get("Name")
            or str(sid)
        )

        # Prefer geometry (x=lon, y=lat in WGS84) over attribute fields
        # Note: In this dataset the attribute fields "Lon" and "Lat" are
        # swapped (Lon holds latitude, Lat holds longitude), so we rely on
        # the ArcGIS geometry which is always correct when outSR=4326.
        lat = float(geo.get("y", 0.0))
        lon = float(geo.get("x", 0.0))
        if lat == 0.0 and lon == 0.0:
            return None

        # Infer params from attribute keys and subcategory
        params: list[str] = []
        subcategory = str(attrs.get("Subcategory") or "").lower()
        hname = str(attrs.get("HydstraName") or "").lower()
        for key in attrs:
            kl = key.lower()
            if "rain" in kl or "precip" in kl:
                params.append("precipitation")
            elif "stage" in kl or "level" in kl or "gauge" in kl:
                params.append("stage")
            elif "flow" in kl or "discharge" in kl:
                params.append("flow")
        if "precip" in hname or "rain" in hname:
            params.append("precipitation")
        if "stage" in hname or "level" in hname or "flood" in subcategory:
            params.append("stage")
        if not params:
            params = ["precipitation"]

        # Parse last_update from ArcGIS timestamp fields (Unix milliseconds)
        last_update = None
        for ts_key in ("LastUpdate", "last_update", "EditDate", "CreationDate",
                        "LastModified", "LAST_UPDATE", "Date_Time"):
            ts_raw = attrs.get(ts_key)
            if ts_raw is not None:
                try:
                    if isinstance(ts_raw, (int, float)) and ts_raw > 1_000_000_000:
                        # Esri epoch milliseconds
                        last_update = datetime.fromtimestamp(
                            float(ts_raw) / 1000.0, tz=timezone.utc
                        )
                    elif isinstance(ts_raw, str) and ts_raw:
                        last_update = datetime.fromisoformat(
                            ts_raw.replace("Z", "+00:00")
                        )
                except (ValueError, TypeError, OSError):
                    pass
                if last_update is not None:
                    break

        return Station(
            id=f"oc_{sid}",
            name=str(name),
            lat=lat,
            lon=lon,
            source="oc_pwd",
            network="oc_alert",
            params=list(dict.fromkeys(params)),
            last_update=last_update,
            url="https://hydrology-data-hub-site-ocpw.hub.arcgis.com/",
            extra={k: v for k, v in attrs.items() if v not in (None, "", " ")},
        )
    except Exception:
        return None


@router.get("/orange/stations", summary="Orange County PWD monitoring stations")
@cached(ttl=config.CACHE_TTL_LONG, prefix="ca:")
async def orange_stations() -> dict:
    """Return Orange County Public Works ALERT hydrology monitoring stations."""
    try:
        async with _client() as client:
            resp = await client.get(_OC_FEATURE_URL)
            resp.raise_for_status()
            data = resp.json()
    except httpx.TimeoutException:
        raise HTTPException(504, "Orange County ArcGIS request timed out")
    except httpx.HTTPStatusError as exc:
        raise HTTPException(exc.response.status_code, f"OC PWD ArcGIS returned HTTP {exc.response.status_code}")
    except Exception as exc:
        log.exception("Orange County fetch error")
        raise HTTPException(502, f"Orange County fetch error: {exc}")

    if "error" in data:
        raise HTTPException(502, f"ArcGIS error: {data['error']}")

    features = data.get("features", [])
    stations = []
    for feat in features:
        st = _oc_feat_to_station(feat)
        if st:
            stations.append(st.model_dump())

    return {
        "source": "oc_pwd",
        "network": "Orange County ALERT Hydrology",
        "hub_url": "https://hydrology-data-hub-site-ocpw.hub.arcgis.com/",
        "count": len(stations),
        "stations": stations,
    }


# ---------------------------------------------------------------------------
# ── 4. Santa Clara Valley Water ALERT API ──────────────────────────────────
# ---------------------------------------------------------------------------

_VW_BASE = "https://alertdata.valleywater.org"
_VW_STATIONS_URL = f"{_VW_BASE}/Sensor/all"
_VW_READINGS_URL = f"{_VW_BASE}/SensorData/{{station_id}}/range/{{start_date}}/{{end_date}}"

# SVWD sensor type mapping (numeric codes → semantic names)
_VW_SENSOR_MAP: dict[str, tuple[str, str]] = {
    "P":  ("precipitation",  "in"),
    "G":  ("stage",          "ft"),
    "Q":  ("flow",           "cfs"),
    "T":  ("water_temp",     "°F"),
    "A":  ("air_temp",       "°F"),
    "W":  ("wind_speed",     "mph"),
    "RH": ("humidity",       "%"),
    "B":  ("battery",        "V"),
}


def _vw_station_to_model(item: dict) -> Optional[Station]:
    try:
        # New API field names: gageId, name, latitude, longitude, gageType, etc.
        sid = str(
            item.get("gageId")
            or item.get("station_id")
            or item.get("id")
            or ""
        ).strip()
        if not sid:
            return None

        name = str(
            item.get("name")
            or item.get("station_name")
            or sid
        )
        lat = float(item.get("latitude") or item.get("lat") or 0.0)
        lon = float(item.get("longitude") or item.get("lon") or 0.0)

        # Infer parameter type from gageType field
        gage_type = str(item.get("gageType") or "").lower()
        sensor_codes = item.get("sensors") or item.get("sensor_types") or []
        params = [_VW_SENSOR_MAP[c][0] for c in sensor_codes if c in _VW_SENSOR_MAP]
        if not params:
            if "rain" in gage_type or "precip" in gage_type:
                params = ["precipitation"]
            elif "reservoir" in gage_type:
                params = ["reservoir_storage", "stage"]
            elif "stream" in gage_type or "creek" in gage_type:
                params = ["stage", "flow"]
            else:
                params = ["stage"]

        last_ts = None
        raw_ts = item.get("last_reading_time") or item.get("last_update")
        if raw_ts:
            try:
                last_ts = datetime.fromisoformat(str(raw_ts).replace("Z", "+00:00"))
            except ValueError:
                pass

        return Station(
            id=f"vw_{sid}",
            name=name,
            lat=lat,
            lon=lon,
            source="valleywater",
            network="scvwd_alert",
            params=params,
            last_update=last_ts,
            url=f"{_VW_BASE}/download.html",
            extra={
                k: v for k, v in item.items()
                if k not in ("latitude", "longitude")
                and v is not None
            },
        )
    except Exception:
        return None


def _vw_reading_to_model(item: dict, station_id: str, metadata: Optional[dict] = None) -> Optional[Reading]:
    """Parse a Valley Water reading.

    The new API returns readings with keys: id, timestamp, value, rating.
    Metadata (unit, type) comes from the parent response's "metadata" field.
    """
    try:
        raw_ts = item.get("timestamp") or item.get("time") or item.get("reading_time")
        if not raw_ts:
            return None
        ts = datetime.fromisoformat(str(raw_ts).replace("Z", "+00:00"))

        # Determine parameter and unit from metadata or sensor_type field
        if metadata:
            sensor_type = str(metadata.get("type") or "").lower()
            unit = str(metadata.get("unit") or "")
            if "rain" in sensor_type or "precip" in sensor_type:
                param = "precipitation"
            elif "reservoir" in sensor_type:
                param = "reservoir_storage"
            elif "stage" in sensor_type or "level" in sensor_type:
                param = "stage"
            elif "flow" in sensor_type or "discharge" in sensor_type:
                param = "flow"
            elif "temp" in sensor_type:
                param = "water_temp"
            else:
                param = sensor_type or "observation"
        else:
            sensor_code = str(item.get("sensor_type") or item.get("type") or "P")
            param, unit = _VW_SENSOR_MAP.get(sensor_code, ("observation", ""))

        raw_val = item.get("value") or item.get("reading")
        if raw_val is None:
            return None
        value = float(raw_val)

        return Reading(
            station_id=f"vw_{station_id}",
            timestamp=ts,
            parameter=param,
            value=value,
            unit=unit,
            quality=item.get("quality") or item.get("rating"),
            source="valleywater",
        )
    except Exception:
        return None


@router.get("/valleywater/stations", summary="Santa Clara Valley Water ALERT gauges")
@cached(ttl=config.CACHE_TTL_LONG, prefix="ca:")
async def valleywater_stations() -> dict:
    """Return Santa Clara Valley Water ALERT telemetry station list."""
    try:
        async with _client() as client:
            resp = await client.get(_VW_STATIONS_URL)
            resp.raise_for_status()
            data = resp.json()
    except httpx.TimeoutException:
        raise HTTPException(504, "Valley Water ALERT API timed out")
    except httpx.HTTPStatusError as exc:
        raise HTTPException(exc.response.status_code, f"Valley Water API returned HTTP {exc.response.status_code}")
    except Exception as exc:
        log.exception("Valley Water stations fetch error")
        raise HTTPException(502, f"Valley Water fetch error: {exc}")

    # The API may return a list directly or a dict with a key
    if isinstance(data, list):
        items = data
    else:
        items = (
            data.get("stations")
            or data.get("data")
            or data.get("results")
            or []
        )

    stations = []
    for item in items:
        st = _vw_station_to_model(item)
        if st:
            stations.append(st.model_dump())

    return {
        "source": "valleywater",
        "network": "Santa Clara Valley Water ALERT",
        "api_url": _VW_STATIONS_URL,
        "count": len(stations),
        "stations": stations,
    }


@router.get("/valleywater/stations/{station_id}/readings", summary="Valley Water time-series readings")
@cached(ttl=config.CACHE_TTL, prefix="ca:")
async def valleywater_readings(
    station_id: str,
    hours: int = Query(24, ge=1, le=8760, description="Hours of data to retrieve"),
) -> dict:
    """Return time-series readings from a Santa Clara Valley Water ALERT station."""
    # Strip optional prefix
    raw_id = station_id.removeprefix("vw_")

    now = datetime.now(timezone.utc)
    start_dt = now - timedelta(hours=hours)

    # New API uses /SensorData/{stationId}/range/{startDate}/{endDate}
    # with dates in MM-DD-YYYY format
    start_str = start_dt.strftime("%m-%d-%Y")
    end_str = now.strftime("%m-%d-%Y")
    url = _VW_READINGS_URL.format(
        station_id=raw_id,
        start_date=start_str,
        end_date=end_str,
    )

    try:
        async with _client() as client:
            resp = await client.get(url)
            resp.raise_for_status()
            data = resp.json()
    except httpx.TimeoutException:
        raise HTTPException(504, f"Valley Water ALERT API timed out for station {raw_id}")
    except httpx.HTTPStatusError as exc:
        if exc.response.status_code == 404:
            raise HTTPException(404, f"Station '{station_id}' not found in Valley Water ALERT")
        raise HTTPException(exc.response.status_code, f"Valley Water API returned HTTP {exc.response.status_code}")
    except Exception as exc:
        log.exception("Valley Water readings fetch error")
        raise HTTPException(502, f"Valley Water fetch error: {exc}")

    # New API returns {"comments":[], "metadata":{...}, "columns":{...}, "data":[...]}
    metadata = None
    if isinstance(data, list):
        items = data
    elif isinstance(data, dict):
        metadata = data.get("metadata")
        items = data.get("data") or data.get("readings") or data.get("results") or []
    else:
        items = []

    readings = []
    for item in items:
        if not isinstance(item, dict):
            continue
        r = _vw_reading_to_model(item, raw_id, metadata=metadata)
        if r:
            readings.append(r.model_dump())

    return {
        "source": "valleywater",
        "station_id": station_id,
        "hours_requested": hours,
        "count": len(readings),
        "readings": readings,
    }


# ---------------------------------------------------------------------------
# ── Overview endpoint ───────────────────────────────────────────────────────
# ---------------------------------------------------------------------------

@router.get("/overview", summary="California networks overview")
async def california_overview() -> dict:
    """Summary of all California water/rain monitoring networks and their status."""
    networks = [
        {
            "id": "cdec",
            "name": "CDEC — California Data Exchange Center",
            "operator": "California Department of Water Resources",
            "coverage": "Statewide — reservoirs, rivers, rain, snow",
            "station_count_approx": 2800,
            "auth_required": False,
            "update_frequency": "15-min to daily depending on sensor",
            "params": ["precipitation", "stage", "flow", "reservoir_storage", "water_temp", "air_temp", "snow_water"],
            "endpoints": [
                "/api/california/cdec/stations",
                "/api/california/cdec/stations/{station_id}",
                "/api/california/cdec/stations/{station_id}/readings",
            ],
            "data_url": "https://cdec.water.ca.gov",
            "notes": "No API key required. CSV download endpoint. Covers all major CA watersheds.",
        },
        {
            "id": "lacounty_dpw",
            "name": "LA County DPW Rainfall Network",
            "operator": "Los Angeles County Dept. of Public Works",
            "coverage": "Los Angeles County",
            "station_count_approx": 100,
            "auth_required": False,
            "update_frequency": "Near real-time",
            "params": ["precipitation"],
            "endpoints": [
                "/api/california/lacounty/stations",
                "/api/california/lacounty/stations/{station_id}",
            ],
            "data_url": "https://dpw.lacounty.gov/wrd/precip/",
            "notes": "ArcGIS REST API. Returns current rainfall accumulation totals (15 min through season).",
        },
        {
            "id": "oc_pwd",
            "name": "Orange County Public Works — ALERT Hydrology",
            "operator": "Orange County Public Works",
            "coverage": "Orange County",
            "station_count_approx": 150,
            "auth_required": False,
            "update_frequency": "Near real-time",
            "params": ["precipitation", "stage", "flow"],
            "endpoints": [
                "/api/california/orange/stations",
            ],
            "data_url": "https://hydrology-data-hub-site-ocpw.hub.arcgis.com/",
            "notes": "ArcGIS Hub feature service. Station list only; historical data via OC Hub.",
        },
        {
            "id": "valleywater",
            "name": "Santa Clara Valley Water — ALERT Telemetry",
            "operator": "Santa Clara Valley Water District",
            "coverage": "Santa Clara County",
            "station_count_approx": 200,
            "auth_required": False,
            "update_frequency": "15-min",
            "params": ["precipitation", "stage", "flow", "water_temp", "air_temp"],
            "endpoints": [
                "/api/california/valleywater/stations",
                "/api/california/valleywater/stations/{station_id}/readings",
            ],
            "data_url": "https://alertdata.valleywater.org",
            "notes": "Documented REST API. No key required. Covers South Bay watersheds.",
        },
    ]

    return {
        "platform": "wxsection.com",
        "region": "California, USA",
        "networks": networks,
        "total_networks": len(networks),
        "highest_priority": "cdec",
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }
