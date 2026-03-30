"""California Extended Data Sources — wxsection.com source module.

Additional California environmental monitoring networks (no auth required):
  1. CA DWR Water Data Library  — 35,000+ groundwater wells (CKAN API)
  2. SFEI Regional Monitoring   — 15 continuous moored sensors in SF Bay
  3. CA Water Board             — impaired waters, SWAMP, sewer, cyanobacteria (ArcGIS)
  4. Lake Tahoe TERC            — 13 met stations (6 buoys + 7 dock)
  5. UC IPM Weather Database    — 350 stations across CA (CIMIS + non-CIMIS)
"""

import logging
from datetime import datetime, timezone
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

SOURCE_NAME = "California Extended"
SOURCE_DESCRIPTION = (
    "Additional California environmental data: DWR Water Data Library "
    "(groundwater wells), SFEI SF Bay moored sensors, CA Water Board "
    "(impaired waters / SWAMP / cyanobacteria), Lake Tahoe TERC met stations, "
    "and UC IPM Weather Database (350 ag-weather stations)."
)
SOURCE_META = {
    "coverage": "California, USA",
    "category": "water / environmental",
    "auth_required": False,
    "params": [
        "groundwater_level", "water_quality", "water_temp", "salinity",
        "dissolved_oxygen", "air_temp", "wind_speed", "humidity",
        "solar_radiation", "precipitation", "cyanobacteria",
    ],
    "update_frequency": "15-min to monthly depending on network",
    "url": "https://data.cnra.ca.gov",
}

# ---------------------------------------------------------------------------
# Router
# ---------------------------------------------------------------------------

router = APIRouter(prefix="/api/ca_extended", tags=["California Extended"])

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
# ── 1. CA DWR Water Data Library — Groundwater wells (CKAN API) ──────────
#    35,000+ wells with periodic groundwater-level measurements and
#    well completion reports.
#    CKAN endpoint: https://data.cnra.ca.gov/api/3/action/datastore_search
# ---------------------------------------------------------------------------

_CNRA_CKAN = "https://data.cnra.ca.gov/api/3/action"

# Known resource IDs for key datasets on CNRA CKAN
_DWR_RESOURCE_IDS = {
    # Periodic groundwater level measurements
    "gwl_measurements": "bfa9f262-24a1-45bd-8dc8-138571571571",
    # Well completion reports (metadata)
    "well_completions": "8da0c37b-e253-4b6a-87e4-9ced23a5e4d3",
    # Station/well metadata
    "stations": "306b8536-4dca-4745-8587-263e46e911e8",
}


@router.get("/dwr/datasets", summary="List available DWR CKAN datasets")
@cached(ttl=config.CACHE_TTL_LONG, prefix="ca_ext:")
async def dwr_datasets() -> dict:
    """List available DWR Water Data Library datasets on CNRA CKAN."""
    async with _client() as client:
        resp = await client.get(
            f"{_CNRA_CKAN}/package_search",
            params={"q": "groundwater level", "rows": 20},
        )
        resp.raise_for_status()
        data = resp.json()

    results = data.get("result", {}).get("results", [])
    datasets = []
    for ds in results:
        datasets.append({
            "id": ds.get("id"),
            "name": ds.get("name"),
            "title": ds.get("title"),
            "num_resources": ds.get("num_resources"),
            "url": f"https://data.cnra.ca.gov/dataset/{ds.get('name')}",
        })

    return {
        "source": "CA DWR Water Data Library (CNRA CKAN)",
        "dataset_count": len(datasets),
        "datasets": datasets,
        "fetched_at": datetime.now(timezone.utc).isoformat(),
    }


@router.get("/dwr/wells", summary="Search DWR groundwater wells")
@cached(ttl=config.CACHE_TTL_LONG, prefix="ca_ext:")
async def dwr_wells(
    county: Optional[str] = Query(None, description="County name filter (e.g. 'Los Angeles')"),
    basin: Optional[str] = Query(None, description="Groundwater basin name filter"),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
) -> dict:
    """Search DWR Water Data Library wells via CNRA CKAN datastore.

    Returns well metadata including location, depth, and basin info.
    """
    resource_id = _DWR_RESOURCE_IDS["stations"]
    filters: dict = {}
    if county:
        filters["COUNTY_NAME"] = county.upper()
    if basin:
        filters["BASIN_NAME"] = basin

    params: dict = {
        "resource_id": resource_id,
        "limit": limit,
        "offset": offset,
    }
    if filters:
        import json
        params["filters"] = json.dumps(filters)

    async with _client() as client:
        resp = await client.get(f"{_CNRA_CKAN}/datastore_search", params=params)
        if resp.status_code == 404:
            raise HTTPException(404, "DWR well stations resource not found on CKAN")
        resp.raise_for_status()
        data = resp.json()

    result = data.get("result", {})
    records = result.get("records", [])
    total = result.get("total", 0)

    stations = []
    for r in records:
        lat = r.get("LATITUDE") or r.get("latitude")
        lon = r.get("LONGITUDE") or r.get("longitude")
        stations.append(
            Station(
                id=str(r.get("SITE_CODE", r.get("STATION", ""))),
                name=r.get("WELL_NAME", r.get("STATION_NAME", "")),
                latitude=float(lat) if lat else None,
                longitude=float(lon) if lon else None,
                source="ca_dwr_wdl",
                params=["groundwater_level"],
                extra={
                    "county": r.get("COUNTY_NAME"),
                    "basin": r.get("BASIN_NAME"),
                    "well_depth_ft": r.get("TOTAL_DEPTH_FT"),
                },
            ).model_dump()
        )

    return {
        "source": "CA DWR Water Data Library",
        "total_records": total,
        "returned": len(stations),
        "stations": stations,
        "fetched_at": datetime.now(timezone.utc).isoformat(),
    }


@router.get("/dwr/wells/{site_code}/levels", summary="Groundwater levels for a well")
@cached(ttl=config.CACHE_TTL, prefix="ca_ext:")
async def dwr_well_levels(
    site_code: str,
    limit: int = Query(100, ge=1, le=5000),
) -> dict:
    """Periodic groundwater-level measurements for a single DWR well."""
    resource_id = _DWR_RESOURCE_IDS["gwl_measurements"]
    params = {
        "resource_id": resource_id,
        "limit": limit,
        "filters": f'{{"SITE_CODE":"{site_code}"}}',
        "sort": "MSMT_DATE desc",
    }

    async with _client() as client:
        resp = await client.get(f"{_CNRA_CKAN}/datastore_search", params=params)
        if resp.status_code == 404:
            raise HTTPException(404, "Groundwater levels resource not found")
        resp.raise_for_status()
        data = resp.json()

    records = data.get("result", {}).get("records", [])
    readings = []
    for r in records:
        ts = r.get("MSMT_DATE")
        val = r.get("GSE_WSE") or r.get("RPE_WSE")
        if ts and val is not None:
            readings.append(
                Reading(
                    timestamp=ts,
                    parameter="groundwater_level",
                    value=float(val),
                    unit="ft",
                    extra={
                        "measurement_method": r.get("MSMT_TYPE"),
                        "reference": r.get("REFERENCE_POINT"),
                    },
                ).model_dump()
            )

    return {
        "source": "CA DWR Water Data Library",
        "site_code": site_code,
        "reading_count": len(readings),
        "readings": readings,
        "fetched_at": datetime.now(timezone.utc).isoformat(),
    }


# ---------------------------------------------------------------------------
# ── 2. SFEI Regional Monitoring — SF Bay moored sensors ──────────────────
#    15 continuous moored sensors across San Francisco Bay measuring
#    water temp, salinity, dissolved oxygen at 15-minute intervals.
#    Data portal: https://www.sfei.org/data-center
#    Uses ERDDAP-style endpoints (publicly accessible).
# ---------------------------------------------------------------------------

_SFEI_BASE = "https://data.sfei.org/erddap/tabledap"

# Known SFEI moored-sensor dataset IDs on their ERDDAP instance
_SFEI_DATASETS = {
    "moored_sensors": "continuous_moored_sensors",
}


@router.get("/sfei/stations", summary="SFEI SF Bay moored sensor stations")
@cached(ttl=config.CACHE_TTL_LONG, prefix="ca_ext:")
async def sfei_stations() -> dict:
    """List SFEI continuous moored sensor stations in San Francisco Bay."""
    # SFEI ERDDAP metadata endpoint
    url = f"{_SFEI_BASE}/{_SFEI_DATASETS['moored_sensors']}.json"
    params = {
        "Station,Latitude,Longitude": "",
        "distinct()": "",
        "orderBy(\"Station\")": "",
    }

    async with _client() as client:
        try:
            resp = await client.get(url, params=params)
            resp.raise_for_status()
            data = resp.json()
        except (httpx.HTTPStatusError, httpx.RequestError) as exc:
            log.warning("SFEI ERDDAP request failed: %s", exc)
            # Return curated station list as fallback
            return _sfei_fallback_stations()

    rows = data.get("table", {}).get("rows", [])
    stations = []
    for row in rows:
        if len(row) >= 3:
            stations.append(
                Station(
                    id=str(row[0]),
                    name=f"SFEI — {row[0]}",
                    latitude=float(row[1]) if row[1] else None,
                    longitude=float(row[2]) if row[2] else None,
                    source="sfei",
                    params=["water_temp", "salinity", "dissolved_oxygen"],
                ).model_dump()
            )

    return {
        "source": "SFEI Regional Monitoring Program",
        "coverage": "San Francisco Bay",
        "station_count": len(stations),
        "stations": stations,
        "update_frequency": "15-min continuous",
        "fetched_at": datetime.now(timezone.utc).isoformat(),
    }


def _sfei_fallback_stations() -> dict:
    """Curated fallback list of SFEI moored sensor locations."""
    known = [
        ("Alviso Slough", 37.432, -121.979),
        ("Bay Bridge", 37.806, -122.351),
        ("Carquinez Strait", 38.063, -122.226),
        ("Dumbarton Bridge", 37.506, -122.119),
        ("Mare Island", 38.071, -122.252),
        ("Point San Pablo", 37.963, -122.415),
        ("Richmond Bridge", 37.933, -122.445),
        ("San Mateo Bridge", 37.583, -122.250),
        ("Suisun Bay", 38.060, -122.082),
    ]
    stations = [
        Station(
            id=name.lower().replace(" ", "_"),
            name=f"SFEI — {name}",
            latitude=lat,
            longitude=lon,
            source="sfei",
            params=["water_temp", "salinity", "dissolved_oxygen"],
        ).model_dump()
        for name, lat, lon in known
    ]
    return {
        "source": "SFEI Regional Monitoring Program (fallback list)",
        "coverage": "San Francisco Bay",
        "station_count": len(stations),
        "stations": stations,
        "note": "Curated fallback — live ERDDAP query failed",
        "fetched_at": datetime.now(timezone.utc).isoformat(),
    }


@router.get("/sfei/stations/{station_id}/readings", summary="SFEI sensor readings")
@cached(ttl=config.CACHE_TTL, prefix="ca_ext:")
async def sfei_readings(
    station_id: str,
    hours: int = Query(24, ge=1, le=168, description="Hours of data to retrieve"),
    param: Optional[str] = Query(
        None, description="Parameter filter: water_temp, salinity, dissolved_oxygen"
    ),
) -> dict:
    """Recent readings from an SFEI moored sensor station."""
    now = datetime.now(timezone.utc)
    start = now.replace(microsecond=0) - __import__("datetime").timedelta(hours=hours)

    url = f"{_SFEI_BASE}/{_SFEI_DATASETS['moored_sensors']}.json"
    time_constraint = (
        f"time>={start.strftime('%Y-%m-%dT%H:%M:%SZ')}"
        f"&Station=\"{station_id}\""
    )

    async with _client() as client:
        try:
            resp = await client.get(f"{url}?{time_constraint}")
            resp.raise_for_status()
            data = resp.json()
        except (httpx.HTTPStatusError, httpx.RequestError) as exc:
            raise HTTPException(502, f"SFEI ERDDAP error: {exc}")

    table = data.get("table", {})
    col_names = table.get("columnNames", [])
    rows = table.get("rows", [])

    readings = []
    for row in rows:
        record = dict(zip(col_names, row))
        ts = record.get("time")
        if not ts:
            continue
        for field, param_name, unit in [
            ("Temperature", "water_temp", "°C"),
            ("Salinity", "salinity", "PSU"),
            ("Dissolved_Oxygen", "dissolved_oxygen", "mg/L"),
        ]:
            val = record.get(field)
            if val is not None and (param is None or param == param_name):
                readings.append(
                    Reading(
                        timestamp=ts,
                        parameter=param_name,
                        value=float(val),
                        unit=unit,
                    ).model_dump()
                )

    return {
        "source": "SFEI Regional Monitoring",
        "station_id": station_id,
        "reading_count": len(readings),
        "readings": readings,
        "fetched_at": now.isoformat(),
    }


# ---------------------------------------------------------------------------
# ── 3. CA Water Board — ArcGIS MapServer services ───────────────────────
#    19 MapServer services covering impaired waters (303d), SWAMP
#    monitoring stations, sewer system overflows, cyanobacteria HABs, etc.
#    Base: https://gispublic.waterboards.ca.gov/arcgis/rest/services/
# ---------------------------------------------------------------------------

_WATERBOARD_BASE = "https://gispublic.waterboards.ca.gov/arcgis/rest/services"

# Key MapServer/FeatureServer endpoints
_WATERBOARD_SERVICES = {
    "impaired_waters": {
        "url": f"{_WATERBOARD_BASE}/Impaired_Waters/MapServer/0/query",
        "name": "Impaired Waters (303d)",
        "params": ["impairment"],
    },
    "swamp_stations": {
        "url": f"{_WATERBOARD_BASE}/SWAMP/MapServer/0/query",
        "name": "SWAMP Monitoring Stations",
        "params": ["water_quality"],
    },
    "sewer_overflows": {
        "url": f"{_WATERBOARD_BASE}/SSO/MapServer/0/query",
        "name": "Sanitary Sewer Overflows",
        "params": ["sewer_overflow"],
    },
    "cyanobacteria": {
        "url": f"{_WATERBOARD_BASE}/CyanoHAB/MapServer/0/query",
        "name": "Cyanobacteria / HABs",
        "params": ["cyanobacteria"],
    },
}


@router.get("/waterboard/services", summary="List CA Water Board ArcGIS services")
async def waterboard_services() -> dict:
    """List available CA State Water Board ArcGIS MapServer services."""
    services = []
    for key, svc in _WATERBOARD_SERVICES.items():
        services.append({
            "id": key,
            "name": svc["name"],
            "params": svc["params"],
            "query_url": svc["url"],
        })

    return {
        "source": "CA State Water Resources Control Board",
        "service_count": len(services),
        "services": services,
        "base_url": _WATERBOARD_BASE,
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }


@router.get("/waterboard/{service_id}/query", summary="Query a Water Board ArcGIS layer")
@cached(ttl=config.CACHE_TTL, prefix="ca_ext:")
async def waterboard_query(
    service_id: str,
    where: str = Query("1=1", description="SQL WHERE clause"),
    bbox: Optional[str] = Query(
        None, description="Bounding box: xmin,ymin,xmax,ymax (WGS84)"
    ),
    limit: int = Query(100, ge=1, le=2000),
) -> dict:
    """Query a CA Water Board ArcGIS MapServer layer.

    Supports impaired_waters, swamp_stations, sewer_overflows, cyanobacteria.
    """
    if service_id not in _WATERBOARD_SERVICES:
        raise HTTPException(
            404,
            f"Unknown service '{service_id}'. "
            f"Available: {', '.join(_WATERBOARD_SERVICES)}",
        )

    svc = _WATERBOARD_SERVICES[service_id]
    params: dict = {
        "where": where,
        "outFields": "*",
        "resultRecordCount": limit,
        "f": "json",
        "returnGeometry": "true",
        "outSR": "4326",
    }
    if bbox:
        parts = [float(x) for x in bbox.split(",")]
        if len(parts) == 4:
            params["geometry"] = bbox
            params["geometryType"] = "esriGeometryEnvelope"
            params["inSR"] = "4326"
            params["spatialRel"] = "esriSpatialRelIntersects"

    async with _client() as client:
        resp = await client.get(svc["url"], params=params)
        if resp.status_code >= 400:
            raise HTTPException(502, f"Water Board ArcGIS error: HTTP {resp.status_code}")
        data = resp.json()

    if "error" in data:
        raise HTTPException(502, f"ArcGIS error: {data['error'].get('message', data['error'])}")

    features = data.get("features", [])
    records = []
    for f in features:
        attr = f.get("attributes", {})
        geom = f.get("geometry", {})
        record = {**attr}
        if "x" in geom and "y" in geom:
            record["longitude"] = geom["x"]
            record["latitude"] = geom["y"]
        elif "rings" in geom:
            # Polygon — take centroid of first ring
            ring = geom["rings"][0]
            record["longitude"] = sum(p[0] for p in ring) / len(ring)
            record["latitude"] = sum(p[1] for p in ring) / len(ring)
        records.append(record)

    return {
        "source": f"CA Water Board — {svc['name']}",
        "service_id": service_id,
        "record_count": len(records),
        "records": records,
        "fetched_at": datetime.now(timezone.utc).isoformat(),
    }


# ---------------------------------------------------------------------------
# ── 4. Lake Tahoe TERC — Met stations and buoys ─────────────────────────
#    Tahoe Environmental Research Center (UC Davis)
#    13 stations: 6 buoys + 7 shore/dock stations
#    Real-time data portal: https://tahoe.ucdavis.edu/real-time-data
# ---------------------------------------------------------------------------

# TERC uses a custom API; curate known stations with coordinates
_TERC_STATIONS = [
    # Buoys
    {"id": "tb1", "name": "TB1 — Mid-lake buoy", "lat": 39.153, "lon": -120.063, "type": "buoy"},
    {"id": "tb2", "name": "TB2 — North buoy", "lat": 39.218, "lon": -120.082, "type": "buoy"},
    {"id": "tb3", "name": "TB3 — South buoy", "lat": 38.958, "lon": -120.050, "type": "buoy"},
    {"id": "tb4", "name": "TB4 — Emerald Bay buoy", "lat": 38.955, "lon": -120.098, "type": "buoy"},
    {"id": "tb5", "name": "TB5 — West Shore buoy", "lat": 39.100, "lon": -120.115, "type": "buoy"},
    {"id": "tb6", "name": "TB6 — East Shore buoy", "lat": 39.120, "lon": -119.980, "type": "buoy"},
    # Dock / shore stations
    {"id": "tahoe_city", "name": "Tahoe City dock", "lat": 39.167, "lon": -120.143, "type": "dock"},
    {"id": "homewood", "name": "Homewood dock", "lat": 39.083, "lon": -120.160, "type": "dock"},
    {"id": "sunnyside", "name": "Sunnyside dock", "lat": 39.130, "lon": -120.155, "type": "dock"},
    {"id": "camp_rich", "name": "Camp Richardson dock", "lat": 38.937, "lon": -120.046, "type": "dock"},
    {"id": "timber_cove", "name": "Timber Cove dock", "lat": 38.953, "lon": -119.980, "type": "dock"},
    {"id": "meeks_bay", "name": "Meeks Bay dock", "lat": 39.035, "lon": -120.133, "type": "dock"},
    {"id": "rubicon", "name": "Rubicon dock", "lat": 39.007, "lon": -120.115, "type": "dock"},
]

_TERC_BASE = "https://tepfsail50.execute-api.us-west-2.amazonaws.com/v1/report/ns"


@router.get("/terc/stations", summary="Lake Tahoe TERC met stations")
async def terc_stations() -> dict:
    """List Lake Tahoe TERC monitoring stations (buoys + dock stations)."""
    stations = [
        Station(
            id=s["id"],
            name=s["name"],
            latitude=s["lat"],
            longitude=s["lon"],
            source="terc_tahoe",
            params=["water_temp", "air_temp", "wind_speed", "dissolved_oxygen", "turbidity"],
            extra={"station_type": s["type"]},
        ).model_dump()
        for s in _TERC_STATIONS
    ]

    return {
        "source": "UC Davis — Tahoe Environmental Research Center",
        "coverage": "Lake Tahoe basin",
        "station_count": len(stations),
        "stations": stations,
        "update_frequency": "10–15 min (buoys), hourly (dock)",
        "data_url": "https://tahoe.ucdavis.edu/real-time-data",
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }


@router.get("/terc/stations/{station_id}/readings", summary="TERC station readings")
@cached(ttl=config.CACHE_TTL, prefix="ca_ext:")
async def terc_readings(
    station_id: str,
    hours: int = Query(24, ge=1, le=168, description="Hours of data to retrieve"),
) -> dict:
    """Recent readings from a Lake Tahoe TERC station.

    Attempts the TERC near-shore (NS) report API. Falls back to metadata-only
    if the API structure has changed.
    """
    known_ids = {s["id"] for s in _TERC_STATIONS}
    if station_id not in known_ids:
        raise HTTPException(404, f"Unknown TERC station '{station_id}'")

    now = datetime.now(timezone.utc)

    async with _client() as client:
        try:
            resp = await client.get(
                _TERC_BASE,
                params={"id": station_id, "hours": hours},
            )
            resp.raise_for_status()
            data = resp.json()
        except (httpx.HTTPStatusError, httpx.RequestError) as exc:
            log.warning("TERC API request failed for %s: %s", station_id, exc)
            return {
                "source": "TERC Lake Tahoe",
                "station_id": station_id,
                "reading_count": 0,
                "readings": [],
                "note": f"TERC API unavailable: {exc}",
                "data_url": "https://tahoe.ucdavis.edu/real-time-data",
                "fetched_at": now.isoformat(),
            }

    # Parse readings from whatever structure TERC returns
    readings = []
    if isinstance(data, list):
        for rec in data:
            ts = rec.get("TmStamp") or rec.get("timestamp") or rec.get("DateTime")
            for field, param_name, unit in [
                ("LS_Temp_Avg", "water_temp", "°C"),
                ("AirTemp", "air_temp", "°C"),
                ("WindSpeed", "wind_speed", "m/s"),
                ("DO", "dissolved_oxygen", "mg/L"),
                ("Turbidity", "turbidity", "NTU"),
            ]:
                val = rec.get(field)
                if ts and val is not None:
                    try:
                        readings.append(
                            Reading(
                                timestamp=str(ts),
                                parameter=param_name,
                                value=float(val),
                                unit=unit,
                            ).model_dump()
                        )
                    except (ValueError, TypeError):
                        pass

    return {
        "source": "TERC Lake Tahoe",
        "station_id": station_id,
        "reading_count": len(readings),
        "readings": readings,
        "fetched_at": now.isoformat(),
    }


# ---------------------------------------------------------------------------
# ── 5. UC IPM Weather Database — 350 stations across CA ──────────────────
#    University of California Integrated Pest Management
#    Aggregates CIMIS + ~200 non-CIMIS ag-weather stations
#    Portal: https://ipm.ucanr.edu/weather/ca-weather-data/
# ---------------------------------------------------------------------------

_UCIPM_BASE = "https://ipm.ucanr.edu/calludt.cgi"


@router.get("/ucipm/stations", summary="UC IPM weather station list")
@cached(ttl=config.CACHE_TTL_LONG, prefix="ca_ext:")
async def ucipm_stations() -> dict:
    """List UC IPM weather stations across California.

    The UC IPM Weather Database aggregates CIMIS and ~200 additional
    non-CIMIS agricultural weather stations.
    """
    # UC IPM uses a CGI endpoint that returns station lists; attempt JSON mode
    async with _client() as client:
        try:
            resp = await client.get(
                _UCIPM_BASE,
                params={"out": "json", "type": "stnlist"},
            )
            resp.raise_for_status()
            data = resp.json()
        except (httpx.HTTPStatusError, httpx.RequestError, ValueError) as exc:
            log.warning("UC IPM station list request failed: %s", exc)
            return _ucipm_fallback_stations()

    # Parse station list
    stations = []
    items = data if isinstance(data, list) else data.get("stations", data.get("data", []))
    for item in items:
        sid = item.get("stn_id") or item.get("id") or item.get("station")
        name = item.get("stn_name") or item.get("name") or str(sid)
        lat = item.get("lat") or item.get("latitude")
        lon = item.get("lng") or item.get("lon") or item.get("longitude")
        stations.append(
            Station(
                id=str(sid),
                name=name,
                latitude=float(lat) if lat else None,
                longitude=float(lon) if lon else None,
                source="ucipm",
                params=["air_temp", "humidity", "wind_speed", "solar_radiation",
                         "precipitation", "evapotranspiration"],
            ).model_dump()
        )

    return {
        "source": "UC IPM Weather Database",
        "coverage": "California (statewide)",
        "station_count": len(stations),
        "stations": stations,
        "note": "Aggregates CIMIS + ~200 non-CIMIS agricultural weather stations",
        "fetched_at": datetime.now(timezone.utc).isoformat(),
    }


def _ucipm_fallback_stations() -> dict:
    """Curated fallback for UC IPM — major regional stations."""
    known = [
        ("001", "Davis", 38.536, -121.776),
        ("002", "Five Points", 36.336, -120.113),
        ("006", "Parlier", 36.597, -119.514),
        ("044", "Riverside", 33.971, -117.319),
        ("047", "Pomona", 34.056, -117.813),
        ("075", "Irvine", 33.685, -117.721),
        ("080", "Fresno State", 36.821, -119.743),
        ("083", "Santa Barbara", 34.425, -119.845),
        ("087", "Salinas North", 36.770, -121.752),
        ("104", "De Laveaga (SC)", 36.991, -121.984),
        ("157", "Brentwood", 37.925, -121.718),
        ("171", "Modesto", 37.641, -120.993),
        ("213", "Merced", 37.321, -120.390),
    ]
    stations = [
        Station(
            id=sid,
            name=f"UC IPM — {name}",
            latitude=lat,
            longitude=lon,
            source="ucipm",
            params=["air_temp", "humidity", "wind_speed", "solar_radiation",
                     "precipitation", "evapotranspiration"],
        ).model_dump()
        for sid, name, lat, lon in known
    ]
    return {
        "source": "UC IPM Weather Database (fallback list)",
        "coverage": "California (statewide)",
        "station_count": len(stations),
        "stations": stations,
        "note": "Curated fallback — live UC IPM request failed",
        "fetched_at": datetime.now(timezone.utc).isoformat(),
    }


@router.get("/ucipm/stations/{station_id}/readings", summary="UC IPM station readings")
@cached(ttl=config.CACHE_TTL, prefix="ca_ext:")
async def ucipm_readings(
    station_id: str,
    days: int = Query(1, ge=1, le=14, description="Days of data (max 14)"),
) -> dict:
    """Recent weather data from a UC IPM station."""
    now = datetime.now(timezone.utc)
    end = now.strftime("%Y-%m-%d")
    start = (now - __import__("datetime").timedelta(days=days)).strftime("%Y-%m-%d")

    async with _client() as client:
        try:
            resp = await client.get(
                _UCIPM_BASE,
                params={
                    "out": "json",
                    "stn": station_id,
                    "startdate": start,
                    "enddate": end,
                },
            )
            resp.raise_for_status()
            data = resp.json()
        except (httpx.HTTPStatusError, httpx.RequestError, ValueError) as exc:
            raise HTTPException(502, f"UC IPM data request failed: {exc}")

    items = data if isinstance(data, list) else data.get("data", data.get("readings", []))
    readings = []
    for rec in items:
        ts = rec.get("date") or rec.get("Date") or rec.get("datetime")
        if not ts:
            continue
        for field, param_name, unit in [
            ("air_tmp_avg", "air_temp", "°F"),
            ("AirTmpAvg", "air_temp", "°F"),
            ("rel_hum_avg", "humidity", "%"),
            ("RelHumAvg", "humidity", "%"),
            ("wind_spd_avg", "wind_speed", "mph"),
            ("WindSpdAvg", "wind_speed", "mph"),
            ("sol_rad_avg", "solar_radiation", "W/m²"),
            ("SolRadAvg", "solar_radiation", "W/m²"),
            ("precip", "precipitation", "in"),
            ("Precip", "precipitation", "in"),
            ("eto", "evapotranspiration", "in"),
            ("ETo", "evapotranspiration", "in"),
        ]:
            val = rec.get(field)
            if val is not None:
                try:
                    readings.append(
                        Reading(
                            timestamp=str(ts),
                            parameter=param_name,
                            value=float(val),
                            unit=unit,
                        ).model_dump()
                    )
                except (ValueError, TypeError):
                    pass

    return {
        "source": "UC IPM Weather Database",
        "station_id": station_id,
        "reading_count": len(readings),
        "readings": readings,
        "fetched_at": now.isoformat(),
    }


# ---------------------------------------------------------------------------
# ── Overview endpoint ─────────────────────────────────────────────────────
# ---------------------------------------------------------------------------

@router.get("/overview", summary="California Extended sources overview")
async def ca_extended_overview() -> dict:
    """Summary of all California Extended data sources and their status."""
    networks = [
        {
            "id": "dwr_wdl",
            "name": "CA DWR Water Data Library",
            "operator": "California Department of Water Resources",
            "coverage": "Statewide — 35,000+ groundwater wells",
            "station_count_approx": 35000,
            "auth_required": False,
            "update_frequency": "Periodic (monthly to semi-annual measurements)",
            "params": ["groundwater_level"],
            "endpoints": [
                "/api/ca_extended/dwr/datasets",
                "/api/ca_extended/dwr/wells",
                "/api/ca_extended/dwr/wells/{site_code}/levels",
            ],
            "data_url": "https://data.cnra.ca.gov",
            "notes": "CKAN API — no key required. Well completion reports and periodic GWL measurements.",
        },
        {
            "id": "sfei",
            "name": "SFEI Regional Monitoring — SF Bay",
            "operator": "San Francisco Estuary Institute",
            "coverage": "San Francisco Bay — 15 moored sensor sites",
            "station_count_approx": 15,
            "auth_required": False,
            "update_frequency": "15-min continuous",
            "params": ["water_temp", "salinity", "dissolved_oxygen"],
            "endpoints": [
                "/api/ca_extended/sfei/stations",
                "/api/ca_extended/sfei/stations/{station_id}/readings",
            ],
            "data_url": "https://www.sfei.org/data-center",
            "notes": "ERDDAP-served continuous moored sensor data. No auth.",
        },
        {
            "id": "waterboard",
            "name": "CA State Water Resources Control Board",
            "operator": "California State Water Board",
            "coverage": "Statewide — impaired waters, SWAMP, sewer overflows, cyanobacteria",
            "station_count_approx": None,
            "auth_required": False,
            "update_frequency": "Varies by service (daily to annual)",
            "params": ["water_quality", "impairment", "cyanobacteria", "sewer_overflow"],
            "endpoints": [
                "/api/ca_extended/waterboard/services",
                "/api/ca_extended/waterboard/{service_id}/query",
            ],
            "data_url": "https://gispublic.waterboards.ca.gov/arcgis/rest/services/",
            "notes": "19 ArcGIS MapServer services. Supports spatial + attribute queries.",
        },
        {
            "id": "terc_tahoe",
            "name": "Lake Tahoe TERC — UC Davis",
            "operator": "UC Davis Tahoe Environmental Research Center",
            "coverage": "Lake Tahoe basin — 6 buoys + 7 dock stations",
            "station_count_approx": 13,
            "auth_required": False,
            "update_frequency": "10–15 min (buoys), hourly (dock)",
            "params": ["water_temp", "air_temp", "wind_speed", "dissolved_oxygen", "turbidity"],
            "endpoints": [
                "/api/ca_extended/terc/stations",
                "/api/ca_extended/terc/stations/{station_id}/readings",
            ],
            "data_url": "https://tahoe.ucdavis.edu/real-time-data",
            "notes": "6 mid-lake/nearshore buoys + 7 dock-mounted met stations.",
        },
        {
            "id": "ucipm",
            "name": "UC IPM Weather Database",
            "operator": "UC Agriculture and Natural Resources",
            "coverage": "Statewide — ~350 stations (CIMIS + non-CIMIS)",
            "station_count_approx": 350,
            "auth_required": False,
            "update_frequency": "Hourly to daily",
            "params": ["air_temp", "humidity", "wind_speed", "solar_radiation",
                        "precipitation", "evapotranspiration"],
            "endpoints": [
                "/api/ca_extended/ucipm/stations",
                "/api/ca_extended/ucipm/stations/{station_id}/readings",
            ],
            "data_url": "https://ipm.ucanr.edu/weather/ca-weather-data/",
            "notes": "Aggregates CIMIS + ~200 additional non-CIMIS ag-weather stations. No key required.",
        },
    ]

    return {
        "platform": "wxsection.com",
        "region": "California, USA",
        "networks": networks,
        "total_networks": len(networks),
        "highest_priority": "dwr_wdl",
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }
