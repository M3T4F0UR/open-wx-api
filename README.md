# Open Weather API

**Free, self-hosted API for real-time weather and water observations from 200,000+ stations worldwide. No API keys required.**

A drop-in alternative to paid services like Synoptic Data. Clone it, run it, query it. All data comes from public government and research networks — USGS, NOAA, NWS, SNOTEL, international met services, and 80+ more.

## Quick Start

```bash
git clone https://github.com/FahrenheitResearch/open-wx-api.git
cd open-wx-api
pip install -r requirements.txt
python server.py
```

Server starts at **http://localhost:5580**. Interactive API docs at **/docs**.

## What You Get

### V2 Unified API (recommended)

One API across all sources, with deduplication, quality tiers, and unit normalization.

```bash
# Browse categories
GET /api/v2/categories

# Weather stations by state
GET /api/v2/weather/stations?state=OR

# Water stations as GeoJSON
GET /api/v2/water/stations.geojson

# Readings near a point (cross-source, auto-deduplicated)
GET /api/v2/readings?lat=45.5&lon=-122.6&radius_km=20

# Source health dashboard
GET /api/v2/health
```

**8 categories:** weather, water, air_quality, fire, seismic, ocean, climate, infrastructure

### V1 Per-Source API

Direct access to individual source modules when you need source-specific features.

```bash
# USGS streamflow
GET /api/usgs/stations?state=OR
GET /api/usgs/readings?site=14211720&hours=24

# NOAA weather (ASOS/METAR + NDBC buoys)
GET /api/noaa_weather/stations?state=CA
GET /api/noaa_weather/ndbc/stations

# SNOTEL snowpack
GET /api/climate/snotel/stations?state=CO

# International
GET /api/international/uk_ea/stations
GET /api/hong_kong/current
GET /api/austria/current

# List everything available
GET /api/sources
```

## 89 Source Modules

All fetch from free, public APIs. No keys needed for the core set.

### Weather
NOAA ASOS/METAR, FAA Aviation Weather, state mesonets (OK, KS, MT, AZ, FL FAWN, NC ECONet, NE, ND), TexMesonet, AgriMet, CIMIS, WSU AgWeatherNet, DOT RWIS (40+ states), CAL FIRE RAWS, Synoptic Data

### Water & Hydrology
USGS NWIS (10K+ gauges), NOAA CO-OPS tides, USACE CWMS, Bureau of Reclamation, CA CDEC, LA County DPW, Valley Water, South FL WMD, Texas LCRA/TCEQ/TWDB, Iowa IFIS, Maricopa FCD, Portland BES Aquarius, 16 Aquarius instances worldwide

### Climate & Snow
SNOTEL, SCAN, ACIS/RCC, COOP networks (all 50 states), NASA POWER, NREL solar

### Air Quality
EPA AirNow, OpenAQ (global), Sensor.Community, state AQ networks

### Ocean & Coastal
NDBC buoys, ERDDAP (53+ servers), Chesapeake Bay, Great Lakes

### Seismic
USGS Earthquakes, EMSC, GeoNet NZ

### International (30+ countries)
UK Environment Agency, Germany DWD, Japan JMA, Canada EC, Sweden SMHI, Singapore NEA, Australia BoM, Austria GeoSphere, Poland IMGW, Iceland IMO, Hong Kong HKO, Taiwan CWA, South Korea KMA, Argentina SMN, Brazil INMET/ANA, Chile, Colombia, Mexico, Norway, Denmark, Finland, Spain, Netherlands, Switzerland, France, Baltic states, and more

### Citizen Science
CoCoRaHS, PurpleAir*, Ambient Weather*, WeatherFlow Tempest*

*\* Requires free API key*

## Data Quality Tiers

Every station is auto-tagged with a quality tier:

| Tier | Label | Description | Examples |
|------|-------|-------------|----------|
| 1 | Reference | Calibrated, strict QC, <1 hr lag | USGS, NOAA CO-OPS, OK Mesonet |
| 2 | Operational | Agency-maintained, regular QC | NWS ASOS, SNOTEL, RAWS, NDBC |
| 3 | Mesonet | Automated, moderate QC | COOP, CDEC, DOT RWIS, IEM |
| 4 | Archive | Low-frequency or historical | Aquarius, daily-only COOP |
| 5 | Caution | Stale, unverified, or personal | PurpleAir, personal stations |

## Configuration

Copy `.env.example` to `.env`. Everything works out of the box — API keys are optional and only needed for a handful of citizen/international sources.

```bash
cp .env.example .env
# Edit .env to add optional API keys for extra sources
```

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `WX_PORT` | 5580 | Server port |
| `WX_CACHE_TTL` | 300 | Cache TTL for readings (seconds) |
| `WX_CACHE_TTL_LONG` | 3600 | Cache TTL for station lists |
| `WX_HTTP_TIMEOUT` | 30 | Upstream request timeout |
| `WX_LOG_LEVEL` | INFO | Log level |
| `WX_LOG_FORMAT` | console | `console` or `json` |

## Architecture

```
server.py              # FastAPI app with auto-discovery
config.py              # Environment variables + API key registry
models.py              # Pydantic models (Station, Reading) + quality tiers
cache.py               # In-memory TTL cache
http_client.py         # Resilient HTTP client with circuit breaker + retry
logging_config.py      # Structured logging (console or JSON)
middleware.py          # Request metrics + timing
sources/               # 89 data source modules (each exports a FastAPI router)
sources/v2/            # Unified category-based API layer
```

**Auto-discovery:** Drop a new `.py` file in `sources/` that exports a `router`, restart, and it's live. No registration needed.

**Resilience:** Every upstream call goes through `http_client` with automatic retry (exponential backoff), per-source circuit breakers, and connection pooling.

## Requirements

- Python 3.11+
- Dependencies: `fastapi`, `uvicorn`, `httpx`, `pydantic`, `python-dotenv`

## License

MIT
