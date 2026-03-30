"""Shared data models for all weather/water data sources."""
from pydantic import BaseModel, model_validator
from typing import Optional
from datetime import datetime


# ---------------------------------------------------------------------------
# Quality tier registry — maps source names to data quality tiers.
# Tiers are auto-applied to Station objects that don't set quality_tier.
#
#   1 = Reference   — calibrated, strict QC, < 1 hr lag
#                     (USGS, NOAA CO-OPS tides, Oklahoma Mesonet)
#   2 = Operational — agency-maintained, regular QC, < 2 hr lag
#                     (NWS ASOS/METAR, NDBC, SNOTEL, RAWS, state mesonets)
#   3 = Mesonet     — automated network, moderate QC, some gaps
#                     (COOP, SCAN, IEM mixed, most DOT RWIS, CDEC, ACIS)
#   4 = Archive     — archival/low-frequency or metadata-only
#                     (Reclamation RISE, COOP daily-only, Aquarius)
#   5 = Caution     — stale data, coordinate issues, or personal-grade
#                     (CDOT RWIS frozen 2021, personal weather stations)
# ---------------------------------------------------------------------------
SOURCE_QUALITY_TIERS: dict[str, int] = {
    # ── Tier 1: Reference / Calibrated ──────────────────────────────────────
    "usgs":              1,   # USGS streamflow: 15-min, strict QC, < 1 hr lag
    "noaa_water":        1,   # CO-OPS tides: 6-min, 4-part QC, < 10 min lag
    "oklahoma_mesonet":  1,   # OK Mesonet: 5-min, 98% complete, 17-min lag

    # ── Tier 2: Operational / Agency-maintained ──────────────────────────────
    "faa_aviation":      2,   # FAA AWC: all US METAR stations (ASOS/AWOS/military/heliports)
    "noaa_arcgis":       2,   # NOAA ArcGIS: NWS METAR surface observations (~4,400 stations)
    "noaa_weather":      2,   # NWS ASOS/METAR + NDBC buoys
    "snotel":            2,   # NRCS SNOTEL: hourly, best-in-class QC
    "raws":              2,   # RAWS fire weather: 10-min, well-maintained
    "raws_dcp":          2,   # RAWS via IEM DCP (same underlying data)
    "kansas_mesonet":    2,   # KS Mesonet: 5-min, strict QC
    "fawn":              2,   # FL FAWN: 15-min agricultural mesonet
    "ndawn":             2,   # ND DAWN: research-grade agricultural
    "scan":              2,   # USDA SCAN: hourly, good soil+weather coverage
    "texmesonet":        2,   # TX TWDB TexMesonet: 5-15 min
    "iadot_rwis":        2,   # Iowa DOT RWIS: good pavement sensor data
    "caltrans_rwis":     2,   # Caltrans CWWP2: 1-min, road weather
    "montana_mesonet":   2,   # MT Mesonet
    "azmet":             2,   # AZ Meteorological Network: hourly, research-grade
    "tva":               2,   # TVA reservoir network
    "uk_ea":             2,   # UK Environment Agency
    "germany_dwd":       2,   # DWD (German Met Service)
    "japan_jma":         2,   # JMA (Japan Met Agency)
    "sweden_smhi":       2,   # SMHI (Swedish Met)
    "canada_ec":         2,   # Environment and Climate Change Canada
    "singapore_nea":     2,   # Singapore NEA
    "austria_geosphere": 2,   # GeoSphere Austria TAWES: 10-min, agency-maintained
    "hong_kong":         2,   # HKO: 1-min updates, government-operated
    "asia_east":         2,   # Taiwan CWA + South Korea KMA: national met services
    "cimis":             2,   # CA CIMIS (agricultural)
    "synoptic":          2,   # Synoptic Data aggregates operational networks
    "wsu_agweathernet":  2,   # WSU AgWeatherNet: 15-min, university-operated ag network

    # ── Tier 3: Mesonet / Moderate QC ───────────────────────────────────────
    "iem":               3,   # IEM mixed networks — quality varies by sub-net
    "climate":           3,   # ACIS/COOP: daily-only in most cases
    "colorado_dwr":      3,   # CO DWR: streamflow, daily, older data
    "cdec":              3,   # CA CDEC: varied sensors, gaps common
    "federal_water":     3,   # USACE CWMS + Reclamation (mixed freshness)
    "nm_water":          3,   # NM Water Data Initiative: OSE/USGS/ABQ/EBID via FROST
    "erddap":            3,   # ERDDAP GLERL: satellite/cloud-dependent
    "maryland_chart":    3,   # MD ChartCenter water quality
    "sfwmd":             3,   # South FL Water Management District
    "florida_water":     3,   # FL WMDs: SJRWMD, SWFWMD, SRWMD
    "texas_water":       3,   # TX: LCRA Hydromet (portal-only), TCEQ SWQMIS, TWDB groundwater
    "chesapeake_bay":    3,   # Chesapeake Bay Program DataHub: cruise-based WQ
    "srbc_cim":          3,   # SRBC CIM: 15-min instream monitoring, Susquehanna Basin
    "maricopa_fcd":      3,   # Maricopa County FCD ALERT: rain/stream/weather
    "lacounty_dpw":      3,   # LA County DPW rain gauges
    "oc_pwd":            3,   # Orange County Public Works
    "valleywater":       3,   # Valley Water (Santa Clara)
    "brazil_inmet":      3,   # INMET Brazil
    "mexico_conagua":    3,   # CONAGUA SMN Mexico: forecast data, no obs QC
    "chile_meteochile":  2,   # MeteoChile DMC: ~300 EMA stations, token auth
    "colombia_ideam":    3,   # Colombia IDEAM: ~2,800 stations, Socrata API
    "poland_imgw":       2,   # IMGW-PIB Poland: synop + hydro, ~60 min
    "argentina_smn":     2,   # Argentina SMN: national met service, ~125 stations
    "usbr_agrimet":      3,   # AgriMet: 7-9 hr publication lag
    "ne_buoys":          2,   # CBIBS + Narragansett Bay: 6-15 min, agency-operated

    # ── Tier 3 continued: state COOP climate networks (IEM, daily obs) ──────
    "nebraska_climate":       3,  "texas_climate":         3,
    "minnesota_climate":      3,  "wisconsin_climate":     3,
    "missouri_climate":       3,  "illinois_climate":      3,
    "north_dakota_climate":   3,  "south_dakota_climate":  3,
    "tennessee_climate":      3,  "north_carolina_climate":3,
    "georgia_climate":        3,  "kansas_climate":        3,
    "wyoming_climate":        3,  "oregon_climate":        3,
    "washington_climate":     3,  "idaho_climate":         3,
    "new_mexico_climate":     3,  "utah_climate":          3,
    "virginia_climate":       3,  "south_carolina_climate":3,
    "kentucky_climate":       3,  "alabama_climate":       3,
    "arkansas_climate":       3,  "florida_climate":       3,
    "hawaii_climate":         3,  "alaska_climate":        3,
    "louisiana_climate":      3,  "mississippi_climate":   3,
    "michigan_climate":       3,  "indiana_climate":       3,
    "ohio_climate":           3,  "pennsylvania_climate":  3,
    "new_york_climate":       3,  "new_jersey_climate":    3,
    "connecticut_climate":    3,  "massachusetts_climate": 3,
    "maine_climate":          3,  "new_hampshire_climate": 3,
    "vermont_climate":        3,  "rhode_island_climate":  3,
    "west_virginia_climate":  3,  "maryland_climate":      3,
    "delaware_climate":       3,  "iowa_climate":          3,
    "colorado_climate":       3,  "nevada_climate":        3,
    "puerto_rico_climate":    3,  "dc_climate":            3,
    # ── Tier 3 continued: state RWIS via IEM (source format: rwis_{state}) ──
    "rwis_ak": 3,  "rwis_ar": 3,  "rwis_az": 3,  "rwis_ca": 3,
    "rwis_co": 3,  "rwis_ct": 3,  "rwis_de": 3,  "rwis_fl": 3,
    "rwis_ga": 3,  "rwis_ia": 3,  "rwis_id": 3,  "rwis_il": 3,
    "rwis_in": 3,  "rwis_ks": 3,  "rwis_ky": 3,  "rwis_ma": 3,
    "rwis_md": 3,  "rwis_me": 3,  "rwis_mi": 3,  "rwis_mn": 3,
    "rwis_mo": 3,  "rwis_mt": 3,  "rwis_nd": 3,  "rwis_ne": 3,
    "rwis_nh": 3,  "rwis_nj": 3,  "rwis_nm": 3,  "rwis_nv": 3,
    "rwis_ny": 3,  "rwis_oh": 3,  "rwis_or": 3,  "rwis_pa": 3,
    "rwis_sd": 3,  "rwis_tn": 3,  "rwis_ut": 3,  "rwis_va": 3,
    "rwis_vt": 3,  "rwis_wa": 3,  "rwis_wi": 3,  "rwis_wv": 3,
    "rwis_wy": 3,

    # ── Tier 4: Archive / Low-frequency ─────────────────────────────────────
    "aquarius":          4,   # Aquarius data management: archival/historical
    "mesonets":          4,   # Generic mesonet aggregator endpoint

    # ── New sources (2026-03-20 integration) ──────────────────────────────
    # Tier 2: National met services & agency-operated
    "hong_kong":         2,   # HKO: ~40 stations, 1-min updates
    "australia_bom":     2,   # BoM: ~700 stations, 30-min
    "argentina_smn":     2,   # SMN: ~125 stations, hourly
    "austria_geosphere": 2,   # GeoSphere: ~270 TAWES, 10-min
    "poland_imgw":       2,   # IMGW: ~60 synop + 800 hydro
    "iceland_imo":       2,   # IMO: ~90 stations, hourly
    "lithuania_meteo":   2,   # Meteo.lt: ~16 stations
    "estonia_ilmateenistus": 2, # Ilmateenistus: ~20 stations
    "chile_meteochile":  2,   # MeteoChile: ~300 stations
    "norway_frost":      2,   # MET.no Frost: ~370 stations
    "denmark_dmi":       2,   # DMI: ~90 stations
    "finland_fmi":       2,   # FMI: ~400 stations
    "spain_aemet":       2,   # AEMET: ~800 stations
    "netherlands_knmi":  2,   # KNMI: ~50 stations
    "switzerland_meteoswiss": 2, # MeteoSwiss: ~260 stations
    "france_meteofrance":2,   # Meteo-France: ~60 SYNOP
    "taiwan_cwa":        2,   # CWA: ~700 stations
    "korea_kma":         2,   # KMA: ~558 stations
    "airnow":            2,   # EPA AirNow: ~2,500 stations
    "usgs_earthquake":   2,   # USGS: ~2,000 seismic
    "emsc_earthquake":   2,   # EMSC: Euro-Med seismicity
    "geonet_earthquake": 2,   # GeoNet NZ: multi-hazard
    "indonesia_bmkg":    2,   # BMKG: ~180 synoptic
    "brazil_ana":        2,   # ANA Hidroweb: ~4,600 hydro
    # Tier 3: Aggregated / moderate QC
    "openaq":            3,   # OpenAQ: 20,700 locations, mixed quality
    "colombia_ideam":    3,   # IDEAM: ~2,800 stations
    "mexico_conagua":    3,   # CONAGUA: ~270 EMAs (forecast data)
    "glos_seagull":      3,   # GLOS: ~150 Great Lakes platforms
    "geoglows":          3,   # GEOGloWS: global river forecasts
    "iowa_ifis":         3,   # Iowa IFIS: ~300 bridge sensors, automated
    "iowa_aquia":        3,   # Iowa DNR AQuIA: ambient water quality
    # Tier 5: Citizen / caution
    "sensor_community":  5,   # Sensor.Community: 35,000 low-cost sensors
    "waggle_sage":       3,   # Waggle/SAGE: 100+ research-grade edge nodes (Argonne/UChicago)
    "opensensemap":      5,   # OpenSenseMap: ~14,000 IoT sensors
    "safecast":          5,   # Safecast: citizen radiation

    # ── Tier 2 continued: fire weather ───────────────────────────────────────
    "calfire_raws":      2,   # CAL FIRE RAWS: ~4,483 stations, 10-min obs
    "nifc_raws":         2,   # NIFC RAWS fire danger: ~946 stations, daily ERC/BI

    # ── Tier 5: Caution ─────────────────────────────────────────────────────
    "codot_rwis":        5,   # CDOT ArcGIS: data FROZEN since Sept 2021
    "citizen":           5,   # Personal weather stations (generic)
    "purpleair":         5,   # PurpleAir: consumer air quality sensors
    "tempest":           5,   # WeatherFlow Tempest: personal stations
    "ambient":           5,   # Ambient Weather: personal stations
    "cocorahs":          5,   # CoCoRaHS: citizen science (manual, unverified)
    "sensor_community":  5,   # Sensor.Community (Luftdaten): citizen air quality sensors
}


class Station(BaseModel):
    """A monitoring station from any data source.

    quality_tier is auto-populated from SOURCE_QUALITY_TIERS based on
    the `source` field if not explicitly set by the data source module.

    Tier meanings:
      1 = Reference   — calibrated, strict QC, < 1 hr lag
      2 = Operational — agency-maintained, regular QC, < 2 hr lag
      3 = Mesonet     — automated network, moderate QC, some gaps
      4 = Archive     — archival or low-frequency data
      5 = Caution     — stale, coordinate issues, or personal-grade
    """
    id: str
    name: str
    lat: float
    lon: float
    source: str          # e.g. "usgs", "uk_ea", "portland_bes"
    network: str = ""    # sub-network within source
    state: Optional[str] = None       # US state abbreviation or admin region
    category: Optional[str] = None    # v2 category: weather, water, air_quality, ...
    params: list[str] = []  # what this station measures
    elevation: Optional[float] = None
    url: Optional[str] = None        # link to original data page
    last_update: Optional[datetime] = None
    quality_tier: Optional[int] = None  # 1=Reference, 2=Operational, 3=Mesonet, 4=Archive, 5=Caution
    extra: dict = {}     # source-specific metadata

    @model_validator(mode="after")
    def _auto_quality_tier(self) -> "Station":
        """Fill quality_tier from the registry if the source didn't set it."""
        if self.quality_tier is None and self.source in SOURCE_QUALITY_TIERS:
            self.quality_tier = SOURCE_QUALITY_TIERS[self.source]
        return self


class Reading(BaseModel):
    """A single observation/measurement."""
    station_id: str
    timestamp: datetime
    parameter: str       # e.g. "precipitation", "temperature", "wind_speed"
    value: float
    unit: str            # e.g. "mm", "°C", "m/s"
    quality: Optional[str] = None
    source: str = ""


class StationDetail(BaseModel):
    """Station with its latest readings."""
    station: Station
    readings: list[Reading] = []


class Source(BaseModel):
    """Metadata about a data source."""
    id: str
    name: str
    description: str
    station_count: Optional[int] = None
    url: str
    auth_required: bool = False
    update_frequency: str = ""
    coverage: str = ""
    params: list[str] = []
    category: str = ""   # "water", "weather", "air_quality", "lightning", etc.
