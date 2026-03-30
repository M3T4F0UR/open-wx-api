"""Shared configuration for the Open Weather API."""
import os

# Server
HOST = os.getenv("WX_HOST", "0.0.0.0")
PORT = int(os.getenv("WX_PORT", "5580"))

# Default cache TTL in seconds
CACHE_TTL = int(os.getenv("WX_CACHE_TTL", "300"))  # 5 min
CACHE_TTL_LONG = int(os.getenv("WX_CACHE_TTL_LONG", "3600"))  # 1 hr (for station lists)

# HTTP client settings
HTTP_TIMEOUT = int(os.getenv("WX_HTTP_TIMEOUT", "30"))
USER_AGENT = "open-wx-api/1.0 (open-source weather data aggregator; github.com/FahrenheitResearch/open-wx-api)"

# API keys (optional — many sources are free/no-auth)
SYNOPTIC_TOKEN = os.getenv("SYNOPTIC_TOKEN", "")
PURPLEAIR_KEY = os.getenv("PURPLEAIR_KEY", "")
AMBIENT_APP_KEY = os.getenv("AMBIENT_APP_KEY", "")
AMBIENT_API_KEY = os.getenv("AMBIENT_API_KEY", "")
WEATHERFLOW_TOKEN = os.getenv("WEATHERFLOW_TOKEN", "")
NETATMO_CLIENT_ID = os.getenv("NETATMO_CLIENT_ID", "")
NETATMO_CLIENT_SECRET = os.getenv("NETATMO_CLIENT_SECRET", "")
CIMIS_APP_KEY = os.getenv("CIMIS_APP_KEY", "")
AIRNOW_API_KEY = os.getenv("AIRNOW_API_KEY", "")
KNMI_API_KEY = os.getenv("KNMI_API_KEY", "")
FROST_CLIENT_ID = os.getenv("FROST_CLIENT_ID", "")
AEMET_API_KEY = os.getenv("AEMET_API_KEY", "")
METEOFRANCE_KEY = os.getenv("METEOFRANCE_KEY", "")
KMA_SERVICE_KEY = os.getenv("KMA_SERVICE_KEY", "")
CWA_AUTH_KEY = os.getenv("CWA_AUTH_KEY", "")
OPENAQ_API_KEY = os.getenv("OPENAQ_API_KEY", "")
DMI_API_KEY = os.getenv("DMI_API_KEY", "")
FMI_API_KEY = os.getenv("FMI_API_KEY", "")
METEOCHILE_USER = os.getenv("METEOCHILE_USER", "")
METEOCHILE_TOKEN = os.getenv("METEOCHILE_TOKEN", "")
NEON_TOKEN = os.getenv("NEON_TOKEN", "")
CBIBS_API_KEY = os.getenv("CBIBS_API_KEY", "")
NC_ECONET_HASH = os.getenv("NC_ECONET_HASH", "")

# AQUARIUS WebPortal instances
AQUARIUS_INSTANCES = {
    "portland_bes": "https://aquarius.portlandoregon.gov",
    "nps": "https://irma.nps.gov/aqwebportal",
    "usibwc": "https://waterdata.ibwc.gov/AQWebportal",
    "sjrwmd": "https://secure.sjrwmd.com/aqportal",
    "orlando": "https://hydrology.orlando.gov",
    "nwfwmd": "https://aquarius-web.nwfwmd.state.fl.us",
    "volusia": "https://aquariusweb.vcgov.org/AQWebPortal",
    "nebraska": "https://nednr.aquaticinformatics.net",
    "bc": "https://aqrt.nrs.gov.bc.ca",
    "somerset_uk": "https://cfh.aquaticinformatics.net",
    "south_australia": "https://water.data.sa.gov.au",
    "northern_territory": "https://ntg.aquaticinformatics.net",
    "waternsw": "https://realtimedata.waternsw.com.au",
    "niwa_nz": "https://hydrowebportal.niwa.co.nz",
    "ideam_colombia": "https://aquariuswebportal.ideam.gov.co",
    "malaysia": "https://hydroportal.water.gov.my",
}
