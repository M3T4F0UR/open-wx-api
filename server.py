"""Open Weather API — Free, keyless access to 200,000+ weather & water stations worldwide.

Self-hosted FastAPI server that auto-discovers and mounts all source routers.
No API keys required for the vast majority of data sources.
"""
import importlib
import logging
import pkgutil
from contextlib import asynccontextmanager
from pathlib import Path
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import config
import cache
import http_client
from logging_config import setup_logging
from middleware import RequestMetricsMiddleware, get_metrics, reset_metrics
from sources.v2 import v2_router

setup_logging()
logger = logging.getLogger("server")


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup and shutdown hooks."""
    yield
    await http_client.close()


app = FastAPI(
    title="Open Weather API",
    description=(
        "Free, self-hosted API aggregating real-time weather & water observations "
        "from 175+ public data sources worldwide. No API keys needed."
    ),
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)
app.add_middleware(RequestMetricsMiddleware)

# Auto-discover and mount all source routers from sources/
sources_dir = Path(__file__).parent / "sources"
loaded_sources = []

if sources_dir.exists():
    for module_info in pkgutil.iter_modules([str(sources_dir)]):
        try:
            mod = importlib.import_module(f"sources.{module_info.name}")
            if hasattr(mod, "router"):
                app.include_router(mod.router)
                loaded_sources.append(module_info.name)
                logger.info("Loaded source: %s", module_info.name)
        except Exception as e:
            logger.error("Failed to load source %s: %s", module_info.name, e)

# Mount the v2 category-based unified API
app.include_router(v2_router)


@app.get("/")
async def root():
    """API overview and quick start."""
    return {
        "name": "Open Weather API",
        "version": "1.0.0",
        "description": "Free, keyless access to 200,000+ weather & water stations worldwide",
        "sources_loaded": len(loaded_sources),
        "docs": "/docs",
        "endpoints": {
            "v2_categories": "/api/v2/categories",
            "v2_weather": "/api/v2/weather/stations",
            "v2_water": "/api/v2/water/stations",
            "v2_readings": "/api/v2/readings?lat=45.5&lon=-122.6",
            "v1_sources": "/api/sources",
            "health": "/api/health",
        },
    }


@app.get("/api/sources")
async def list_sources():
    """List all loaded data source modules and their endpoints."""
    source_info = []
    for name in loaded_sources:
        mod = importlib.import_module(f"sources.{name}")
        info = {
            "id": name,
            "name": getattr(mod, "SOURCE_NAME", name),
            "description": getattr(mod, "SOURCE_DESCRIPTION", ""),
            "prefix": f"/api/{name}",
        }
        if hasattr(mod, "SOURCE_META"):
            info.update(mod.SOURCE_META)
        source_info.append(info)
    return {"sources": source_info, "count": len(source_info)}


@app.get("/api/health")
async def health():
    return {"status": "ok", "sources_loaded": len(loaded_sources), "sources": loaded_sources}


@app.get("/api/cache/stats")
async def cache_stats():
    return cache.stats()


@app.get("/api/http/stats")
async def http_stats():
    """HTTP client health: circuit breaker states, retry counts, connection pool info."""
    return http_client.stats()


@app.get("/api/metrics")
async def metrics():
    """Per-source request counts, error rates, and response times."""
    return get_metrics()


@app.post("/api/metrics/reset")
async def metrics_reset():
    """Reset all accumulated metrics counters."""
    reset_metrics()
    return {"status": "ok"}


if __name__ == "__main__":
    import uvicorn
    logger.info(
        "Open Weather API starting on http://%s:%s",
        config.HOST, config.PORT,
    )
    logger.info("Loaded %d sources: %s", len(loaded_sources), ", ".join(loaded_sources))
    uvicorn.run(app, host=config.HOST, port=config.PORT)
