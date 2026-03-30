"""v2 API — category-based routing for all observation sources.

Assembles a single ``v2_router`` APIRouter (prefix ``/api/v2``) that
includes stub sub-routers for each category defined in the registry.
Categories with dedicated router modules (weather, water, air_quality,
fire, seismic, ...) are wired explicitly; remaining categories get
empty stubs so the URL namespace is reserved.
"""

from __future__ import annotations

from fastapi import APIRouter

from .registry import CATEGORIES

# ── Master v2 router ────────────────────────────────────────────────────────

v2_router = APIRouter(prefix="/api/v2", tags=["v2"])


# ── Category sub-routers (stubs for categories without a dedicated module) ─

# Categories that have a dedicated router module — skip stub generation.
_IMPLEMENTED_CATEGORIES = {
    "weather", "water", "air_quality", "fire", "seismic",
    "ocean", "climate", "infrastructure",
}

_category_routers: dict[str, APIRouter] = {}

for _cat_name, _cat_meta in CATEGORIES.items():
    if _cat_name in _IMPLEMENTED_CATEGORIES:
        continue
    _cat_router = APIRouter(
        prefix=f"/{_cat_name}",
        tags=[f"v2-{_cat_name}"],
    )
    _category_routers[_cat_name] = _cat_router
    v2_router.include_router(_cat_router)


# ── Discovery endpoint ──────────────────────────────────────────────────────

@v2_router.get("/categories")
async def list_categories():
    """List all v2 categories and the source modules they contain."""
    return {
        name: {
            "description": meta["description"],
            "sources": meta["sources"],
            "source_count": len(meta["sources"]),
        }
        for name, meta in CATEGORIES.items()
    }


# ── Include implemented category routers ───────────────────────────────────

from .weather import router as _weather_router  # noqa: E402

v2_router.include_router(_weather_router)

from .readings import router as _readings_router  # noqa: E402

v2_router.include_router(_readings_router)

from .water import router as _water_router  # noqa: E402

v2_router.include_router(_water_router)

from .health import router as _health_router  # noqa: E402

v2_router.include_router(_health_router)

from .air_quality import router as _air_quality_router  # noqa: E402

v2_router.include_router(_air_quality_router)

from .fire import router as _fire_router  # noqa: E402

v2_router.include_router(_fire_router)

from .seismic import router as _seismic_router  # noqa: E402

v2_router.include_router(_seismic_router)

from .ocean import router as _ocean_router  # noqa: E402

v2_router.include_router(_ocean_router)

from .climate import router as _climate_router  # noqa: E402

v2_router.include_router(_climate_router)

from .infrastructure import router as _infrastructure_router  # noqa: E402

v2_router.include_router(_infrastructure_router)


__all__ = ["v2_router", "CATEGORIES"]
