"""Request logging and metrics middleware for the observation platform.

Tracks per-source request counts, error rates, and response times in memory.
All state is process-local — suitable for single-instance deployments.
"""
import logging
import time
from collections import defaultdict
from threading import Lock
from typing import Any

from fastapi import Request, Response
from starlette.middleware.base import BaseHTTPMiddleware

logger = logging.getLogger("http")

# ---------------------------------------------------------------------------
# Metrics storage (process-local, thread-safe)
# ---------------------------------------------------------------------------

_lock = Lock()
_request_count: dict[str, int] = defaultdict(int)
_error_count: dict[str, int] = defaultdict(int)
_total_duration_ms: dict[str, float] = defaultdict(float)
_status_codes: dict[str, dict[int, int]] = defaultdict(lambda: defaultdict(int))
_max_duration_ms: dict[str, float] = defaultdict(float)
_min_duration_ms: dict[str, float] = {}


def get_metrics() -> dict[str, Any]:
    """Return a snapshot of current metrics."""
    with _lock:
        sources = sorted(
            set(_request_count.keys()) | set(_error_count.keys())
        )
        per_source = {}
        for src in sources:
            count = _request_count[src]
            errors = _error_count[src]
            total_ms = _total_duration_ms[src]
            per_source[src] = {
                "requests": count,
                "errors": errors,
                "error_rate": round(errors / count, 4) if count else 0.0,
                "avg_duration_ms": round(total_ms / count, 1) if count else 0.0,
                "max_duration_ms": round(_max_duration_ms[src], 1),
                "min_duration_ms": round(_min_duration_ms.get(src, 0.0), 1),
                "total_duration_ms": round(total_ms, 1),
                "status_codes": dict(_status_codes[src]),
            }

        total_requests = sum(_request_count.values())
        total_errors = sum(_error_count.values())
        return {
            "total_requests": total_requests,
            "total_errors": total_errors,
            "error_rate": round(total_errors / total_requests, 4) if total_requests else 0.0,
            "sources": per_source,
        }


def reset_metrics() -> None:
    """Clear all accumulated metrics."""
    with _lock:
        _request_count.clear()
        _error_count.clear()
        _total_duration_ms.clear()
        _status_codes.clear()
        _max_duration_ms.clear()
        _min_duration_ms.clear()


def _extract_source(path: str) -> str:
    """Extract source name from request path.

    /api/usgs/stations      -> usgs
    /api/noaa_weather/...   -> noaa_weather
    /api/metrics            -> _internal
    /api/health             -> _internal
    /static/...             -> _static
    /                       -> _root
    """
    parts = path.strip("/").split("/")
    if len(parts) >= 2 and parts[0] == "api":
        source = parts[1]
        # Internal endpoints that aren't source modules
        if source in ("sources", "health", "cache", "metrics"):
            return "_internal"
        return source
    if parts and parts[0] == "static":
        return "_static"
    return "_root"


class RequestMetricsMiddleware(BaseHTTPMiddleware):
    """Log every request and accumulate per-source metrics.

    Overhead: one time.perf_counter() pair + dict updates behind a lock —
    well under 0.1ms on any modern machine.
    """

    async def dispatch(self, request: Request, call_next) -> Response:
        start = time.perf_counter()

        try:
            response = await call_next(request)
        except Exception:
            # Let the exception propagate but still record the failure
            duration_ms = (time.perf_counter() - start) * 1000
            path = request.url.path
            source = _extract_source(path)
            with _lock:
                _request_count[source] += 1
                _error_count[source] += 1
                _total_duration_ms[source] += duration_ms
            logger.error(
                "Request failed with unhandled exception",
                extra={
                    "method": request.method,
                    "endpoint": path,
                    "source": source,
                    "duration_ms": round(duration_ms, 1),
                    "status_code": 500,
                },
            )
            raise

        duration_ms = (time.perf_counter() - start) * 1000
        path = request.url.path
        source = _extract_source(path)
        status = response.status_code

        with _lock:
            _request_count[source] += 1
            _total_duration_ms[source] += duration_ms
            _status_codes[source][status] += 1
            if duration_ms > _max_duration_ms[source]:
                _max_duration_ms[source] = duration_ms
            if source not in _min_duration_ms or duration_ms < _min_duration_ms[source]:
                _min_duration_ms[source] = duration_ms
            if status >= 400:
                _error_count[source] += 1

        # Choose log level based on status code
        if status >= 500:
            log_fn = logger.error
        elif status >= 400:
            log_fn = logger.warning
        else:
            log_fn = logger.info

        log_fn(
            "%s %s %d %.0fms",
            request.method,
            path,
            status,
            duration_ms,
            extra={
                "method": request.method,
                "endpoint": path,
                "source": source,
                "duration_ms": round(duration_ms, 1),
                "status_code": status,
            },
        )

        # Attach timing header for debugging
        response.headers["X-Response-Time-Ms"] = f"{duration_ms:.1f}"
        return response
