"""Shared resilient HTTP client with retry, circuit breaker, and connection pooling.

Usage from any source module:
    import http_client
    data = await http_client.fetch_json(url, params=params, source="usgs")
    text = await http_client.fetch_text(url, source="snotel")
"""
import asyncio
import logging
import time
from typing import Optional
from urllib.parse import urlparse

import httpx

import config

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Module-level state
# ---------------------------------------------------------------------------
_client: Optional[httpx.AsyncClient] = None
_client_lock = asyncio.Lock()

# Retry / circuit-breaker counters
_retry_counts: dict[str, int] = {}           # source -> total retries
_circuit_state: dict[str, dict] = {}          # source -> {failures, last_failure, open_until}
_circuit_lock = asyncio.Lock()

# Circuit breaker thresholds
_CB_FAILURE_THRESHOLD = 5       # consecutive failures to trip
_CB_FAILURE_WINDOW = 300        # seconds — failures older than this don't count
_CB_OPEN_DURATION = 30          # seconds — how long the circuit stays open

# Retryable HTTP status codes
_RETRYABLE_STATUS = {502, 503, 504}


# ---------------------------------------------------------------------------
# Connection-pooled client
# ---------------------------------------------------------------------------

async def get_client() -> httpx.AsyncClient:
    """Return a shared, connection-pooled httpx.AsyncClient (lazy init)."""
    global _client
    if _client is not None and not _client.is_closed:
        return _client
    async with _client_lock:
        # Double-check after acquiring lock
        if _client is not None and not _client.is_closed:
            return _client
        _client = httpx.AsyncClient(
            headers={
                "User-Agent": config.USER_AGENT,
                "Accept": "application/json",
            },
            timeout=config.HTTP_TIMEOUT,
            follow_redirects=True,
            limits=httpx.Limits(
                max_connections=100,
                max_keepalive_connections=20,
            ),
        )
        logger.info("Shared HTTP client initialized (pool: 100 conn, 20 keepalive)")
        return _client


async def close() -> None:
    """Close the shared HTTP client. Call on app shutdown."""
    global _client
    async with _client_lock:
        if _client is not None and not _client.is_closed:
            await _client.aclose()
            logger.info("Shared HTTP client closed")
        _client = None


# ---------------------------------------------------------------------------
# Circuit breaker
# ---------------------------------------------------------------------------

def _source_key(source: str, url: str) -> str:
    """Derive a circuit-breaker key from the source name or URL domain."""
    if source:
        return source
    parsed = urlparse(url)
    return parsed.netloc or "unknown"


async def _check_circuit(key: str) -> None:
    """Raise if the circuit is open for this source."""
    async with _circuit_lock:
        state = _circuit_state.get(key)
        if state is None:
            return
        now = time.monotonic()
        if state.get("open_until", 0) > now:
            raise CircuitOpenError(
                f"Circuit breaker open for '{key}' — too many consecutive failures. "
                f"Will retry after {state['open_until'] - now:.0f}s."
            )
        # If open_until has passed, reset
        if state.get("open_until", 0) > 0 and state["open_until"] <= now:
            state["failures"] = 0
            state["open_until"] = 0


async def _record_failure(key: str) -> None:
    """Record a failure and potentially trip the circuit."""
    async with _circuit_lock:
        now = time.monotonic()
        state = _circuit_state.setdefault(key, {"failures": 0, "last_failure": 0, "open_until": 0})

        # Reset if last failure was outside the window
        if now - state["last_failure"] > _CB_FAILURE_WINDOW:
            state["failures"] = 0

        state["failures"] += 1
        state["last_failure"] = now

        if state["failures"] >= _CB_FAILURE_THRESHOLD:
            state["open_until"] = now + _CB_OPEN_DURATION
            logger.warning(
                "Circuit breaker OPEN for '%s' — %d consecutive failures in %.0fs. "
                "Blocking requests for %ds.",
                key, state["failures"], _CB_FAILURE_WINDOW, _CB_OPEN_DURATION,
            )


async def _record_success(key: str) -> None:
    """Reset failure count on success."""
    async with _circuit_lock:
        if key in _circuit_state:
            _circuit_state[key]["failures"] = 0
            _circuit_state[key]["open_until"] = 0


class CircuitOpenError(Exception):
    """Raised when the circuit breaker is open for a source."""
    pass


# ---------------------------------------------------------------------------
# Retry helpers
# ---------------------------------------------------------------------------

def _is_retryable(exc: Exception) -> bool:
    """Return True if the exception warrants a retry."""
    if isinstance(exc, (httpx.TimeoutException, httpx.ConnectError)):
        return True
    if isinstance(exc, httpx.HTTPStatusError):
        return exc.response.status_code in _RETRYABLE_STATUS
    return False


# ---------------------------------------------------------------------------
# Public fetch functions
# ---------------------------------------------------------------------------

async def fetch_json(
    url: str,
    *,
    params: Optional[dict] = None,
    headers: Optional[dict] = None,
    timeout: Optional[float] = None,
    retries: int = 2,
    source: str = "",
) -> dict:
    """Fetch JSON with automatic retry on transient errors and circuit breaker.

    Args:
        url: The URL to fetch.
        params: Query parameters.
        headers: Extra headers (merged with defaults).
        timeout: Per-request timeout override (seconds).
        retries: Max retry attempts (default 2 = 3 total attempts).
        source: Source name for logging and circuit breaker tracking.

    Returns:
        Parsed JSON as a dict.

    Raises:
        httpx.HTTPStatusError: On non-retryable HTTP errors.
        httpx.TimeoutException: After all retries exhausted on timeout.
        CircuitOpenError: When circuit breaker is open.
    """
    key = _source_key(source, url)
    await _check_circuit(key)

    client = await get_client()
    last_exc: Optional[Exception] = None
    backoff_schedule = [0.5, 1.0, 2.0]

    for attempt in range(retries + 1):
        try:
            kwargs: dict = {"params": params}
            if headers:
                kwargs["headers"] = headers
            if timeout is not None:
                kwargs["timeout"] = timeout

            resp = await client.get(url, **kwargs)
            resp.raise_for_status()
            data = resp.json()
            await _record_success(key)
            return data

        except Exception as exc:
            last_exc = exc
            if not _is_retryable(exc) or attempt >= retries:
                await _record_failure(key)
                raise

            # Log and retry
            _retry_counts[key] = _retry_counts.get(key, 0) + 1
            delay = backoff_schedule[min(attempt, len(backoff_schedule) - 1)]
            logger.warning(
                "[%s] Retry %d/%d after %s (waiting %.1fs): %s",
                key, attempt + 1, retries, type(exc).__name__, delay, url,
            )
            await asyncio.sleep(delay)

    # Should not reach here, but just in case
    await _record_failure(key)
    raise last_exc  # type: ignore[misc]


async def fetch_text(
    url: str,
    *,
    params: Optional[dict] = None,
    headers: Optional[dict] = None,
    timeout: Optional[float] = None,
    retries: int = 2,
    source: str = "",
) -> str:
    """Fetch text with automatic retry on transient errors and circuit breaker.

    Same retry/circuit-breaker behavior as fetch_json, but returns response text.
    """
    key = _source_key(source, url)
    await _check_circuit(key)

    client = await get_client()
    last_exc: Optional[Exception] = None
    backoff_schedule = [0.5, 1.0, 2.0]

    for attempt in range(retries + 1):
        try:
            kwargs: dict = {"params": params}
            if headers:
                kwargs["headers"] = headers
            if timeout is not None:
                kwargs["timeout"] = timeout

            resp = await client.get(url, **kwargs)
            resp.raise_for_status()
            text = resp.text
            await _record_success(key)
            return text

        except Exception as exc:
            last_exc = exc
            if not _is_retryable(exc) or attempt >= retries:
                await _record_failure(key)
                raise

            _retry_counts[key] = _retry_counts.get(key, 0) + 1
            delay = backoff_schedule[min(attempt, len(backoff_schedule) - 1)]
            logger.warning(
                "[%s] Retry %d/%d after %s (waiting %.1fs): %s",
                key, attempt + 1, retries, type(exc).__name__, delay, url,
            )
            await asyncio.sleep(delay)

    await _record_failure(key)
    raise last_exc  # type: ignore[misc]


# ---------------------------------------------------------------------------
# Stats / health
# ---------------------------------------------------------------------------

def stats() -> dict:
    """Return HTTP client health stats: circuit breaker states, retry counts, pool info."""
    now = time.monotonic()

    circuit_info = {}
    for key, state in _circuit_state.items():
        open_until = state.get("open_until", 0)
        is_open = open_until > now
        circuit_info[key] = {
            "consecutive_failures": state["failures"],
            "state": "open" if is_open else "closed",
            "open_seconds_remaining": max(0, round(open_until - now, 1)) if is_open else 0,
        }

    pool_info = {}
    if _client is not None and not _client.is_closed:
        pool = _client._transport  # type: ignore[attr-defined]
        if hasattr(pool, "_pool"):
            inner = pool._pool
            pool_info = {
                "connections_in_pool": len(getattr(inner, "_connections", [])),
                "max_connections": 100,
                "max_keepalive": 20,
            }
        else:
            pool_info = {"status": "active", "max_connections": 100, "max_keepalive": 20}
    else:
        pool_info = {"status": "not_initialized"}

    return {
        "client_active": _client is not None and not _client.is_closed,
        "connection_pool": pool_info,
        "circuit_breakers": circuit_info,
        "total_retries_by_source": dict(_retry_counts),
    }
