"""Simple in-memory TTL cache for API responses."""
import time
import asyncio
from typing import Any, Optional
from functools import wraps

_cache: dict[str, tuple[float, Any]] = {}
_lock = asyncio.Lock()


async def get(key: str) -> Optional[Any]:
    entry = _cache.get(key)
    if entry is None:
        return None
    expires, value = entry
    if time.time() > expires:
        _cache.pop(key, None)
        return None
    return value


async def set(key: str, value: Any, ttl: int = 300):
    _cache[key] = (time.time() + ttl, value)


async def invalidate(prefix: str = ""):
    keys = [k for k in _cache if k.startswith(prefix)]
    for k in keys:
        _cache.pop(k, None)


def cached(ttl: int = 300, prefix: str = ""):
    """Decorator for caching async function results."""
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            key = f"{prefix}{func.__name__}:{args}:{kwargs}"
            result = await get(key)
            if result is not None:
                return result
            result = await func(*args, **kwargs)
            await set(key, result, ttl)
            return result
        return wrapper
    return decorator


def stats() -> dict:
    now = time.time()
    total = len(_cache)
    alive = sum(1 for _, (exp, _) in _cache.items() if exp > now)
    return {"total_entries": total, "alive": alive, "expired": total - alive}
