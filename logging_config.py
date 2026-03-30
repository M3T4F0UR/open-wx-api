"""Structured logging configuration for the wxsection observation platform.

Provides JSON-formatted logs for aggregation tools and clean console output
for local development. Log level controlled via WX_LOG_LEVEL env var.

Usage:
    from logging_config import setup_logging
    setup_logging()  # call once at startup
"""
import logging
import json
import os
import sys
from datetime import datetime, timezone


class JSONFormatter(logging.Formatter):
    """Emit each log record as a single JSON line."""

    def format(self, record: logging.LogRecord) -> str:
        log_data = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        # Structured extras attached by callers via logger.info("msg", extra={...})
        for key in ("source", "duration_ms", "status_code", "endpoint",
                     "method", "error", "client", "cache_hit", "stations"):
            val = getattr(record, key, None)
            if val is not None:
                log_data[key] = val
        # Include exception info when present
        if record.exc_info and record.exc_info[0] is not None:
            log_data["exception"] = self.formatException(record.exc_info)
        return json.dumps(log_data, default=str)


class ConsoleFormatter(logging.Formatter):
    """Human-friendly single-line format for local development."""

    GREY = "\033[90m"
    GREEN = "\033[32m"
    YELLOW = "\033[33m"
    RED = "\033[31m"
    BOLD_RED = "\033[1;31m"
    RESET = "\033[0m"

    LEVEL_COLORS = {
        logging.DEBUG: GREY,
        logging.INFO: GREEN,
        logging.WARNING: YELLOW,
        logging.ERROR: RED,
        logging.CRITICAL: BOLD_RED,
    }

    def format(self, record: logging.LogRecord) -> str:
        color = self.LEVEL_COLORS.get(record.levelno, self.RESET)
        ts = datetime.now().strftime("%H:%M:%S")

        extras = []
        for key in ("source", "duration_ms", "status_code", "method", "endpoint"):
            val = getattr(record, key, None)
            if val is not None:
                if key == "duration_ms":
                    extras.append(f"{val:.0f}ms")
                else:
                    extras.append(str(val))
        suffix = f" [{' '.join(extras)}]" if extras else ""

        msg = record.getMessage()
        line = (
            f"{self.GREY}{ts}{self.RESET} "
            f"{color}{record.levelname:<7}{self.RESET} "
            f"{record.name}: {msg}{suffix}"
        )
        if record.exc_info and record.exc_info[0] is not None:
            line += "\n" + self.formatException(record.exc_info)
        return line


def setup_logging() -> None:
    """Configure the root logger and category loggers.

    Reads WX_LOG_LEVEL (default INFO) and WX_LOG_FORMAT (json|console,
    default console) from environment variables.
    """
    level_name = os.getenv("WX_LOG_LEVEL", "INFO").upper()
    level = getattr(logging, level_name, logging.INFO)
    log_format = os.getenv("WX_LOG_FORMAT", "console").lower()

    # Choose formatter
    if log_format == "json":
        formatter = JSONFormatter()
    else:
        formatter = ConsoleFormatter()

    # Single stream handler to stdout
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(formatter)

    # Configure root logger
    root = logging.getLogger()
    root.setLevel(level)
    # Remove any pre-existing handlers (e.g. basicConfig defaults)
    root.handlers.clear()
    root.addHandler(handler)

    # Category loggers — allow per-category tuning via env vars
    categories = {
        "server": "WX_LOG_LEVEL_SERVER",
        "sources": "WX_LOG_LEVEL_SOURCES",
        "cache": "WX_LOG_LEVEL_CACHE",
        "http": "WX_LOG_LEVEL_HTTP",
    }
    for name, env_key in categories.items():
        cat_logger = logging.getLogger(name)
        cat_level = os.getenv(env_key)
        if cat_level:
            cat_logger.setLevel(getattr(logging, cat_level.upper(), level))

    # Quiet down noisy third-party loggers
    for noisy in ("httpx", "httpcore", "uvicorn.access", "uvicorn.error",
                   "watchfiles", "hpack"):
        logging.getLogger(noisy).setLevel(max(level, logging.WARNING))

    logging.getLogger("server").info(
        "Logging configured",
        extra={"level": level_name, "format": log_format},
    )
