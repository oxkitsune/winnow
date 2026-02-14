"""Structured logging setup for Winnow."""

from __future__ import annotations

from datetime import datetime, timezone
import json
import logging
from typing import Any


_RESERVED = {
    "name",
    "msg",
    "args",
    "levelname",
    "levelno",
    "pathname",
    "filename",
    "module",
    "exc_info",
    "exc_text",
    "stack_info",
    "lineno",
    "funcName",
    "created",
    "msecs",
    "relativeCreated",
    "thread",
    "threadName",
    "processName",
    "process",
    "message",
    "taskName",
}


class _JsonFormatter(logging.Formatter):
    """Emit one JSON object per log record."""

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "ts": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }

        for key, value in record.__dict__.items():
            if key in _RESERVED or key.startswith("_"):
                continue
            payload[key] = value

        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)

        return json.dumps(payload, sort_keys=True)


def configure_logging(level: str = "INFO") -> logging.Logger:
    """Configure and return the root Winnow logger."""

    logger = logging.getLogger("winnow")
    if logger.handlers:
        return logger

    handler = logging.StreamHandler()
    handler.setFormatter(_JsonFormatter())
    logger.addHandler(handler)
    logger.setLevel(level.upper())
    logger.propagate = False
    return logger


def get_logger(name: str = "winnow") -> logging.Logger:
    """Return a logger under the configured Winnow namespace."""

    configure_logging()
    return logging.getLogger(name)


def log_event(
    logger: logging.Logger,
    event: str,
    *,
    level: int = logging.INFO,
    **fields: Any,
) -> None:
    """Emit a structured event log line."""

    logger.log(level, event, extra={"event": event, **fields})
