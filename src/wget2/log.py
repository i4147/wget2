"""Structured logging with credential redaction.

All diagnostics go to stderr (or a log file) so that ``--json`` reports on
stdout stay machine-parsable.
"""

from __future__ import annotations

import json
import logging
import sys
from pathlib import Path
from typing import Any, Final

from .util import redact

LOGGER_NAME: Final[str] = "wget2"
LEVELS: Final[dict[str, int]] = {
    "critical": logging.CRITICAL,
    "error": logging.ERROR,
    "warning": logging.WARNING,
    "info": logging.INFO,
    "debug": logging.DEBUG,
    "trace": 5,
}
logging.addLevelName(5, "TRACE")

_RESERVED: Final[frozenset[str]] = frozenset(
    {
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
        "taskName",
        "message",
        "asctime",
    }
)


class RedactionFilter(logging.Filter):
    """Scrub credentials from the rendered message and any extra fields."""

    def filter(self, record: logging.LogRecord) -> bool:  # noqa: A003
        try:
            record.msg = redact(record.getMessage())
            record.args = ()
        except Exception:  # pragma: no cover - defensive
            pass
        for key, value in list(record.__dict__.items()):
            if key in _RESERVED or key.startswith("_"):
                continue
            if isinstance(value, str):
                record.__dict__[key] = redact(value)
            elif isinstance(value, dict):
                record.__dict__[key] = {k: (redact(v) if isinstance(v, str) else v) for k, v in value.items()}
        return True


def _extras(record: logging.LogRecord) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for key, value in record.__dict__.items():
        if key in _RESERVED or key.startswith("_") or key in {"message", "color_message"}:
            continue
        out[key] = value
    return out


class TextFormatter(logging.Formatter):
    default_time_format = "%H:%M:%S"

    def format(self, record: logging.LogRecord) -> str:
        stamp = self.formatTime(record, self.default_time_format)
        base = f"{stamp} {record.levelname.lower():<8} {record.getMessage()}"
        extra = _extras(record)
        if extra:
            base += "  " + " ".join(f"{k}={v}" for k, v in extra.items())
        if record.exc_info:
            base += "\n" + self.formatException(record.exc_info)
        return base


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "ts": self.formatTime(record, "%Y-%m-%dT%H:%M:%S%z"),
            "level": record.levelname.lower(),
            "logger": record.name,
            "event": record.getMessage(),
        }
        payload.update(_extras(record))
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, ensure_ascii=False, default=str)


class StructuredLogger:
    """Thin adapter that lets call sites pass structured fields as kwargs.

    ``log.info("event", url=..., size=...)`` becomes ``extra={...}`` on the
    underlying :mod:`logging` logger, which the formatters render as text
    ``key=value`` pairs or as JSON fields.
    """

    __slots__ = ("_logger",)

    def __init__(self, logger: logging.Logger) -> None:
        self._logger = logger

    # -- level helpers ---------------------------------------------------- #
    def _log(self, level: int, msg: str, /, exc_info: Any = None, **fields: Any) -> None:
        if not self._logger.isEnabledFor(level):
            return
        extra = None
        if fields:
            extra = {}
            for key, value in fields.items():
                safe = (
                    f"ctx_{key}"
                    if key in _RESERVED or key in {"exc_info", "level", "msg"} or key.startswith("_")
                    else key
                )
                extra[safe] = value
        self._logger.log(level, redact(msg), exc_info=exc_info, extra=extra)

    def debug(self, msg: str, **fields: Any) -> None:
        self._log(logging.DEBUG, msg, **fields)

    def info(self, msg: str, **fields: Any) -> None:
        self._log(logging.INFO, msg, **fields)

    def warning(self, msg: str, **fields: Any) -> None:
        self._log(logging.WARNING, msg, **fields)

    def error(self, msg: str, **fields: Any) -> None:
        self._log(logging.ERROR, msg, **fields)

    def critical(self, msg: str, **fields: Any) -> None:
        self._log(logging.CRITICAL, msg, **fields)

    def exception(self, msg: str, **fields: Any) -> None:
        self._log(logging.ERROR, msg, exc_info=True, **fields)

    def trace(self, msg: str, **fields: Any) -> None:
        self._log(5, msg, **fields)

    # -- passthrough ------------------------------------------------------ #
    def isEnabledFor(self, level: int) -> bool:  # noqa: N802
        return self._logger.isEnabledFor(level)

    def setLevel(self, level: Any) -> None:  # noqa: N802
        self._logger.setLevel(level)

    def addHandler(self, handler: logging.Handler) -> None:  # noqa: N802
        self._logger.addHandler(handler)

    @property
    def unwrap(self) -> logging.Logger:
        return self._logger


_configured = False


_loggers: dict[str, StructuredLogger] = {}


def setup_logging(
    *,
    level: str = "info",
    json_logs: bool = False,
    log_file: Path | None = None,
    quiet: bool = False,
    verbose: bool = False,
    force: bool = False,
) -> logging.Logger:
    """Configure the ``wget2`` logger once per process."""
    global _configured  # noqa: PLW0603 - process-wide one-shot configuration
    logger = logging.getLogger(LOGGER_NAME)
    if _configured and not force:
        return logger
    for handler in list(logger.handlers):
        logger.removeHandler(handler)
        handler.close()

    if quiet:
        resolved = logging.ERROR
    elif verbose:
        resolved = logging.DEBUG
    else:
        resolved = LEVELS.get(level.lower(), logging.INFO)

    formatter: logging.Formatter = JsonFormatter() if json_logs else TextFormatter()
    handlers: list[logging.Handler] = []

    if not quiet or log_file is not None:
        stream = logging.StreamHandler(sys.stderr)
        stream.setFormatter(formatter)
        stream.addFilter(RedactionFilter())
        handlers.append(stream)

    if quiet and not handlers:  # keep --quiet truly silent on stderr
        logger.addHandler(logging.NullHandler())

    if log_file is not None:
        log_file.parent.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(log_file, encoding="utf-8")
        file_handler.setFormatter(formatter)
        file_handler.addFilter(RedactionFilter())
        handlers.append(file_handler)

    for handler in handlers:
        logger.addHandler(handler)
    logger.setLevel(resolved if not quiet else logging.CRITICAL)
    logger.propagate = False
    _configured = True
    return logger


def get_logger(name: str | None = None) -> StructuredLogger:
    """Return the shared structured logger for ``name``."""
    full = LOGGER_NAME if name is None else f"{LOGGER_NAME}.{name}"
    existing = _loggers.get(full)
    if existing is None:
        existing = StructuredLogger(logging.getLogger(full))
        _loggers[full] = existing
    return existing
