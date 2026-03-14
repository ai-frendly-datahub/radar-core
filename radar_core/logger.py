from __future__ import annotations

import logging
import os
import sys
from typing import Any

import structlog


def configure_logging(
    log_level: str | None = None,
    use_json: bool | None = None,
) -> None:
    if log_level is None:
        log_level = os.environ.get("RADAR_LOG_LEVEL", "INFO").upper()

    if use_json is None:
        use_json = not sys.stderr.isatty()

    processors: list[Any] = [
        structlog.processors.TimeStamper(fmt="iso", utc=True),
        structlog.processors.add_log_level,
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
    ]

    if use_json:
        processors.append(structlog.processors.JSONRenderer())
    else:
        processors.append(
            structlog.dev.ConsoleRenderer(
                colors=True,
                exception_formatter=structlog.dev.rich_traceback,
            )
        )

    structlog.configure(
        processors=processors,
        wrapper_class=structlog.BoundLogger,
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(file=sys.stderr),
        cache_logger_on_first_use=True,
    )

    logging_level = getattr(logging, log_level, logging.INFO)
    logging.basicConfig(level=logging_level)


def get_logger(name: str) -> structlog.BoundLogger:
    return structlog.get_logger(name)
