"""Structured logging setup shared by every service."""

from __future__ import annotations

import logging as _logging
import os
import sys

import structlog


def configure_logging(level: str | None = None) -> None:
    level_name = (level or os.environ.get("LOG_LEVEL", "INFO")).upper()
    _logging.basicConfig(stream=sys.stdout, format="%(message)s", level=level_name)
    structlog.configure(
        processors=[
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso", utc=True),
            structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(getattr(_logging, level_name)),
    )


def get_logger(name: str):
    return structlog.get_logger(name)
