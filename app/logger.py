"""Centralised logging helpers for scripts and future services."""
from __future__ import annotations

import logging
import os
from pathlib import Path

_LOGGERS: dict[str, logging.Logger] = {}
_DEFAULT_LEVEL = os.getenv("APP_LOG_LEVEL", "INFO").upper()
_LOG_DIR = Path(os.getenv("APP_LOG_DIR", "logs"))


def get_logger(name: str = "finance") -> logging.Logger:
    """Return a configured logger.

    The logger logs to stdout by default and, if ``APP_LOG_DIR`` is set, also to a
    rotating file handler. This keeps scripts lightweight while making it easy to
    plug into FastAPI or background workers later.
    """

    if name in _LOGGERS:
        return _LOGGERS[name]

    logger = logging.getLogger(name)
    if logger.handlers:
        return logger

    level = getattr(logging, _DEFAULT_LEVEL, logging.INFO)
    logger.setLevel(level)

    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    if _LOG_DIR:
        _LOG_DIR.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(_LOG_DIR / "app.log")
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    logger.propagate = False
    _LOGGERS[name] = logger
    return logger


def set_level(level: int | str) -> None:
    """Override the log level for all managed loggers."""

    if isinstance(level, str):
        level = getattr(logging, level.upper(), logging.INFO)
    for logger in _LOGGERS.values():
        logger.setLevel(level)
