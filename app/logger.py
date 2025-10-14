
"""Compatibility wrapper that exposes the shared logging utilities."""
from __future__ import annotations

from .log import (
    get_logger,
    init_logging,
    log_context,
    progress_manager,
    set_level,
    shutdown_logging,
    timeit,
)

__all__ = [
    "get_logger",
    "init_logging",
    "set_level",
    "shutdown_logging",
    "log_context",
    "progress_manager",
    "timeit",
]
