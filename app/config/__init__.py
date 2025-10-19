"""Backwards-compatible entrypoint for configuration helpers."""

from app.core.config import Settings, get_settings

__all__ = ["Settings", "get_settings"]
