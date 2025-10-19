"""Placeholder service implementation for admin tooling."""
from __future__ import annotations

from app.core.logger import get_logger

LOGGER = get_logger(__name__)


class AdminService:
    """Service skeleton for administrator workflows."""

    def overview(self, *_args: object, **_kwargs: object) -> None:
        LOGGER.debug("AdminService.overview called")
        raise NotImplementedError("Admin overview logic not yet implemented.")
