"""Placeholder service implementation for company-facing features."""
from __future__ import annotations

from app.core.logger import get_logger

LOGGER = get_logger(__name__)


class CompaniesService:
    """Service skeleton that will orchestrate company dashboards."""

    def list_companies(self, *_args: object, **_kwargs: object) -> None:
        LOGGER.debug("CompaniesService.list_companies called")
        raise NotImplementedError("Company listing logic not yet implemented.")
