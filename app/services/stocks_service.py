"""Placeholder service implementation for stock analytics."""
from __future__ import annotations

from app.core.logger import get_logger

LOGGER = get_logger(__name__)


class StocksService:
    """Service skeleton for aggregate stock analytics."""

    def summarise_holdings(self, *_args: object, **_kwargs: object) -> None:
        LOGGER.debug("StocksService.summarise_holdings called")
        raise NotImplementedError("Stock analysis logic not yet implemented.")
