"""Placeholder service implementation for transaction reporting."""
from __future__ import annotations

from app.core.logger import get_logger

LOGGER = get_logger(__name__)


class TransactionsService:
    """Service skeleton for transaction exploration."""

    def list_transactions(self, *_args: object, **_kwargs: object) -> None:
        LOGGER.debug("TransactionsService.list_transactions called")
        raise NotImplementedError("Transaction listing logic not yet implemented.")
