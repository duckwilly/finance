"""Schemas for administrator metrics responses."""
from __future__ import annotations

from datetime import datetime
from decimal import Decimal

from pydantic import BaseModel, field_serializer


class AdminMetrics(BaseModel):
    """Aggregate metrics for the admin dashboard overview."""

    total_individuals: int
    total_companies: int
    total_transactions: int
    first_transaction_at: datetime | None
    last_transaction_at: datetime | None
    total_aum: Decimal

    @field_serializer("total_aum")
    def serialize_total_aum(self, value: Decimal) -> str:
        return str(value)
