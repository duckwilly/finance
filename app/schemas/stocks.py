"""Stock and market data schemas aligned with the normalised holdings model."""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from typing import List

from pydantic import BaseModel, Field, field_serializer


class InstrumentIdentifierPayload(BaseModel):
    """External identifier associated with an instrument (e.g. ISIN, ticker)."""

    type: str
    value: str


class InstrumentSnapshot(BaseModel):
    """Summary of instrument metadata used in holdings reporting."""

    id: int
    symbol: str
    name: str
    instrument_type_code: str
    primary_currency_code: str
    primary_market_id: int | None = None
    identifiers: List[InstrumentIdentifierPayload] = Field(default_factory=list)


class PriceQuoteSnapshot(BaseModel):
    """Price quote payload for charting and valuation."""

    instrument_id: int
    price_date: date
    quote_type: str = "CLOSE"
    quote_value: Decimal

    @field_serializer("quote_value")
    def serialize_quote(cls, value: Decimal) -> str:
        return format(value, "f")


class FxRateSnapshot(BaseModel):
    """Snapshot of a daily FX rate."""

    base_currency_code: str
    quote_currency_code: str
    rate_date: date
    rate_value: Decimal

    @field_serializer("rate_value")
    def serialize_rate(cls, value: Decimal) -> str:
        return format(value, "f")


class LotAllocation(BaseModel):
    """Lot-level association between a holding and a trade execution."""

    id: int
    holding_id: int
    trade_id: int
    quantity: Decimal
    cost_basis: Decimal

    @field_serializer("quantity", "cost_basis")
    def serialize_decimal(cls, value: Decimal) -> str:
        return format(value, "f")


class TradeExecution(BaseModel):
    """Executed trade captured in the normalised trade table."""

    id: int
    account_id: int
    instrument_id: int
    trade_time: datetime
    side: str
    quantity: Decimal
    price: Decimal
    fees_amount: Decimal = Decimal("0")
    tax_amount: Decimal = Decimal("0")
    trade_currency_code: str
    settlement_date: date | None = None
    lots: list[LotAllocation] | None = None

    @field_serializer("quantity", "price", "fees_amount", "tax_amount")
    def serialize_decimal(cls, value: Decimal) -> str:
        return format(value, "f")


class HoldingPosition(BaseModel):
    """Current holding snapshot for a party/account/instrument combination."""

    id: int
    account_id: int | None = None
    instrument_id: int
    quantity: Decimal
    average_cost: Decimal
    updated_at: datetime
    instrument: InstrumentSnapshot | None = None
    lots: list[LotAllocation] | None = None
    market_value: Decimal | None = None
    unrealized_pl: Decimal | None = None

    @field_serializer("quantity", "average_cost", "market_value", "unrealized_pl")
    def serialize_decimal(cls, value: Decimal | None) -> str | None:
        if value is None:
            return None
        return format(value, "f")
