"""Schema definitions for individual dashboards."""
from __future__ import annotations

from datetime import date
from decimal import Decimal

from pydantic import BaseModel, field_serializer


class IndividualProfile(BaseModel):
    """Basic information that appears at the top of the dashboard."""

    id: int
    name: str
    email: str | None = None
    job_title: str | None = None


class SummaryMetrics(BaseModel):
    """Headline metrics highlighting the user's financial position."""

    net_worth: Decimal = Decimal("0")
    cash_balance: Decimal = Decimal("0")
    holdings_value: Decimal = Decimal("0")
    period_income: Decimal = Decimal("0")
    period_expenses: Decimal = Decimal("0")
    net_cash_flow: Decimal = Decimal("0")

    @field_serializer(
        "net_worth",
        "cash_balance",
        "holdings_value",
        "period_income",
        "period_expenses",
        "net_cash_flow",
    )
    def _serialize_decimal(cls, value: Decimal) -> str:
        return str(value)


class AccountSummary(BaseModel):
    """Summary information for one of the individual's accounts."""

    id: int
    name: str | None = None
    type: str
    currency: str
    balance: Decimal = Decimal("0")

    @field_serializer("balance")
    def _serialize_balance(cls, value: Decimal) -> str:
        return str(value)


class TransactionSummary(BaseModel):
    """Compact representation of a transaction for detail views."""

    txn_date: date
    description: str | None = None
    amount: Decimal = Decimal("0")

    @field_serializer("amount")
    def _serialize_amount(cls, value: Decimal) -> str:
        return str(value)


class CategoryBreakdown(BaseModel):
    """Aggregated amounts per category with supporting transactions."""

    name: str
    total: Decimal = Decimal("0")
    transactions: list[TransactionSummary]

    @field_serializer("total")
    def _serialize_total(cls, value: Decimal) -> str:
        return str(value)


class HoldingSummary(BaseModel):
    """Market exposure for one brokerage holding."""

    instrument_symbol: str
    instrument_name: str
    quantity: Decimal = Decimal("0")
    last_price: Decimal = Decimal("0")
    market_value: Decimal = Decimal("0")
    unrealized_pl: Decimal = Decimal("0")

    @field_serializer(
        "quantity",
        "last_price",
        "market_value",
        "unrealized_pl",
    )
    def _serialize_decimal(cls, value: Decimal) -> str:
        return str(value)


class IndividualDashboard(BaseModel):
    """Payload passed to the individual dashboard template."""

    profile: IndividualProfile
    employer_name: str | None
    summary: SummaryMetrics
    period_label: str
    accounts: list[AccountSummary]
    income_breakdown: list[CategoryBreakdown]
    expense_breakdown: list[CategoryBreakdown]
    holdings: list[HoldingSummary]
