"""Schema definitions for company dashboards."""
from __future__ import annotations

from datetime import date
from decimal import Decimal

from pydantic import BaseModel, field_serializer


class CompanyProfile(BaseModel):
    """Basic company information displayed on the dashboard."""

    id: int
    name: str


class SummaryMetrics(BaseModel):
    """Headline metrics summarising company performance."""

    net_worth: Decimal = Decimal("0")
    cash_balance: Decimal = Decimal("0")
    holdings_value: Decimal = Decimal("0")
    period_income: Decimal = Decimal("0")
    period_expenses: Decimal = Decimal("0")
    net_cash_flow: Decimal = Decimal("0")
    total_profit: Decimal = Decimal("0")
    employee_count: int = 0
    monthly_salary_cost: Decimal = Decimal("0")

    @field_serializer(
        "net_worth",
        "cash_balance",
        "holdings_value",
        "period_income",
        "period_expenses",
        "net_cash_flow",
        "total_profit",
        "monthly_salary_cost",
    )
    def _serialize_decimal(cls, value: Decimal) -> str:
        return str(value)


class AccountSummary(BaseModel):
    """Balances for company accounts."""

    id: int
    name: str | None = None
    type: str
    currency: str
    balance: Decimal = Decimal("0")

    @field_serializer("balance")
    def _serialize_balance(cls, value: Decimal) -> str:
        return str(value)


class TransactionSummary(BaseModel):
    """Transaction detail used in expandable lists."""

    txn_date: date
    description: str | None = None
    amount: Decimal = Decimal("0")

    @field_serializer("amount")
    def _serialize_amount(cls, value: Decimal) -> str:
        return str(value)


class CategoryBreakdown(BaseModel):
    """Aggregated totals for a transaction category."""

    name: str
    total: Decimal = Decimal("0")
    transactions: list[TransactionSummary]

    @field_serializer("total")
    def _serialize_total(cls, value: Decimal) -> str:
        return str(value)


class PayrollEntry(BaseModel):
    """Monthly salary information for an employee."""

    user_id: int
    name: str
    salary_amount: Decimal = Decimal("0")

    @field_serializer("salary_amount")
    def _serialize_amount(cls, value: Decimal) -> str:
        return str(value)


class CompanyDashboard(BaseModel):
    """Payload passed to the company dashboard template."""

    profile: CompanyProfile
    period_label: str
    summary: SummaryMetrics
    accounts: list[AccountSummary]
    income_breakdown: list[CategoryBreakdown]
    expense_breakdown: list[CategoryBreakdown]
    payroll: list[PayrollEntry]
