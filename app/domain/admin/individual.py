"""Compact domain models backing admin individual views."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from typing import Iterable, Sequence


@dataclass(frozen=True, slots=True)
class PeriodRange:
    key: str
    label: str
    start: date
    end: date

    @classmethod
    def normalized(cls, *, key: str, label: str, start: date, end: date) -> "PeriodRange":
        if start > end:
            start, end = end, start
        return cls(key=key, label=label, start=start, end=end)


@dataclass(frozen=True, slots=True)
class IndividualSummary:
    user_id: int
    name: str
    email: str | None
    net_worth: Decimal

    @classmethod
    def from_row(cls, row: object) -> "IndividualSummary":
        return cls(
            user_id=int(getattr(row, "user_id")),
            name=str(getattr(row, "name")),
            email=_nullable_text(getattr(row, "email")),
            net_worth=Decimal(getattr(row, "net_worth")),
        )


@dataclass(frozen=True, slots=True)
class IndividualPage:
    items: Sequence[IndividualSummary]
    total: int
    page: int
    page_size: int

    @classmethod
    def assemble(
        cls,
        items: Iterable[IndividualSummary],
        *,
        total: int,
        page: int,
        page_size: int,
    ) -> "IndividualPage":
        return cls(items=tuple(items), total=int(total), page=int(page), page_size=int(page_size))


@dataclass(frozen=True, slots=True)
class AccountSnapshot:
    account_id: int
    name: str
    type: str
    balance: Decimal

    @classmethod
    def from_row(cls, row: object) -> "AccountSnapshot":
        return cls(
            account_id=int(getattr(row, "account_id")),
            name=str(getattr(row, "account_name")),
            type=str(getattr(row, "account_type")),
            balance=Decimal(getattr(row, "balance")),
        )


@dataclass(frozen=True, slots=True)
class HoldingPerformance:
    symbol: str
    name: str
    quantity: Decimal
    cost_basis: Decimal
    market_value: Decimal
    gain_loss: Decimal

    @classmethod
    def from_row(cls, row: object) -> "HoldingPerformance":
        return cls(
            symbol=str(getattr(row, "symbol")),
            name=str(getattr(row, "name")),
            quantity=Decimal(getattr(row, "quantity")),
            cost_basis=Decimal(getattr(row, "cost_basis")),
            market_value=Decimal(getattr(row, "market_value")),
            gain_loss=Decimal(getattr(row, "gain_loss")),
        )


@dataclass(frozen=True, slots=True)
class CashTotals:
    cash_total: Decimal
    holdings_total: Decimal
    portfolio_gain_loss: Decimal

    @classmethod
    def from_components(
        cls,
        accounts: Sequence[AccountSnapshot],
        holdings: Sequence[HoldingPerformance],
    ) -> "CashTotals":
        cash_total = sum((account.balance for account in accounts), Decimal(0))
        holdings_total = sum((holding.market_value for holding in holdings), Decimal(0))
        portfolio_gain_loss = sum((holding.gain_loss for holding in holdings), Decimal(0))
        return cls(cash_total=cash_total, holdings_total=holdings_total, portfolio_gain_loss=portfolio_gain_loss)


@dataclass(frozen=True, slots=True)
class CashflowMetrics:
    income_total: Decimal
    expense_total: Decimal
    net_cash_flow: Decimal

    @classmethod
    def from_row(cls, row: object) -> "CashflowMetrics":
        income_total = Decimal(getattr(row, "income_total"))
        expense_total = Decimal(getattr(row, "expense_total"))
        return cls(
            income_total=income_total,
            expense_total=expense_total,
            net_cash_flow=income_total - expense_total,
        )


@dataclass(frozen=True, slots=True)
class CategoryBreakdown:
    category: str
    amount: Decimal

    @classmethod
    def from_row(cls, row: object, *, absolute: bool = False) -> "CategoryBreakdown":
        amount = Decimal(getattr(row, "amount"))
        if absolute:
            amount = amount.copy_abs()
        return cls(category=str(getattr(row, "category")), amount=amount)


@dataclass(frozen=True, slots=True)
class RecentTransaction:
    txn_id: int
    posted_at: date
    description: str
    amount: Decimal

    @classmethod
    def from_row(cls, row: object) -> "RecentTransaction":
        return cls(
            txn_id=int(getattr(row, "txn_id")),
            posted_at=getattr(row, "posted_at"),
            description=str(getattr(row, "description")),
            amount=Decimal(getattr(row, "amount")),
        )


@dataclass(frozen=True, slots=True)
class IndividualDetail:
    user_id: int
    name: str
    email: str | None
    net_worth: Decimal
    cash_total: Decimal
    holdings_total: Decimal
    portfolio_gain_loss: Decimal
    accounts: Sequence[AccountSnapshot]
    holdings: Sequence[HoldingPerformance]
    income_total: Decimal
    expense_total: Decimal
    net_cash_flow: Decimal
    income_breakdown: Sequence[CategoryBreakdown]
    expense_breakdown: Sequence[CategoryBreakdown]
    period: PeriodRange
    recent_transactions: Sequence[RecentTransaction]

    @classmethod
    def assemble(
        cls,
        *,
        user_id: int,
        name: str,
        email: str | None,
        net_worth: Decimal,
        cash_total: Decimal,
        holdings_total: Decimal,
        portfolio_gain_loss: Decimal,
        accounts: Iterable[AccountSnapshot],
        holdings: Iterable[HoldingPerformance],
        income_total: Decimal,
        expense_total: Decimal,
        net_cash_flow: Decimal,
        income_breakdown: Iterable[CategoryBreakdown],
        expense_breakdown: Iterable[CategoryBreakdown],
        period: PeriodRange,
        recent_transactions: Iterable[RecentTransaction],
    ) -> "IndividualDetail":
        return cls(
            user_id=int(user_id),
            name=str(name),
            email=_nullable_text(email),
            net_worth=Decimal(net_worth),
            cash_total=Decimal(cash_total),
            holdings_total=Decimal(holdings_total),
            portfolio_gain_loss=Decimal(portfolio_gain_loss),
            accounts=tuple(accounts),
            holdings=tuple(holdings),
            income_total=Decimal(income_total),
            expense_total=Decimal(expense_total),
            net_cash_flow=Decimal(net_cash_flow),
            income_breakdown=tuple(income_breakdown),
            expense_breakdown=tuple(expense_breakdown),
            period=period,
            recent_transactions=tuple(recent_transactions),
        )


def _nullable_text(value: object | None) -> str | None:
    return None if value is None else str(value)


__all__ = [
    "AccountSnapshot",
    "CashTotals",
    "CashflowMetrics",
    "CategoryBreakdown",
    "HoldingPerformance",
    "IndividualDetail",
    "IndividualPage",
    "IndividualSummary",
    "PeriodRange",
    "RecentTransaction",
]

