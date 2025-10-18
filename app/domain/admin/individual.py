"""Domain models backing admin individual views."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
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
            name=str(getattr(row, "user_name")),
            email=_nullable_text(getattr(row, "user_email", None)),
            net_worth=_decimal(getattr(row, "net_worth")),
        )


@dataclass(frozen=True, slots=True)
class IndividualPage:
    items: Sequence[IndividualSummary]
    total: int
    page: int
    page_size: int

    @property
    def total_pages(self) -> int:
        if self.total <= 0:
            return 1
        return (self.total + self.page_size - 1) // self.page_size

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
    name: str | None
    type: str
    currency: str
    balance: Decimal

    @classmethod
    def from_row(cls, row: object) -> "AccountSnapshot":
        return cls(
            account_id=int(getattr(row, "account_id")),
            name=_nullable_text(getattr(row, "account_name", None)),
            type=str(getattr(row, "account_type")),
            currency=str(getattr(row, "account_currency")),
            balance=_decimal(getattr(row, "balance")),
        )


@dataclass(frozen=True, slots=True)
class HoldingPerformance:
    account_id: int
    account_name: str | None
    instrument_id: int
    symbol: str
    name: str
    currency: str
    quantity: Decimal
    avg_cost: Decimal
    start_price: Decimal | None
    end_price: Decimal
    price_change: Decimal
    price_change_pct: Decimal | None
    period_pl: Decimal
    market_value: Decimal

    @classmethod
    def from_row(cls, row: object) -> "HoldingPerformance":
        quantity = _decimal(getattr(row, "quantity"))
        avg_cost = _decimal(getattr(row, "avg_cost"))
        start_price = _optional_decimal(getattr(row, "start_price", None))
        end_price = _optional_decimal(getattr(row, "end_price", None))
        last_price = _optional_decimal(getattr(row, "last_price", None))

        effective_end_price = end_price if end_price is not None else last_price
        if effective_end_price is None:
            effective_end_price = Decimal(0)

        market_value = quantity * effective_end_price

        price_change = Decimal(0)
        price_change_pct: Decimal | None = None
        period_pl = Decimal(0)

        if start_price is not None:
            price_change = effective_end_price - start_price
            period_pl = price_change * quantity
            if start_price != 0:
                price_change_pct = (price_change / start_price) * Decimal("100")

        return cls(
            account_id=int(getattr(row, "account_id")),
            account_name=_nullable_text(getattr(row, "account_name", None)),
            instrument_id=int(getattr(row, "instrument_id")),
            symbol=str(getattr(row, "instrument_symbol")),
            name=str(getattr(row, "instrument_name")),
            currency=str(getattr(row, "instrument_currency")),
            quantity=quantity,
            avg_cost=avg_cost,
            start_price=start_price,
            end_price=effective_end_price,
            price_change=price_change,
            price_change_pct=price_change_pct,
            period_pl=period_pl,
            market_value=market_value,
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
        portfolio_gain_loss = sum((holding.period_pl for holding in holdings), Decimal(0))
        return cls(
            cash_total=cash_total,
            holdings_total=holdings_total,
            portfolio_gain_loss=portfolio_gain_loss,
        )


@dataclass(frozen=True, slots=True)
class CashflowMetrics:
    income_total: Decimal
    expense_total: Decimal
    net_cash_flow: Decimal

    @classmethod
    def from_row(cls, row: object) -> "CashflowMetrics":
        income_total = _decimal(getattr(row, "income_total"))
        expense_total = _decimal(getattr(row, "expense_total")).copy_abs()
        return cls(
            income_total=income_total,
            expense_total=expense_total,
            net_cash_flow=income_total - expense_total,
        )


@dataclass(frozen=True, slots=True)
class CategoryBreakdown:
    category_id: int | None
    name: str
    amount: Decimal

    @classmethod
    def from_row(cls, row: object, *, absolute: bool = False) -> "CategoryBreakdown":
        amount = _decimal(getattr(row, "total_amount"))
        if absolute:
            amount = amount.copy_abs()
        category_id = getattr(row, "category_id", None)
        return cls(
            category_id=(int(category_id) if category_id is not None else None),
            name=str(getattr(row, "category_name")),
            amount=amount,
        )


@dataclass(frozen=True, slots=True)
class CounterpartyInfo:
    counterparty_id: int | None
    name: str | None


@dataclass(frozen=True, slots=True)
class RelatedParty:
    type: str
    party_id: int
    name: str


@dataclass(frozen=True, slots=True)
class RecentTransaction:
    transaction_id: int
    posted_at: datetime
    txn_date: date
    signed_amount: Decimal
    currency: str
    section: str
    category: str | None
    account_name: str | None
    description: str | None
    counterparty: CounterpartyInfo | None
    related_party: RelatedParty | None

    @classmethod
    def from_row(cls, row: object) -> "RecentTransaction":
        direction = str(getattr(row, "direction"))
        amount = _decimal(getattr(row, "amount"))
        signed_amount = amount if direction == "CREDIT" else -amount

        counterparty_id = getattr(row, "counterparty_id", None)
        counterparty_name = getattr(row, "counterparty_name", None)
        counterparty: CounterpartyInfo | None = None
        if counterparty_id is not None or counterparty_name is not None:
            counterparty = CounterpartyInfo(
                counterparty_id=(int(counterparty_id) if counterparty_id is not None else None),
                name=_nullable_text(counterparty_name),
            )

        owner_type = getattr(row, "other_owner_type", None)
        owner_id = getattr(row, "other_owner_id", None)
        related_party: RelatedParty | None = None
        if owner_type and owner_id:
            owner_name: str | None = None
            owner_kind: str | None = None
            if owner_type == "user":
                owner_name = _nullable_text(getattr(row, "other_user_name", None))
                owner_kind = "user" if owner_name else None
            elif owner_type == "org":
                owner_name = _nullable_text(getattr(row, "other_org_name", None))
                owner_kind = "company" if owner_name else None
            if owner_name and owner_kind:
                related_party = RelatedParty(
                    type=owner_kind,
                    party_id=int(owner_id),
                    name=owner_name,
                )

        section_name = _nullable_text(getattr(row, "section_name", None)) or ""

        return cls(
            transaction_id=int(getattr(row, "transaction_id")),
            posted_at=getattr(row, "posted_at"),
            txn_date=getattr(row, "txn_date"),
            signed_amount=signed_amount,
            currency=str(getattr(row, "currency")),
            section=section_name,
            category=_nullable_text(getattr(row, "category_name", None)),
            account_name=_nullable_text(getattr(row, "account_name", None)),
            description=_nullable_text(getattr(row, "description", None)),
            counterparty=counterparty,
            related_party=related_party,
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
            net_worth=_decimal(net_worth),
            cash_total=_decimal(cash_total),
            holdings_total=_decimal(holdings_total),
            portfolio_gain_loss=_decimal(portfolio_gain_loss),
            accounts=tuple(accounts),
            holdings=tuple(holdings),
            income_total=_decimal(income_total),
            expense_total=_decimal(expense_total),
            net_cash_flow=_decimal(net_cash_flow),
            income_breakdown=tuple(income_breakdown),
            expense_breakdown=tuple(expense_breakdown),
            period=period,
            recent_transactions=tuple(recent_transactions),
        )


def _nullable_text(value: object | None) -> str | None:
    return None if value is None else str(value)


def _decimal(value: object) -> Decimal:
    return value if isinstance(value, Decimal) else Decimal(value)


def _optional_decimal(value: object | None) -> Decimal | None:
    if value is None:
        return None
    return _decimal(value)


__all__ = [
    "AccountSnapshot",
    "CashTotals",
    "CashflowMetrics",
    "CategoryBreakdown",
    "CounterpartyInfo",
    "HoldingPerformance",
    "IndividualDetail",
    "IndividualPage",
    "IndividualSummary",
    "PeriodRange",
    "RecentTransaction",
    "RelatedParty",
]
