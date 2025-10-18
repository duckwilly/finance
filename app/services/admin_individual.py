"""Service logic for individual-focused admin views."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta
from decimal import Decimal
from math import ceil

from sqlalchemy.orm import Session

from app.repositories.admin.individual_repository import AdminIndividualRepository


@dataclass(frozen=True)
class IndividualSummary:
    """Aggregated snapshot of a user's finances."""

    user_id: int
    name: str
    email: str | None
    net_worth: Decimal


@dataclass(frozen=True)
class IndividualPage:
    """Paginated collection of individual summaries."""

    items: list[IndividualSummary]
    total: int
    page: int
    page_size: int

    @property
    def total_pages(self) -> int:
        if self.total == 0:
            return 1
        return ceil(self.total / self.page_size)


@dataclass(frozen=True)
class PeriodRange:
    """Normalized representation of a reporting period."""

    key: str
    label: str
    start: date
    end: date


@dataclass(frozen=True)
class AccountSnapshot:
    """Balance information for a user's account."""

    account_id: int
    name: str | None
    type: str
    currency: str
    balance: Decimal


@dataclass(frozen=True)
class HoldingPerformance:
    """Performance metrics for a brokerage holding."""

    account_id: int
    account_name: str | None
    instrument_id: int
    symbol: str
    name: str
    currency: str
    quantity: Decimal
    avg_cost: Decimal
    start_price: Decimal | None
    end_price: Decimal | None
    price_change: Decimal
    price_change_pct: Decimal | None
    period_pl: Decimal
    market_value: Decimal


@dataclass(frozen=True)
class CategoryBreakdown:
    """Aggregate totals for a transaction category."""

    category_id: int | None
    name: str
    amount: Decimal


@dataclass(frozen=True)
class CounterpartyInfo:
    """Representation of a transaction counterparty."""

    counterparty_id: int | None
    name: str | None


@dataclass(frozen=True)
class RelatedParty:
    """Linked organization or user involved in a transfer."""

    type: str
    party_id: int
    name: str


@dataclass(frozen=True)
class RecentTransaction:
    """Recent transaction enriched with metadata."""

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


@dataclass(frozen=True)
class IndividualDetail:
    """Composite detail view for a single user."""

    user_id: int
    name: str
    email: str | None
    net_worth: Decimal
    cash_total: Decimal
    holdings_total: Decimal
    portfolio_gain_loss: Decimal
    accounts: list[AccountSnapshot]
    holdings: list[HoldingPerformance]
    income_total: Decimal
    expense_total: Decimal
    net_cash_flow: Decimal
    income_breakdown: list[CategoryBreakdown]
    expense_breakdown: list[CategoryBreakdown]
    period: PeriodRange
    recent_transactions: list[RecentTransaction]


class AdminIndividualService:
    """Facade exposing individual level aggregates for administrators."""

    _PERIOD_DEFAULT = "mtd"
    _PERIOD_LABELS: dict[str, str] = {
        "mtd": "Month to date",
        "qtd": "Quarter to date",
        "ytd": "Year to date",
        "last_30_days": "Last 30 days",
        "last_90_days": "Last 90 days",
        "last_12_months": "Last 12 months",
    }
    _PERIOD_ORDER: tuple[str, ...] = (
        "mtd",
        "qtd",
        "ytd",
        "last_30_days",
        "last_90_days",
        "last_12_months",
    )

    def __init__(
        self,
        session: Session,
        repository: AdminIndividualRepository | None = None,
    ) -> None:
        self._repository = repository or AdminIndividualRepository(session)

    def list_individuals(
        self,
        *,
        page: int,
        page_size: int,
        search: str | None = None,
    ) -> IndividualPage:
        """Return a paginated list of user summaries."""

        if page < 1:
            page = 1
        if page_size < 1:
            raise ValueError("page_size must be greater than zero")

        total = self._repository.count_individuals(search=search)

        max_page = max(1, ceil(total / page_size)) if total else 1
        page = min(page, max_page)
        offset = (page - 1) * page_size

        rows = self._repository.fetch_individual_summaries(
            search=search,
            limit=page_size,
            offset=offset,
        )

        items = [
            IndividualSummary(
                user_id=row.user_id,
                name=row.user_name,
                email=row.user_email,
                net_worth=row.net_worth,
            )
            for row in rows
        ]

        return IndividualPage(
            items=items,
            total=total,
            page=page,
            page_size=page_size,
        )

    def get_individual_detail(
        self,
        user_id: int,
        *,
        period_key: str | None = None,
        start_date: date | None = None,
        end_date: date | None = None,
        today: date | None = None,
    ) -> IndividualDetail | None:
        """Return detail information for a user."""

        reference_today = today or date.today()

        range_start = start_date
        range_end = end_date

        if range_start is not None and range_end is not None:
            if range_start > range_end:
                range_start, range_end = range_end, range_start
            if range_end > reference_today:
                range_end = reference_today
            period = PeriodRange(
                key="custom",
                label="Custom range",
                start=range_start,
                end=range_end,
            )
        else:
            period = self._resolve_period(period_key, today=reference_today)
            range_start = period.start
            range_end = period.end

        user_row = self._repository.get_individual(user_id)

        if user_row is None:
            return None

        accounts = [
            AccountSnapshot(
                account_id=row.account_id,
                name=row.account_name,
                type=row.account_type,
                currency=row.account_currency,
                balance=row.balance,
            )
            for row in self._repository.list_accounts(user_id)
        ]

        cash_total = sum((account.balance for account in accounts), Decimal(0))

        holdings: list[HoldingPerformance] = []
        holdings_total = Decimal(0)
        portfolio_gain_loss = Decimal(0)

        for row in self._repository.list_holdings(
            user_id,
            start_date=range_start,
            end_date=range_end,
        ):
            quantity = row.quantity
            avg_cost = row.avg_cost
            start_price = row.start_price
            end_price = row.end_price
            last_price = row.last_price
            effective_end_price = end_price if end_price is not None else last_price
            if effective_end_price is None:
                effective_end_price = Decimal(0)
            market_value = quantity * effective_end_price
            price_change = Decimal(0)
            change_pct: Decimal | None = None
            period_pl = Decimal(0)
            if start_price is not None and effective_end_price is not None:
                price_change = effective_end_price - start_price
                period_pl = price_change * quantity
                if start_price != 0:
                    change_pct = (price_change / start_price) * Decimal("100")
            holdings_total += market_value
            portfolio_gain_loss += period_pl
            holdings.append(
                HoldingPerformance(
                    account_id=row.account_id,
                    account_name=row.account_name,
                    instrument_id=row.instrument_id,
                    symbol=row.instrument_symbol,
                    name=row.instrument_name,
                    currency=row.instrument_currency,
                    quantity=quantity,
                    avg_cost=avg_cost,
                    start_price=start_price,
                    end_price=effective_end_price,
                    price_change=price_change,
                    price_change_pct=change_pct,
                    period_pl=period_pl,
                    market_value=market_value,
                )
            )

        cashflow = self._repository.get_cashflow(
            user_id,
            start_date=range_start,
            end_date=range_end,
        )

        income_total = cashflow.income_total
        expense_total = cashflow.expense_total.copy_abs()
        net_cash_flow = income_total - expense_total

        recent_transactions: list[RecentTransaction] = []
        for row in self._repository.list_recent_transactions(user_id):
            direction = row.direction
            amount_value = row.amount
            signed_amount = amount_value if direction == "CREDIT" else -amount_value
            counterparty = None
            if row.counterparty_id is not None or row.counterparty_name is not None:
                counterparty = CounterpartyInfo(
                    counterparty_id=row.counterparty_id,
                    name=row.counterparty_name,
                )
            related_party = None
            owner_type = row.other_owner_type
            owner_id = row.other_owner_id
            if owner_type and owner_id:
                party_name = None
                party_type = None
                if owner_type == "user" and row.other_user_name:
                    party_name = str(row.other_user_name)
                    party_type = "user"
                elif owner_type == "org" and row.other_org_name:
                    party_name = str(row.other_org_name)
                    party_type = "company"
                if party_name and party_type:
                    related_party = RelatedParty(
                        type=party_type,
                        party_id=int(owner_id),
                        name=party_name,
                    )
            recent_transactions.append(
                RecentTransaction(
                    transaction_id=row.transaction_id,
                    posted_at=row.posted_at,
                    txn_date=row.txn_date,
                    signed_amount=signed_amount,
                    currency=row.currency,
                    section=row.section_name or "",
                    category=row.category_name,
                    account_name=row.account_name,
                    description=row.description,
                    counterparty=counterparty,
                    related_party=related_party,
                )
            )

        income_breakdown = [
            CategoryBreakdown(
                category_id=row.category_id,
                name=row.category_name,
                amount=row.total_amount,
            )
            for row in self._repository.list_income_breakdown(
                user_id,
                start_date=range_start,
                end_date=range_end,
            )
        ]

        expense_breakdown = [
            CategoryBreakdown(
                category_id=row.category_id,
                name=row.category_name,
                amount=row.total_amount.copy_abs(),
            )
            for row in self._repository.list_expense_breakdown(
                user_id,
                start_date=range_start,
                end_date=range_end,
            )
        ]

        net_worth = cash_total + holdings_total

        return IndividualDetail(
            user_id=user_row.user_id,
            name=user_row.name,
            email=user_row.email,
            net_worth=net_worth,
            cash_total=cash_total,
            holdings_total=holdings_total,
            portfolio_gain_loss=portfolio_gain_loss,
            accounts=accounts,
            holdings=holdings,
            income_total=income_total,
            expense_total=expense_total,
            net_cash_flow=net_cash_flow,
            income_breakdown=income_breakdown,
            expense_breakdown=expense_breakdown,
            period=period,
            recent_transactions=recent_transactions,
        )

    @classmethod
    def period_options(cls, *, today: date | None = None) -> list[PeriodRange]:
        """Return available reporting windows."""

        return [cls._resolve_period(key, today=today) for key in cls._PERIOD_ORDER]

    @classmethod
    def _resolve_period(
        cls,
        period_key: str | None,
        *,
        today: date | None = None,
    ) -> PeriodRange:
        resolved_key = (period_key or cls._PERIOD_DEFAULT).lower()
        if resolved_key not in cls._PERIOD_LABELS:
            resolved_key = cls._PERIOD_DEFAULT

        reference_date = today or date.today()

        if resolved_key == "mtd":
            start = reference_date.replace(day=1)
        elif resolved_key == "qtd":
            quarter_index = (reference_date.month - 1) // 3
            start_month = quarter_index * 3 + 1
            start = reference_date.replace(month=start_month, day=1)
        elif resolved_key == "last_30_days":
            start = reference_date - timedelta(days=29)
        elif resolved_key == "last_90_days":
            start = reference_date - timedelta(days=89)
        elif resolved_key == "last_12_months":
            start = reference_date - timedelta(days=364)
        else:  # ytd
            start = reference_date.replace(month=1, day=1)

        label = cls._PERIOD_LABELS[resolved_key]
        return PeriodRange(key=resolved_key, label=label, start=start, end=reference_date)
