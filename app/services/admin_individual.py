"""Service logic for individual-focused admin views."""
from __future__ import annotations

from datetime import date, timedelta
from math import ceil

from sqlalchemy.orm import Session

from app.domain.admin.individual import (
    AccountSnapshot,
    CashTotals,
    CashflowMetrics,
    CategoryBreakdown,
    HoldingPerformance,
    IndividualDetail,
    IndividualPage,
    IndividualSummary,
    PeriodRange,
    RecentTransaction,
)
from app.repositories.admin.individual_repository import AdminIndividualRepository


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

        items = [IndividualSummary.from_row(row) for row in rows]

        return IndividualPage.assemble(items, total=total, page=page, page_size=page_size)

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
            period = PeriodRange.normalized(
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
            AccountSnapshot.from_row(row)
            for row in self._repository.list_accounts(user_id)
        ]

        holdings = [
            HoldingPerformance.from_row(row)
            for row in self._repository.list_holdings(
                user_id,
                start_date=range_start,
                end_date=range_end,
            )
        ]

        totals = CashTotals.from_components(accounts, holdings)

        cashflow = self._repository.get_cashflow(
            user_id,
            start_date=range_start,
            end_date=range_end,
        )

        cashflow_metrics = CashflowMetrics.from_row(cashflow)

        recent_transactions = [
            RecentTransaction.from_row(row)
            for row in self._repository.list_recent_transactions(user_id)
        ]

        income_breakdown = [
            CategoryBreakdown.from_row(row)
            for row in self._repository.list_income_breakdown(
                user_id,
                start_date=range_start,
                end_date=range_end,
            )
        ]

        expense_breakdown = [
            CategoryBreakdown.from_row(row, absolute=True)
            for row in self._repository.list_expense_breakdown(
                user_id,
                start_date=range_start,
                end_date=range_end,
            )
        ]

        net_worth = totals.cash_total + totals.holdings_total

        return IndividualDetail.assemble(
            user_id=user_row.user_id,
            name=user_row.name,
            email=user_row.email,
            net_worth=net_worth,
            cash_total=totals.cash_total,
            holdings_total=totals.holdings_total,
            portfolio_gain_loss=totals.portfolio_gain_loss,
            accounts=accounts,
            holdings=holdings,
            income_total=cashflow_metrics.income_total,
            expense_total=cashflow_metrics.expense_total,
            net_cash_flow=cashflow_metrics.net_cash_flow,
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
        return PeriodRange.normalized(key=resolved_key, label=label, start=start, end=reference_date)
