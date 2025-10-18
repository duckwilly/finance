"""Service logic for organization listings in the admin area."""
from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from math import ceil

from datetime import date, timedelta

from sqlalchemy.orm import Session

from app.repositories.admin.company_repository import AdminCompanyRepository


@dataclass(frozen=True)
class CompanySummary:
    """Aggregated view of an organization's financial footprint."""

    org_id: int
    name: str
    total_balance: Decimal
    payroll_headcount: int


@dataclass(frozen=True)
class CompanyPage:
    """Paginated collection of ``CompanySummary`` items."""

    items: list[CompanySummary]
    total: int
    page: int
    page_size: int

    @property
    def total_pages(self) -> int:
        """Return the number of pages represented by the dataset."""

        if self.total == 0:
            return 1
        return ceil(self.total / self.page_size)


@dataclass(frozen=True)
class CompanyAccount:
    """Account level details for a single organization."""

    account_id: int
    name: str | None
    type: str
    currency: str
    balance: Decimal


@dataclass(frozen=True)
class CompanyMember:
    """User membership metadata for an organization."""

    user_id: int
    name: str
    email: str | None
    role: str


@dataclass(frozen=True)
class PeriodRange:
    """Normalized representation of a reporting period."""

    key: str
    label: str
    start: date
    end: date


@dataclass(frozen=True)
class PayrollEmployee:
    """Aggregated payroll information for an employee."""

    counterparty_id: int | None
    name: str
    total_compensation: Decimal


@dataclass(frozen=True)
class ExpenseCategorySummary:
    """Expense totals aggregated by category."""

    category_id: int | None
    name: str
    total_spent: Decimal


@dataclass(frozen=True)
class IncomeSeriesPoint:
    """Monthly aggregation of income values for a reporting window."""

    period_start: date
    label: str
    amount: Decimal


@dataclass(frozen=True)
class CompanyDetail:
    """Composite detail view of an organization."""

    org_id: int
    name: str
    total_balance: Decimal
    payroll_headcount: int
    accounts: list[CompanyAccount]
    members: list[CompanyMember]
    period: PeriodRange
    income_total: Decimal
    expense_total: Decimal
    net_cash_flow: Decimal
    payroll_total: Decimal
    payroll_employees: list[PayrollEmployee]
    top_expense_categories: list[ExpenseCategorySummary]
    income_series: list[IncomeSeriesPoint]


class AdminCompanyService:
    """Facade exposing company level aggregates for administrators."""
    _PERIOD_DEFAULT = "ytd"
    _PERIOD_LABELS: dict[str, str] = {
        "ytd": "Year to date",
        "qtd": "Quarter to date",
        "mtd": "Month to date",
        "last_30_days": "Last 30 days",
        "last_90_days": "Last 90 days",
        "last_12_months": "Last 12 months",
    }
    _PERIOD_ORDER: tuple[str, ...] = (
        "ytd",
        "qtd",
        "mtd",
        "last_30_days",
        "last_90_days",
        "last_12_months",
    )

    def __init__(
        self,
        session: Session,
        repository: AdminCompanyRepository | None = None,
    ) -> None:
        self._repository = repository or AdminCompanyRepository(session)

    def list_companies(
        self,
        *,
        page: int,
        page_size: int,
        search: str | None = None,
    ) -> CompanyPage:
        """Return a paginated set of organization aggregates."""

        if page < 1:
            page = 1
        if page_size < 1:
            raise ValueError("page_size must be greater than zero")

        total = self._repository.count_companies(search=search)

        max_page = max(1, ceil(total / page_size)) if total else 1
        page = min(page, max_page)
        offset = (page - 1) * page_size

        rows = self._repository.fetch_company_summaries(
            search=search,
            limit=page_size,
            offset=offset,
        )

        items = [
            CompanySummary(
                org_id=row.org_id,
                name=row.org_name,
                total_balance=row.total_balance,
                payroll_headcount=row.payroll_headcount,
            )
            for row in rows
        ]

        return CompanyPage(
            items=items,
            total=total,
            page=page,
            page_size=page_size,
        )

    def get_company_detail(
        self,
        org_id: int,
        *,
        period_key: str | None = None,
        start_date: date | None = None,
        end_date: date | None = None,
        today: date | None = None,
    ) -> CompanyDetail | None:
        """Return detailed information for a single organization."""

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

        org_row = self._repository.get_company(org_id)

        if org_row is None:
            return None

        accounts = [
            CompanyAccount(
                account_id=row.account_id,
                name=row.account_name,
                type=row.account_type,
                currency=row.account_currency,
                balance=row.balance,
            )
            for row in self._repository.list_accounts(org_id)
        ]

        total_balance = sum((account.balance for account in accounts), Decimal(0))

        payroll_headcount_raw = self._repository.get_payroll_headcount(org_id)

        members = [
            CompanyMember(
                user_id=row.user_id,
                name=row.user_name,
                email=row.user_email,
                role=row.membership_role,
            )
            for row in self._repository.list_members(org_id)
        ]

        cashflow = self._repository.get_cashflow(
            org_id,
            start_date=range_start,
            end_date=range_end,
        )

        income_total = cashflow.income_total
        expense_total = cashflow.expense_total
        net_cash_flow = income_total - expense_total

        monthly_lookup: dict[date, Decimal] = {}
        for row in self._repository.list_income_transactions(
            org_id,
            start_date=range_start,
            end_date=range_end,
        ):
            txn_date = row.txn_date
            month_start = txn_date.replace(day=1)
            amount = row.normalized_amount
            monthly_lookup[month_start] = monthly_lookup.get(month_start, Decimal(0)) + amount

        series: list[IncomeSeriesPoint] = []

        def _increment_month(value: date) -> date:
            if value.month == 12:
                return value.replace(year=value.year + 1, month=1, day=1)
            return value.replace(month=value.month + 1, day=1)

        current_month = range_start.replace(day=1)
        last_month = range_end.replace(day=1)
        while current_month <= last_month:
            amount = monthly_lookup.get(current_month, Decimal(0))
            series.append(
                IncomeSeriesPoint(
                    period_start=current_month,
                    label=current_month.strftime("%b %Y"),
                    amount=amount,
                )
            )
            current_month = _increment_month(current_month)

        payroll_employees = [
            PayrollEmployee(
                counterparty_id=row.counterparty_id,
                name=row.counterparty_name,
                total_compensation=row.total_paid.copy_abs(),
            )
            for row in self._repository.list_payroll_employees(
                org_id,
                start_date=range_start,
                end_date=range_end,
            )
        ]

        payroll_total = sum((employee.total_compensation for employee in payroll_employees), Decimal(0))

        top_expense_categories = [
            ExpenseCategorySummary(
                category_id=row.category_id,
                name=row.category_name,
                total_spent=row.total_spent.copy_abs(),
            )
            for row in self._repository.list_top_expense_categories(
                org_id,
                start_date=range_start,
                end_date=range_end,
            )
        ]

        return CompanyDetail(
            org_id=org_row.org_id,
            name=org_row.org_name,
            total_balance=total_balance,
            payroll_headcount=int(payroll_headcount_raw or 0),
            accounts=accounts,
            members=members,
            period=period,
            income_total=income_total,
            expense_total=expense_total,
            net_cash_flow=net_cash_flow,
            payroll_total=payroll_total,
            payroll_employees=payroll_employees,
            top_expense_categories=top_expense_categories,
            income_series=series,
        )

    @classmethod
    def period_options(cls, *, today: date | None = None) -> list[PeriodRange]:
        """Return the available reporting periods."""

        return [cls._resolve_period(key, today=today) for key in cls._PERIOD_ORDER]

    @classmethod
    def _resolve_period(
        cls,
        period_key: str | None,
        *,
        today: date | None = None,
    ) -> PeriodRange:
        """Resolve the provided key into a date range."""

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
        else:  # YTD
            start = reference_date.replace(month=1, day=1)

        label = cls._PERIOD_LABELS[resolved_key]
        return PeriodRange(key=resolved_key, label=label, start=start, end=reference_date)

