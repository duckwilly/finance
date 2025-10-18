"""Compact domain models backing admin company views."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from typing import Iterable, Sequence


@dataclass(frozen=True, slots=True)
class PeriodRange:
    """Normalized representation of a reporting period."""

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
class CompanySummary:
    """Aggregated view of an organization's financial footprint."""

    org_id: int
    name: str
    total_balance: Decimal
    payroll_headcount: int

    @classmethod
    def from_row(cls, row: object) -> "CompanySummary":
        return cls(
            org_id=int(getattr(row, "org_id")),
            name=str(getattr(row, "org_name")),
            total_balance=Decimal(getattr(row, "total_balance")),
            payroll_headcount=int(getattr(row, "payroll_headcount")),
        )


@dataclass(frozen=True, slots=True)
class CompanyPage:
    """Paginated collection of company summaries."""

    items: Sequence[CompanySummary]
    total: int
    page: int
    page_size: int

    @property
    def total_pages(self) -> int:
        """Return the number of pages represented by the dataset."""

        if self.total <= 0:
            return 1
        return (self.total + self.page_size - 1) // self.page_size

    @classmethod
    def assemble(
        cls,
        items: Iterable[CompanySummary],
        *,
        total: int,
        page: int,
        page_size: int,
    ) -> "CompanyPage":
        return cls(items=tuple(items), total=int(total), page=int(page), page_size=int(page_size))


@dataclass(frozen=True, slots=True)
class CompanyAccount:
    account_id: int
    name: str | None
    type: str
    currency: str
    balance: Decimal

    @classmethod
    def from_row(cls, row: object) -> "CompanyAccount":
        return cls(
            account_id=int(getattr(row, "account_id")),
            name=cls._nullable_text(getattr(row, "account_name")),
            type=str(getattr(row, "account_type")),
            currency=str(getattr(row, "account_currency")),
            balance=Decimal(getattr(row, "balance")),
        )

    @staticmethod
    def _nullable_text(value: object | None) -> str | None:
        return None if value is None else str(value)


@dataclass(frozen=True, slots=True)
class CompanyMember:
    user_id: int
    name: str
    email: str | None
    role: str

    @classmethod
    def from_row(cls, row: object) -> "CompanyMember":
        return cls(
            user_id=int(getattr(row, "user_id")),
            name=str(getattr(row, "user_name")),
            email=CompanyAccount._nullable_text(getattr(row, "user_email")),
            role=str(getattr(row, "membership_role")),
        )


@dataclass(frozen=True, slots=True)
class PayrollEmployee:
    counterparty_id: int | None
    name: str
    total_compensation: Decimal

    @classmethod
    def from_row(cls, row: object) -> "PayrollEmployee":
        counterparty_id = getattr(row, "counterparty_id")
        return cls(
            counterparty_id=None if counterparty_id is None else int(counterparty_id),
            name=str(getattr(row, "counterparty_name")),
            total_compensation=Decimal(getattr(row, "total_paid")).copy_abs(),
        )


@dataclass(frozen=True, slots=True)
class ExpenseCategorySummary:
    category_id: int | None
    name: str
    total_spent: Decimal

    @classmethod
    def from_row(cls, row: object) -> "ExpenseCategorySummary":
        category_id = getattr(row, "category_id")
        return cls(
            category_id=None if category_id is None else int(category_id),
            name=str(getattr(row, "category_name")),
            total_spent=Decimal(getattr(row, "total_spent")).copy_abs(),
        )


@dataclass(frozen=True, slots=True)
class IncomeSeriesPoint:
    period_start: date
    label: str
    amount: Decimal

    @classmethod
    def create(cls, *, period_start: date, label: str, amount: Decimal) -> "IncomeSeriesPoint":
        return cls(period_start=period_start, label=str(label), amount=Decimal(amount))


@dataclass(frozen=True, slots=True)
class CompanyDetail:
    org_id: int
    name: str
    total_balance: Decimal
    payroll_headcount: int
    accounts: Sequence[CompanyAccount]
    members: Sequence[CompanyMember]
    period: PeriodRange
    income_total: Decimal
    expense_total: Decimal
    net_cash_flow: Decimal
    payroll_total: Decimal
    payroll_employees: Sequence[PayrollEmployee]
    top_expense_categories: Sequence[ExpenseCategorySummary]
    income_series: Sequence[IncomeSeriesPoint]

    @classmethod
    def assemble(
        cls,
        *,
        org_id: int,
        name: str,
        total_balance: Decimal,
        payroll_headcount: int,
        accounts: Iterable[CompanyAccount],
        members: Iterable[CompanyMember],
        period: PeriodRange,
        income_total: Decimal,
        expense_total: Decimal,
        net_cash_flow: Decimal,
        payroll_total: Decimal,
        payroll_employees: Iterable[PayrollEmployee],
        top_expense_categories: Iterable[ExpenseCategorySummary],
        income_series: Iterable[IncomeSeriesPoint],
    ) -> "CompanyDetail":
        return cls(
            org_id=int(org_id),
            name=str(name),
            total_balance=Decimal(total_balance),
            payroll_headcount=int(payroll_headcount),
            accounts=tuple(accounts),
            members=tuple(members),
            period=period,
            income_total=Decimal(income_total),
            expense_total=Decimal(expense_total),
            net_cash_flow=Decimal(net_cash_flow),
            payroll_total=Decimal(payroll_total),
            payroll_employees=tuple(payroll_employees),
            top_expense_categories=tuple(top_expense_categories),
            income_series=tuple(income_series),
        )


__all__ = [
    "CompanyAccount",
    "CompanyDetail",
    "CompanyMember",
    "CompanyPage",
    "CompanySummary",
    "ExpenseCategorySummary",
    "IncomeSeriesPoint",
    "PayrollEmployee",
    "PeriodRange",
]

