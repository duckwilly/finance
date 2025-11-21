"""ORM-backed analytics helpers used by the chatbot tool registry."""
from __future__ import annotations

from datetime import date, timedelta
from decimal import Decimal
from typing import Any, Optional

from sqlalchemy import case, func, select
from sqlalchemy.orm import Session

from app.models import Account, Category, JournalEntry, JournalLine, Party, Section
from .types import ToolResult, UserScope


def _as_float(value: Decimal | float | int | None) -> float:
    if value is None:
        return 0.0
    if isinstance(value, Decimal):
        return float(value)
    return float(value)


def _clean_int(value: Any, default: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _start_date(days: Any) -> date:
    clean_days = max(1, _clean_int(days, 30))
    return date.today() - timedelta(days=clean_days)


def expenses_by_category(
    session: Session,
    scope: UserScope,
    *,
    days: int = 30,
    limit: int = 8,
    party_id: Optional[int] = None,
) -> ToolResult:
    """Return total expenses grouped by category for the requested window."""
    start = _start_date(days)
    target_party = scope.resolve_party_id(party_id)

    category_label = func.coalesce(Category.name, "Uncategorised").label("category")
    total_expr = func.coalesce(func.sum(func.abs(JournalLine.amount)), 0).label("total")

    filters = [Section.name == "expense", JournalEntry.txn_date >= start]
    if target_party:
        filters.append(Account.party_id == target_party)

    query = (
        select(category_label, total_expr)
        .select_from(JournalLine)
        .join(JournalEntry, JournalEntry.id == JournalLine.entry_id)
        .join(Account, Account.id == JournalLine.account_id)
        .outerjoin(Category, Category.id == JournalLine.category_id)
        .outerjoin(Section, Section.id == Category.section_id)
        .where(*filters)
        .group_by(category_label)
        .order_by(total_expr.desc())
        .limit(limit)
    )
    rows = session.execute(query).all()
    data = [{"category": row.category, "total": _as_float(row.total)} for row in rows]

    return ToolResult(
        keyword="expenses_by_category",
        title=f"Expenses by category (last {days} days)",
        rows=data,
        chart_type="bar",
        x_axis="category",
        y_axis="total",
        sort="desc",
        unit="currency",
    )


def income_by_category(
    session: Session,
    scope: UserScope,
    *,
    days: int = 30,
    limit: int = 8,
    party_id: Optional[int] = None,
) -> ToolResult:
    """Return total income grouped by category for the requested window."""
    start = _start_date(days)
    target_party = scope.resolve_party_id(party_id)

    category_label = func.coalesce(Category.name, "Uncategorised").label("category")
    total_expr = func.coalesce(func.sum(func.abs(JournalLine.amount)), 0).label("total")

    filters = [Section.name == "income", JournalEntry.txn_date >= start]
    if target_party:
        filters.append(Account.party_id == target_party)

    query = (
        select(category_label, total_expr)
        .select_from(JournalLine)
        .join(JournalEntry, JournalEntry.id == JournalLine.entry_id)
        .join(Account, Account.id == JournalLine.account_id)
        .outerjoin(Category, Category.id == JournalLine.category_id)
        .outerjoin(Section, Section.id == Category.section_id)
        .where(*filters)
        .group_by(category_label)
        .order_by(total_expr.desc())
        .limit(limit)
    )
    rows = session.execute(query).all()
    data = [{"category": row.category, "total": _as_float(row.total)} for row in rows]

    return ToolResult(
        keyword="income_by_category",
        title=f"Income by category (last {days} days)",
        rows=data,
        chart_type="bar",
        x_axis="category",
        y_axis="total",
        sort="desc",
        unit="currency",
    )


def monthly_cash_flow_comparison(
    session: Session,
    scope: UserScope,
    *,
    months: int = 6,
    party_id: Optional[int] = None,
) -> ToolResult:
    """Return month-by-month income vs expense totals."""
    month_window = _clean_int(months, 6)
    start = _start_date(month_window * 30)
    target_party = scope.resolve_party_id(party_id)

    period_label = func.date_format(JournalEntry.txn_date, "%Y-%m").label("period")
    income_total = func.coalesce(
        func.sum(
            case((Section.name == "income", func.abs(JournalLine.amount)), else_=0)
        ),
        0,
    ).label("income_total")
    expense_total = func.coalesce(
        func.sum(
            case((Section.name == "expense", func.abs(JournalLine.amount)), else_=0)
        ),
        0,
    ).label("expenses")

    filters = [JournalEntry.txn_date >= start]
    if target_party:
        filters.append(Account.party_id == target_party)

    query = (
        select(period_label, income_total, expense_total)
        .select_from(JournalLine)
        .join(JournalEntry, JournalEntry.id == JournalLine.entry_id)
        .join(Account, Account.id == JournalLine.account_id)
        .outerjoin(Category, Category.id == JournalLine.category_id)
        .outerjoin(Section, Section.id == Category.section_id)
        .where(*filters)
        .group_by(period_label)
        .order_by(period_label.asc())
    )
    rows = session.execute(query).all()
    data = [
        {
            "month": row.period,
            "income_total": _as_float(row.income_total),
            "expenses": _as_float(row.expenses),
        }
        for row in rows
    ]

    return ToolResult(
        keyword="monthly_comparison",
        title="Monthly income vs expenses",
        rows=data,
        chart_type="bar",
        x_axis="month",
        y_axis=["income_total", "expenses"],
        sort="asc",
        unit="currency",
    )


def spending_trend(
    session: Session,
    scope: UserScope,
    *,
    days: int = 180,
    party_id: Optional[int] = None,
) -> ToolResult:
    """Return a time-series of expenses for the provided window."""
    start = _start_date(days)
    target_party = scope.resolve_party_id(party_id)

    period_label = func.date_format(JournalEntry.txn_date, "%Y-%m").label("period")
    total_expr = func.coalesce(func.sum(func.abs(JournalLine.amount)), 0).label("total")

    filters = [Section.name == "expense", JournalEntry.txn_date >= start]
    if target_party:
        filters.append(Account.party_id == target_party)

    query = (
        select(period_label, total_expr)
        .select_from(JournalLine)
        .join(JournalEntry, JournalEntry.id == JournalLine.entry_id)
        .join(Account, Account.id == JournalLine.account_id)
        .outerjoin(Category, Category.id == JournalLine.category_id)
        .outerjoin(Section, Section.id == Category.section_id)
        .where(*filters)
        .group_by(period_label)
        .order_by(period_label.asc())
    )
    rows = session.execute(query).all()
    data = [{"month": row.period, "total": _as_float(row.total)} for row in rows]

    return ToolResult(
        keyword="monthly_expense_trend",
        title=f"Expense trend (last {days} days)",
        rows=data,
        chart_type="line",
        x_axis="month",
        y_axis="total",
        sort="asc",
        unit="currency",
    )


def top_spenders(
    session: Session,
    scope: UserScope,
    *,
    days: int = 30,
    limit: int = 5,
) -> ToolResult:
    """Return a leaderboard of parties with the highest expenses."""
    scope.require_admin()
    start = _start_date(days)

    total_expr = func.coalesce(func.sum(func.abs(JournalLine.amount)), 0).label("total")

    query = (
        select(Party.id.label("party_id"), Party.display_name, total_expr)
        .select_from(JournalLine)
        .join(JournalEntry, JournalEntry.id == JournalLine.entry_id)
        .join(Account, Account.id == JournalLine.account_id)
        .join(Party, Party.id == Account.party_id)
        .outerjoin(Category, Category.id == JournalLine.category_id)
        .outerjoin(Section, Section.id == Category.section_id)
        .where(Section.name == "expense", JournalEntry.txn_date >= start)
        .group_by(Party.id, Party.display_name)
        .order_by(total_expr.desc())
        .limit(limit)
    )

    rows = session.execute(query).all()
    data = [
        {
            "party_name": row.display_name,
            "party_id": row.party_id,
            "total": _as_float(row.total),
        }
        for row in rows
    ]

    return ToolResult(
        keyword="top_spenders",
        title=f"Top spenders in the last {days} days",
        rows=data,
        chart_type="bar",
        x_axis="party_name",
        y_axis="total",
        sort="desc",
        unit="currency",
    )


__all__ = [
    "expenses_by_category",
    "income_by_category",
    "monthly_cash_flow_comparison",
    "spending_trend",
    "top_spenders",
]
