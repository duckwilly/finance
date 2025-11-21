"""ORM-backed analytics helpers used by the chatbot tool registry."""
from __future__ import annotations

from datetime import date, timedelta
from decimal import Decimal
from typing import Any, Optional

import re

from sqlalchemy import case, func, or_, select
from sqlalchemy.orm import Session

from app.models import (
    Account,
    AppUser,
    Category,
    EmploymentContract,
    JournalEntry,
    JournalLine,
    OrgPartyMap,
    Party,
    PartyType,
    PositionAgg,
    Section,
    UserPartyMap,
)
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


def _normalize_direction(value: Any) -> str:
    """Sanitize leaderboard direction."""
    text = str(value or "top").strip().lower()
    return "bottom" if text in {"bottom", "low", "lowest"} else "top"


def _normalize_party_type(value: Any) -> PartyType | None:
    """Convert human-friendly strings into PartyType enums."""
    text = str(value or "all").strip().lower()
    if text in {"company", "companies", "business"}:
        return PartyType.COMPANY
    if text in {"individual", "person", "people", "user"}:
        return PartyType.INDIVIDUAL
    return None


def _normalize_party_type_hint(value: Any) -> PartyType | None:
    """Party type hint that defaults to individuals when not provided."""
    hint = _normalize_party_type(value)
    return hint if hint is not None else PartyType.INDIVIDUAL


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


def leaderboard(
    session: Session,
    scope: UserScope,
    *,
    metric: str = "expenses",
    direction: str = "top",
    party_type: str = "all",
    days: int = 30,
    limit: int = 5,
) -> ToolResult:
    """
    Generalized leaderboard for admin users across supported metrics.

    Metrics: expenses, income, net_stock_gains, category_expenses:<name>
    Direction: top|bottom
    Party type: company|individual|all
    """

    scope.require_admin()
    metric_key = str(metric or "expenses").strip().lower()
    direction_key = _normalize_direction(direction)
    party_filter = _normalize_party_type(party_type)
    start = _start_date(days)
    limit_val = max(1, _clean_int(limit, 5))

    # Extract category when the metric targets a specific expense category
    category_name = None
    if metric_key.startswith("category_expenses"):
        _, _, category_part = metric_key.partition(":")
        category_name = category_part.strip() or None
        metric_key = "category_expenses"

    order_by_clause = (
        lambda expr: expr.asc() if direction_key == "bottom" else expr.desc()
    )
    total_expr = func.coalesce(func.sum(func.abs(JournalLine.amount)), 0).label("total")

    metric_label: str
    period_label: str = f"last {days} days"

    if metric_key in {"expenses", "income", "category_expenses"}:
        if metric_key == "expenses":
            section_filter = Section.name == "expense"
            metric_label = "Total expenses"
        elif metric_key == "income":
            section_filter = Section.name == "income"
            metric_label = "Total income"
        else:
            section_filter = Section.name == "expense"
            metric_label = (
                f"Expenses in {category_name}" if category_name else "Category expenses"
            )

        filters = [section_filter, JournalEntry.txn_date >= start]
        if category_name:
            filters.append(Category.name.ilike(category_name))
        if party_filter:
            filters.append(Party.party_type == party_filter)

        query = (
            select(
                Party.id.label("party_id"),
                Party.display_name.label("party_name"),
                Party.party_type.label("party_type"),
                total_expr,
            )
            .select_from(JournalLine)
            .join(JournalEntry, JournalEntry.id == JournalLine.entry_id)
            .join(Account, Account.id == JournalLine.account_id)
            .join(Party, Party.id == Account.party_id)
            .outerjoin(Category, Category.id == JournalLine.category_id)
            .outerjoin(Section, Section.id == Category.section_id)
            .where(*filters)
            .group_by(Party.id, Party.display_name, Party.party_type)
            .order_by(order_by_clause(total_expr))
            .limit(limit_val)
        )

    elif metric_key in {"net_stock_gains", "stock_gains", "stock_pl"}:
        metric_label = "Net stock gains/losses"
        period_label = "current holdings"
        unrealized_expr = func.coalesce(func.sum(PositionAgg.unrealized_pl), 0).label(
            "total"
        )
        filters = []
        if party_filter:
            filters.append(Party.party_type == party_filter)

        query = (
            select(
                Party.id.label("party_id"),
                Party.display_name.label("party_name"),
                Party.party_type.label("party_type"),
                unrealized_expr,
            )
            .select_from(PositionAgg)
            .join(Account, Account.id == PositionAgg.account_id)
            .join(Party, Party.id == Account.party_id)
            .where(*filters)
            .group_by(Party.id, Party.display_name, Party.party_type)
            .order_by(order_by_clause(unrealized_expr))
            .limit(limit_val)
        )
    else:
        raise ValueError(f"Unsupported metric for leaderboard: {metric}")

    rows = session.execute(query).all()

    metric_slug = metric_key
    if metric_key == "category_expenses" and category_name:
        safe_category = re.sub(r"[^a-z0-9]+", "_", category_name.lower()).strip("_")
        if safe_category:
            metric_slug = f"category_{safe_category}"

    def _party_url(party_id: int, party_type: Any) -> str:
        raw_type = getattr(party_type, "value", party_type) or ""
        type_text = str(raw_type).upper()
        if type_text.startswith("COMPANY"):
            return f"/corporate/{party_id}"
        return f"/individuals/{party_id}"

    data = [
        {
            "party_name": row.party_name,
            "party_id": row.party_id,
            "party_type": getattr(row, "party_type", None),
            "party_url": _party_url(row.party_id, getattr(row, "party_type", "")),
            "metric_label": metric_label,
            "total": _as_float(row.total),
        }
        for row in rows
    ]

    direction_label = "Top" if direction_key == "top" else "Bottom"
    keyword = f"leaderboard_{metric_slug}_{direction_key}"
    chart_title = (
        f"{direction_label} {metric_label.lower()} ({period_label})"
    )

    return ToolResult(
        keyword=keyword,
        title=chart_title,
        rows=data,
        chart_type="bar",
        x_axis="party_name",
        y_axis="total",
        sort="asc" if direction_key == "bottom" else "desc",
        unit="currency",
    )


def top_spenders(
    session: Session,
    scope: UserScope,
    *,
    metric: str = "expenses",
    direction: str = "top",
    party_type: str = "all",
    days: int = 30,
    limit: int = 5,
) -> ToolResult:
    """Backward-compatible alias that routes to the leaderboard helper."""
    return leaderboard(
        session,
        scope,
        metric=metric or "expenses",
        direction=direction or "top",
        party_type=party_type,
        days=days,
        limit=limit,
    )


def party_insights(
    session: Session,
    scope: UserScope,
    *,
    party_id: Optional[int] = None,
    party_name: Optional[str] = None,
    metric: str = "summary",
    days: int = 365,
    granularity: str = "total",
    party_type: str = "individual",
) -> ToolResult:
    """
    Admin-only snapshot for a specific party (individual or company).

    Metrics: summary, income, expenses, net_cash_flow, category_expenses:<name>
    Granularity: total|monthly
    """

    scope.require_admin()
    metric_key = str(metric or "summary").strip().lower()
    granularity_key = str(granularity or "total").strip().lower()
    party_type_filter = _normalize_party_type_hint(party_type)
    extracted_id: Optional[int] = None
    if party_id is None and party_name:
        match = re.search(r"\b(\d+)\b", str(party_name))
        if match:
            extracted_id = int(match.group(1))
            party_id = extracted_id

    def _resolve_party() -> tuple[int, str, Any, Optional[int], Optional[int]]:
        """
        Resolve the target party and capture the canonical profile ids for linking.

        Returns (party_id, display_name, party_type, individual_id, company_id)
        """
        base_query = (
            select(
                Party.id.label("party_id"),
                Party.display_name,
                Party.party_type,
                UserPartyMap.user_id.label("user_id"),
                OrgPartyMap.org_id.label("org_id"),
            )
            .select_from(Party)
            .outerjoin(UserPartyMap, UserPartyMap.party_id == Party.id)
            .outerjoin(OrgPartyMap, OrgPartyMap.party_id == Party.id)
        )

        # Try explicit party id first (authoritative even if type hint disagrees)
        if party_id is not None:
            # Always try org mapping first so company ids remain stable
            mapped_org = session.execute(
                base_query.where(OrgPartyMap.org_id == party_id)
            ).first()
            if mapped_org:
                return (
                    mapped_org.party_id,
                    mapped_org.display_name,
                    mapped_org.party_type,
                    mapped_org.user_id,
                    mapped_org.org_id,
                )

            # Next try legacy user mapping
            mapped_user = session.execute(
                base_query.where(UserPartyMap.user_id == party_id)
            ).first()
            if mapped_user:
                return (
                    mapped_user.party_id,
                    mapped_user.display_name,
                    mapped_user.party_type,
                    mapped_user.user_id,
                    mapped_user.org_id,
                )

            record = session.execute(
                base_query.where(Party.id == party_id)
            ).first()
            if record:
                return (
                    record.party_id,
                    record.display_name,
                    record.party_type,
                    record.user_id,
                    record.org_id,
                )
            # Attempt resolving via application user id mapping (respecting type hint)
            user_match = session.execute(
                base_query.select_from(AppUser)
                .join(Party, Party.id == AppUser.party_id)
                .where(AppUser.id == party_id)
            ).first()
            if user_match:
                return (
                    user_match.party_id,
                    user_match.display_name,
                    user_match.party_type,
                    user_match.user_id,
                    user_match.org_id,
                )

        # Fallback to fuzzy name match
        if party_name:
            query = base_query.where(Party.display_name.ilike(f"%{party_name}%"))
            if party_type_filter:
                query = query.where(Party.party_type == party_type_filter)
            record = session.execute(
                query.order_by(Party.display_name.asc()).limit(1)
            ).first()
            if record:
                return (
                    record.party_id,
                    record.display_name,
                    record.party_type,
                    record.user_id,
                    record.org_id,
                )
        raise ValueError("Party not found; provide a valid party_id or name")

    def _party_url(
        target_id: int,
        target_type: Any,
        *,
        individual_id: Optional[int],
        company_id: Optional[int],
    ) -> str:
        raw_type = getattr(target_type, "value", target_type) or ""
        type_text = str(raw_type).upper()
        if type_text.startswith("COMPANY"):
            link_id = company_id or target_id
            return f"/corporate/{link_id}"
        link_id = individual_id or target_id
        return f"/individuals/{link_id}"

    (
        target_party_id,
        target_party_name,
        target_party_type,
        target_user_id,
        target_company_id,
    ) = _resolve_party()
    requested_identifier = party_id or extracted_id
    link_individual_id: Optional[int] = None
    link_company_id: Optional[int] = None

    if target_party_type == PartyType.COMPANY:
        link_company_id = requested_identifier or target_company_id
    else:
        link_individual_id = requested_identifier or target_user_id

    link_party_id = (
        link_company_id
        or link_individual_id
        or target_user_id
        or target_company_id
        or target_party_id
    )
    employee_rows: list[dict[str, Any]] = []
    if target_party_type == PartyType.COMPANY:
        employee_records = session.execute(
            select(
                Party.id,
                Party.display_name,
                UserPartyMap.user_id.label("user_id"),
                EmploymentContract.position_title,
            )
            .select_from(EmploymentContract)
            .join(Party, Party.id == EmploymentContract.employee_party_id)
            .outerjoin(UserPartyMap, UserPartyMap.party_id == Party.id)
            .where(
                EmploymentContract.employer_party_id == target_party_id,
                EmploymentContract.start_date <= date.today(),
                or_(
                    EmploymentContract.end_date.is_(None),
                    EmploymentContract.end_date >= date.today(),
                ),
            )
            .order_by(Party.display_name.asc())
            .limit(50)
        ).all()
        for record in employee_records:
            link_id = record.user_id or record.id
            employee_rows.append(
                {
                    "party_name": record.display_name,
                    "party_id": link_id,
                    "position": record.position_title,
                    "party_type": PartyType.INDIVIDUAL,
                    "party_url": _party_url(
                        record.id,
                        PartyType.INDIVIDUAL,
                        individual_id=link_id,
                        company_id=None,
                    ),
                }
            )
    start = _start_date(days)
    category_name = None
    if metric_key.startswith("category_expenses"):
        _, _, category_part = metric_key.partition(":")
        category_name = category_part.strip() or None
        metric_key = "category_expenses"

    period_label = f"last {days} days"
    metric_label = "Summary"

    base_filters = [Account.party_id == target_party_id, JournalEntry.txn_date >= start]

    if metric_key in {"income", "expenses", "category_expenses"}:
        if metric_key == "income":
            section_filter = Section.name == "income"
            metric_label = "Income"
        elif metric_key == "expenses":
            section_filter = Section.name == "expense"
            metric_label = "Expenses"
        else:
            section_filter = Section.name == "expense"
            metric_label = f"Expenses in {category_name}" if category_name else "Category expenses"

        filters = base_filters + [section_filter]
        if category_name:
            filters.append(Category.name.ilike(category_name))

        if granularity_key == "monthly":
            period = func.date_format(JournalEntry.txn_date, "%Y-%m").label("period")
            total_expr = func.coalesce(func.sum(func.abs(JournalLine.amount)), 0).label("total")
            query = (
                select(period, total_expr)
                .select_from(JournalLine)
                .join(JournalEntry, JournalEntry.id == JournalLine.entry_id)
                .join(Account, Account.id == JournalLine.account_id)
                .outerjoin(Category, Category.id == JournalLine.category_id)
                .outerjoin(Section, Section.id == Category.section_id)
                .where(*filters)
                .group_by(period)
                .order_by(period.asc())
            )
            rows = session.execute(query).all()
            data_rows = [
                {
                    "party_name": target_party_name,
                    "party_id": link_party_id,
                    "party_type": target_party_type,
                    "party_url": _party_url(
                        target_party_id,
                        target_party_type,
                        individual_id=link_individual_id,
                        company_id=link_company_id,
                    ),
                    "period": row.period,
                    "metric_label": metric_label,
                    "total": _as_float(row.total),
                }
                for row in rows
            ]
            chart_type = "line"
            x_axis = "period"
            y_axis = "total"
            sort = "asc"
        else:
            total_expr = func.coalesce(func.sum(func.abs(JournalLine.amount)), 0).label("total")
            query = (
                select(total_expr)
                .select_from(JournalLine)
                .join(JournalEntry, JournalEntry.id == JournalLine.entry_id)
                .join(Account, Account.id == JournalLine.account_id)
                .outerjoin(Category, Category.id == JournalLine.category_id)
                .outerjoin(Section, Section.id == Category.section_id)
                .where(*filters)
            )
            total = session.execute(query).scalar_one_or_none() or 0
            data_rows = [
                {
                    "party_name": target_party_name,
                    "party_id": link_party_id,
                    "party_type": target_party_type,
                    "party_url": _party_url(
                        target_party_id,
                        target_party_type,
                        individual_id=link_individual_id,
                        company_id=link_company_id,
                    ),
                    "metric_label": metric_label,
                    "period": period_label,
                    "total": _as_float(total),
                }
            ]
            chart_type = "bar"
            x_axis = "party_name"
            y_axis = "total"
            sort = None

    elif metric_key in {"net_cash_flow", "summary"}:
        income_expr = func.coalesce(
            func.sum(case((Section.name == "income", func.abs(JournalLine.amount)), else_=0)),
            0,
        ).label("income_total")
        expense_expr = func.coalesce(
            func.sum(case((Section.name == "expense", func.abs(JournalLine.amount)), else_=0)),
            0,
        ).label("expense_total")

        query = (
            select(income_expr, expense_expr)
            .select_from(JournalLine)
            .join(JournalEntry, JournalEntry.id == JournalLine.entry_id)
            .join(Account, Account.id == JournalLine.account_id)
            .outerjoin(Category, Category.id == JournalLine.category_id)
            .outerjoin(Section, Section.id == Category.section_id)
            .where(*base_filters)
        )
        income_total, expense_total = session.execute(query).one()
        income_val = _as_float(income_total)
        expense_val = _as_float(expense_total)
        net_cash_flow = income_val - expense_val
        metric_label = "Income vs expenses"

        data_rows = [
            {
                "party_name": target_party_name,
                "party_id": link_party_id,
                "party_type": target_party_type,
                "party_url": _party_url(
                    target_party_id,
                    target_party_type,
                    individual_id=link_individual_id,
                    company_id=link_company_id,
                ),
                "metric_label": "Income",
                "period": period_label,
                "total": income_val,
            },
            {
                "party_name": target_party_name,
                "party_id": link_party_id,
                "party_type": target_party_type,
                "party_url": _party_url(
                    target_party_id,
                    target_party_type,
                    individual_id=link_individual_id,
                    company_id=link_company_id,
                ),
                "metric_label": "Expenses",
                "period": period_label,
                "total": expense_val,
            },
            {
                "party_name": target_party_name,
                "party_id": link_party_id,
                "party_type": target_party_type,
                "party_url": _party_url(
                    target_party_id,
                    target_party_type,
                    individual_id=link_individual_id,
                    company_id=link_company_id,
                ),
                "metric_label": "Net cash flow",
                "period": period_label,
                "total": net_cash_flow,
            },
        ]
        chart_type = "bar"
        x_axis = "metric_label"
        y_axis = "total"
        sort = None
    else:
        raise ValueError(f"Unsupported metric for party insights: {metric}")

    keyword = f"party_insights_{target_party_id}_{metric_key}"
    title = f"{metric_label} for {target_party_name} ({period_label})"

    main_result = ToolResult(
        keyword=keyword,
        title=title,
        rows=data_rows,
        chart_type=chart_type,
        x_axis=x_axis,
        y_axis=y_axis,
        sort=sort,
        unit="currency",
    )

    if employee_rows and target_party_type == PartyType.COMPANY:
        employee_result = ToolResult(
            keyword=f"company_employees_{link_company_id or target_party_id}",
            title=f"Employees at {target_party_name}",
            rows=employee_rows,
            chart_type=None,  # table only
            x_axis=None,
            y_axis=None,
            sort=None,
        )
        return [main_result, employee_result]

    return main_result


__all__ = [
    "expenses_by_category",
    "income_by_category",
    "monthly_cash_flow_comparison",
    "spending_trend",
    "leaderboard",
    "party_insights",
    "top_spenders",
]
