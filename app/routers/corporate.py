"""Routes serving dashboards for corporate users."""
from __future__ import annotations

from collections.abc import Generator
from decimal import Decimal
from types import SimpleNamespace

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import HTMLResponse
from sqlalchemy.orm import Session, sessionmaker

from app.core.formatting import humanize_currency
from app.core.logger import get_logger
from app.core.security import AuthenticatedUser, require_company_access
from app.core.templates import templates
from app.db.session import get_sessionmaker
from app.routers.dashboard_views import HTMX_SHELL_CONFIG, render_dashboard_template
from app.schemas.admin import (
    LineChartData,
    ListView,
    ListViewColumn,
    ListViewRow,
    PieChartData,
)
from app.schemas.companies import CompanyDashboard
from app.services import CompaniesService

router = APIRouter(prefix="/corporate", tags=["corporate"])
SESSION_FACTORY: sessionmaker = get_sessionmaker()
LOGGER = get_logger(__name__)


def get_db_session() -> Generator[Session, None, None]:
    """Yield a database session for the request lifecycle."""

    session = SESSION_FACTORY()
    try:
        yield session
    finally:
        session.close()


def get_companies_service() -> CompaniesService:
    """Return a service instance per request."""

    return CompaniesService()


def _user_is_admin(user: AuthenticatedUser) -> bool:
    return user.role == "admin" or "ADMIN" in user.roles


def _build_pie_chart(title: str, items: list[tuple[str, float]], hint: str | None = None) -> PieChartData:
    labels = [label for label, value in items if value is not None]
    values = [float(abs(value)) for _, value in items if value is not None]
    if not labels or not values:
        return PieChartData(title=title, labels=["No data"], values=[0], hint=hint)
    return PieChartData(title=title, labels=labels, values=values, hint=hint)


def _build_line_chart(
    title: str,
    series,
    series_label: str,
    hint: str | None = None,
) -> LineChartData:
    labels = [point.label for point in series] if series else []
    values: list[float] = []

    for point in series or []:
        try:
            values.append(float(point.value))
        except (TypeError, ValueError):
            values.append(0.0)

    if not labels or not values:
        labels = ["No data"]
        values = [0]

    return LineChartData(
        title=title,
        labels=labels,
        values=values,
        series_label=series_label,
        hint=hint,
    )


def _build_bar_chart(
    title: str,
    breakdown,
    *,
    hint: str | None = None,
    series_label: str = "Expenses",
    max_bars: int = 7,
) -> LineChartData:
    labels: list[str] = []
    values: list[float] = []
    overflow_total = Decimal("0")

    for index, category in enumerate(sorted(breakdown or [], key=lambda item: item.total, reverse=True)):
        total = Decimal(category.total or 0)
        if max_bars and index >= max_bars:
            overflow_total += total
            continue
        labels.append(category.name)
        try:
            values.append(float(total))
        except (TypeError, ValueError):
            values.append(0.0)

    if overflow_total:
        labels.append("Other")
        values.append(float(overflow_total))

    if not labels or not values:
        labels = ["No data"]
        values = [0]

    return LineChartData(
        title=title,
        labels=labels,
        values=values,
        series_label=series_label,
        hint=hint,
    )


def _category_panel(
    title: str,
    breakdown,
    *,
    list_id: str,
    search_placeholder: str,
    empty_message: str,
    hint: str,
    chart_type: str = "pie",
    chart_data: PieChartData | LineChartData | None = None,
) -> tuple[SimpleNamespace, PieChartData | LineChartData, ListView]:
    items: list[tuple[str, float]] = []
    rows: list[ListViewRow] = []

    for category in breakdown:
        items.append((category.name, float(category.total)))
        recent = category.transactions[0] if category.transactions else None
        recent_text = None
        if recent:
            description = recent.description or "Recent transaction"
            recent_text = f"{recent.txn_date.strftime('%d %b %Y')} — {description}"

        rows.append(
            ListViewRow(
                key=category.name,
                values={
                    "category": category.name,
                    "recent": recent_text or "—",
                    "total": category.total,
                },
                search_text=category.name.lower(),
            )
        )

    view = ListView(
        title=title,
        columns=[
            ListViewColumn(key="category", title="Category"),
            ListViewColumn(key="recent", title="Recent activity"),
            ListViewColumn(key="total", title="Total", column_type="currency", align="right"),
        ],
        rows=rows,
        search_placeholder=search_placeholder,
        empty_message=empty_message,
    )

    return (
        SimpleNamespace(chart_type=chart_type, list_id=list_id),
        chart_data or _build_pie_chart(title, items, hint=hint),
        view,
    )


def _company_panel_payload(
    panel_name: str,
    dashboard: CompanyDashboard,
    *,
    include_links: bool = False,
) -> tuple[
    SimpleNamespace,
    PieChartData | LineChartData,
    ListView,
]:
    if panel_name == "income":
        return _category_panel(
            "Income by category",
            dashboard.income_breakdown,
            list_id="company-income-list",
            search_placeholder="Search income",
            empty_message="No income captured during this window.",
            hint="Revenue per month across the full simulation window.",
            chart_type="line",
            chart_data=_build_line_chart(
                "Monthly income",
                dashboard.income_trend,
                "Income",
                hint="Revenue by month across the simulated period.",
            ),
        )

    if panel_name == "expenses":
        return _category_panel(
            "Expenses by category",
            dashboard.expense_breakdown,
            list_id="company-expense-list",
            search_placeholder="Search expenses",
            empty_message="No expenses captured during this window.",
            hint="Category spend for the latest reporting window.",
            chart_type="bar",
            chart_data=_build_bar_chart(
                "Top expenses by category",
                dashboard.expense_breakdown,
                hint=f"Highest spend categories across {dashboard.period_label}.",
            ),
        )

    if panel_name == "profit":
        rows = [
            ListViewRow(
                key="income",
                values={"metric": "Income", "amount": dashboard.summary.period_income},
                search_text="income",
            ),
            ListViewRow(
                key="expenses",
                values={"metric": "Expenses", "amount": dashboard.summary.period_expenses},
                search_text="expenses",
            ),
            ListViewRow(
                key="net_cash_flow",
                values={"metric": "Net cash flow", "amount": dashboard.summary.net_cash_flow},
                search_text="net cash flow",
            ),
            ListViewRow(
                key="total_profit",
                values={"metric": "Total profit to date", "amount": dashboard.summary.total_profit},
                search_text="profit",
            ),
        ]

        view = ListView(
            title="Profit snapshot",
            columns=[
                ListViewColumn(key="metric", title="Metric"),
                ListViewColumn(key="amount", title="Amount", column_type="currency", align="right"),
            ],
            rows=rows,
            search_placeholder="Search metrics",
            empty_message="No profit data available for this organisation.",
        )

        return (
            SimpleNamespace(chart_type="line", list_id="company-profit-list"),
            _build_line_chart(
                "Profit over time",
                dashboard.profit_trend,
                "Profit",
                hint="Income minus expenses, per month across the simulated period.",
            ),
            view,
        )

    if panel_name == "employees":
        payroll_entries = dashboard.payroll
        rows: list[ListViewRow] = []
        chart_pairs: list[tuple[str, float]] = []
        max_slices = 7

        for entry in payroll_entries[:max_slices]:
            chart_pairs.append((entry.name, float(entry.salary_amount)))

        if len(payroll_entries) > max_slices:
            others_total = sum((entry.salary_amount for entry in payroll_entries[max_slices:]), Decimal("0"))
            chart_pairs.append(("Others", float(others_total)))

        for entry in payroll_entries:
            links = {"employee": f"/individuals/{entry.user_id}"} if include_links else None
            rows.append(
                ListViewRow(
                    key=str(entry.user_id),
                    values={"employee": entry.name, "salary": entry.salary_amount},
                    search_text=entry.name.lower(),
                    links=links,
                )
            )

        view = ListView(
            title="Employees",
            columns=[
                ListViewColumn(key="employee", title="Employee"),
                ListViewColumn(key="salary", title="Monthly salary", column_type="currency", align="right"),
            ],
            rows=rows,
            search_placeholder="Search employees",
            empty_message="No salary records available for this organisation.",
        )

        return (
            SimpleNamespace(chart_type="pie", list_id="company-employees-list"),
            _build_pie_chart(
                "Payroll distribution",
                chart_pairs,
                hint="Salary allocation across employees.",
            ),
            view,
        )

    raise HTTPException(status_code=404, detail="Unknown dashboard panel")


@router.get("/{company_id}", response_class=HTMLResponse, summary="Company dashboard")
async def read_company_dashboard(
    request: Request,
    company_id: int,
    user: AuthenticatedUser = Depends(require_company_access),
    service: CompaniesService = Depends(get_companies_service),
    session: Session = Depends(get_db_session),
) -> HTMLResponse:
    """Render the dashboard dedicated to a specific company."""

    LOGGER.info("Company dashboard requested", extra={"company_id": company_id})
    try:
        dashboard = service.get_dashboard(session, company_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc

    is_admin = _user_is_admin(user)
    panel_config, panel_chart, panel_view = _company_panel_payload(
        "income",
        dashboard,
        include_links=is_admin,
    )

    cards = [
        {
            "panel": "income",
            "label": "Income",
            "value": dashboard.summary.period_income,
            "value_type": "currency",
            "decimals": 2,
            "hint": "Revenue captured in this window.",
        },
        {
            "panel": "expenses",
            "label": "Expenses",
            "value": dashboard.summary.period_expenses,
            "value_type": "currency",
            "decimals": 2,
            "hint": "Operating and capital expenses.",
        },
        {
            "panel": "profit",
            "label": "Profit",
            "value": dashboard.summary.net_cash_flow,
            "value_type": "currency",
            "decimals": 2,
            "hint": "Net cash flow for the selected period.",
        },
        {
            "panel": "employees",
            "label": "Employees",
            "value": dashboard.summary.employee_count,
            "value_type": "number",
            "decimals": 0,
            "hint": "Monthly payroll {}".format(
                humanize_currency(dashboard.summary.monthly_salary_cost, decimals=2),
            ),
        },
    ]

    return render_dashboard_template(
        request,
        view_key="company",
        hero_title=dashboard.profile.name,
        hero_eyebrow="Company dashboard",
        hero_badge=None,
        cards=cards,
        panel_name="income",
        panel_chart=panel_chart,
        panel_config=panel_config,
        panel_view=panel_view,
        panel_endpoint="render_company_panel",
        panel_params={"company_id": company_id},
        show_admin_return=is_admin,
        page_title=f"{dashboard.profile.name} • Company dashboard",
        list_htmx_config=HTMX_SHELL_CONFIG if is_admin else None,
    )


@router.get("/{company_id}/panel/{panel_name}", response_class=HTMLResponse, summary="Company dashboard panel")
async def render_company_panel(
    request: Request,
    company_id: int,
    panel_name: str,
    user: AuthenticatedUser = Depends(require_company_access),
    service: CompaniesService = Depends(get_companies_service),
    session: Session = Depends(get_db_session),
) -> HTMLResponse:
    """Return a single company dashboard panel for HTMX swaps."""

    try:
        dashboard = service.get_dashboard(session, company_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc

    is_admin = _user_is_admin(user)
    panel_config, panel_chart, panel_view = _company_panel_payload(
        panel_name,
        dashboard,
        include_links=is_admin,
    )

    return templates.TemplateResponse(
        request=request,
        name="dashboard/partials/panel.html",
        context={
            "chart": panel_chart,
            "chart_type": panel_config.chart_type,
            "list_id": panel_config.list_id,
            "panel_key": panel_name,
            "request": request,
            "view": panel_view,
            "list_htmx_config": HTMX_SHELL_CONFIG if is_admin else None,
        },
    )
