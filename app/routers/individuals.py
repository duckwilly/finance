"""Routes serving dashboards for individual users."""
from __future__ import annotations

from collections.abc import Generator
from decimal import Decimal
from types import SimpleNamespace

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import HTMLResponse
from sqlalchemy.orm import Session, sessionmaker

from app.core.logger import get_logger
from app.core.security import AuthenticatedUser, require_individual_access
from app.core.templates import templates
from app.db.session import get_sessionmaker
from app.routers.dashboard_views import render_dashboard_template
from app.schemas.admin import LineChartData, ListView, ListViewColumn, ListViewRow, PieChartData
from app.schemas.individuals import IndividualDashboard
from app.services import IndividualsService

router = APIRouter(prefix="/individuals", tags=["individuals"])
SESSION_FACTORY: sessionmaker = get_sessionmaker()
LOGGER = get_logger(__name__)


def get_db_session() -> Generator[Session, None, None]:
    """Yield a database session for the request lifecycle."""

    session = SESSION_FACTORY()
    try:
        yield session
    finally:
        session.close()


def get_individuals_service() -> IndividualsService:
    """Return a service instance per request."""

    return IndividualsService()


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


def _category_panel(
    title: str,
    breakdown,
    *,
    list_id: str,
    search_placeholder: str,
    empty_message: str,
    hint: str,
) -> tuple[SimpleNamespace, PieChartData, ListView]:
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
        SimpleNamespace(chart_type="pie", list_id=list_id),
        _build_pie_chart(title, items, hint=hint),
        view,
    )


def _individual_panel_payload(
    panel_name: str,
    dashboard: IndividualDashboard,
) -> tuple[SimpleNamespace, PieChartData | LineChartData, ListView]:
    if panel_name == "accounts":
        rows: list[ListViewRow] = []
        for account in dashboard.accounts:
            label = account.name or f"Account #{account.id}"
            rows.append(
                ListViewRow(
                    key=str(account.id),
                    values={
                        "name": label,
                        "type": account.type.replace("_", " ").title(),
                        "currency": account.currency,
                        "balance": account.balance,
                    },
                    search_text=f"{label} {account.type} {account.currency}".lower(),
                )
            )

        view = ListView(
            title="Accounts",
            columns=[
                ListViewColumn(key="name", title="Account"),
                ListViewColumn(key="type", title="Type"),
                ListViewColumn(key="currency", title="Currency", align="center"),
                ListViewColumn(key="balance", title="Balance", column_type="currency", align="right"),
            ],
            rows=rows,
            search_placeholder="Search accounts",
            empty_message="No accounts recorded for this user.",
        )
        return (
            SimpleNamespace(chart_type="line", list_id="individual-accounts-list"),
            _build_line_chart(
                "Net worth over time",
                dashboard.net_worth_trend,
                "Net worth",
                hint="Month-end balances across the simulation window.",
            ),
            view,
        )

    if panel_name == "brokerage":
        rows: list[ListViewRow] = []
        for holding in dashboard.holdings:
            label = f"{holding.instrument_symbol} • {holding.instrument_name}"
            rows.append(
                ListViewRow(
                    key=holding.instrument_symbol,
                    values={
                        "instrument": label,
                        "quantity": holding.quantity,
                        "last_price": holding.last_price,
                        "market_value": holding.market_value,
                        "unrealized_pl": holding.unrealized_pl,
                    },
                    search_text=label.lower(),
                )
            )

        view = ListView(
            title="Brokerage holdings",
            columns=[
                ListViewColumn(key="instrument", title="Instrument"),
                ListViewColumn(key="quantity", title="Quantity", align="right"),
                ListViewColumn(key="last_price", title="Last price", column_type="currency", align="right"),
                ListViewColumn(key="market_value", title="Market value", column_type="currency", align="right"),
                ListViewColumn(key="unrealized_pl", title="Unrealised P/L", column_type="currency", align="right"),
            ],
            rows=rows,
            search_placeholder="Search holdings",
            empty_message="No active brokerage holdings.",
        )
        return (
            SimpleNamespace(chart_type="line", list_id="individual-brokerage-list"),
            _build_line_chart(
                "Brokerage value over time",
                dashboard.brokerage_value_trend,
                "Market value",
                hint="Month-end brokerage market value across the simulation window.",
            ),
            view,
        )

    if panel_name == "income":
        list_config, _, view = _category_panel(
            title="Income by category",
            breakdown=dashboard.income_breakdown,
            list_id="individual-income-list",
            search_placeholder="Search income",
            empty_message="No income posted in this window.",
            hint="Recent inflows grouped by category.",
        )
        labels = []
        values = []
        if dashboard.income_peer_split:
            labels = list(dashboard.income_peer_split.keys())
            values = list(dashboard.income_peer_split.values())
        if not labels or not values:
            labels = ["No data"]
            values = [0]
        percentile_hint = "Latest payroll snapshot."
        if dashboard.summary.income_percentile is not None:
            percentile_hint = (
                f"Latest payroll snapshot • {dashboard.summary.income_percentile:.1f}th percentile."
            )
        return (
            SimpleNamespace(chart_type="pie", list_id=list_config.list_id),
            PieChartData(
                title="Income comparison",
                labels=labels,
                values=values,
                hint=percentile_hint,
            ),
            view,
        )

    if panel_name == "expenses":
        return _category_panel(
            "Expenses by category",
            dashboard.expense_breakdown,
            list_id="individual-expense-list",
            search_placeholder="Search expenses",
            empty_message="No expenses posted in this window.",
            hint="Spending grouped by category for this period.",
        )

    raise HTTPException(status_code=404, detail="Unknown dashboard panel")


@router.get("/{user_id}", response_class=HTMLResponse, summary="Individual dashboard")
async def read_individual_dashboard(
    request: Request,
    user_id: int,
    user: AuthenticatedUser = Depends(require_individual_access),
    service: IndividualsService = Depends(get_individuals_service),
    session: Session = Depends(get_db_session),
) -> HTMLResponse:
    """Render the dashboard dedicated to a single individual user."""

    LOGGER.info("Individual dashboard requested", extra={"user_id": user_id})
    try:
        dashboard = service.get_dashboard(session, user_id)
    except ValueError as exc:  # convert domain error into HTTP 404
        raise HTTPException(status_code=404, detail=str(exc)) from exc

    panel_config, panel_chart, panel_view = _individual_panel_payload("accounts", dashboard)

    income_value = dashboard.summary.monthly_income or dashboard.summary.period_income
    income_value = income_value or Decimal("0")
    income_hint = "Latest monthly salary from payroll data."
    if dashboard.summary.income_percentile is not None:
        income_hint = (
            f"Latest monthly salary • {dashboard.summary.income_percentile:.1f}th percentile across individuals."
        )

    cards = [
        {
            "panel": "accounts",
            "label": "Accounts",
            "value": dashboard.summary.net_worth,
            "value_type": "currency",
            "decimals": 2,
            "hint": "Net worth across cash and brokerage holdings.",
        },
        {
            "panel": "brokerage",
            "label": "Brokerage",
            "value": dashboard.summary.holdings_value,
            "value_type": "currency",
            "decimals": 2,
            "hint": "Market value of open positions.",
        },
        {
            "panel": "income",
            "label": "Income",
            "value": income_value,
            "value_type": "currency",
            "decimals": 2,
            "hint": income_hint,
        },
        {
            "panel": "expenses",
            "label": "Expenses",
            "value": dashboard.summary.period_expenses,
            "value_type": "currency",
            "decimals": 2,
            "hint": "Money out during the selected window.",
        },
    ]

    subtitle_url = None
    if dashboard.employer_id:
        subtitle_url = request.url_for("read_company_dashboard", company_id=dashboard.employer_id)

    if dashboard.profile.job_title and dashboard.employer_name:
        subtitle = f"{dashboard.profile.job_title} at {dashboard.employer_name}"
    elif dashboard.profile.job_title:
        subtitle = dashboard.profile.job_title
    elif dashboard.employer_name:
        subtitle = f"Employed at {dashboard.employer_name}"
    else:
        subtitle = "Personal finances overview tailored to this account."

    return render_dashboard_template(
        request,
        view_key="individual",
        hero_title=dashboard.profile.name,
        hero_eyebrow="Individual dashboard",
        hero_subtitle=subtitle,
        hero_subtitle_url=subtitle_url,
        hero_badge=None,
        cards=cards,
        panel_name="accounts",
        panel_chart=panel_chart,
        panel_config=panel_config,
        panel_view=panel_view,
        panel_endpoint="render_individual_panel",
        panel_params={"user_id": user_id},
        show_admin_return=_user_is_admin(user),
        page_title=f"{dashboard.profile.name} • Individual dashboard",
    )


@router.get("/{user_id}/panel/{panel_name}", response_class=HTMLResponse, summary="Individual dashboard panel")
async def render_individual_panel(
    request: Request,
    user_id: int,
    panel_name: str,
    _: AuthenticatedUser = Depends(require_individual_access),
    service: IndividualsService = Depends(get_individuals_service),
    session: Session = Depends(get_db_session),
) -> HTMLResponse:
    """Return a single individual dashboard panel for HTMX swaps."""

    try:
        dashboard = service.get_dashboard(session, user_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc

    panel_config, panel_chart, panel_view = _individual_panel_payload(panel_name, dashboard)

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
            "list_htmx_config": None,
        },
    )
