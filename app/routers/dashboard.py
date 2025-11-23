"""Dashboard routes for server-rendered admin pages."""
from __future__ import annotations

from collections.abc import Generator
from dataclasses import dataclass

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import HTMLResponse
from sqlalchemy.orm import Session, sessionmaker

from app.core.formatting import humanize_currency
from app.core.logger import get_logger
from app.core.security import AuthenticatedUser, require_admin_user
from app.core.templates import templates
from app.db.session import get_sessionmaker
from app.routers.dashboard_views import HTMX_SHELL_CONFIG, render_dashboard_template
from app.schemas.admin import DashboardCharts, LineChartData, ListView, PieChartData
from app.services import AdminService

router = APIRouter(prefix="/dashboard", tags=["dashboard"])
SESSION_FACTORY: sessionmaker = get_sessionmaker()
LOGGER = get_logger(__name__)


@dataclass(frozen=True)
class PanelConfig:
    list_methods: tuple[str, ...]
    chart_attr: str
    chart_type: str
    list_id: str


PANEL_CONFIGS = {
    "individuals": PanelConfig(
        list_methods=("get_individual_overview",),
        chart_attr="individuals_income",
        chart_type="pie",
        list_id="individuals-list",
    ),
    "companies": PanelConfig(
        list_methods=("get_company_overview",),
        chart_attr="companies_profit_margin",
        chart_type="pie",
        list_id="companies-list",
    ),
    "transactions": PanelConfig(
        list_methods=("get_transaction_overview",),
        chart_attr="transactions_amounts",
        chart_type="pie",
        list_id="transactions-list",
    ),
    "stocks": PanelConfig(
        list_methods=("get_stock_holdings_overview", "get_company_overview"),
        chart_attr="stock_price_trend",
        chart_type="line",
        list_id="stocks-list",
    ),
}


def get_db_session() -> Generator[Session, None, None]:
    """Yield a database session and ensure it is closed after use."""

    session = SESSION_FACTORY()
    try:
        yield session
    finally:
        session.close()


def get_admin_service() -> AdminService:
    """Return a new ``AdminService`` instance for the request lifecycle."""

    return AdminService()


def _resolve_chart(
    admin_service: AdminService,
    session: Session,
    chart_attr: str,
) -> PieChartData | LineChartData:
    if not hasattr(admin_service, "get_dashboard_charts"):
        LOGGER.error("AdminService is missing get_dashboard_charts implementation")
        raise HTTPException(status_code=500, detail="Dashboard charts unavailable")

    charts: DashboardCharts = admin_service.get_dashboard_charts(session)

    if not hasattr(charts, chart_attr):
        LOGGER.error("DashboardCharts missing attribute '%s'", chart_attr)
        raise HTTPException(status_code=500, detail="Dashboard chart missing")

    return getattr(charts, chart_attr)


def _panel_payload(
    panel_name: str,
    admin_service: AdminService,
    session: Session,
) -> tuple[PanelConfig, PieChartData | LineChartData, ListView]:
    config = PANEL_CONFIGS.get(panel_name)
    if config is None:
        raise HTTPException(status_code=404, detail="Unknown dashboard panel")

    list_getter = None
    for method in config.list_methods:
        list_getter = getattr(admin_service, method, None)
        if list_getter is not None:
            break
    if list_getter is None:
        raise HTTPException(status_code=404, detail="Panel data unavailable")

    view = list_getter(session)
    chart = _resolve_chart(admin_service, session, config.chart_attr)

    return config, chart, view



def _render_panel(
    request: Request,
    panel_name: str,
    admin_service: AdminService,
    session: Session,
    list_htmx_config: dict[str, object] | None = None,
) -> HTMLResponse:
    config, chart, view = _panel_payload(panel_name, admin_service, session)

    return templates.TemplateResponse(
        request=request,
        name="dashboard/partials/panel.html",
        context={
            "chart": chart,
            "chart_type": config.chart_type,
            "list_id": config.list_id,
            "panel_key": panel_name,
            "request": request,
            "view": view,
            "list_htmx_config": list_htmx_config,
        },
    )


@router.get("/", summary="Finance dashboard", response_class=HTMLResponse)
async def read_dashboard(
    request: Request,
    _: AuthenticatedUser = Depends(require_admin_user),
    admin_service: AdminService = Depends(get_admin_service),
    session: Session = Depends(get_db_session),
) -> HTMLResponse:
    """Render the administrative dashboard populated with metrics."""

    LOGGER.info("Dashboard endpoint requested")
    metrics = admin_service.get_metrics(session)
    panel_config, panel_chart, panel_view = _panel_payload(
        panel_name="individuals",
        admin_service=admin_service,
        session=session,
    )
    cards = [
        {
            "panel": "individuals",
            "label": "Individuals",
            "value": metrics.total_individuals,
            "value_type": "number",
            "decimals": 0,
            "hint": "Individual users.",
        },
        {
            "panel": "companies",
            "label": "Companies",
            "value": metrics.total_companies,
            "value_type": "number",
            "decimals": 0,
            "hint": "Companies simulated",
        },
        {
            "panel": "transactions",
            "label": "Transactions",
            "value": metrics.total_transactions,
            "value_type": "number",
            "decimals": 0,
            "hint": "Simulated transactions",
        },
        {
            "panel": "stocks",
            "label": "Assets under management",
            "value": metrics.total_aum,
            "value_type": "currency",
            "short": True,
            "decimals": 1,
            "hint": "{} in cash • {} invested".format(
                humanize_currency(metrics.total_cash, short=True, decimals=1),
                humanize_currency(metrics.total_holdings, short=True, decimals=1),
            ),
        },
    ]

    first_date = metrics.first_transaction_at.date() if metrics.first_transaction_at else "—"
    last_date = metrics.last_transaction_at.date() if metrics.last_transaction_at else "—"

    return render_dashboard_template(
        request,
        view_key="admin",
        hero_title="Admin dashboard",
        hero_eyebrow="Finance platform",
        hero_badge=f"Simulated data • {first_date} to {last_date}",
        cards=cards,
        panel_name="individuals",
        panel_chart=panel_chart,
        panel_config=panel_config,
        panel_view=panel_view,
        panel_endpoint="render_panel",
        list_htmx_config=HTMX_SHELL_CONFIG,
        show_admin_return=False,
        page_title="Admin dashboard • Finance",
    )


@router.get(
    "/panel/{panel_name}",
    summary="Dashboard detail panel",
    response_class=HTMLResponse,
)
async def render_panel(
    request: Request,
    panel_name: str,
    _: AuthenticatedUser = Depends(require_admin_user),
    admin_service: AdminService = Depends(get_admin_service),
    session: Session = Depends(get_db_session),
) -> HTMLResponse:
    """Return a panel partial for HTMX swaps."""

    return _render_panel(
        request=request,
        panel_name=panel_name,
        admin_service=admin_service,
        session=session,
        list_htmx_config=HTMX_SHELL_CONFIG,
    )
