"""Dashboard routes for server-rendered admin pages."""
from __future__ import annotations

from collections.abc import Generator
from dataclasses import dataclass

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import HTMLResponse
from sqlalchemy.orm import Session, sessionmaker

from app.core.logger import get_logger
from app.core.security import AuthenticatedUser, require_admin_user
from app.core.templates import templates
from app.db.session import get_sessionmaker
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


FALLBACK_CHARTS = DashboardCharts(
    individuals_income=PieChartData(
        title="Income mix",
        labels=["Demo"],
        values=[1.0],
        hint="Demo data",
    ),
    companies_profit_margin=PieChartData(
        title="Profit margin",
        labels=["Demo"],
        values=[1.0],
        hint="Demo data",
    ),
    transactions_amounts=PieChartData(
        title="Transactions",
        labels=["Demo"],
        values=[1.0],
        hint="Demo data",
    ),
    stock_price_trend=LineChartData(
        title="Stock price trend",
        labels=["T0", "T1"],
        values=[100.0, 100.0],
        series_label="Demo",
        hint="Demo data",
    ),
)


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
    if hasattr(admin_service, "get_dashboard_charts"):
        charts = admin_service.get_dashboard_charts(session)
    else:  # pragma: no cover - simplified stubs in tests
        charts = FALLBACK_CHARTS
    return getattr(charts, chart_attr, getattr(FALLBACK_CHARTS, chart_attr))


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



def _first_link(view: ListView | None) -> str | None:
    if view and view.rows:
        for row in view.rows:
            if row.links:
                for url in row.links.values():
                    if url:
                        return url
    return None



def _collect_dashboard_links(
    request: Request,
    admin_service: AdminService,
    session: Session,
    initial_panel: str,
    initial_view: ListView,
) -> list[dict[str, str]]:
    links = [
        {"label": "Admin dashboard", "href": request.url_for("read_dashboard"), "active": True}
    ]

    views: dict[str, ListView] = {initial_panel: initial_view}

    if "individuals" not in views:
        views["individuals"] = admin_service.get_individual_overview(session)
    if "companies" not in views:
        views["companies"] = admin_service.get_company_overview(session)

    label_map = {
        "individuals": "Individuals dashboard",
        "companies": "Companies dashboard",
    }

    for key, label in label_map.items():
        view = views.get(key)
        url = _first_link(view)
        if url:
            links.append({"label": label, "href": url, "active": False})

    return links



def _render_panel(
    request: Request,
    panel_name: str,
    admin_service: AdminService,
    session: Session,
) -> HTMLResponse:
    config, chart, view = _panel_payload(panel_name, admin_service, session)

    return templates.TemplateResponse(
        request=request,
        name="admin/partials/panel.html",
        context={
            "chart": chart,
            "chart_type": config.chart_type,
            "list_id": config.list_id,
            "panel_key": panel_name,
            "request": request,
            "view": view,
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

    dashboard_links = _collect_dashboard_links(
        request=request,
        admin_service=admin_service,
        session=session,
        initial_panel="individuals",
        initial_view=panel_view,
    )

    context = {
        "request": request,
        "metrics": metrics,
        "dashboard_links": dashboard_links,
        "panel_chart": panel_chart,
        "panel_config": panel_config,
        "panel_name": "individuals",
        "panel_view": panel_view,
    }
    return templates.TemplateResponse(
        request=request,
        name="admin/dashboard.html",
        context=context,
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
    )
