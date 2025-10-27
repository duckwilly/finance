"""Dashboard routes for server-rendered admin pages."""
from __future__ import annotations

from collections.abc import Generator

from fastapi import APIRouter, Depends, Request
from fastapi.responses import HTMLResponse, JSONResponse
from sqlalchemy.orm import Session, sessionmaker

from app.core.logger import get_logger
from app.core.security import AuthenticatedUser, require_admin_user
from app.db.session import get_sessionmaker
from app.services import AdminService
from app.schemas.admin import DashboardCharts, LineChartData, PieChartData
from app.core.templates import templates

router = APIRouter(prefix="/dashboard", tags=["dashboard"])
SESSION_FACTORY: sessionmaker = get_sessionmaker()
LOGGER = get_logger(__name__)


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


@router.get(
    "/",
    summary="Finance dashboard",
    response_class=HTMLResponse,
)
async def read_dashboard(
    request: Request,
    _: AuthenticatedUser = Depends(require_admin_user),
    admin_service: AdminService = Depends(get_admin_service),
    session: Session = Depends(get_db_session),
) -> HTMLResponse:
    """Render the administrative dashboard populated with metrics."""

    LOGGER.info("Dashboard endpoint requested")
    metrics = admin_service.get_metrics(session)
    individuals_list = admin_service.get_individual_overview(session)
    companies_list = admin_service.get_company_overview(session)
    if hasattr(admin_service, "get_stock_holdings_overview"):
        stock_holdings_list = admin_service.get_stock_holdings_overview(session)
    else:  # pragma: no cover - fallback for simplified stubs used in tests
        stock_holdings_list = companies_list
    transactions_list = admin_service.get_transaction_overview(session)
    if hasattr(admin_service, "get_dashboard_charts"):
        charts = admin_service.get_dashboard_charts(session)
    else:  # pragma: no cover - fallback for simplified stubs used in tests
        charts = DashboardCharts(
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
    context = {
        "request": request,
        "metrics": metrics,
        "individuals_list": individuals_list,
        "companies_list": companies_list,
        "stock_holdings_list": stock_holdings_list,
        "transactions_list": transactions_list,
        "charts": charts,
    }
    return templates.TemplateResponse(
        request=request,
        name="admin/dashboard.html",
        context=context,
    )


@router.get(
    "/api/stocks",
    summary="Get available stocks",
)
async def get_available_stocks(
    admin_service: AdminService = Depends(get_admin_service),
    session: Session = Depends(get_db_session),
) -> JSONResponse:
    """Return a list of all available stocks with price data."""
    
    stocks = admin_service.get_available_stocks(session)
    return JSONResponse(content={"stocks": stocks})


@router.get(
    "/api/stocks/{instrument_id}/prices",
    summary="Get stock price data",
)
async def get_stock_prices(
    instrument_id: int,
    admin_service: AdminService = Depends(get_admin_service),
    session: Session = Depends(get_db_session),
) -> JSONResponse:
    """Return price series for a specific stock."""
    
    series, label, hint = admin_service.get_stock_price_data(session, instrument_id)
    
    labels = [point[0] for point in series]
    values = [point[1] for point in series]
    
    return JSONResponse(content={
        "title": f"{label} price trend",
        "labels": labels,
        "values": values,
        "series_label": "Closing price",
        "hint": hint,
    })
