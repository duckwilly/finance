"""FastAPI application instance and startup hooks."""
from __future__ import annotations

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.responses import RedirectResponse

from app.core import get_logger
from app.core.security import get_security_provider
from app.middleware.auth import AuthMiddleware
from app.routers import auth_router, corporate_router, dashboard_router, individuals_router
from app.db.session import get_sessionmaker
from app.services import AdminService

LOGGER = get_logger(__name__)


def create_app() -> FastAPI:
    """Create and configure the FastAPI application."""

    app = FastAPI(title="Finance Platform", version="0.1.0")
    app.mount("/static", StaticFiles(directory="app/static"), name="static")
    security_provider = get_security_provider()
    if security_provider.is_enabled:
        app.add_middleware(
            AuthMiddleware,
            security_provider=security_provider,
        )
    app.include_router(auth_router)
    app.include_router(dashboard_router)
    app.include_router(individuals_router)
    app.include_router(corporate_router)

    session_factory = get_sessionmaker()

    @app.on_event("startup")
    def warm_admin_datasets() -> None:
        LOGGER.info("Precomputing admin dashboard datasets on startup")
        session = session_factory()
        admin_service = AdminService()
        try:
            AdminService.refresh_metrics(session)
            admin_service.get_individual_overview(session)
            admin_service.get_company_overview(session)
            admin_service.get_stock_holdings_overview(session)
            admin_service.get_transaction_overview(session)
            admin_service.get_dashboard_charts(session)
        except Exception:  # pragma: no cover - fail fast on startup issues
            LOGGER.exception("Failed to precompute admin dashboard data")
            raise
        finally:
            session.close()

    @app.get("/", include_in_schema=False)
    async def root_redirect():
        return RedirectResponse(url="/dashboard/")

    LOGGER.info("FastAPI application initialised")
    return app


app = create_app()
