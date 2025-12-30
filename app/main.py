"""FastAPI application instance and startup hooks."""
from __future__ import annotations

from fastapi import FastAPI, Request
from fastapi.staticfiles import StaticFiles
from fastapi.responses import RedirectResponse
from fastapi.templating import Jinja2Templates

from app.core import get_logger
from app.core.paths import with_root_path
from app.core.security import get_security_provider
from app.middleware.auth import AuthMiddleware
from app.routers import (
    auth_router,
    corporate_router,
    dashboard_router,
    individuals_router,
    presentation_router,
)
from app.routers.auth import default_destination
from app.db.session import get_sessionmaker
from app.services import AdminService

# Import chatbot integration
from app.ai_chatbot import (
    router as chatbot_router,
    configure_dependencies as configure_chatbot_dependencies,
    configure_templates as configure_chatbot_templates,
)
from app.chatbot_integration import (
    get_db_session,
    get_current_user_context,
    get_database_schema,
)

LOGGER = get_logger(__name__)


def create_app() -> FastAPI:
    """Create and configure the FastAPI application."""

    app = FastAPI(title="Finance Platform", version="0.1.0")
    app.mount("/static", StaticFiles(directory="app/static"), name="static")
    security_provider = get_security_provider()
    app.add_middleware(
        AuthMiddleware,
        security_provider=security_provider,
    )
    app.include_router(auth_router)
    app.include_router(dashboard_router)
    app.include_router(individuals_router)
    app.include_router(corporate_router)
    app.include_router(presentation_router)

    # Configure AI Chatbot
    templates = Jinja2Templates(directory="app/templates")
    configure_chatbot_templates(templates)
    configure_chatbot_dependencies(
        get_db=get_db_session,
        get_user=get_current_user_context,
        database_schema=get_database_schema(),
    )
    app.include_router(chatbot_router)
    LOGGER.info("AI Chatbot integrated successfully")

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
    async def root_redirect(request: Request):
        user = getattr(request.state, "user", None)
        destination = "/dashboard/"
        if user is not None:
            destination = default_destination(user)
        return RedirectResponse(url=with_root_path(request, destination))

    LOGGER.info("FastAPI application initialised")
    return app


app = create_app()
