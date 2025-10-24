"""FastAPI application instance and startup hooks."""
from __future__ import annotations

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.responses import RedirectResponse

from app.core import get_logger
from app.routers import corporate_router, dashboard_router, individuals_router

LOGGER = get_logger(__name__)


def create_app() -> FastAPI:
    """Create and configure the FastAPI application."""

    app = FastAPI(title="Finance Platform", version="0.1.0")
    app.mount("/static", StaticFiles(directory="app/static"), name="static")
    app.include_router(dashboard_router)
    app.include_router(individuals_router)
    app.include_router(corporate_router)

    @app.get("/", include_in_schema=False)
    async def root_redirect():
        return RedirectResponse(url="/dashboard/")

    LOGGER.info("FastAPI application initialised")
    return app


app = create_app()
