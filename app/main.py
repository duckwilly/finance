"""FastAPI application instance and startup hooks."""
from __future__ import annotations

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

from app.core import get_logger
from app.routers import dashboard_router

LOGGER = get_logger(__name__)


def create_app() -> FastAPI:
    """Create and configure the FastAPI application."""

    app = FastAPI(title="Finance Platform", version="0.1.0")
    app.mount("/static", StaticFiles(directory="app/static"), name="static")
    app.include_router(dashboard_router)
    LOGGER.info("FastAPI application initialised")
    return app


app = create_app()
