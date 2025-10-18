"""FastAPI application factory for the finance web UI."""
from __future__ import annotations

from fastapi import FastAPI
from fastapi.responses import RedirectResponse

from .routers import (
    admin as admin_router,
    admin_companies,
    admin_individuals,
)


def create_app() -> FastAPI:
    """Instantiate and configure the FastAPI application."""

    application = FastAPI(title="Finance Admin Portal")
    application.include_router(admin_router.router)
    application.include_router(admin_companies.router)
    application.include_router(admin_individuals.router)

    @application.get("/", include_in_schema=False)
    async def root() -> RedirectResponse:  # pragma: no cover - simple redirect
        return RedirectResponse(url="/admin", status_code=302)

    return application


app = create_app()
