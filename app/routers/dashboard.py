"""Dashboard routes for server-rendered admin pages."""
from __future__ import annotations

from collections.abc import Generator

from fastapi import APIRouter, Depends, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy.orm import Session, sessionmaker

from app.core.logger import get_logger
from app.db.session import get_sessionmaker
from app.services import AdminService

router = APIRouter(prefix="/dashboard", tags=["dashboard"])
templates = Jinja2Templates(directory="app/templates")
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
    admin_service: AdminService = Depends(get_admin_service),
    session: Session = Depends(get_db_session),
) -> HTMLResponse:
    """Render the administrative dashboard populated with metrics."""

    LOGGER.info("Dashboard endpoint requested")
    metrics = admin_service.get_metrics(session)
    context = {
        "metrics": metrics,
    }
    return templates.TemplateResponse(request, "admin/dashboard.html", context)
