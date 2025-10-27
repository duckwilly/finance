"""Routes serving dashboards for individual users."""
from __future__ import annotations

from collections.abc import Generator

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import HTMLResponse
from sqlalchemy.orm import Session, sessionmaker

from app.core.logger import get_logger
from app.core.security import AuthenticatedUser, require_individual_access
from app.core.templates import templates
from app.db.session import get_sessionmaker
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


@router.get("/{user_id}", response_class=HTMLResponse, summary="Individual dashboard")
async def read_individual_dashboard(
    request: Request,
    user_id: int,
    _: AuthenticatedUser = Depends(require_individual_access),
    service: IndividualsService = Depends(get_individuals_service),
    session: Session = Depends(get_db_session),
) -> HTMLResponse:
    """Render the dashboard dedicated to a single individual user."""

    LOGGER.info("Individual dashboard requested", extra={"user_id": user_id})
    try:
        dashboard = service.get_dashboard(session, user_id)
    except ValueError as exc:  # convert domain error into HTTP 404
        raise HTTPException(status_code=404, detail=str(exc)) from exc

    context = {"request": request, "dashboard": dashboard}
    return templates.TemplateResponse(
        request=request,
        name="individuals/dashboard.html",
        context=context,
    )
