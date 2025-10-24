"""Routes serving dashboards for corporate users."""
from __future__ import annotations

from collections.abc import Generator

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import HTMLResponse
from sqlalchemy.orm import Session, sessionmaker

from app.core.logger import get_logger
from app.core.templates import templates
from app.db.session import get_sessionmaker
from app.services import CompaniesService

router = APIRouter(prefix="/corporate", tags=["corporate"])
SESSION_FACTORY: sessionmaker = get_sessionmaker()
LOGGER = get_logger(__name__)


def get_db_session() -> Generator[Session, None, None]:
    """Yield a database session for the request lifecycle."""

    session = SESSION_FACTORY()
    try:
        yield session
    finally:
        session.close()


def get_companies_service() -> CompaniesService:
    """Return a service instance per request."""

    return CompaniesService()


@router.get("/{company_id}", response_class=HTMLResponse, summary="Company dashboard")
async def read_company_dashboard(
    request: Request,
    company_id: int,
    service: CompaniesService = Depends(get_companies_service),
    session: Session = Depends(get_db_session),
) -> HTMLResponse:
    """Render the dashboard dedicated to a specific company."""

    LOGGER.info("Company dashboard requested", extra={"company_id": company_id})
    try:
        dashboard = service.get_dashboard(session, company_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc

    context = {"request": request, "dashboard": dashboard}
    return templates.TemplateResponse(
        request=request,
        name="corporate/dashboard.html",
        context=context,
    )
