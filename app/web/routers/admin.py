"""Routes for administrative views."""
from __future__ import annotations

from pathlib import Path

from fastapi import APIRouter, Depends, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy.orm import Session

from app.services.admin_dashboard import AdminDashboardService
from app.web.dependencies import get_db_session

router = APIRouter(prefix="/admin", tags=["admin"])

_templates = Jinja2Templates(
    directory=str(Path(__file__).resolve().parent.parent / "templates")
)


@router.get("/", response_class=HTMLResponse)
async def admin_dashboard(
    request: Request,
    session: Session = Depends(get_db_session),
) -> HTMLResponse:
    """Render the admin landing page."""

    dashboard_service = AdminDashboardService(session)
    dashboard_data = dashboard_service.get_dashboard_data()

    return _templates.TemplateResponse(
        "admin/dashboard.html",
        {
            "request": request,
            "dashboard": dashboard_data,
        },
    )
