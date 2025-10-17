"""Routes for administrative views."""
from __future__ import annotations

from pathlib import Path

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy.orm import Session

from app.services.admin_dashboard import AdminDashboardService
from app.services.company import CompanyService
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


@router.get("/companies/{org_id}", response_class=HTMLResponse)
async def admin_company_detail(
    org_id: int,
    request: Request,
    session: Session = Depends(get_db_session),
) -> HTMLResponse:
    """Render the detail page for a single organization."""

    service = CompanyService(session)
    detail = service.get_company(org_id)

    if detail is None:
        raise HTTPException(status_code=404, detail="Organization not found")

    return _templates.TemplateResponse(
        "admin/company_detail.html",
        {
            "request": request,
            "company": detail,
        },
    )
