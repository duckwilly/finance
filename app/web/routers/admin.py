"""Routes for administrative views."""
from __future__ import annotations

from pathlib import Path
from urllib.parse import urlencode

from fastapi import APIRouter, Depends, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy.orm import Session

from app.services.admin_company import AdminCompanyService
from app.services.admin_dashboard import AdminDashboardService
from app.web.dependencies import get_db_session

router = APIRouter(prefix="/admin", tags=["admin"])

_templates = Jinja2Templates(
    directory=str(Path(__file__).resolve().parent.parent / "templates")
)

_PAGE_SIZE_OPTIONS: tuple[int, ...] = (10, 20, 50)


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


@router.get("/companies", response_class=HTMLResponse)
async def admin_companies(
    request: Request,
    session: Session = Depends(get_db_session),
) -> HTMLResponse:
    """Render the paginated company listing view."""

    query_params = request.query_params

    page = _parse_positive_int(query_params.get("page"), default=1)
    page_size = _parse_page_size(query_params.get("page_size"))
    search = query_params.get("search")
    search_value = search if search else None

    service = AdminCompanyService(session)
    company_page = service.list_companies(
        page=page, page_size=page_size, search=search_value
    )

    total_pages = company_page.total_pages

    preserved_params = {
        key: value
        for key, value in query_params.multi_items()
        if key != "page"
    }

    prev_url = None
    if company_page.page > 1:
        prev_params = dict(preserved_params)
        prev_params["page"] = str(company_page.page - 1)
        prev_url = f"?{urlencode(prev_params)}"

    next_url = None
    if company_page.page < total_pages:
        next_params = dict(preserved_params)
        next_params["page"] = str(company_page.page + 1)
        next_url = f"?{urlencode(next_params)}"

    return _templates.TemplateResponse(
        "admin/companies.html",
        {
            "request": request,
            "companies": company_page.items,
            "page": company_page.page,
            "page_size": company_page.page_size,
            "total": company_page.total,
            "total_pages": total_pages,
            "page_size_options": _PAGE_SIZE_OPTIONS,
            "search": search_value or "",
            "has_previous": company_page.page > 1,
            "has_next": company_page.page < total_pages,
            "prev_url": prev_url,
            "next_url": next_url,
        },
    )


def _parse_positive_int(value: str | None, *, default: int) -> int:
    try:
        parsed = int(value) if value is not None else default
    except (TypeError, ValueError):
        return default
    return parsed if parsed > 0 else default


def _parse_page_size(value: str | None) -> int:
    try:
        parsed = int(value) if value is not None else _PAGE_SIZE_OPTIONS[1]
    except (TypeError, ValueError):
        return _PAGE_SIZE_OPTIONS[1]
    for option in _PAGE_SIZE_OPTIONS:
        if parsed <= option:
            return option
    return _PAGE_SIZE_OPTIONS[-1]
