"""Routes for administrative views."""
from __future__ import annotations

from datetime import date
from pathlib import Path

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy.orm import Session

from app.services.admin_company import AdminCompanyService
from app.services.admin_dashboard import AdminDashboardService
from app.services.admin_individual import AdminIndividualService
from app.web.dependencies import get_db_session
from app.web.utils.pagination import (
    DEFAULT_PAGE_SIZE_OPTIONS,
    build_pagination_links,
    normalize_page_size,
    parse_iso_date,
    parse_positive_int,
)

router = APIRouter(prefix="/admin", tags=["admin"])

_templates = Jinja2Templates(
    directory=str(Path(__file__).resolve().parent.parent / "templates")
)

_PAGE_SIZE_OPTIONS: tuple[int, ...] = DEFAULT_PAGE_SIZE_OPTIONS


@router.get("/", include_in_schema=False)
async def admin_root() -> RedirectResponse:
    """Redirect the admin landing page to the dashboard view."""

    return RedirectResponse(url="/admin/dashboard")


@router.get("/dashboard", response_class=HTMLResponse)
async def admin_dashboard(
    request: Request,
    session: Session = Depends(get_db_session),
) -> HTMLResponse:
    """Render the admin dashboard page."""

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

    page = parse_positive_int(query_params.get("page"), default=1)
    page_size = normalize_page_size(query_params.get("page_size"))
    search = query_params.get("search")
    search_value = search if search else None

    service = AdminCompanyService(session)
    company_page = service.list_companies(
        page=page, page_size=page_size, search=search_value
    )

    total_pages = company_page.total_pages
    pagination_links = build_pagination_links(
        query_params, page=company_page.page, total_pages=total_pages
    )

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
            "has_previous": pagination_links.has_previous,
            "has_next": pagination_links.has_next,
            "prev_url": pagination_links.prev_url,
            "next_url": pagination_links.next_url,
        },
    )


@router.get("/companies/{org_id}", response_class=HTMLResponse)
async def admin_company_detail(
    org_id: int,
    request: Request,
    session: Session = Depends(get_db_session),
) -> HTMLResponse:
    """Render the detail page for a single company."""

    service = AdminCompanyService(session)
    period_param = request.query_params.get("period")
    start_param = request.query_params.get("start_date")
    end_param = request.query_params.get("end_date")
    start_date = parse_iso_date(start_param)
    end_date = parse_iso_date(end_param)
    today = date.today()
    detail = service.get_company_detail(
        org_id,
        period_key=period_param,
        start_date=start_date,
        end_date=end_date,
        today=today,
    )
    if detail is None:
        raise HTTPException(status_code=404, detail="Company not found")

    return _templates.TemplateResponse(
        "admin/company_detail.html",
        {
            "request": request,
            "company": detail,
        },
    )


@router.get("/individuals", response_class=HTMLResponse)
async def admin_individuals(
    request: Request,
    session: Session = Depends(get_db_session),
) -> HTMLResponse:
    """Render the paginated individuals listing view."""

    query_params = request.query_params

    page = parse_positive_int(query_params.get("page"), default=1)
    page_size = normalize_page_size(query_params.get("page_size"))
    search = query_params.get("search")
    search_value = search if search else None

    service = AdminIndividualService(session)
    individual_page = service.list_individuals(
        page=page, page_size=page_size, search=search_value
    )

    total_pages = individual_page.total_pages
    pagination_links = build_pagination_links(
        query_params, page=individual_page.page, total_pages=total_pages
    )

    return _templates.TemplateResponse(
        "admin/individuals.html",
        {
            "request": request,
            "individuals": individual_page.items,
            "page": individual_page.page,
            "page_size": individual_page.page_size,
            "total": individual_page.total,
            "total_pages": total_pages,
            "page_size_options": _PAGE_SIZE_OPTIONS,
            "search": search_value or "",
            "has_previous": pagination_links.has_previous,
            "has_next": pagination_links.has_next,
            "prev_url": pagination_links.prev_url,
            "next_url": pagination_links.next_url,
        },
    )


@router.get("/individuals/{user_id}", response_class=HTMLResponse)
async def admin_individual_detail(
    user_id: int,
    request: Request,
    session: Session = Depends(get_db_session),
) -> HTMLResponse:
    """Render the detail page for a single individual."""

    service = AdminIndividualService(session)
    period_param = request.query_params.get("period")
    start_param = request.query_params.get("start_date")
    end_param = request.query_params.get("end_date")
    start_date = parse_iso_date(start_param)
    end_date = parse_iso_date(end_param)
    today = date.today()
    detail = service.get_individual_detail(
        user_id,
        period_key=period_param,
        start_date=start_date,
        end_date=end_date,
        today=today,
    )
    if detail is None:
        raise HTTPException(status_code=404, detail="Individual not found")

    return _templates.TemplateResponse(
        "admin/individual_detail.html",
        {
            "request": request,
            "individual": detail,
            "period_options": AdminIndividualService.period_options(today=today),
        },
    )
