from __future__ import annotations

from datetime import date
from pathlib import Path

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy.orm import Session

from app.services.admin import GetIndividualDetail, ListIndividuals
from app.web.dependencies import get_db_session
from app.web.utils.pagination import (
    DEFAULT_PAGE_SIZE_OPTIONS,
    build_pagination_links,
)
from app.web.utils.query_params import (
    extract_pagination,
    extract_search_term,
    parse_iso_date,
)

router = APIRouter(prefix="/admin/individuals", tags=["admin", "individuals"])

templates = Jinja2Templates(
    directory=str(Path(__file__).resolve().parent.parent / "templates")
)


@router.get("", response_class=HTMLResponse)
async def admin_individuals(
    request: Request,
    session: Session = Depends(get_db_session),
) -> HTMLResponse:
    pagination = extract_pagination(
        request.query_params,
        page_size_options=DEFAULT_PAGE_SIZE_OPTIONS,
    )
    search = extract_search_term(request.query_params)

    individual_page = ListIndividuals(session).execute(
        page=pagination.page, page_size=pagination.page_size, search=search
    )

    pagination_links = build_pagination_links(
        request.query_params,
        page=individual_page.page,
        total_pages=individual_page.total_pages,
    )

    return templates.TemplateResponse(
        "admin/individuals.html",
        {
            "request": request,
            "individuals": individual_page.items,
            "page": individual_page.page,
            "page_size": individual_page.page_size,
            "total": individual_page.total,
            "total_pages": individual_page.total_pages,
            "page_size_options": DEFAULT_PAGE_SIZE_OPTIONS,
            "search": search or "",
            "has_previous": pagination_links.has_previous,
            "has_next": pagination_links.has_next,
            "prev_url": pagination_links.prev_url,
            "next_url": pagination_links.next_url,
        },
    )


@router.get("/{user_id}", response_class=HTMLResponse)
async def admin_individual_detail(
    user_id: int,
    request: Request,
    session: Session = Depends(get_db_session),
) -> HTMLResponse:
    period_param = request.query_params.get("period")
    start_date = parse_iso_date(request.query_params.get("start_date"))
    end_date = parse_iso_date(request.query_params.get("end_date"))
    today = date.today()

    detail = GetIndividualDetail(session).execute(
        user_id,
        period_key=period_param,
        start_date=start_date,
        end_date=end_date,
        today=today,
    )
    if detail is None:
        raise HTTPException(status_code=404, detail="Individual not found")

    return templates.TemplateResponse(
        "admin/individual_detail.html",
        {
            "request": request,
            "individual": detail,
            "period_options": GetIndividualDetail.period_options(today=today),
        },
    )
