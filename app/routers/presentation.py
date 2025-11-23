"""Presentation routes for the interactive slide deck."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import HTMLResponse
from sqlalchemy import select
from sqlalchemy.orm import sessionmaker

from app.core.logger import get_logger
from app.core.security import AuthenticatedUser, require_admin_user
from app.core.templates import templates
from app.db.session import get_sessionmaker
from app.models import AppUser, OrgPartyMap, UserPartyMap
from app.services import IndividualsService

LOGGER = get_logger(__name__)
router = APIRouter(prefix="/presentation", tags=["presentation"])
SESSION_FACTORY: sessionmaker = get_sessionmaker()
INDIVIDUALS_SERVICE = IndividualsService()


@dataclass(frozen=True)
class Slide:
    slug: str
    title: str
    template: str
    summary: str
    order: str | None = None


SLIDES: tuple[Slide, ...] = (
    Slide(
        slug="overview",
        title="Project overview",
        template="presentation/slides/overview.html",
        summary="High-level goals and the audiences we support.",
        order="1",
    ),
    Slide(
        slug="simulated-data",
        title="Simulated bank data",
        template="presentation/slides/simulated_data.html",
        summary="Individuals and companies with transactions, holdings, and user 3 as a real example.",
        order="1.1",
    ),
    Slide(
        slug="admin-browse",
        title="Admin browse and search",
        template="presentation/slides/stub.html",
        summary="Dashboard entry points to search individuals and companies at a glance.",
        order="1.2",
    ),
    Slide(
        slug="individual-access",
        title="Individual and company access",
        template="presentation/slides/stub.html",
        summary="People can log in to view their own data and any linked company records.",
        order="1.3",
    ),
    Slide(
        slug="architecture",
        title="Architecture & tech stack",
        template="presentation/slides/architecture.html",
        summary="How the FastAPI, database, and front-end layers fit together.",
        order="2",
    ),
    Slide(
        slug="frontend-stack",
        title="Frontend stack",
        template="presentation/slides/stub.html",
        summary="HTML, CSS, JavaScript, HTMX, and Chart.js in the browser.",
        order="2.1",
    ),
    Slide(
        slug="backend-stack",
        title="Backend stack",
        template="presentation/slides/stub.html",
        summary="Python services with SQLAlchemy, FastAPI, Jinja2, Pydantic, and PyJWT.",
        order="2.2",
    ),
    Slide(
        slug="database-stack",
        title="Database platform",
        template="presentation/slides/stub.html",
        summary="MariaDB on Docker backing the simulations.",
        order="2.3",
    ),
    Slide(
        slug="database",
        title="Database-ontwerp",
        template="presentation/slides/database.html",
        summary="Aard van financiële data, 3NF/BCNF-normalisatie en ons dubbelboekings-journal.",
        order="3",
    ),
    Slide(
        slug="features",
        title="Demo",
        template="presentation/slides/features.html",
        summary="Kort overzicht van de onderdelen die we live laten zien.",
        order="4",
    ),
    Slide(
        slug="auth",
        title="Authenticatie en autorisatie",
        template="presentation/slides/auth.html",
        summary="Aanmeldflow, JWT-cookies en toegang per rol.",
        order="4.1",
    ),
    Slide(
        slug="admin-dashboard",
        title="Admin dashboard",
        template="presentation/slides/admin_dashboard.html",
        summary="Navigatie voor zoekbare lijsten van personen, bedrijven en transacties.",
        order="4.2",
    ),
    Slide(
        slug="company-dashboard",
        title="Company dashboard",
        template="presentation/slides/company_dashboard.html",
        summary="Bedrijfswaarden, cashflow en payroll in één scherm.",
        order="4.3",
    ),
    Slide(
        slug="individual-dashboard",
        title="Individual dashboard",
        template="presentation/slides/individual_dashboard.html",
        summary="Persoonlijk vermogen, holdings, inkomsten en uitgaven.",
        order="4.4",
    ),
    Slide(
        slug="show-log",
        title="Show log",
        template="presentation/slides/stub.html",
        summary="Logging views for demos and audits.",
        order="4.6",
    ),
    Slide(
        slug="ai-chatbot",
        title="AI chatbot capabilities",
        template="presentation/slides/ai_chatbot.html",
        summary="Prompt assembly, visual responses, and database-aware tooling.",
        order="5",
    ),
    Slide(
        slug="prompt-assembly",
        title="Prompt assembly",
        template="presentation/slides/stub.html",
        summary="How prompts are constructed before model calls.",
        order="5.1",
    ),
    Slide(
        slug="json-response",
        title="JSON responses",
        template="presentation/slides/stub.html",
        summary="Structured outputs returned by the chatbot.",
        order="5.2",
    ),
    Slide(
        slug="db-querying",
        title="Python database querying",
        template="presentation/slides/stub.html",
        summary="Using Python to read and aggregate data for answers.",
        order="5.3",
    ),
    Slide(
        slug="dashboard-charts",
        title="Dashboard charts via httpx",
        template="presentation/slides/stub.html",
        summary="Generating dashboard-friendly charts over HTTP.",
        order="5.4",
    ),
    Slide(
        slug="operational-notes",
        title="Operations & logging",
        template="presentation/slides/operations.html",
        summary="Telemetry, startup warmups, and how we monitor the platform.",
        order="6",
    ),
)


def _get_slide(slug: str) -> Slide:
    for slide in SLIDES:
        if slide.slug == slug:
            return slide
    raise HTTPException(status_code=404, detail="Slide not found")


def _serialize_slides(slides: Iterable[Slide]) -> list[dict[str, str]]:
    serialized: list[dict[str, str]] = []
    for index, slide in enumerate(slides, start=1):
        serialized.append(
            {
                "slug": slide.slug,
                "title": slide.title,
                "summary": slide.summary,
                "order": slide.order or str(index),
            }
        )
    return serialized


def _load_individual_dashboard(user_id: int):
    session = SESSION_FACTORY()
    try:
        return INDIVIDUALS_SERVICE.get_dashboard(session, user_id)
    except Exception:
        LOGGER.exception("Failed to load individual dashboard for slide", extra={"user_id": user_id})
        return None
    finally:
        session.close()


def _load_demo_targets() -> dict[str, int | None]:
    session = SESSION_FACTORY()
    try:
        company_id = (
            session.execute(select(OrgPartyMap.org_id).order_by(OrgPartyMap.org_id).limit(1)).scalar_one_or_none()
        )
        individual_user_id = (
            session.execute(
                select(UserPartyMap.user_id)
                .join(AppUser, AppUser.id == UserPartyMap.user_id)
                .where(AppUser.username != "admin")
                .order_by(UserPartyMap.user_id)
                .limit(1)
            ).scalar_one_or_none()
        )
        return {"company_id": company_id, "individual_user_id": individual_user_id}
    except Exception:
        LOGGER.exception("Failed to load demo targets for presentation slides")
        return {"company_id": None, "individual_user_id": None}
    finally:
        session.close()


def _get_slide_payload(slide: Slide) -> dict[str, object]:
    if slide.slug in {"admin-dashboard", "company-dashboard", "individual-dashboard"}:
        return {"demo_targets": _load_demo_targets()}
    if slide.slug == "simulated-data":
        return {"individual_dashboard": _load_individual_dashboard(3)}
    return {}


@router.get("/", summary="Interactive presentation", response_class=HTMLResponse)
async def presentation_home(
    request: Request,
    _: AuthenticatedUser = Depends(require_admin_user),
) -> HTMLResponse:
    LOGGER.info("Presentation view requested")
    active_slide = SLIDES[0]
    context = {
        "request": request,
        "slides": SLIDES,
        "active_slide": active_slide,
        "slides_data": _serialize_slides(SLIDES),
        "slide_payload": _get_slide_payload(active_slide),
    }
    return templates.TemplateResponse("presentation/index.html", context=context)


@router.get(
    "/slide/{slug}",
    summary="Render a single presentation slide",
    response_class=HTMLResponse,
)
async def render_slide(
    request: Request,
    slug: str,
    _: AuthenticatedUser = Depends(require_admin_user),
) -> HTMLResponse:
    slide = _get_slide(slug)
    LOGGER.info("Rendering presentation slide %s", slug)
    context = {
        "request": request,
        "slide": slide,
        "slide_payload": _get_slide_payload(slide),
    }
    return templates.TemplateResponse(slide.template, context)
