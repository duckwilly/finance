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
from app.services import AdminService, CompaniesService, IndividualsService

LOGGER = get_logger(__name__)
router = APIRouter(prefix="/presentation", tags=["presentation"])
SESSION_FACTORY: sessionmaker = get_sessionmaker()
INDIVIDUALS_SERVICE = IndividualsService()
COMPANIES_SERVICE = CompaniesService()
ADMIN_SERVICE = AdminService()


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
        title="Projectoverzicht",
        template="presentation/slides/projectoverzicht.html",
        summary="Hoofddoelen en de doelgroepen die we ondersteunen.",
        order="1",
    ),
    Slide(
        slug="simulated-data",
        title="Gesimuleerde bankdata",
        template="presentation/slides/gesimuleerde_bankdata.html",
        summary="Individuen en bedrijven met transacties, holdings, en gebruiker 3 als echt voorbeeld.",
        order="1.1",
    ),
    Slide(
        slug="admin-browse",
        title="Admin browse en zoeken",
        template="presentation/slides/admin_browse_en_zoeken.html",
        summary="Dashboard ingangspunten om individuen en bedrijven in één oogopslag te doorzoeken.",
        order="1.2",
    ),
    Slide(
        slug="access-control",
        title="Login, rollen en dashboards",
        template="presentation/slides/login_rollen_en_dashboards.html",
        summary="JWT cookies, rolchecks en routing naar de juiste dashboards met een live voorbeeld.",
        order="1.3",
    ),
    Slide(
        slug="architecture",
        title="Architectuur & tech stack",
        template="presentation/slides/architectuur_en_tech_stack.html",
        summary="Hoe de FastAPI, database en front-end lagen samenwerken.",
        order="2",
    ),
    Slide(
        slug="frontend-stack",
        title="Frontend stack",
        template="presentation/slides/frontend_stack.html",
        summary="HTML, CSS, JavaScript, HTMX en Chart.js in de browser.",
        order="2.1",
    ),
    Slide(
        slug="backend-stack",
        title="Backend stack",
        template="presentation/slides/backend_stack.html",
        summary="Python services met SQLAlchemy, FastAPI, Jinja2, Pydantic en PyJWT.",
        order="2.2",
    ),
    Slide(
        slug="database-stack",
        title="Database platform",
        template="presentation/slides/database_platform.html",
        summary="MariaDB op Docker die de simulaties ondersteunt.",
        order="2.3",
    ),
    Slide(
        slug="database",
        title="Database-ontwerp",
        template="presentation/slides/database_ontwerp.html",
        summary="Aard van financiële data, 3NF/BCNF-normalisatie en ons dubbelboekings-journal.",
        order="2.4",
    ),
    Slide(
        slug="admin-dashboard-build",
        title="Admin dashboard: details",
        template="presentation/slides/admin_dashboard_details.html",
        summary="De opbouw van het admindashboard",
        order="2.5",
    ),
    Slide(
        slug="show-log",
        title="Logging",
        template="presentation/slides/logging.html",
        summary="Logging voor debuggen en monitoren",
        order="2.6",
    ),
    Slide(
        slug="ai-chatbot",
        title="AI chatbot",
        template="presentation/slides/ai_chatbot_mogelijkheden.html",
        summary="Prompt assembly, visuele responses en database-aware tooling.",
        order="3",
    ),
    Slide(
        slug="prompt-assembly",
        title="Prompt assembly",
        template="presentation/slides/prompt_assembly.html",
        summary="Hoe prompts worden geconstrueerd voor model calls.",
        order="3.1",
    ),
    Slide(
        slug="json-response",
        title="JSON responses",
        template="presentation/slides/json_responses.html",
        summary="Gestructureerde outputs die door de chatbot worden geretourneerd, gevalideerd met Pydantic.",
        order="3.2",
    ),
    Slide(
        slug="db-querying",
        title="Python database querying",
        template="presentation/slides/python_database_querying.html",
        summary="SQLAlchemy ORM queries met automatische toegangscontrole via Tool Registry systeem.",
        order="3.3",
    ),
    Slide(
        slug="dashboard-charts",
        title="Dashboard grafieken via HTTP",
        template="presentation/slides/dashboard_grafieken_via_http.html",
        summary="ToolResult → ChartGenerator → Chart.js config → HTTP response → Frontend rendering.",
        order="3.4",
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


def _load_company_dashboard(company_id: int):
    session = SESSION_FACTORY()
    try:
        return COMPANIES_SERVICE.get_dashboard(session, company_id)
    except Exception:
        LOGGER.exception("Failed to load company dashboard for slide", extra={"company_id": company_id})
        return None
    finally:
        session.close()


def _load_income_chart() -> dict[str, object] | None:
    session = SESSION_FACTORY()
    try:
        charts = ADMIN_SERVICE.get_dashboard_charts(session)
        return charts.individuals_income.model_dump()
    except Exception:
        LOGGER.exception("Failed to load income chart for presentation slide")
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
    if slide.slug == "access-control":
        return {"demo_targets": _load_demo_targets()}
    if slide.slug == "admin-dashboard-build":
        return {"income_chart": _load_income_chart()}
    if slide.slug == "simulated-data":
        individual_dashboard = _load_individual_dashboard(3)
        company_dashboard = None
        if individual_dashboard and individual_dashboard.employer_id:
            company_dashboard = _load_company_dashboard(individual_dashboard.employer_id)

        return {
            "individual_dashboard": individual_dashboard,
            "company_dashboard": company_dashboard,
        }
    return {}


def _get_slide_index(slide: Slide) -> int:
    for index, current in enumerate(SLIDES, start=1):
        if current.slug == slide.slug:
            return index
    return 1


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
        "active_slide_index": _get_slide_index(active_slide),
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
