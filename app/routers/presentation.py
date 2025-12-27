"""Presentation routes for the interactive slide deck."""
from __future__ import annotations

from dataclasses import dataclass
from html import escape
from pathlib import Path
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
from app.services import CompaniesService, IndividualsService

LOGGER = get_logger(__name__)
router = APIRouter(prefix="/presentation", tags=["presentation"])
SESSION_FACTORY: sessionmaker = get_sessionmaker()
INDIVIDUALS_SERVICE = IndividualsService()
COMPANIES_SERVICE = CompaniesService()
DASHBOARD_TEMPLATE_PATH = Path(__file__).resolve().parent.parent / "templates" / "dashboard" / "index.html"


@dataclass(frozen=True)
class Slide:
    slug: str
    title_nl: str
    title_en: str
    template_nl: str
    template_en: str
    summary_nl: str
    summary_en: str
    order: str | None = None


@dataclass(frozen=True)
class SlideView:
    slug: str
    title: str
    template: str
    summary: str
    order: str | None = None


SUPPORTED_LANGS = ("nl", "en")


SLIDES: tuple[Slide, ...] = (
    Slide(
        slug="overview",
        title_nl="Projectoverzicht",
        title_en="Project overview",
        template_nl="presentation/slides/projectoverzicht.html",
        template_en="presentation/slides_en/projectoverzicht.html",
        summary_nl="Hoofddoelen en de doelgroepen die we ondersteunen.",
        summary_en="Key goals and the audiences we support.",
        order="1",
    ),
    Slide(
        slug="simulated-data",
        title_nl="Gesimuleerde bankdata",
        title_en="Simulated bank data",
        template_nl="presentation/slides/gesimuleerde_bankdata.html",
        template_en="presentation/slides_en/gesimuleerde_bankdata.html",
        summary_nl="Individuen en bedrijven met transacties, holdings, en gebruiker 3 als echt voorbeeld.",
        summary_en="Individuals and companies with transactions, holdings, and a live demo user.",
        order="1.1",
    ),
    Slide(
        slug="admin-browse",
        title_nl="Admin browse en zoeken",
        title_en="Admin browse and search",
        template_nl="presentation/slides/admin_browse_en_zoeken.html",
        template_en="presentation/slides_en/admin_browse_en_zoeken.html",
        summary_nl="Dashboard ingangspunten om individuen en bedrijven in één oogopslag te doorzoeken.",
        summary_en="Entry points to browse individuals and companies at a glance.",
        order="1.2",
    ),
    Slide(
        slug="access-control",
        title_nl="Login, rollen en dashboards",
        title_en="Login, roles, and dashboards",
        template_nl="presentation/slides/login_rollen_en_dashboards.html",
        template_en="presentation/slides_en/login_rollen_en_dashboards.html",
        summary_nl="JWT cookies, rolchecks en routing naar de juiste dashboards met een live voorbeeld.",
        summary_en="JWT cookies, role checks, and routing to the right dashboards with a live example.",
        order="1.3",
    ),
    Slide(
        slug="architecture",
        title_nl="Architectuur & tech stack",
        title_en="Architecture & tech stack",
        template_nl="presentation/slides/architectuur_en_tech_stack.html",
        template_en="presentation/slides_en/architectuur_en_tech_stack.html",
        summary_nl="Hoe de FastAPI, database en front-end lagen samenwerken.",
        summary_en="How FastAPI, the database, and the frontend layers work together.",
        order="2",
    ),
    Slide(
        slug="database-stack",
        title_nl="Database platform",
        title_en="Database platform",
        template_nl="presentation/slides/database_platform.html",
        template_en="presentation/slides_en/database_platform.html",
        summary_nl="MariaDB op Docker die de simulaties ondersteunt.",
        summary_en="MariaDB on Docker powering the simulations.",
        order="2.1",
    ),
    Slide(
        slug="database",
        title_nl="Database-ontwerp",
        title_en="Database design",
        template_nl="presentation/slides/database_ontwerp.html",
        template_en="presentation/slides_en/database_ontwerp.html",
        summary_nl="Aard van financiële data, 3NF/BCNF-normalisatie en ons dubbelboekings-journal.",
        summary_en="Financial data, 3NF/BCNF normalization, and our double-entry journal.",
        order="2.2",
    ),
    Slide(
        slug="show-log",
        title_nl="Logging",
        title_en="Logging",
        template_nl="presentation/slides/logging.html",
        template_en="presentation/slides_en/logging.html",
        summary_nl="Logging voor debuggen en monitoren",
        summary_en="Logging for debugging and monitoring.",
        order="2.3",
    ),
    Slide(
        slug="ai-chatbot",
        title_nl="Bonus",
        title_en="Bonus",
        template_nl="presentation/slides/ai_chatbot_mogelijkheden.html",
        template_en="presentation/slides_en/ai_chatbot_mogelijkheden.html",
        summary_nl="Prompt assembly, visuele responses en database-aware tooling.",
        summary_en="Prompt assembly, visual responses, and database-aware tooling.",
        order="3",
    ),
    Slide(
        slug="prompt-assembly",
        title_nl="Prompt assembly",
        title_en="Prompt assembly",
        template_nl="presentation/slides/prompt_assembly.html",
        template_en="presentation/slides_en/prompt_assembly.html",
        summary_nl="Hoe prompts worden geconstrueerd voor model calls.",
        summary_en="How prompts are constructed for model calls.",
        order="3.1",
    ),
    Slide(
        slug="json-response",
        title_nl="LLM output naar Chart.js",
        title_en="LLM output to Chart.js",
        template_nl="presentation/slides/json_responses.html",
        template_en="presentation/slides_en/json_responses.html",
        summary_nl="LLM JSON response wordt gevalideerd, opgeschoond en omgezet naar Chart.js configuraties.",
        summary_en="LLM JSON output is validated, cleaned, and converted into Chart.js configs.",
        order="3.2",
    ),
)


def _get_slide(slug: str) -> Slide:
    for slide in SLIDES:
        if slide.slug == slug:
            return slide
    raise HTTPException(status_code=404, detail="Slide not found")

def _resolve_lang(request: Request) -> str:
    raw = request.query_params.get("lang", "nl").lower()
    return raw if raw in SUPPORTED_LANGS else "nl"


def _resolve_slide(slide: Slide, lang: str) -> SlideView:
    if lang == "en":
        return SlideView(
            slug=slide.slug,
            title=slide.title_en,
            template=slide.template_en,
            summary=slide.summary_en,
            order=slide.order,
        )
    return SlideView(
        slug=slide.slug,
        title=slide.title_nl,
        template=slide.template_nl,
        summary=slide.summary_nl,
        order=slide.order,
    )


def _serialize_slides(slides: Iterable[Slide], lang: str) -> list[dict[str, str]]:
    serialized: list[dict[str, str]] = []
    for index, slide in enumerate(slides, start=1):
        resolved = _resolve_slide(slide, lang)
        serialized.append(
            {
                "slug": resolved.slug,
                "title": resolved.title,
                "summary": resolved.summary,
                "order": resolved.order or str(index),
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


def _get_slide_payload(slug: str) -> dict[str, object]:
    if slug == "access-control":
        return {"demo_targets": _load_demo_targets()}
    if slug == "simulated-data":
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
    lang = _resolve_lang(request)
    requested_slug = request.query_params.get("slide")
    active_base = _get_slide(requested_slug) if requested_slug else SLIDES[0]
    active_slide = _resolve_slide(active_base, lang)
    slides = [_resolve_slide(slide, lang) for slide in SLIDES]
    template_name = "presentation/shell.html" if request.headers.get("HX-Request") == "true" else "presentation/index.html"
    context = {
        "request": request,
        "lang": lang,
        "slides": slides,
        "active_slide": active_slide,
        "active_slide_index": _get_slide_index(active_base),
        "slides_data": _serialize_slides(SLIDES, lang),
        "slide_payload": _get_slide_payload(active_slide.slug),
    }
    return templates.TemplateResponse(template_name, context=context)


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
    lang = _resolve_lang(request)
    slide = _resolve_slide(_get_slide(slug), lang)
    LOGGER.info("Rendering presentation slide %s", slug)
    context = {
        "request": request,
        "slide": slide,
        "slide_payload": _get_slide_payload(slug),
    }
    return templates.TemplateResponse(slide.template, context)
