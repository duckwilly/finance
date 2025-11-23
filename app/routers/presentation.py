"""Presentation routes for the interactive slide deck."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import HTMLResponse
from sqlalchemy.orm import sessionmaker

from app.core.logger import get_logger
from app.core.security import AuthenticatedUser, require_admin_user
from app.core.templates import templates
from app.db.session import get_sessionmaker
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
        title="Projectoverzicht",
        template="presentation/slides/overview.html",
        summary="Hoofddoelen en de doelgroepen die we ondersteunen.",
        order="1",
    ),
    Slide(
        slug="simulated-data",
        title="Gesimuleerde bankdata",
        template="presentation/slides/simulated_data.html",
        summary="Individuen en bedrijven met transacties, holdings, en gebruiker 3 als echt voorbeeld.",
        order="1.1",
    ),
    Slide(
        slug="admin-browse",
        title="Admin browse en zoeken",
        template="presentation/slides/admin_browse.html",
        summary="Dashboard ingangspunten om individuen en bedrijven in één oogopslag te doorzoeken.",
        order="1.2",
    ),
    Slide(
        slug="individual-access",
        title="Individuele en bedrijfstoegang",
        template="presentation/slides/individual_access.html",
        summary="Mensen kunnen inloggen om hun eigen data en gekoppelde bedrijfsrecords te bekijken.",
        order="1.3",
    ),
    Slide(
        slug="architecture",
        title="Architectuur & tech stack",
        template="presentation/slides/architecture.html",
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
        template="presentation/slides/database_stack.html",
        summary="MariaDB op Docker die de simulaties ondersteunt.",
        order="2.3",
    ),
    Slide(
        slug="database",
        title="Database ontwerp",
        template="presentation/slides/database.html",
        summary="Schema highlights voor financiële data, relaties en doelen.",
        order="3",
    ),
    Slide(
        slug="data-nature",
        title="Aard van financiële data",
        template="presentation/slides/stub.html",
        summary="Wat we bijhouden voor partijen, accounts en holdings.",
        order="3.1",
    ),
    Slide(
        slug="data-structure",
        title="Structuur van de database",
        template="presentation/slides/stub.html",
        summary="Hoe tabellen, journaals en seed data zijn opgebouwd.",
        order="3.2",
    ),
    Slide(
        slug="data-relationships",
        title="Relaties tussen tabellen",
        template="presentation/slides/stub.html",
        summary="Verbindingen tussen accounts, transacties en effecten.",
        order="3.3",
    ),
    Slide(
        slug="data-goals",
        title="Doelen en ontwerpbeslissingen",
        template="presentation/slides/stub.html",
        summary="Waarom het schema is gemodelleerd voor financiële rapportage.",
        order="3.4",
    ),
    Slide(
        slug="features",
        title="Features & demo's",
        template="presentation/slides/features.html",
        summary="Dashboards, admin workflows en storytelling momenten om door te klikken.",
        order="4",
    ),
    Slide(
        slug="auth",
        title="Authenticatie en autorisatie",
        template="presentation/slides/stub.html",
        summary="Inlog flows en rol-gevoelige views.",
        order="4.1",
    ),
    Slide(
        slug="admin-dashboard",
        title="Admin dashboard overzicht",
        template="presentation/slides/stub.html",
        summary="Admin hubs voor het doorbladeren van bedrijven, individuen en transacties.",
        order="4.2",
    ),
    Slide(
        slug="admin-big-picture",
        title="Groot overzicht database",
        template="presentation/slides/stub.html",
        summary="Top-level metrics over de hele dataset.",
        order="4.2.1",
    ),
    Slide(
        slug="admin-views",
        title="Entity drill-downs",
        template="presentation/slides/stub.html",
        summary="Views van bedrijven, individuen, transacties en holdings.",
        order="4.2.2",
    ),
    Slide(
        slug="company-dashboard",
        title="Bedrijfsdashboard",
        template="presentation/slides/stub.html",
        summary="Netto waarde, payroll en transactie breakdowns voor een bedrijf.",
        order="4.3",
    ),
    Slide(
        slug="individual-dashboard",
        title="Individueel dashboard",
        template="presentation/slides/stub.html",
        summary="Persoonlijke netto waarde, brokerage holdings, inkomsten en uitgaven.",
        order="4.4",
    ),
    Slide(
        slug="ai-insights",
        title="AI inzichten demo",
        template="presentation/slides/stub.html",
        summary="De AI chatbot gebruiken om trends te beschrijven en visuals te genereren.",
        order="4.5",
    ),
    Slide(
        slug="ai-trend-example",
        title="AI trend walkthrough",
        template="presentation/slides/stub.html",
        summary="Voorbeeld van het visualiseren van een bedrijf dat omhoog of omlaag gaat.",
        order="4.5.1",
    ),
    Slide(
        slug="show-log",
        title="Log tonen",
        template="presentation/slides/show_log.html",
        summary="Logging views voor demo's en audits.",
        order="4.6",
    ),
    Slide(
        slug="ai-chatbot",
        title="AI chatbot mogelijkheden",
        template="presentation/slides/ai_chatbot.html",
        summary="Prompt assembly, visuele responses en database-aware tooling.",
        order="5",
    ),
    Slide(
        slug="prompt-assembly",
        title="Prompt assembly",
        template="presentation/slides/stub.html",
        summary="Hoe prompts worden geconstrueerd voor model calls.",
        order="5.1",
    ),
    Slide(
        slug="json-response",
        title="JSON responses",
        template="presentation/slides/stub.html",
        summary="Gestructureerde outputs die door de chatbot worden geretourneerd.",
        order="5.2",
    ),
    Slide(
        slug="db-querying",
        title="Python database querying",
        template="presentation/slides/stub.html",
        summary="Python gebruiken om data te lezen en aggregeren voor antwoorden.",
        order="5.3",
    ),
    Slide(
        slug="dashboard-charts",
        title="Dashboard grafieken via httpx",
        template="presentation/slides/stub.html",
        summary="Dashboard-vriendelijke grafieken genereren over HTTP.",
        order="5.4",
    ),
    Slide(
        slug="operational-notes",
        title="Operations & logging",
        template="presentation/slides/operations.html",
        summary="Telemetrie, startup warmups en hoe we het platform monitoren.",
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


def _get_slide_payload(slide: Slide) -> dict[str, object]:
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
