"""Presentation routes for the interactive slide deck."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import HTMLResponse

from app.core.logger import get_logger
from app.core.security import AuthenticatedUser, require_admin_user
from app.core.templates import templates

LOGGER = get_logger(__name__)
router = APIRouter(prefix="/presentation", tags=["presentation"])


@dataclass(frozen=True)
class Slide:
    slug: str
    title: str
    template: str
    summary: str


SLIDES: tuple[Slide, ...] = (
    Slide(
        slug="overview",
        title="Project overview",
        template="presentation/slides/overview.html",
        summary="High-level goals and the audiences we support.",
    ),
    Slide(
        slug="architecture",
        title="Architecture & tech stack",
        template="presentation/slides/architecture.html",
        summary="How the FastAPI, database, and front-end layers fit together.",
    ),
    Slide(
        slug="database",
        title="Database design",
        template="presentation/slides/database.html",
        summary="Schema highlights for financial data, relationships, and goals.",
    ),
    Slide(
        slug="features",
        title="Features & demos",
        template="presentation/slides/features.html",
        summary="Dashboards, admin workflows, and storytelling moments to click through.",
    ),
    Slide(
        slug="ai-chatbot",
        title="AI chatbot capabilities",
        template="presentation/slides/ai_chatbot.html",
        summary="Prompt assembly, visual responses, and database-aware tooling.",
    ),
    Slide(
        slug="operational-notes",
        title="Operations & logging",
        template="presentation/slides/operations.html",
        summary="Telemetry, startup warmups, and how we monitor the platform.",
    ),
)


def _get_slide(slug: str) -> Slide:
    for slide in SLIDES:
        if slide.slug == slug:
            return slide
    raise HTTPException(status_code=404, detail="Slide not found")


def _serialize_slides(slides: Iterable[Slide]) -> list[dict[str, str]]:
    return [{"slug": slide.slug, "title": slide.title, "summary": slide.summary} for slide in slides]


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
    return templates.TemplateResponse(slide.template, {"request": request, "slide": slide})
