"""Shared helpers for rendering dashboard-style pages."""
from __future__ import annotations

from typing import Mapping, Sequence

from fastapi import Request
from fastapi.responses import HTMLResponse

from app.core.templates import templates

HTMX_SHELL_CONFIG: dict[str, object] = {
    "target": "#dashboard-shell",
    "swap": "outerHTML",
    "select": "#dashboard-shell",
    "push_url": True,
}


def render_dashboard_template(
    request: Request,
    *,
    view_key: str,
    hero_title: str,
    hero_eyebrow: str,
    hero_badge: str | None,
    cards: Sequence[Mapping[str, object]],
    panel_name: str,
    panel_chart,
    panel_config,
    panel_view,
    panel_endpoint: str,
    hero_subtitle: str | None = None,
    hero_subtitle_url: str | None = None,
    panel_params: Mapping[str, object] | None = None,
    show_admin_return: bool = False,
    page_title: str | None = None,
    list_htmx_config: Mapping[str, object] | None = None,
) -> HTMLResponse:
    """Render the unified dashboard template with supplied content."""

    embed_flag = str(request.query_params.get("embed", "")).lower() in {"1", "true", "yes", "on"}

    params = dict(panel_params or {})
    card_payloads: list[dict[str, object]] = []

    for card in cards:
        payload = dict(card)
        payload.setdefault("value_type", "text")
        payload.setdefault("short", False)
        payload.setdefault("decimals", 1)
        payload["panel_url"] = request.url_for(
            panel_endpoint,
            panel_name=payload["panel"],
            **params,
        )
        card_payloads.append(payload)

    context = {
        "request": request,
        "page_title": page_title or f"{hero_title} • Finance",
        "view_key": view_key,
        "hero_title": hero_title,
        "hero_eyebrow": hero_eyebrow,
        "hero_subtitle": hero_subtitle,
        "hero_subtitle_url": hero_subtitle_url,
        "hero_badge": hero_badge,
        "cards": card_payloads,
        "panel_chart": panel_chart,
        "panel_config": panel_config,
        "panel_name": panel_name,
        "panel_view": panel_view,
        "panel_endpoint": panel_endpoint,
        "panel_params": params,
        "show_admin_return": show_admin_return,
        "admin_return_url": request.url_for("read_dashboard"),
        "list_htmx_config": list_htmx_config,
        "is_embed": embed_flag,
    }

    return templates.TemplateResponse(
        request=request,
        name="dashboard/index.html",
        context=context,
    )


__all__ = ["HTMX_SHELL_CONFIG", "render_dashboard_template"]
