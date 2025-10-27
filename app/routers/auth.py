"""Authentication routes providing login and logout actions."""
from __future__ import annotations

from fastapi import APIRouter, Depends, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse, Response

from app.core.logger import get_logger
from app.core.security import (
    AuthenticatedUser,
    SecurityProvider,
    get_security_provider,
)
from app.core.templates import templates
from app.db.session import get_sessionmaker
from app.models.individuals import Individual
from app.models.companies import Company
from sqlalchemy import select, func

LOGGER = get_logger(__name__)
router = APIRouter(tags=["auth"])


def _default_destination(user: AuthenticatedUser) -> str:
    if user.role == "admin":
        return "/dashboard/"
    if user.role == "individual" and user.subject_id is not None:
        return f"/individuals/{user.subject_id}"
    if user.role == "company" and user.subject_id is not None:
        return f"/corporate/{user.subject_id}"
    return "/dashboard/"


def _safe_next_path(next_path: str | None) -> str | None:
    if not next_path:
        return None
    next_path = next_path.strip()
    if not next_path:
        return None
    if next_path.startswith("/") and not next_path.startswith("//"):
        return next_path
    return None


def get_security() -> SecurityProvider:
    return get_security_provider()


def _get_user_counts() -> dict[str, int]:
    """Get counts of individuals and companies from the database."""
    session_factory = get_sessionmaker()
    with session_factory() as session:
        individual_count = session.scalar(select(func.count()).select_from(Individual))
        company_count = session.scalar(select(func.count()).select_from(Company))
        return {
            "individual_count": individual_count or 0,
            "company_count": company_count or 0,
        }

@router.get("/login", response_class=HTMLResponse, include_in_schema=False)
async def login_form(request: Request) -> HTMLResponse:
    """Render the login form. Redirect when already authenticated."""

    user: AuthenticatedUser | None = getattr(request.state, "user", None)
    if user is not None:
        LOGGER.debug("User already authenticated", extra={"username": user.username})
        return RedirectResponse(_default_destination(user), status_code=303)

    next_path = _safe_next_path(request.query_params.get("next"))
    security = get_security()
    counts = _get_user_counts()
    context = {
        "request": request,
        "next": next_path or "",
        "error": None,
        "admin_username": security.admin_username,
        "demo_password": security.demo_user_password,
        "username": "",
        **counts,
    }
    return templates.TemplateResponse("auth/login.html", context, status_code=200)


@router.post("/login", include_in_schema=False)
async def login_submit(
    request: Request,
    username: str = Form(...),
    password: str = Form(...),
    next_path: str = Form("", alias="next"),
    security: SecurityProvider = Depends(get_security),
) -> Response:
    """Handle login submissions and issue an access token cookie."""

    user = security.authenticate(username, password)
    if user is None:
        LOGGER.info("Invalid login attempt", extra={"username": username})
        counts = _get_user_counts()
        context = {
            "request": request,
            "next": _safe_next_path(next_path) or "",
            "error": "Invalid username or password",
            "admin_username": security.admin_username,
            "demo_password": security.demo_user_password,
            "username": username,
            **counts,
        }
        return templates.TemplateResponse(
            "auth/login.html",
            context,
            status_code=401,
        )

    token = security.create_access_token(user)
    redirect_to = _safe_next_path(next_path) or _default_destination(user)
    response = RedirectResponse(redirect_to, status_code=303)
    response.set_cookie(
        security.cookie_name,
        token,
        max_age=security.token_ttl_seconds,
        httponly=True,
        samesite="lax",
    )
    LOGGER.info("User logged in", extra={"username": user.username, "role": user.role})
    return response


@router.get("/logout", include_in_schema=False)
async def logout(security: SecurityProvider = Depends(get_security)) -> Response:
    """Clear the access token and redirect to the login page."""

    response = RedirectResponse("/login", status_code=303)
    response.delete_cookie(security.cookie_name)
    return response


__all__ = ["router"]
