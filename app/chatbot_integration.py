"""
Integration layer for the AI chatbot with the Finance Dashboard.

This module provides dependency injection and configuration for the AI chatbot package.
"""
import re
from typing import Dict, Any
from urllib.parse import urlparse

from fastapi import HTTPException, Request, status
from sqlalchemy import select

from app.chatbot_schema import DATABASE_SCHEMA
from app.core.logger import get_logger
from app.core.security import AuthenticatedUser
from app.db.session import get_sessionmaker
from app.models import OrgPartyMap

LOGGER = get_logger(__name__)
SESSION_FACTORY = get_sessionmaker()


def get_db_session():
    """Provide a database session for the chatbot."""

    session = SESSION_FACTORY()
    try:
        yield session
    finally:
        session.close()


def _parse_route_scope(request: Request) -> tuple[str | None, int | None]:
    """Extract the active dashboard scope from the request/HTMX headers."""

    raw_url = (
        request.headers.get("Hx-Current-Url")
        or request.headers.get("HX-Current-URL")
        or request.headers.get("referer")
        or str(request.url)
    )
    path = urlparse(raw_url).path

    individual_match = re.search(r"/individuals/(?P<user_id>\d+)", path)
    if individual_match:
        return "individual", int(individual_match.group("user_id"))

    company_match = re.search(r"/corporate/(?P<company_id>\d+)", path)
    if company_match:
        return "company", int(company_match.group("company_id"))

    return None, None


def _resolve_company_party_id(company_org_id: int | None) -> int | None:
    """Translate a company org_id into the corresponding party_id."""

    if not company_org_id:
        return None

    session = SESSION_FACTORY()
    try:
        return (
            session.execute(
                select(OrgPartyMap.party_id).where(OrgPartyMap.org_id == company_org_id)
            ).scalar_one_or_none()
        )
    except Exception as exc:  # pragma: no cover - runtime safeguard
        LOGGER.warning("Failed to resolve company org_id %s: %s", company_org_id, exc)
        return None
    finally:
        session.close()


def get_current_user_context_from_request(request: Request) -> Dict[str, Any]:
    """Extract user context from the request for the chatbot.

    The chatbot needs user information in this format:
    {
        "role": "person" | "company" | "admin",
        "person_id": int | None,
        "company_id": int | None,
        "username": str
    }

    Args:
        request: The FastAPI request object

    Returns:
        User context dictionary

    Raises:
        HTTPException: If user is not authenticated or not allowed
    """
    user: AuthenticatedUser | None = getattr(request.state, "user", None)

    if user is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Not authenticated",
        )

    route_scope, route_id = _parse_route_scope(request)
    person_party_id = user.party_id
    company_party_id = None

    if route_scope == "company" and route_id is not None:
        if user.role != "admin" and "ADMIN" not in user.roles and route_id not in user.company_ids:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Access denied for this company",
            )
        company_party_id = _resolve_company_party_id(route_id)
    elif route_scope == "individual" and route_id is not None:
        if user.role != "admin" and "ADMIN" not in user.roles and user.subject_id != route_id:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Access denied for this user",
            )

    is_admin = user.role == "admin" or "ADMIN" in user.roles
    if is_admin:
        chatbot_role = "admin"
        person_id = None
        company_id = None
    elif company_party_id and route_scope == "company":
        chatbot_role = "company"
        person_id = None
        company_id = company_party_id
    else:
        chatbot_role = "person"
        person_id = person_party_id
        company_id = None

    if chatbot_role == "person" and not person_id:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="User profile missing party scope for chatbot queries",
        )
    if chatbot_role == "company" and not company_id:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Company profile missing party scope for chatbot queries",
        )

    return {
        "role": chatbot_role,
        "person_id": person_id,
        "company_id": company_id,
        "username": user.username,
    }


def get_current_user_context(request: Request) -> Dict[str, Any]:
    """Wrapper function that uses the standardized name expected by main.py."""

    return get_current_user_context_from_request(request)


def get_database_schema() -> str:
    """Return the database schema description for the chatbot."""

    return DATABASE_SCHEMA
