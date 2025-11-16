"""Integration layer for the AI chatbot with the Finance Dashboard.

This module provides dependency injection and configuration for the AI chatbot package.
"""
from typing import Iterator, Dict, Any
from fastapi import Request, HTTPException, status
from sqlalchemy.orm import Session

from app.db.session import get_sessionmaker
from app.core.security import AuthenticatedUser
from app.chatbot_schema import DATABASE_SCHEMA


def get_db_session():
    """Provide a database session for the chatbot.

    This is used as a FastAPI dependency, so it returns a generator
    that FastAPI will manage.
    """
    session_factory = get_sessionmaker()
    session = session_factory()
    try:
        yield session
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
        HTTPException: If user is not authenticated
    """
    # Get the authenticated user from request state (set by AuthMiddleware)
    user: AuthenticatedUser | None = getattr(request.state, 'user', None)

    if user is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Not authenticated"
        )

    # Map the finance dashboard user model to chatbot expected format
    # Determine role based on the user's role and context
    if user.role == "admin" or "ADMIN" in user.roles:
        chatbot_role = "admin"
        person_id = None
        company_id = None
    elif user.subject_id:
        # If user has a subject_id, they're likely a person
        chatbot_role = "person"
        person_id = user.subject_id
        company_id = None
    elif user.company_ids:
        # If user has company_ids, they're accessing as a company
        chatbot_role = "company"
        person_id = None
        # Use the first company ID (you may want to make this selectable)
        company_id = user.company_ids[0] if user.company_ids else None
    else:
        # Default to person role with party_id
        chatbot_role = "person"
        person_id = user.party_id
        company_id = None

    return {
        "role": chatbot_role,
        "person_id": person_id,
        "company_id": company_id,
        "username": user.username,
    }


def get_current_user_context(request: Request) -> Dict[str, Any]:
    """Wrapper function that uses the standardized name expected by main.py.

    This is an alias for get_current_user_context_from_request.
    """
    return get_current_user_context_from_request(request)


def get_database_schema() -> str:
    """Return the database schema description for the chatbot.

    Returns:
        The schema description string
    """
    return DATABASE_SCHEMA
