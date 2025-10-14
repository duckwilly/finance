"""Shared FastAPI dependency definitions."""
from __future__ import annotations

from collections.abc import Generator

from sqlalchemy.orm import Session, sessionmaker

from app.db.session import get_sessionmaker

# Instantiate a session factory once so that connections can be reused by the
# FastAPI dependency system. ``get_sessionmaker`` internally shares the engine
# through SQLAlchemy, so this avoids re-creating engines on every request.
SessionFactory: sessionmaker = get_sessionmaker()


def get_db_session() -> Generator[Session, None, None]:
    """Yield a database session suitable for request-scoped usage."""

    session = SessionFactory()
    try:
        yield session
    finally:
        session.close()
