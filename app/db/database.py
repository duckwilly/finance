"""Database utilities for interacting with SQLAlchemy."""
from __future__ import annotations

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.orm import sessionmaker

from app.core.config import get_settings
from app.core.logger import get_logger

LOGGER = get_logger(__name__)


def create_engine_from_settings() -> Engine:
    """Create a synchronous SQLAlchemy engine using project settings."""

    settings = get_settings()
    LOGGER.debug(
        "Creating SQLAlchemy engine",
        extra={
            "driver": settings.database.driver,
            "host": settings.database.host,
            "port": settings.database.port,
            "name": settings.database.name,
        },
    )
    return create_engine(settings.database.sqlalchemy_url, future=True)


def get_session_factory() -> sessionmaker:
    """Return a session factory bound to the shared engine."""

    engine = create_engine_from_settings()
    return sessionmaker(bind=engine, autoflush=False, expire_on_commit=False)
