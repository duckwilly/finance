"""Database engine factories."""
from __future__ import annotations

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

from app.config import get_settings
from app.core.logger import get_logger

LOGGER = get_logger(__name__)


def get_sqlalchemy_url() -> str:
    """Return the configured SQLAlchemy URL."""

    settings = get_settings()
    return settings.database.sqlalchemy_url


def create_sync_engine(url: str | None = None, **kwargs) -> Engine:
    """Create a synchronous SQLAlchemy engine using configured defaults."""

    settings = get_settings()
    resolved_url = url or settings.database.sqlalchemy_url

    options = dict(kwargs)
    options.setdefault("echo", settings.sqlalchemy_echo)

    masked_url = "{driver}://{user}:{pwd}@{host}:{port}/{name}".format(
        driver=settings.database.driver,
        user=settings.database.user,
        pwd="***" if settings.database.password else "",
        host=settings.database.host,
        port=settings.database.port,
        name=settings.database.name,
    )
    LOGGER.debug("Creating SQLAlchemy engine", extra={"url": masked_url, "options": options})
    return create_engine(resolved_url, future=True, **options)
