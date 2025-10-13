"""Opinionated SQLAlchemy session helpers."""
from __future__ import annotations

import os
from contextlib import contextmanager
from typing import Iterator

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.logger import get_logger

LOGGER = get_logger(__name__)


def _default_url() -> str:
    driver = os.getenv("DB_DRIVER", "mysql+pymysql")
    user = os.getenv("DB_USER", "app")
    password = os.getenv("DB_PASSWORD", "apppwd")
    host = os.getenv("DB_HOST", "127.0.0.1")
    port = os.getenv("DB_PORT", "3306")
    name = os.getenv("DB_NAME", "finance")
    return f"{driver}://{user}:{password}@{host}:{port}/{name}"


def create_sync_engine(url: str | None = None, **kwargs):
    """Create a synchronous SQLAlchemy engine using env defaults."""

    url = url or _default_url()
    LOGGER.debug("Creating SQLAlchemy engine", extra={"url": url})
    return create_engine(url, future=True, **kwargs)


def get_sessionmaker(url: str | None = None, **kwargs) -> sessionmaker:
    """Return a ``sessionmaker`` bound to a shared engine."""

    engine = create_sync_engine(url, **kwargs)
    return sessionmaker(bind=engine, autoflush=False, expire_on_commit=False)


@contextmanager
def session_scope(url: str | None = None, **kwargs) -> Iterator:
    """Provide a transactional scope for imperative scripts."""

    Session = get_sessionmaker(url, **kwargs)
    session = Session()
    try:
        yield session
        session.commit()
    except Exception:  # pragma: no cover - re-raise for callers
        session.rollback()
        raise
    finally:
        session.close()
