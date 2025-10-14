"""Opinionated SQLAlchemy session helpers."""
from __future__ import annotations

from contextlib import contextmanager
from typing import Iterator

from sqlalchemy.orm import sessionmaker

from .engine import create_sync_engine


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
