"""Base declarative class for SQLAlchemy models."""
from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.orm import DeclarativeBase, Session

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .transactions import Account, AccountOwnerType


class Base(DeclarativeBase):
    """Declarative base shared by all ORM models."""

    pass


class EntityBase(Base):
    """Base class for entities that own accounts (Individual, Company).
    
    Provides common functionality for entities that can own financial accounts.
    Subclasses must define id, name columns and implement owner_type property.
    """

    __abstract__ = True
    
    id: int
    name: str

    @property
    def owner_type(self) -> "AccountOwnerType":
        """Account owner type for this entity. Must be implemented in subclass."""
        raise NotImplementedError("Subclass must implement owner_type property")

    def __repr__(self) -> str:
        """String representation showing id and name."""
        return f"<{self.__class__.__name__}(id={self.id}, name={self.name})>"

    def get_accounts(self, session: Session) -> list["Account"]:
        """Query all accounts owned by this entity."""
        from .transactions import Account

        query = select(Account).where(
            Account.owner_type == self.owner_type,
            Account.owner_id == self.id,
        )
        return list(session.scalars(query).all())
