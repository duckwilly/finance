"""ORM models representing individual customers."""
from __future__ import annotations

from datetime import datetime

from sqlalchemy import BigInteger, DateTime, Integer, String
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.sql import func

from .base import EntityBase
from .transactions import AccountOwnerType

_ID_TYPE = BigInteger().with_variant(Integer, "sqlite")


class Individual(EntityBase):
    """Represents an end-user/individual in the system."""

    __tablename__ = "user"

    id: Mapped[int] = mapped_column(_ID_TYPE, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(120), nullable=False)
    email: Mapped[str | None] = mapped_column(String(255), unique=True)
    job_title: Mapped[str | None] = mapped_column(String(120), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.current_timestamp(), nullable=False
    )

    @property
    def owner_type(self) -> AccountOwnerType:
        """Return the account owner type for individuals."""
        return AccountOwnerType.USER
