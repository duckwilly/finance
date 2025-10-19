"""ORM model for linking individuals to organisations."""
from __future__ import annotations

from sqlalchemy import BigInteger, ForeignKey, Integer, String
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base

_ID_TYPE = BigInteger().with_variant(Integer, "sqlite")


class Membership(Base):
    """Associates an individual with an employer/company."""

    __tablename__ = "membership"

    id: Mapped[int] = mapped_column(_ID_TYPE, primary_key=True, autoincrement=True)
    user_id: Mapped[int] = mapped_column(ForeignKey("user.id"), nullable=False, index=True)
    org_id: Mapped[int] = mapped_column(ForeignKey("org.id"), nullable=False, index=True)
    role: Mapped[str] = mapped_column(String(64), nullable=False, server_default="member")
