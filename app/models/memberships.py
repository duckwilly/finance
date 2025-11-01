"""Employment and relationship models."""
from __future__ import annotations

from datetime import date, datetime

from sqlalchemy import (
    BigInteger,
    Boolean,
    Date,
    DateTime,
    ForeignKey,
    Integer,
    String,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from .base import Base

_ID_TYPE = BigInteger().with_variant(Integer, "sqlite")


class EmploymentContract(Base):
    """Employment relationship between an individual party and a company party."""

    __tablename__ = "employment_contract"

    id: Mapped[int] = mapped_column(_ID_TYPE, primary_key=True, autoincrement=True)
    employee_party_id: Mapped[int] = mapped_column(ForeignKey("party.id"), nullable=False, index=True)
    employer_party_id: Mapped[int] = mapped_column(ForeignKey("party.id"), nullable=False, index=True)
    position_title: Mapped[str] = mapped_column(String(160), nullable=False)
    start_date: Mapped[date] = mapped_column(Date, nullable=False)
    end_date: Mapped[date | None] = mapped_column(Date)
    is_primary: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default="1")

    employee_party = relationship("Party", foreign_keys=[employee_party_id])
    employer_party = relationship("Party", foreign_keys=[employer_party_id])


class PartyRelationship(Base):
    """Generic directed relationship between parties."""

    __tablename__ = "party_relationship"

    id: Mapped[int] = mapped_column(_ID_TYPE, primary_key=True, autoincrement=True)
    from_party_id: Mapped[int] = mapped_column(ForeignKey("party.id"), nullable=False)
    to_party_id: Mapped[int] = mapped_column(ForeignKey("party.id"), nullable=False)
    relationship_type: Mapped[str] = mapped_column(String(64), nullable=False)
    start_date: Mapped[date | None] = mapped_column(Date)
    end_date: Mapped[date | None] = mapped_column(Date)

    from_party = relationship("Party", foreign_keys=[from_party_id])
    to_party = relationship("Party", foreign_keys=[to_party_id])


class CompanyAccessGrant(Base):
    """Materialised access grants linking authenticated users to company workspaces."""

    __tablename__ = "company_access_grant"

    id: Mapped[int] = mapped_column(_ID_TYPE, primary_key=True, autoincrement=True)
    contract_id: Mapped[int] = mapped_column(ForeignKey("employment_contract.id"), nullable=False)
    app_user_id: Mapped[int] = mapped_column(ForeignKey("app_user.id"), nullable=False)
    role_code: Mapped[str] = mapped_column(String(32), ForeignKey("app_role.code"), nullable=False)
    granted_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )
    revoked_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    contract = relationship("EmploymentContract")
    app_user = relationship("AppUser")
    role = relationship("AppRole")
