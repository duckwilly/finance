"""Party and profile models."""
from __future__ import annotations

from datetime import date, datetime
from enum import Enum

from sqlalchemy import (
    BigInteger,
    Boolean,
    Date,
    DateTime,
    Enum as SQLEnum,
    ForeignKey,
    String,
    UniqueConstraint,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy.sql import func

from app.models.base import Base


class PartyType(str, Enum):
    """Enumeration of supported party types."""

    INDIVIDUAL = "INDIVIDUAL"
    COMPANY = "COMPANY"


class Party(Base):
    """Represents any entity (individual or company) participating in the system."""

    __tablename__ = "party"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    party_type: Mapped[PartyType] = mapped_column(
        SQLEnum(PartyType, native_enum=False), nullable=False
    )
    display_name: Mapped[str] = mapped_column(String(160), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.current_timestamp()
    )

    app_user: Mapped["AppUser | None"] = relationship(back_populates="party", uselist=False)


class IndividualProfile(Base):
    """Additional data associated with individual parties."""

    __tablename__ = "individual_profile"

    party_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("party.id"), primary_key=True
    )
    given_name: Mapped[str] = mapped_column(String(80), nullable=False)
    family_name: Mapped[str] = mapped_column(String(80), nullable=False)
    primary_email: Mapped[str] = mapped_column(String(255), unique=True, nullable=False)
    residency_country: Mapped[str | None] = mapped_column(String(2))
    birth_date: Mapped["date | None"] = mapped_column(Date)


class CompanyProfile(Base):
    """Additional data associated with company parties."""

    __tablename__ = "company_profile"

    party_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("party.id"), primary_key=True
    )
    legal_name: Mapped[str] = mapped_column(String(160), nullable=False)
    registration_number: Mapped[str | None] = mapped_column(String(32), unique=True)
    tax_identifier: Mapped[str | None] = mapped_column(String(32), unique=True)
    industry_code: Mapped[str | None] = mapped_column(String(16))
    incorporation_date: Mapped[date | None] = mapped_column(Date)


class AppRole(Base):
    """Application role enumeration for RBAC."""

    __tablename__ = "app_role"

    code: Mapped[str] = mapped_column(String(32), primary_key=True)
    description: Mapped[str] = mapped_column(String(128), nullable=False)


class AppUser(Base):
    """Authenticated user account bound to an optional individual party."""

    __tablename__ = "app_user"
    __table_args__ = (UniqueConstraint("party_id", name="uq_app_user_party"),)

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    party_id: Mapped[int | None] = mapped_column(ForeignKey("party.id"), nullable=True)
    username: Mapped[str] = mapped_column(String(120), nullable=False, unique=True)
    email: Mapped[str] = mapped_column(String(255), nullable=False, unique=True)
    password_hash: Mapped[str] = mapped_column(String(255), nullable=False)
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default="1")
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.current_timestamp()
    )

    party: Mapped["Party | None"] = relationship(back_populates="app_user")
    roles: Mapped[list["AppUserRole"]] = relationship(
        back_populates="app_user", cascade="all, delete-orphan"
    )


class AppUserRole(Base):
    """Join table mapping authenticated users to granted roles."""

    __tablename__ = "app_user_role"

    app_user_id: Mapped[int] = mapped_column(
        BigInteger,
        ForeignKey("app_user.id"),
        primary_key=True,
    )
    role_code: Mapped[str] = mapped_column(
        String(32),
        ForeignKey("app_role.code"),
        primary_key=True,
    )

    app_user: Mapped[AppUser] = relationship(back_populates="roles")
    role: Mapped[AppRole] = relationship()


class LegacyUser(Base):
    """Legacy user records retained for mapping to parties."""

    __tablename__ = "user"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(120), nullable=False)
    email: Mapped[str | None] = mapped_column(String(255), unique=True)
    job_title: Mapped[str | None] = mapped_column(String(120))
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.current_timestamp()
    )


class LegacyOrg(Base):
    """Legacy organisation records retained for mapping to parties."""

    __tablename__ = "org"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(160), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.current_timestamp()
    )


class UserPartyMap(Base):
    """Mapping between legacy user records and party identities."""

    __tablename__ = "user_party_map"

    user_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("user.id"), primary_key=True
    )
    party_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("party.id"), nullable=False
    )


class OrgPartyMap(Base):
    """Mapping between legacy org records and party identities."""

    __tablename__ = "org_party_map"

    org_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("org.id"), primary_key=True
    )
    party_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("party.id"), nullable=False
    )
