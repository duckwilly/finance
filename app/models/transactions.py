"""ORM models for financial accounts and transactions."""
from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from enum import Enum

from sqlalchemy import (
    BigInteger,
    Boolean,
    Date,
    DateTime,
    ForeignKey,
    Integer,
    Numeric,
    SmallInteger,
    String,
    UniqueConstraint,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship, synonym
from sqlalchemy.sql import func

from .base import Base
from .party import Party

_ID_TYPE = BigInteger().with_variant(Integer, "sqlite")
_SMALL_ID_TYPE = SmallInteger().with_variant(Integer, "sqlite")


class AccountRole(Base):
    """Lookup model representing participant roles on an account."""

    __tablename__ = "account_role"

    code: Mapped[str] = mapped_column(String(32), primary_key=True)
    description: Mapped[str] = mapped_column(String(128), nullable=False)


class Currency(Base):
    """ISO-style currency codes and display metadata."""

    __tablename__ = "currency"

    code: Mapped[str] = mapped_column(String(3), primary_key=True)
    name: Mapped[str] = mapped_column(String(64), nullable=False)
    exponent: Mapped[int] = mapped_column(_SMALL_ID_TYPE, nullable=False, server_default="2")


class AccountType(str, Enum):
    """Enumeration of supported account types (mirrors account_type table codes)."""

    CHECKING = "checking"
    SAVINGS = "savings"
    BROKERAGE = "brokerage"
    OPERATING = "operating"


class AccountTypeLookup(Base):
    """Lookup table for account type metadata."""

    __tablename__ = "account_type"

    code: Mapped[str] = mapped_column(String(32), primary_key=True)
    description: Mapped[str] = mapped_column(String(128), nullable=False)
    is_cash: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default="1")
    is_brokerage: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default="0")


class TxnChannel(Base):
    """Lookup model for transaction channels used by journal entries."""

    __tablename__ = "txn_channel"

    code: Mapped[str] = mapped_column(String(32), primary_key=True)
    description: Mapped[str] = mapped_column(String(128), nullable=False)


class Section(Base):
    """Transaction section grouping (income/expense/transfer)."""

    __tablename__ = "section"

    id: Mapped[int] = mapped_column(_SMALL_ID_TYPE, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(32), unique=True, nullable=False)


class Category(Base):
    """Detailed transaction category."""

    __tablename__ = "category"
    __table_args__ = (UniqueConstraint("section_id", "name", name="uniq_section_name"),)

    id: Mapped[int] = mapped_column(_ID_TYPE, primary_key=True, autoincrement=True)
    section_id: Mapped[int] = mapped_column(ForeignKey("section.id"), nullable=False)
    name: Mapped[str] = mapped_column(String(64), nullable=False)


class Account(Base):
    """Bank/brokerage account belonging to an individual or company."""

    __tablename__ = "account"

    __table_args__ = (UniqueConstraint("iban", name="uniq_iban"),)

    id: Mapped[int] = mapped_column(_ID_TYPE, primary_key=True, autoincrement=True)
    party_id: Mapped[int] = mapped_column(_ID_TYPE, ForeignKey("party.id"), nullable=False)
    account_type_code: Mapped[str] = mapped_column(String(32), ForeignKey("account_type.code"), nullable=False)
    currency_code: Mapped[str] = mapped_column(String(3), ForeignKey("currency.code"), nullable=False)
    name: Mapped[str | None] = mapped_column(String(120))
    iban: Mapped[str | None] = mapped_column(String(34))
    opened_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.current_timestamp()
    )
    closed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    journal_lines: Mapped[list["JournalLine"]] = relationship(
        back_populates="account", cascade="all, delete-orphan"
    )
    party: Mapped[Party] = relationship()
    party_roles: Mapped[list["AccountPartyRole"]] = relationship(
        back_populates="account", cascade="all, delete-orphan"
    )
    account_type_info: Mapped["AccountTypeLookup"] = relationship("AccountTypeLookup")
    currency_info: Mapped["Currency"] = relationship("Currency")

    type = synonym("account_type_code")
    currency = synonym("currency_code")


class AccountPartyRole(Base):
    """Role assignment of a party to an account."""

    __tablename__ = "account_party_role"
    __table_args__ = (UniqueConstraint("account_id", "party_id", "role_code", name="uq_account_party_role"),)

    id: Mapped[int] = mapped_column(_ID_TYPE, primary_key=True, autoincrement=True)
    account_id: Mapped[int] = mapped_column(_ID_TYPE, ForeignKey("account.id"), nullable=False)
    party_id: Mapped[int] = mapped_column(_ID_TYPE, ForeignKey("party.id"), nullable=False)
    role_code: Mapped[str] = mapped_column(String(32), ForeignKey("account_role.code"), nullable=False)
    start_date: Mapped[date | None] = mapped_column(Date)
    end_date: Mapped[date | None] = mapped_column(Date)
    is_primary: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default="0")

    account: Mapped[Account] = relationship(back_populates="party_roles")
    party: Mapped[Party] = relationship()
    role: Mapped[AccountRole] = relationship()


class JournalEntry(Base):
    """Double-entry journal header."""

    __tablename__ = "journal_entry"

    id: Mapped[int] = mapped_column(_ID_TYPE, primary_key=True, autoincrement=True)
    entry_code: Mapped[str] = mapped_column(String(64), unique=True, nullable=False)
    txn_date: Mapped[date] = mapped_column(Date, nullable=False)
    posted_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    description: Mapped[str | None] = mapped_column(String(255))
    channel_code: Mapped[str | None] = mapped_column(String(32), ForeignKey("txn_channel.code"))
    counterparty_party_id: Mapped[int | None] = mapped_column(ForeignKey("party.id"))
    transfer_reference: Mapped[str | None] = mapped_column(String(64))
    external_reference: Mapped[str | None] = mapped_column(String(64))
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.current_timestamp()
    )

    lines: Mapped[list["JournalLine"]] = relationship(
        back_populates="entry", cascade="all, delete-orphan"
    )
    channel: Mapped[TxnChannel | None] = relationship("TxnChannel")
    counterparty_party: Mapped[Party | None] = relationship(
        "Party", foreign_keys=[counterparty_party_id]
    )


class JournalLine(Base):
    """Double-entry journal line item."""

    __tablename__ = "journal_line"

    id: Mapped[int] = mapped_column(_ID_TYPE, primary_key=True, autoincrement=True)
    entry_id: Mapped[int] = mapped_column(_ID_TYPE, ForeignKey("journal_entry.id"), nullable=False)
    account_id: Mapped[int] = mapped_column(_ID_TYPE, ForeignKey("account.id"), nullable=False)
    party_id: Mapped[int | None] = mapped_column(_ID_TYPE, ForeignKey("party.id"))
    amount: Mapped[Decimal] = mapped_column(Numeric(18, 4), nullable=False)
    currency_code: Mapped[str] = mapped_column(String(3), ForeignKey("currency.code"), nullable=False)
    category_id: Mapped[int | None] = mapped_column(ForeignKey("category.id"))
    line_memo: Mapped[str | None] = mapped_column(String(255))
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.current_timestamp()
    )

    entry: Mapped[JournalEntry] = relationship(back_populates="lines")
    account: Mapped[Account] = relationship(back_populates="journal_lines")
    party: Mapped[Party | None] = relationship()
    category: Mapped[Category | None] = relationship()
