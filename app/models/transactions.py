"""ORM models for financial accounts and transactions."""
from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from enum import Enum

from sqlalchemy import (
    BigInteger,
    CheckConstraint,
    Date,
    DateTime,
    Enum as SQLEnum,
    ForeignKey,
    Integer,
    Numeric,
    SmallInteger,
    String,
    UniqueConstraint,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy.sql import func

from .base import Base

_ID_TYPE = BigInteger().with_variant(Integer, "sqlite")
_SMALL_ID_TYPE = SmallInteger().with_variant(Integer, "sqlite")


class AccountOwnerType(str, Enum):
    """Enumeration of account owner entity types."""

    USER = "user"
    ORG = "org"


class AccountType(str, Enum):
    """Enumeration of supported account types."""

    CHECKING = "checking"
    SAVINGS = "savings"
    BROKERAGE = "brokerage"
    OPERATING = "operating"


class TransactionDirection(str, Enum):
    """Direction of a booked transaction."""

    DEBIT = "DEBIT"
    CREDIT = "CREDIT"


class TransactionChannel(str, Enum):
    """Channel or rail used for the transaction."""

    SEPA = "SEPA"
    CARD = "CARD"
    WIRE = "WIRE"
    CASH = "CASH"
    INTERNAL = "INTERNAL"


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


class Counterparty(Base):
    """External counterparty information."""

    __tablename__ = "counterparty"

    id: Mapped[int] = mapped_column(_ID_TYPE, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(160), nullable=False)
    account_ref: Mapped[str | None] = mapped_column(String(64))
    bic: Mapped[str | None] = mapped_column(String(11))
    country_code: Mapped[str | None] = mapped_column(String(2))


class Account(Base):
    """Bank/brokerage account belonging to an individual or company."""

    __tablename__ = "account"
    __table_args__ = (
        UniqueConstraint("iban", name="uniq_iban"),
    )

    id: Mapped[int] = mapped_column(_ID_TYPE, primary_key=True, autoincrement=True)
    owner_type: Mapped[AccountOwnerType] = mapped_column(
        SQLEnum(AccountOwnerType, native_enum=False), nullable=False
    )
    owner_id: Mapped[int] = mapped_column(_ID_TYPE, nullable=False)
    type: Mapped[AccountType] = mapped_column(SQLEnum(AccountType, native_enum=False), nullable=False)
    currency: Mapped[str] = mapped_column(String(3), nullable=False, server_default="EUR")
    name: Mapped[str | None] = mapped_column(String(120))
    iban: Mapped[str | None] = mapped_column(String(34))
    opened_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.current_timestamp()
    )
    closed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    transactions: Mapped[list["Transaction"]] = relationship(
        back_populates="account", cascade="all, delete-orphan"
    )


class Transaction(Base):
    """Posted transaction statement line."""

    __tablename__ = "transaction"
    __table_args__ = (
        CheckConstraint("amount > 0", name="ck_transaction_amount_positive"),
    )

    id: Mapped[int] = mapped_column(_ID_TYPE, primary_key=True, autoincrement=True)
    account_id: Mapped[int] = mapped_column(ForeignKey("account.id"), nullable=False, index=True)
    posted_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    txn_date: Mapped[date] = mapped_column(Date, nullable=False)
    amount: Mapped[Decimal] = mapped_column(Numeric(18, 4), nullable=False)
    currency: Mapped[str] = mapped_column(String(3), nullable=False, server_default="EUR")
    direction: Mapped[TransactionDirection] = mapped_column(
        SQLEnum(TransactionDirection, native_enum=False), nullable=False
    )
    section_id: Mapped[int] = mapped_column(ForeignKey("section.id"), nullable=False)
    category_id: Mapped[int | None] = mapped_column(ForeignKey("category.id"))
    channel: Mapped[TransactionChannel] = mapped_column(
        SQLEnum(TransactionChannel, native_enum=False), nullable=False, server_default=TransactionChannel.SEPA.value
    )
    description: Mapped[str | None] = mapped_column(String(255))
    counterparty_id: Mapped[int | None] = mapped_column(ForeignKey("counterparty.id"))
    transfer_group_id: Mapped[str | None] = mapped_column(String(64))
    ext_reference: Mapped[str | None] = mapped_column(String(64))
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.current_timestamp(), nullable=False
    )

    account: Mapped[Account] = relationship(back_populates="transactions")
    section: Mapped[Section] = relationship()
    category: Mapped[Category | None] = relationship()
    counterparty: Mapped[Counterparty | None] = relationship()
