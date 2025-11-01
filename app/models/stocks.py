from __future__ import annotations
from datetime import date, datetime
from decimal import Decimal
from typing import TYPE_CHECKING

from sqlalchemy import (
    BigInteger,
    Date,
    DateTime,
    ForeignKey,
    Numeric,
    String,
    UniqueConstraint,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy.sql import func

from app.models.base import Base

if TYPE_CHECKING:  # pragma: no cover - imported for type checking only
    from app.models.transactions import Account


class InstrumentType(Base):
    """Lookup table for instrument types."""

    __tablename__ = "instrument_type"

    code: Mapped[str] = mapped_column(String(32), primary_key=True)
    description: Mapped[str] = mapped_column(String(128), nullable=False)


class Instrument(Base):
    """Financial product traded within the simulation."""

    __tablename__ = "instrument"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    instrument_type_code: Mapped[str] = mapped_column(
        String(32), ForeignKey("instrument_type.code"), nullable=False
    )
    symbol: Mapped[str] = mapped_column(String(32), nullable=False)
    name: Mapped[str] = mapped_column(String(160), nullable=False)
    primary_currency_code: Mapped[str] = mapped_column(String(3), ForeignKey("currency.code"), nullable=False)
    primary_market_id: Mapped[int | None] = mapped_column(ForeignKey("market.id"))
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.current_timestamp()
    )

    market: Mapped["Market | None"] = relationship("Market", back_populates="instruments")
    identifiers: Mapped[list["InstrumentIdentifier"]] = relationship(
        back_populates="instrument", cascade="all, delete-orphan"
    )
    price_quotes: Mapped[list["PriceQuote"]] = relationship(
        back_populates="instrument", cascade="all, delete-orphan"
    )
    trades: Mapped[list["Trade"]] = relationship(
        back_populates="instrument", cascade="all, delete-orphan"
    )
    holdings: Mapped[list["Holding"]] = relationship(
        back_populates="instrument", cascade="all, delete-orphan"
    )


class InstrumentIdentifier(Base):
    """External identifier associated with an instrument (ISIN, ticker, etc)."""

    __tablename__ = "instrument_identifier"

    __table_args__ = (UniqueConstraint("identifier_type", "identifier_value", name="uq_identifier"),)

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    instrument_id: Mapped[int] = mapped_column(ForeignKey("instrument.id"), nullable=False)
    identifier_type: Mapped[str] = mapped_column(String(32), nullable=False)
    identifier_value: Mapped[str] = mapped_column(String(64), nullable=False)

    instrument: Mapped[Instrument] = relationship(back_populates="identifiers")


class Market(Base):
    """Trading market/exchange metadata."""

    __tablename__ = "market"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    mic: Mapped[str] = mapped_column(String(10), unique=True, nullable=False)
    name: Mapped[str] = mapped_column(String(160), nullable=False)
    timezone: Mapped[str] = mapped_column(String(64), nullable=False)
    country_code: Mapped[str] = mapped_column(String(2), nullable=False)

    instruments: Mapped[list[Instrument]] = relationship(back_populates="market")


class PriceQuote(Base):
    """Historical price quotes for instruments (e.g., daily close)."""

    __tablename__ = "price_quote"

    instrument_id: Mapped[int] = mapped_column(
        ForeignKey("instrument.id"), primary_key=True
    )
    price_date: Mapped[date] = mapped_column(Date, primary_key=True)
    quote_type: Mapped[str] = mapped_column(String(16), primary_key=True)
    quote_value: Mapped[Decimal] = mapped_column(Numeric(18, 6), nullable=False)

    instrument: Mapped[Instrument] = relationship(back_populates="price_quotes")


class FxRateDaily(Base):
    """Daily FX rate between two currencies."""

    __tablename__ = "fx_rate_daily"

    base_currency_code: Mapped[str] = mapped_column("base", String(3), primary_key=True)
    quote_currency_code: Mapped[str] = mapped_column("quote", String(3), primary_key=True)
    rate_date: Mapped[date] = mapped_column(Date, primary_key=True)
    rate_value: Mapped[Decimal] = mapped_column("rate", Numeric(18, 10), nullable=False)


class Trade(Base):
    """Individual trade execution for an account and instrument."""

    __tablename__ = "trade"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    account_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("account.id"), nullable=False)
    instrument_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("instrument.id"), nullable=False)
    trade_time: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    side: Mapped[str] = mapped_column(String(8), nullable=False)
    quantity: Mapped[Decimal] = mapped_column("qty", Numeric(18, 6), nullable=False)
    price: Mapped[Decimal] = mapped_column(Numeric(18, 6), nullable=False)
    fees_amount: Mapped[Decimal] = mapped_column("fees", Numeric(18, 6), nullable=False, server_default="0")
    tax_amount: Mapped[Decimal] = mapped_column("tax", Numeric(18, 6), nullable=False, server_default="0")
    trade_currency_code: Mapped[str] = mapped_column("currency", String(3), nullable=False, server_default="EUR")
    settlement_date: Mapped[date | None] = mapped_column("settle_dt", Date)

    instrument: Mapped[Instrument] = relationship(back_populates="trades")
    account: Mapped["Account"] = relationship("Account")
    lots: Mapped[list["Lot"]] = relationship(
        back_populates="trade", cascade="all, delete-orphan"
    )


class Holding(Base):
    """Current position for an account/instrument combination."""

    __tablename__ = "holding"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    account_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("account.id"), nullable=False)
    instrument_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("instrument.id"), nullable=False)
    quantity: Mapped[Decimal] = mapped_column("qty", Numeric(18, 6), nullable=False, server_default="0")
    average_cost: Mapped[Decimal] = mapped_column("avg_cost", Numeric(18, 6), nullable=False, server_default="0")
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.current_timestamp()
    )

    instrument: Mapped[Instrument] = relationship(back_populates="holdings")
    account: Mapped["Account"] = relationship("Account")
    lots: Mapped[list["Lot"]] = relationship(
        back_populates="holding", cascade="all, delete-orphan"
    )


class Lot(Base):
    """Lot-level association between trades and holdings."""

    __tablename__ = "lot"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    holding_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("holding.id"), nullable=False)
    trade_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("trade.id"), nullable=False)
    quantity: Mapped[Decimal] = mapped_column("qty", Numeric(18, 6), nullable=False)
    cost_basis: Mapped[Decimal] = mapped_column(Numeric(18, 6), nullable=False)

    holding: Mapped[Holding] = relationship(back_populates="lots")
    trade: Mapped[Trade] = relationship(back_populates="lots")

class PositionAgg(Base):
    __tablename__ = "position_agg"  # maps to the DB view created in sql/schema.sql

    # Composite primary key so SQLAlchemy can map the view
    account_id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    instrument_id: Mapped[int] = mapped_column(BigInteger, primary_key=True)

    qty: Mapped[Decimal] = mapped_column(Numeric(18, 6))
    avg_cost: Mapped[Decimal] = mapped_column(Numeric(18, 6))
    last_price: Mapped[Decimal] = mapped_column(Numeric(18, 6))
    unrealized_pl: Mapped[Decimal] = mapped_column(Numeric(18, 6))
