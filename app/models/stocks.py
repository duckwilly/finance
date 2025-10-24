from __future__ import annotations
from datetime import date
from decimal import Decimal
from enum import Enum

from sqlalchemy import BigInteger, Date, Enum as SQLEnum, ForeignKey, Numeric, String
from sqlalchemy.orm import Mapped, mapped_column

from app.models.base import Base


class InstrumentType(str, Enum):
    """Enumeration of supported financial instrument types."""

    EQUITY = "EQUITY"
    ETF = "ETF"


class Instrument(Base):
    """Financial product traded within the simulation."""

    __tablename__ = "instrument"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    symbol: Mapped[str] = mapped_column(String(32), nullable=False)
    name: Mapped[str] = mapped_column(String(160), nullable=False)
    type: Mapped[InstrumentType] = mapped_column(
        SQLEnum(InstrumentType, native_enum=False), nullable=False
    )
    isin: Mapped[str | None] = mapped_column(String(16), unique=True)
    mic: Mapped[str | None] = mapped_column(String(10))
    currency: Mapped[str] = mapped_column(String(3), nullable=False, server_default="EUR")


class PriceDaily(Base):
    """Historical end-of-day price per instrument."""

    __tablename__ = "price_daily"

    instrument_id: Mapped[int] = mapped_column(
        ForeignKey("instrument.id"), primary_key=True
    )
    price_date: Mapped[date] = mapped_column(Date, primary_key=True)
    close_price: Mapped[Decimal] = mapped_column(Numeric(18, 6), nullable=False)
    currency: Mapped[str] = mapped_column(String(3), nullable=False)

class PositionAgg(Base):
    __tablename__ = "position_agg"  # maps to the DB view created in sql/schema.sql

    # Composite primary key so SQLAlchemy can map the view
    account_id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    instrument_id: Mapped[int] = mapped_column(BigInteger, primary_key=True)

    qty: Mapped[Decimal] = mapped_column(Numeric(18, 6))
    avg_cost: Mapped[Decimal] = mapped_column(Numeric(18, 6))
    last_price: Mapped[Decimal] = mapped_column(Numeric(18, 6))
    unrealized_pl: Mapped[Decimal] = mapped_column(Numeric(18, 6))
