from __future__ import annotations
from decimal import Decimal

from sqlalchemy import BigInteger, Numeric
from sqlalchemy.orm import Mapped, mapped_column

from app.models.base import Base

class PositionAgg(Base):
    __tablename__ = "position_agg"  # maps to the DB view created in sql/schema.sql

    # Composite primary key so SQLAlchemy can map the view
    account_id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    instrument_id: Mapped[int] = mapped_column(BigInteger, primary_key=True)

    qty: Mapped[Decimal] = mapped_column(Numeric(18, 6))
    avg_cost: Mapped[Decimal] = mapped_column(Numeric(18, 6))
    last_price: Mapped[Decimal] = mapped_column(Numeric(18, 6))
    unrealized_pl: Mapped[Decimal] = mapped_column(Numeric(18, 6))