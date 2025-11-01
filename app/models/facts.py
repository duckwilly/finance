"""ORM models for aggregated fact tables used by dashboards."""
from __future__ import annotations

from datetime import date
from typing import TYPE_CHECKING

from sqlalchemy import BigInteger, Date, ForeignKey, Numeric, SmallInteger, String
from sqlalchemy.orm import Mapped, mapped_column, relationship

from .base import Base

if TYPE_CHECKING:  # pragma: no cover
    from app.models.memberships import EmploymentContract
    from app.models.party import Party
    from app.models.stocks import Instrument
    from app.models.transactions import Section


class ReportingPeriod(Base):
    """Calendar period used for summarised analytics (typically month granularity)."""

    __tablename__ = "reporting_period"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    period_start: Mapped[date] = mapped_column(Date, nullable=False)
    period_end: Mapped[date] = mapped_column(Date, nullable=False)
    label: Mapped[str] = mapped_column(String(64), unique=True, nullable=False)

    cash_flows: Mapped[list["CashFlowFact"]] = relationship(
        back_populates="reporting_period", cascade="all, delete-orphan"
    )
    payrolls: Mapped[list["PayrollFact"]] = relationship(
        back_populates="reporting_period", cascade="all, delete-orphan"
    )
    holding_performance: Mapped[list["HoldingPerformanceFact"]] = relationship(
        back_populates="reporting_period", cascade="all, delete-orphan"
    )


class PayrollFact(Base):
    """Summarised payroll information per employment contract and reporting period."""

    __tablename__ = "payroll_fact"

    reporting_period_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("reporting_period.id"), primary_key=True
    )
    contract_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("employment_contract.id"), primary_key=True
    )
    gross_amount: Mapped[float] = mapped_column(Numeric(18, 4), nullable=False)
    net_amount: Mapped[float] = mapped_column(Numeric(18, 4), nullable=False)
    taxes_withheld: Mapped[float] = mapped_column(Numeric(18, 4), nullable=False)

    reporting_period: Mapped[ReportingPeriod] = relationship(back_populates="payrolls")
    contract: Mapped["EmploymentContract"] = relationship()


class CashFlowFact(Base):
    """Aggregated cash-flow totals per party, section, and reporting period."""

    __tablename__ = "cash_flow_fact"

    reporting_period_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("reporting_period.id"), primary_key=True
    )
    party_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("party.id"), primary_key=True)
    section_id: Mapped[int] = mapped_column(SmallInteger, ForeignKey("section.id"), primary_key=True)
    inflow_amount: Mapped[float] = mapped_column(Numeric(18, 4), nullable=False, default=0)
    outflow_amount: Mapped[float] = mapped_column(Numeric(18, 4), nullable=False, default=0)
    net_amount: Mapped[float] = mapped_column(Numeric(18, 4), nullable=False, default=0)

    reporting_period: Mapped[ReportingPeriod] = relationship(back_populates="cash_flows")
    party: Mapped["Party"] = relationship("Party")
    section: Mapped[Section] = relationship()


class HoldingPerformanceFact(Base):
    """Summarised holding metrics per party and instrument for a reporting period."""

    __tablename__ = "holding_performance_fact"

    reporting_period_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("reporting_period.id"), primary_key=True
    )
    party_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("party.id"), primary_key=True)
    instrument_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("instrument.id"), primary_key=True)
    quantity: Mapped[float] = mapped_column(Numeric(18, 6), nullable=False, default=0)
    cost_basis: Mapped[float] = mapped_column(Numeric(18, 4), nullable=False, default=0)
    market_value: Mapped[float] = mapped_column(Numeric(18, 4), nullable=False, default=0)
    unrealized_pl: Mapped[float] = mapped_column(Numeric(18, 4), nullable=False, default=0)

    reporting_period: Mapped[ReportingPeriod] = relationship(back_populates="holding_performance")
    party: Mapped["Party"] = relationship("Party")
    instrument: Mapped["Instrument"] = relationship("Instrument")
