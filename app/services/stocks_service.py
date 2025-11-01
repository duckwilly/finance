"""Service implementation and helpers for stock analytics."""
from __future__ import annotations

from collections.abc import Sequence
from datetime import date, datetime, time
from decimal import Decimal

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.core.logger import get_logger
from app.models import (
    Account,
    AccountType,
    HoldingPerformanceFact,
    Instrument,
    InstrumentIdentifier,
    PositionAgg,
    ReportingPeriod,
)
from app.schemas.stocks import (
    HoldingPosition,
    InstrumentIdentifierPayload,
    InstrumentSnapshot,
)

LOGGER = get_logger(__name__)


def brokerage_aum_by_party(
    session: Session,
    *,
    party_ids: Sequence[int] | None = None,
) -> dict[int, Decimal]:
    """Return brokerage assets under management keyed by party id."""

    if not party_ids:
        return {}

    result = session.execute(
        select(
            Account.party_id.label("party_id"),
            func.coalesce(func.sum(PositionAgg.qty * PositionAgg.last_price), 0).label("aum"),
        )
        .join(PositionAgg, PositionAgg.account_id == Account.id, isouter=True)
        .where(
            Account.party_id.in_(list(party_ids)),
            Account.account_type_code == AccountType.BROKERAGE.value,
        )
        .group_by(Account.party_id)
    )
    return {row.party_id: Decimal(row.aum or 0) for row in result}


class StocksService:
    """Service for retrieving holdings and market analytics."""

    DEFAULT_LIMIT = 50

    def summarise_holdings(
        self,
        session: Session,
        *,
        party_id: int,
        reporting_period_id: int | None = None,
        limit: int | None = None,
    ) -> list[HoldingPosition]:
        """Return holdings for ``party_id`` using the latest reporting period.

        When ``reporting_period_id`` is omitted the most recent period containing
        holdings for the party is selected automatically. Results are ordered by
        market value descending and capped by ``limit`` (defaults to
        :data:`DEFAULT_LIMIT`).
        """

        LOGGER.debug(
            "Loading holdings summary for party_id=%s reporting_period_id=%s",
            party_id,
            reporting_period_id,
        )

        target_period_id = reporting_period_id
        period_end: date | None = None
        if target_period_id is None:
            period_row = session.execute(
                select(ReportingPeriod.id, ReportingPeriod.period_end)
                .join(
                    HoldingPerformanceFact,
                    HoldingPerformanceFact.reporting_period_id == ReportingPeriod.id,
                )
                .where(HoldingPerformanceFact.party_id == party_id)
                .order_by(ReportingPeriod.period_end.desc())
                .limit(1)
            ).first()
            if period_row:
                target_period_id = period_row.id
                period_end = period_row.period_end

        if target_period_id is None:
            LOGGER.debug("No holdings found for party_id=%s", party_id)
            return []

        if period_end is None:
            period_end = session.execute(
                select(ReportingPeriod.period_end).where(ReportingPeriod.id == target_period_id)
            ).scalar_one_or_none()

        query_limit = limit or self.DEFAULT_LIMIT

        holdings_rows = session.execute(
            select(
                HoldingPerformanceFact.instrument_id,
                HoldingPerformanceFact.quantity,
                HoldingPerformanceFact.cost_basis,
                HoldingPerformanceFact.market_value,
                HoldingPerformanceFact.unrealized_pl,
                Instrument.id,
                Instrument.symbol,
                Instrument.name,
                Instrument.instrument_type_code,
                Instrument.primary_currency_code,
                Instrument.primary_market_id,
            )
            .join(Instrument, Instrument.id == HoldingPerformanceFact.instrument_id)
            .where(
                HoldingPerformanceFact.party_id == party_id,
                HoldingPerformanceFact.reporting_period_id == target_period_id,
            )
            .order_by(HoldingPerformanceFact.market_value.desc())
            .limit(query_limit)
        ).all()

        if not holdings_rows:
            LOGGER.debug(
                "Reporting period %s contained no holdings for party_id=%s",
                target_period_id,
                party_id,
            )
            return []

        instrument_ids = {row.instrument_id for row in holdings_rows}

        identifier_rows = []
        if instrument_ids:
            identifier_rows = session.execute(
                select(
                    InstrumentIdentifier.instrument_id,
                    InstrumentIdentifier.identifier_type,
                    InstrumentIdentifier.identifier_value,
                )
                .where(InstrumentIdentifier.instrument_id.in_(instrument_ids))
            ).all()

        identifiers_by_instrument: dict[int, list[InstrumentIdentifierPayload]] = {
            instrument_id: [] for instrument_id in instrument_ids
        }
        for row in identifier_rows:
            identifiers_by_instrument.setdefault(row.instrument_id, []).append(
                InstrumentIdentifierPayload(type=row.identifier_type, value=row.identifier_value)
            )

        holdings: list[HoldingPosition] = []

        as_of_timestamp = None
        if period_end is not None:
            as_of_timestamp = datetime.combine(period_end, time.min)

        for row in holdings_rows:
            quantity = Decimal(row.quantity or 0)
            cost_basis = Decimal(row.cost_basis or 0)
            market_value = Decimal(row.market_value or 0)
            unrealized_pl = Decimal(row.unrealized_pl or 0)

            average_cost = Decimal("0")
            if quantity != 0:
                average_cost = cost_basis / quantity

            instrument_snapshot = InstrumentSnapshot(
                id=row.id,
                symbol=row.symbol,
                name=row.name,
                instrument_type_code=row.instrument_type_code,
                primary_currency_code=row.primary_currency_code,
                primary_market_id=row.primary_market_id,
                identifiers=identifiers_by_instrument.get(row.instrument_id, []),
            )

            holdings.append(
                HoldingPosition(
                    id=row.instrument_id,
                    instrument_id=row.instrument_id,
                    quantity=quantity,
                    average_cost=average_cost,
                    updated_at=as_of_timestamp or datetime.utcnow(),
                    instrument=instrument_snapshot,
                    lots=None,
                    market_value=market_value,
                    unrealized_pl=unrealized_pl,
                )
            )

        LOGGER.debug(
            "Returning %d holdings for party_id=%s reporting_period_id=%s",
            len(holdings),
            party_id,
            target_period_id,
        )

        return holdings
