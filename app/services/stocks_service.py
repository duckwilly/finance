"""Service implementation and helpers for stock analytics."""
from __future__ import annotations

from collections.abc import Sequence
from decimal import Decimal

from sqlalchemy import func, select
from sqlalchemy.orm import Session
from sqlalchemy.sql import Select

from app.core.logger import get_logger
from app.models import Account, AccountOwnerType, AccountType, PositionAgg

LOGGER = get_logger(__name__)


def brokerage_aum_select(
    owner_type: AccountOwnerType,
    *,
    owner_ids: Sequence[int] | None = None,
    label: str = "brokerage_aum",
) -> Select:
    """Return a selectable for brokerage AUM grouped by account owner."""

    query = (
        select(
            Account.owner_id.label("owner_id"),
            func.coalesce(func.sum(PositionAgg.qty * PositionAgg.last_price), 0).label(label),
        )
        .join(PositionAgg, PositionAgg.account_id == Account.id, isouter=True)
        .where(
            Account.owner_type == owner_type,
            Account.type == AccountType.BROKERAGE,
        )
        .group_by(Account.owner_id)
    )

    if owner_ids:
        query = query.where(Account.owner_id.in_(owner_ids))

    return query


def brokerage_aum_by_owner(
    session: Session, owner_type: AccountOwnerType, *, owner_ids: Sequence[int] | None = None
) -> dict[int, Decimal]:
    """Materialise the brokerage AUM mapping for the requested owners."""

    result = session.execute(brokerage_aum_select(owner_type, owner_ids=owner_ids))
    aum_by_owner: dict[int, Decimal] = {}
    for row in result:
        value = row.brokerage_aum
        if value is None:
            value = Decimal(0)
        aum_by_owner[row.owner_id] = Decimal(value)
    return aum_by_owner


class StocksService:
    """Service skeleton for aggregate stock analytics."""

    def summarise_holdings(self, *_args: object, **_kwargs: object) -> None:
        LOGGER.debug("StocksService.summarise_holdings called")
        raise NotImplementedError("Stock analysis logic not yet implemented.")
