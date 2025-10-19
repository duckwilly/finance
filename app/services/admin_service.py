"""Service implementation for admin tooling."""
from __future__ import annotations

from decimal import Decimal

from sqlalchemy import case, func, select
from sqlalchemy.orm import Session

from app.core.logger import get_logger
from app.models import Individual, Company, Transaction, TransactionDirection
from app.schemas.admin import AdminMetrics

LOGGER = get_logger(__name__)


class AdminService:
    """Service encapsulating administrator dashboard workflows."""

    def get_metrics(self, session: Session) -> AdminMetrics:
        """Return high level metrics for the administrative overview."""

        LOGGER.debug("Collecting admin metrics from the database")

        total_individuals = session.execute(select(func.count(Individual.id))).scalar_one()
        total_companies = session.execute(select(func.count(Company.id))).scalar_one()
        total_transactions = session.execute(select(func.count(Transaction.id))).scalar_one()

        first_transaction_at = session.execute(
            select(func.min(Transaction.posted_at))
        ).scalar_one()
        last_transaction_at = session.execute(select(func.max(Transaction.posted_at))).scalar_one()

        balance_expression = func.sum(
            case(
                (Transaction.direction == TransactionDirection.CREDIT, Transaction.amount),
                else_=-Transaction.amount,
            )
        )
        aum_result = session.execute(select(balance_expression)).scalar_one()
        total_aum = aum_result if aum_result is not None else Decimal("0")

        if not isinstance(total_aum, Decimal):
            total_aum = Decimal(total_aum)

        metrics = AdminMetrics(
            total_individuals=total_individuals,
            total_companies=total_companies,
            total_transactions=total_transactions,
            first_transaction_at=first_transaction_at,
            last_transaction_at=last_transaction_at,
            total_aum=total_aum,
        )

        LOGGER.debug("Admin metrics computed: %s", metrics)

        return metrics
