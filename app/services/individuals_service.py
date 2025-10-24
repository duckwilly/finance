"""Service implementation for individual client dashboards."""
from __future__ import annotations

from datetime import date, timedelta
from decimal import Decimal

from sqlalchemy import case, func, select
from sqlalchemy.orm import Session

from app.core.logger import get_logger
from app.models import (
    Account,
    AccountOwnerType,
    AccountType,
    Company,
    Individual,
    Instrument,
    Membership,
    PositionAgg,
    Transaction,
    TransactionDirection,
)
from app.schemas.individuals import (
    AccountSummary,
    CategoryBreakdown,
    HoldingSummary,
    IndividualDashboard,
    IndividualProfile,
    SummaryMetrics,
    TransactionSummary,
)
from app.services.dashboard_helpers import fetch_category_breakdown

LOGGER = get_logger(__name__)


class IndividualsService:
    """Service exposing data needed to render individual dashboards."""

    DEFAULT_PERIOD_DAYS = 90

    def get_dashboard(self, session: Session, user_id: int) -> IndividualDashboard:
        """Collect aggregated data for the individual dashboard."""

        LOGGER.debug("Loading dashboard for individual id=%s", user_id)

        individual = session.get(Individual, user_id)
        if not individual:
            msg = f"Individual {user_id} not found"
            LOGGER.warning(msg)
            raise ValueError(msg)

        latest_txn_date = session.execute(
            select(func.max(Transaction.txn_date))
            .join(Account, Transaction.account_id == Account.id)
            .where(
                Account.owner_type == AccountOwnerType.USER,
                Account.owner_id == user_id,
            )
        ).scalar_one_or_none()

        if latest_txn_date:
            start_date = latest_txn_date - timedelta(days=self.DEFAULT_PERIOD_DAYS)
            period_label = (
                f"Last {self.DEFAULT_PERIOD_DAYS} days ending {latest_txn_date.isoformat()}"
            )
        else:
            start_date = date.today() - timedelta(days=self.DEFAULT_PERIOD_DAYS)
            period_label = f"Last {self.DEFAULT_PERIOD_DAYS} days"

        LOGGER.debug(
            "Dashboard period start=%s end=%s", start_date.isoformat(),
            latest_txn_date.isoformat() if latest_txn_date else "today",
        )

        balance_case = func.coalesce(
            func.sum(
                case(
                    (Transaction.direction == TransactionDirection.CREDIT, Transaction.amount),
                    else_=-Transaction.amount,
                )
            ),
            0,
        ).label("balance")

        accounts_query = (
            select(
                Account.id,
                Account.name,
                Account.type,
                Account.currency,
                balance_case,
            )
            .outerjoin(Transaction, Transaction.account_id == Account.id)
            .where(
                Account.owner_type == AccountOwnerType.USER,
                Account.owner_id == user_id,
            )
            .group_by(Account.id)
            .order_by(Account.type, Account.name)
        )
        account_rows = session.execute(accounts_query).all()

        accounts = [
            AccountSummary(
                id=row.id,
                name=row.name,
                type=row.type.value if isinstance(row.type, AccountType) else str(row.type),
                currency=row.currency,
                balance=Decimal(row.balance or 0),
            )
            for row in account_rows
        ]

        holdings_query = (
            select(
                Instrument.symbol,
                Instrument.name,
                PositionAgg.qty,
                PositionAgg.last_price,
                PositionAgg.unrealized_pl,
            )
            .join(Account, Account.id == PositionAgg.account_id)
            .join(Instrument, Instrument.id == PositionAgg.instrument_id)
            .where(
                Account.owner_type == AccountOwnerType.USER,
                Account.owner_id == user_id,
            )
            .order_by(Instrument.symbol)
        )
        holding_rows = session.execute(holdings_query).all()

        holdings: list[HoldingSummary] = []
        holdings_total = Decimal("0")
        for row in holding_rows:
            qty = Decimal(row.qty or 0)
            last_price = Decimal(row.last_price or 0)
            market_value = qty * last_price
            holdings_total += market_value
            holdings.append(
                HoldingSummary(
                    instrument_symbol=row.symbol,
                    instrument_name=row.name,
                    quantity=qty,
                    last_price=last_price,
                    market_value=market_value,
                    unrealized_pl=Decimal(row.unrealized_pl or 0),
                )
            )

        total_account_balance = sum((account.balance for account in accounts), Decimal("0"))
        cash_balance = sum(
            (account.balance for account in accounts if account.type in {
                AccountType.CHECKING.value,
                AccountType.SAVINGS.value,
            }),
            Decimal("0"),
        )

        income_breakdown = [
            CategoryBreakdown(
                name=name,
                total=total,
                transactions=[
                    TransactionSummary(
                        txn_date=txn_date,
                        description=description,
                        amount=amount,
                    )
                    for txn_date, description, amount in transactions
                ],
            )
            for name, total, transactions in fetch_category_breakdown(
                session,
                owner_type=AccountOwnerType.USER,
                owner_id=user_id,
                section_name="income",
                direction=TransactionDirection.CREDIT,
                start_date=start_date,
            )
        ]
        expense_breakdown = [
            CategoryBreakdown(
                name=name,
                total=total,
                transactions=[
                    TransactionSummary(
                        txn_date=txn_date,
                        description=description,
                        amount=amount,
                    )
                    for txn_date, description, amount in transactions
                ],
            )
            for name, total, transactions in fetch_category_breakdown(
                session,
                owner_type=AccountOwnerType.USER,
                owner_id=user_id,
                section_name="expense",
                direction=TransactionDirection.DEBIT,
                start_date=start_date,
            )
        ]

        total_income = sum((category.total for category in income_breakdown), Decimal("0"))
        total_expenses = sum((category.total for category in expense_breakdown), Decimal("0"))

        summary = SummaryMetrics(
            net_worth=total_account_balance + holdings_total,
            cash_balance=cash_balance,
            holdings_value=holdings_total,
            period_income=total_income,
            period_expenses=total_expenses,
            net_cash_flow=total_income - total_expenses,
        )

        employer_name = session.execute(
            select(Company.name)
            .join(Membership, Membership.org_id == Company.id)
            .where(Membership.user_id == user_id, Membership.is_primary.is_(True))
            .limit(1)
        ).scalar_one_or_none()

        dashboard = IndividualDashboard(
            profile=IndividualProfile(
                id=individual.id,
                name=individual.name,
                email=individual.email,
                job_title=individual.job_title,
            ),
            employer_name=employer_name,
            summary=summary,
            period_label=period_label,
            accounts=accounts,
            income_breakdown=income_breakdown,
            expense_breakdown=expense_breakdown,
            holdings=holdings,
        )

        LOGGER.debug("Dashboard assembled for individual id=%s", user_id)
        return dashboard

