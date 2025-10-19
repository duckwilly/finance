"""Service implementation for admin tooling."""
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
    Membership,
    PositionAgg,
    Section,
    Transaction,
    TransactionDirection,
)
from app.schemas.admin import AdminMetrics, ListView, ListViewColumn, ListViewRow

LOGGER = get_logger(__name__)


class AdminService:
    """Service encapsulating administrator dashboard workflows."""

    def get_metrics(self, session: Session) -> AdminMetrics:
        """Return high level metrics for the administrative overview."""

        LOGGER.debug("Collecting admin metrics from the database")

        total_individuals = session.execute(select(func.count(Individual.id))).scalar_one()
        total_companies = session.execute(select(func.count(Company.id))).scalar_one()
        total_transactions = session.execute(select(func.count(Transaction.id))).scalar_one()

        first_transaction_at = session.execute(select(func.min(Transaction.posted_at))).scalar_one()
        last_transaction_at = session.execute(select(func.max(Transaction.posted_at))).scalar_one()

        # Cash balance across all accounts
        cash_total = session.execute(
            select(
                func.sum(
                    case(
                        (Transaction.direction == TransactionDirection.CREDIT, Transaction.amount),
                        else_=-Transaction.amount,
                    )
                )
            )
        ).scalar_one() or Decimal("0")

        # Stock holdings total
        holdings_total = session.execute(
            select(func.coalesce(func.sum(PositionAgg.qty * PositionAgg.last_price), 0))
        ).scalar_one() or Decimal("0")

        total_aum = Decimal(cash_total) + Decimal(holdings_total)

        metrics = AdminMetrics(
            total_individuals=total_individuals,
            total_companies=total_companies,
            total_transactions=total_transactions,
            first_transaction_at=first_transaction_at,
            last_transaction_at=last_transaction_at,
            total_aum=total_aum,
            total_cash=cash_total,
            total_holdings=holdings_total,
        )

        LOGGER.debug("Admin metrics computed: %s", metrics)

        return metrics

    def get_individual_overview(self, session: Session) -> ListView:
        """Return a reusable list view model for individual users."""

        LOGGER.debug("Collecting individual overview data for admin dashboard")

        # Determine the time window for monthly income (rolling 30 days)
        income_cutoff = date.today() - timedelta(days=30)

        income_subquery = (
            select(
                Account.owner_id.label("user_id"),
                func.coalesce(
                    func.sum(
                        case(
                            (Transaction.direction == TransactionDirection.CREDIT, Transaction.amount),
                            else_=-Transaction.amount,
                        )
                    ),
                    0,
                ).label("monthly_income"),
            )
            .join(Transaction, Transaction.account_id == Account.id)
            .join(Section, Section.id == Transaction.section_id)
            .where(
                Account.owner_type == AccountOwnerType.USER,
                Section.name == "income",
                Transaction.txn_date >= income_cutoff,
            )
            .group_by(Account.owner_id)
            .subquery()
        )

        balance_cte = (
            select(
                Account.owner_id.label("user_id"),
                Account.type.label("account_type"),
                func.coalesce(
                    func.sum(
                        case(
                            (Transaction.direction == TransactionDirection.CREDIT, Transaction.amount),
                            else_=-Transaction.amount,
                        )
                    ),
                    0,
                ).label("balance"),
            )
            .where(Account.owner_type == AccountOwnerType.USER)
            .join(Transaction, Transaction.account_id == Account.id, isouter=True)
            .group_by(Account.owner_id, Account.type)
            .cte("account_balances")
        )

        balance_totals = (
            select(
                balance_cte.c.user_id,
                func.coalesce(
                    func.sum(
                        case(
                            (balance_cte.c.account_type == AccountType.CHECKING.value, balance_cte.c.balance),
                            else_=0,
                        )
                    ),
                    0,
                ).label("checking_balance"),
                func.coalesce(
                    func.sum(
                        case(
                            (balance_cte.c.account_type == AccountType.SAVINGS.value, balance_cte.c.balance),
                            else_=0,
                        )
                    ),
                    0,
                ).label("savings_balance"),
                func.coalesce(
                    func.sum(
                        case(
                            (balance_cte.c.account_type == AccountType.BROKERAGE.value, balance_cte.c.balance),
                            else_=0,
                        )
                    ),
                    0,
                ).label("brokerage_balance"),
            )
            .group_by(balance_cte.c.user_id)
            .subquery()
        )


        overview_query = (
            select(
                Individual.id.label("individual_id"),
                Individual.name,
                func.coalesce(income_subquery.c.monthly_income, 0).label("monthly_income"),
                func.coalesce(balance_totals.c.checking_balance, 0).label("checking_balance"),
                func.coalesce(balance_totals.c.savings_balance, 0).label("savings_balance"),
                func.coalesce(balance_totals.c.brokerage_balance, 0).label("brokerage_balance"),
            )
            .outerjoin(income_subquery, income_subquery.c.user_id == Individual.id)
            .outerjoin(balance_totals, balance_totals.c.user_id == Individual.id)
            .order_by(Individual.name)
        )

        rows = []
        for record in session.execute(overview_query).all():
            employer = Membership.find_employer_for_user(session, record.individual_id)
            employer_name = employer.name if employer else None
            
            search_terms = [record.name]
            if employer_name:
                search_terms.append(employer_name)

            rows.append(
                ListViewRow(
                    key=str(record.individual_id),
                    values={
                        "name": record.name,
                        "employer": employer_name,
                        "monthly_income": record.monthly_income,
                        "checking_aum": record.checking_balance,
                        "savings_aum": record.savings_balance,
                        "brokerage_aum": record.brokerage_balance,
                    },
                    search_text=" ".join(filter(None, search_terms)).lower(),
                )
            )

        list_view = ListView(
            title="Individual users",
            columns=[
                ListViewColumn(key="name", title="Name"),
                ListViewColumn(key="employer", title="Employer"),
                ListViewColumn(key="monthly_income", title="Monthly income", column_type="currency", align="right"),
                ListViewColumn(key="checking_aum", title="Checking AUM", column_type="currency", align="right"),
                ListViewColumn(key="savings_aum", title="Savings AUM", column_type="currency", align="right"),
                ListViewColumn(key="brokerage_aum", title="Brokerage AUM", column_type="currency", align="right"),
            ],
            rows=rows,
            search_placeholder="Search individuals",
            empty_message="No individual users found.",
        )

        LOGGER.debug("Prepared %d individual overview rows", len(rows))

        return list_view
