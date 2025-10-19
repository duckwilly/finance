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
from app.services.stocks_service import brokerage_aum_select
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

        # Find the most recent salary transaction per user to get their monthly income
        latest_salary_subquery = (
            select(
                Account.owner_id.label("user_id"),
                Transaction.amount.label("salary_amount"),
                func.row_number().over(
                    partition_by=Account.owner_id,
                    order_by=Transaction.txn_date.desc()
                ).label("rn")
            )
            .select_from(Account)
            .join(Transaction, Transaction.account_id == Account.id)
            .join(Section, Section.id == Transaction.section_id)
            .where(
                Account.owner_type == AccountOwnerType.USER,
                Section.name == "income",
                Transaction.direction == TransactionDirection.CREDIT,
            )
            .subquery()
        )

        income_subquery = (
            select(
                latest_salary_subquery.c.user_id,
                func.coalesce(latest_salary_subquery.c.salary_amount, 0).label("monthly_income"),
            )
            .select_from(latest_salary_subquery)
            .where(latest_salary_subquery.c.rn == 1)
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
            .select_from(Account)
            .join(Transaction, Transaction.account_id == Account.id, isouter=True)
            .where(
                Account.owner_type == AccountOwnerType.USER,
                Account.type.in_([AccountType.CHECKING, AccountType.SAVINGS]),
            )
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
            )
            .group_by(balance_cte.c.user_id)
            .subquery()
        )

        brokerage_aum_cte = brokerage_aum_select(AccountOwnerType.USER).subquery()


        overview_query = (
            select(
                Individual.id.label("individual_id"),
                Individual.name,
                func.coalesce(income_subquery.c.monthly_income, 0).label("monthly_income"),
                func.coalesce(balance_totals.c.checking_balance, 0).label("checking_balance"),
                func.coalesce(balance_totals.c.savings_balance, 0).label("savings_balance"),
                func.coalesce(brokerage_aum_cte.c.brokerage_aum, 0).label("brokerage_aum"),
            )
            .select_from(Individual)
            .outerjoin(income_subquery, income_subquery.c.user_id == Individual.id)
            .outerjoin(balance_totals, balance_totals.c.user_id == Individual.id)
            .outerjoin(brokerage_aum_cte, brokerage_aum_cte.c.owner_id == Individual.id)
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
                        "brokerage_aum": record.brokerage_aum,
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

    def get_company_overview(self, session: Session) -> ListView:
        """Return a reusable list view model for corporate users."""

        LOGGER.debug("Collecting company overview data for admin dashboard")

        # Query all companies
        companies_query = select(Company.id, Company.name).order_by(Company.name)
        companies = session.execute(companies_query).all()

        rows = []
        for company in companies:
            # Get metrics for this company
            employee_count = Company.get_employee_count(session, company.id)
            monthly_salary_cost = Company.get_monthly_salary_cost(session, company.id)
            monthly_income = Company.get_monthly_income(session, company.id)
            monthly_expenses = Company.get_monthly_expenses(session, company.id)
            profit_total = Company.get_total_profit(session, company.id)

            rows.append(
                ListViewRow(
                    key=str(company.id),
                    values={
                        "name": company.name,
                        "employee_count": employee_count,
                        "monthly_salary_cost": monthly_salary_cost,
                        "monthly_income": monthly_income,
                        "monthly_expenses": monthly_expenses,
                        "profit_total": profit_total,
                    },
                    search_text=company.name.lower(),
                )
            )

        list_view = ListView(
            title="Corporate users",
            columns=[
                ListViewColumn(key="name", title="Company name"),
                ListViewColumn(key="employee_count", title="Employees", align="right"),
                ListViewColumn(key="monthly_salary_cost", title="Monthly salary cost", column_type="currency", align="right"),
                ListViewColumn(key="monthly_income", title="Monthly income", column_type="currency", align="right"),
                ListViewColumn(key="monthly_expenses", title="Monthly expenses", column_type="currency", align="right"),
                ListViewColumn(key="profit_total", title="Profit YTD", column_type="currency", align="right"),
            ],
            rows=rows,
            search_placeholder="Search companies",
            empty_message="No corporate users found.",
        )

        LOGGER.debug("Prepared %d company overview rows", len(rows))

        return list_view
