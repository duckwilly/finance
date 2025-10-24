"""Service implementation for company dashboards."""
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
    PositionAgg,
    Transaction,
    TransactionDirection,
    UserSalaryMonthly,
)
from app.schemas.companies import (
    AccountSummary,
    CategoryBreakdown,
    CompanyDashboard,
    CompanyProfile,
    PayrollEntry,
    SummaryMetrics,
    TransactionSummary,
)
from app.services.dashboard_helpers import fetch_category_breakdown

LOGGER = get_logger(__name__)


class CompaniesService:
    """Service that aggregates corporate level metrics."""

    DEFAULT_PERIOD_DAYS = 90

    def get_dashboard(self, session: Session, company_id: int) -> CompanyDashboard:
        """Return the dashboard payload for a company."""

        LOGGER.debug("Loading dashboard for company id=%s", company_id)

        company = session.get(Company, company_id)
        if not company:
            msg = f"Company {company_id} not found"
            LOGGER.warning(msg)
            raise ValueError(msg)

        latest_txn_date = session.execute(
            select(func.max(Transaction.txn_date))
            .join(Account, Transaction.account_id == Account.id)
            .where(
                Account.owner_type == AccountOwnerType.ORG,
                Account.owner_id == company_id,
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
                Account.owner_type == AccountOwnerType.ORG,
                Account.owner_id == company_id,
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
                Account.owner_type == AccountOwnerType.ORG,
                Account.owner_id == company_id,
            )
            .order_by(Instrument.symbol)
        )
        holding_rows = session.execute(holdings_query).all()

        holdings_total = Decimal("0")
        for row in holding_rows:
            qty = Decimal(row.qty or 0)
            last_price = Decimal(row.last_price or 0)
            holdings_total += qty * last_price

        total_account_balance = sum((account.balance for account in accounts), Decimal("0"))
        cash_balance = sum(
            (account.balance for account in accounts if account.type != AccountType.BROKERAGE.value),
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
                owner_type=AccountOwnerType.ORG,
                owner_id=company_id,
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
                owner_type=AccountOwnerType.ORG,
                owner_id=company_id,
                section_name="expense",
                direction=TransactionDirection.DEBIT,
                start_date=start_date,
            )
        ]

        period_income = sum((category.total for category in income_breakdown), Decimal("0"))
        period_expenses = sum((category.total for category in expense_breakdown), Decimal("0"))

        total_profit = Company.get_total_profit(session, company_id)
        employee_count = Company.get_employee_count(session, company_id)
        monthly_salary_cost = Company.get_monthly_salary_cost(session, company_id)

        summary = SummaryMetrics(
            net_worth=total_account_balance + holdings_total,
            cash_balance=cash_balance,
            holdings_value=holdings_total,
            period_income=period_income,
            period_expenses=period_expenses,
            net_cash_flow=period_income - period_expenses,
            total_profit=Decimal(total_profit or 0),
            employee_count=employee_count,
            monthly_salary_cost=Decimal(monthly_salary_cost or 0),
        )

        payroll_latest = (
            select(
                UserSalaryMonthly.user_id.label("user_id"),
                UserSalaryMonthly.salary_amount.label("salary"),
                func.row_number().over(
                    partition_by=UserSalaryMonthly.user_id,
                    order_by=(
                        UserSalaryMonthly.year.desc(),
                        UserSalaryMonthly.month.desc(),
                    ),
                ).label("rn"),
            )
            .where(UserSalaryMonthly.employer_org_id == company_id)
            .subquery()
        )

        payroll_query = (
            select(
                Individual.id,
                Individual.name,
                payroll_latest.c.salary,
            )
            .join(payroll_latest, payroll_latest.c.user_id == Individual.id)
            .where(payroll_latest.c.rn == 1)
            .order_by(payroll_latest.c.salary.desc())
            .limit(25)
        )
        payroll_rows = session.execute(payroll_query).all()

        payroll = [
            PayrollEntry(
                user_id=row.id,
                name=row.name,
                salary_amount=Decimal(row.salary or 0),
            )
            for row in payroll_rows
        ]

        dashboard = CompanyDashboard(
            profile=CompanyProfile(id=company.id, name=company.name),
            period_label=period_label,
            summary=summary,
            accounts=accounts,
            income_breakdown=income_breakdown,
            expense_breakdown=expense_breakdown,
            payroll=payroll,
        )

        LOGGER.debug("Dashboard assembled for company id=%s", company_id)
        return dashboard
