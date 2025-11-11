"""Service implementation for company dashboards."""
from __future__ import annotations

from datetime import date, timedelta
from decimal import Decimal

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.core.logger import get_logger
from app.models import (
    Account,
    AccountType,
    CashFlowFact,
    EmploymentContract,
    HoldingPerformanceFact,
    JournalEntry,
    JournalLine,
    OrgPartyMap,
    PayrollFact,
    ReportingPeriod,
    Section,
    UserPartyMap,
)
from app.models.party import CompanyProfile as CompanyProfileModel, Party
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

        company_party_id = session.execute(
            select(OrgPartyMap.party_id).where(OrgPartyMap.org_id == company_id)
        ).scalar_one_or_none()

        if company_party_id is None:
            company_party_id = company_id

        party = session.get(Party, company_party_id)
        company_profile_row = session.get(CompanyProfileModel, company_party_id)

        if not party or not company_profile_row:
            msg = f"Company {company_id} not found"
            LOGGER.warning(msg)
            raise ValueError(msg)

        company_name = party.display_name or company_profile_row.legal_name or f"Company {company_id}"

        latest_cash_period = None
        if company_party_id:
            latest_cash_period = session.execute(
                select(ReportingPeriod)
                .join(CashFlowFact, CashFlowFact.reporting_period_id == ReportingPeriod.id)
                .where(CashFlowFact.party_id == company_party_id)
                .order_by(ReportingPeriod.period_end.desc())
                .limit(1)
            ).scalar_one_or_none()

        latest_entry_date = None
        if company_party_id:
            latest_entry_date = session.execute(
                select(func.max(JournalEntry.txn_date))
                .join(JournalLine, JournalLine.entry_id == JournalEntry.id)
                .join(Account, Account.id == JournalLine.account_id)
                .where(Account.party_id == company_party_id)
            ).scalar_one_or_none()

        if latest_cash_period:
            start_date = latest_cash_period.period_start
            period_label = latest_cash_period.label
        else:
            reference_end = latest_entry_date or date.today()
            start_date = reference_end - timedelta(days=self.DEFAULT_PERIOD_DAYS)
            if latest_entry_date:
                period_label = (
                    f"Last {self.DEFAULT_PERIOD_DAYS} days ending {latest_entry_date.isoformat()}"
                )
            else:
                period_label = f"Last {self.DEFAULT_PERIOD_DAYS} days"


        accounts: list[AccountSummary] = []
        total_account_balance = Decimal("0")
        cash_balance = Decimal("0")

        if company_party_id:
            balance_case = func.coalesce(func.sum(JournalLine.amount), 0).label("balance")

            accounts_query = (
                select(
                    Account.id,
                    Account.name,
                    Account.account_type_code.label("account_type"),
                    Account.currency_code.label("currency"),
                    balance_case,
                )
                .outerjoin(JournalLine, JournalLine.account_id == Account.id)
                .where(Account.party_id == company_party_id)
                .group_by(Account.id)
                .order_by(Account.account_type_code, Account.name)
            )
            account_rows = session.execute(accounts_query).all()

            accounts = [
                AccountSummary(
                    id=row.id,
                    name=row.name,
                    type=str(row.account_type),
                    currency=row.currency,
                    balance=Decimal(row.balance or 0),
                )
                for row in account_rows
            ]

            total_account_balance = sum((account.balance for account in accounts), Decimal("0"))
            cash_balance = sum(
                (
                    account.balance
                    for account in accounts
                    if account.type != AccountType.BROKERAGE.value
                ),
                Decimal("0"),
            )

        holdings_total = Decimal("0")
        if company_party_id:
            holdings_period_id = session.execute(
                select(ReportingPeriod.id)
                .join(
                    HoldingPerformanceFact,
                    HoldingPerformanceFact.reporting_period_id == ReportingPeriod.id,
                )
                .where(HoldingPerformanceFact.party_id == company_party_id)
                .order_by(ReportingPeriod.period_end.desc())
                .limit(1)
            ).scalar_one_or_none()

            if holdings_period_id:
                holdings_value = session.execute(
                    select(func.coalesce(func.sum(HoldingPerformanceFact.market_value), 0))
                    .where(
                        HoldingPerformanceFact.reporting_period_id == holdings_period_id,
                        HoldingPerformanceFact.party_id == company_party_id,
                    )
                ).scalar_one_or_none()
                holdings_total = Decimal(holdings_value or 0)

        income_breakdown: list[CategoryBreakdown] = []
        expense_breakdown: list[CategoryBreakdown] = []

        if company_party_id:
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
                    section_name="income",
                    start_date=start_date,
                    party_ids=[company_party_id],
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
                    section_name="expense",
                    start_date=start_date,
                    party_ids=[company_party_id],
                )
            ]

        period_income = sum((category.total for category in income_breakdown), Decimal("0"))
        period_expenses = sum((category.total for category in expense_breakdown), Decimal("0"))
        net_cash_flow = period_income - period_expenses

        if latest_cash_period and company_party_id:
            flow_rows = session.execute(
                select(
                    Section.name,
                    CashFlowFact.inflow_amount,
                    CashFlowFact.outflow_amount,
                    CashFlowFact.net_amount,
                )
                .join(Section, Section.id == CashFlowFact.section_id)
                .where(
                    CashFlowFact.reporting_period_id == latest_cash_period.id,
                    CashFlowFact.party_id == company_party_id,
                )
            ).all()

            income_from_fact = Decimal("0")
            expense_from_fact = Decimal("0")
            net_from_fact = Decimal("0")

            for row in flow_rows:
                inflow = Decimal(row.inflow_amount or 0)
                outflow = Decimal(row.outflow_amount or 0)
                net = Decimal(row.net_amount or 0)
                if row.name == "income":
                    income_from_fact += inflow
                elif row.name == "expense":
                    expense_from_fact += outflow
                net_from_fact += net

            period_income = income_from_fact
            period_expenses = expense_from_fact
            net_cash_flow = net_from_fact

        total_profit = Decimal("0")
        if company_party_id:
            total_profit_value = session.execute(
                select(func.coalesce(func.sum(CashFlowFact.net_amount), 0))
                .join(Section, Section.id == CashFlowFact.section_id)
                .where(
                    CashFlowFact.party_id == company_party_id,
                    Section.name.in_(("income", "expense")),
                )
            ).scalar_one_or_none()
            total_profit = Decimal(total_profit_value or 0)

        employee_count = 0
        monthly_salary_cost = Decimal("0")
        payroll_period_id = None
        if company_party_id:
            payroll_period_id = session.execute(
                select(ReportingPeriod.id)
                .join(PayrollFact, PayrollFact.reporting_period_id == ReportingPeriod.id)
                .join(EmploymentContract, EmploymentContract.id == PayrollFact.contract_id)
                .where(EmploymentContract.employer_party_id == company_party_id)
                .order_by(ReportingPeriod.period_end.desc())
                .limit(1)
            ).scalar_one_or_none()

            if payroll_period_id:
                employee_result = session.execute(
                    select(func.count(func.distinct(EmploymentContract.employee_party_id)))
                    .select_from(PayrollFact)
                    .join(EmploymentContract, EmploymentContract.id == PayrollFact.contract_id)
                    .where(
                        PayrollFact.reporting_period_id == payroll_period_id,
                        EmploymentContract.employer_party_id == company_party_id,
                    )
                ).scalar_one_or_none()
                employee_count = int(employee_result or 0)

                salary_total = session.execute(
                    select(func.coalesce(func.sum(PayrollFact.gross_amount), 0))
                    .select_from(PayrollFact)
                    .join(EmploymentContract, EmploymentContract.id == PayrollFact.contract_id)
                    .where(
                        PayrollFact.reporting_period_id == payroll_period_id,
                        EmploymentContract.employer_party_id == company_party_id,
                    )
                ).scalar_one_or_none()
                monthly_salary_cost = Decimal(salary_total or 0)

        payroll: list[PayrollEntry] = []
        if payroll_period_id:
            payroll_rows = session.execute(
                select(
                    UserPartyMap.user_id,
                    Party.display_name,
                    PayrollFact.gross_amount,
                )
                .select_from(PayrollFact)
                .join(EmploymentContract, EmploymentContract.id == PayrollFact.contract_id)
                .join(Party, Party.id == EmploymentContract.employee_party_id)
                .join(UserPartyMap, UserPartyMap.party_id == Party.id)
                .where(
                    PayrollFact.reporting_period_id == payroll_period_id,
                    EmploymentContract.employer_party_id == company_party_id,
                )
                .order_by(PayrollFact.gross_amount.desc())
                .limit(25)
            ).all()
            payroll = [
                PayrollEntry(
                    user_id=row.user_id,
                    name=row.display_name,
                    salary_amount=Decimal(row.gross_amount or 0),
                )
                for row in payroll_rows
            ]

        summary = SummaryMetrics(
            net_worth=total_account_balance + holdings_total,
            cash_balance=cash_balance,
            holdings_value=holdings_total,
            period_income=period_income,
            period_expenses=period_expenses,
            net_cash_flow=net_cash_flow,
            total_profit=total_profit,
            employee_count=employee_count,
            monthly_salary_cost=monthly_salary_cost,
        )

        dashboard = CompanyDashboard(
            profile=CompanyProfile(id=company_id, name=company_name),
            period_label=period_label,
            summary=summary,
            accounts=accounts,
            income_breakdown=income_breakdown,
            expense_breakdown=expense_breakdown,
            payroll=payroll,
        )

        LOGGER.debug("Dashboard assembled for company id=%s", company_id)
        return dashboard
