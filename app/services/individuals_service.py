"""Service implementation for individual client dashboards."""
from __future__ import annotations

from datetime import date, timedelta
from decimal import Decimal

from sqlalchemy import func, or_, select
from sqlalchemy.orm import Session

from app.core.logger import get_logger
from app.models import (
    Account,
    AccountType,
    CashFlowFact,
    EmploymentContract,
    HoldingPerformanceFact,
    Instrument,
    JournalEntry,
    JournalLine,
    ReportingPeriod,
    Section,
    UserPartyMap,
)
from app.models.party import IndividualProfile as IndividualProfileModel, Party
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

        user_party_id = session.execute(
            select(UserPartyMap.party_id).where(UserPartyMap.user_id == user_id)
        ).scalar_one_or_none()
        if user_party_id is None:
            msg = f"Individual {user_id} not found"
            LOGGER.warning(msg)
            raise ValueError(msg)

        party = session.get(Party, user_party_id)
        if not party:
            msg = f"Individual {user_id} not found"
            LOGGER.warning(msg)
            raise ValueError(msg)

        profile_row = session.get(IndividualProfileModel, user_party_id)
        display_name = party.display_name or f"User {user_id}"
        email = profile_row.primary_email if profile_row else None

        employment_row = session.execute(
            select(
                EmploymentContract.position_title,
                Party.display_name,
            )
            .join(Party, Party.id == EmploymentContract.employer_party_id)
            .where(
                EmploymentContract.employee_party_id == user_party_id,
                EmploymentContract.is_primary.is_(True),
                EmploymentContract.start_date <= date.today(),
                or_(
                    EmploymentContract.end_date.is_(None),
                    EmploymentContract.end_date >= date.today(),
                ),
            )
            .order_by(EmploymentContract.start_date.desc())
            .limit(1)
        ).first()

        job_title = employment_row.position_title if employment_row else None
        employer_name = employment_row.display_name if employment_row else None

        latest_cash_period = None
        if user_party_id:
            latest_cash_period = session.execute(
                select(ReportingPeriod)
                .join(CashFlowFact, CashFlowFact.reporting_period_id == ReportingPeriod.id)
                .where(CashFlowFact.party_id == user_party_id)
                .order_by(ReportingPeriod.period_end.desc())
                .limit(1)
            ).scalar_one_or_none()

        latest_entry_date = None
        if user_party_id:
            latest_entry_date = session.execute(
                select(func.max(JournalEntry.txn_date))
                .join(JournalLine, JournalLine.entry_id == JournalEntry.id)
                .join(Account, Account.id == JournalLine.account_id)
                .where(Account.party_id == user_party_id)
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

        if user_party_id:
            account_rows = session.execute(
                select(
                    Account.id,
                    Account.name,
                    Account.account_type_code.label("account_type"),
                    Account.currency_code.label("currency"),
                    func.coalesce(func.sum(JournalLine.amount), 0).label("balance"),
                )
                .outerjoin(JournalLine, JournalLine.account_id == Account.id)
                .where(Account.party_id == user_party_id)
                .group_by(Account.id)
                .order_by(Account.account_type_code, Account.name)
            ).all()

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
                    if account.type in (AccountType.CHECKING.value, AccountType.SAVINGS.value)
                ),
                Decimal("0"),
            )

        holdings: list[HoldingSummary] = []
        holdings_total = Decimal("0")
        if user_party_id:
            holdings_period_id = session.execute(
                select(ReportingPeriod.id)
                .join(
                    HoldingPerformanceFact,
                    HoldingPerformanceFact.reporting_period_id == ReportingPeriod.id,
                )
                .where(HoldingPerformanceFact.party_id == user_party_id)
                .order_by(ReportingPeriod.period_end.desc())
                .limit(1)
            ).scalar_one_or_none()

            if holdings_period_id:
                holding_rows = session.execute(
                    select(
                        Instrument.symbol,
                        Instrument.name,
                        HoldingPerformanceFact.quantity,
                        HoldingPerformanceFact.market_value,
                        HoldingPerformanceFact.unrealized_pl,
                    )
                    .join(Instrument, Instrument.id == HoldingPerformanceFact.instrument_id)
                    .where(
                        HoldingPerformanceFact.reporting_period_id == holdings_period_id,
                        HoldingPerformanceFact.party_id == user_party_id,
                    )
                    .order_by(Instrument.symbol)
                ).all()

                for row in holding_rows:
                    qty = Decimal(row.quantity or 0)
                    market_value = Decimal(row.market_value or 0)
                    holdings_total += market_value
                    last_price = market_value / qty if qty else Decimal("0")
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

        income_breakdown: list[CategoryBreakdown] = []
        expense_breakdown: list[CategoryBreakdown] = []

        if user_party_id:
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
                    party_ids=[user_party_id],
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
                    party_ids=[user_party_id],
                )
            ]

        period_income = sum((category.total for category in income_breakdown), Decimal("0"))
        period_expenses = sum((category.total for category in expense_breakdown), Decimal("0"))
        net_cash_flow = period_income - period_expenses

        if latest_cash_period and user_party_id:
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
                    CashFlowFact.party_id == user_party_id,
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

        summary = SummaryMetrics(
            net_worth=total_account_balance + holdings_total,
            cash_balance=cash_balance,
            holdings_value=holdings_total,
            period_income=period_income,
            period_expenses=period_expenses,
            net_cash_flow=net_cash_flow,
        )

        dashboard = IndividualDashboard(
            profile=IndividualProfile(
                id=user_id,
                name=display_name,
                email=email,
                job_title=job_title,
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
