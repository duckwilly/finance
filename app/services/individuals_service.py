"""Service implementation for individual client dashboards."""
from __future__ import annotations

import calendar
import math
from collections import defaultdict
from datetime import date, timedelta
from decimal import Decimal

from sqlalchemy import and_, func, or_, select
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
    PayrollFact,
    PriceQuote,
    ReportingPeriod,
    Section,
    OrgPartyMap,
    UserPartyMap,
)
from app.models.party import IndividualProfile as IndividualProfileModel, Party
from app.schemas.individuals import (
    AccountSummary,
    CategoryBreakdown,
    HoldingSummary,
    IndividualDashboard,
    IndividualProfile,
    SeriesPoint,
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
                EmploymentContract.employer_party_id.label("employer_party_id"),
                OrgPartyMap.org_id.label("company_id"),
                Party.display_name,
            )
            .join(Party, Party.id == EmploymentContract.employer_party_id)
            .outerjoin(OrgPartyMap, OrgPartyMap.party_id == EmploymentContract.employer_party_id)
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
        employer_id = None
        if employment_row:
            employer_id = employment_row.company_id or employment_row.employer_party_id

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

        earliest_entry_date = None
        if user_party_id:
            earliest_entry_date = session.execute(
                select(func.min(JournalEntry.txn_date))
                .join(JournalLine, JournalLine.entry_id == JournalEntry.id)
                .join(Account, Account.id == JournalLine.account_id)
                .where(Account.party_id == user_party_id)
            ).scalar_one_or_none()

        holding_period_bounds = None
        if user_party_id:
            holding_period_bounds = session.execute(
                select(
                    func.min(ReportingPeriod.period_start).label("start"),
                    func.max(ReportingPeriod.period_end).label("end"),
                )
                .join(
                    HoldingPerformanceFact,
                    HoldingPerformanceFact.reporting_period_id == ReportingPeriod.id,
                )
                .where(HoldingPerformanceFact.party_id == user_party_id)
            ).first()

        def _month_end(value: date) -> date:
            last_day = calendar.monthrange(value.year, value.month)[1]
            return date(value.year, value.month, last_day)

        def _month_sequence(start: date, end: date) -> list[date]:
            start_month_end = _month_end(start)
            end_month_end = _month_end(end)
            months: list[date] = []
            current = start_month_end
            while current <= end_month_end:
                months.append(current)
                if current.month == 12:
                    current = date(current.year + 1, 1, calendar.monthrange(current.year + 1, 1)[1])
                else:
                    current = date(current.year, current.month + 1, calendar.monthrange(current.year, current.month + 1)[1])
            return months

        def _quantile(values: list[Decimal], percentile: float) -> Decimal:
            if not values:
                return Decimal("0")
            if len(values) == 1:
                return values[0]

            rank = (len(values) - 1) * (percentile / 100)
            lower = math.floor(rank)
            upper = math.ceil(rank)

            if lower == upper:
                return values[int(rank)]

            lower_value = values[lower]
            upper_value = values[upper]
            fraction = Decimal(str(rank - lower))
            return lower_value + (upper_value - lower_value) * fraction

        simulation_start_candidates = [
            candidate
            for candidate in (
                earliest_entry_date,
                holding_period_bounds.start if holding_period_bounds else None,
            )
            if candidate
        ]
        simulation_end_candidates = [
            candidate
            for candidate in (
                latest_entry_date,
                holding_period_bounds.end if holding_period_bounds else None,
            )
            if candidate
        ]

        simulation_start = (
            min(simulation_start_candidates)
            if simulation_start_candidates
            else date.today() - timedelta(days=self.DEFAULT_PERIOD_DAYS)
        )
        simulation_end = max(simulation_end_candidates) if simulation_end_candidates else date.today()
        if simulation_start > simulation_end:
            simulation_start = simulation_end

        month_ends = _month_sequence(simulation_start, simulation_end)
        if not month_ends:
            month_ends = [_month_end(simulation_end)]

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
                        Instrument.id.label("instrument_id"),
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

        if accounts:
            brokerage_accounts = [account for account in accounts if account.type == AccountType.BROKERAGE.value]
            if brokerage_accounts:
                per_account_value = holdings_total / len(brokerage_accounts) if holdings_total else Decimal("0")
                brokerage_remaining = holdings_total
                brokerage_seen = 0
                updated_accounts: list[AccountSummary] = []

                for account in accounts:
                    if account.type != AccountType.BROKERAGE.value:
                        updated_accounts.append(account)
                        continue

                    brokerage_seen += 1
                    if brokerage_seen == len(brokerage_accounts):
                        balance = brokerage_remaining
                    else:
                        balance = per_account_value
                        brokerage_remaining -= balance

                    updated_accounts.append(account.model_copy(update={"balance": balance}))

                accounts = updated_accounts

        cash_deltas: defaultdict[date, Decimal] = defaultdict(lambda: Decimal("0"))
        if user_party_id:
            cash_rows = session.execute(
                select(JournalEntry.txn_date, JournalLine.amount)
                .select_from(JournalLine)
                .join(JournalEntry, JournalEntry.id == JournalLine.entry_id)
                .join(Account, Account.id == JournalLine.account_id)
                .where(
                    Account.party_id == user_party_id,
                    Account.account_type_code != AccountType.BROKERAGE.value,
                )
            ).all()

            for row in cash_rows:
                if not row.txn_date:
                    continue
                cash_deltas[_month_end(row.txn_date)] += Decimal(row.amount or 0)

        holding_snapshots: list[tuple[date, Decimal, Decimal]] = []
        instrument_qty: dict[int, Decimal] = {}
        if user_party_id:
            holding_rows = session.execute(
                select(
                    ReportingPeriod.period_end,
                    func.coalesce(func.sum(HoldingPerformanceFact.market_value), 0).label("market_value"),
                    func.coalesce(func.sum(HoldingPerformanceFact.unrealized_pl), 0).label("unrealized_pl"),
                )
                .join(
                    ReportingPeriod,
                    ReportingPeriod.id == HoldingPerformanceFact.reporting_period_id,
                )
                .where(HoldingPerformanceFact.party_id == user_party_id)
                .group_by(ReportingPeriod.period_end)
                .order_by(ReportingPeriod.period_end)
            ).all()

            for row in holding_rows:
                if row.period_end is None:
                    continue
                holding_snapshots.append(
                    (
                        row.period_end,
                        Decimal(row.market_value or 0),
                        Decimal(row.unrealized_pl or 0),
                    )
                )

            # Capture current quantities per instrument for historical pricing
            latest_holdings = session.execute(
                select(
                    HoldingPerformanceFact.instrument_id,
                    func.coalesce(func.sum(HoldingPerformanceFact.quantity), 0).label("quantity"),
                )
                .where(HoldingPerformanceFact.party_id == user_party_id)
                .group_by(HoldingPerformanceFact.instrument_id)
            ).all()
            for row in latest_holdings:
                instrument_qty[row.instrument_id] = Decimal(row.quantity or 0)

        # Build month-end brokerage value using historical prices
        brokerage_value_by_month: dict[date, Decimal] = {month: Decimal("0") for month in month_ends}
        if instrument_qty:
            price_rows = session.execute(
                select(PriceQuote.instrument_id, PriceQuote.price_date, PriceQuote.quote_value)
                .where(
                    PriceQuote.instrument_id.in_(list(instrument_qty.keys())),
                    PriceQuote.quote_type == "CLOSE",
                    PriceQuote.price_date <= simulation_end,
                )
                .order_by(PriceQuote.instrument_id, PriceQuote.price_date)
            ).all()

            prices_by_instrument: dict[int, list[tuple[date, Decimal]]] = defaultdict(list)
            for row in price_rows:
                if row.price_date:
                    prices_by_instrument[row.instrument_id].append(
                        (row.price_date, Decimal(row.quote_value or 0))
                    )

            for instrument_id, qty in instrument_qty.items():
                if not qty:
                    continue
                series = prices_by_instrument.get(instrument_id, [])
                pointer = 0
                last_price = Decimal("0")
                for month_end in month_ends:
                    while pointer < len(series) and series[pointer][0] <= month_end:
                        last_price = series[pointer][1]
                        pointer += 1
                    brokerage_value_by_month[month_end] += qty * last_price

        if not any(brokerage_value_by_month.values()) and holding_snapshots:
            holding_snapshots.sort(key=lambda snap: snap[0])
            current_value = holding_snapshots[0][1]
            snap_idx = 1
            for month_end in month_ends:
                while snap_idx < len(holding_snapshots) and holding_snapshots[snap_idx][0] <= month_end:
                    current_value = holding_snapshots[snap_idx][1]
                    snap_idx += 1
                brokerage_value_by_month[month_end] = current_value

        net_worth_trend: list[SeriesPoint] = []
        brokerage_value_trend: list[SeriesPoint] = []

        running_cash = Decimal("0")

        for month_end in month_ends:
            running_cash += cash_deltas.get(month_end, Decimal("0"))
            current_holdings_value = brokerage_value_by_month.get(month_end, Decimal("0"))

            label = month_end.strftime("%b %Y")
            net_worth_trend.append(
                SeriesPoint(label=label, value=running_cash + current_holdings_value)
            )
            brokerage_value_trend.append(SeriesPoint(label=label, value=current_holdings_value))

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

        income_peer_split: dict[str, int] | None = None
        monthly_income = None
        income_percentile = None

        if user_party_id:
            latest_pay_periods = (
                select(
                    EmploymentContract.employee_party_id.label("party_id"),
                    func.max(ReportingPeriod.period_end).label("max_period_end"),
                )
                .select_from(PayrollFact)
                .join(ReportingPeriod, ReportingPeriod.id == PayrollFact.reporting_period_id)
                .join(EmploymentContract, EmploymentContract.id == PayrollFact.contract_id)
                .group_by(EmploymentContract.employee_party_id)
            ).cte("latest_pay_periods")

            income_rows = session.execute(
                select(
                    latest_pay_periods.c.party_id,
                    func.sum(PayrollFact.gross_amount).label("monthly_income"),
                )
                .select_from(PayrollFact)
                .join(EmploymentContract, EmploymentContract.id == PayrollFact.contract_id)
                .join(ReportingPeriod, ReportingPeriod.id == PayrollFact.reporting_period_id)
                .join(
                    latest_pay_periods,
                    and_(
                        latest_pay_periods.c.party_id == EmploymentContract.employee_party_id,
                        latest_pay_periods.c.max_period_end == ReportingPeriod.period_end,
                    ),
                )
                .group_by(latest_pay_periods.c.party_id)
            ).all()

            income_by_party = {
                row.party_id: Decimal(row.monthly_income or 0)
                for row in income_rows
                if row.party_id is not None
            }
            monthly_income = income_by_party.get(user_party_id)

            income_values = sorted(income_by_party.values())
            if income_values:
                if monthly_income is not None:
                    rank_position = sum(1 for value in income_values if value <= monthly_income)
                    income_percentile = round((rank_position / len(income_values)) * 100, 1)
                    higher = sum(1 for value in income_values if value > monthly_income)
                    lower = sum(1 for value in income_values if value < monthly_income)
                    peers = sum(1 for value in income_values if value == monthly_income)
                    income_peer_split = {
                        "Higher income": higher,
                        "Same income": peers,
                        "Lower income": lower,
                    }

        non_brokerage_balance = sum(
            (account.balance for account in accounts if account.type != AccountType.BROKERAGE.value),
            Decimal("0"),
        )
        cash_balance = sum(
            (
                account.balance
                for account in accounts
                if account.type in (AccountType.CHECKING.value, AccountType.SAVINGS.value)
            ),
            Decimal("0"),
        )
        net_worth = non_brokerage_balance + holdings_total

        summary = SummaryMetrics(
            net_worth=net_worth,
            cash_balance=cash_balance,
            holdings_value=holdings_total,
            period_income=period_income,
            period_expenses=period_expenses,
            net_cash_flow=net_cash_flow,
            monthly_income=monthly_income,
            income_percentile=income_percentile,
        )

        dashboard = IndividualDashboard(
            profile=IndividualProfile(
                id=user_id,
                name=display_name,
                email=email,
                job_title=job_title,
            ),
            employer_id=employer_id,
            employer_name=employer_name,
            summary=summary,
            period_label=period_label,
            accounts=accounts,
            income_breakdown=income_breakdown,
            expense_breakdown=expense_breakdown,
            holdings=holdings,
            net_worth_trend=net_worth_trend,
            brokerage_value_trend=brokerage_value_trend,
            income_peer_split=income_peer_split,
        )

        LOGGER.debug("Dashboard assembled for individual id=%s", user_id)
        return dashboard
