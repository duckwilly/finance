"""Service implementation for admin tooling."""
from __future__ import annotations

from collections import Counter, defaultdict
from datetime import date
from decimal import Decimal
from threading import Lock
from typing import Sequence

from sqlalchemy import and_, case, extract, func, or_, select
from sqlalchemy.orm import Session, aliased

from app.core.logger import get_logger, timeit
from app.models import (
    Account,
    AccountType,
    CashFlowFact,
    Category,
    EmploymentContract,
    Instrument,
    JournalEntry,
    JournalLine,
    OrgPartyMap,
    PayrollFact,
    HoldingPerformanceFact,
    PositionAgg,
    PriceQuote,
    ReportingPeriod,
    Section,
    UserPartyMap,
)
from app.models.party import CompanyProfile, IndividualProfile, Party, PartyType
from app.services.stocks_service import brokerage_aum_by_party
from app.schemas.admin import (
    AdminMetrics,
    DashboardCharts,
    LineChartData,
    ListView,
    ListViewColumn,
    ListViewRow,
    PieChartData,
)

LOGGER = get_logger(__name__)


_METRICS_SNAPSHOTS: dict[str, AdminMetrics] = {}
_INDIVIDUAL_OVERVIEWS: dict[str, ListView] = {}
_INDIVIDUAL_DISTRIBUTIONS: dict[str, dict[str, int]] = {}
_COMPANY_OVERVIEWS: dict[str, ListView] = {}
_COMPANY_DISTRIBUTIONS: dict[str, dict[str, int]] = {}
_STOCK_OVERVIEWS: dict[str, ListView] = {}
_STOCK_SERIES_CACHE: dict[str, tuple[list[tuple[str, float]], str | None, str | None]] = {}
_TRANSACTION_OVERVIEWS: dict[str, ListView] = {}
_TRANSACTION_DISTRIBUTIONS: dict[str, dict[str, int]] = {}
_metrics_lock = Lock()


class AdminService:
    """Service encapsulating administrator dashboard workflows."""

    INCOME_BUCKETS: tuple[tuple[str, Decimal | None, Decimal | None], ...] = (
        ("Under €50k", Decimal("0"), Decimal("50000")),
        ("€50k-€150k", Decimal("50000"), Decimal("150000")),
        ("€150k+", Decimal("150000"), None),
    )
    PROFIT_MARGIN_BUCKETS: tuple[tuple[str, Decimal | None, Decimal | None], ...] = (
        ("Loss (< -5%)", None, Decimal("-0.05")),
        ("-5% to 0%", Decimal("-0.05"), Decimal("0")),
        ("0-5% margin", Decimal("0"), Decimal("0.05")),
        ("5-10% margin", Decimal("0.05"), Decimal("0.10")),
        ("10-20% margin", Decimal("0.10"), Decimal("0.20")),
        ("20%+ margin", Decimal("0.20"), None),
    )
    TRANSACTION_SIZE_BUCKETS: tuple[tuple[str, Decimal | None, Decimal | None], ...] = (
        ("<€1k", None, Decimal("1000")),
        ("€1k-€5k", Decimal("1000"), Decimal("5000")),
        ("€5k-€10k", Decimal("5000"), Decimal("10000")),
        ("€10k+", Decimal("10000"), None),
    )

    def __init__(self) -> None:
        self._income_distribution: dict[str, int] | None = None
        self._profit_margin_distribution: dict[str, int] | None = None
        self._transaction_amount_distribution: dict[str, int] | None = None
        self._stock_price_series: list[tuple[str, float]] | None = None
        self._stock_price_series_label: str | None = None
        self._stock_price_series_hint: str | None = None

    @staticmethod
    def _bucketize(
        value: Decimal,
        buckets: Sequence[tuple[str, Decimal | None, Decimal | None]],
    ) -> str:
        """Return the label matching ``value`` using inclusive lower bounds."""

        for label, lower, upper in buckets:
            lower_ok = lower is None or value >= lower
            upper_ok = upper is None or value < upper
            if lower_ok and upper_ok:
                return label
        # Fallback for any values that might fall outside configured ranges.
        return buckets[-1][0]

    def _categorize_income(self, annual_income: Decimal) -> str:
        """Bucketize annual income into configured brackets."""

        return self._bucketize(annual_income, self.INCOME_BUCKETS)

    def _categorize_margin(
        self, monthly_income: Decimal, margin_ratio: Decimal | None
    ) -> str:
        """Bucketize profit margin, capturing edge cases for no revenue."""

        if monthly_income <= 0 or margin_ratio is None:
            return self.PROFIT_MARGIN_BUCKETS[0][0]
        return self._bucketize(margin_ratio, self.PROFIT_MARGIN_BUCKETS)

    def _categorize_transaction_amount(self, amount: Decimal) -> str:
        """Bucketize a transaction amount by absolute value."""

        return self._bucketize(amount, self.TRANSACTION_SIZE_BUCKETS)

    @staticmethod
    def _engine_key(session: Session) -> str:
        bind = session.get_bind()
        if bind is None:
            return "unbound"
        try:
            url = bind.url
            key = url.render_as_string(hide_password=True)
            if getattr(url, "drivername", "") == "sqlite" and (
                url.database in {None, ":memory:"}
            ):
                return f"{key}-{id(bind)}"
            return key
        except AttributeError:  # pragma: no cover - fallback for unusual engines
            return str(id(bind))

    @classmethod
    def refresh_metrics(cls, session: Session) -> AdminMetrics:
        """Compute and store the metrics snapshot for the current engine."""

        metrics = cls._compute_metrics(session)
        key = cls._engine_key(session)
        with _metrics_lock:
            _METRICS_SNAPSHOTS[key] = metrics
        return metrics

    @classmethod
    def _compute_metrics(cls, session: Session) -> AdminMetrics:
        LOGGER.debug("Collecting admin metrics from the database")

        with timeit(
            "Admin dashboard metrics loading",
            logger=LOGGER,
            track_db_calls=True,
            session=session,
            unit="metrics",
        ) as timer:
            latest_period_id = session.execute(
                select(ReportingPeriod.id)
                .order_by(ReportingPeriod.period_end.desc())
                .limit(1)
            ).scalar_one_or_none()

            individual_count = select(func.count(IndividualProfile.party_id)).scalar_subquery()
            company_count = select(func.count(CompanyProfile.party_id)).scalar_subquery()

            journal_stats = (
                select(
                    func.count(JournalEntry.id).label("total_transactions"),
                    func.min(JournalEntry.posted_at).label("first_transaction_at"),
                    func.max(JournalEntry.posted_at).label("last_transaction_at"),
                )
            ).subquery()

            cash_total = (
                select(func.coalesce(func.sum(JournalLine.amount), 0))
                .select_from(JournalLine)
                .join(Account, JournalLine.account_id == Account.id)
                .join(Party, Party.id == Account.party_id)
                .where(
                    Account.account_type_code != AccountType.BROKERAGE.value,
                    Party.party_type.in_((PartyType.INDIVIDUAL, PartyType.COMPANY)),
                    Party.display_name != "Ledger Clearing",
                )
            ).scalar_subquery()

            if latest_period_id is not None:
                holdings_total = (
                    select(func.coalesce(func.sum(HoldingPerformanceFact.market_value), 0))
                    .join(Party, Party.id == HoldingPerformanceFact.party_id)
                    .where(
                        HoldingPerformanceFact.reporting_period_id == latest_period_id,
                        Party.party_type.in_((PartyType.INDIVIDUAL, PartyType.COMPANY)),
                        Party.display_name != "Ledger Clearing",
                    )
                ).scalar_subquery()
            else:
                holdings_total = (
                    select(func.coalesce(func.sum(PositionAgg.qty * PositionAgg.last_price), 0))
                    .join(Account, Account.id == PositionAgg.account_id)
                    .join(Party, Party.id == Account.party_id)
                    .where(
                        Party.party_type.in_((PartyType.INDIVIDUAL, PartyType.COMPANY)),
                        Party.display_name != "Ledger Clearing",
                    )
                ).scalar_subquery()

            metrics_stmt = (
                select(
                    individual_count.label("total_individuals"),
                    company_count.label("total_companies"),
                    journal_stats.c.total_transactions,
                    journal_stats.c.first_transaction_at,
                    journal_stats.c.last_transaction_at,
                    cash_total.label("total_cash"),
                    holdings_total.label("total_holdings"),
                )
                .select_from(journal_stats)
            )

            row = session.execute(metrics_stmt).one()

            total_cash = Decimal(row.total_cash or 0)
            total_holdings = Decimal(row.total_holdings or 0)
            total_aum = total_cash + total_holdings

            metrics = AdminMetrics(
                total_individuals=int(row.total_individuals or 0),
                total_companies=int(row.total_companies or 0),
                total_transactions=int(row.total_transactions or 0),
                first_transaction_at=row.first_transaction_at,
                last_transaction_at=row.last_transaction_at,
                total_aum=total_aum,
                total_cash=total_cash,
                total_holdings=total_holdings,
            )

            timer.set_total(1)
            LOGGER.debug("Admin metrics computed: %s", metrics)

        return metrics

    def get_metrics(self, session: Session) -> AdminMetrics:
        """Return the precomputed metrics snapshot for the session's engine."""

        key = self._engine_key(session)
        with _metrics_lock:
            metrics = _METRICS_SNAPSHOTS.get(key)

        if metrics is not None:
            return metrics

        LOGGER.debug("Metrics snapshot missing for engine %s; computing now", key)
        return self.refresh_metrics(session)

    @classmethod
    def clear_metrics_cache(cls) -> None:
        with _metrics_lock:
            _METRICS_SNAPSHOTS.clear()
            _INDIVIDUAL_OVERVIEWS.clear()
            _COMPANY_OVERVIEWS.clear()
            _STOCK_OVERVIEWS.clear()
            _TRANSACTION_OVERVIEWS.clear()

    def get_individual_overview(self, session: Session) -> ListView:
        """Return a reusable list view model for individual users."""

        key = self._engine_key(session)
        with _metrics_lock:
            cached = _INDIVIDUAL_OVERVIEWS.get(key)
            cached_dist = _INDIVIDUAL_DISTRIBUTIONS.get(key)
            company_dist = _COMPANY_DISTRIBUTIONS.get(key)
            transaction_dist = _TRANSACTION_DISTRIBUTIONS.get(key)
            stock_cached = _STOCK_SERIES_CACHE.get(key)
        if cached is not None and cached_dist is not None:
            self._income_distribution = dict(cached_dist)
            if company_dist is not None:
                self._profit_margin_distribution = dict(company_dist)
            if transaction_dist is not None:
                self._transaction_amount_distribution = dict(transaction_dist)
            if stock_cached:
                series, label, hint = stock_cached
                self._stock_price_series = list(series)
                self._stock_price_series_label = label
                self._stock_price_series_hint = hint
            return cached
        elif cached is not None:
            LOGGER.debug(
                "Individual overview cache missing distributions for engine %s; recomputing",
                key,
            )

        LOGGER.debug("Collecting individual overview data for admin dashboard")

        with timeit(
            "Individual user list loading",
            logger=LOGGER,
            track_db_calls=True,
            session=session,
            unit="users"
        ) as timer:
            base_individuals = (
                select(
                    Party.id.label("party_id"),
                    Party.display_name.label("display_name"),
                    UserPartyMap.user_id.label("user_id"),
                )
                .join(IndividualProfile, IndividualProfile.party_id == Party.id)
                .outerjoin(UserPartyMap, UserPartyMap.party_id == Party.id)
                .where(Party.party_type == PartyType.INDIVIDUAL)
            ).cte("base_individuals")

            latest_contracts = (
                select(
                    EmploymentContract.employee_party_id.label("party_id"),
                    EmploymentContract.employer_party_id.label("employer_party_id"),
                    EmploymentContract.position_title.label("position_title"),
                    func.row_number()
                    .over(
                        partition_by=EmploymentContract.employee_party_id,
                        order_by=EmploymentContract.start_date.desc(),
                    )
                    .label("rn"),
                )
                .join(base_individuals, base_individuals.c.party_id == EmploymentContract.employee_party_id)
                .where(
                    EmploymentContract.start_date <= date.today(),
                    or_(
                        EmploymentContract.end_date.is_(None),
                        EmploymentContract.end_date >= date.today(),
                    ),
                )
            ).cte("latest_contracts")

            active_contracts = (
                select(
                    latest_contracts.c.party_id,
                    latest_contracts.c.employer_party_id,
                    latest_contracts.c.position_title,
                )
                .where(latest_contracts.c.rn == 1)
            ).cte("active_contracts")

            latest_pay_periods = (
                select(
                    EmploymentContract.employee_party_id.label("party_id"),
                    func.max(ReportingPeriod.period_end).label("max_period_end"),
                )
                .select_from(PayrollFact)
                .join(ReportingPeriod, ReportingPeriod.id == PayrollFact.reporting_period_id)
                .join(EmploymentContract, EmploymentContract.id == PayrollFact.contract_id)
                .join(base_individuals, base_individuals.c.party_id == EmploymentContract.employee_party_id)
                .group_by(EmploymentContract.employee_party_id)
            ).cte("latest_pay_periods")

            income_totals = (
                select(
                    EmploymentContract.employee_party_id.label("party_id"),
                    func.sum(PayrollFact.gross_amount).label("monthly_income"),
                )
                .select_from(PayrollFact)
                .join(ReportingPeriod, ReportingPeriod.id == PayrollFact.reporting_period_id)
                .join(EmploymentContract, EmploymentContract.id == PayrollFact.contract_id)
                .join(
                    latest_pay_periods,
                    and_(
                        latest_pay_periods.c.party_id == EmploymentContract.employee_party_id,
                        ReportingPeriod.period_end == latest_pay_periods.c.max_period_end,
                    ),
                )
                .group_by(EmploymentContract.employee_party_id)
            ).cte("income_totals")

            balances = (
                select(
                    Account.party_id.label("party_id"),
                    func.sum(
                        case(
                            (Account.account_type_code == AccountType.CHECKING.value, func.coalesce(JournalLine.amount, 0)),
                            else_=0,
                        )
                    ).label("checking_balance"),
                    func.sum(
                        case(
                            (Account.account_type_code == AccountType.SAVINGS.value, func.coalesce(JournalLine.amount, 0)),
                            else_=0,
                        )
                    ).label("savings_balance"),
                )
                .select_from(Account)
                .join(base_individuals, base_individuals.c.party_id == Account.party_id)
                .join(JournalLine, JournalLine.account_id == Account.id, isouter=True)
                .group_by(Account.party_id)
            ).cte("balances")

            brokerage = (
                select(
                    Account.party_id.label("party_id"),
                    func.sum(PositionAgg.qty * func.coalesce(PositionAgg.last_price, 0)).label("brokerage_aum"),
                )
                .select_from(PositionAgg)
                .join(Account, Account.id == PositionAgg.account_id)
                .join(base_individuals, base_individuals.c.party_id == Account.party_id)
                .group_by(Account.party_id)
            ).cte("brokerage")

            employer_party = aliased(Party, name="employer_party")

            rows_stmt = (
                select(
                    base_individuals.c.party_id,
                    base_individuals.c.display_name,
                    base_individuals.c.user_id,
                    active_contracts.c.position_title,
                    employer_party.display_name.label("employer_name"),
                    func.coalesce(income_totals.c.monthly_income, 0).label("monthly_income"),
                    func.coalesce(balances.c.checking_balance, 0).label("checking_balance"),
                    func.coalesce(balances.c.savings_balance, 0).label("savings_balance"),
                    func.coalesce(brokerage.c.brokerage_aum, 0).label("brokerage_aum"),
                )
                .select_from(base_individuals)
                .outerjoin(active_contracts, active_contracts.c.party_id == base_individuals.c.party_id)
                .outerjoin(employer_party, employer_party.id == active_contracts.c.employer_party_id)
                .outerjoin(income_totals, income_totals.c.party_id == base_individuals.c.party_id)
                .outerjoin(balances, balances.c.party_id == base_individuals.c.party_id)
                .outerjoin(brokerage, brokerage.c.party_id == base_individuals.c.party_id)
                .order_by(base_individuals.c.display_name)
            )

            records = session.execute(rows_stmt).all()

            if not records:
                timer.set_total(0)
                self._income_distribution = {
                    label: 0 for label, *_ in self.INCOME_BUCKETS
                }
                list_view = ListView(
                    title="Individual users",
                    columns=[
                        ListViewColumn(key="name", title="Name"),
                        ListViewColumn(key="employer", title="Employer"),
                        ListViewColumn(key="job_title", title="Job Title"),
                        ListViewColumn(key="monthly_income", title="Monthly income", column_type="currency", align="right"),
                        ListViewColumn(key="checking_aum", title="Checking AUM", column_type="currency", align="right"),
                        ListViewColumn(key="savings_aum", title="Savings AUM", column_type="currency", align="right"),
                        ListViewColumn(key="brokerage_aum", title="Brokerage AUM", column_type="currency", align="right"),
                    ],
                    rows=[],
                    search_placeholder="Search individuals",
                    empty_message="No individual users found.",
                )
                with _metrics_lock:
                    _INDIVIDUAL_OVERVIEWS[key] = list_view
                    _INDIVIDUAL_DISTRIBUTIONS[key] = dict(self._income_distribution)
                return list_view

            rows: list[ListViewRow] = []
            income_counts: Counter[str] = Counter()

            for record in records:
                monthly_income = Decimal(record.monthly_income or 0)
                checking_balance = Decimal(record.checking_balance or 0)
                savings_balance = Decimal(record.savings_balance or 0)
                brokerage_aum = Decimal(record.brokerage_aum or 0)

                annual_income = monthly_income * Decimal(12)
                income_bucket = self._categorize_income(annual_income)
                income_counts[income_bucket] += 1

                search_terms = [record.display_name]
                if record.employer_name:
                    search_terms.append(record.employer_name)
                if record.position_title:
                    search_terms.append(record.position_title)

                identifier = record.user_id or record.party_id

                rows.append(
                    ListViewRow(
                        key=str(identifier),
                        values={
                            "name": record.display_name,
                            "job_title": record.position_title or "",
                            "employer": record.employer_name,
                            "monthly_income": monthly_income,
                            "checking_aum": checking_balance,
                            "savings_aum": savings_balance,
                            "brokerage_aum": brokerage_aum,
                        },
                        search_text=" ".join(filter(None, search_terms)).lower(),
                        links={"name": f"/individuals/{identifier}"},
                    )
                )

            list_view = ListView(
                title="Individual users",
                columns=[
                    ListViewColumn(key="name", title="Name"),
                    ListViewColumn(key="employer", title="Employer"),
                    ListViewColumn(key="job_title", title="Job Title"),
                    ListViewColumn(key="monthly_income", title="Monthly income", column_type="currency", align="right"),
                    ListViewColumn(key="checking_aum", title="Checking AUM", column_type="currency", align="right"),
                    ListViewColumn(key="savings_aum", title="Savings AUM", column_type="currency", align="right"),
                    ListViewColumn(key="brokerage_aum", title="Brokerage AUM", column_type="currency", align="right"),
                ],
                rows=rows,
                search_placeholder="Search individuals",
                empty_message="No individual users found.",
            )

            timer.set_total(len(rows))
            LOGGER.debug("Prepared %d individual overview rows", len(rows))

            self._income_distribution = {
                label: income_counts.get(label, 0)
                for label, *_ in self.INCOME_BUCKETS
            }

        with _metrics_lock:
            _INDIVIDUAL_OVERVIEWS[key] = list_view
            _INDIVIDUAL_DISTRIBUTIONS[key] = dict(self._income_distribution)

        return list_view

    def get_company_overview(self, session: Session) -> ListView:
        """Return a reusable list view model for corporate users."""

        key = self._engine_key(session)
        with _metrics_lock:
            cached = _COMPANY_OVERVIEWS.get(key)
            cached_dist = _COMPANY_DISTRIBUTIONS.get(key)
        if cached is not None and cached_dist is not None:
            self._profit_margin_distribution = dict(cached_dist)
            return cached
        elif cached is not None:
            LOGGER.debug(
                "Company overview cache missing distributions for engine %s; recomputing",
                key,
            )

        LOGGER.debug("Collecting company overview data for admin dashboard")

        with timeit(
            "Company list loading",
            logger=LOGGER,
            track_db_calls=True,
            session=session,
            unit="companies"
        ) as timer:
            company_records = session.execute(
                select(
                    OrgPartyMap.org_id.label("company_id"),
                    Party.id.label("party_id"),
                    Party.display_name.label("display_name"),
                )
                .select_from(Party)
                .join(CompanyProfile, CompanyProfile.party_id == Party.id)
                .outerjoin(OrgPartyMap, OrgPartyMap.party_id == Party.id)
                .order_by(Party.display_name)
            ).all()

            if not company_records:
                timer.set_total(0)
                self._profit_margin_distribution = {
                    label: 0 for label, *_ in self.PROFIT_MARGIN_BUCKETS
                }
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
                    rows=[],
                    search_placeholder="Search companies",
                    empty_message="No corporate users found.",
                )
                with _metrics_lock:
                    _COMPANY_OVERVIEWS[key] = list_view
                    _COMPANY_DISTRIBUTIONS[key] = dict(self._profit_margin_distribution)
                return list_view

            company_party_ids = {
                record.party_id for record in company_records if record.party_id is not None
            }

            monthly_income_map: defaultdict[int, Decimal] = defaultdict(lambda: Decimal("0"))
            monthly_expense_map: defaultdict[int, Decimal] = defaultdict(lambda: Decimal("0"))
            profit_total_map: dict[int, Decimal] = {}

            if company_party_ids:
                latest_cash = (
                    select(
                        CashFlowFact.party_id.label("party_id"),
                        CashFlowFact.reporting_period_id,
                        func.row_number().over(
                            partition_by=CashFlowFact.party_id,
                            order_by=ReportingPeriod.period_end.desc(),
                        ).label("rn"),
                    )
                    .join(ReportingPeriod, ReportingPeriod.id == CashFlowFact.reporting_period_id)
                    .where(CashFlowFact.party_id.in_(company_party_ids))
                    .subquery()
                )

                cash_rows = session.execute(
                    select(
                        CashFlowFact.party_id,
                        Section.name.label("section_name"),
                        func.sum(CashFlowFact.inflow_amount).label("inflow_amount"),
                        func.sum(CashFlowFact.outflow_amount).label("outflow_amount"),
                        func.sum(CashFlowFact.net_amount).label("net_amount"),
                    )
                    .join(
                        latest_cash,
                        and_(
                            latest_cash.c.party_id == CashFlowFact.party_id,
                            latest_cash.c.reporting_period_id == CashFlowFact.reporting_period_id,
                            latest_cash.c.rn == 1,
                        ),
                    )
                    .join(Section, Section.id == CashFlowFact.section_id)
                    .group_by(CashFlowFact.party_id, Section.name)
                ).all()

                for row in cash_rows:
                    section_name = row.section_name
                    if section_name == "income":
                        monthly_income_map[row.party_id] = Decimal(row.inflow_amount or 0)
                    elif section_name == "expense":
                        monthly_expense_map[row.party_id] = Decimal(row.outflow_amount or 0)

                profit_rows = session.execute(
                    select(
                        CashFlowFact.party_id,
                        func.sum(CashFlowFact.net_amount).label("profit_total"),
                    )
                    .join(Section, Section.id == CashFlowFact.section_id)
                    .where(
                        CashFlowFact.party_id.in_(company_party_ids),
                        Section.name.in_(("income", "expense")),
                    )
                    .group_by(CashFlowFact.party_id)
                ).all()
                profit_total_map = {
                    row.party_id: Decimal(row.profit_total or 0) for row in profit_rows
                }

            monthly_salary_map: defaultdict[int, Decimal] = defaultdict(lambda: Decimal("0"))
            payroll_employee_map: dict[int, int] = {}
            if company_party_ids:
                latest_payroll = (
                    select(
                        EmploymentContract.employer_party_id.label("employer_party_id"),
                        PayrollFact.reporting_period_id,
                        func.row_number().over(
                            partition_by=EmploymentContract.employer_party_id,
                            order_by=ReportingPeriod.period_end.desc(),
                        ).label("rn"),
                    )
                    .select_from(PayrollFact)
                    .join(ReportingPeriod, ReportingPeriod.id == PayrollFact.reporting_period_id)
                    .join(EmploymentContract, EmploymentContract.id == PayrollFact.contract_id)
                    .where(EmploymentContract.employer_party_id.in_(company_party_ids))
                    .subquery()
                )

                payroll_rows = session.execute(
                    select(
                        EmploymentContract.employer_party_id,
                        func.sum(PayrollFact.gross_amount).label("monthly_salary_cost"),
                        func.count(func.distinct(EmploymentContract.employee_party_id)).label("employee_count"),
                    )
                    .select_from(PayrollFact)
                    .join(EmploymentContract, EmploymentContract.id == PayrollFact.contract_id)
                    .join(
                        latest_payroll,
                        and_(
                            latest_payroll.c.employer_party_id == EmploymentContract.employer_party_id,
                            latest_payroll.c.reporting_period_id == PayrollFact.reporting_period_id,
                            latest_payroll.c.rn == 1,
                        ),
                    )
                    .group_by(EmploymentContract.employer_party_id)
                ).all()

                for row in payroll_rows:
                    employer_party_id = row.employer_party_id
                    monthly_salary_map[employer_party_id] = Decimal(row.monthly_salary_cost or 0)
                    payroll_employee_map[employer_party_id] = int(row.employee_count or 0)

            contract_employee_map: dict[int, int] = {}
            if company_party_ids:
                contract_rows = session.execute(
                    select(
                        EmploymentContract.employer_party_id,
                        func.count(func.distinct(EmploymentContract.employee_party_id)).label("employee_count"),
                    )
                    .where(
                        EmploymentContract.employer_party_id.in_(company_party_ids),
                        EmploymentContract.is_primary.is_(True),
                        EmploymentContract.start_date <= date.today(),
                        or_(
                            EmploymentContract.end_date.is_(None),
                            EmploymentContract.end_date >= date.today(),
                        ),
                    )
                    .group_by(EmploymentContract.employer_party_id)
                ).all()
                contract_employee_map = {
                    row.employer_party_id: int(row.employee_count or 0) for row in contract_rows
                }

            rows: list[ListViewRow] = []
            profit_margin_counts: Counter[str] = Counter()

            for record in company_records:
                company_id = record.company_id or record.party_id
                company_name = record.display_name or "Unknown company"
                party_id = record.party_id

                monthly_income = Decimal("0")
                monthly_expenses = Decimal("0")
                profit_total = Decimal("0")
                monthly_salary_cost = Decimal("0")
                employee_count = 0

                if party_id is not None:
                    monthly_income = monthly_income_map.get(party_id, Decimal("0"))
                    monthly_expenses = monthly_expense_map.get(party_id, Decimal("0"))
                    profit_total = profit_total_map.get(party_id, Decimal("0"))
                    monthly_salary_cost = monthly_salary_map.get(party_id, Decimal("0"))

                    payroll_count = payroll_employee_map.get(party_id)
                    contract_count = contract_employee_map.get(party_id, 0)

                    if payroll_count is not None:
                        employee_count = payroll_count
                        if contract_count:
                            employee_count = max(employee_count, contract_count)
                    else:
                        employee_count = contract_count

                margin_ratio: Decimal | None = None
                if monthly_income > 0:
                    margin_ratio = (monthly_income - monthly_expenses) / monthly_income

                margin_bucket = self._categorize_margin(monthly_income, margin_ratio)
                profit_margin_counts[margin_bucket] += 1

                rows.append(
                    ListViewRow(
                        key=str(company_id),
                        values={
                            "name": company_name,
                            "employee_count": employee_count,
                            "monthly_salary_cost": monthly_salary_cost,
                            "monthly_income": monthly_income,
                            "monthly_expenses": monthly_expenses,
                            "profit_total": profit_total,
                        },
                        search_text=(company_name or "").lower(),
                        links={"name": f"/corporate/{company_id}"},
                    )
                )

            timer.set_total(len(rows))

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

            self._profit_margin_distribution = {
                label: profit_margin_counts.get(label, 0)
                for label, *_ in self.PROFIT_MARGIN_BUCKETS
            }

        with _metrics_lock:
            _COMPANY_OVERVIEWS[key] = list_view
            _COMPANY_DISTRIBUTIONS[key] = dict(self._profit_margin_distribution)

        with _metrics_lock:
            _COMPANY_OVERVIEWS[key] = list_view
            _COMPANY_DISTRIBUTIONS[key] = dict(self._profit_margin_distribution)

        return list_view

    def get_stock_holdings_overview(self, session: Session) -> ListView:
        """Return a list view representation of stock holdings by product."""

        key = self._engine_key(session)
        with _metrics_lock:
            cached = _STOCK_OVERVIEWS.get(key)
            cached_series = _STOCK_SERIES_CACHE.get(key)
        if cached is not None and cached_series is not None:
            series, label, hint = cached_series
            self._stock_price_series = list(series)
            self._stock_price_series_label = label
            self._stock_price_series_hint = hint
            return cached
        elif cached is not None:
            LOGGER.debug(
                "Stock overview cache missing price series for engine %s; recomputing",
                key,
            )

        LOGGER.debug("Collecting stock holdings overview data for admin dashboard")

        with timeit(
            "Stock holdings list loading",
            logger=LOGGER,
            track_db_calls=True,
            session=session,
            unit="products",
        ) as timer:
            # Get simulation period from journal data
            first_transaction_at = session.execute(select(func.min(JournalEntry.posted_at))).scalar_one()
            last_transaction_at = session.execute(select(func.max(JournalEntry.posted_at))).scalar_one()
            
            # Convert to dates for price lookup
            simulation_start_date = first_transaction_at.date() if first_transaction_at else None
            simulation_end_date = last_transaction_at.date() if last_transaction_at else None

            user_positions = (
                select(
                    PositionAgg.instrument_id.label("instrument_id"),
                    func.coalesce(func.sum(PositionAgg.qty), 0).label("total_qty"),
                    func.coalesce(
                        func.sum(PositionAgg.qty * PositionAgg.last_price), 0
                    ).label("market_value"),
                )
                .select_from(PositionAgg)
                .join(Account, Account.id == PositionAgg.account_id)
                .join(UserPartyMap, UserPartyMap.party_id == Account.party_id)
                .where(Account.account_type_code == AccountType.BROKERAGE.value)
                .group_by(PositionAgg.instrument_id)
                .cte("user_positions")
            )

            # Use window functions for efficient price lookup based on close quotes
            start_query = (
                select(
                    PriceQuote.instrument_id.label("instrument_id"),
                    PriceQuote.quote_value.label("start_price"),
                    PriceQuote.price_date.label("start_date"),
                    func.row_number().over(
                        partition_by=PriceQuote.instrument_id,
                        order_by=PriceQuote.price_date
                    ).label("rn")
                )
                .select_from(PriceQuote)
                .where(PriceQuote.quote_type == "CLOSE")
            )
            if simulation_start_date:
                start_query = start_query.where(PriceQuote.price_date >= simulation_start_date)
            start_prices = start_query.subquery()

            end_query = (
                select(
                    PriceQuote.instrument_id.label("instrument_id"),
                    PriceQuote.quote_value.label("end_price"),
                    PriceQuote.price_date.label("end_date"),
                    func.row_number().over(
                        partition_by=PriceQuote.instrument_id,
                        order_by=PriceQuote.price_date.desc()
                    ).label("rn")
                )
                .select_from(PriceQuote)
                .where(PriceQuote.quote_type == "CLOSE")
            )
            if simulation_end_date:
                end_query = end_query.where(PriceQuote.price_date <= simulation_end_date)
            end_prices = end_query.subquery()

            # Filter to get only the first row for each instrument
            start_prices_filtered = (
                select(
                    start_prices.c.instrument_id,
                    start_prices.c.start_price,
                    start_prices.c.start_date,
                )
                .select_from(start_prices)
                .where(start_prices.c.rn == 1)
                .subquery()
            )

            end_prices_filtered = (
                select(
                    end_prices.c.instrument_id,
                    end_prices.c.end_price,
                    end_prices.c.end_date,
                )
                .select_from(end_prices)
                .where(end_prices.c.rn == 1)
                .subquery()
            )

            holdings_query = (
                select(
                    Instrument.id.label("instrument_id"),
                    Instrument.symbol,
                    Instrument.name,
                    start_prices_filtered.c.start_price,
                    start_prices_filtered.c.start_date,
                    end_prices_filtered.c.end_price,
                    end_prices_filtered.c.end_date,
                    user_positions.c.total_qty,
                    user_positions.c.market_value,
                )
                .select_from(user_positions)
                .join(Instrument, Instrument.id == user_positions.c.instrument_id)
                .outerjoin(start_prices_filtered, start_prices_filtered.c.instrument_id == Instrument.id)
                .outerjoin(end_prices_filtered, end_prices_filtered.c.instrument_id == Instrument.id)
                .order_by(Instrument.symbol)
            )

            records = session.execute(holdings_query).all()

            if not records and self._stock_price_series is None:
                self._stock_price_series = []
                self._stock_price_series_label = "Top holding"
                self._stock_price_series_hint = None

            rows: list[ListViewRow] = []
            top_holding: tuple[int, str | None, str | None, Decimal] | None = None

            # Get the actual dates from the first record to use in column headers
            start_date_str = None
            end_date_str = None
            if records:
                first_record = records[0]
                if first_record.start_date:
                    start_date_str = first_record.start_date.strftime("%d/%m/%y")
                if first_record.end_date:
                    end_date_str = first_record.end_date.strftime("%d/%m/%y")

            for record in records:
                start_price = (
                    Decimal(record.start_price)
                    if record.start_price is not None
                    else None
                )
                end_price = (
                    Decimal(record.end_price) if record.end_price is not None else None
                )
                total_qty = (
                    Decimal(record.total_qty)
                    if record.total_qty is not None
                    else Decimal(0)
                )
                market_value = (
                    Decimal(record.market_value)
                    if record.market_value is not None
                    else Decimal(0)
                )

                product_label = (
                    f"{record.symbol} • {record.name}"
                    if record.name and record.symbol
                    else record.symbol or record.name
                )

                search_terms = list(filter(None, [record.symbol, record.name]))

                rows.append(
                    ListViewRow(
                        key=str(record.instrument_id),
                        values={
                            "product": product_label,
                            "start_price": start_price,
                            "end_price": end_price,
                            "shares": f"{total_qty:,.2f}",
                            "market_value": market_value,
                        },
                        search_text=" ".join(search_terms).lower(),
                    )
                )

                if top_holding is None or market_value > top_holding[3]:
                    top_holding = (
                        record.instrument_id,
                        record.symbol,
                        record.name,
                        market_value,
                    )

            timer.set_total(len(rows))

            # Format column titles with dates
            start_price_title = f"Price {start_date_str}" if start_date_str else "Start price"
            end_price_title = f"Price {end_date_str}" if end_date_str else "End price"

            list_view = ListView(
                title="Stock holdings",
                columns=[
                    ListViewColumn(key="product", title="Product"),
                    ListViewColumn(
                        key="start_price",
                        title=start_price_title,
                        column_type="currency",
                        align="right",
                    ),
                    ListViewColumn(
                        key="end_price",
                        title=end_price_title,
                        column_type="currency",
                        align="right",
                    ),
                    ListViewColumn(key="shares", title="Shares held", align="right"),
                    ListViewColumn(
                        key="market_value",
                        title="Value held",
                        column_type="currency",
                        align="right",
                    ),
                ],
                rows=rows,
                search_placeholder="Search financial products",
                empty_message="No stock holdings found.",
            )

            LOGGER.debug("Prepared %d stock holding rows", len(rows))

            if top_holding and self._stock_price_series is None:
                instrument_id, symbol, name, _ = top_holding
                label_parts = list(filter(None, [symbol, name]))
                label = " • ".join(label_parts) if label_parts else "Top holding"
                
                # Get price data for the entire simulation period
                query = (
                    select(PriceQuote.price_date, PriceQuote.quote_value)
                    .where(
                        PriceQuote.instrument_id == instrument_id,
                        PriceQuote.quote_type == "CLOSE",
                    )
                )

                if simulation_start_date:
                    query = query.where(PriceQuote.price_date >= simulation_start_date)
                if simulation_end_date:
                    query = query.where(PriceQuote.price_date <= simulation_end_date)

                query = query.order_by(PriceQuote.price_date)
                price_rows = session.execute(query).all()

                if price_rows:
                    self._stock_price_series = [
                        (row.price_date.isoformat(), float(row.quote_value))
                        for row in price_rows
                        if row.quote_value is not None
                    ]
                    self._stock_price_series_label = label
                    start_date_str = simulation_start_date.isoformat() if simulation_start_date else "start"
                    end_date_str = simulation_end_date.isoformat() if simulation_end_date else "end"
                    self._stock_price_series_hint = (
                        f"{label} closing prices across {len(self._stock_price_series)} sessions ({start_date_str} to {end_date_str})"
                    )
                else:
                    self._stock_price_series = []
                    self._stock_price_series_label = label
                    self._stock_price_series_hint = None

        with _metrics_lock:
            _STOCK_OVERVIEWS[key] = list_view
            _STOCK_SERIES_CACHE[key] = (
                list(self._stock_price_series or []),
                self._stock_price_series_label,
                self._stock_price_series_hint,
            )

        return list_view

    def get_available_stocks(self, session: Session) -> list[dict[str, str | int]]:
        """Return a list of all available stocks with price data."""
        
        LOGGER.debug("Fetching available stocks with price data")
        
        # Get all instruments that have price data
        stocks_query = (
            select(
                Instrument.id,
                Instrument.symbol,
                Instrument.name,
            )
            .select_from(Instrument)
            .join(PriceQuote, and_(
                PriceQuote.instrument_id == Instrument.id,
                PriceQuote.quote_type == "CLOSE",
            ))
            .distinct()
            .order_by(Instrument.symbol)
        )
        
        stocks = session.execute(stocks_query).all()
        
        return [
            {
                "id": stock.id,
                "symbol": stock.symbol or "",
                "name": stock.name or "",
                "label": " • ".join(filter(None, [stock.symbol, stock.name])) or f"Stock {stock.id}"
            }
            for stock in stocks
        ]

    def get_stock_price_data(
        self, session: Session, instrument_id: int
    ) -> tuple[list[tuple[str, float]], str, str | None]:
        """Return price series for a specific stock within the simulation period.
        
        Returns:
            Tuple of (price_series, label, hint)
        """
        
        LOGGER.debug("Fetching price data for instrument %d", instrument_id)
        
        # Get simulation period from journal data
        first_transaction_at = session.execute(select(func.min(JournalEntry.posted_at))).scalar_one()
        last_transaction_at = session.execute(select(func.max(JournalEntry.posted_at))).scalar_one()
        
        # Convert to dates for price lookup
        simulation_start_date = first_transaction_at.date() if first_transaction_at else None
        simulation_end_date = last_transaction_at.date() if last_transaction_at else None
        
        # Get instrument details
        instrument = session.execute(
            select(Instrument.symbol, Instrument.name)
            .where(Instrument.id == instrument_id)
        ).first()
        
        if not instrument:
            return [], f"Unknown stock ({instrument_id})", None
        
        symbol, name = instrument
        label_parts = list(filter(None, [symbol, name]))
        label = " • ".join(label_parts) if label_parts else f"Stock {instrument_id}"
        
        # Get price data for this instrument within the simulation period
        query = (
            select(PriceQuote.price_date, PriceQuote.quote_value)
            .where(
                PriceQuote.instrument_id == instrument_id,
                PriceQuote.quote_type == "CLOSE",
            )
        )

        if simulation_start_date:
            query = query.where(PriceQuote.price_date >= simulation_start_date)
        if simulation_end_date:
            query = query.where(PriceQuote.price_date <= simulation_end_date)

        query = query.order_by(PriceQuote.price_date)
        price_rows = session.execute(query).all()

        if not price_rows:
            return [], label, None
        
        series = [
            (row.price_date.isoformat(), float(row.quote_value))
            for row in price_rows
            if row.quote_value is not None
        ]
        
        start_date_str = simulation_start_date.isoformat() if simulation_start_date else "start"
        end_date_str = simulation_end_date.isoformat() if simulation_end_date else "end"
        hint = f"{label} closing prices across {len(series)} sessions ({start_date_str} to {end_date_str})"
        
        return series, label, hint

    def get_transaction_overview(self, session: Session, limit: int = 500) -> ListView:
        """Return a reusable list view model for recent transactions."""

        key = self._engine_key(session)
        with _metrics_lock:
            cached = _TRANSACTION_OVERVIEWS.get(key)
            cached_dist = _TRANSACTION_DISTRIBUTIONS.get(key)
        if cached is not None and cached_dist is not None:
            self._transaction_amount_distribution = dict(cached_dist)
            return cached
        elif cached is not None:
            LOGGER.debug(
                "Transaction overview cache missing distributions for engine %s; recomputing",
                key,
            )

        LOGGER.debug("Collecting transaction overview data for admin dashboard")

        with timeit(
            "Transaction list loading",
            logger=LOGGER,
            track_db_calls=True,
            session=session,
            unit="transactions"
        ) as timer:
            latest_entries = (
                select(
                    JournalEntry.id.label("entry_id"),
                    JournalEntry.txn_date,
                    JournalEntry.posted_at,
                    JournalEntry.description,
                    JournalEntry.counterparty_party_id,
                )
                .order_by(JournalEntry.posted_at.desc())
                .limit(limit)
                .cte("latest_entries")
            )

            payer_candidates = (
                select(
                    JournalLine.entry_id.label("entry_id"),
                    Party.display_name.label("payer_name"),
                    func.abs(JournalLine.amount).label("payer_amount"),
                    Account.currency_code.label("currency_code"),
                    func.row_number()
                    .over(partition_by=JournalLine.entry_id, order_by=JournalLine.amount.asc())
                    .label("rn"),
                )
                .join(Account, Account.id == JournalLine.account_id)
                .join(Party, Party.id == Account.party_id)
                .where(JournalLine.amount < 0)
            ).subquery()

            payee_candidates = (
                select(
                    JournalLine.entry_id.label("entry_id"),
                    Party.display_name.label("payee_name"),
                    func.abs(JournalLine.amount).label("payee_amount"),
                    Account.currency_code.label("currency_code"),
                    Category.name.label("category_name"),
                    Section.name.label("section_name"),
                    func.row_number()
                    .over(partition_by=JournalLine.entry_id, order_by=JournalLine.amount.desc())
                    .label("rn"),
                )
                .join(Account, Account.id == JournalLine.account_id)
                .join(Party, Party.id == Account.party_id)
                .outerjoin(Category, Category.id == JournalLine.category_id)
                .outerjoin(Section, Section.id == Category.section_id)
                .where(JournalLine.amount > 0)
            ).subquery()

            payer = payer_candidates.alias()
            payee = payee_candidates.alias()
            counterparty_party = aliased(Party)

            transactions_query = (
                select(
                    latest_entries.c.entry_id,
                    latest_entries.c.txn_date,
                    latest_entries.c.posted_at,
                    latest_entries.c.description,
                    payee.c.payee_name,
                    payee.c.payee_amount,
                    payee.c.currency_code.label("payee_currency"),
                    payee.c.category_name,
                    payee.c.section_name,
                    payer.c.payer_name,
                    payer.c.payer_amount,
                    payer.c.currency_code.label("payer_currency"),
                    counterparty_party.display_name.label("counterparty_name"),
                )
                .select_from(latest_entries)
                .outerjoin(payee, and_(payee.c.entry_id == latest_entries.c.entry_id, payee.c.rn == 1))
                .outerjoin(payer, and_(payer.c.entry_id == latest_entries.c.entry_id, payer.c.rn == 1))
                .outerjoin(counterparty_party, counterparty_party.id == latest_entries.c.counterparty_party_id)
                .order_by(latest_entries.c.posted_at.desc())
            )

            records = session.execute(transactions_query).all()

            rows: list[ListViewRow] = []
            transaction_amount_counts: Counter[str] = Counter()

            for record in records:
                payer_name = record.payer_name or record.counterparty_name or "Account transfer"
                payee_name = record.payee_name or record.counterparty_name or "Account transfer"

                amount_value = Decimal(record.payee_amount or record.payer_amount or 0)
                currency_code = record.payee_currency or record.payer_currency
                category_name = record.category_name or record.section_name or "Uncategorised"

                amount_bucket = self._categorize_transaction_amount(abs(amount_value))
                transaction_amount_counts[amount_bucket] += 1

                search_terms = [str(record.txn_date), payer_name, payee_name]
                if category_name:
                    search_terms.append(category_name)
                if record.description:
                    search_terms.append(record.description)

                rows.append(
                    ListViewRow(
                        key=str(record.entry_id),
                        values={
                            "date": record.txn_date.strftime("%Y-%m-%d"),
                            "payer": payer_name,
                            "payee": payee_name,
                            "amount": amount_value,
                            "category": category_name,
                            "description": record.description or "",
                        },
                        search_text=" ".join(filter(None, search_terms)).lower(),
                    )
                )

            timer.set_total(len(rows))

            list_view = ListView(
                title="Recent transactions",
                columns=[
                    ListViewColumn(key="date", title="Date"),
                    ListViewColumn(key="payer", title="Payer"),
                    ListViewColumn(key="payee", title="Payee"),
                    ListViewColumn(key="amount", title="Amount", column_type="currency", align="right"),
                    ListViewColumn(key="category", title="Category"),
                    ListViewColumn(key="description", title="Description"),
                ],
                rows=rows,
                search_placeholder="Search transactions",
                empty_message="No transactions found.",
            )

            LOGGER.debug("Prepared %d transaction overview rows", len(rows))

            self._transaction_amount_distribution = {
                label: transaction_amount_counts.get(label, 0)
                for label, *_ in self.TRANSACTION_SIZE_BUCKETS
            }

        with _metrics_lock:
            _TRANSACTION_OVERVIEWS[key] = list_view
            _TRANSACTION_DISTRIBUTIONS[key] = dict(self._transaction_amount_distribution)

        with _metrics_lock:
            _TRANSACTION_OVERVIEWS[key] = list_view
            _TRANSACTION_DISTRIBUTIONS[key] = dict(self._transaction_amount_distribution)

        return list_view

    def get_dashboard_charts(self, session: Session) -> DashboardCharts:
        """Aggregate chart payloads for the admin dashboard."""

        key = self._engine_key(session)

        if self._income_distribution is None:
            self.get_individual_overview(session)
        if self._profit_margin_distribution is None:
            self.get_company_overview(session)
        if self._transaction_amount_distribution is None:
            self.get_transaction_overview(session)
        if self._stock_price_series is None:
            self.get_stock_holdings_overview(session)

        # Final guard in case the underlying datasets are genuinely empty
        if self._income_distribution is None:
            self._income_distribution = {label: 0 for label, *_ in self.INCOME_BUCKETS}
        if self._profit_margin_distribution is None:
            self._profit_margin_distribution = {label: 0 for label, *_ in self.PROFIT_MARGIN_BUCKETS}
        if self._transaction_amount_distribution is None:
            self._transaction_amount_distribution = {label: 0 for label, *_ in self.TRANSACTION_SIZE_BUCKETS}
        if self._stock_price_series is None:
            self._stock_price_series = []
            self._stock_price_series_label = "Top holding"
            self._stock_price_series_hint = None

        income_labels = [label for label, *_ in self.INCOME_BUCKETS]
        income_values = [
            float(self._income_distribution.get(label, 0))
            for label in income_labels
        ]

        margin_labels = [label for label, *_ in self.PROFIT_MARGIN_BUCKETS]
        margin_values = [
            float(self._profit_margin_distribution.get(label, 0))
            for label in margin_labels
        ]

        transaction_labels = [label for label, *_ in self.TRANSACTION_SIZE_BUCKETS]
        transaction_values = [
            float(self._transaction_amount_distribution.get(label, 0))
            for label in transaction_labels
        ]

        stock_series = self._stock_price_series or []
        stock_labels = [point[0] for point in stock_series]
        stock_values = [float(point[1]) for point in stock_series]
        stock_label = self._stock_price_series_label or "Top holding"

        return DashboardCharts(
            individuals_income=PieChartData(
                title="Users by income bracket",
                labels=income_labels,
                values=income_values,
                hint="Annualized salaries from the latest payroll snapshot.",
            ),
            companies_profit_margin=PieChartData(
                title="Companies by profit margin",
                labels=margin_labels,
                values=margin_values,
                hint="Margin computed from the most recent month's income and expenses.",
            ),
            transactions_amounts=PieChartData(
                title="Transactions by amount",
                labels=transaction_labels,
                values=transaction_values,
                hint=f"Distribution across the latest {int(sum(transaction_values))} posted transactions.",
            ),
            stock_price_trend=LineChartData(
                title=f"{stock_label} price trend",
                labels=stock_labels,
                values=stock_values,
                series_label="Closing price",
                hint=self._stock_price_series_hint,
            ),
        )
