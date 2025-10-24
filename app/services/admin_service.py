"""Service implementation for admin tooling."""
from __future__ import annotations

from collections import Counter
from datetime import date, timedelta
from decimal import Decimal
from typing import Sequence

from sqlalchemy import case, func, select, extract, tuple_, and_
from sqlalchemy.orm import Session, aliased

from app.core.logger import get_logger, timeit
from app.models import (
    Account,
    AccountOwnerType,
    AccountType,
    Category,
    Counterparty,
    Company,
    Individual,
    Instrument,
    Membership,
    UserSalaryMonthly,
    PositionAgg,
    PriceDaily,
    Section,
    Transaction,
    TransactionDirection,
)
from app.services.stocks_service import brokerage_aum_select
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

    def get_metrics(self, session: Session) -> AdminMetrics:
        """Return high level metrics for the administrative overview."""

        LOGGER.debug("Collecting admin metrics from the database")

        with timeit(
            "Admin dashboard metrics loading",
            logger=LOGGER,
            track_db_calls=True,
            session=session,
            unit="metrics"
        ) as timer:
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

        with timeit(
            "Individual user list loading",
            logger=LOGGER,
            track_db_calls=True,
            session=session,
            unit="users"
        ) as timer:
            # Latest monthly salary per user from precomputed table
            salary_latest = (
                select(
                    UserSalaryMonthly.user_id.label("user_id"),
                    UserSalaryMonthly.employer_org_id.label("employer_org_id"),
                    UserSalaryMonthly.salary_amount.label("monthly_income"),
                    func.row_number().over(
                        partition_by=UserSalaryMonthly.user_id,
                        order_by=(UserSalaryMonthly.year.desc(), UserSalaryMonthly.month.desc()),
                    ).label("rn"),
                )
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
                    Individual.job_title,
                    func.coalesce(salary_latest.c.monthly_income, 0).label("monthly_income"),
                    func.coalesce(balance_totals.c.checking_balance, 0).label("checking_balance"),
                    func.coalesce(balance_totals.c.savings_balance, 0).label("savings_balance"),
                    func.coalesce(brokerage_aum_cte.c.brokerage_aum, 0).label("brokerage_aum"),
                    Company.name.label("employer_name"),
                )
                .select_from(Individual)
                .outerjoin(salary_latest, (salary_latest.c.user_id == Individual.id) & (salary_latest.c.rn == 1))
                .outerjoin(Company, Company.id == salary_latest.c.employer_org_id)
                .outerjoin(balance_totals, balance_totals.c.user_id == Individual.id)
                .outerjoin(brokerage_aum_cte, brokerage_aum_cte.c.owner_id == Individual.id)
                .order_by(Individual.name)
            )

            rows = []
            income_counts: Counter[str] = Counter()
            for record in session.execute(overview_query).all():
                employer_name = record.employer_name

                search_terms = [record.name]
                if employer_name:
                    search_terms.append(employer_name)
                if record.job_title:
                    search_terms.append(record.job_title)

                monthly_income = Decimal(record.monthly_income or 0)
                annual_income = monthly_income * Decimal(12)
                income_bucket = self._categorize_income(annual_income)
                income_counts[income_bucket] += 1

                rows.append(
                    ListViewRow(
                        key=str(record.individual_id),
                        values={
                            "name": record.name,
                            "job_title": record.job_title or "",
                            "employer": employer_name,
                            "monthly_income": record.monthly_income,
                            "checking_aum": record.checking_balance,
                            "savings_aum": record.savings_balance,
                            "brokerage_aum": record.brokerage_aum,
                        },
                        search_text=" ".join(filter(None, search_terms)).lower(),
                        links={"name": f"/individuals/{record.individual_id}"},
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

        return list_view

    def get_company_overview(self, session: Session) -> ListView:
        """Return a reusable list view model for corporate users."""

        LOGGER.debug("Collecting company overview data for admin dashboard")

        with timeit(
            "Company list loading",
            logger=LOGGER,
            track_db_calls=True,
            session=session,
            unit="companies"
        ) as timer:
            # Use batch processing to reduce N+1 queries while maintaining logic compatibility
            # Get all companies first
            companies_query = select(Company.id, Company.name).order_by(Company.name)
            companies = session.execute(companies_query).all()

            if not companies:
                timer.set_total(0)
                self._profit_margin_distribution = {
                    label: 0 for label, *_ in self.PROFIT_MARGIN_BUCKETS
                }
                return ListView(
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

            # Batch the expensive operations by loading all company names first
            company_ids = [company.id for company in companies]
            company_names = {company.id: company.name for company in companies}

            # Batch load latest months for all companies in one query
            latest_months_query = (
                select(
                    Account.owner_id.label("company_id"),
                    extract("year", Transaction.txn_date).label("year"),
                    extract("month", Transaction.txn_date).label("month"),
                    func.row_number().over(
                        partition_by=Account.owner_id,
                        order_by=Transaction.txn_date.desc()
                    ).label("rn")
                )
                .select_from(Account)
                .join(Transaction, Transaction.account_id == Account.id)
                .where(
                    Account.owner_type == AccountOwnerType.ORG,
                    Account.owner_id.in_(company_ids)
                )
            )

            latest_months = session.execute(latest_months_query).all()
            latest_month_map = {}
            for row in latest_months:
                if row.rn == 1:  # Only the most recent month
                    latest_month_map[row.company_id] = (row.year, row.month)

            # Employee counts from membership
            employee_counts = session.execute(
                select(Membership.org_id, func.count(func.distinct(Membership.user_id)))
                .where(Membership.org_id.in_(company_ids), Membership.is_primary == True)  # noqa: E712
                .group_by(Membership.org_id)
            ).all()
            employee_count_map = {org_id: cnt for org_id, cnt in employee_counts}

            # Monthly salary cost from precomputed table at latest month per company
            monthly_salary_map: dict[int, Decimal] = {}
            if latest_month_map:
                # Query salaries filtered to latest ym per company
                union_rows = []
                for cid, (y, m) in latest_month_map.items():
                    union_rows.append((cid, y, m))
                # Build a single SELECT with IN filters
                salary_rows = session.execute(
                    select(
                        UserSalaryMonthly.employer_org_id,
                        func.sum(UserSalaryMonthly.salary_amount),
                    )
                    .where(
                        tuple_(UserSalaryMonthly.employer_org_id, UserSalaryMonthly.year, UserSalaryMonthly.month).in_(
                            union_rows
                        )
                    )
                    .group_by(UserSalaryMonthly.employer_org_id)
                ).all()
                for org_id, total in salary_rows:
                    monthly_salary_map[org_id] = Decimal(total)

            # Batch load income/expense data for all companies
            financial_query = (
                select(
                    Account.owner_id.label("company_id"),
                    Section.name.label("section_name"),
                    Transaction.direction,
                    Transaction.amount,
                    extract("year", Transaction.txn_date).label("year"),
                    extract("month", Transaction.txn_date).label("month")
                )
                .select_from(Account)
                .join(Transaction, Transaction.account_id == Account.id)
                .join(Section, Section.id == Transaction.section_id)
                .where(
                    Account.owner_type == AccountOwnerType.ORG,
                    Account.owner_id.in_(company_ids),
                    Section.name.in_(["income", "expense"])
                )
            )

            financial_data = session.execute(financial_query).all()

            # Process financial data
            monthly_income_map = {}
            monthly_expenses_map = {}
            total_profit_map = {}

            for row in financial_data:
                company_id = row.company_id
                year = row.year
                month = row.month

                # Check if this is the latest month for this company
                if company_id in latest_month_map:
                    latest_year, latest_month = latest_month_map[company_id]
                    is_latest_month = (year == latest_year and month == latest_month)
                else:
                    is_latest_month = False

                amount = float(row.amount)

                if row.section_name == "income":
                    if row.direction == TransactionDirection.CREDIT:
                        # Monthly income
                        if is_latest_month:
                            if company_id not in monthly_income_map:
                                monthly_income_map[company_id] = 0
                            monthly_income_map[company_id] += amount

                        # Total profit (all time)
                        if company_id not in total_profit_map:
                            total_profit_map[company_id] = 0
                        total_profit_map[company_id] += amount

                elif row.section_name == "expense":
                    if row.direction == TransactionDirection.DEBIT:
                        # Monthly expenses
                        if is_latest_month:
                            if company_id not in monthly_expenses_map:
                                monthly_expenses_map[company_id] = 0
                            monthly_expenses_map[company_id] += amount

                        # Total profit (all time) - expenses subtract
                        if company_id not in total_profit_map:
                            total_profit_map[company_id] = 0
                        total_profit_map[company_id] -= amount

            # Build result rows
            rows = []
            profit_margin_counts: Counter[str] = Counter()
            for company in companies:
                company_id = company.id
                company_name = company_names[company_id]

                monthly_income = Decimal(monthly_income_map.get(company_id, 0))
                monthly_expenses = Decimal(monthly_expenses_map.get(company_id, 0))
                profit_total = Decimal(total_profit_map.get(company_id, 0))

                margin_ratio: Decimal | None = None
                if monthly_income > 0:
                    monthly_profit = monthly_income - monthly_expenses
                    margin_ratio = (monthly_profit / monthly_income)

                margin_bucket = self._categorize_margin(monthly_income, margin_ratio)
                profit_margin_counts[margin_bucket] += 1

                rows.append(
                    ListViewRow(
                        key=str(company_id),
                        values={
                            "name": company_name,
                            "employee_count": int(employee_count_map.get(company_id, 0)),
                            "monthly_salary_cost": Decimal(monthly_salary_map.get(company_id, 0)),
                            "monthly_income": monthly_income,
                            "monthly_expenses": monthly_expenses,
                            "profit_total": profit_total,
                        },
                        search_text=company_name.lower(),
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

        return list_view

    def get_stock_holdings_overview(self, session: Session) -> ListView:
        """Return a list view representation of stock holdings by product."""

        LOGGER.debug("Collecting stock holdings overview data for admin dashboard")

        with timeit(
            "Stock holdings list loading",
            logger=LOGGER,
            track_db_calls=True,
            session=session,
            unit="products",
        ) as timer:
            # Get simulation period from transaction data
            first_transaction_at = session.execute(select(func.min(Transaction.posted_at))).scalar_one()
            last_transaction_at = session.execute(select(func.max(Transaction.posted_at))).scalar_one()
            
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
                .where(
                    Account.owner_type == AccountOwnerType.USER,
                    Account.type == AccountType.BROKERAGE,
                )
                .group_by(PositionAgg.instrument_id)
                .cte("user_positions")
            )

            # Use window functions for efficient price lookup
            # Get the earliest price >= simulation start date
            start_prices = (
                select(
                    PriceDaily.instrument_id.label("instrument_id"),
                    PriceDaily.close_price.label("start_price"),
                    PriceDaily.price_date.label("start_date"),
                    func.row_number().over(
                        partition_by=PriceDaily.instrument_id,
                        order_by=PriceDaily.price_date
                    ).label("rn")
                )
                .select_from(PriceDaily)
                .where(
                    PriceDaily.price_date >= simulation_start_date
                )
                .subquery()
            )

            # Get the latest price <= simulation end date
            end_prices = (
                select(
                    PriceDaily.instrument_id.label("instrument_id"),
                    PriceDaily.close_price.label("end_price"),
                    PriceDaily.price_date.label("end_date"),
                    func.row_number().over(
                        partition_by=PriceDaily.instrument_id,
                        order_by=PriceDaily.price_date.desc()
                    ).label("rn")
                )
                .select_from(PriceDaily)
                .where(
                    PriceDaily.price_date <= simulation_end_date
                )
                .subquery()
            )

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
                    select(PriceDaily.price_date, PriceDaily.close_price)
                    .where(PriceDaily.instrument_id == instrument_id)
                )
                
                if simulation_start_date:
                    query = query.where(PriceDaily.price_date >= simulation_start_date)
                if simulation_end_date:
                    query = query.where(PriceDaily.price_date <= simulation_end_date)
                
                query = query.order_by(PriceDaily.price_date)
                price_rows = session.execute(query).all()

                if price_rows:
                    self._stock_price_series = [
                        (row.price_date.isoformat(), float(row.close_price))
                        for row in price_rows
                        if row.close_price is not None
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
            .join(PriceDaily, PriceDaily.instrument_id == Instrument.id)
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
        
        # Get simulation period from transaction data
        first_transaction_at = session.execute(select(func.min(Transaction.posted_at))).scalar_one()
        last_transaction_at = session.execute(select(func.max(Transaction.posted_at))).scalar_one()
        
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
            select(PriceDaily.price_date, PriceDaily.close_price)
            .where(PriceDaily.instrument_id == instrument_id)
        )
        
        if simulation_start_date:
            query = query.where(PriceDaily.price_date >= simulation_start_date)
        if simulation_end_date:
            query = query.where(PriceDaily.price_date <= simulation_end_date)
        
        query = query.order_by(PriceDaily.price_date)
        price_rows = session.execute(query).all()
        
        if not price_rows:
            return [], label, None
        
        series = [
            (row.price_date.isoformat(), float(row.close_price))
            for row in price_rows
            if row.close_price is not None
        ]
        
        start_date_str = simulation_start_date.isoformat() if simulation_start_date else "start"
        end_date_str = simulation_end_date.isoformat() if simulation_end_date else "end"
        hint = f"{label} closing prices across {len(series)} sessions ({start_date_str} to {end_date_str})"
        
        return series, label, hint

    def get_transaction_overview(self, session: Session, limit: int = 500) -> ListView:
        """Return a reusable list view model for recent transactions."""

        LOGGER.debug("Collecting transaction overview data for admin dashboard")

        with timeit(
            "Transaction list loading",
            logger=LOGGER,
            track_db_calls=True,
            session=session,
            unit="transactions"
        ) as timer:
            latest_transactions = (
                select(
                    Transaction.id,
                    Transaction.account_id,
                    Transaction.txn_date,
                    Transaction.posted_at,
                    Transaction.amount,
                    Transaction.currency,
                    Transaction.direction,
                    Transaction.category_id,
                    Transaction.description,
                    Transaction.counterparty_id,
                    Transaction.transfer_group_id,
                )
                .order_by(Transaction.posted_at.desc())
                .limit(limit)
                .cte("latest_transactions")
            )

            account_alias = aliased(Account)
            partner_account = aliased(Account)
            partner_txn = aliased(Transaction)
            owner_individual = aliased(Individual)
            owner_company = aliased(Company)
            partner_individual = aliased(Individual)
            partner_company = aliased(Company)
            category_alias = aliased(Category)
            counterparty_alias = aliased(Counterparty)

            transactions_query = (
                select(
                    latest_transactions.c.id.label("transaction_id"),
                    latest_transactions.c.txn_date,
                    latest_transactions.c.posted_at,
                    latest_transactions.c.direction,
                    latest_transactions.c.amount,
                    latest_transactions.c.currency,
                    latest_transactions.c.description,
                    category_alias.name.label("category_name"),
                    counterparty_alias.name.label("counterparty_name"),
                    func.coalesce(owner_individual.name, owner_company.name).label("account_owner_name"),
                    func.coalesce(partner_individual.name, partner_company.name).label("partner_owner_name"),
                )
                .select_from(latest_transactions)
                .join(account_alias, account_alias.id == latest_transactions.c.account_id)
                .outerjoin(
                    owner_individual,
                    and_(
                        account_alias.owner_type == AccountOwnerType.USER,
                        owner_individual.id == account_alias.owner_id,
                    ),
                )
                .outerjoin(
                    owner_company,
                    and_(
                        account_alias.owner_type == AccountOwnerType.ORG,
                        owner_company.id == account_alias.owner_id,
                    ),
                )
                .outerjoin(category_alias, category_alias.id == latest_transactions.c.category_id)
                .outerjoin(counterparty_alias, counterparty_alias.id == latest_transactions.c.counterparty_id)
                .outerjoin(
                    partner_txn,
                    and_(
                        latest_transactions.c.transfer_group_id.isnot(None),
                        partner_txn.transfer_group_id == latest_transactions.c.transfer_group_id,
                        partner_txn.id != latest_transactions.c.id,
                    ),
                )
                .outerjoin(partner_account, partner_account.id == partner_txn.account_id)
                .outerjoin(
                    partner_individual,
                    and_(
                        partner_account.owner_type == AccountOwnerType.USER,
                        partner_individual.id == partner_account.owner_id,
                    ),
                )
                .outerjoin(
                    partner_company,
                    and_(
                        partner_account.owner_type == AccountOwnerType.ORG,
                        partner_company.id == partner_account.owner_id,
                    ),
                )
                .order_by(latest_transactions.c.posted_at.desc())
            )

            records = session.execute(transactions_query).all()

            rows: list[ListViewRow] = []
            transaction_amount_counts: Counter[str] = Counter()

            for record in records:
                owner_name = record.account_owner_name or "Unknown account"
                counterparty_name = record.counterparty_name
                partner_name = record.partner_owner_name

                if record.direction == TransactionDirection.DEBIT:
                    payer = owner_name
                    payee = counterparty_name or partner_name or "External counterparty"
                else:
                    payer = counterparty_name or partner_name or "External counterparty"
                    payee = owner_name

                search_terms = [
                    str(record.txn_date),
                    payer,
                    payee,
                ]

                if record.category_name:
                    search_terms.append(record.category_name)
                if record.description:
                    search_terms.append(record.description)

                amount_value = Decimal(record.amount or 0)
                amount_bucket = self._categorize_transaction_amount(abs(amount_value))
                transaction_amount_counts[amount_bucket] += 1

                rows.append(
                    ListViewRow(
                        key=str(record.transaction_id),
                        values={
                            "date": record.txn_date.strftime("%Y-%m-%d"),
                            "payer": payer,
                            "payee": payee,
                            "amount": record.amount,
                            "category": record.category_name or "Uncategorized",
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

        return list_view

    def get_dashboard_charts(self, session: Session) -> DashboardCharts:
        """Aggregate chart payloads for the admin dashboard."""

        if self._income_distribution is None:
            self.get_individual_overview(session)
        if self._profit_margin_distribution is None:
            self.get_company_overview(session)
        if self._transaction_amount_distribution is None:
            self.get_transaction_overview(session)
        if self._stock_price_series is None:
            self.get_stock_holdings_overview(session)

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
