"""Service implementation for admin tooling."""
from __future__ import annotations

from datetime import date, timedelta
from decimal import Decimal

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
from app.schemas.admin import AdminMetrics, ListView, ListViewColumn, ListViewRow

LOGGER = get_logger(__name__)


class AdminService:
    """Service encapsulating administrator dashboard workflows."""

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
            for record in session.execute(overview_query).all():
                employer_name = record.employer_name

                search_terms = [record.name]
                if employer_name:
                    search_terms.append(employer_name)
                if record.job_title:
                    search_terms.append(record.job_title)

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
            for company in companies:
                company_id = company.id
                company_name = company_names[company_id]

                rows.append(
                    ListViewRow(
                        key=str(company_id),
                        values={
                            "name": company_name,
                            "employee_count": int(employee_count_map.get(company_id, 0)),
                            "monthly_salary_cost": Decimal(monthly_salary_map.get(company_id, 0)),
                            "monthly_income": Decimal(monthly_income_map.get(company_id, 0)),
                            "monthly_expenses": Decimal(monthly_expenses_map.get(company_id, 0)),
                            "profit_total": Decimal(total_profit_map.get(company_id, 0)),
                        },
                        search_text=company_name.lower(),
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

            price_extents = (
                select(
                    PriceDaily.instrument_id.label("instrument_id"),
                    func.min(PriceDaily.price_date).label("start_date"),
                    func.max(PriceDaily.price_date).label("end_date"),
                )
                .group_by(PriceDaily.instrument_id)
                .subquery()
            )

            start_prices = (
                select(
                    PriceDaily.instrument_id.label("instrument_id"),
                    PriceDaily.close_price.label("start_price"),
                )
                .select_from(PriceDaily)
                .join(
                    price_extents,
                    and_(
                        PriceDaily.instrument_id == price_extents.c.instrument_id,
                        PriceDaily.price_date == price_extents.c.start_date,
                    ),
                )
                .subquery()
            )

            end_prices = (
                select(
                    PriceDaily.instrument_id.label("instrument_id"),
                    PriceDaily.close_price.label("end_price"),
                )
                .select_from(PriceDaily)
                .join(
                    price_extents,
                    and_(
                        PriceDaily.instrument_id == price_extents.c.instrument_id,
                        PriceDaily.price_date == price_extents.c.end_date,
                    ),
                )
                .subquery()
            )

            holdings_query = (
                select(
                    Instrument.id.label("instrument_id"),
                    Instrument.symbol,
                    Instrument.name,
                    start_prices.c.start_price,
                    end_prices.c.end_price,
                    user_positions.c.total_qty,
                    user_positions.c.market_value,
                )
                .select_from(user_positions)
                .join(Instrument, Instrument.id == user_positions.c.instrument_id)
                .outerjoin(start_prices, start_prices.c.instrument_id == Instrument.id)
                .outerjoin(end_prices, end_prices.c.instrument_id == Instrument.id)
                .order_by(Instrument.symbol)
            )

            records = session.execute(holdings_query).all()

            rows: list[ListViewRow] = []

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

            timer.set_total(len(rows))

            list_view = ListView(
                title="Stock holdings",
                columns=[
                    ListViewColumn(key="product", title="Product"),
                    ListViewColumn(
                        key="start_price",
                        title="Start price",
                        column_type="currency",
                        align="right",
                    ),
                    ListViewColumn(
                        key="end_price",
                        title="End price",
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

        return list_view

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

        return list_view
