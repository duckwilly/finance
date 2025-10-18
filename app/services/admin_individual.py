"""Service logic for individual-focused admin views."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta
from decimal import Decimal
from math import ceil
from typing import Iterable, Mapping

from sqlalchemy import text
from sqlalchemy.engine import Result
from sqlalchemy.orm import Session
from sqlalchemy.sql.elements import TextClause


@dataclass(frozen=True)
class IndividualSummary:
    """Aggregated snapshot of a user's finances."""

    user_id: int
    name: str
    email: str | None
    net_worth: Decimal


@dataclass(frozen=True)
class IndividualPage:
    """Paginated collection of individual summaries."""

    items: list[IndividualSummary]
    total: int
    page: int
    page_size: int

    @property
    def total_pages(self) -> int:
        if self.total == 0:
            return 1
        return ceil(self.total / self.page_size)


@dataclass(frozen=True)
class PeriodRange:
    """Normalized representation of a reporting period."""

    key: str
    label: str
    start: date
    end: date


@dataclass(frozen=True)
class AccountSnapshot:
    """Balance information for a user's account."""

    account_id: int
    name: str | None
    type: str
    currency: str
    balance: Decimal


@dataclass(frozen=True)
class HoldingPerformance:
    """Performance metrics for a brokerage holding."""

    account_id: int
    account_name: str | None
    instrument_id: int
    symbol: str
    name: str
    currency: str
    quantity: Decimal
    avg_cost: Decimal
    start_price: Decimal | None
    end_price: Decimal | None
    price_change: Decimal
    price_change_pct: Decimal | None
    period_pl: Decimal
    market_value: Decimal


@dataclass(frozen=True)
class CategoryBreakdown:
    """Aggregate totals for a transaction category."""

    category_id: int | None
    name: str
    amount: Decimal


@dataclass(frozen=True)
class CounterpartyInfo:
    """Representation of a transaction counterparty."""

    counterparty_id: int | None
    name: str | None


@dataclass(frozen=True)
class RelatedParty:
    """Linked organization or user involved in a transfer."""

    type: str
    party_id: int
    name: str


@dataclass(frozen=True)
class RecentTransaction:
    """Recent transaction enriched with metadata."""

    transaction_id: int
    posted_at: datetime
    txn_date: date
    signed_amount: Decimal
    currency: str
    section: str
    category: str | None
    account_name: str | None
    description: str | None
    counterparty: CounterpartyInfo | None
    related_party: RelatedParty | None


@dataclass(frozen=True)
class IndividualDetail:
    """Composite detail view for a single user."""

    user_id: int
    name: str
    email: str | None
    net_worth: Decimal
    cash_total: Decimal
    holdings_total: Decimal
    portfolio_gain_loss: Decimal
    accounts: list[AccountSnapshot]
    holdings: list[HoldingPerformance]
    income_total: Decimal
    expense_total: Decimal
    net_cash_flow: Decimal
    income_breakdown: list[CategoryBreakdown]
    expense_breakdown: list[CategoryBreakdown]
    period: PeriodRange
    recent_transactions: list[RecentTransaction]


class AdminIndividualService:
    """Facade exposing individual level aggregates for administrators."""

    _PERIOD_DEFAULT = "mtd"
    _PERIOD_LABELS: dict[str, str] = {
        "mtd": "Month to date",
        "qtd": "Quarter to date",
        "ytd": "Year to date",
        "last_30_days": "Last 30 days",
        "last_90_days": "Last 90 days",
        "last_12_months": "Last 12 months",
    }
    _PERIOD_ORDER: tuple[str, ...] = (
        "mtd",
        "qtd",
        "ytd",
        "last_30_days",
        "last_90_days",
        "last_12_months",
    )

    def __init__(self, session: Session) -> None:
        self._session = session

    def list_individuals(
        self,
        *,
        page: int,
        page_size: int,
        search: str | None = None,
    ) -> IndividualPage:
        """Return a paginated list of user summaries."""

        if page < 1:
            page = 1
        if page_size < 1:
            raise ValueError("page_size must be greater than zero")

        search_pattern = f"%{search.lower()}%" if search else None

        total = self._scalar(
            text(
                """
                SELECT COUNT(*)
                FROM user u
                WHERE (
                    :search IS NULL
                    OR LOWER(u.name) LIKE :search
                    OR LOWER(COALESCE(u.email, '')) LIKE :search
                )
                """
            ),
            {"search": search_pattern},
        )

        max_page = max(1, ceil(total / page_size)) if total else 1
        page = min(page, max_page)
        offset = (page - 1) * page_size

        result = self._session.execute(
            text(
                """
                WITH filtered_users AS (
                    SELECT u.id, u.name, u.email
                    FROM user u
                    WHERE (
                        :search IS NULL
                        OR LOWER(u.name) LIKE :search
                        OR LOWER(COALESCE(u.email, '')) LIKE :search
                    )
                    ORDER BY u.name
                ),
                cash_balances AS (
                    SELECT
                        a.owner_id AS user_id,
                        COALESCE(SUM(v.balance), 0) AS total_balance
                    FROM account a
                    LEFT JOIN v_account_balance v ON v.account_id = a.id
                    WHERE a.owner_type = 'user'
                      AND a.owner_id IN (SELECT id FROM filtered_users)
                    GROUP BY a.owner_id
                ),
                holding_values AS (
                    SELECT
                        a.owner_id AS user_id,
                        COALESCE(SUM(pa.qty * pa.last_price), 0) AS holdings_total
                    FROM account a
                    JOIN position_agg pa ON pa.account_id = a.id
                    WHERE a.owner_type = 'user'
                      AND a.owner_id IN (SELECT id FROM filtered_users)
                    GROUP BY a.owner_id
                )
                SELECT
                    fu.id AS user_id,
                    fu.name AS user_name,
                    fu.email AS user_email,
                    COALESCE(cb.total_balance, 0) + COALESCE(hv.holdings_total, 0) AS net_worth
                FROM filtered_users fu
                LEFT JOIN cash_balances cb ON cb.user_id = fu.id
                LEFT JOIN holding_values hv ON hv.user_id = fu.id
                ORDER BY fu.name
                LIMIT :limit OFFSET :offset
                """
            ),
            {
                "search": search_pattern,
                "limit": page_size,
                "offset": offset,
            },
        )

        items = self._parse_summary_rows(result.mappings())

        return IndividualPage(
            items=items,
            total=total,
            page=page,
            page_size=page_size,
        )

    def get_individual_detail(
        self,
        user_id: int,
        *,
        period_key: str | None = None,
        start_date: date | None = None,
        end_date: date | None = None,
        today: date | None = None,
    ) -> IndividualDetail | None:
        """Return detail information for a user."""

        reference_today = today or date.today()

        range_start = start_date
        range_end = end_date

        if range_start is not None and range_end is not None:
            if range_start > range_end:
                range_start, range_end = range_end, range_start
            if range_end > reference_today:
                range_end = reference_today
            period = PeriodRange(
                key="custom",
                label="Custom range",
                start=range_start,
                end=range_end,
            )
        else:
            period = self._resolve_period(period_key, today=reference_today)
            range_start = period.start
            range_end = period.end

        user_row = self._session.execute(
            text(
                """
                SELECT id, name, email
                FROM user
                WHERE id = :user_id
                """
            ),
            {"user_id": user_id},
        ).mappings().one_or_none()

        if user_row is None:
            return None

        accounts_result = self._session.execute(
            text(
                """
                SELECT
                    a.id AS account_id,
                    a.name AS account_name,
                    a.type AS account_type,
                    a.currency AS account_currency,
                    COALESCE(v.balance, 0) AS balance
                FROM account a
                LEFT JOIN v_account_balance v ON v.account_id = a.id
                WHERE a.owner_type = 'user' AND a.owner_id = :user_id
                ORDER BY
                    CASE WHEN a.name IS NULL OR a.name = '' THEN 1 ELSE 0 END,
                    a.name,
                    a.id
                """
            ),
            {"user_id": user_id},
        )

        accounts = [
            AccountSnapshot(
                account_id=int(row["account_id"]),
                name=row.get("account_name") or None,
                type=str(row["account_type"]),
                currency=str(row["account_currency"]),
                balance=self._to_decimal(row.get("balance")),
            )
            for row in accounts_result.mappings()
        ]

        cash_total = sum((account.balance for account in accounts), Decimal(0))

        holdings_result = self._session.execute(
            text(
                """
                SELECT
                    a.id AS account_id,
                    a.name AS account_name,
                    i.id AS instrument_id,
                    i.symbol AS instrument_symbol,
                    i.name AS instrument_name,
                    i.currency AS instrument_currency,
                    pa.qty AS quantity,
                    pa.avg_cost AS avg_cost,
                    (
                        SELECT pd.close_price
                        FROM price_daily pd
                        WHERE pd.instrument_id = pa.instrument_id
                          AND pd.price_date <= :start_date
                        ORDER BY pd.price_date DESC
                        LIMIT 1
                    ) AS start_price,
                    (
                        SELECT pd.close_price
                        FROM price_daily pd
                        WHERE pd.instrument_id = pa.instrument_id
                          AND pd.price_date <= :end_date
                        ORDER BY pd.price_date DESC
                        LIMIT 1
                    ) AS end_price,
                    pa.last_price AS last_price
                FROM position_agg pa
                JOIN account a ON a.id = pa.account_id
                JOIN instrument i ON i.id = pa.instrument_id
                WHERE a.owner_type = 'user' AND a.owner_id = :user_id
                ORDER BY i.symbol
                """
            ),
            {
                "user_id": user_id,
                "start_date": range_start.isoformat(),
                "end_date": range_end.isoformat(),
            },
        )

        holdings: list[HoldingPerformance] = []
        holdings_total = Decimal(0)
        portfolio_gain_loss = Decimal(0)

        for row in holdings_result.mappings():
            quantity = self._to_decimal(row.get("quantity"))
            avg_cost = self._to_decimal(row.get("avg_cost"))
            start_price = self._to_optional_decimal(row.get("start_price"))
            end_price = self._to_optional_decimal(row.get("end_price"))
            last_price = self._to_optional_decimal(row.get("last_price"))
            effective_end_price = end_price if end_price is not None else last_price
            if effective_end_price is None:
                effective_end_price = Decimal(0)
            market_value = quantity * effective_end_price
            price_change = Decimal(0)
            change_pct: Decimal | None = None
            period_pl = Decimal(0)
            if start_price is not None and effective_end_price is not None:
                price_change = effective_end_price - start_price
                period_pl = price_change * quantity
                if start_price != 0:
                    change_pct = (price_change / start_price) * Decimal("100")
            holdings_total += market_value
            portfolio_gain_loss += period_pl
            holdings.append(
                HoldingPerformance(
                    account_id=int(row["account_id"]),
                    account_name=row.get("account_name") or None,
                    instrument_id=int(row["instrument_id"]),
                    symbol=str(row["instrument_symbol"]),
                    name=str(row["instrument_name"]),
                    currency=str(row["instrument_currency"]),
                    quantity=quantity,
                    avg_cost=avg_cost,
                    start_price=start_price,
                    end_price=effective_end_price,
                    price_change=price_change,
                    price_change_pct=change_pct,
                    period_pl=period_pl,
                    market_value=market_value,
                )
            )

        cashflow_row = self._session.execute(
            text(
                """
                SELECT
                    SUM(CASE
                        WHEN t.section_id = 1 THEN
                            CASE WHEN t.direction = 'CREDIT' THEN t.amount ELSE -t.amount END
                        ELSE 0
                    END) AS income_total,
                    SUM(CASE
                        WHEN t.section_id = 2 THEN
                            CASE WHEN t.direction = 'DEBIT' THEN t.amount ELSE -t.amount END
                        ELSE 0
                    END) AS expense_total
                FROM account a
                LEFT JOIN `transaction` t
                    ON t.account_id = a.id
                   AND t.txn_date BETWEEN :start_date AND :end_date
                WHERE a.owner_type = 'user' AND a.owner_id = :user_id
                """
            ),
            {
                "user_id": user_id,
                "start_date": range_start.isoformat(),
                "end_date": range_end.isoformat(),
            },
        ).mappings().one()

        income_total = self._to_decimal(cashflow_row.get("income_total"))
        expense_total = self._to_decimal(cashflow_row.get("expense_total")).copy_abs()
        net_cash_flow = income_total - expense_total

        income_breakdown_rows = self._session.execute(
            text(
                """
                SELECT
                    t.category_id AS category_id,
                    COALESCE(c.name, 'Uncategorized') AS category_name,
                    SUM(CASE WHEN t.direction = 'CREDIT' THEN t.amount ELSE -t.amount END) AS total_amount
                FROM account a
                JOIN `transaction` t ON t.account_id = a.id
                LEFT JOIN category c ON c.id = t.category_id
                WHERE a.owner_type = 'user'
                  AND a.owner_id = :user_id
                  AND t.txn_date BETWEEN :start_date AND :end_date
                  AND t.section_id = 1
                GROUP BY t.category_id, c.name
                HAVING ABS(total_amount) > 0
                ORDER BY total_amount DESC
                """
            ),
            {
                "user_id": user_id,
                "start_date": range_start.isoformat(),
                "end_date": range_end.isoformat(),
            },
        )

        income_breakdown = [
            CategoryBreakdown(
                category_id=(int(row["category_id"]) if row.get("category_id") is not None else None),
                name=str(row["category_name"]),
                amount=self._to_decimal(row.get("total_amount")),
            )
            for row in income_breakdown_rows.mappings()
        ]

        expense_breakdown_rows = self._session.execute(
            text(
                """
                SELECT
                    t.category_id AS category_id,
                    COALESCE(c.name, 'Uncategorized') AS category_name,
                    SUM(CASE WHEN t.direction = 'DEBIT' THEN t.amount ELSE -t.amount END) AS total_amount
                FROM account a
                JOIN `transaction` t ON t.account_id = a.id
                LEFT JOIN category c ON c.id = t.category_id
                WHERE a.owner_type = 'user'
                  AND a.owner_id = :user_id
                  AND t.txn_date BETWEEN :start_date AND :end_date
                  AND t.section_id = 2
                GROUP BY t.category_id, c.name
                HAVING ABS(total_amount) > 0
                ORDER BY total_amount DESC
                """
            ),
            {
                "user_id": user_id,
                "start_date": range_start.isoformat(),
                "end_date": range_end.isoformat(),
            },
        )

        expense_breakdown = [
            CategoryBreakdown(
                category_id=(int(row["category_id"]) if row.get("category_id") is not None else None),
                name=str(row["category_name"]),
                amount=self._to_decimal(row.get("total_amount")).copy_abs(),
            )
            for row in expense_breakdown_rows.mappings()
        ]

        transactions_rows = self._session.execute(
            text(
                """
                SELECT
                    t.id AS transaction_id,
                    t.posted_at AS posted_at,
                    t.txn_date AS txn_date,
                    t.amount AS amount,
                    t.currency AS currency,
                    t.direction AS direction,
                    s.name AS section_name,
                    c.name AS category_name,
                    a.name AS account_name,
                    t.description AS description,
                    t.counterparty_id AS counterparty_id,
                    cp.name AS counterparty_name,
                    tl.debit_txn_id,
                    tl.credit_txn_id,
                    other_txn.account_id AS other_account_id,
                    other_account.owner_type AS other_owner_type,
                    other_account.owner_id AS other_owner_id,
                    other_user.name AS other_user_name,
                    other_org.name AS other_org_name
                FROM account a
                JOIN `transaction` t ON t.account_id = a.id
                LEFT JOIN section s ON s.id = t.section_id
                LEFT JOIN category c ON c.id = t.category_id
                LEFT JOIN counterparty cp ON cp.id = t.counterparty_id
                LEFT JOIN transfer_link tl ON tl.debit_txn_id = t.id OR tl.credit_txn_id = t.id
                LEFT JOIN `transaction` other_txn ON other_txn.id = CASE
                    WHEN tl.debit_txn_id = t.id THEN tl.credit_txn_id
                    WHEN tl.credit_txn_id = t.id THEN tl.debit_txn_id
                    ELSE NULL
                END
                LEFT JOIN account other_account ON other_account.id = other_txn.account_id
                LEFT JOIN user other_user ON other_account.owner_type = 'user' AND other_user.id = other_account.owner_id
                LEFT JOIN org other_org ON other_account.owner_type = 'org' AND other_org.id = other_account.owner_id
                WHERE a.owner_type = 'user' AND a.owner_id = :user_id
                ORDER BY t.posted_at DESC
                LIMIT 20
                """
            ),
            {"user_id": user_id},
        )

        recent_transactions: list[RecentTransaction] = []
        for row in transactions_rows.mappings():
            direction = str(row["direction"])
            amount_value = self._to_decimal(row.get("amount"))
            signed_amount = amount_value if direction == "CREDIT" else -amount_value
            counterparty = None
            if row.get("counterparty_id") is not None or row.get("counterparty_name") is not None:
                counterparty = CounterpartyInfo(
                    counterparty_id=(int(row["counterparty_id"]) if row.get("counterparty_id") is not None else None),
                    name=row.get("counterparty_name"),
                )
            related_party = None
            owner_type = row.get("other_owner_type")
            owner_id = row.get("other_owner_id")
            if owner_type and owner_id:
                party_name = None
                party_type = None
                if owner_type == "user" and row.get("other_user_name"):
                    party_name = str(row["other_user_name"])
                    party_type = "user"
                elif owner_type == "org" and row.get("other_org_name"):
                    party_name = str(row["other_org_name"])
                    party_type = "company"
                if party_name and party_type:
                    related_party = RelatedParty(
                        type=party_type,
                        party_id=int(owner_id),
                        name=party_name,
                    )
            recent_transactions.append(
                RecentTransaction(
                    transaction_id=int(row["transaction_id"]),
                    posted_at=self._coerce_datetime(row.get("posted_at")),
                    txn_date=self._coerce_date(row.get("txn_date")),
                    signed_amount=signed_amount,
                    currency=str(row["currency"]),
                    section=row.get("section_name") or "",
                    category=row.get("category_name"),
                    account_name=row.get("account_name"),
                    description=row.get("description"),
                    counterparty=counterparty,
                    related_party=related_party,
                )
            )

        net_worth = cash_total + holdings_total

        return IndividualDetail(
            user_id=int(user_row["id"]),
            name=str(user_row["name"]),
            email=user_row.get("email"),
            net_worth=net_worth,
            cash_total=cash_total,
            holdings_total=holdings_total,
            portfolio_gain_loss=portfolio_gain_loss,
            accounts=accounts,
            holdings=holdings,
            income_total=income_total,
            expense_total=expense_total,
            net_cash_flow=net_cash_flow,
            income_breakdown=income_breakdown,
            expense_breakdown=expense_breakdown,
            period=period,
            recent_transactions=recent_transactions,
        )

    @classmethod
    def period_options(cls, *, today: date | None = None) -> list[PeriodRange]:
        """Return available reporting windows."""

        return [cls._resolve_period(key, today=today) for key in cls._PERIOD_ORDER]

    def _parse_summary_rows(
        self, rows: Iterable[Mapping[str, object]]
    ) -> list[IndividualSummary]:
        summaries: list[IndividualSummary] = []
        for row in rows:
            summaries.append(
                IndividualSummary(
                    user_id=int(row["user_id"]),
                    name=str(row["user_name"]),
                    email=row.get("user_email"),
                    net_worth=self._to_decimal(row.get("net_worth")),
                )
            )
        return summaries

    def _scalar(self, statement: TextClause, params: dict | None = None) -> int:
        result: Result = self._session.execute(statement, params or {})
        value = result.scalar() or 0
        return int(value)

    @staticmethod
    def _to_decimal(value: object) -> Decimal:
        if isinstance(value, Decimal):
            return value
        if value is None:
            return Decimal(0)
        return Decimal(str(value))

    @staticmethod
    def _to_optional_decimal(value: object) -> Decimal | None:
        if value is None:
            return None
        if isinstance(value, Decimal):
            return value
        return Decimal(str(value))

    @staticmethod
    def _coerce_date(value: object) -> date:
        if isinstance(value, date) and not isinstance(value, datetime):
            return value
        if isinstance(value, datetime):
            return value.date()
        if value is None:
            raise ValueError("Cannot convert None to date")
        text_value = str(value)
        if len(text_value) >= 10:
            text_value = text_value[:10]
        return date.fromisoformat(text_value)

    @staticmethod
    def _coerce_datetime(value: object) -> datetime:
        if isinstance(value, datetime):
            return value
        if isinstance(value, date):
            return datetime(value.year, value.month, value.day)
        if value is None:
            raise ValueError("Cannot convert None to datetime")
        text_value = str(value)
        try:
            return datetime.fromisoformat(text_value)
        except ValueError:
            if len(text_value) >= 19:
                return datetime.fromisoformat(text_value[:19])
            raise

    @classmethod
    def _resolve_period(
        cls,
        period_key: str | None,
        *,
        today: date | None = None,
    ) -> PeriodRange:
        resolved_key = (period_key or cls._PERIOD_DEFAULT).lower()
        if resolved_key not in cls._PERIOD_LABELS:
            resolved_key = cls._PERIOD_DEFAULT

        reference_date = today or date.today()

        if resolved_key == "mtd":
            start = reference_date.replace(day=1)
        elif resolved_key == "qtd":
            quarter_index = (reference_date.month - 1) // 3
            start_month = quarter_index * 3 + 1
            start = reference_date.replace(month=start_month, day=1)
        elif resolved_key == "last_30_days":
            start = reference_date - timedelta(days=29)
        elif resolved_key == "last_90_days":
            start = reference_date - timedelta(days=89)
        elif resolved_key == "last_12_months":
            start = reference_date - timedelta(days=364)
        else:  # ytd
            start = reference_date.replace(month=1, day=1)

        label = cls._PERIOD_LABELS[resolved_key]
        return PeriodRange(key=resolved_key, label=label, start=start, end=reference_date)
