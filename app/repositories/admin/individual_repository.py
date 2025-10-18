"""Data access helpers for admin individual views."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal

from sqlalchemy import text

from .base import BaseAdminRepository


@dataclass(frozen=True)
class IndividualSummaryRow:
    user_id: int
    user_name: str
    user_email: str | None
    net_worth: Decimal


@dataclass(frozen=True)
class IndividualRow:
    user_id: int
    name: str
    email: str | None


@dataclass(frozen=True)
class AccountSnapshotRow:
    account_id: int
    account_name: str | None
    account_type: str
    account_currency: str
    balance: Decimal


@dataclass(frozen=True)
class HoldingRow:
    account_id: int
    account_name: str | None
    instrument_id: int
    instrument_symbol: str
    instrument_name: str
    instrument_currency: str
    quantity: Decimal
    avg_cost: Decimal
    start_price: Decimal | None
    end_price: Decimal | None
    last_price: Decimal | None


@dataclass(frozen=True)
class CashflowRow:
    income_total: Decimal
    expense_total: Decimal


@dataclass(frozen=True)
class CategoryBreakdownRow:
    category_id: int | None
    category_name: str
    total_amount: Decimal


@dataclass(frozen=True)
class RecentTransactionRow:
    transaction_id: int
    posted_at: datetime
    txn_date: date
    amount: Decimal
    currency: str
    direction: str
    section_name: str | None
    category_name: str | None
    account_name: str | None
    description: str | None
    counterparty_id: int | None
    counterparty_name: str | None
    other_owner_type: str | None
    other_owner_id: int | None
    other_user_name: str | None
    other_org_name: str | None


class AdminIndividualRepository(BaseAdminRepository):
    """Repository encapsulating SQL for individual admin views."""

    def count_individuals(self, *, search: str | None = None) -> int:
        pattern = self._search_pattern(search)
        statement = text(
            """
            SELECT COUNT(*)
            FROM user u
            WHERE (
                :search IS NULL
                OR LOWER(u.name) LIKE :search
                OR LOWER(COALESCE(u.email, '')) LIKE :search
            )
            """
        )
        return self._scalar(statement, {"search": pattern})

    def fetch_individual_summaries(
        self,
        *,
        search: str | None,
        limit: int,
        offset: int,
    ) -> list[IndividualSummaryRow]:
        pattern = self._search_pattern(search)
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
                "search": pattern,
                "limit": limit,
                "offset": offset,
            },
        )
        rows: list[IndividualSummaryRow] = []
        for row in result.mappings():
            rows.append(
                IndividualSummaryRow(
                    user_id=int(row["user_id"]),
                    user_name=str(row["user_name"]),
                    user_email=row.get("user_email") or None,
                    net_worth=self._to_decimal(row.get("net_worth")),
                )
            )
        return rows

    def get_individual(self, user_id: int) -> IndividualRow | None:
        row = self._session.execute(
            text(
                """
                SELECT id, name, email
                FROM user
                WHERE id = :user_id
                """
            ),
            {"user_id": user_id},
        ).mappings().one_or_none()
        if row is None:
            return None
        return IndividualRow(
            user_id=int(row["id"]),
            name=str(row["name"]),
            email=row.get("email") or None,
        )

    def list_accounts(self, user_id: int) -> list[AccountSnapshotRow]:
        result = self._session.execute(
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
        accounts: list[AccountSnapshotRow] = []
        for row in result.mappings():
            accounts.append(
                AccountSnapshotRow(
                    account_id=int(row["account_id"]),
                    account_name=row.get("account_name") or None,
                    account_type=str(row["account_type"]),
                    account_currency=str(row["account_currency"]),
                    balance=self._to_decimal(row.get("balance")),
                )
            )
        return accounts

    def list_holdings(
        self,
        user_id: int,
        *,
        start_date: date,
        end_date: date,
    ) -> list[HoldingRow]:
        result = self._session.execute(
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
                "start_date": start_date.isoformat(),
                "end_date": end_date.isoformat(),
            },
        )
        holdings: list[HoldingRow] = []
        for row in result.mappings():
            holdings.append(
                HoldingRow(
                    account_id=int(row["account_id"]),
                    account_name=row.get("account_name") or None,
                    instrument_id=int(row["instrument_id"]),
                    instrument_symbol=str(row["instrument_symbol"]),
                    instrument_name=str(row["instrument_name"]),
                    instrument_currency=str(row["instrument_currency"]),
                    quantity=self._to_decimal(row.get("quantity")),
                    avg_cost=self._to_decimal(row.get("avg_cost")),
                    start_price=self._to_optional_decimal(row.get("start_price")),
                    end_price=self._to_optional_decimal(row.get("end_price")),
                    last_price=self._to_optional_decimal(row.get("last_price")),
                )
            )
        return holdings

    def get_cashflow(self, user_id: int, *, start_date: date, end_date: date) -> CashflowRow:
        row = self._session.execute(
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
                "start_date": start_date.isoformat(),
                "end_date": end_date.isoformat(),
            },
        ).mappings().one()
        return CashflowRow(
            income_total=self._to_decimal(row.get("income_total")),
            expense_total=self._to_decimal(row.get("expense_total")),
        )

    def list_income_breakdown(
        self,
        user_id: int,
        *,
        start_date: date,
        end_date: date,
    ) -> list[CategoryBreakdownRow]:
        result = self._session.execute(
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
                "start_date": start_date.isoformat(),
                "end_date": end_date.isoformat(),
            },
        )
        categories: list[CategoryBreakdownRow] = []
        for row in result.mappings():
            categories.append(
                CategoryBreakdownRow(
                    category_id=(
                        int(row["category_id"]) if row.get("category_id") is not None else None
                    ),
                    category_name=str(row["category_name"]),
                    total_amount=self._to_decimal(row.get("total_amount")),
                )
            )
        return categories

    def list_expense_breakdown(
        self,
        user_id: int,
        *,
        start_date: date,
        end_date: date,
    ) -> list[CategoryBreakdownRow]:
        result = self._session.execute(
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
                "start_date": start_date.isoformat(),
                "end_date": end_date.isoformat(),
            },
        )
        categories: list[CategoryBreakdownRow] = []
        for row in result.mappings():
            categories.append(
                CategoryBreakdownRow(
                    category_id=(
                        int(row["category_id"]) if row.get("category_id") is not None else None
                    ),
                    category_name=str(row["category_name"]),
                    total_amount=self._to_decimal(row.get("total_amount")),
                )
            )
        return categories

    def list_recent_transactions(self, user_id: int) -> list[RecentTransactionRow]:
        result = self._session.execute(
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
        transactions: list[RecentTransactionRow] = []
        for row in result.mappings():
            transactions.append(
                RecentTransactionRow(
                    transaction_id=int(row["transaction_id"]),
                    posted_at=self._coerce_datetime(row.get("posted_at")),
                    txn_date=self._coerce_date(row.get("txn_date")),
                    amount=self._to_decimal(row.get("amount")),
                    currency=str(row["currency"]),
                    direction=str(row["direction"]),
                    section_name=row.get("section_name"),
                    category_name=row.get("category_name"),
                    account_name=row.get("account_name"),
                    description=row.get("description"),
                    counterparty_id=(
                        int(row["counterparty_id"]) if row.get("counterparty_id") is not None else None
                    ),
                    counterparty_name=row.get("counterparty_name"),
                    other_owner_type=row.get("other_owner_type"),
                    other_owner_id=(
                        int(row["other_owner_id"]) if row.get("other_owner_id") is not None else None
                    ),
                    other_user_name=row.get("other_user_name"),
                    other_org_name=row.get("other_org_name"),
                )
            )
        return transactions

