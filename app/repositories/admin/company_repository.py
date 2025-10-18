"""Data access helpers for admin company views."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from sqlalchemy import text

from .base import BaseAdminRepository


@dataclass(frozen=True)
class CompanySummaryRow:
    org_id: int
    org_name: str
    total_balance: Decimal
    payroll_headcount: int


@dataclass(frozen=True)
class CompanyRow:
    org_id: int
    org_name: str


@dataclass(frozen=True)
class CompanyAccountRow:
    account_id: int
    account_name: str | None
    account_type: str
    account_currency: str
    balance: Decimal


@dataclass(frozen=True)
class CompanyMemberRow:
    user_id: int
    user_name: str
    user_email: str | None
    membership_role: str


@dataclass(frozen=True)
class CashflowRow:
    income_total: Decimal
    expense_total: Decimal


@dataclass(frozen=True)
class IncomeTransactionRow:
    txn_date: date
    normalized_amount: Decimal


@dataclass(frozen=True)
class PayrollEmployeeRow:
    counterparty_id: int | None
    counterparty_name: str
    total_paid: Decimal


@dataclass(frozen=True)
class ExpenseCategoryRow:
    category_id: int | None
    category_name: str
    total_spent: Decimal


class AdminCompanyRepository(BaseAdminRepository):
    """Repository encapsulating SQL for company admin views."""

    _PAYROLL_SECTION_ID = 2
    _PAYROLL_PATTERNS: tuple[str, str, str] = ("%payroll%", "%salary%", "%wage%")

    def count_companies(self, *, search: str | None = None) -> int:
        pattern = self._search_pattern(search)
        statement = text(
            """
            SELECT COUNT(*)
            FROM org o
            WHERE (:search IS NULL OR LOWER(o.name) LIKE :search)
            """
        )
        return self._scalar(statement, {"search": pattern})

    def fetch_company_summaries(
        self,
        *,
        search: str | None,
        limit: int,
        offset: int,
    ) -> list[CompanySummaryRow]:
        pattern = self._search_pattern(search)
        result = self._session.execute(
            text(
                """
                WITH filtered_orgs AS (
                    SELECT o.id, o.name
                    FROM org o
                    WHERE (:search IS NULL OR LOWER(o.name) LIKE :search)
                ),
                balance_agg AS (
                    SELECT fo.id AS org_id, COALESCE(SUM(v.balance), 0) AS total_balance
                    FROM filtered_orgs fo
                    LEFT JOIN account a
                        ON a.owner_type = 'org' AND a.owner_id = fo.id
                    LEFT JOIN v_account_balance v
                        ON v.account_id = a.id
                    GROUP BY fo.id
                ),
                payroll_agg AS (
                    SELECT
                        fo.id AS org_id,
                        COUNT(DISTINCT CASE
                            WHEN c.section_id = :expense_section
                             AND (
                                LOWER(c.name) LIKE :pattern_payroll OR
                                LOWER(c.name) LIKE :pattern_salary OR
                                LOWER(c.name) LIKE :pattern_wage
                             )
                            THEN t.counterparty_id
                        END) AS payroll_headcount
                    FROM filtered_orgs fo
                    LEFT JOIN account a
                        ON a.owner_type = 'org' AND a.owner_id = fo.id
                    LEFT JOIN `transaction` t
                        ON t.account_id = a.id
                    LEFT JOIN category c
                        ON c.id = t.category_id
                    GROUP BY fo.id
                )
                SELECT
                    fo.id AS org_id,
                    fo.name AS org_name,
                    COALESCE(balance_agg.total_balance, 0) AS total_balance,
                    COALESCE(payroll_agg.payroll_headcount, 0) AS payroll_headcount
                FROM filtered_orgs fo
                LEFT JOIN balance_agg ON balance_agg.org_id = fo.id
                LEFT JOIN payroll_agg ON payroll_agg.org_id = fo.id
                ORDER BY fo.name
                LIMIT :limit OFFSET :offset
                """
            ),
            {
                "search": pattern,
                "limit": limit,
                "offset": offset,
                "expense_section": self._PAYROLL_SECTION_ID,
                "pattern_payroll": self._PAYROLL_PATTERNS[0],
                "pattern_salary": self._PAYROLL_PATTERNS[1],
                "pattern_wage": self._PAYROLL_PATTERNS[2],
            },
        )
        rows: list[CompanySummaryRow] = []
        for row in result.mappings():
            rows.append(
                CompanySummaryRow(
                    org_id=int(row["org_id"]),
                    org_name=str(row["org_name"]),
                    total_balance=self._to_decimal(row.get("total_balance")),
                    payroll_headcount=int(row.get("payroll_headcount") or 0),
                )
            )
        return rows

    def get_company(self, org_id: int) -> CompanyRow | None:
        result = self._session.execute(
            text(
                """
                SELECT id, name
                FROM org
                WHERE id = :org_id
                """
            ),
            {"org_id": org_id},
        ).mappings().one_or_none()
        if result is None:
            return None
        return CompanyRow(org_id=int(result["id"]), org_name=str(result["name"]))

    def list_accounts(self, org_id: int) -> list[CompanyAccountRow]:
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
                WHERE a.owner_type = 'org' AND a.owner_id = :org_id
                ORDER BY
                    CASE WHEN a.name IS NULL OR a.name = '' THEN 1 ELSE 0 END,
                    a.name,
                    a.id
                """
            ),
            {"org_id": org_id},
        )
        rows: list[CompanyAccountRow] = []
        for row in result.mappings():
            rows.append(
                CompanyAccountRow(
                    account_id=int(row["account_id"]),
                    account_name=row.get("account_name") or None,
                    account_type=str(row["account_type"]),
                    account_currency=str(row["account_currency"]),
                    balance=self._to_decimal(row.get("balance")),
                )
            )
        return rows

    def get_payroll_headcount(self, org_id: int) -> int:
        statement = text(
            """
            SELECT COUNT(DISTINCT CASE
                WHEN t.section_id = :expense_section
                 AND (
                    LOWER(c.name) LIKE :pattern_payroll OR
                    LOWER(c.name) LIKE :pattern_salary OR
                    LOWER(c.name) LIKE :pattern_wage
                 )
                THEN t.counterparty_id
            END) AS payroll_headcount
            FROM account a
            LEFT JOIN `transaction` t ON t.account_id = a.id
            LEFT JOIN category c ON c.id = t.category_id
            WHERE a.owner_type = 'org' AND a.owner_id = :org_id
            """
        )
        return self._scalar(
            statement,
            {
                "org_id": org_id,
                "expense_section": self._PAYROLL_SECTION_ID,
                "pattern_payroll": self._PAYROLL_PATTERNS[0],
                "pattern_salary": self._PAYROLL_PATTERNS[1],
                "pattern_wage": self._PAYROLL_PATTERNS[2],
            },
        )

    def list_members(self, org_id: int) -> list[CompanyMemberRow]:
        result = self._session.execute(
            text(
                """
                SELECT
                    u.id AS user_id,
                    u.name AS user_name,
                    u.email AS user_email,
                    m.role AS membership_role
                FROM membership m
                JOIN user u ON u.id = m.user_id
                WHERE m.org_id = :org_id
                ORDER BY u.name
                """
            ),
            {"org_id": org_id},
        )
        members: list[CompanyMemberRow] = []
        for row in result.mappings():
            members.append(
                CompanyMemberRow(
                    user_id=int(row["user_id"]),
                    user_name=str(row["user_name"]),
                    user_email=row.get("user_email") or None,
                    membership_role=str(row["membership_role"]),
                )
            )
        return members

    def get_cashflow(self, org_id: int, *, start_date: date, end_date: date) -> CashflowRow:
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
                        WHEN t.section_id = :expense_section THEN
                            CASE WHEN t.direction = 'DEBIT' THEN t.amount ELSE -t.amount END
                        ELSE 0
                    END) AS expense_total
                FROM account a
                LEFT JOIN `transaction` t
                    ON t.account_id = a.id
                   AND t.txn_date BETWEEN :start_date AND :end_date
                WHERE a.owner_type = 'org' AND a.owner_id = :org_id
                """
            ),
            {
                "org_id": org_id,
                "expense_section": self._PAYROLL_SECTION_ID,
                "start_date": start_date.isoformat(),
                "end_date": end_date.isoformat(),
            },
        ).mappings().one()
        return CashflowRow(
            income_total=self._to_decimal(row.get("income_total")),
            expense_total=self._to_decimal(row.get("expense_total")),
        )

    def list_income_transactions(
        self, org_id: int, *, start_date: date, end_date: date
    ) -> list[IncomeTransactionRow]:
        result = self._session.execute(
            text(
                """
                SELECT
                    t.txn_date AS txn_date,
                    CASE
                        WHEN t.direction = 'CREDIT' THEN t.amount
                        ELSE -t.amount
                    END AS normalized_amount
                FROM account a
                JOIN `transaction` t ON t.account_id = a.id
                WHERE a.owner_type = 'org'
                  AND a.owner_id = :org_id
                  AND t.txn_date BETWEEN :start_date AND :end_date
                  AND t.section_id = 1
                """
            ),
            {
                "org_id": org_id,
                "start_date": start_date.isoformat(),
                "end_date": end_date.isoformat(),
            },
        )
        transactions: list[IncomeTransactionRow] = []
        for row in result.mappings():
            txn_value = row.get("txn_date")
            if txn_value is None:
                continue
            txn_date = self._coerce_date(txn_value)
            transactions.append(
                IncomeTransactionRow(
                    txn_date=txn_date,
                    normalized_amount=self._to_decimal(row.get("normalized_amount")),
                )
            )
        return transactions

    def list_payroll_employees(
        self,
        org_id: int,
        *,
        start_date: date,
        end_date: date,
    ) -> list[PayrollEmployeeRow]:
        result = self._session.execute(
            text(
                """
                SELECT
                    t.counterparty_id AS counterparty_id,
                    COALESCE(cp.name, 'Unspecified') AS counterparty_name,
                    SUM(CASE WHEN t.direction = 'DEBIT' THEN t.amount ELSE -t.amount END) AS total_paid
                FROM account a
                JOIN `transaction` t ON t.account_id = a.id
                LEFT JOIN category c ON c.id = t.category_id
                LEFT JOIN counterparty cp ON cp.id = t.counterparty_id
                WHERE a.owner_type = 'org'
                  AND a.owner_id = :org_id
                  AND t.txn_date BETWEEN :start_date AND :end_date
                  AND t.section_id = :expense_section
                  AND (
                        LOWER(COALESCE(c.name, '')) LIKE :pattern_payroll OR
                        LOWER(COALESCE(c.name, '')) LIKE :pattern_salary OR
                        LOWER(COALESCE(c.name, '')) LIKE :pattern_wage
                  )
                GROUP BY t.counterparty_id, cp.name
                HAVING ABS(total_paid) > 0
                ORDER BY total_paid DESC
                """
            ),
            {
                "org_id": org_id,
                "start_date": start_date.isoformat(),
                "end_date": end_date.isoformat(),
                "expense_section": self._PAYROLL_SECTION_ID,
                "pattern_payroll": self._PAYROLL_PATTERNS[0],
                "pattern_salary": self._PAYROLL_PATTERNS[1],
                "pattern_wage": self._PAYROLL_PATTERNS[2],
            },
        )
        employees: list[PayrollEmployeeRow] = []
        for row in result.mappings():
            employees.append(
                PayrollEmployeeRow(
                    counterparty_id=(
                        int(row["counterparty_id"]) if row.get("counterparty_id") is not None else None
                    ),
                    counterparty_name=str(row["counterparty_name"]),
                    total_paid=self._to_decimal(row.get("total_paid")),
                )
            )
        return employees

    def list_top_expense_categories(
        self,
        org_id: int,
        *,
        start_date: date,
        end_date: date,
    ) -> list[ExpenseCategoryRow]:
        result = self._session.execute(
            text(
                """
                SELECT
                    c.id AS category_id,
                    COALESCE(c.name, 'Uncategorized') AS category_name,
                    SUM(CASE WHEN t.direction = 'DEBIT' THEN t.amount ELSE -t.amount END) AS total_spent
                FROM account a
                JOIN `transaction` t ON t.account_id = a.id
                LEFT JOIN category c ON c.id = t.category_id
                WHERE a.owner_type = 'org'
                  AND a.owner_id = :org_id
                  AND t.txn_date BETWEEN :start_date AND :end_date
                  AND t.section_id = :expense_section
                GROUP BY c.id, c.name
                HAVING ABS(total_spent) > 0
                ORDER BY total_spent DESC
                LIMIT 5
                """
            ),
            {
                "org_id": org_id,
                "start_date": start_date.isoformat(),
                "end_date": end_date.isoformat(),
                "expense_section": self._PAYROLL_SECTION_ID,
            },
        )
        categories: list[ExpenseCategoryRow] = []
        for row in result.mappings():
            categories.append(
                ExpenseCategoryRow(
                    category_id=(
                        int(row["category_id"]) if row.get("category_id") is not None else None
                    ),
                    category_name=str(row["category_name"]),
                    total_spent=self._to_decimal(row.get("total_spent")),
                )
            )
        return categories

