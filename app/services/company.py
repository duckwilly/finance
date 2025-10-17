"""Services for organization-centric administrative views."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from typing import Any

from sqlalchemy import text
from sqlalchemy.engine import Result
from sqlalchemy.orm import Session


PAYROLL_CATEGORY_NAMES = {"payroll", "salary", "salaries", "wages"}


@dataclass(frozen=True)
class OrganizationSummary:
    """Basic identifying metadata for an organization."""

    id: int
    name: str
    created_at: datetime | None


@dataclass(frozen=True)
class AccountBalance:
    """Snapshot of an organization's account balance."""

    id: int
    name: str | None
    type: str
    currency: str
    balance: Decimal


@dataclass(frozen=True)
class MonthlyCashflow:
    """Income and expense aggregates for a month."""

    month: date
    income: Decimal
    expenses: Decimal


@dataclass(frozen=True)
class EmployeePayroll:
    """Aggregated payroll information for a single counterparty."""

    counterparty_id: int | None
    name: str
    latest_salary: Decimal
    total_paid: Decimal


@dataclass(frozen=True)
class PayrollSummary:
    """Collection of payroll statistics for an organization."""

    total_paid: Decimal
    employees: list[EmployeePayroll]

    @property
    def employee_count(self) -> int:
        return len(self.employees)


@dataclass(frozen=True)
class CompanyOverview:
    """High-level aggregates for front-end summary cards."""

    total_balance: Decimal
    income_total: Decimal
    expense_total: Decimal
    payroll_total: Decimal
    employee_count: int


@dataclass(frozen=True)
class CompanyDetail:
    """Full payload consumed by the organization detail view."""

    organization: OrganizationSummary
    accounts: list[AccountBalance]
    cashflow: list[MonthlyCashflow]
    payroll: PayrollSummary
    overview: CompanyOverview


class CompanyService:
    """Facade for retrieving organization information for admin screens."""

    def __init__(self, session: Session) -> None:
        self._session = session

    def get_company(self, org_id: int, *, months: int = 6) -> CompanyDetail | None:
        """Return a detailed snapshot for the requested organization."""

        organization = self._fetch_organization(org_id)
        if organization is None:
            return None

        accounts = self._fetch_accounts(org_id)
        cashflow = self._fetch_cashflow(org_id, months=months)
        payroll = self._fetch_payroll(org_id)

        total_balance = sum((account.balance for account in accounts), Decimal(0))
        income_total = sum((entry.income for entry in cashflow), Decimal(0))
        expense_total = sum((entry.expenses for entry in cashflow), Decimal(0))

        overview = CompanyOverview(
            total_balance=total_balance,
            income_total=income_total,
            expense_total=expense_total,
            payroll_total=payroll.total_paid,
            employee_count=payroll.employee_count,
        )

        return CompanyDetail(
            organization=organization,
            accounts=accounts,
            cashflow=cashflow,
            payroll=payroll,
            overview=overview,
        )

    def _fetch_organization(self, org_id: int) -> OrganizationSummary | None:
        result: Result = self._session.execute(
            text(
                """
                SELECT id, name, created_at
                FROM org
                WHERE id = :org_id
                """
            ),
            {"org_id": org_id},
        )

        row = result.mappings().first()
        if row is None:
            return None

        return OrganizationSummary(
            id=int(row["id"]),
            name=str(row["name"]),
            created_at=row.get("created_at"),
        )

    def _fetch_accounts(self, org_id: int) -> list[AccountBalance]:
        result: Result = self._session.execute(
            text(
                """
                SELECT a.id,
                       a.name,
                       a.type,
                       a.currency,
                       COALESCE(b.balance, 0) AS balance
                FROM account AS a
                LEFT JOIN v_account_balance AS b
                  ON b.account_id = a.id
                WHERE a.owner_type = 'org'
                  AND a.owner_id = :org_id
                ORDER BY a.opened_at ASC, a.id ASC
                """
            ),
            {"org_id": org_id},
        )

        accounts: list[AccountBalance] = []
        for row in result.mappings():
            accounts.append(
                AccountBalance(
                    id=int(row["id"]),
                    name=row.get("name"),
                    type=str(row["type"]),
                    currency=str(row["currency"]),
                    balance=_as_decimal(row.get("balance")),
                )
            )

        return accounts

    def _fetch_cashflow(self, org_id: int, *, months: int) -> list[MonthlyCashflow]:
        month_expression = self._dialect_month_expression()

        query = text(
            f"""
            SELECT {month_expression} AS month_start,
                   SUM(
                       CASE
                           WHEN COALESCE(c.section_id, t.section_id) = 1 THEN
                               CASE WHEN t.direction = 'CREDIT' THEN t.amount ELSE -t.amount END
                           ELSE 0
                       END
                   ) AS income_total,
                   SUM(
                       CASE
                           WHEN COALESCE(c.section_id, t.section_id) = 2 THEN
                               CASE WHEN t.direction = 'DEBIT' THEN t.amount ELSE -t.amount END
                           ELSE 0
                       END
                   ) AS expense_total
            FROM `transaction` AS t
            JOIN account AS a ON a.id = t.account_id
            LEFT JOIN category AS c ON c.id = t.category_id
            WHERE a.owner_type = 'org'
              AND a.owner_id = :org_id
            GROUP BY month_start
            ORDER BY month_start DESC
            LIMIT :limit
            """
        )

        result = self._session.execute(query, {"org_id": org_id, "limit": months})

        items: list[MonthlyCashflow] = []
        for row in result.mappings():
            month_value = row["month_start"]
            month_date = _parse_month(month_value)
            income = _as_decimal(row.get("income_total"))
            expenses = _as_decimal(row.get("expense_total"))

            # Expenses should surface as positive costs for presentation.
            if expenses < 0:
                expenses = -expenses

            items.append(
                MonthlyCashflow(
                    month=month_date,
                    income=income,
                    expenses=expenses,
                )
            )

        items.sort(key=lambda entry: entry.month)
        return items

    def _fetch_payroll(self, org_id: int) -> PayrollSummary:
        payroll_categories = tuple(PAYROLL_CATEGORY_NAMES)
        placeholders = ", ".join(":c" + str(idx) for idx in range(len(payroll_categories)))

        query = text(
            f"""
            WITH payroll_txn AS (
                SELECT t.id,
                       t.account_id,
                       t.txn_date,
                       t.amount,
                       t.direction,
                       t.counterparty_id,
                       cp.name AS counterparty_name
                FROM `transaction` AS t
                JOIN account AS a ON a.id = t.account_id
                LEFT JOIN category AS c ON c.id = t.category_id
                LEFT JOIN counterparty AS cp ON cp.id = t.counterparty_id
                WHERE a.owner_type = 'org'
                  AND a.owner_id = :org_id
                  AND t.counterparty_id IS NOT NULL
                  AND LOWER(COALESCE(c.name, '')) IN ({placeholders})
            )
            SELECT counterparty_id,
                   counterparty_name,
                   SUM(CASE WHEN direction = 'DEBIT' THEN amount ELSE -amount END) AS total_paid,
                   (
                       SELECT CASE WHEN pt.direction = 'DEBIT' THEN pt.amount ELSE -pt.amount END
                       FROM payroll_txn AS pt
                       WHERE pt.counterparty_id = base.counterparty_id
                       ORDER BY pt.txn_date DESC, pt.id DESC
                       LIMIT 1
                   ) AS latest_salary
            FROM payroll_txn AS base
            GROUP BY counterparty_id, counterparty_name
            ORDER BY counterparty_name ASC
            """
        )

        params: dict[str, Any] = {"org_id": org_id}
        for idx, name in enumerate(payroll_categories):
            params[f"c{idx}"] = name

        result = self._session.execute(query, params)

        employees: list[EmployeePayroll] = []
        for row in result.mappings():
            total_paid = _as_decimal(row.get("total_paid"))
            if total_paid < 0:
                total_paid = -total_paid

            latest_salary = _as_decimal(row.get("latest_salary"))
            if latest_salary < 0:
                latest_salary = -latest_salary

            employees.append(
                EmployeePayroll(
                    counterparty_id=row.get("counterparty_id"),
                    name=row.get("counterparty_name") or "Unknown",
                    latest_salary=latest_salary,
                    total_paid=total_paid,
                )
            )

        total_payroll = sum((employee.total_paid for employee in employees), Decimal(0))

        return PayrollSummary(total_paid=total_payroll, employees=employees)

    def _dialect_month_expression(self) -> str:
        dialect = getattr(self._session.bind, "dialect", None)
        name = getattr(dialect, "name", "") if dialect else ""

        if name == "sqlite":
            return "strftime('%Y-%m-01', t.txn_date)"
        if name in {"mysql", "mariadb"}:
            return "DATE_FORMAT(t.txn_date, '%Y-%m-01')"

        # Fallback to ISO formatted string via standard SQL concatenation.
        return "TO_CHAR(t.txn_date, 'YYYY-MM-01')"


def _as_decimal(value: Any) -> Decimal:
    """Coerce arbitrary database values into ``Decimal`` objects."""

    if value is None:
        return Decimal(0)
    if isinstance(value, Decimal):
        return value
    if isinstance(value, (int, float)):
        return Decimal(str(value))

    return Decimal(str(value))


def _parse_month(value: Any) -> date:
    """Parse database-specific month identifiers into ``date`` objects."""

    if isinstance(value, date):
        return value
    if isinstance(value, datetime):
        return value.date().replace(day=1)
    if not value:
        return date.today().replace(day=1)

    text_value = str(value)
    for fmt in ("%Y-%m-%d", "%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"):
        try:
            parsed = datetime.strptime(text_value, fmt)
            return parsed.date().replace(day=1)
        except ValueError:
            continue

    # As a fallback, slice the string assuming ISO layout YYYY-MM-DD.
    return date(int(text_value[0:4]), int(text_value[5:7]), 1)
