"""Unit tests for the company service."""
from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
import sys

from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session, sessionmaker

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.services.company import CompanyService


def _create_session() -> Session:
    engine = create_engine("sqlite:///:memory:", future=True)
    connection = engine.connect()

    ddl_statements = [
        """
        CREATE TABLE org (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            created_at TEXT
        )
        """,
        """
        CREATE TABLE account (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            owner_type TEXT NOT NULL,
            owner_id INTEGER NOT NULL,
            type TEXT NOT NULL,
            currency TEXT NOT NULL,
            name TEXT,
            opened_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
        """,
        """
        CREATE TABLE category (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            section_id INTEGER NOT NULL,
            name TEXT NOT NULL
        )
        """,
        """
        CREATE TABLE counterparty (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL
        )
        """,
        """
        CREATE TABLE "transaction" (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            account_id INTEGER NOT NULL,
            posted_at TEXT NOT NULL,
            txn_date TEXT NOT NULL,
            amount NUMERIC NOT NULL,
            currency TEXT NOT NULL,
            direction TEXT NOT NULL,
            section_id INTEGER NOT NULL,
            category_id INTEGER,
            counterparty_id INTEGER
        )
        """,
        """
        CREATE VIEW v_account_balance AS
        SELECT account_id,
               SUM(CASE WHEN direction = 'CREDIT' THEN amount ELSE -amount END) AS balance
        FROM "transaction"
        GROUP BY account_id
        """,
    ]

    for statement in ddl_statements:
        connection.execute(text(statement))

    connection.commit()
    SessionLocal = sessionmaker(bind=connection, future=True)
    return SessionLocal()


def test_get_company_detail_with_payroll_and_cashflow() -> None:
    session = _create_session()

    session.execute(
        text("INSERT INTO org (id, name, created_at) VALUES (:id, :name, :created)"),
        {"id": 1, "name": "Acme Corp", "created": "2024-01-01 00:00:00"},
    )

    accounts = [
        {"id": 1, "name": "Operating", "type": "operating"},
        {"id": 2, "name": "Reserve", "type": "savings"},
    ]
    for account in accounts:
        session.execute(
            text(
                """
                INSERT INTO account (id, owner_type, owner_id, type, currency, name, opened_at)
                VALUES (:id, 'org', :owner_id, :type, 'EUR', :name, :opened_at)
                """
            ),
            {
                "id": account["id"],
                "owner_id": 1,
                "type": account["type"],
                "name": account["name"],
                "opened_at": "2024-01-01 00:00:00",
            },
        )

    categories = [
        {"id": 1, "section_id": 1, "name": "Sales"},
        {"id": 2, "section_id": 2, "name": "Rent"},
        {"id": 3, "section_id": 2, "name": "Payroll"},
    ]
    for category in categories:
        session.execute(
            text(
                "INSERT INTO category (id, section_id, name) VALUES (:id, :section_id, :name)"
            ),
            category,
        )

    counterparties = [
        {"id": 1, "name": "Globex"},
        {"id": 2, "name": "Jane Doe"},
        {"id": 3, "name": "John Doe"},
    ]
    for counterparty in counterparties:
        session.execute(
            text("INSERT INTO counterparty (id, name) VALUES (:id, :name)"),
            counterparty,
        )

    transactions = [
        # Income
        {
            "account_id": 1,
            "posted_at": "2024-04-05 12:00:00",
            "txn_date": "2024-04-05",
            "amount": "1000.00",
            "direction": "CREDIT",
            "section_id": 1,
            "category_id": 1,
            "counterparty_id": 1,
        },
        {
            "account_id": 1,
            "posted_at": "2024-05-05 12:00:00",
            "txn_date": "2024-05-02",
            "amount": "1500.00",
            "direction": "CREDIT",
            "section_id": 1,
            "category_id": 1,
            "counterparty_id": 1,
        },
        # Rent expense
        {
            "account_id": 1,
            "posted_at": "2024-04-01 09:00:00",
            "txn_date": "2024-04-01",
            "amount": "400.00",
            "direction": "DEBIT",
            "section_id": 2,
            "category_id": 2,
            "counterparty_id": 1,
        },
        # Payroll - Jane
        {
            "account_id": 1,
            "posted_at": "2024-04-28 09:00:00",
            "txn_date": "2024-04-28",
            "amount": "300.00",
            "direction": "DEBIT",
            "section_id": 2,
            "category_id": 3,
            "counterparty_id": 2,
        },
        {
            "account_id": 1,
            "posted_at": "2024-05-28 09:00:00",
            "txn_date": "2024-05-28",
            "amount": "350.00",
            "direction": "DEBIT",
            "section_id": 2,
            "category_id": 3,
            "counterparty_id": 2,
        },
        # Payroll - John
        {
            "account_id": 1,
            "posted_at": "2024-04-29 09:00:00",
            "txn_date": "2024-04-29",
            "amount": "320.00",
            "direction": "DEBIT",
            "section_id": 2,
            "category_id": 3,
            "counterparty_id": 3,
        },
    ]

    for txn in transactions:
        session.execute(
            text(
                """
                INSERT INTO "transaction"
                    (account_id, posted_at, txn_date, amount, currency, direction, section_id, category_id, counterparty_id)
                VALUES
                    (:account_id, :posted_at, :txn_date, :amount, 'EUR', :direction, :section_id, :category_id, :counterparty_id)
                """
            ),
            txn,
        )

    session.commit()

    service = CompanyService(session)
    detail = service.get_company(1)

    assert detail is not None
    assert detail.organization.name == "Acme Corp"

    balances = {account.id: account.balance for account in detail.accounts}
    assert balances[1] == Decimal("1130")
    assert balances[2] == Decimal("0")

    months = {bucket.month.strftime("%Y-%m"): bucket for bucket in detail.cashflow}
    assert months["2024-04"].income == Decimal("1000")
    assert months["2024-04"].expenses == Decimal("1020")
    assert months["2024-05"].income == Decimal("1500")
    assert months["2024-05"].expenses == Decimal("350")

    payroll = {employee.name: employee for employee in detail.payroll.employees}
    assert payroll["Jane Doe"].total_paid == Decimal("650")
    assert payroll["Jane Doe"].latest_salary == Decimal("350")
    assert payroll["John Doe"].total_paid == Decimal("320")
    assert payroll["John Doe"].latest_salary == Decimal("320")

    assert detail.overview.total_balance == Decimal("1130")
    assert detail.overview.income_total == Decimal("2500")
    assert detail.overview.expense_total == Decimal("1370")
    assert detail.overview.payroll_total == Decimal("970")
    assert detail.overview.employee_count == 2


def test_get_company_detail_handles_missing_payroll() -> None:
    session = _create_session()

    session.execute(
        text("INSERT INTO org (id, name, created_at) VALUES (:id, :name, :created)"),
        {
            "id": 2,
            "name": "Beta LLC",
            "created": datetime.now(timezone.utc).isoformat(sep=" "),
        },
    )
    session.execute(
        text(
            """
            INSERT INTO account (id, owner_type, owner_id, type, currency, name, opened_at)
            VALUES (10, 'org', 2, 'operating', 'EUR', 'Main', '2024-01-01 00:00:00')
            """
        )
    )

    session.commit()

    service = CompanyService(session)
    detail = service.get_company(2)

    assert detail is not None
    assert detail.payroll.employees == []
    assert detail.payroll.total_paid == Decimal("0")
    assert detail.cashflow == []
    assert detail.overview.total_balance == Decimal("0")
    assert detail.overview.income_total == Decimal("0")
    assert detail.overview.expense_total == Decimal("0")
    assert detail.overview.payroll_total == Decimal("0")
    assert detail.overview.employee_count == 0


def test_get_company_returns_none_for_missing_org() -> None:
    session = _create_session()
    service = CompanyService(session)

    assert service.get_company(999) is None
