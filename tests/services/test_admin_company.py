"""Unit tests for the ``AdminCompanyService``."""
from __future__ import annotations

from decimal import Decimal

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Connection
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from app.services.admin_company import AdminCompanyService


@pytest.fixture()
def session() -> Session:
    engine = create_engine(
        "sqlite+pysqlite:///:memory:",
        future=True,
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )

    with engine.begin() as conn:
        _create_schema(conn)

    SessionLocal = sessionmaker(bind=engine, autoflush=False, expire_on_commit=False)

    with SessionLocal() as session:
        yield session

    engine.dispose()


def test_pagination_bounds(session: Session) -> None:
    """Pages beyond the dataset collapse to the last available page."""

    for idx in range(1, 24):
        session.execute(text("INSERT INTO org (name) VALUES (:name)"), {"name": f"Org {idx:02d}"})
        session.execute(
            text(
                """
                INSERT INTO account (owner_type, owner_id, type)
                VALUES ('org', :org_id, 'operating')
                """
            ),
            {"org_id": idx},
        )
    session.commit()

    service = AdminCompanyService(session)

    page = service.list_companies(page=5, page_size=10)

    assert page.total == 23
    assert page.page == 3
    assert page.page_size == 10
    assert len(page.items) == 3
    assert all(company.total_balance == Decimal("0") for company in page.items)


def test_metrics_with_payroll_mix(session: Session) -> None:
    """Payroll headcount and balances aggregate correctly across accounts."""

    session.execute(text("INSERT INTO org (id, name) VALUES (1, 'Acme Corp'), (2, 'Globex LLC')"))
    session.execute(
        text(
            """
            INSERT INTO account (id, owner_type, owner_id, type) VALUES
            (1, 'org', 1, 'operating'),
            (2, 'org', 1, 'operating'),
            (3, 'org', 2, 'operating')
            """
        )
    )
    session.execute(
        text(
            """
            INSERT INTO category (id, section_id, name) VALUES
            (1, 2, 'Payroll'),
            (2, 2, 'Rent'),
            (3, 2, 'Salary Bonus'),
            (4, 1, 'Revenue')
            """
        )
    )
    session.execute(
        text(
            """
            INSERT INTO counterparty (id, name) VALUES
            (1, 'Alice'),
            (2, 'Bob'),
            (3, 'Charlie'),
            (4, 'Dana')
            """
        )
    )

    payroll_txns = [
        (1, 1000, "CREDIT", 1, 4, None),
        (1, 200, "DEBIT", 2, 1, 1),
        (1, 150, "DEBIT", 2, 1, 2),
        (2, 120, "DEBIT", 2, 3, 2),
        (1, 90, "DEBIT", 2, 2, 3),
        (3, 800, "CREDIT", 1, 4, None),
        (3, 220, "DEBIT", 2, 1, 3),
        (3, 80, "DEBIT", 2, 3, 4),
    ]

    for account_id, amount, direction, section_id, category_id, counterparty_id in payroll_txns:
        session.execute(
            text(
                """
                INSERT INTO `transaction`
                    (account_id, amount, direction, section_id, category_id, counterparty_id)
                VALUES
                    (:account_id, :amount, :direction, :section_id, :category_id, :counterparty_id)
                """
            ),
            {
                "account_id": account_id,
                "amount": amount,
                "direction": direction,
                "section_id": section_id,
                "category_id": category_id,
                "counterparty_id": counterparty_id,
            },
        )

    session.commit()

    service = AdminCompanyService(session)
    page = service.list_companies(page=1, page_size=10)

    by_name = {company.name: company for company in page.items}

    acme = by_name["Acme Corp"]
    globex = by_name["Globex LLC"]

    assert acme.total_balance == Decimal("440")
    assert acme.payroll_headcount == 2

    assert globex.total_balance == Decimal("500")
    assert globex.payroll_headcount == 2

    filtered = service.list_companies(page=1, page_size=10, search="globex")
    assert filtered.total == 1
    assert filtered.items[0].name == "Globex LLC"



def test_get_company_detail(session: Session) -> None:
    """Detailed view returns account and membership metadata."""

    session.execute(text("INSERT INTO org (id, name) VALUES (1, 'Acme Corp')"))
    session.execute(
        text(
            """
            INSERT INTO account (id, owner_type, owner_id, name, type, currency) VALUES
            (1, 'org', 1, 'Operating Account', 'operating', 'EUR'),
            (2, 'org', 1, NULL, 'savings', 'EUR')
            """
        )
    )
    session.execute(
        text(
            """
            INSERT INTO category (id, section_id, name) VALUES
            (1, 2, 'Payroll'),
            (2, 2, 'Salary Bonus'),
            (3, 1, 'Revenue')
            """
        )
    )
    session.execute(
        text(
            """
            INSERT INTO counterparty (id, name) VALUES
            (1, 'Alice'),
            (2, 'Bob')
            """
        )
    )

    transactions = [
        (1, 1000, 'CREDIT', 1, 3, None),
        (1, 200, 'DEBIT', 2, 1, 1),
        (1, 150, 'DEBIT', 2, 1, 2),
        (2, 90, 'DEBIT', 2, 2, 2),
    ]

    for account_id, amount, direction, section_id, category_id, counterparty_id in transactions:
        session.execute(
            text(
                """
                INSERT INTO `transaction`
                    (account_id, amount, direction, section_id, category_id, counterparty_id)
                VALUES
                    (:account_id, :amount, :direction, :section_id, :category_id, :counterparty_id)
                """
            ),
            {
                'account_id': account_id,
                'amount': amount,
                'direction': direction,
                'section_id': section_id,
                'category_id': category_id,
                'counterparty_id': counterparty_id,
            },
        )

    session.execute(
        text(
            """
            INSERT INTO user (id, name, email) VALUES
            (1, 'Dana Carvey', 'dana@example.com'),
            (2, 'Lee Jordan', NULL)
            """
        )
    )
    session.execute(
        text(
            """
            INSERT INTO membership (user_id, org_id, role) VALUES
            (1, 1, 'OWNER'),
            (2, 1, 'ACCOUNTANT')
            """
        )
    )

    session.commit()

    service = AdminCompanyService(session)
    detail = service.get_company_detail(1)

    assert detail is not None
    assert detail.name == 'Acme Corp'
    assert detail.total_balance == Decimal('560')
    assert detail.payroll_headcount == 2
    assert {account.account_id for account in detail.accounts} == {1, 2}

    operating = next(account for account in detail.accounts if account.account_id == 1)
    assert operating.name == 'Operating Account'
    assert operating.balance == Decimal('650')

    savings = next(account for account in detail.accounts if account.account_id == 2)
    assert savings.name is None
    assert savings.balance == Decimal('-90')

    assert {member.name for member in detail.members} == {'Dana Carvey', 'Lee Jordan'}
    owner = next(member for member in detail.members if member.name == 'Dana Carvey')
    assert owner.role == 'OWNER'
    assert owner.email == 'dana@example.com'

    assert service.get_company_detail(999) is None


def _create_schema(conn: Connection) -> None:
    conn.exec_driver_sql(
        """
        CREATE TABLE org (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL
        );
        """
    )

    conn.exec_driver_sql(
        """
        CREATE TABLE account (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            owner_type TEXT NOT NULL,
            owner_id INTEGER NOT NULL,
            name TEXT,
            type TEXT NOT NULL,
            currency TEXT NOT NULL DEFAULT 'EUR'
        );
        """
    )

    conn.exec_driver_sql(
        """
        CREATE TABLE category (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            section_id INTEGER NOT NULL,
            name TEXT NOT NULL
        );
        """
    )

    conn.exec_driver_sql(
        """
        CREATE TABLE counterparty (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL
        );
        """
    )

    conn.exec_driver_sql(
        """
        CREATE TABLE user (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            email TEXT
        );
        """
    )

    conn.exec_driver_sql(
        """
        CREATE TABLE membership (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            org_id INTEGER NOT NULL,
            role TEXT NOT NULL,
            FOREIGN KEY (user_id) REFERENCES user(id),
            FOREIGN KEY (org_id) REFERENCES org(id)
        );
        """
    )

    conn.exec_driver_sql(
        """
        CREATE TABLE `transaction` (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            account_id INTEGER NOT NULL,
            amount NUMERIC NOT NULL,
            direction TEXT NOT NULL,
            section_id INTEGER NOT NULL,
            category_id INTEGER,
            counterparty_id INTEGER
        );
        """
    )

    conn.exec_driver_sql(
        """
        CREATE VIEW v_account_balance AS
        SELECT
            account_id,
            SUM(CASE WHEN direction = 'CREDIT' THEN amount ELSE -amount END) AS balance
        FROM `transaction`
        GROUP BY account_id;
        """
    )
