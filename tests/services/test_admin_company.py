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
                INSERT INTO account (owner_type, owner_id)
                VALUES ('org', :org_id)
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
            INSERT INTO account (id, owner_type, owner_id) VALUES
            (1, 'org', 1),
            (2, 'org', 1),
            (3, 'org', 2)
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
            owner_id INTEGER NOT NULL
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
