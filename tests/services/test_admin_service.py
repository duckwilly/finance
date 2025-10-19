"""Tests for the admin service metrics aggregation."""
from __future__ import annotations

from datetime import date, datetime, timezone
from decimal import Decimal

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from app.models import (
    Account,
    AccountOwnerType,
    AccountType,
    Base,
    Company,
    Individual,
    Section,
    Transaction,
    TransactionChannel,
    TransactionDirection,
)
from app.schemas.admin import AdminMetrics
from app.services.admin_service import AdminService


@pytest.fixture()
def session() -> Session:
    """Provide an in-memory database session for each test."""

    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(engine)
    SessionLocal = sessionmaker(bind=engine, future=True)
    with SessionLocal() as session:
        yield session


@pytest.fixture()
def admin_service() -> AdminService:
    return AdminService()


def test_get_metrics_with_data(session: Session, admin_service: AdminService) -> None:
    """The service should aggregate counts and balances across records."""

    section_income = Section(name="income")
    section_expense = Section(name="expense")
    session.add_all([section_income, section_expense])

    alice = Individual(name="Alice", email="alice@example.com")
    bob = Individual(name="Bob", email="bob@example.com")
    acme = Company(name="ACME Corp")
    session.add_all([alice, bob, acme])
    session.flush()

    user_account = Account(
        owner_type=AccountOwnerType.USER,
        owner_id=alice.id,
        type=AccountType.CHECKING,
        name="Alice Checking",
    )
    company_account = Account(
        owner_type=AccountOwnerType.ORG,
        owner_id=acme.id,
        type=AccountType.OPERATING,
        name="ACME Operating",
    )
    session.add_all([user_account, company_account])
    session.flush()

    transactions = [
        Transaction(
            account=user_account,
            posted_at=datetime(2024, 1, 1, 12, 0, tzinfo=timezone.utc),
            txn_date=date(2024, 1, 1),
            amount=Decimal("100.00"),
            direction=TransactionDirection.CREDIT,
            section=section_income,
            channel=TransactionChannel.SEPA,
        ),
        Transaction(
            account=user_account,
            posted_at=datetime(2024, 1, 5, 9, 30, tzinfo=timezone.utc),
            txn_date=date(2024, 1, 5),
            amount=Decimal("40.00"),
            direction=TransactionDirection.DEBIT,
            section=section_expense,
            channel=TransactionChannel.CARD,
        ),
        Transaction(
            account=company_account,
            posted_at=datetime(2024, 1, 10, 15, 45, tzinfo=timezone.utc),
            txn_date=date(2024, 1, 10),
            amount=Decimal("250.00"),
            direction=TransactionDirection.CREDIT,
            section=section_income,
            channel=TransactionChannel.WIRE,
        ),
    ]
    session.add_all(transactions)
    session.commit()

    metrics = admin_service.get_metrics(session)
    assert isinstance(metrics, AdminMetrics)
    assert metrics.total_individuals == 2
    assert metrics.total_companies == 1
    assert metrics.total_transactions == 3
    assert metrics.first_transaction_at == datetime(2024, 1, 1, 12, 0)
    assert metrics.last_transaction_at == datetime(2024, 1, 10, 15, 45)
    assert metrics.total_aum == Decimal("310.0000")


def test_get_metrics_with_no_transactions(session: Session, admin_service: AdminService) -> None:
    """Empty datasets should yield zero counts and null timestamps."""

    metrics = admin_service.get_metrics(session)

    assert metrics.total_individuals == 0
    assert metrics.total_companies == 0
    assert metrics.total_transactions == 0
    assert metrics.first_transaction_at is None
    assert metrics.last_transaction_at is None
    assert metrics.total_aum == Decimal("0")
