"""Tests for the admin service metrics aggregation."""
from __future__ import annotations

from datetime import date, datetime, timezone
from decimal import Decimal

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session, sessionmaker

from app.models import (
    Account,
    AccountPartyRole,
    AccountType,
    Base,
    Category,
    JournalEntry,
    JournalLine,
    Section,
    UserPartyMap,
)
from app.models.memberships import EmploymentContract
from app.models.party import CompanyProfile as CompanyProfileModel
from app.models.party import IndividualProfile as IndividualProfileModel
from app.models.party import Party, PartyType
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


def _seed_reference_data(session: Session) -> None:
    session.execute(text("INSERT INTO currency (code, name, exponent) VALUES ('EUR', 'Euro', 2)"))
    session.execute(
        text(
            "INSERT INTO account_type (code, description, is_cash, is_brokerage) VALUES "
            "('checking', 'Checking', 1, 0),"
            "('operating', 'Operating', 1, 0),"
            "('brokerage', 'Brokerage', 0, 1)"
        )
    )
    session.execute(text("INSERT INTO account_role (code, description) VALUES ('OWNER', 'Owner')"))
    session.execute(text("INSERT INTO txn_channel (code, description) VALUES ('SEPA', 'SEPA payment')"))
    session.commit()


def _create_party(session: Session, *, party_type: PartyType, display_name: str) -> Party:
    party = Party(party_type=party_type, display_name=display_name)
    session.add(party)
    session.flush()
    return party


def _create_account(
    session: Session,
    *,
    party_id: int,
    account_type: AccountType,
    name: str,
) -> Account:
    account = Account(
        party_id=party_id,
        account_type_code=account_type.value,
        currency_code="EUR",
        name=name,
    )
    session.add(account)
    session.flush()
    session.add(
        AccountPartyRole(
            account_id=account.id,
            party_id=party_id,
            role_code="OWNER",
            is_primary=True,
        )
    )
    return account


def test_get_metrics_with_data(session: Session, admin_service: AdminService) -> None:
    """The service should aggregate counts and balances across records."""

    _seed_reference_data(session)

    section_income = Section(name="income")
    section_expense = Section(name="expense")
    section_transfer = Section(name="transfer")
    session.add_all([section_income, section_expense, section_transfer])
    session.flush()

    salary_category = Category(section_id=section_income.id, name="Salary")
    groceries_category = Category(section_id=section_expense.id, name="Groceries")
    client_category = Category(section_id=section_income.id, name="Client Payment")
    session.add_all([salary_category, groceries_category, client_category])
    session.flush()

    alice_party = _create_party(session, party_type=PartyType.INDIVIDUAL, display_name="Alice Example")
    bob_party = _create_party(session, party_type=PartyType.INDIVIDUAL, display_name="Bob Example")
    company_party = _create_party(session, party_type=PartyType.COMPANY, display_name="ACME Corp")

    session.add_all(
        [
            IndividualProfileModel(
                party_id=alice_party.id,
                given_name="Alice",
                family_name="Example",
                primary_email="alice@example.com",
            ),
            IndividualProfileModel(
                party_id=bob_party.id,
                given_name="Bob",
                family_name="Example",
                primary_email="bob@example.com",
            ),
            CompanyProfileModel(
                party_id=company_party.id,
                legal_name="ACME Corp",
            ),
        ]
    )

    session.add_all(
        [
            UserPartyMap(user_id=1, party_id=alice_party.id),
            UserPartyMap(user_id=2, party_id=bob_party.id),
        ]
    )

    session.add(
        EmploymentContract(
            employee_party_id=alice_party.id,
            employer_party_id=company_party.id,
            position_title="Engineer",
            start_date=date(2023, 1, 1),
            is_primary=True,
        )
    )

    alice_account = _create_account(
        session,
        party_id=alice_party.id,
        account_type=AccountType.CHECKING,
        name="Alice Checking",
    )
    company_account = _create_account(
        session,
        party_id=company_party.id,
        account_type=AccountType.OPERATING,
        name="ACME Operating",
    )
    brokerage_account = _create_account(
        session,
        party_id=company_party.id,
        account_type=AccountType.BROKERAGE,
        name="Brokerage Clearing",
    )

    entries: list[JournalEntry] = []

    entry_1 = JournalEntry(
        entry_code="JE-1",
        txn_date=date(2024, 1, 1),
        posted_at=datetime(2024, 1, 1, 12, 0, tzinfo=timezone.utc),
        description="Salary payment",
    )
    entry_1.lines = [
        JournalLine(
            account_id=alice_account.id,
            amount=Decimal("100.00"),
            currency_code="EUR",
            category=salary_category,
        ),
        JournalLine(
            account_id=brokerage_account.id,
            amount=Decimal("-100.00"),
            currency_code="EUR",
            category=salary_category,
        ),
    ]
    entries.append(entry_1)

    entry_2 = JournalEntry(
        entry_code="JE-2",
        txn_date=date(2024, 1, 5),
        posted_at=datetime(2024, 1, 5, 9, 30, tzinfo=timezone.utc),
        description="Groceries",
    )
    entry_2.lines = [
        JournalLine(
            account_id=alice_account.id,
            amount=Decimal("-40.00"),
            currency_code="EUR",
            category=groceries_category,
        ),
        JournalLine(
            account_id=brokerage_account.id,
            amount=Decimal("40.00"),
            currency_code="EUR",
            category=groceries_category,
        ),
    ]
    entries.append(entry_2)

    entry_3 = JournalEntry(
        entry_code="JE-3",
        txn_date=date(2024, 1, 10),
        posted_at=datetime(2024, 1, 10, 15, 45, tzinfo=timezone.utc),
        description="Client payment",
    )
    entry_3.lines = [
        JournalLine(
            account_id=company_account.id,
            amount=Decimal("250.00"),
            currency_code="EUR",
            category=client_category,
        ),
        JournalLine(
            account_id=brokerage_account.id,
            amount=Decimal("-250.00"),
            currency_code="EUR",
            category=client_category,
        ),
    ]
    entries.append(entry_3)

    session.add_all(entries)
    session.commit()

    metrics = admin_service.get_metrics(session)
    assert isinstance(metrics, AdminMetrics)
    assert metrics.total_individuals == 2
    assert metrics.total_companies == 1
    assert metrics.total_transactions == 3
    assert metrics.first_transaction_at == datetime(2024, 1, 1, 12, 0)
    assert metrics.last_transaction_at == datetime(2024, 1, 10, 15, 45)
    assert metrics.total_cash == Decimal("310.00")
    assert metrics.total_aum == Decimal("310.00")


def test_get_metrics_with_no_transactions(session: Session, admin_service: AdminService) -> None:
    """Empty datasets should yield zero counts and null timestamps."""

    _seed_reference_data(session)

    metrics = admin_service.get_metrics(session)

    assert metrics.total_individuals == 0
    assert metrics.total_companies == 0
    assert metrics.total_transactions == 0
    assert metrics.first_transaction_at is None
    assert metrics.last_transaction_at is None
    assert metrics.total_aum == Decimal("0")
