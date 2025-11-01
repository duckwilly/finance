"""Schemas for the journal/ledger layer and account-party relationships."""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal

from pydantic import BaseModel, field_serializer


class TransactionCategoryPayload(BaseModel):
    """Represents a transaction category joined to a journal line."""

    id: int
    section_id: int
    name: str


class AccountRoleAssignment(BaseModel):
    """Role granted to a party on an account."""

    account_id: int
    party_id: int
    role_code: str
    start_date: date | None = None
    end_date: date | None = None
    is_primary: bool = False


class JournalLinePayload(BaseModel):
    """Line within a journal entry enforcing double-entry bookkeeping."""

    id: int
    entry_id: int
    account_id: int
    party_id: int | None = None
    amount: Decimal
    currency_code: str
    category_id: int | None = None
    category: TransactionCategoryPayload | None = None
    line_memo: str | None = None
    created_at: datetime

    @field_serializer("amount")
    def serialize_amount(cls, value: Decimal) -> str:
        return format(value, "f")


class JournalEntryPayload(BaseModel):
    """Aggregated journal entry with associated lines."""

    id: int
    entry_code: str
    txn_date: date
    posted_at: datetime
    description: str | None = None
    channel_code: str | None = None
    counterparty_party_id: int | None = None
    transfer_reference: str | None = None
    external_reference: str | None = None
    created_at: datetime
    lines: list[JournalLinePayload]
