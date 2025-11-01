"""Reusable helpers for assembling dashboard datasets."""
from __future__ import annotations

from datetime import date
from decimal import Decimal
from typing import Any, Sequence

from sqlalchemy import func, select
from sqlalchemy.orm import Session
from sqlalchemy.sql import ColumnElement

from app.models import Account, AccountType, Category, JournalEntry, JournalLine, PositionAgg, Section

BreakdownResult = tuple[str, Decimal, list[tuple[date, str | None, Decimal]]]


def fetch_category_breakdown(
    session: Session,
    *,
    section_name: str,
    start_date: date,
    limit_transactions: int = 10,
    party_ids: Sequence[int] | None = None,
) -> list[BreakdownResult]:
    """Return totals and sample transactions for a given section.

    The function returns a list of tuples ``(category_name, total_amount, transactions)``
    where ``transactions`` contains tuples ``(txn_date, description, amount)``.
    """

    category_label = func.coalesce(Category.name, "Uncategorised").label("category_name")
    total_column = func.coalesce(func.sum(func.abs(JournalLine.amount)), 0).label("total")

    filters: list[ColumnElement[Any]] = [Section.name == section_name]

    if party_ids:
        filters.append(Account.party_id.in_(list(party_ids)))

    if start_date:
        filters.append(JournalEntry.txn_date >= start_date)

    totals_query = (
        select(category_label, total_column)
        .select_from(JournalLine)
        .join(Account, JournalLine.account_id == Account.id)
        .join(JournalEntry, JournalEntry.id == JournalLine.entry_id)
        .outerjoin(Category, Category.id == JournalLine.category_id)
        .outerjoin(Section, Section.id == Category.section_id)
        .where(*filters)
        .group_by(category_label)
        .order_by(total_column.desc())
    )
    totals = session.execute(totals_query).all()

    results: list[BreakdownResult] = []
    for total in totals:
        if total.category_name == "Uncategorised":
            category_filter = Category.id.is_(None)
        else:
            category_filter = Category.name == total.category_name

        transactions_query = (
            select(
                JournalEntry.txn_date,
                JournalEntry.description,
                JournalLine.amount,
            )
            .select_from(JournalLine)
            .join(JournalEntry, JournalEntry.id == JournalLine.entry_id)
            .join(Account, JournalLine.account_id == Account.id)
            .outerjoin(Category, Category.id == JournalLine.category_id)
            .outerjoin(Section, Section.id == Category.section_id)
            .where(*filters, category_filter)
            .order_by(JournalEntry.txn_date.desc(), JournalEntry.created_at.desc())
            .limit(limit_transactions)
        )
        txn_rows = session.execute(transactions_query).all()

        transactions = [
            (
                row.txn_date,
                row.description,
                Decimal(abs(row.amount or 0)),
            )
            for row in txn_rows
        ]

        results.append(
            (
                total.category_name,
                Decimal(total.total or 0),
                transactions,
            )
        )

    return results


def calculate_entity_net_worth(
    session: Session,
    *,
    party_id: int,
) -> Decimal:
    """Calculate total net worth for an entity (cash + brokerage holdings).
    
    Args:
        session: Database session
        owner_type: Type of owner (USER or ORG)
        owner_id: ID of the owner entity
        
    Returns:
        Total net worth as Decimal
    """
    owner_filters: list[ColumnElement[Any]] = [Account.party_id == party_id]

    cash_balance_query = (
        select(func.coalesce(func.sum(JournalLine.amount), 0))
        .select_from(Account)
        .join(JournalLine, JournalLine.account_id == Account.id, isouter=True)
        .where(Account.account_type_code != AccountType.BROKERAGE.value, *owner_filters)
    )
    cash_balance = session.execute(cash_balance_query).scalar_one() or Decimal("0")

    # Calculate brokerage holdings value from PositionAgg view
    holdings_query = (
        select(func.coalesce(func.sum(PositionAgg.qty * PositionAgg.last_price), 0))
        .select_from(PositionAgg)
        .join(Account, Account.id == PositionAgg.account_id)
        .where(*owner_filters)
    )
    holdings_value = session.execute(holdings_query).scalar_one() or Decimal("0")

    return cash_balance + holdings_value
