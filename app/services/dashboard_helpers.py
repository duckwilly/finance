"""Reusable helpers for assembling dashboard datasets."""
from __future__ import annotations

from datetime import date
from decimal import Decimal
from typing import Any

from sqlalchemy import case, func, select
from sqlalchemy.orm import Session
from sqlalchemy.sql import ColumnElement

from app.models import (
    Account,
    AccountOwnerType,
    Category,
    PositionAgg,
    Section,
    Transaction,
    TransactionDirection,
)

BreakdownResult = tuple[str, Decimal, list[tuple[date, str | None, Decimal]]]


def fetch_category_breakdown(
    session: Session,
    *,
    owner_type: AccountOwnerType,
    owner_id: int,
    section_name: str,
    direction: TransactionDirection,
    start_date: date,
    limit_transactions: int = 10,
) -> list[BreakdownResult]:
    """Return totals and sample transactions for a given section.

    The function returns a list of tuples ``(category_name, total_amount, transactions)``
    where ``transactions`` contains tuples ``(txn_date, description, amount)``.
    """

    category_label = func.coalesce(Category.name, "Uncategorised").label("category_name")
    total_column = func.coalesce(func.sum(Transaction.amount), 0).label("total")

    filters: list[ColumnElement[Any]] = [
        Account.owner_type == owner_type,
        Account.owner_id == owner_id,
        Section.name == section_name,
        Transaction.direction == direction,
    ]

    if start_date:
        filters.append(Transaction.txn_date >= start_date)

    totals_query = (
        select(category_label, total_column)
        .select_from(Transaction)
        .join(Account, Transaction.account_id == Account.id)
        .join(Section, Section.id == Transaction.section_id)
        .outerjoin(Category, Category.id == Transaction.category_id)
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
                Transaction.txn_date,
                Transaction.description,
                Transaction.amount,
            )
            .join(Account, Transaction.account_id == Account.id)
            .join(Section, Section.id == Transaction.section_id)
            .outerjoin(Category, Category.id == Transaction.category_id)
            .where(*filters, category_filter)
            .order_by(Transaction.txn_date.desc())
            .limit(limit_transactions)
        )
        txn_rows = session.execute(transactions_query).all()

        transactions = [
            (
                row.txn_date,
                row.description,
                Decimal(row.amount or 0),
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
    owner_type: AccountOwnerType,
    owner_id: int,
) -> Decimal:
    """Calculate total net worth for an entity (cash + brokerage holdings).
    
    Args:
        session: Database session
        owner_type: Type of owner (USER or ORG)
        owner_id: ID of the owner entity
        
    Returns:
        Total net worth as Decimal
    """
    # Calculate cash balance from all accounts
    cash_balance_query = (
        select(
            func.coalesce(
                func.sum(
                    case(
                        (Transaction.direction == TransactionDirection.CREDIT, Transaction.amount),
                        else_=-Transaction.amount,
                    )
                ),
                0,
            )
        )
        .select_from(Account)
        .join(Transaction, Transaction.account_id == Account.id, isouter=True)
        .where(
            Account.owner_type == owner_type,
            Account.owner_id == owner_id,
        )
    )
    cash_balance = session.execute(cash_balance_query).scalar_one() or Decimal("0")

    # Calculate brokerage holdings value from PositionAgg view
    holdings_query = (
        select(func.coalesce(func.sum(PositionAgg.qty * PositionAgg.last_price), 0))
        .select_from(PositionAgg)
        .join(Account, Account.id == PositionAgg.account_id)
        .where(
            Account.owner_type == owner_type,
            Account.owner_id == owner_id,
        )
    )
    holdings_value = session.execute(holdings_query).scalar_one() or Decimal("0")

    return cash_balance + holdings_value
