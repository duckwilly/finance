"""Business logic for the administrative dashboard."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal

from sqlalchemy import text
from sqlalchemy.engine import Result
from sqlalchemy.sql import TextClause
from sqlalchemy.orm import Session


@dataclass(frozen=True)
class AdminMetrics:
    """Aggregate metrics shown on the admin landing page."""

    user_count: int
    org_count: int
    account_count: int
    transaction_count: int
    total_balance: Decimal


@dataclass(frozen=True)
class RecentAccount:
    """Lightweight representation of a recently opened account."""

    id: int
    name: str | None
    type: str
    opened_at: datetime


@dataclass(frozen=True)
class AdminDashboardData:
    """Container for all data required by the dashboard view."""

    generated_at: datetime
    metrics: AdminMetrics
    recent_accounts: list[RecentAccount]


class AdminDashboardService:
    """Service facade for retrieving admin dashboard data."""

    def __init__(self, session: Session) -> None:
        self._session = session

    def get_dashboard_data(self, *, recent_accounts: int = 5) -> AdminDashboardData:
        """Return dashboard metrics and auxiliary information."""

        metrics = self._fetch_metrics()
        accounts = self._fetch_recent_accounts(limit=recent_accounts)

        return AdminDashboardData(
            generated_at=datetime.utcnow(),
            metrics=metrics,
            recent_accounts=accounts,
        )

    def _fetch_metrics(self) -> AdminMetrics:
        """Collect aggregate counts and totals."""

        user_count = self._scalar(text("SELECT COUNT(*) FROM user"))
        org_count = self._scalar(text("SELECT COUNT(*) FROM org"))
        account_count = self._scalar(text("SELECT COUNT(*) FROM account"))
        transaction_count = self._scalar(text("SELECT COUNT(*) FROM `transaction`"))

        balance_result = self._session.execute(
            text(
                """
                SELECT COALESCE(SUM(CASE WHEN direction = 'CREDIT' THEN amount ELSE -amount END), 0)
                FROM `transaction`
                """
            )
        )
        total_balance = balance_result.scalar() or Decimal(0)

        return AdminMetrics(
            user_count=user_count,
            org_count=org_count,
            account_count=account_count,
            transaction_count=transaction_count,
            total_balance=Decimal(total_balance),
        )

    def _fetch_recent_accounts(self, *, limit: int) -> list[RecentAccount]:
        """Return the most recently opened accounts."""

        result = self._session.execute(
            text(
                """
                SELECT id, name, type, opened_at
                FROM account
                ORDER BY opened_at DESC
                LIMIT :limit
                """
            ),
            {"limit": limit},
        )

        accounts: list[RecentAccount] = []
        for row in result.mappings():
            accounts.append(
                RecentAccount(
                    id=int(row["id"]),
                    name=row.get("name"),
                    type=str(row["type"]),
                    opened_at=row["opened_at"],
                )
            )
        return accounts

    def _scalar(self, statement: TextClause) -> int:
        """Execute a scalar SQL statement returning an integer value."""

        result: Result = self._session.execute(statement)
        value = result.scalar()
        return int(value or 0)
