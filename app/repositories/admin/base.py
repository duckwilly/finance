"""Shared helpers for admin repositories."""
from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from typing import Any

from sqlalchemy.engine import Result
from sqlalchemy.orm import Session
from sqlalchemy.sql.elements import TextClause


class BaseAdminRepository:
    """Base repository providing convenience helpers."""

    def __init__(self, session: Session) -> None:
        self._session = session

    def _scalar(self, statement: TextClause, params: dict[str, Any] | None = None) -> int:
        """Execute ``statement`` and return the scalar integer result."""

        result: Result[Any] = self._session.execute(statement, params or {})
        value = result.scalar() or 0
        return int(value)

    @staticmethod
    def _to_decimal(value: Any) -> Decimal:
        if isinstance(value, Decimal):
            return value
        if value is None:
            return Decimal(0)
        return Decimal(str(value))

    @staticmethod
    def _to_optional_decimal(value: Any) -> Decimal | None:
        if value is None:
            return None
        if isinstance(value, Decimal):
            return value
        return Decimal(str(value))

    @staticmethod
    def _coerce_date(value: Any) -> date:
        if isinstance(value, date) and not isinstance(value, datetime):
            return value
        if isinstance(value, datetime):
            return value.date()
        if value is None:
            raise ValueError("Cannot convert None to date")
        text_value = str(value)
        if len(text_value) >= 10:
            text_value = text_value[:10]
        return date.fromisoformat(text_value)

    @staticmethod
    def _coerce_datetime(value: Any) -> datetime:
        if isinstance(value, datetime):
            return value
        if isinstance(value, date):
            return datetime(value.year, value.month, value.day)
        if value is None:
            raise ValueError("Cannot convert None to datetime")
        text_value = str(value)
        try:
            return datetime.fromisoformat(text_value)
        except ValueError:
            if len(text_value) >= 19:
                return datetime.fromisoformat(text_value[:19])
            raise

    @staticmethod
    def _search_pattern(value: str | None) -> str | None:
        if not value:
            return None
        return f"%{value.lower()}%"
