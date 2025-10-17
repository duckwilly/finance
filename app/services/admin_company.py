"""Service logic for organization listings in the admin area."""
from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from math import ceil
from typing import Iterable, Mapping

from sqlalchemy import text
from sqlalchemy.engine import Result
from sqlalchemy.orm import Session
from sqlalchemy.sql.elements import TextClause


@dataclass(frozen=True)
class CompanySummary:
    """Aggregated view of an organization's financial footprint."""

    org_id: int
    name: str
    total_balance: Decimal
    payroll_headcount: int


@dataclass(frozen=True)
class CompanyPage:
    """Paginated collection of ``CompanySummary`` items."""

    items: list[CompanySummary]
    total: int
    page: int
    page_size: int

    @property
    def total_pages(self) -> int:
        """Return the number of pages represented by the dataset."""

        if self.total == 0:
            return 1
        return ceil(self.total / self.page_size)


class AdminCompanyService:
    """Facade exposing company level aggregates for administrators."""

    _PAYROLL_SECTION_ID = 2
    _PAYROLL_PATTERNS: tuple[str, str, str] = ("%payroll%", "%salary%", "%wage%")

    def __init__(self, session: Session) -> None:
        self._session = session

    def list_companies(
        self,
        *,
        page: int,
        page_size: int,
        search: str | None = None,
    ) -> CompanyPage:
        """Return a paginated set of organization aggregates."""

        if page < 1:
            page = 1
        if page_size < 1:
            raise ValueError("page_size must be greater than zero")

        search_pattern = f"%{search.lower()}%" if search else None

        total = self._scalar(
            text(
                """
                SELECT COUNT(*)
                FROM org o
                WHERE (:search IS NULL OR LOWER(o.name) LIKE :search)
                """
            ),
            {"search": search_pattern},
        )

        max_page = max(1, ceil(total / page_size)) if total else 1
        page = min(page, max_page)
        offset = (page - 1) * page_size

        result = self._session.execute(
            text(
                """
                WITH filtered_orgs AS (
                    SELECT o.id, o.name
                    FROM org o
                    WHERE (:search IS NULL OR LOWER(o.name) LIKE :search)
                ),
                balance_agg AS (
                    SELECT fo.id AS org_id, COALESCE(SUM(v.balance), 0) AS total_balance
                    FROM filtered_orgs fo
                    LEFT JOIN account a
                        ON a.owner_type = 'org' AND a.owner_id = fo.id
                    LEFT JOIN v_account_balance v
                        ON v.account_id = a.id
                    GROUP BY fo.id
                ),
                payroll_agg AS (
                    SELECT
                        fo.id AS org_id,
                        COUNT(DISTINCT CASE
                            WHEN c.section_id = :expense_section
                             AND (
                                LOWER(c.name) LIKE :pattern_payroll
                                OR LOWER(c.name) LIKE :pattern_salary
                                OR LOWER(c.name) LIKE :pattern_wage
                             )
                            THEN t.counterparty_id
                        END) AS payroll_headcount
                    FROM filtered_orgs fo
                    LEFT JOIN account a
                        ON a.owner_type = 'org' AND a.owner_id = fo.id
                    LEFT JOIN `transaction` t
                        ON t.account_id = a.id
                    LEFT JOIN category c
                        ON c.id = t.category_id
                    GROUP BY fo.id
                )
                SELECT
                    fo.id AS org_id,
                    fo.name AS org_name,
                    COALESCE(balance_agg.total_balance, 0) AS total_balance,
                    COALESCE(payroll_agg.payroll_headcount, 0) AS payroll_headcount
                FROM filtered_orgs fo
                LEFT JOIN balance_agg ON balance_agg.org_id = fo.id
                LEFT JOIN payroll_agg ON payroll_agg.org_id = fo.id
                ORDER BY fo.name
                LIMIT :limit OFFSET :offset
                """
            ),
            {
                "search": search_pattern,
                "limit": page_size,
                "offset": offset,
                "expense_section": self._PAYROLL_SECTION_ID,
                "pattern_payroll": self._PAYROLL_PATTERNS[0],
                "pattern_salary": self._PAYROLL_PATTERNS[1],
                "pattern_wage": self._PAYROLL_PATTERNS[2],
            },
        )

        items = self._parse_rows(result.mappings())

        return CompanyPage(
            items=items,
            total=total,
            page=page,
            page_size=page_size,
        )

    def _parse_rows(self, rows: Iterable[Mapping[str, object]]) -> list[CompanySummary]:
        summaries: list[CompanySummary] = []
        for row in rows:
            balance_raw = row.get("total_balance")
            if isinstance(balance_raw, Decimal):
                balance_value = balance_raw
            elif balance_raw is None:
                balance_value = Decimal(0)
            else:
                balance_value = Decimal(str(balance_raw))
            summaries.append(
                CompanySummary(
                    org_id=int(row["org_id"]),
                    name=str(row["org_name"]),
                    total_balance=balance_value,
                    payroll_headcount=int(row.get("payroll_headcount") or 0),
                )
            )
        return summaries

    def _scalar(self, statement: TextClause, params: dict | None = None) -> int:
        result: Result = self._session.execute(statement, params or {})
        value = result.scalar() or 0
        return int(value)

