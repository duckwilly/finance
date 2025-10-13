# monthly_overview.py
from __future__ import annotations

import os
import sys
from typing import Iterable, List, Tuple
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import text
from sqlalchemy.engine import Result

from db import get_engine
from logger import log

load_dotenv()

DBNAME = os.getenv("DB_NAME", "finance")
TABLE  = os.getenv("DB_TABLE", "transactions")  # expects columns: Date, Amount, Type ("Income"/"Expense")


def _quote_ident(name: str) -> str:
    """Backtick-quote identifiers for MySQL-like dialects; minimal validation."""
    if not name or any(ch in name for ch in "`\";() "):
        raise ValueError(f"Suspicious identifier: {name!r}")
    return f"`{name}`"


def _format_rows(rows: Iterable[Tuple[str, float, float]]) -> str:
    """Return a nicely aligned (string) table for logging."""
    header = ["Month", "Income", "Expense", "Net"]
    lines = [f"{header[0]:<7} | {header[1]:>12} | {header[2]:>12} | {header[3]:>12}"]
    lines.append("-" * len(lines[0]))
    total_inc = 0.0
    total_exp = 0.0

    for ym, income, expense in rows:
        income = float(income or 0)
        expense = float(expense or 0)
        net = income - expense
        total_inc += income
        total_exp += expense
        lines.append(f"{ym:<7} | {income:12.2f} | {expense:12.2f} | {net:12.2f}")

    lines.append("-" * len(lines[0]))
    lines.append(f"{'TOTAL':<7} | {total_inc:12.2f} | {total_exp:12.2f} | {(total_inc - total_exp):12.2f}")
    return "\n".join(lines)


def monthly_overview(year: int, table: str = TABLE) -> list[tuple[str, float, float]]:
    """
    Compute monthly income and expenses for a given year.

    Returns a list of tuples: (YYYY-MM, income, expense)
    """
    log.info(f"Preparing monthly overview for year={year} in database '{DBNAME}', table '{table}'.")
    engine = get_engine()
    log.debug("Acquired SQLAlchemy engine; opening connection.")

    date_col = _quote_ident("Date")
    amount_col = _quote_ident("Amount")
    type_col = _quote_ident("Type")
    table_ident = _quote_ident(table)

    # MySQL/MariaDB friendly aggregation
    q = text(
        f"""
        SELECT
            DATE_FORMAT({date_col}, '%Y-%m') AS ym,
            SUM(CASE WHEN {type_col} = 'Income' THEN {amount_col} ELSE 0 END) AS income,
            SUM(CASE WHEN {type_col} = 'Expense' THEN {amount_col} ELSE 0 END) AS expense
        FROM {table_ident}
        WHERE YEAR({date_col}) = :year
        GROUP BY ym
        ORDER BY ym
        """
    )

    with engine.connect() as con:
        log.debug("Executing monthly aggregation query.")
        res: Result = con.execute(q, {"year": int(year)})
        rows = [(r[0], float(r[1] or 0), float(r[2] or 0)) for r in res.fetchall()]

    if not rows:
        log.warning(f"No rows found for year {year} in '{table}'.")
        return []

    log.info(f"Monthly income/expense for {year}:\n{_format_rows(rows)}")
    return rows


# ---------------- CLI ----------------
if __name__ == "__main__":
    if len(sys.argv) < 2:
        log.warning("Usage: python monthly_overview.py <year>")
        sys.exit(1)

    try:
        year = int(sys.argv[1])
    except ValueError:
        log.error(f"Year must be an integer, got: {sys.argv[1]!r}")
        sys.exit(1)

    try:
        monthly_overview(year)
    except Exception as e:
        log.error(f"Failed to generate monthly overview for {year}: {e}")
        sys.exit(1)