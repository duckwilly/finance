"""
Simple SQL Manager (SQLAlchemy)
-------------------------------
Works with your shared engine: from db import get_engine

Usage:
    python manager.py               # shows tables and row counts
    python manager.py preview 10    # show 10 rows
    python manager.py unique Category
    python manager.py export
"""

import os
import csv
import sys
from pathlib import Path
from typing import Iterable

from dotenv import load_dotenv
from sqlalchemy import text, inspect
from db import get_engine

from logger import log

load_dotenv()

log.info("RUNNING FILE: manager-1.py")

# Defaults match your current project setup
DBNAME = os.getenv("DB_NAME", "finance")          # informational only
TABLE  = os.getenv("DB_TABLE", "transactions")    # set via env if you like

# ---------- helpers ----------------------------------------------------------

def _quote_ident(name: str) -> str:
    """Backtick-quote identifiers for MySQL-like dialects; minimal validation."""
    if not name or any(ch in name for ch in "`\";() "):
        raise ValueError(f"Suspicious identifier: {name!r}")
    return f"`{name}`"

def _print_table(rows: Iterable[Iterable], header: list[str] | None = None):
    """Format rows into readable strings for logging."""
    lines = []
    if header:
        lines.append("Columns: " + ", ".join(header))
    for r in rows:
        lines.append(str(r))
    return "\n".join(lines)

# ---------- core ops ----------------------------------------------------------

def list_tables():
    engine = get_engine()
    insp = inspect(engine)
    names = insp.get_table_names()
    log.info(f"Tables in {DBNAME}: {', '.join(names) if names else '(none found)'}")
    return names

def count_rows(table: str):
    engine = get_engine()
    q = text(f"SELECT COUNT(*) FROM {_quote_ident(table)}")
    with engine.connect() as con:
        count = con.execute(q).scalar_one()
    log.info(f"Table '{table}' has {count} rows.")
    return count

def preview(table: str, limit: int = 5):
    engine = get_engine()
    q = text(f"SELECT * FROM {_quote_ident(table)} LIMIT :lim")
    with engine.connect() as con:
        res = con.execute(q, {"lim": int(limit)})
        cols = list(res.keys())
        rows = res.fetchall()

    body = _print_table(rows, header=cols)
    log.info(f"Preview of '{table}' (first {limit} rows):\n{body}")

def show_unique(table: str, column: str):
    engine = get_engine()
    # Validate column exists
    insp = inspect(engine)
    colnames = {c["name"] for c in insp.get_columns(table)}
    if column not in colnames:
        log.error(f"Column '{column}' not found in '{table}'. Available: {sorted(colnames)}")
        raise ValueError(f"Column '{column}' not found in '{table}'.")

    q = text(
        f"SELECT DISTINCT {_quote_ident(column)} "
        f"FROM {_quote_ident(table)} "
        f"ORDER BY {_quote_ident(column)} "
        f"LIMIT 50"
    )
    with engine.connect() as con:
        values = [r[0] for r in con.execute(q).fetchall()]

    joined = "\n   ".join(str(v) for v in values)
    log.info(f"Unique values in '{column}' (max 50 shown):\n   {joined}\n({len(values)} unique values shown)")
    return values

def export_to_csv(table: str, out_path: str | Path | None = None):
    if out_path is None:
        out_path = Path(f"{table}_export.csv")

    engine = get_engine()
    q = text(f"SELECT * FROM {_quote_ident(table)}")
    with engine.connect() as con:
        res = con.execute(q)
        cols = list(res.keys())
        rows = res.fetchall()

    with open(out_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(cols)
        w.writerows(rows)

    log.info(f"Exported table '{table}' to '{out_path}' ({len(rows)} rows).")

# ---------- CLI --------------------------------------------------------------

if __name__ == "__main__":
    args = sys.argv[1:]

    try:
        tables = list_tables()
        if not tables:
            log.warning("No tables found in database.")
    except Exception as e:
        log.error(f"Could not list tables: {e}")
        sys.exit(1)

    try:
        count_rows(TABLE)
    except Exception as e:
        log.warning(f"Could not count rows in '{TABLE}': {e}")

    if not args:
        preview(TABLE)
    elif args[0] == "preview":
        n = int(args[1]) if len(args) > 1 else 5
        preview(TABLE, n)
    elif args[0] == "unique":
        if len(args) < 2:
            log.warning("Usage: python manager.py unique <column_name>")
        else:
            show_unique(TABLE, args[1])
    elif args[0] == "export":
        export_to_csv(TABLE)
    else:
        log.warning("Unknown command. Try: preview | unique <column> | export")