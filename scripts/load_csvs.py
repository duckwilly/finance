#!/usr/bin/env python3
import csv
import os
import sys
from contextlib import contextmanager
from pathlib import Path
from itertools import islice
from typing import Dict, Iterable, Iterator, Optional, Sequence, Tuple, TypeVar, Union

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import pymysql

SEED_DIR = Path("data/seed")

from app.config import get_settings
from app.log import get_logger, init_logging, log_context, progress_manager, timeit

logger = get_logger(__name__)
ALLOWED_ACCOUNT_TYPES = {"checking", "savings", "brokerage", "operating"}
ALLOWED_DIRECTIONS = {"DEBIT", "CREDIT"}
ALLOWED_CHANNELS = {"SEPA", "CARD", "WIRE", "CASH", "INTERNAL"}
ALLOWED_TRADE_SIDES = {"BUY", "SELL"}

settings = get_settings()
db_settings = settings.database

DB_CFG = dict(
    host=db_settings.host,
    port=db_settings.port,
    user=db_settings.user,
    password=db_settings.password,
    database=db_settings.name,
    charset="utf8mb4",
    autocommit=False,
    cursorclass=pymysql.cursors.DictCursor,
)

@contextmanager
def conn():
    target = f"{DB_CFG['user']}@{DB_CFG['host']}:{DB_CFG['port']}/{DB_CFG['database']}"
    logger.debug("Connecting to %s", target)
    c = pymysql.connect(**DB_CFG)
    logger.info("Connected to %s", target)
    try:
        yield c
        c.commit()
        logger.debug("Committed transaction for %s", target)
    except Exception:
        logger.exception("Error during database work against %s", target)
        c.rollback()
        raise
    finally:
        c.close()
        logger.debug("Closed connection to %s", target)

def read_rows(name: str) -> Sequence[dict]:
    path = SEED_DIR / name
    if not path.exists():
        return []
    with path.open(newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def stream_rows(name: str) -> Iterator[dict]:
    path = SEED_DIR / name
    if not path.exists():
        return
    with path.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            yield row


def count_rows(name: str) -> int:
    path = SEED_DIR / name
    if not path.exists():
        return 0
    with path.open(encoding="utf-8") as f:
        header = next(f, None)
        if header is None:
            return 0
        return sum(1 for _ in f)

T = TypeVar("T")


def chunked(iterable: Union[Sequence[T], Iterable[T]], size: int) -> Iterator[list[T]]:
    if size <= 0:
        raise ValueError("size must be positive")
    if isinstance(iterable, Sequence):
        for start in range(0, len(iterable), size):
            yield list(iterable[start : start + size])
        return

    iterator = iter(iterable)
    while True:
        chunk = list(islice(iterator, size))
        if not chunk:
            break
        yield chunk


def executemany_in_chunks(cur, sql: str, rows: Union[Sequence[Tuple], Iterable[Tuple]], batch_size: int = 1000) -> None:
    for chunk_rows in chunked(rows, batch_size):
        cur.executemany(sql, chunk_rows)


def insertmany_with_ids(
    cur,
    sql: str,
    rows: Union[Sequence[Tuple], Iterable[Tuple]],
    batch_size: int = 1000,
) -> list[int]:
    inserted_ids: list[int] = []
    for chunk_rows in chunked(rows, batch_size):
        cur.executemany(sql, chunk_rows)
        first_id = cur.lastrowid
        if not first_id:
            raise RuntimeError("Unable to determine lastrowid after batch insert")
        chunk_size = len(chunk_rows)
        inserted_ids.extend(range(first_id, first_id + chunk_size))
    return inserted_ids


CATEGORY_CACHE: Dict[Tuple[int, str], int] = {}


def get_or_create_category(cur, section_id, category_name):
    if not category_name:
        return None
    key = (section_id, category_name)
    cached = CATEGORY_CACHE.get(key)
    if cached:
        return cached
    cur.execute(
        "SELECT id FROM category WHERE section_id=%s AND name=%s",
        (section_id, category_name),
    )
    r = cur.fetchone()
    if r:
        CATEGORY_CACHE[key] = r["id"]
        return r["id"]
    cur.execute(
        "INSERT INTO category(section_id, name) VALUES (%s,%s)",
        (section_id, category_name),
    )
    CATEGORY_CACHE[key] = cur.lastrowid
    return cur.lastrowid

COUNTERPARTY_CACHE: Dict[Tuple[str, str, str], int] = {}


def get_or_create_counterparty(cur, name, account_ref, bic, country_code=None):
    key = (name or "", account_ref or "", bic or "")
    cached = COUNTERPARTY_CACHE.get(key)
    if cached:
        return cached
    cur.execute(
        """
        SELECT id FROM counterparty
        WHERE name=%s AND account_ref<=>%s AND bic<=>%s
    """,
        key,
    )
    r = cur.fetchone()
    if r:
        COUNTERPARTY_CACHE[key] = r["id"]
        return r["id"]
    cur.execute(
        """
        INSERT INTO counterparty(name, account_ref, bic, country_code)
        VALUES (%s,%s,%s,%s)
    """,
        (name or "", account_ref or None, bic or None, country_code),
    )
    COUNTERPARTY_CACHE[key] = cur.lastrowid
    return cur.lastrowid

def load_users_orgs_accounts_and_memberships():
    users = read_rows("users.csv")
    orgs  = read_rows("orgs.csv")
    accts = read_rows("accounts.csv")
    mems  = read_rows("account_memberships.csv")
    employment = read_rows("memberships.csv")

    user_map, org_map, acct_map = {}, {}, {}

    with conn() as c:
        cur = c.cursor()
        # users
        cur.execute("SELECT id, email FROM `user`")
        existing_users = {row["email"]: row["id"] for row in cur.fetchall()}
        pending_user_rows: list[tuple] = []
        pending_user_emails: list[str] = []
        pending_user_exts: Dict[str, list[str]] = {}
        for u in users:
            email = u["email"]
            uid = existing_users.get(email)
            if uid:
                user_map[u["ext_id"]] = uid
                continue
            ext_list = pending_user_exts.setdefault(email, [])
            if ext_list:
                ext_list.append(u["ext_id"])
                continue
            ext_list.append(u["ext_id"])
            pending_user_rows.append((u["name"], email, u.get("job_title")))
            pending_user_emails.append(email)

        if pending_user_rows:
            inserted = insertmany_with_ids(
                cur,
                "INSERT INTO `user`(name, email, job_title) VALUES (%s,%s,%s)",
                ((name, email, job or None) for name, email, job in pending_user_rows),
            )
            for email, new_id in zip(pending_user_emails, inserted):
                existing_users[email] = new_id
                for ext in pending_user_exts[email]:
                    user_map[ext] = new_id

        # orgs
        cur.execute("SELECT id, name FROM org")
        existing_orgs = {row["name"]: row["id"] for row in cur.fetchall()}
        pending_org_rows: list[tuple] = []
        pending_org_names: list[str] = []
        pending_org_exts: Dict[str, list[str]] = {}
        for o in orgs:
            name = o["name"]
            oid = existing_orgs.get(name)
            if oid:
                org_map[o["ext_id"]] = oid
                continue
            ext_list = pending_org_exts.setdefault(name, [])
            if ext_list:
                ext_list.append(o["ext_id"])
                continue
            ext_list.append(o["ext_id"])
            pending_org_rows.append((name,))
            pending_org_names.append(name)

        if pending_org_rows:
            inserted_orgs = insertmany_with_ids(
                cur,
                "INSERT INTO org(name) VALUES (%s)",
                pending_org_rows,
            )
            for name, new_id in zip(pending_org_names, inserted_orgs):
                existing_orgs[name] = new_id
                for ext in pending_org_exts[name]:
                    org_map[ext] = new_id

        # accounts
        cur.execute("SELECT id, owner_type, owner_id, type, name FROM account")
        existing_accounts = {
            (row["owner_type"], row["owner_id"], row["type"], row["name"] or None): row["id"]
            for row in cur.fetchall()
        }
        pending_accounts: list[tuple] = []
        pending_account_keys: list[tuple] = []
        pending_account_exts: Dict[tuple, list[str]] = {}

        for a in accts:
            owner_type = a["owner_type"]
            owner_ext = a["owner_ext_id"]

            if owner_type not in ("user", "org"):
                raise ValueError(
                    f"Unknown owner_type '{owner_type}' in accounts.csv for ext_id {a['ext_id']}"
                )

            if a["type"] not in ALLOWED_ACCOUNT_TYPES:
                raise ValueError(
                    f"Unsupported account type '{a['type']}' for owner {owner_type}:{owner_ext}; "
                    f"expected one of {sorted(ALLOWED_ACCOUNT_TYPES)}"
                )

            try:
                owner_id = user_map[owner_ext] if owner_type == "user" else org_map[owner_ext]
            except KeyError as exc:
                raise ValueError(
                    f"Account owner reference '{owner_ext}' (type {owner_type}) not found while loading accounts"
                ) from exc

            name = a.get("name") or None
            key = (owner_type, owner_id, a["type"], name)
            existing_id = existing_accounts.get(key)
            if existing_id:
                acct_map[a["ext_id"]] = existing_id
                continue

            ext_list = pending_account_exts.setdefault(key, [])
            if ext_list:
                ext_list.append(a["ext_id"])
                continue

            ext_list.append(a["ext_id"])
            pending_accounts.append(
                (
                    owner_type,
                    owner_id,
                    a["type"],
                    a["currency"],
                    name,
                    a.get("iban") or None,
                    a.get("opened_at") or None,
                )
            )
            pending_account_keys.append(key)

        if pending_accounts:
            inserted_accounts = insertmany_with_ids(
                cur,
                """
                INSERT INTO account (owner_type, owner_id, type, currency, name, iban, opened_at)
                VALUES (%s,%s,%s,%s,%s,%s,%s)
                """,
                pending_accounts,
            )
            for key, new_id in zip(pending_account_keys, inserted_accounts):
                existing_accounts[key] = new_id
                for ext in pending_account_exts[key]:
                    acct_map[ext] = new_id

        # memberships
        cur.execute("SELECT party_type, party_id, account_id FROM account_membership")
        existing_memberships = {
            (row["party_type"], row["party_id"], row["account_id"]) for row in cur.fetchall()
        }
        pending_memberships: list[tuple] = []
        for m in mems:
            party_type = m["party_type"]
            party_ext = m["party_ext_id"]
            if party_type == "user":
                try:
                    party_id = user_map[party_ext]
                except KeyError as exc:
                    raise ValueError(f"Membership references unknown user ext_id '{party_ext}'") from exc
            elif party_type == "org":
                try:
                    party_id = org_map[party_ext]
                except KeyError as exc:
                    raise ValueError(f"Membership references unknown org ext_id '{party_ext}'") from exc
            else:
                raise ValueError(f"Unknown party_type '{party_type}' in account_memberships.csv")

            try:
                account_id = acct_map[m["account_ext_id"]]
            except KeyError as exc:
                raise ValueError(
                    f"Membership references unknown account ext_id '{m['account_ext_id']}'"
                ) from exc

            key = (party_type, party_id, account_id)
            if key in existing_memberships:
                continue

            existing_memberships.add(key)
            pending_memberships.append((party_type, party_id, account_id, m["role"]))

        if pending_memberships:
            executemany_in_chunks(
                cur,
                """
                INSERT INTO account_membership(party_type, party_id, account_id, role)
                VALUES (%s,%s,%s,%s)
                """,
                pending_memberships,
            )

        # employer memberships (user -> org)
        employment_rows: list[tuple] = []
        for e in employment:
            try:
                uid = user_map[e["user_ext_id"]]
            except KeyError as exc:
                raise ValueError(f"Employment references unknown user '{e['user_ext_id']}'") from exc
            try:
                oid = org_map[e["org_ext_id"]]
            except KeyError as exc:
                raise ValueError(f"Employment references unknown org '{e['org_ext_id']}'") from exc

            employment_rows.append(
                (
                    uid,
                    oid,
                    e.get("role") or "employee",
                    1 if str(e.get("is_primary")).lower() in ("1", "true", "t", "yes") else 0,
                    e.get("start_date") or None,
                    e.get("end_date") or None,
                )
            )

        if employment_rows:
            executemany_in_chunks(
                cur,
                """
                INSERT INTO membership(user_id, org_id, role, is_primary, start_date, end_date)
                VALUES (%s,%s,%s,%s,%s,%s)
                ON DUPLICATE KEY UPDATE
                  role=VALUES(role),
                  is_primary=VALUES(is_primary),
                  start_date=VALUES(start_date),
                  end_date=VALUES(end_date)
                """,
                employment_rows,
            )

    return user_map, org_map, acct_map

def load_instruments_prices_fx():
    insts = read_rows("instruments.csv")
    prices = read_rows("price_daily.csv")
    fx = read_rows("fx_rate_daily.csv")
    inst_map: Dict[str, int] = {}

    with conn() as c:
        cur = c.cursor()

        cur.execute("SELECT id, symbol, mic FROM instrument")
        existing_instruments = {
            (row["symbol"], row["mic"] or None): row["id"] for row in cur.fetchall()
        }
        pending_instruments: list[tuple] = []
        pending_inst_keys: list[tuple[str, Optional[str]]] = []
        pending_inst_exts: Dict[tuple, list[str]] = {}

        for ins in insts:
            key = (ins["symbol"], ins.get("mic") or None)
            iid = existing_instruments.get(key)
            if iid:
                inst_map[ins["ext_id"]] = iid
                continue

            ext_list = pending_inst_exts.setdefault(key, [])
            if ext_list:
                ext_list.append(ins["ext_id"])
                continue

            ext_list.append(ins["ext_id"])
            pending_instruments.append(
                (
                    ins["symbol"],
                    ins.get("mic") or None,
                    ins["name"],
                    ins["type"],
                    ins["currency"],
                    ins.get("isin") or None,
                )
            )
            pending_inst_keys.append(key)

        if pending_instruments:
            inserted_instruments = insertmany_with_ids(
                cur,
                """
                INSERT INTO instrument(symbol, mic, name, type, currency, isin)
                VALUES (%s,%s,%s,%s,%s,%s)
                """,
                pending_instruments,
            )
            for key, new_id in zip(pending_inst_keys, inserted_instruments):
                existing_instruments[key] = new_id
                for ext in pending_inst_exts[key]:
                    inst_map[ext] = new_id

        price_rows: list[tuple] = []
        for pr in prices:
            iid = inst_map.get(pr["instrument_ext_id"])
            if not iid:
                continue
            price_rows.append((iid, pr["price_date"], pr["close_price"], pr["currency"]))

        if price_rows:
            executemany_in_chunks(
                cur,
                """
                INSERT INTO price_daily(instrument_id, price_date, close_price, currency)
                VALUES (%s,%s,%s,%s)
                ON DUPLICATE KEY UPDATE close_price=VALUES(close_price), currency=VALUES(currency)
                """,
                price_rows,
            )

        fx_rows = [(r["base"], r["quote"], r["rate_date"], r["rate"]) for r in fx]
        if fx_rows:
            executemany_in_chunks(
                cur,
                """
                INSERT INTO fx_rate_daily(base, quote, rate_date, rate)
                VALUES (%s,%s,%s,%s)
                ON DUPLICATE KEY UPDATE rate=VALUES(rate)
                """,
                fx_rows,
            )

    return inst_map

def load_transactions(acct_map):
    total_rows = count_rows("transactions.csv")
    if total_rows == 0:
        logger.info("No transactions.csv found — skipping transaction load.")
        return 0

    logger.info("Loading %s transactions from CSV", total_rows)

    with conn() as c:
        cur = c.cursor()

        # cache sections by name and pre-fill category cache
        cur.execute("SELECT id, name FROM section")
        sec_map = {row["name"]: row["id"] for row in cur.fetchall()}

        CATEGORY_CACHE.clear()
        COUNTERPARTY_CACHE.clear()
        cur.execute("SELECT id, section_id, name FROM category")
        for row in cur.fetchall():
            CATEGORY_CACHE[(row["section_id"], row["name"])] = row["id"]

        transfer_refs = set()
        processed = 0
        insert_batch_size = 1000
        commit_batch_size = 20000
        rows_since_commit = 0

        insert_stmt = """
            INSERT INTO `transaction`(
              account_id, posted_at, txn_date, amount, currency, direction,
              section_id, category_id, channel, description,
              counterparty_id, transfer_group_id, ext_reference
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """

        def flush_batch(*, final: bool = False) -> None:
            nonlocal processed, rows_since_commit
            if not batch:
                return
            cur.executemany(insert_stmt, batch)
            inserted_now = len(batch)
            processed += inserted_now
            rows_since_commit += inserted_now
            timer.add(inserted_now)
            task.advance(inserted_now)
            batch.clear()

            if rows_since_commit >= commit_batch_size or final:
                c.commit()
                rows_since_commit = 0
                logger.info("Committed %s transactions", processed)

        batch: list[tuple] = []

        with timeit(
            "transaction import",
            logger=logger,
            unit="rows",
            total=total_rows,
        ) as timer, progress_manager.task(
            "Inserting transactions", total=total_rows, unit="rows"
        ) as task:
            for t in stream_rows("transactions.csv"):
                try:
                    account_id = acct_map[t["account_ext_id"]]
                except KeyError as exc:
                    logger.error(
                        "Transaction references unknown account ext_id '%s'",
                        t["account_ext_id"],
                    )
                    raise ValueError(
                        f"Transaction references unknown account ext_id '{t['account_ext_id']}'"
                    ) from exc

                posted_at = t["posted_at"]
                txn_date = t["txn_date"]
                amount = t["amount"]
                currency = t["currency"]
                direction = (t["direction"] or "").upper()
                channel = (t["channel"] or "").upper()
                description = t.get("description") or None
                cp_name = t.get("counterparty_name") or None
                cp_acct = t.get("counterparty_account") or None
                cp_bic = t.get("counterparty_bic") or None
                transfer_ref = t.get("transfer_ref") or None
                ext_ref = t.get("ext_reference") or None

                if direction not in ALLOWED_DIRECTIONS:
                    logger.error(
                        "Unsupported transaction direction '%s' for account %s",
                        t["direction"],
                        t["account_ext_id"],
                    )
                    raise ValueError(
                        f"Unsupported transaction direction '{t['direction']}' for account {t['account_ext_id']}"
                    )

                if channel not in ALLOWED_CHANNELS:
                    logger.error(
                        "Unsupported transaction channel '%s' for account %s",
                        t["channel"],
                        t["account_ext_id"],
                    )
                    raise ValueError(
                        f"Unsupported transaction channel '{t['channel']}' for account {t['account_ext_id']}"
                    )

                section_name = t.get("section_name") or None
                category_name = t.get("category_name") or None

                if section_name and section_name not in sec_map:
                    logger.error("Unknown section %s", section_name)
                    raise ValueError(
                        f"Unknown section {section_name} (expected one of {list(sec_map)})"
                    )
                section_id = sec_map.get(section_name) if section_name else sec_map["transfer"]

                category_id = (
                    get_or_create_category(cur, section_id, category_name)
                    if category_name
                    else None
                )
                counterparty_id = None
                if any([cp_name, cp_acct, cp_bic]):
                    counterparty_id = get_or_create_counterparty(cur, cp_name, cp_acct, cp_bic)

                batch.append(
                    (
                        account_id,
                        posted_at,
                        txn_date,
                        amount,
                        currency,
                        direction,
                        section_id,
                        category_id,
                        channel,
                        description,
                        counterparty_id,
                        transfer_ref,
                        ext_ref,
                    )
                )

                if transfer_ref:
                    transfer_refs.add(transfer_ref)

                if len(batch) >= insert_batch_size:
                    flush_batch()

        flush_batch(final=True)

        # ensure final batch is committed before creating transfer links
        c.commit()

        if transfer_refs:
            logger.info("Linking %s transfer groups", len(transfer_refs))
            refs_list = list(transfer_refs)
            chunk_size = 1000

            with progress_manager.spinner(
                f"Linking {len(refs_list):,} transfer groups"
            ):
                for i in range(0, len(refs_list), chunk_size):
                    chunk = refs_list[i : i + chunk_size]
                    placeholders = ",".join(["%s"] * len(chunk))
                    cur.execute(
                        f"""
                        INSERT IGNORE INTO transfer_link(debit_txn_id, credit_txn_id)
                        SELECT d.id, c.id
                        FROM `transaction` d
                        JOIN `transaction` c
                          ON d.transfer_group_id = c.transfer_group_id
                         AND d.direction = 'DEBIT'
                         AND c.direction = 'CREDIT'
                        WHERE d.transfer_group_id IN ({placeholders})
                    """,
                        chunk,
                    )

                anomalies = []
                for i in range(0, len(refs_list), chunk_size):
                    chunk = refs_list[i : i + chunk_size]
                    placeholders = ",".join(["%s"] * len(chunk))
                    cur.execute(
                        f"""
                        SELECT transfer_group_id AS ref,
                               COUNT(*) AS cnt,
                               SUM(direction='DEBIT') AS debits,
                               SUM(direction='CREDIT') AS credits
                        FROM `transaction`
                        WHERE transfer_group_id IN ({placeholders})
                        GROUP BY transfer_group_id
                        HAVING cnt <> 2 OR debits <> 1 OR credits <> 1
                    """,
                        chunk,
                    )
                    anomalies.extend(cur.fetchall())

            for row in anomalies:
                logger.warning(
                    "transfer_ref %s has %s entries (%s debits/%s credits); skipping link.",
                    row["ref"],
                    row["cnt"],
                    row["debits"],
                    row["credits"],
                )

        return processed

def load_trades_and_holdings(acct_map, inst_map):
    trades = read_rows("trades.csv")
    if not trades:
        return

    with conn() as c:
        cur = c.cursor()
        cur.execute("SELECT id, account_id, instrument_id, qty, avg_cost FROM holding")
        holding_state: Dict[Tuple[int, int], Dict[str, Union[float, int]]] = {
            (row["account_id"], row["instrument_id"]): {
                "id": row["id"],
                "qty": float(row["qty"]),
                "avg_cost": float(row["avg_cost"]),
            }
            for row in cur.fetchall()
        }

        def get_or_create_holding(account_id: int, instrument_id: int) -> Dict[str, Union[float, int]]:
            key = (account_id, instrument_id)
            holding = holding_state.get(key)
            if holding:
                return holding
            new_id = insertmany_with_ids(
                cur,
                "INSERT INTO holding(account_id, instrument_id, qty, avg_cost) VALUES (%s,%s,0,0)",
                [(account_id, instrument_id)],
                batch_size=1,
            )[0]
            holding = {"id": new_id, "qty": 0.0, "avg_cost": 0.0}
            holding_state[key] = holding
            return holding

        trade_insert_stmt = """
            INSERT INTO trade(account_id, instrument_id, trade_time, side, qty, price, fees, tax, currency, settle_dt)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """
        lot_insert_stmt = """
            INSERT INTO lot(holding_id, trade_id, qty, cost_basis)
            VALUES (%s,%s,%s,%s)
        """
        trade_batch_size = 1000
        commit_batch_size = 10000
        rows_since_commit = 0
        trade_batch: list[tuple] = []
        trade_effects: list[tuple[int, float, float, float, float]] = []
        processed_trades = 0

        with timeit(
            "trade import",
            logger=logger,
            unit="trades",
            total=len(trades),
        ) as timer, progress_manager.task(
            "Processing trades", total=len(trades), unit="rows"
        ) as task:
            def flush_trade_batch(*, final: bool = False) -> None:
                nonlocal rows_since_commit
                if not trade_batch:
                    return
                trade_ids = insertmany_with_ids(
                    cur,
                    trade_insert_stmt,
                    trade_batch,
                    batch_size=trade_batch_size,
                )
                updates = [(new_qty, new_avg, holding_id) for holding_id, new_qty, new_avg, _, _ in trade_effects]
                lots = [
                    (holding_id, trade_id, lot_qty, lot_cost)
                    for trade_id, (holding_id, _, _, lot_qty, lot_cost) in zip(trade_ids, trade_effects)
                ]
                if updates:
                    executemany_in_chunks(
                        cur,
                        "UPDATE holding SET qty=%s, avg_cost=%s WHERE id=%s",
                        updates,
                        batch_size=trade_batch_size,
                    )
                if lots:
                    executemany_in_chunks(
                        cur,
                        lot_insert_stmt,
                        lots,
                        batch_size=trade_batch_size,
                    )
                processed_now = len(trade_ids)
                processed_trades += processed_now
                timer.add(processed_now)
                task.advance(processed_now)
                rows_since_commit += processed_now
                trade_batch.clear()
                trade_effects.clear()
                if rows_since_commit >= commit_batch_size or final:
                    c.commit()
                    rows_since_commit = 0
                    logger.info("Committed %s trades", processed_trades)

            for tr in trades:
                try:
                    account_id = acct_map[tr["account_ext_id"]]
                except KeyError as exc:
                    logger.error(
                        "Trade references unknown account ext_id '%s'",
                        tr["account_ext_id"],
                    )
                    raise ValueError(
                        f"Trade references unknown account ext_id '{tr['account_ext_id']}'"
                    ) from exc

                try:
                    instrument_id = inst_map[tr["instrument_ext_id"]]
                except KeyError as exc:
                    logger.error(
                        "Trade references unknown instrument ext_id '%s'",
                        tr["instrument_ext_id"],
                    )
                    raise ValueError(
                        f"Trade references unknown instrument ext_id '{tr['instrument_ext_id']}'"
                    ) from exc

                side = (tr["side"] or "").upper()

                if side not in ALLOWED_TRADE_SIDES:
                    logger.error(
                        "Unsupported trade side '%s' for account %s",
                        tr["side"],
                        tr["account_ext_id"],
                    )
                    raise ValueError(
                        f"Unsupported trade side '{tr['side']}' for account {tr['account_ext_id']}"
                    )

                qty = float(tr["qty"])
                price = float(tr["price"])
                fees = float(tr.get("fees") or 0.0)
                tax  = float(tr.get("tax") or 0.0)
                trade_time = tr["trade_time"]
                settle_dt  = tr.get("settle_dt") or None
                currency   = tr.get("currency") or "EUR"

                holding = get_or_create_holding(account_id, instrument_id)
                h_id = int(holding["id"])
                h_qty = float(holding["qty"])
                h_avg = float(holding["avg_cost"])

                if side == "BUY":
                    buy_cost = qty * price + fees + tax
                    new_qty = h_qty + qty
                    new_avg = (h_qty * h_avg + buy_cost) / new_qty if new_qty else 0.0
                    lot_qty = qty
                    lot_cost = buy_cost
                elif side == "SELL":
                    if qty > h_qty + 1e-9:
                        logger.error(
                            "Sell qty %s exceeds holding qty %s for account_id=%s instrument_id=%s",
                            qty,
                            h_qty,
                            account_id,
                            instrument_id,
                        )
                        raise ValueError(f"Sell qty {qty} exceeds holding qty {h_qty}")
                    new_qty = h_qty - qty
                    new_avg = h_avg
                    lot_qty = -qty
                    lot_cost = -(qty * h_avg)
                else:
                    raise ValueError(f"Unknown side {side}")

                holding["qty"] = new_qty
                holding["avg_cost"] = new_avg

                trade_batch.append(
                    (account_id, instrument_id, trade_time, side, qty, price, fees, tax, currency, settle_dt)
                )
                trade_effects.append((h_id, new_qty, new_avg, lot_qty, lot_cost))

                if len(trade_batch) >= trade_batch_size:
                    flush_trade_batch()

            flush_trade_batch(final=True)

def load_user_salary_monthly(user_map, org_map):
    salaries = read_rows("user_salary_monthly.csv")
    if not salaries:
        return 0
    with conn() as c:
        cur = c.cursor()
        logger.info("Loading %s user_salary_monthly rows…", len(salaries))
        rows: list[tuple] = []
        for r in salaries:
            user_ext = r["user_ext_id"]
            org_ext = r["org_ext_id"]
            try:
                uid = user_map[user_ext]
                oid = org_map[org_ext]
            except KeyError:
                continue
            rows.append(
                (
                    uid,
                    oid,
                    int(r["year"]),
                    int(r["month"]),
                    r["salary_amount"],
                )
            )
        if rows:
            executemany_in_chunks(
                cur,
                """
                INSERT INTO user_salary_monthly(user_id, employer_org_id, year, month, salary_amount)
                VALUES (%s,%s,%s,%s,%s)
                ON DUPLICATE KEY UPDATE salary_amount=VALUES(salary_amount)
                """,
                rows,
            )
    return len(rows)


def main():
    logger.info("Loading users/orgs/accounts/memberships…")
    user_map, org_map, acct_map = load_users_orgs_accounts_and_memberships()
    logger.info("Loading instruments/prices/fx…")
    inst_map = load_instruments_prices_fx()
    logger.info("Loading transactions…")
    inserted = load_transactions(acct_map)
    logger.info("Inserted %s transactions", inserted)
    logger.info("Loading user monthly salaries…")
    sal_inserted = load_user_salary_monthly(user_map, org_map)
    logger.info("Inserted %s user_salary_monthly rows", sal_inserted)
    logger.info("Loading trades & updating holdings (WAC)…")
    load_trades_and_holdings(acct_map, inst_map)
    logger.info("Load complete.")

if __name__ == "__main__":
    init_logging(app_name="csv-loader")
    log_context.bind(job="load_csvs", database=DB_CFG["database"], host=DB_CFG["host"])

    missing = [
        k
        for k in ["DB_HOST", "DB_PORT", "DB_USER", "DB_PASSWORD", "DB_NAME"]
        if os.getenv(k) is None
    ]
    if missing:
        logger.info("Using defaults for env vars: %s", ", ".join(missing))

    try:
        main()
    except Exception:
        logger.exception("Loader failed")
        sys.exit(1)
