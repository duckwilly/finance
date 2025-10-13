#!/usr/bin/env python3
import csv
import logging
import os
import sys
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Dict, Iterator, Optional, Sequence, Tuple

import pymysql

SEED_DIR = Path("data/seed")

logger = logging.getLogger(__name__)


class ProgressTracker:
    """Light-weight terminal progress bar with optional totals."""

    def __init__(
        self,
        label: str,
        *,
        total: Optional[int] = None,
        unit: str = "rows",
        width: int = 30,
        min_interval: float = 0.5,
    ) -> None:
        self.label = label
        self.total = total
        self.unit = unit
        self.width = width
        self.min_interval = min_interval
        self.start = time.time()
        self.last_print = 0.0
        self.current = 0
        self._last_line_length = 0
        self._active = False

    def advance(self, step: int = 1) -> None:
        self.current += step
        self._display()

    def pause(self) -> None:
        if self._active:
            print()
            self._active = False
            self._last_line_length = 0

    def finish(self) -> None:
        self._display(force=True, final=True)

    def _display(self, force: bool = False, final: bool = False) -> None:
        now = time.time()
        if not force and now - self.last_print < self.min_interval and not final:
            return

        elapsed = max(now - self.start, 1e-9)
        rate = self.current / elapsed

        if self.total:
            pct = min(self.current / self.total, 1.0) if self.total else 0.0
            filled = int(self.width * pct)
            bar = "#" * filled + "-" * (self.width - filled)
            msg = (
                f"{self.label}: [{bar}] {self.current:,}/{self.total:,} "
                f"({pct * 100:5.1f}%) {rate:,.0f} {self.unit}/s"
            )
        else:
            msg = f"{self.label}: {self.current:,} {self.unit} ({rate:,.0f} {self.unit}/s)"

        padding = " " * max(0, self._last_line_length - len(msg))
        print(f"\r{msg}{padding}", end="", flush=True)
        self._last_line_length = len(msg)
        self.last_print = now
        self._active = True

        if final:
            print()
            self._active = False
            self._last_line_length = 0

ALLOWED_ACCOUNT_TYPES = {"checking", "savings", "brokerage", "operating"}
ALLOWED_DIRECTIONS = {"DEBIT", "CREDIT"}
ALLOWED_CHANNELS = {"SEPA", "CARD", "WIRE", "CASH", "INTERNAL"}
ALLOWED_TRADE_SIDES = {"BUY", "SELL"}

DB_CFG = dict(
    host=os.getenv("DB_HOST", "127.0.0.1"),
    port=int(os.getenv("DB_PORT", "3306")),
    user=os.getenv("DB_USER", "root"),
    password=os.getenv("DB_PASSWORD", ""),
    database=os.getenv("DB_NAME", "finance"),
    charset="utf8mb4",
    autocommit=False,
    cursorclass=pymysql.cursors.DictCursor,
)

@contextmanager
def conn():
    c = pymysql.connect(**DB_CFG)
    try:
        yield c
        c.commit()
    except Exception:
        c.rollback()
        raise
    finally:
        c.close()

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

def upsert_user(cur, name, email):
    # Unique by email (schema has UNIQUE email)
    cur.execute("SELECT id FROM `user` WHERE email=%s", (email,))
    row = cur.fetchone()
    if row:
        return row["id"]
    cur.execute("INSERT INTO `user`(name, email) VALUES (%s,%s)", (name, email))
    return cur.lastrowid

def upsert_org(cur, name):
    # No unique key on name; emulate idempotency by lookup
    cur.execute("SELECT id FROM `org` WHERE name=%s", (name,))
    row = cur.fetchone()
    if row:
        return row["id"]
    cur.execute("INSERT INTO `org`(name) VALUES (%s)", (name,))
    return cur.lastrowid

def get_section_id(cur, section_name):
    cur.execute("SELECT id FROM section WHERE name=%s", (section_name,))
    r = cur.fetchone()
    if not r:
        raise ValueError(f"Unknown section: {section_name}")
    return r["id"]

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

def upsert_account(cur, owner_type, owner_id, acct):
    # Try to find an existing account by owner + name + type (safe for seed)
    cur.execute("""
        SELECT id FROM account
        WHERE owner_type=%s AND owner_id=%s AND type=%s AND name<=>%s
    """, (owner_type, owner_id, acct["type"], acct["name"] or None))
    r = cur.fetchone()
    if r:
        return r["id"]
    cur.execute("""
        INSERT INTO account (owner_type, owner_id, type, currency, name, iban, opened_at)
        VALUES (%s,%s,%s,%s,%s,%s,%s)
    """, (owner_type, owner_id, acct["type"], acct["currency"], acct["name"] or None,
          acct["iban"] or None, acct["opened_at"]))
    return cur.lastrowid

def ensure_membership(cur, party_type, party_id, account_id, role):
    cur.execute("""
        SELECT id FROM account_membership
        WHERE party_type=%s AND party_id=%s AND account_id=%s
    """, (party_type, party_id, account_id))
    r = cur.fetchone()
    if r:
        return r["id"]
    cur.execute("""
        INSERT INTO account_membership(party_type, party_id, account_id, role)
        VALUES (%s,%s,%s,%s)
    """, (party_type, party_id, account_id, role))
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

def upsert_instrument(cur, symbol, mic, name, typ, currency, isin):
    cur.execute("SELECT id FROM instrument WHERE symbol=%s AND mic<=>%s", (symbol, mic or None))
    r = cur.fetchone()
    if r:
        return r["id"]
    cur.execute("""
        INSERT INTO instrument(symbol, mic, name, type, currency, isin)
        VALUES (%s,%s,%s,%s,%s,%s)
    """, (symbol, mic or None, name, typ, currency, isin or None))
    return cur.lastrowid

def get_holding(cur, account_id, instrument_id):
    cur.execute("""
        SELECT id, qty, avg_cost FROM holding
        WHERE account_id=%s AND instrument_id=%s
    """, (account_id, instrument_id))
    return cur.fetchone()

def ensure_holding(cur, account_id, instrument_id):
    h = get_holding(cur, account_id, instrument_id)
    if h:
        return h
    cur.execute("""
        INSERT INTO holding(account_id, instrument_id, qty, avg_cost)
        VALUES (%s,%s,0,0)
    """, (account_id, instrument_id))
    return {"id": cur.lastrowid, "qty": 0.0, "avg_cost": 0.0}

def insert_lot(cur, holding_id, trade_id, qty, cost_basis):
    cur.execute("""
        INSERT INTO lot(holding_id, trade_id, qty, cost_basis)
        VALUES (%s,%s,%s,%s)
    """, (holding_id, trade_id, qty, cost_basis))

def load_users_orgs_accounts_and_memberships():
    users = read_rows("users.csv")
    orgs  = read_rows("orgs.csv")
    accts = read_rows("accounts.csv")
    mems  = read_rows("account_memberships.csv")

    user_map, org_map, acct_map = {}, {}, {}

    with conn() as c:
        cur = c.cursor()

        # users
        for u in users:
            uid = upsert_user(cur, u["name"], u["email"])
            user_map[u["ext_id"]] = uid

        # orgs
        for o in orgs:
            oid = upsert_org(cur, o["name"])
            org_map[o["ext_id"]] = oid

        # accounts
        for a in accts:
            owner_type = a["owner_type"]
            owner_ext  = a["owner_ext_id"]

            if owner_type not in ("user", "org"):
                raise ValueError(f"Unknown owner_type '{owner_type}' in accounts.csv for ext_id {a['ext_id']}")

            if a["type"] not in ALLOWED_ACCOUNT_TYPES:
                raise ValueError(
                    f"Unsupported account type '{a['type']}' for owner {owner_type}:{owner_ext}; "
                    f"expected one of {sorted(ALLOWED_ACCOUNT_TYPES)}"
                )

            try:
                if owner_type == "user":
                    owner_id = user_map[owner_ext]
                else:
                    owner_id = org_map[owner_ext]
            except KeyError as exc:
                raise ValueError(
                    f"Account owner reference '{owner_ext}' (type {owner_type}) not found while loading accounts"
                ) from exc

            aid = upsert_account(cur, owner_type, owner_id, a)
            acct_map[a["ext_id"]] = aid

        # memberships
        for m in mems:
            party_type = m["party_type"]
            party_ext  = m["party_ext_id"]
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
            ensure_membership(cur, party_type, party_id, account_id, m["role"])

    return user_map, org_map, acct_map

def load_instruments_prices_fx():
    insts = read_rows("instruments.csv")
    prices = read_rows("price_daily.csv")
    fx = read_rows("fx_rate_daily.csv")
    inst_map = {}

    with conn() as c:
        cur = c.cursor()
        for ins in insts:
            iid = upsert_instrument(
                cur,
                ins["symbol"],
                ins.get("mic"),
                ins["name"],
                ins["type"],
                ins["currency"],
                ins.get("isin"),
            )
            inst_map[ins["ext_id"]] = iid

        # prices (optional)
        for pr in prices:
            iid = inst_map.get(pr["instrument_ext_id"])
            if not iid:
                continue
            cur.execute("""
                INSERT INTO price_daily(instrument_id, price_date, close_price, currency)
                VALUES (%s,%s,%s,%s)
                ON DUPLICATE KEY UPDATE close_price=VALUES(close_price), currency=VALUES(currency)
            """, (iid, pr["price_date"], pr["close_price"], pr["currency"]))

        # fx (optional)
        for r in fx:
            cur.execute("""
                INSERT INTO fx_rate_daily(base, quote, rate_date, rate)
                VALUES (%s,%s,%s,%s)
                ON DUPLICATE KEY UPDATE rate=VALUES(rate)
            """, (r["base"], r["quote"], r["rate_date"], r["rate"]))

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

        progress = ProgressTracker("Inserting transactions", total=total_rows)

        transfer_refs = set()
        processed = 0
        batch_size = 5000

        for t in stream_rows("transactions.csv"):
            processed += 1
            try:
                account_id = acct_map[t["account_ext_id"]]
            except KeyError as exc:
                progress.pause()
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
                progress.pause()
                raise ValueError(
                    f"Unsupported transaction direction '{t['direction']}' for account {t['account_ext_id']}"
                )

            if channel not in ALLOWED_CHANNELS:
                progress.pause()
                raise ValueError(
                    f"Unsupported transaction channel '{t['channel']}' for account {t['account_ext_id']}"
                )

            section_name = t.get("section_name") or None
            category_name = t.get("category_name") or None

            if section_name and section_name not in sec_map:
                progress.pause()
                raise ValueError(f"Unknown section {section_name} (expected one of {list(sec_map)})")
            section_id = sec_map.get(section_name) if section_name else sec_map["transfer"]

            category_id = (
                get_or_create_category(cur, section_id, category_name)
                if category_name
                else None
            )
            counterparty_id = None
            if any([cp_name, cp_acct, cp_bic]):
                counterparty_id = get_or_create_counterparty(cur, cp_name, cp_acct, cp_bic)

            cur.execute(
                """
                INSERT INTO `transaction`(
                  account_id, posted_at, txn_date, amount, currency, direction,
                  section_id, category_id, channel, description,
                  counterparty_id, transfer_group_id, ext_reference
                )
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """,
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
                ),
            )

            if transfer_ref:
                transfer_refs.add(transfer_ref)

            if processed % batch_size == 0:
                progress.pause()
                c.commit()
                logger.info("Committed %s transactions", processed)

            progress.advance()

        progress.finish()

        # ensure final batch is committed before creating transfer links
        c.commit()

        if transfer_refs:
            logger.info("Linking %s transfer groups", len(transfer_refs))
            refs_list = list(transfer_refs)
            chunk_size = 1000

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

        for tr in trades:
            try:
                account_id = acct_map[tr["account_ext_id"]]
            except KeyError as exc:
                raise ValueError(
                    f"Trade references unknown account ext_id '{tr['account_ext_id']}'"
                ) from exc

            try:
                instrument_id = inst_map[tr["instrument_ext_id"]]
            except KeyError as exc:
                raise ValueError(
                    f"Trade references unknown instrument ext_id '{tr['instrument_ext_id']}'"
                ) from exc

            side = (tr["side"] or "").upper()

            if side not in ALLOWED_TRADE_SIDES:
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

            # Insert trade
            cur.execute("""
                INSERT INTO trade(account_id, instrument_id, trade_time, side, qty, price, fees, tax, currency, settle_dt)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """, (account_id, instrument_id, trade_time, side, qty, price, fees, tax, currency, settle_dt))
            trade_id = cur.lastrowid

            # Update holding (WAC method)
            h = ensure_holding(cur, account_id, instrument_id)
            h_id, h_qty, h_avg = h["id"], float(h["qty"]), float(h["avg_cost"])

            if side == "BUY":
                buy_cost = qty * price + fees + tax
                new_qty = h_qty + qty
                new_avg = (h_qty * h_avg + buy_cost) / new_qty if new_qty else 0.0
                cur.execute("UPDATE holding SET qty=%s, avg_cost=%s WHERE id=%s", (new_qty, new_avg, h_id))
                insert_lot(cur, h_id, trade_id, qty, buy_cost)
            elif side == "SELL":
                if qty > h_qty + 1e-9:
                    raise ValueError(f"Sell qty {qty} exceeds holding qty {h_qty}")
                # Under WAC, avg stays constant; reduce qty and record a negative lot at WAC basis
                new_qty = h_qty - qty
                cur.execute("UPDATE holding SET qty=%s WHERE id=%s", (new_qty, h_id))
                sell_basis = qty * h_avg
                insert_lot(cur, h_id, trade_id, -qty, -sell_basis)
            else:
                raise ValueError(f"Unknown side {side}")

def main():
    logger.info("Loading users/orgs/accounts/memberships…")
    user_map, org_map, acct_map = load_users_orgs_accounts_and_memberships()
    logger.info("Loading instruments/prices/fx…")
    inst_map = load_instruments_prices_fx()
    logger.info("Loading transactions…")
    inserted = load_transactions(acct_map)
    logger.info("Inserted %s transactions", inserted)
    logger.info("Loading trades & updating holdings (WAC)…")
    load_trades_and_holdings(acct_map, inst_map)
    logger.info("Load complete.")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    # Quick sanity for env
    missing = [k for k in ["DB_HOST","DB_PORT","DB_USER","DB_PASSWORD","DB_NAME"] if os.getenv(k) is None]
    if missing:
        logger.info("Using defaults for env vars: %s", ", ".join(missing))
    try:
        main()
    except Exception as e:
        logger.exception("Loader failed: %s", e)
        sys.exit(1)
