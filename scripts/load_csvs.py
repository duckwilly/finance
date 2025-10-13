#!/usr/bin/env python3
import os, sys, csv
from pathlib import Path
from contextlib import contextmanager
import pymysql

SEED_DIR = Path("data/seed")

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

def read_rows(name):
    path = SEED_DIR / name
    if not path.exists():
        return []
    with path.open(newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))

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

def get_or_create_category(cur, section_id, category_name):
    if not category_name:
        return None
    cur.execute("SELECT id FROM category WHERE section_id=%s AND name=%s",
                (section_id, category_name))
    r = cur.fetchone()
    if r:
        return r["id"]
    cur.execute("INSERT INTO category(section_id, name) VALUES (%s,%s)",
                (section_id, category_name))
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

def get_or_create_counterparty(cur, name, account_ref, bic, country_code=None):
    key = (name or "", account_ref or "", bic or "")
    cur.execute("""
        SELECT id FROM counterparty
        WHERE name=%s AND account_ref<=>%s AND bic<=>%s
    """, key)
    r = cur.fetchone()
    if r:
        return r["id"]
    cur.execute("""
        INSERT INTO counterparty(name, account_ref, bic, country_code)
        VALUES (%s,%s,%s,%s)
    """, (name or "", account_ref or None, bic or None, country_code))
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
            if owner_type == "user":
                owner_id = user_map[owner_ext]
            elif owner_type == "org":
                owner_id = org_map[owner_ext]
            else:
                raise ValueError(f"Unknown owner_type: {owner_type}")
            aid = upsert_account(cur, owner_type, owner_id, a)
            acct_map[a["ext_id"]] = aid

        # memberships
        for m in mems:
            party_type = m["party_type"]
            party_ext  = m["party_ext_id"]
            if party_type == "user":
                party_id = user_map[party_ext]
            elif party_type == "org":
                party_id = org_map[party_ext]
            else:
                raise ValueError(f"Unknown party_type: {party_type}")
            account_id = acct_map[m["account_ext_id"]]
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
    txns = read_rows("transactions.csv")
    if not txns:
        return

    with conn() as c:
        cur = c.cursor()

        # cache sections by name
        cur.execute("SELECT id, name FROM section")
        sec_map = {row["name"]: row["id"] for row in cur.fetchall()}

        # keep track of inserted txns per transfer_ref for linking
        transfer_bucket = {}

        for t in txns:
            account_id = acct_map[t["account_ext_id"]]
            posted_at  = t["posted_at"]
            txn_date   = t["txn_date"]
            amount     = t["amount"]
            currency   = t["currency"]
            direction  = t["direction"]
            channel    = t["channel"]
            description= t.get("description") or None
            cp_name    = t.get("counterparty_name") or None
            cp_acct    = t.get("counterparty_account") or None
            cp_bic     = t.get("counterparty_bic") or None
            transfer_ref = t.get("transfer_ref") or None
            ext_ref      = t.get("ext_reference") or None

            section_name = t.get("section_name") or None
            category_name= t.get("category_name") or None

            if section_name and section_name not in sec_map:
                raise ValueError(f"Unknown section {section_name} (expected one of {list(sec_map)})")
            section_id = sec_map.get(section_name) if section_name else sec_map["transfer"]  # fallback

            category_id = get_or_create_category(cur, section_id, category_name) if category_name else None
            counterparty_id = None
            if any([cp_name, cp_acct, cp_bic]):
                counterparty_id = get_or_create_counterparty(cur, cp_name, cp_acct, cp_bic)

            cur.execute("""
                INSERT INTO `transaction`(
                  account_id, posted_at, txn_date, amount, currency, direction,
                  section_id, category_id, channel, description,
                  counterparty_id, transfer_group_id, ext_reference
                )
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """, (account_id, posted_at, txn_date, amount, currency, direction,
                  section_id, category_id, channel, description,
                  counterparty_id, transfer_ref, ext_ref))
            txn_id = cur.lastrowid

            if transfer_ref:
                transfer_bucket.setdefault(transfer_ref, []).append((txn_id, direction))

        # pair internal transfers
        for ref, entries in transfer_bucket.items():
            if len(entries) != 2:
                # Soft warning; skip imperfect groups
                print(f"[warn] transfer_ref {ref} has {len(entries)} entries (expected 2); skipping link.")
                continue
            # find debit/credit ids
            debit_id = next((i for i,d in entries if d == "DEBIT"), None)
            credit_id= next((i for i,d in entries if d == "CREDIT"), None)
            if not debit_id or not credit_id:
                print(f"[warn] transfer_ref {ref} missing debit/credit; skipping link.")
                continue
            cur.execute("""
                INSERT IGNORE INTO transfer_link(debit_txn_id, credit_txn_id)
                VALUES (%s,%s)
            """, (debit_id, credit_id))

def load_trades_and_holdings(acct_map, inst_map):
    trades = read_rows("trades.csv")
    if not trades:
        return

    with conn() as c:
        cur = c.cursor()

        for tr in trades:
            account_id = acct_map[tr["account_ext_id"]]
            instrument_id = inst_map[tr["instrument_ext_id"]]
            side = tr["side"].upper()
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
    print("Loading users/orgs/accounts/memberships…")
    user_map, org_map, acct_map = load_users_orgs_accounts_and_memberships()
    print("Loading instruments/prices/fx…")
    inst_map = load_instruments_prices_fx()
    print("Loading transactions…")
    load_transactions(acct_map)
    print("Loading trades & updating holdings (WAC)…")
    load_trades_and_holdings(acct_map, inst_map)
    print("Done.")

if __name__ == "__main__":
    # Quick sanity for env
    missing = [k for k in ["DB_HOST","DB_PORT","DB_USER","DB_PASSWORD","DB_NAME"] if os.getenv(k) is None]
    if missing:
        print(f"[note] Using defaults for: {', '.join(missing)} (override with env vars)")
    try:
        main()
    except Exception as e:
        print("ERROR:", e)
        sys.exit(1)
