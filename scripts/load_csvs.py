#!/usr/bin/env python3
import csv
import os
import sys
from contextlib import contextmanager
from datetime import date, timedelta
from pathlib import Path
from itertools import islice
from typing import Dict, Iterable, Iterator, Optional, Sequence, Tuple, TypeVar, Union
from decimal import Decimal

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import pymysql

SEED_DIR = Path("data/seed")

from app.config import get_settings
from app.log import get_logger, init_logging, log_context, progress_manager, timeit

logger = get_logger(__name__)
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


def month_start(day: date) -> date:
    return date(day.year, day.month, 1)


def month_end(day: date) -> date:
    if day.month == 12:
        return date(day.year, 12, 31)
    return date(day.year, day.month + 1, 1) - timedelta(days=1)

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


def load_parties_and_profiles() -> Dict[str, int]:
    parties = read_rows("parties.csv")
    individual_profiles = read_rows("individual_profiles.csv")
    company_profiles = read_rows("company_profiles.csv")

    if not parties and not individual_profiles and not company_profiles:
        return {}

    party_map: Dict[str, int] = {}

    with conn() as c:
        cur = c.cursor()
        cur.execute("SELECT id, party_type, display_name FROM party")
        existing = {(row["party_type"], row["display_name"]): row["id"] for row in cur.fetchall()}

        pending_rows: list[tuple] = []
        pending_keys: list[tuple] = []
        pending_exts: Dict[tuple, list[str]] = {}

        for row in parties:
            key = (row["party_type"], row["display_name"])
            existing_id = existing.get(key)
            if existing_id:
                party_map[row["ext_id"]] = existing_id
                continue

            ext_list = pending_exts.setdefault(key, [])
            if ext_list:
                ext_list.append(row["ext_id"])
                continue

            ext_list.append(row["ext_id"])
            pending_rows.append((row["party_type"], row["display_name"]))
            pending_keys.append(key)

        if pending_rows:
            inserted = insertmany_with_ids(
                cur,
                "INSERT INTO party(party_type, display_name) VALUES (%s,%s)",
                pending_rows,
            )
            for key, new_id in zip(pending_keys, inserted):
                existing[key] = new_id
                for ext in pending_exts[key]:
                    party_map[ext] = new_id

        for row in parties:
            key = (row["party_type"], row["display_name"])
            if row["ext_id"] not in party_map and key in existing:
                party_map[row["ext_id"]] = existing[key]

        if individual_profiles:
            profile_rows = []
            for profile in individual_profiles:
                party_id = party_map.get(profile["party_ext_id"])
                if not party_id:
                    continue
                profile_rows.append(
                    (
                        party_id,
                        profile["given_name"],
                        profile["family_name"],
                        profile["primary_email"],
                        profile.get("residency_country") or None,
                        profile.get("birth_date") or None,
                    )
                )
            if profile_rows:
                executemany_in_chunks(
                    cur,
                    """
                    INSERT INTO individual_profile(
                        party_id, given_name, family_name, primary_email, residency_country, birth_date
                    )
                    VALUES (%s,%s,%s,%s,%s,%s)
                    ON DUPLICATE KEY UPDATE
                        given_name=VALUES(given_name),
                        family_name=VALUES(family_name),
                        primary_email=VALUES(primary_email),
                        residency_country=VALUES(residency_country),
                        birth_date=VALUES(birth_date)
                    """,
                    profile_rows,
                )

        if company_profiles:
            company_rows = []
            for profile in company_profiles:
                party_id = party_map.get(profile["party_ext_id"])
                if not party_id:
                    continue
                company_rows.append(
                    (
                        party_id,
                        profile["legal_name"],
                        profile.get("registration_number") or None,
                        profile.get("tax_identifier") or None,
                        profile.get("industry_code") or None,
                        profile.get("incorporation_date") or None,
                    )
                )
            if company_rows:
                executemany_in_chunks(
                    cur,
                    """
                    INSERT INTO company_profile(
                        party_id, legal_name, registration_number, tax_identifier, industry_code, incorporation_date
                    )
                    VALUES (%s,%s,%s,%s,%s,%s)
                    ON DUPLICATE KEY UPDATE
                        legal_name=VALUES(legal_name),
                        registration_number=VALUES(registration_number),
                        tax_identifier=VALUES(tax_identifier),
                        industry_code=VALUES(industry_code),
                        incorporation_date=VALUES(incorporation_date)
                    """,
                    company_rows,
                )

    return party_map


def load_app_users_and_roles(party_map: Dict[str, int]) -> Dict[str, int]:
    app_users = read_rows("app_users.csv")
    app_user_roles = read_rows("app_user_roles.csv")

    if not app_users:
        return {}

    app_user_map: Dict[str, int] = {}

    with conn() as c:
        cur = c.cursor()
        cur.execute("SELECT id, username, party_id FROM app_user")
        existing_rows = cur.fetchall()
        existing_users = {row["username"]: row["id"] for row in existing_rows}
        existing_party_users = {row["party_id"]: row["id"] for row in existing_rows if row["party_id"]}

        pending_rows: list[tuple] = []
        pending_usernames: list[str] = []
        pending_exts: Dict[str, list[str]] = {}
        seen_party_ids: set[int] = set()

        for row in app_users:
            username = row["username"]
            existing_id = existing_users.get(username)
            party_ext = row.get("party_ext_id") or ""
            party_id = party_map.get(party_ext) if party_ext else None
            
            if existing_id:
                app_user_map[row["ext_id"]] = existing_id
                continue
            
            # Check if party_id already has an app_user (either existing or pending)
            if party_id and party_id in existing_party_users:
                logger.warning(
                    "Skipping app_user %s: party_id %s already has an app_user (id=%s)",
                    row["ext_id"], party_id, existing_party_users[party_id]
                )
                app_user_map[row["ext_id"]] = existing_party_users[party_id]
                continue
            
            if party_id and party_id in seen_party_ids:
                logger.warning(
                    "Skipping app_user %s: party_id %s already pending for insert in this batch",
                    row["ext_id"], party_id
                )
                continue

            ext_list = pending_exts.setdefault(username, [])
            if ext_list:
                ext_list.append(row["ext_id"])
                continue

            ext_list.append(row["ext_id"])
            if party_id:
                seen_party_ids.add(party_id)
            pending_rows.append(
                (
                    party_id,
                    username,
                    row.get("email") or None,
                    row.get("password_hash") or "",
                    1 if str(row.get("is_active", "1")).lower() in {"1", "true", "yes"} else 0,
                )
            )
            pending_usernames.append(username)

        if pending_rows:
            inserted = insertmany_with_ids(
                cur,
                "INSERT INTO app_user(party_id, username, email, password_hash, is_active) VALUES (%s,%s,%s,%s,%s)",
                pending_rows,
            )
            for username, new_id in zip(pending_usernames, inserted):
                existing_users[username] = new_id
                for ext in pending_exts[username]:
                    app_user_map[ext] = new_id

        for row in app_users:
            if row["ext_id"] not in app_user_map:
                existing_id = existing_users.get(row["username"])
                if existing_id:
                    app_user_map[row["ext_id"]] = existing_id

        if app_user_roles:
            role_rows = []
            for row in app_user_roles:
                app_user_id = app_user_map.get(row["app_user_ext_id"])
                role_code = row.get("role_code")
                if not app_user_id or not role_code:
                    continue
                role_rows.append((app_user_id, role_code))
            if role_rows:
                executemany_in_chunks(
                    cur,
                    "INSERT IGNORE INTO app_user_role(app_user_id, role_code) VALUES (%s,%s)",
                    role_rows,
                )

    return app_user_map


def load_employment_and_access(
    party_map: Dict[str, int],
    app_user_map: Dict[str, int],
) -> Dict[str, int]:
    contracts = read_rows("employment_contracts.csv")
    grants = read_rows("company_access_grants.csv")
    relationships = read_rows("party_relationships.csv")

    if not contracts and not grants and not relationships:
        return {}

    contract_map: Dict[str, int] = {}

    with conn() as c:
        cur = c.cursor()
        cur.execute(
            "SELECT id, employee_party_id, employer_party_id, start_date FROM employment_contract"
        )
        existing_contracts = {
            (row["employee_party_id"], row["employer_party_id"], str(row["start_date"])): row["id"]
            for row in cur.fetchall()
        }

        pending_rows: list[tuple] = []
        pending_keys: list[tuple] = []
        pending_exts: Dict[tuple, list[str]] = {}

        for row in contracts:
            employee_id = party_map.get(row["employee_party_ext_id"])
            employer_id = party_map.get(row["employer_party_ext_id"])
            if not employee_id or not employer_id:
                continue
            start_date = row.get("start_date") or "2023-01-01"
            key = (employee_id, employer_id, start_date)
            existing_id = existing_contracts.get(key)
            if existing_id:
                contract_map[row["ext_id"]] = existing_id
                continue

            ext_list = pending_exts.setdefault(key, [])
            if ext_list:
                ext_list.append(row["ext_id"])
                continue

            is_primary = 1 if str(row.get("is_primary", "1")).lower() in {"1", "true", "yes"} else 0
            ext_list.append(row["ext_id"])
            pending_rows.append(
                (
                    employee_id,
                    employer_id,
                    row.get("position_title") or "Employee",
                    start_date,
                    row.get("end_date") or None,
                    is_primary,
                )
            )
            pending_keys.append(key)

        if pending_rows:
            inserted_contracts = insertmany_with_ids(
                cur,
                """
                INSERT INTO employment_contract(
                    employee_party_id, employer_party_id, position_title, start_date, end_date, is_primary
                )
                VALUES (%s,%s,%s,%s,%s,%s)
                """,
                pending_rows,
            )
            for key, new_id in zip(pending_keys, inserted_contracts):
                existing_contracts[key] = new_id
                for ext in pending_exts[key]:
                    contract_map[ext] = new_id

        for row in contracts:
            if row["ext_id"] not in contract_map:
                employee_id = party_map.get(row["employee_party_ext_id"])
                employer_id = party_map.get(row["employer_party_ext_id"])
                start_date = row.get("start_date") or "2023-01-01"
                key = (employee_id, employer_id, start_date)
                existing_id = existing_contracts.get(key)
                if existing_id:
                    contract_map[row["ext_id"]] = existing_id

        if grants:
            grant_rows = []
            for row in grants:
                contract_id = contract_map.get(row["contract_ext_id"])
                app_user_id = app_user_map.get(row["app_user_ext_id"])
                role_code = row.get("role_code")
                if not (contract_id and app_user_id and role_code):
                    continue
                grant_rows.append(
                    (
                        contract_id,
                        app_user_id,
                        role_code,
                        row.get("granted_at") or None,
                        row.get("revoked_at") or None,
                    )
                )
            if grant_rows:
                executemany_in_chunks(
                    cur,
                    """
                    INSERT INTO company_access_grant(
                        contract_id, app_user_id, role_code, granted_at, revoked_at
                    )
                    VALUES (%s,%s,%s,%s,%s)
                    ON DUPLICATE KEY UPDATE
                        revoked_at=VALUES(revoked_at)
                    """,
                    grant_rows,
                )

        if relationships:
            relationship_rows = []
            for row in relationships:
                from_id = party_map.get(row["from_party_ext_id"])
                to_id = party_map.get(row["to_party_ext_id"])
                if not from_id or not to_id:
                    continue
                relationship_rows.append(
                    (
                        from_id,
                        to_id,
                        row.get("relationship_type") or "RELATED",
                        row.get("start_date") or None,
                        row.get("end_date") or None,
                    )
                )
            if relationship_rows:
                executemany_in_chunks(
                    cur,
                    """
                    INSERT INTO party_relationship(
                        from_party_id, to_party_id, relationship_type, start_date, end_date
                    )
                    VALUES (%s,%s,%s,%s,%s)
                    ON DUPLICATE KEY UPDATE
                        relationship_type=VALUES(relationship_type),
                        end_date=VALUES(end_date)
                    """,
                    relationship_rows,
                )

    return contract_map


def load_journal_entries(party_map: Dict[str, int]) -> Dict[str, int]:
    entries = read_rows("journal_entries.csv")
    if not entries:
        return {}

    with conn() as c:
        cur = c.cursor()
        rows: list[tuple] = []
        for entry in entries:
            counterparty_ext = entry.get("counterparty_party_ext_id") or ""
            counterparty_id = party_map.get(counterparty_ext) if counterparty_ext else None
            rows.append(
                (
                    entry["entry_code"],
                    entry["txn_date"],
                    entry["posted_at"],
                    entry.get("description") or None,
                    entry.get("channel_code") or None,
                    counterparty_id,
                    entry.get("transfer_reference") or None,
                    entry.get("external_reference") or None,
                )
            )

        if rows:
            executemany_in_chunks(
                cur,
                """
                INSERT INTO journal_entry(
                    entry_code, txn_date, posted_at, description, channel_code,
                    counterparty_party_id, transfer_reference, external_reference
                )
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                ON DUPLICATE KEY UPDATE
                    txn_date=VALUES(txn_date),
                    posted_at=VALUES(posted_at),
                    description=VALUES(description),
                    channel_code=VALUES(channel_code),
                    counterparty_party_id=VALUES(counterparty_party_id),
                    transfer_reference=VALUES(transfer_reference),
                    external_reference=VALUES(external_reference)
                """,
                rows,
            )

        cur.execute("SELECT entry_code, id FROM journal_entry")
        return {row["entry_code"]: row["id"] for row in cur.fetchall()}


def load_journal_lines(
    entry_map: Dict[str, int],
    acct_map: Dict[str, int],
    party_map: Dict[str, int],
) -> None:
    lines = read_rows("journal_lines.csv")
    if not lines:
        return

    with conn() as c:
        cur = c.cursor()
        cur.execute("SELECT id, name FROM section")
        section_map = {row["name"].lower(): row["id"] for row in cur.fetchall()}

        batch: list[tuple] = []
        for line in lines:
            entry_id = entry_map.get(line["entry_code"])
            if not entry_id:
                continue

            account_id = acct_map.get(line["account_ext_id"])
            if not account_id:
                continue

            party_ext = line.get("party_ext_id") or ""
            party_id = party_map.get(party_ext) if party_ext else None

            try:
                amount_value = Decimal(line["amount"])
            except Exception:
                continue

            currency_code = line["currency_code"]
            section_name = (line.get("section_name") or "transfer").lower()
            section_id = section_map.get(section_name)
            if section_id is None:
                section_id = section_map.get("transfer")

            category_name = line.get("category_name") or ""
            category_id = None
            if category_name and section_id:
                category_id = get_or_create_category(cur, section_id, category_name)

            line_memo = line.get("line_memo") or None

            batch.append(
                (
                    entry_id,
                    account_id,
                    party_id,
                    amount_value,
                    currency_code,
                    category_id,
                    line_memo,
                )
            )

        if batch:
            executemany_in_chunks(
                cur,
                """
                INSERT INTO journal_line(
                    entry_id, account_id, party_id, amount, currency_code, category_id, line_memo
                )
                VALUES (%s,%s,%s,%s,%s,%s,%s)
                """,
                batch,
            )

def load_users_orgs_accounts(party_map: Dict[str, int]):
    users = read_rows("users.csv")
    orgs  = read_rows("orgs.csv")
    accts = read_rows("accounts.csv")
    account_role_rows = read_rows("account_party_roles.csv")
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

        user_party_rows = [
            (uid, party_map[ext])
            for ext, uid in user_map.items()
            if ext in party_map
        ]
        if user_party_rows:
            executemany_in_chunks(
                cur,
                "REPLACE INTO user_party_map(user_id, party_id) VALUES (%s,%s)",
                user_party_rows,
            )

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

        org_party_rows = [
            (oid, party_map[ext])
            for ext, oid in org_map.items()
            if ext in party_map
        ]
        if org_party_rows:
            executemany_in_chunks(
                cur,
                "REPLACE INTO org_party_map(org_id, party_id) VALUES (%s,%s)",
                org_party_rows,
            )

        # accounts
        cur.execute("SELECT code FROM account_type")
        allowed_account_types = {row["code"] for row in cur.fetchall()}
        cur.execute("SELECT code FROM currency")
        allowed_currencies = {row["code"] for row in cur.fetchall()}
        cur.execute("SELECT id, party_id, account_type_code, currency_code, name FROM account")
        existing_accounts = {
            (row["party_id"], row["account_type_code"], row["currency_code"], row["name"] or None): row["id"]
            for row in cur.fetchall()
        }
        pending_accounts: list[tuple] = []
        pending_account_keys: list[tuple] = []
        pending_account_exts: Dict[tuple, list[str]] = {}

        for a in accts:
            party_ext = a["party_ext_id"]
            party_id = party_map.get(party_ext)
            if not party_id:
                raise ValueError(f"Account references unknown party_ext_id '{party_ext}'")

            account_type_code = a["account_type_code"]
            if account_type_code not in allowed_account_types:
                raise ValueError(
                    f"Unsupported account type '{account_type_code}' for account {a['ext_id']}; "
                    f"expected one of {sorted(allowed_account_types)}"
                )

            currency_code = a["currency_code"]
            if currency_code not in allowed_currencies:
                raise ValueError(
                    f"Unsupported currency '{currency_code}' for account {a['ext_id']}; "
                    f"expected one of {sorted(allowed_currencies)}"
                )

            name = a.get("name") or None
            key = (party_id, account_type_code, currency_code, name)
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
                    party_id,
                    account_type_code,
                    currency_code,
                    name,
                    a.get("iban") or None,
                    a.get("opened_at") or None,
                    a.get("closed_at") or None,
                )
            )
            pending_account_keys.append(key)

        if pending_accounts:
            inserted_accounts = insertmany_with_ids(
                cur,
                """
                INSERT INTO account (party_id, account_type_code, currency_code, name, iban, opened_at, closed_at)
                VALUES (%s,%s,%s,%s,%s,%s,%s)
                """,
                pending_accounts,
            )
            for key, new_id in zip(pending_account_keys, inserted_accounts):
                existing_accounts[key] = new_id
                for ext in pending_account_exts[key]:
                    acct_map[ext] = new_id

        if account_role_rows:
            pending_roles: list[tuple] = []
            for role_row in account_role_rows:
                account_id = acct_map.get(role_row["account_ext_id"])
                party_id = party_map.get(role_row["party_ext_id"])
                role_code = role_row.get("role_code")
                if not account_id or not party_id or not role_code:
                    continue
                start_date = role_row.get("start_date") or None
                end_date = role_row.get("end_date") or None
                is_primary = 1 if str(role_row.get("is_primary", "0")).lower() in {"1", "true", "yes"} else 0
                pending_roles.append((account_id, party_id, role_code, start_date, end_date, bool(is_primary)))

            if pending_roles:
                executemany_in_chunks(
                    cur,
                    """
                    INSERT INTO account_party_role(
                        account_id, party_id, role_code, start_date, end_date, is_primary
                    )
                    VALUES (%s,%s,%s,%s,%s,%s)
                    ON DUPLICATE KEY UPDATE
                        start_date=VALUES(start_date),
                        end_date=VALUES(end_date),
                        is_primary=VALUES(is_primary)
                    """,
                    pending_roles,
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
    identifiers = read_rows("instrument_identifiers.csv")
    prices = read_rows("price_quotes.csv")
    if not prices:
        logger.warning(
            "price_quotes.csv not found or empty; skip loading prices. "
            "Run scripts/fetch_stock_prices.py to generate fresh market data."
        )
    fx = read_rows("fx_rate_daily.csv")
    inst_map: Dict[str, int] = {}

    with conn() as c:
        cur = c.cursor()

        cur.execute("SELECT mic, id FROM market")
        market_map = {row["mic"]: row["id"] for row in cur.fetchall()}

        cur.execute("SELECT id, symbol, primary_market_id FROM instrument")
        existing_instruments = {
            (row["symbol"], row["primary_market_id"]): row["id"] for row in cur.fetchall()
        }

        pending_instruments: list[tuple] = []
        pending_inst_keys: list[tuple[str, Optional[int]]] = []
        pending_inst_exts: Dict[tuple, list[str]] = {}

        for ins in insts:
            market_mic = ins.get("primary_market_mic") or ""
            market_id = market_map.get(market_mic) if market_mic else None
            key = (ins["symbol"], market_id)
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
                    ins["name"],
                    ins["instrument_type_code"],
                    ins["primary_currency_code"],
                    market_id,
                )
            )
            pending_inst_keys.append(key)

        if pending_instruments:
            inserted_instruments = insertmany_with_ids(
                cur,
                """
                INSERT INTO instrument(symbol, name, instrument_type_code, primary_currency_code, primary_market_id)
                VALUES (%s,%s,%s,%s,%s)
                """,
                pending_instruments,
            )
            for key, new_id in zip(pending_inst_keys, inserted_instruments):
                existing_instruments[key] = new_id
                for ext in pending_inst_exts[key]:
                    inst_map[ext] = new_id

        for key, iid in existing_instruments.items():
            ext_list = pending_inst_exts.get(key, [])
            for ext in ext_list:
                inst_map.setdefault(ext, iid)

        if identifiers:
            identifier_rows: list[tuple] = []
            for ident in identifiers:
                inst_id = inst_map.get(ident["instrument_ext_id"])
                if not inst_id:
                    continue
                identifier_rows.append(
                    (
                        inst_id,
                        ident.get("identifier_type") or "GENERIC",
                        ident.get("identifier_value") or "",
                    )
                )
            if identifier_rows:
                executemany_in_chunks(
                    cur,
                    """
                    INSERT INTO instrument_identifier(instrument_id, identifier_type, identifier_value)
                    VALUES (%s,%s,%s)
                    ON DUPLICATE KEY UPDATE instrument_id=VALUES(instrument_id)
                    """,
                    identifier_rows,
                )

        quote_rows: list[tuple] = []
        for pr in prices:
            iid = inst_map.get(pr.get("instrument_ext_id"))
            if not iid:
                continue
            quote_type = (pr.get("quote_type") or "CLOSE").upper()
            quote_value = pr.get("quote_value") or pr.get("close_price")
            if not quote_value:
                continue
            quote_rows.append((iid, pr["price_date"], quote_type, quote_value))

        if quote_rows:
            executemany_in_chunks(
                cur,
                """
                INSERT INTO price_quote(instrument_id, price_date, quote_type, quote_value)
                VALUES (%s,%s,%s,%s)
                ON DUPLICATE KEY UPDATE quote_value=VALUES(quote_value)
                """,
                quote_rows,
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
                nonlocal rows_since_commit, processed_trades
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

def label_to_start(label: str) -> Optional[date]:
    try:
        year_str, month_str = label.split("-", 1)
        year = int(year_str)
        month = int(month_str)
        return date(year, month, 1)
    except Exception:
        logger.warning("Skipping invalid reporting period label '%s'", label)
        return None


def ensure_reporting_periods(extra_labels: Optional[Iterable[str]] = None) -> Dict[str, int]:
    extra_starts: set[date] = set()
    if extra_labels:
        for label in extra_labels:
            start = label_to_start(label)
            if start:
                extra_starts.add(start)

    with conn() as c:
        cur = c.cursor()

        cur.execute("SELECT DISTINCT txn_date FROM journal_entry")
        period_starts = {
            month_start(row["txn_date"])
            for row in cur.fetchall()
            if row.get("txn_date")
        }
        period_starts.update(extra_starts)

        if period_starts:
            period_rows = [
                (start, month_end(start), f"{start.year}-{start.month:02d}")
                for start in sorted(period_starts)
            ]
            executemany_in_chunks(
                cur,
                """
                INSERT INTO reporting_period(period_start, period_end, label)
                VALUES (%s,%s,%s)
                ON DUPLICATE KEY UPDATE
                  period_start=VALUES(period_start),
                  period_end=VALUES(period_end)
                """,
                period_rows,
            )

        cur.execute("SELECT id, label FROM reporting_period")
        return {row["label"]: row["id"] for row in cur.fetchall()}


def load_payroll_fact(contract_map: Dict[str, int]) -> int:
    payroll_rows = read_rows("payroll_fact.csv")
    if not payroll_rows:
        ensure_reporting_periods()
        return 0

    labels = [row["reporting_period_label"] for row in payroll_rows if row.get("reporting_period_label")]
    period_map = ensure_reporting_periods(labels)

    with conn() as c:
        cur = c.cursor()
        cur.execute("DELETE FROM payroll_fact")

        insert_rows: list[tuple] = []
        for row in payroll_rows:
            label = row.get("reporting_period_label")
            contract_ext = row.get("contract_ext_id")
            if not (label and contract_ext):
                continue

            period_id = period_map.get(label)
            contract_id = contract_map.get(contract_ext)
            if not (period_id and contract_id):
                continue

            insert_rows.append(
                (
                    period_id,
                    contract_id,
                    row.get("gross_amount") or 0,
                    row.get("net_amount") or 0,
                    row.get("taxes_withheld") or 0,
                )
            )

        if insert_rows:
            executemany_in_chunks(
                cur,
                """
                INSERT INTO payroll_fact(
                    reporting_period_id,
                    contract_id,
                    gross_amount,
                    net_amount,
                    taxes_withheld
                )
                VALUES (%s,%s,%s,%s,%s)
                ON DUPLICATE KEY UPDATE
                    gross_amount=VALUES(gross_amount),
                    net_amount=VALUES(net_amount),
                    taxes_withheld=VALUES(taxes_withheld)
                """,
                insert_rows,
            )

        return len(insert_rows)


def rebuild_cash_flow_fact() -> int:
    """Recompute cash flow facts from the journal tables."""
    with conn() as c:
        cur = c.cursor()
        cur.execute("DELETE FROM cash_flow_fact")
        cur.execute("SELECT id FROM section WHERE name = 'transfer'")
        transfer_row = cur.fetchone()
        transfer_section_id = transfer_row["id"] if transfer_row else None

        insert_sql = """
            INSERT INTO cash_flow_fact(
                reporting_period_id,
                party_id,
                section_id,
                inflow_amount,
                outflow_amount,
                net_amount
            )
            SELECT
                rp.id AS reporting_period_id,
                jl.party_id,
                CASE
                    WHEN jl.category_id IS NOT NULL THEN (
                        SELECT section_id FROM category WHERE id = jl.category_id
                    )
                    ELSE %(transfer_section_id)s
                END AS section_id,
                SUM(CASE WHEN jl.amount > 0 THEN jl.amount ELSE 0 END) AS inflow_amount,
                SUM(CASE WHEN jl.amount < 0 THEN -jl.amount ELSE 0 END) AS outflow_amount,
                SUM(jl.amount) AS net_amount
            FROM journal_line jl
            JOIN journal_entry je ON je.id = jl.entry_id
            JOIN reporting_period rp
              ON je.txn_date BETWEEN rp.period_start AND rp.period_end
            JOIN party p ON p.id = jl.party_id
            WHERE jl.party_id IS NOT NULL
              AND p.display_name <> 'Ledger Clearing'
              AND p.party_type IN ('INDIVIDUAL', 'COMPANY')
              AND (
                  jl.category_id IS NOT NULL
                  OR %(transfer_section_id)s IS NOT NULL
              )
            GROUP BY rp.id, jl.party_id, section_id
        """

        cur.execute(insert_sql, {"transfer_section_id": transfer_section_id})
        return cur.rowcount or 0


def rebuild_holding_performance_fact() -> int:
    """Recompute holding performance using the latest reporting period."""
    with conn() as c:
        cur = c.cursor()
        
        logger.info("Fetching latest reporting period...")
        cur.execute(
            "SELECT id, period_end FROM reporting_period ORDER BY period_end DESC LIMIT 1"
        )
        latest = cur.fetchone()
        if not latest:
            logger.warning("No reporting periods found, skipping holding performance rebuild")
            return 0

        period_id = latest["id"]
        period_end = latest["period_end"]
        logger.info("Using reporting period %s ending %s", period_id, period_end)

        logger.info("Clearing existing holding performance facts for period %s...", period_id)
        cur.execute(
            "DELETE FROM holding_performance_fact WHERE reporting_period_id = %s",
            (period_id,),
        )
        deleted = cur.rowcount or 0
        logger.info("Deleted %s old holding performance fact rows", deleted)

        logger.info("Computing holding performance facts...")
        try:
            # Use window function to get latest price - much faster than self-join
            cur.execute(
                """
                INSERT INTO holding_performance_fact(
                    reporting_period_id,
                    party_id,
                    instrument_id,
                    quantity,
                    cost_basis,
                    market_value,
                    unrealized_pl
                )
                SELECT
                    %s AS reporting_period_id,
                    a.party_id,
                    h.instrument_id,
                    SUM(h.qty)                                                   AS quantity,
                    SUM(h.qty * h.avg_cost)                                      AS cost_basis,
                    SUM(h.qty * COALESCE(price_data.quote_value, h.avg_cost))    AS market_value,
                    SUM(h.qty * (COALESCE(price_data.quote_value, h.avg_cost) - h.avg_cost)) AS unrealized_pl
                FROM holding h
                JOIN account a ON a.id = h.account_id
                LEFT JOIN (
                    SELECT 
                        instrument_id,
                        quote_value
                    FROM (
                        SELECT 
                            instrument_id,
                            quote_value,
                            ROW_NUMBER() OVER (
                                PARTITION BY instrument_id 
                                ORDER BY price_date DESC
                            ) AS rn
                        FROM price_quote
                        WHERE price_date <= %s
                          AND quote_type = 'CLOSE'
                    ) ranked
                    WHERE rn = 1
                ) AS price_data
                  ON price_data.instrument_id = h.instrument_id
                WHERE h.qty <> 0
                GROUP BY a.party_id, h.instrument_id
                """,
                (period_id, period_end),
            )
            inserted = cur.rowcount or 0
            logger.info("Inserted %s holding performance fact rows", inserted)
            return inserted
        except Exception as e:
            logger.exception("Failed to rebuild holding performance facts: %s", e)
            raise


def main():
    logger.info("Loading parties and profiles…")
    party_map = load_parties_and_profiles()
    logger.info("Loaded %s parties", len(party_map))
    logger.info("Loading app users and roles…")
    app_user_map = load_app_users_and_roles(party_map)
    logger.info("Loaded %s app users", len(app_user_map))
    logger.info("Loading employment contracts and company access…")
    contract_map = load_employment_and_access(party_map, app_user_map)
    logger.info("Loading users/orgs/accounts/memberships…")
    _user_map, _org_map, acct_map = load_users_orgs_accounts(party_map)
    logger.info("Loading instruments/prices/fx…")
    inst_map = load_instruments_prices_fx()
    logger.info("Loading journal entries…")
    entry_map = load_journal_entries(party_map)
    logger.info("Loaded %s journal entries", len(entry_map))
    logger.info("Loading journal lines…")
    load_journal_lines(entry_map, acct_map, party_map)
    logger.info("Ensuring reporting periods for journal activity…")
    ensure_reporting_periods()
    logger.info("Loading payroll facts…")
    payroll_rows = load_payroll_fact(contract_map)
    logger.info("Inserted %s payroll_fact rows", payroll_rows)
    logger.info("Loading trades & updating holdings (WAC)…")
    load_trades_and_holdings(acct_map, inst_map)
    logger.info("Rebuilding cash flow facts…")
    cash_rows = rebuild_cash_flow_fact()
    logger.info("Inserted %s cash_flow_fact rows", cash_rows)
    logger.info("Rebuilding holding performance facts…")
    holding_rows = rebuild_holding_performance_fact()
    logger.info("Inserted %s holding_performance_fact rows", holding_rows)
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
