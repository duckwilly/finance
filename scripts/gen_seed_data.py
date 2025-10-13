#!/usr/bin/env python3
from pathlib import Path
import csv
from datetime import datetime

SEED_DIR = Path("data/seed")
SEED_DIR.mkdir(parents=True, exist_ok=True)

def write_csv(path, headers, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=headers)
        w.writeheader()
        for r in rows:
            w.writerow(r)

def main():
    # 1) users.csv
    write_csv(
        SEED_DIR / "users.csv",
        ["ext_id", "name", "email"],
        [
            {"ext_id": "U-ALICE", "name": "Alice Janssen", "email": "alice@example.com"},
            {"ext_id": "U-BOB",   "name": "Bob de Vries",  "email": "bob@example.com"},
            {"ext_id": "U-ADMIN", "name": "Bank Admin",    "email": "admin@example.com"},
        ],
    )

    # 2) orgs.csv
    write_csv(
        SEED_DIR / "orgs.csv",
        ["ext_id", "name"],
        [
            {"ext_id": "C-ACME",  "name": "ACME B.V."},
            {"ext_id": "C-OMEGA", "name": "Omega Consulting B.V."},
        ],
    )

    # 3) accounts.csv
    write_csv(
        SEED_DIR / "accounts.csv",
        ["ext_id", "owner_type", "owner_ext_id", "type", "currency", "iban", "opened_at", "name"],
        [
            {"ext_id": "A-CHK-ALICE", "owner_type": "user", "owner_ext_id": "U-ALICE", "type": "checking",
             "currency": "EUR", "iban": "NL91ABNA0417164300", "opened_at": "2024-01-01T00:00:00", "name": "Alice Checking"},
            {"ext_id": "A-BRK-ALICE", "owner_type": "user", "owner_ext_id": "U-ALICE", "type": "brokerage",
             "currency": "EUR", "iban": "", "opened_at": "2024-01-01T00:00:00", "name": "Alice Brokerage"},
            {"ext_id": "A-EXP-BOB", "owner_type": "user", "owner_ext_id": "U-BOB", "type": "checking",
             "currency": "EUR", "iban": "NL12RABO0123456789", "opened_at": "2024-02-01T00:00:00", "name": "Bob Expense"},
            {"ext_id": "A-OP-ACME", "owner_type": "org", "owner_ext_id": "C-ACME", "type": "checking",
             "currency": "EUR", "iban": "NL44INGB0001234567", "opened_at": "2024-01-05T00:00:00", "name": "ACME Operating"},
        ],
    )

    # 4) account_memberships.csv
    write_csv(
        SEED_DIR / "account_memberships.csv",
        ["party_type", "party_ext_id", "account_ext_id", "role"],
        [
            {"party_type": "user", "party_ext_id": "U-ALICE", "account_ext_id": "A-CHK-ALICE", "role": "OWNER"},
            {"party_type": "user", "party_ext_id": "U-ALICE", "account_ext_id": "A-BRK-ALICE", "role": "OWNER"},
            {"party_type": "org",  "party_ext_id": "C-ACME",  "account_ext_id": "A-OP-ACME",   "role": "OWNER"},
            {"party_type": "org",  "party_ext_id": "C-ACME",  "account_ext_id": "A-EXP-BOB",   "role": "MANAGER"},
            {"party_type": "user", "party_ext_id": "U-BOB",   "account_ext_id": "A-EXP-BOB",   "role": "EMPLOYEE_CARDHOLDER"},
            {"party_type": "user", "party_ext_id": "U-ADMIN", "account_ext_id": "A-OP-ACME",   "role": "BANK_ADMIN"},
            {"party_type": "user", "party_ext_id": "U-ADMIN", "account_ext_id": "A-CHK-ALICE", "role": "BANK_ADMIN"},
            {"party_type": "user", "party_ext_id": "U-ADMIN", "account_ext_id": "A-BRK-ALICE", "role": "BANK_ADMIN"},
        ],
    )

    # 5) transactions.csv
    write_csv(
        SEED_DIR / "transactions.csv",
        [
            "ext_id","account_ext_id","posted_at","txn_date","amount","currency","direction",
            "section_name","category_name","channel","description",
            "counterparty_name","counterparty_account","counterparty_bic",
            "transfer_ref","ext_reference"
        ],
        [
            # Alice pays rent to ACME (external SEPA)
            {"ext_id":"T-1001","account_ext_id":"A-CHK-ALICE","posted_at":"2025-03-02T09:05:00","txn_date":"2025-03-02",
             "amount":"1250.00","currency":"EUR","direction":"DEBIT",
             "section_name":"expense","category_name":"Rent","channel":"SEPA","description":"Rent March",
             "counterparty_name":"ACME B.V.","counterparty_account":"NL44INGB0001234567","counterparty_bic":"INGBNL2A",
             "transfer_ref":"","ext_reference":""},
            {"ext_id":"T-1002","account_ext_id":"A-OP-ACME","posted_at":"2025-03-02T09:06:00","txn_date":"2025-03-02",
             "amount":"1250.00","currency":"EUR","direction":"CREDIT",
             "section_name":"income","category_name":"Customer payment","channel":"SEPA","description":"Rent from Alice",
             "counterparty_name":"Alice Janssen","counterparty_account":"NL91ABNA0417164300","counterparty_bic":"ABNANL2A",
             "transfer_ref":"","ext_reference":""},
            # Card expense (external merchant)
            {"ext_id":"T-1003","account_ext_id":"A-CHK-ALICE","posted_at":"2025-03-05T12:00:00","txn_date":"2025-03-05",
             "amount":"250.00","currency":"EUR","direction":"DEBIT",
             "section_name":"expense","category_name":"Food","channel":"CARD","description":"Expense card lunch",
             "counterparty_name":"RESTAURANT XYZ","counterparty_account":"","counterparty_bic":"",
             "transfer_ref":"","ext_reference":""},
            # On-us internal transfer (link by transfer_ref)
            {"ext_id":"T-2001","account_ext_id":"A-CHK-ALICE","posted_at":"2025-03-10T10:00:00","txn_date":"2025-03-10",
             "amount":"500.00","currency":"EUR","direction":"DEBIT",
             "section_name":"transfer","category_name":"Internal transfer","channel":"INTERNAL","description":"To ACME invoice",
             "counterparty_name":"","counterparty_account":"","counterparty_bic":"",
             "transfer_ref":"XFER-777","ext_reference":""},
            {"ext_id":"T-2002","account_ext_id":"A-OP-ACME","posted_at":"2025-03-10T10:00:01","txn_date":"2025-03-10",
             "amount":"500.00","currency":"EUR","direction":"CREDIT",
             "section_name":"transfer","category_name":"Internal transfer","channel":"INTERNAL","description":"From Alice invoice",
             "counterparty_name":"","counterparty_account":"","counterparty_bic":"",
             "transfer_ref":"XFER-777","ext_reference":""},
        ],
    )

    # 6) instruments.csv
    write_csv(
        SEED_DIR / "instruments.csv",
        ["ext_id","symbol","name","type","currency","isin","mic"],
        [
            {"ext_id":"I-AAPL","symbol":"AAPL","name":"Apple Inc.","type":"EQUITY","currency":"USD","isin":"US0378331005","mic":"XNAS"},
            {"ext_id":"I-VWRL","symbol":"VWRL","name":"Vanguard FTSE All-World UCITS","type":"ETF","currency":"USD","isin":"IE00B3RBWM25","mic":"XLON"},
        ],
    )

    # 7) trades.csv
    write_csv(
        SEED_DIR / "trades.csv",
        ["ext_id","account_ext_id","instrument_ext_id","side","qty","price","fees","tax","trade_time","settle_dt","currency"],
        [
            {"ext_id":"TR-1","account_ext_id":"A-BRK-ALICE","instrument_ext_id":"I-AAPL","side":"BUY","qty":"10","price":"180.50","fees":"1.00","tax":"0.00","trade_time":"2025-03-15T14:05:00","settle_dt":"2025-03-17","currency":"USD"},
            {"ext_id":"TR-2","account_ext_id":"A-BRK-ALICE","instrument_ext_id":"I-AAPL","side":"BUY","qty":"5","price":"190.00","fees":"1.00","tax":"0.00","trade_time":"2025-04-02T10:30:00","settle_dt":"2025-04-04","currency":"USD"},
            {"ext_id":"TR-3","account_ext_id":"A-BRK-ALICE","instrument_ext_id":"I-AAPL","side":"SELL","qty":"8","price":"210.00","fees":"1.00","tax":"0.00","trade_time":"2025-05-10T16:45:00","settle_dt":"2025-05-12","currency":"USD"},
        ],
    )

    # Optional: prices
    write_csv(
        SEED_DIR / "price_daily.csv",
        ["instrument_ext_id","price_date","close_price","currency"],
        [
            {"instrument_ext_id":"I-AAPL","price_date":"2025-05-10","close_price":"210.00","currency":"USD"},
            {"instrument_ext_id":"I-AAPL","price_date":"2025-05-09","close_price":"208.50","currency":"USD"},
            {"instrument_ext_id":"I-VWRL","price_date":"2025-05-10","close_price":"115.20","currency":"USD"},
        ],
    )

    # Optional: FX
    write_csv(
        SEED_DIR / "fx_rate_daily.csv",
        ["base","quote","rate_date","rate"],
        [
            {"base":"USD","quote":"EUR","rate_date":"2025-05-10","rate":"0.92"},
            {"base":"USD","quote":"EUR","rate_date":"2025-05-09","rate":"0.91"},
        ],
    )

    print(f"Seed CSVs written to {SEED_DIR.resolve()}")

if __name__ == "__main__":
    main()