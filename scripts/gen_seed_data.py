#!/usr/bin/env python3
"""Generate rich synthetic data for the finance dashboard."""
from __future__ import annotations

import argparse
import csv
import itertools
import os
import random
import time
import unicodedata
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Iterable, Iterator, Optional, Sequence
import sys

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

SEED_DIR = Path("data/seed")
STREAM_DIR = Path("data/stream")

from app.log import get_logger, init_logging, log_context, progress_manager, timeit
from scripts.name_data import random_company_name, random_person_name

logger = get_logger(__name__)
INDUSTRIES = [
    "Technology",
    "Retail",
    "Manufacturing",
    "Hospitality",
    "Healthcare",
    "Logistics",
    "Energy",
    "Finance",
    "Media",
    "Education",
]
PROPERTY_MANAGERS = [
    "Canal Properties",
    "Skyline Rentals",
    "Harbor Estates",
    "CityLiving BV",
]
CARD_MERCHANTS = [
    ("Albert Heijn", "Groceries"),
    ("Jumbo Supermarkt", "Groceries"),
    ("Bol.com", "Shopping"),
    ("Coolblue", "Electronics"),
    ("NS International", "Transport"),
    ("Uber BV", "Transport"),
    ("Thuisbezorgd", "Dining"),
    ("Starbucks", "Dining"),
    ("Spotify", "Subscriptions"),
    ("Netflix", "Subscriptions"),
    ("Basic-Fit", "Fitness"),
    ("Ziggo", "Utilities"),
    ("Waternet", "Utilities"),
    ("HEMA", "Shopping"),
    ("KLM", "Travel"),
    ("Booking.com", "Travel"),
    ("IKEA", "Home"),
    ("Blokker", "Home"),
    ("Etos", "Healthcare"),
    ("Gall & Gall", "Dining"),
]
BUSINESS_VENDORS = [
    "CloudServe NL",
    "Green Energy Co",
    "OfficePlus",
    "TalentSource",
    "MarketingHive",
    "SupplyChain One",
    "EventMakers",
    "FleetMotion",
    "Insight Analytics",
    "Canal Works",
]
CUSTOMERS = [
    "Riverside Hotels",
    "City Council",
    "Orion Retail",
    "Bright Schools",
    "Lumen Health",
    "Vertex Labs",
    "Horizon Logistics",
    "Zenith Media",
    "Orbit Foods",
    "Nimbus Software",
]

INSTRUMENTS = [
    {"ext_id": "I-AAPL", "symbol": "AAPL", "name": "Apple Inc.", "type": "EQUITY", "currency": "USD", "isin": "US0378331005", "mic": "XNAS"},
    {"ext_id": "I-MSFT", "symbol": "MSFT", "name": "Microsoft Corporation", "type": "EQUITY", "currency": "USD", "isin": "US5949181045", "mic": "XNAS"},
    {"ext_id": "I-NVDA", "symbol": "NVDA", "name": "NVIDIA Corporation", "type": "EQUITY", "currency": "USD", "isin": "US67066G1040", "mic": "XNAS"},
    {"ext_id": "I-TSLA", "symbol": "TSLA", "name": "Tesla Inc.", "type": "EQUITY", "currency": "USD", "isin": "US88160R1014", "mic": "XNAS"},
    {"ext_id": "I-VWRL", "symbol": "VWRL", "name": "Vanguard FTSE All-World UCITS", "type": "ETF", "currency": "USD", "isin": "IE00B3RBWM25", "mic": "XLON"},
    {"ext_id": "I-ASM", "symbol": "ASML", "name": "ASML Holding N.V.", "type": "EQUITY", "currency": "EUR", "isin": "NL0010273215", "mic": "XAMS"},
]

# Price and FX data are now fetched from real sources via fetch_stock_prices.py


@dataclass
class Company:
    ext_id: str
    name: str
    industry: str
    account_ext_id: str
    iban: str


@dataclass
class Individual:
    ext_id: str
    name: str
    email: str
    employer: Company
    salary: float
    rent: float
    checking_account: str
    savings_account: str
    landlord: str


class IdFactory:
    def __init__(self, prefix: str, start: int = 1):
        self.prefix = prefix
        self.counter = start

    def next(self) -> str:
        value = f"{self.prefix}{self.counter:07d}"
        self.counter += 1
        return value


TXN_HEADERS = [
    "ext_id",
    "account_ext_id",
    "posted_at",
    "txn_date",
    "amount",
    "currency",
    "direction",
    "section_name",
    "category_name",
    "channel",
    "description",
    "counterparty_name",
    "counterparty_account",
    "counterparty_bic",
    "transfer_ref",
    "ext_reference",
]


def parse_args() -> argparse.Namespace:
    # Calculate default start and months based on current year
    current_year = date.today().year
    current_month = date.today().month
    default_start = f"{current_year}-01"
    default_months = current_month
    
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--individuals", type=int, default=500, help="Number of individual users to create")
    parser.add_argument("--companies", type=int, default=50, help="Number of corporate organisations")
    parser.add_argument("--months", type=int, default=default_months, help="How many months of history to generate")
    parser.add_argument("--start", type=str, default=default_start, help="Start month (YYYY-MM)")
    parser.add_argument("--seed", type=int, default=42, help="Random seed for reproducibility")
    parser.add_argument("--currency", type=str, default="EUR", help="Base currency for cash accounts")
    parser.add_argument(
        "--continuous",
        action="store_true",
        help="Keep generating live transactions after the historical snapshot",
    )
    parser.add_argument(
        "--live-interval",
        type=float,
        default=2.0,
        help="Seconds to sleep between live batches when --continuous is used",
    )
    parser.add_argument(
        "--live-batch-size",
        type=int,
        default=250,
        help="Approximate number of transactions per live batch",
    )
    return parser.parse_args()


def month_sequence(start: date, months: int) -> Iterator[date]:
    year = start.year
    month = start.month
    for _ in range(months):
        yield date(year, month, 1)
        month += 1
        if month > 12:
            year += 1
            month = 1


def month_end(day: date) -> date:
    if day.month == 12:
        return date(day.year, 12, 31)
    next_month = date(day.year, day.month + 1, 1)
    return next_month - timedelta(days=1)


def random_name() -> tuple[str, str]:
    """Return a first/last combination using the shared name pools."""
    return random_person_name()


def sanitize_email(name: str, idx: int) -> str:
    normalized = unicodedata.normalize("NFKD", name)
    ascii_name = "".join(ch for ch in normalized if not unicodedata.combining(ch))
    slug = ascii_name.lower().replace(" ", ".").replace("'", "").replace("-", ".")
    return f"{slug}{idx:04d}@example.com"


def build_companies(n: int) -> list[Company]:
    companies: list[Company] = []
    seen_names: set[str] = set()
    for i in range(1, n + 1):
        name = random_company_name()
        while name in seen_names:
            name = random_company_name()
        seen_names.add(name)
        industry = random.choice(INDUSTRIES)
        ext_id = f"C-{i:05d}"
        account_ext_id = f"A-C{i:05d}-OP"
        iban = f"NL{i % 90 + 10:02d}BANK{1000000000 + i:010d}"
        companies.append(Company(ext_id, name, industry, account_ext_id, iban))
    return companies


def build_individuals(n: int, companies: Sequence[Company]) -> list[Individual]:
    individuals: list[Individual] = []
    for i in range(1, n + 1):
        first, last = random_name()
        name = f"{first} {last}"
        email = sanitize_email(name, i)
        employer = random.choice(companies)
        salary = round(random.uniform(2800, 9200), 2)
        rent = round(salary * random.uniform(0.24, 0.32), 2)
        checking_account = f"A-U{i:05d}-CHK"
        savings_account = f"A-U{i:05d}-SAV"
        landlord = random.choice(PROPERTY_MANAGERS)
        individuals.append(
            Individual(
                ext_id=f"U-{i:05d}",
                name=name,
                email=email,
                employer=employer,
                salary=salary,
                rent=rent,
                checking_account=checking_account,
                savings_account=savings_account,
                landlord=landlord,
            )
        )
    return individuals


def write_csv(path: Path, headers: Sequence[str], rows: Iterable[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def iso_datetime(day: date, hour: int, minute: int) -> str:
    return datetime(day.year, day.month, day.day, hour, minute).isoformat()


def fmt_amount(value: float) -> str:
    return f"{value:.2f}"


def individual_transactions(
    individuals: Sequence[Individual],
    txn_ids: IdFactory,
    start_month: date,
    months: int,
    currency: str,
    stats: dict[str, int],
) -> Iterator[dict[str, object]]:
    transfer_ids = IdFactory("XFER-")
    for month_start in month_sequence(start_month, months):
        month_finish = month_end(month_start)
        for person in individuals:
            payday = month_start + timedelta(days=random.randint(24, 27))
            posted_at = iso_datetime(payday, random.randint(8, 11), random.randint(0, 59))
            amount = person.salary
            stats["individual"] += 1
            yield {
                "ext_id": txn_ids.next(),
                "account_ext_id": person.checking_account,
                "posted_at": posted_at,
                "txn_date": payday.isoformat(),
                "amount": fmt_amount(amount),
                "currency": currency,
                "direction": "CREDIT",
                "section_name": "income",
                "category_name": "Salary",
                "channel": "SEPA",
                "description": f"Salary from {person.employer.name}",
                "counterparty_name": person.employer.name,
                "counterparty_account": person.employer.iban,
                "counterparty_bic": "",
                "transfer_ref": "",
                "ext_reference": "",
            }
            stats["company"] += 1
            yield {
                "ext_id": txn_ids.next(),
                "account_ext_id": person.employer.account_ext_id,
                "posted_at": posted_at,
                "txn_date": payday.isoformat(),
                "amount": fmt_amount(amount),
                "currency": currency,
                "direction": "DEBIT",
                "section_name": "expense",
                "category_name": "Payroll",
                "channel": "SEPA",
                "description": f"Payroll to {person.name}",
                "counterparty_name": person.name,
                "counterparty_account": "",
                "counterparty_bic": "",
                "transfer_ref": "",
                "ext_reference": "",
            }

            rent_day = month_start + timedelta(days=random.randint(0, 4))
            rent_posted = iso_datetime(rent_day, random.randint(6, 9), random.randint(0, 59))
            stats["individual"] += 1
            yield {
                "ext_id": txn_ids.next(),
                "account_ext_id": person.checking_account,
                "posted_at": rent_posted,
                "txn_date": rent_day.isoformat(),
                "amount": fmt_amount(person.rent),
                "currency": currency,
                "direction": "DEBIT",
                "section_name": "expense",
                "category_name": "Rent",
                "channel": "SEPA",
                "description": f"Rent {month_start.strftime('%B %Y')}",
                "counterparty_name": person.landlord,
                "counterparty_account": "",
                "counterparty_bic": "",
                "transfer_ref": "",
                "ext_reference": "",
            }

            for utility, description in [("Utilities", "Energy bill"), ("Utilities", "Fiber internet")]:
                util_day = month_start + timedelta(days=random.randint(10, 20))
                util_posted = iso_datetime(util_day, random.randint(8, 11), random.randint(0, 59))
                amount = round(random.uniform(70, 160), 2)
                stats["individual"] += 1
                yield {
                    "ext_id": txn_ids.next(),
                    "account_ext_id": person.checking_account,
                    "posted_at": util_posted,
                    "txn_date": util_day.isoformat(),
                    "amount": fmt_amount(amount),
                    "currency": currency,
                    "direction": "DEBIT",
                    "section_name": "expense",
                    "category_name": utility,
                    "channel": "SEPA",
                    "description": description,
                    "counterparty_name": random.choice(PROPERTY_MANAGERS),
                    "counterparty_account": "",
                    "counterparty_bic": "",
                    "transfer_ref": "",
                    "ext_reference": "",
                }

            transfer_amount = round(person.salary * random.uniform(0.08, 0.18), 2)
            transfer_day = month_start + timedelta(days=random.randint(3, 6))
            transfer_posted = iso_datetime(transfer_day, random.randint(7, 9), random.randint(0, 59))
            transfer_ref = transfer_ids.next()
            stats["individual"] += 1
            yield {
                "ext_id": txn_ids.next(),
                "account_ext_id": person.checking_account,
                "posted_at": transfer_posted,
                "txn_date": transfer_day.isoformat(),
                "amount": fmt_amount(transfer_amount),
                "currency": currency,
                "direction": "DEBIT",
                "section_name": "transfer",
                "category_name": "Savings",
                "channel": "INTERNAL",
                "description": "Monthly savings transfer",
                "counterparty_name": person.name,
                "counterparty_account": person.savings_account,
                "counterparty_bic": "",
                "transfer_ref": transfer_ref,
                "ext_reference": "",
            }
            stats["individual"] += 1
            yield {
                "ext_id": txn_ids.next(),
                "account_ext_id": person.savings_account,
                "posted_at": transfer_posted,
                "txn_date": transfer_day.isoformat(),
                "amount": fmt_amount(transfer_amount),
                "currency": currency,
                "direction": "CREDIT",
                "section_name": "transfer",
                "category_name": "Savings",
                "channel": "INTERNAL",
                "description": "Monthly savings transfer",
                "counterparty_name": person.name,
                "counterparty_account": person.checking_account,
                "counterparty_bic": "",
                "transfer_ref": transfer_ref,
                "ext_reference": "",
            }

            purchases = random.randint(6, 10)
            for _ in range(purchases):
                merchant, category = random.choice(CARD_MERCHANTS)
                spend_day = month_start + timedelta(days=random.randint(0, month_finish.day - 1))
                spend_time = iso_datetime(spend_day, random.randint(7, 22), random.randint(0, 59))
                amount = round(random.uniform(8, 220), 2)
                stats["individual"] += 1
                yield {
                    "ext_id": txn_ids.next(),
                    "account_ext_id": person.checking_account,
                    "posted_at": spend_time,
                    "txn_date": spend_day.isoformat(),
                    "amount": fmt_amount(amount),
                    "currency": currency,
                    "direction": "DEBIT",
                    "section_name": "expense",
                    "category_name": category,
                    "channel": "CARD",
                    "description": f"{category} - {merchant}",
                    "counterparty_name": merchant,
                    "counterparty_account": "",
                    "counterparty_bic": "",
                    "transfer_ref": "",
                    "ext_reference": "",
                }


def company_transactions(
    companies: Sequence[Company],
    txn_ids: IdFactory,
    start_month: date,
    months: int,
    currency: str,
    stats: dict[str, int],
) -> Iterator[dict[str, object]]:
    for month_start in month_sequence(start_month, months):
        month_finish = month_end(month_start)
        for company in companies:
            rent_day = month_start + timedelta(days=random.randint(1, 5))
            rent_amount = round(random.uniform(4500, 11000), 2)
            stats["company"] += 1
            yield {
                "ext_id": txn_ids.next(),
                "account_ext_id": company.account_ext_id,
                "posted_at": iso_datetime(rent_day, 9, 12),
                "txn_date": rent_day.isoformat(),
                "amount": fmt_amount(rent_amount),
                "currency": currency,
                "direction": "DEBIT",
                "section_name": "expense",
                "category_name": "Rent",
                "channel": "SEPA",
                "description": "Office lease",
                "counterparty_name": random.choice(PROPERTY_MANAGERS),
                "counterparty_account": "",
                "counterparty_bic": "",
                "transfer_ref": "",
                "ext_reference": "",
            }

            for vendor in random.sample(BUSINESS_VENDORS, k=3):
                service_day = month_start + timedelta(days=random.randint(8, 20))
                amount = round(random.uniform(600, 4200), 2)
                stats["company"] += 1
                yield {
                    "ext_id": txn_ids.next(),
                    "account_ext_id": company.account_ext_id,
                    "posted_at": iso_datetime(service_day, random.randint(9, 14), random.randint(0, 59)),
                    "txn_date": service_day.isoformat(),
                    "amount": fmt_amount(amount),
                    "currency": currency,
                    "direction": "DEBIT",
                    "section_name": "expense",
                    "category_name": "Vendors",
                    "channel": "SEPA",
                    "description": f"Payment to {vendor}",
                    "counterparty_name": vendor,
                    "counterparty_account": "",
                    "counterparty_bic": "",
                    "transfer_ref": "",
                    "ext_reference": "",
                }

            for _ in range(random.randint(12, 24)):
                client = random.choice(CUSTOMERS)
                invoice_day = month_start + timedelta(days=random.randint(1, month_finish.day - 1))
                amount = round(random.uniform(8500, 97500), 2)
                stats["company"] += 1
                yield {
                    "ext_id": txn_ids.next(),
                    "account_ext_id": company.account_ext_id,
                    "posted_at": iso_datetime(invoice_day, random.randint(8, 17), random.randint(0, 59)),
                    "txn_date": invoice_day.isoformat(),
                    "amount": fmt_amount(amount),
                    "currency": currency,
                    "direction": "CREDIT",
                    "section_name": "income",
                    "category_name": "Customer payment",
                    "channel": "SEPA",
                    "description": f"Invoice settlement from {client}",
                    "counterparty_name": client,
                    "counterparty_account": "",
                    "counterparty_bic": "",
                    "transfer_ref": "",
                    "ext_reference": "",
                }

            if (month_start.month - 1) % 3 == 0:
                tax_day = month_start + timedelta(days=random.randint(20, 27))
                tax_amount = round(random.uniform(18000, 42000), 2)
                stats["company"] += 1
                yield {
                    "ext_id": txn_ids.next(),
                    "account_ext_id": company.account_ext_id,
                    "posted_at": iso_datetime(tax_day, 11, random.randint(0, 59)),
                    "txn_date": tax_day.isoformat(),
                    "amount": fmt_amount(tax_amount),
                    "currency": currency,
                    "direction": "DEBIT",
                    "section_name": "expense",
                    "category_name": "Taxes",
                    "channel": "SEPA",
                    "description": "Quarterly VAT remittance",
                    "counterparty_name": "Belastingdienst",
                    "counterparty_account": "",
                    "counterparty_bic": "",
                    "transfer_ref": "",
                    "ext_reference": "",
                }


def generate_historical_positions(individuals: Sequence[Individual], start_month: date) -> list[dict[str, object]]:
    """Generate historical positions that exist before the simulation period."""
    historical_trades = []
    trade_id = IdFactory("TR-HIST-", start=1)
    
    for person in individuals:
        # Generate 1-5 historical trades 6-18 months before simulation start
        num_historical = random.randint(1, 5)
        historical_start = start_month - timedelta(days=random.randint(180, 540))  # 6-18 months before
        
        holdings: dict[str, float] = {}
        for _ in range(num_historical):
            instrument = random.choice(INSTRUMENTS)
            qty = round(random.uniform(1, 15), 2)
            # Use realistic price ranges based on instrument
            if instrument["ext_id"] == "I-AAPL":
                price = round(random.uniform(120, 200), 2)
            elif instrument["ext_id"] == "I-MSFT":
                price = round(random.uniform(300, 450), 2)
            elif instrument["ext_id"] == "I-NVDA":
                price = round(random.uniform(200, 600), 2)
            elif instrument["ext_id"] == "I-TSLA":
                price = round(random.uniform(150, 300), 2)
            elif instrument["ext_id"] == "I-VWRL":
                price = round(random.uniform(80, 130), 2)
            elif instrument["ext_id"] == "I-ASM":
                price = round(random.uniform(400, 800), 2)
            else:
                price = round(random.uniform(50, 300), 2)
            
            # Historical trades are always BUY
            trade_time = historical_start + timedelta(days=random.randint(0, 90))
            trade_time = datetime.combine(trade_time, datetime.min.time().replace(hour=random.randint(9, 16)))
            
            historical_trades.append({
                "ext_id": trade_id.next(),
                "account_ext_id": person.checking_account.replace("-CHK", "-BRK"),
                "instrument_ext_id": instrument["ext_id"],
                "side": "BUY",
                "qty": f"{qty:.2f}",
                "price": f"{price:.2f}",
                "fees": f"{random.uniform(0.5, 4.5):.2f}",
                "tax": f"{random.uniform(0, 2.0):.2f}",
                "trade_time": trade_time.isoformat(),
                "settle_dt": (trade_time.date() + timedelta(days=2)).isoformat(),
                "currency": instrument["currency"],
            })
            holdings[instrument["ext_id"]] = holdings.get(instrument["ext_id"], 0) + qty
    
    return historical_trades


def generate_trades(individuals: Sequence[Individual], start_month: date, months: int) -> list[dict[str, object]]:
    """Generate trades for the simulation period with enhanced portfolio distribution."""
    if not individuals:
        return []
    
    # Select ~70% of users for portfolios
    portfolio_size = int(len(individuals) * 0.7)
    random_investors = random.sample(individuals, k=portfolio_size)
    
    trade_rows: list[dict[str, object]] = []
    trade_id = IdFactory("TR-", start=1)
    
    # Generate historical positions first
    historical_trades = generate_historical_positions(random_investors, start_month)
    trade_rows.extend(historical_trades)
    
    # Assign portfolio tiers
    small_investors = random_investors[:int(len(random_investors) * 0.4)]  # 40%
    medium_investors = random_investors[int(len(random_investors) * 0.4):int(len(random_investors) * 0.8)]  # 40%
    large_investors = random_investors[int(len(random_investors) * 0.8):]  # 20%
    
    # Generate trades for each tier
    for tier_name, investors, min_trades, max_trades, min_stocks, max_stocks in [
        ("small", small_investors, 2, 5, 1, 2),
        ("medium", medium_investors, 6, 12, 2, 4),
        ("large", large_investors, 13, 25, 3, 6),
    ]:
        for person in investors:
            num_trades = random.randint(min_trades, max_trades)
            num_stocks = random.randint(min_stocks, max_stocks)
            selected_stocks = random.sample(INSTRUMENTS, k=num_stocks)
            
            holdings: dict[str, float] = {}
            
            # Generate trades throughout the simulation period
            for _ in range(num_trades):
                instrument = random.choice(selected_stocks)
                qty = round(random.uniform(1, 25), 2)
                
                # Use realistic price ranges
                if instrument["ext_id"] == "I-AAPL":
                    price = round(random.uniform(120, 200), 2)
                elif instrument["ext_id"] == "I-MSFT":
                    price = round(random.uniform(300, 450), 2)
                elif instrument["ext_id"] == "I-NVDA":
                    price = round(random.uniform(200, 600), 2)
                elif instrument["ext_id"] == "I-TSLA":
                    price = round(random.uniform(150, 300), 2)
                elif instrument["ext_id"] == "I-VWRL":
                    price = round(random.uniform(80, 130), 2)
                elif instrument["ext_id"] == "I-ASM":
                    price = round(random.uniform(400, 800), 2)
                else:
                    price = round(random.uniform(50, 300), 2)
                
                # Determine if this should be a BUY or SELL
                current_holding = holdings.get(instrument["ext_id"], 0)
                if current_holding > qty * 2:  # Can sell if we have enough
                    side_options = ["BUY", "SELL"]
                else:
                    side_options = ["BUY"]
                
                side = random.choice(side_options)
                if side == "SELL":
                    qty = min(qty, current_holding)
                
                # Generate trade time within simulation period
                # More trades at month boundaries (rebalancing behavior)
                if random.random() < 0.3:  # 30% chance of month-end trade
                    trade_day = start_month + timedelta(days=random.randint(0, months * 30))
                    # Move to end of month
                    if trade_day.month == 12:
                        trade_day = date(trade_day.year, 12, 31)
                    else:
                        next_month = date(trade_day.year, trade_day.month + 1, 1)
                        trade_day = next_month - timedelta(days=1)
                else:
                    trade_day = start_month + timedelta(days=random.randint(0, months * 30))
                
                trade_time = datetime.combine(trade_day, datetime.min.time().replace(
                    hour=random.randint(9, 16), 
                    minute=random.randint(0, 59)
                ))
                
                trade_rows.append({
                    "ext_id": trade_id.next(),
                    "account_ext_id": person.checking_account.replace("-CHK", "-BRK"),
                    "instrument_ext_id": instrument["ext_id"],
                    "side": side,
                    "qty": f"{qty:.2f}",
                    "price": f"{price:.2f}",
                    "fees": f"{random.uniform(0.5, 4.5):.2f}",
                    "tax": f"{random.uniform(0, 2.0):.2f}",
                    "trade_time": trade_time.isoformat(),
                    "settle_dt": (trade_time.date() + timedelta(days=2)).isoformat(),
                    "currency": instrument["currency"],
                })
                holdings[instrument["ext_id"]] = holdings.get(instrument["ext_id"], 0) + (qty if side == "BUY" else -qty)
    
    return trade_rows


# FX data is now handled by fetch_stock_prices.py


def write_core_tables(individuals: list[Individual], companies: list[Company], currency: str) -> None:
    users_rows = [
        {"ext_id": "U-ADMIN", "name": "Bank Admin", "email": "admin@simulatedbank.dev"},
    ] + [
        {"ext_id": person.ext_id, "name": person.name, "email": person.email}
        for person in individuals
    ]
    org_rows = [
        {"ext_id": company.ext_id, "name": company.name, "industry": company.industry}
        for company in companies
    ]
    account_rows = [
        {
            "ext_id": company.account_ext_id,
            "owner_type": "org",
            "owner_ext_id": company.ext_id,
            "type": "operating",
            "currency": currency,
            "iban": company.iban,
            "opened_at": "2022-01-01T00:00:00",
            "name": f"{company.name} Operating",
        }
        for company in companies
    ]
    for person in individuals:
        identifier = int(person.ext_id.split("-")[1])
        account_rows.extend(
            [
                {
                    "ext_id": person.checking_account,
                    "owner_type": "user",
                    "owner_ext_id": person.ext_id,
                    "type": "checking",
                    "currency": currency,
                    "iban": f"NL91SIMU{100000000 + identifier:010d}",
                    "opened_at": "2023-01-01T00:00:00",
                    "name": f"{person.name.split()[0]} Checking",
                },
                {
                    "ext_id": person.savings_account,
                    "owner_type": "user",
                    "owner_ext_id": person.ext_id,
                    "type": "savings",
                    "currency": currency,
                    "iban": "",
                    "opened_at": "2023-01-01T00:00:00",
                    "name": f"{person.name.split()[0]} Savings",
                },
                {
                    "ext_id": person.checking_account.replace("-CHK", "-BRK"),
                    "owner_type": "user",
                    "owner_ext_id": person.ext_id,
                    "type": "brokerage",
                    "currency": "USD",
                    "iban": "",
                    "opened_at": "2023-01-01T00:00:00",
                    "name": f"{person.name.split()[0]} Brokerage",
                },
            ]
        )

    membership_rows = []
    for company in companies:
        membership_rows.extend(
            [
                {"party_type": "org", "party_ext_id": company.ext_id, "account_ext_id": company.account_ext_id, "role": "OWNER"},
                {"party_type": "user", "party_ext_id": "U-ADMIN", "account_ext_id": company.account_ext_id, "role": "BANK_ADMIN"},
            ]
        )
    for person in individuals:
        membership_rows.extend(
            [
                {"party_type": "user", "party_ext_id": person.ext_id, "account_ext_id": person.checking_account, "role": "OWNER"},
                {"party_type": "user", "party_ext_id": person.ext_id, "account_ext_id": person.savings_account, "role": "OWNER"},
                {"party_type": "user", "party_ext_id": person.ext_id, "account_ext_id": person.checking_account.replace("-CHK", "-BRK"), "role": "OWNER"},
            ]
        )

    write_csv(
        SEED_DIR / "users.csv",
        ["ext_id", "name", "email"],
        users_rows,
    )
    write_csv(
        SEED_DIR / "orgs.csv",
        ["ext_id", "name", "industry"],
        org_rows,
    )
    write_csv(
        SEED_DIR / "accounts.csv",
        ["ext_id", "owner_type", "owner_ext_id", "type", "currency", "iban", "opened_at", "name"],
        account_rows,
    )
    write_csv(
        SEED_DIR / "account_memberships.csv",
        ["party_type", "party_ext_id", "account_ext_id", "role"],
        membership_rows,
    )


def write_transactions(rows: Iterator[dict[str, object]]) -> int:
    count = 0
    path = SEED_DIR / "transactions.csv"
    path.parent.mkdir(parents=True, exist_ok=True)
    logger.info("Writing transactions to %s", path)
    with timeit("transaction csv write", logger=logger, unit="rows") as timer, progress_manager.task(
        "Generating transactions", unit="rows"
    ) as task:
        with path.open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=TXN_HEADERS)
            writer.writeheader()
            for row in rows:
                writer.writerow(row)
                count += 1
                timer.add()
                task.advance()
    logger.info("Generated %s transactions", count)
    return count


def write_trades(trades: Sequence[dict[str, object]]) -> None:
    write_csv(
        SEED_DIR / "instruments.csv",
        ["ext_id", "symbol", "name", "type", "currency", "isin", "mic"],
        INSTRUMENTS,
    )
    write_csv(
        SEED_DIR / "trades.csv",
        ["ext_id", "account_ext_id", "instrument_ext_id", "side", "qty", "price", "fees", "tax", "trade_time", "settle_dt", "currency"],
        trades,
    )
    # Price and FX data are now handled by fetch_stock_prices.py


def live_stream(
    individuals: Sequence[Individual],
    txn_ids: IdFactory,
    currency: str,
    batch_size: int,
    interval: float,
) -> None:
    STREAM_DIR.mkdir(parents=True, exist_ok=True)
    path = STREAM_DIR / "transactions_live.csv"
    file_exists = path.exists()
    with path.open("a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=TXN_HEADERS)
        if not file_exists:
            writer.writeheader()
        simulated_day = date.today()
        try:
            while True:
                rows = []
                for _ in range(batch_size):
                    person = random.choice(individuals)
                    merchant, category = random.choice(CARD_MERCHANTS)
                    amount = round(random.uniform(5, 180), 2)
                    spend_time = datetime.combine(simulated_day, datetime.min.time()) + timedelta(
                        hours=random.randint(7, 21), minutes=random.randint(0, 59)
                    )
                    rows.append(
                        {
                            "ext_id": txn_ids.next(),
                            "account_ext_id": person.checking_account,
                            "posted_at": spend_time.isoformat(),
                            "txn_date": simulated_day.isoformat(),
                            "amount": fmt_amount(amount),
                            "currency": currency,
                            "direction": "DEBIT",
                            "section_name": "expense",
                            "category_name": category,
                            "channel": "CARD",
                            "description": f"Live spend at {merchant}",
                            "counterparty_name": merchant,
                            "counterparty_account": "",
                            "counterparty_bic": "",
                            "transfer_ref": "",
                            "ext_reference": "",
                        }
                    )
                for row in rows:
                    writer.writerow(row)
                f.flush()
                os.fsync(f.fileno())
                logger.info(
                    "Streamed %s live transactions for %s",
                    len(rows),
                    simulated_day.isoformat(),
                )
                simulated_day += timedelta(days=1)
                time.sleep(interval)
        except KeyboardInterrupt:
            logger.info("Live stream stopped by user.")


def main() -> None:
    args = parse_args()
    random.seed(args.seed)

    start_year, start_month = map(int, args.start.split("-"))
    start_month_date = date(start_year, start_month, 1)

    companies = build_companies(args.companies)
    individuals = build_individuals(args.individuals, companies)

    logger.info(
        "Generating dataset for %s individuals and %s companies", len(individuals), len(companies)
    )
    write_core_tables(individuals, companies, args.currency)

    txn_ids = IdFactory("T-")
    stats = {"individual": 0, "company": 0}
    txn_iter = itertools.chain(
        individual_transactions(
            individuals=individuals,
            txn_ids=txn_ids,
            start_month=start_month_date,
            months=args.months,
            currency=args.currency,
            stats=stats,
        ),
        company_transactions(
            companies=companies,
            txn_ids=txn_ids,
            start_month=start_month_date,
            months=args.months,
            currency=args.currency,
            stats=stats,
        ),
    )
    total_transactions = write_transactions(txn_iter)

    trades = generate_trades(individuals, start_month_date, args.months)
    logger.info("Writing trade data CSV")
    write_trades(trades)

    logger.info(
        "Generated %s users, %s organisations, %s transactions",
        len(individuals),
        len(companies),
        total_transactions,
    )
    logger.info(
        "Breakdown — individual accounts: %s rows, corporate accounts: %s rows",
        f"{stats['individual']:,}",
        f"{stats['company']:,}",
    )

    if args.continuous:
        logger.info("Starting live stream. Press Ctrl+C to stop.")
        live_stream(
            individuals=individuals,
            txn_ids=txn_ids,
            currency=args.currency,
            batch_size=args.live_batch_size,
            interval=args.live_interval,
        )


if __name__ == "__main__":
    init_logging(app_name="seed-data")
    log_context.bind(job="generate_seed", seed_dir=str(SEED_DIR))
    main()
