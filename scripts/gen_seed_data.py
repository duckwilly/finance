#!/usr/bin/env python3
"""Generate rich synthetic data for the finance dashboard."""
from __future__ import annotations

import argparse
import csv
import itertools
import logging
import os
import random
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Iterable, Iterator, Optional, Sequence

SEED_DIR = Path("data/seed")
STREAM_DIR = Path("data/stream")

logger = logging.getLogger(__name__)


class ProgressTracker:
    """Simple textual progress indicator for generation steps."""

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
            pct = min(self.current / self.total, 1.0)
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

FIRST_NAMES = [
    "Liam",
    "Emma",
    "Noah",
    "Olivia",
    "Ava",
    "Isabella",
    "Sophia",
    "Mia",
    "Charlotte",
    "Amelia",
    "Lucas",
    "Mila",
    "Jack",
    "Emily",
    "Benjamin",
    "Ethan",
    "Samuel",
    "Eva",
    "Thomas",
    "Zoë",
]
LAST_NAMES = [
    "de Jong",
    "Jansen",
    "Bakker",
    "Visser",
    "Smit",
    "Meijer",
    "Mulder",
    "Bos",
    "Vos",
    "Peters",
    "Hendriks",
    "Kok",
    "van Dijk",
    "de Graaf",
    "van Leeuwen",
    "van der Meer",
    "Sanders",
    "Willems",
    "Kuipers",
    "Koster",
]
COMPANY_PREFIXES = [
    "North",
    "Blue",
    "Bright",
    "Next",
    "Prime",
    "Urban",
    "Atlas",
    "Delta",
    "Aurora",
    "Summit",
]
COMPANY_SUFFIXES = [
    "Analytics",
    "Logistics",
    "Studios",
    "Foods",
    "Consulting",
    "Industries",
    "Labs",
    "Retail",
    "Capital",
    "Ventures",
]
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

PRICE_POINTS = [
    ("2024-12-31", {"I-AAPL": 195.48, "I-MSFT": 389.25, "I-NVDA": 487.22, "I-TSLA": 253.50, "I-VWRL": 115.72, "I-ASM": 652.10}),
    ("2025-01-31", {"I-AAPL": 199.11, "I-MSFT": 402.18, "I-NVDA": 512.02, "I-TSLA": 261.34, "I-VWRL": 118.64, "I-ASM": 671.92}),
    ("2025-02-28", {"I-AAPL": 205.43, "I-MSFT": 410.55, "I-NVDA": 533.10, "I-TSLA": 275.11, "I-VWRL": 121.03, "I-ASM": 689.35}),
]

FX_POINTS = [
    ("2024-12-31", "USD", "EUR", 0.92),
    ("2025-01-31", "USD", "EUR", 0.93),
    ("2025-02-28", "USD", "EUR", 0.94),
]


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
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--individuals", type=int, default=2500, help="Number of individual users to create")
    parser.add_argument("--companies", type=int, default=120, help="Number of corporate organisations")
    parser.add_argument("--months", type=int, default=18, help="How many months of history to generate")
    parser.add_argument("--start", type=str, default="2023-01", help="Start month (YYYY-MM)")
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
    first = random.choice(FIRST_NAMES)
    last = random.choice(LAST_NAMES)
    return first, last


def sanitize_email(name: str, idx: int) -> str:
    slug = name.lower().replace(" ", ".").replace("'", "")
    return f"{slug}{idx:04d}@example.com"


def build_companies(n: int) -> list[Company]:
    companies: list[Company] = []
    for i in range(1, n + 1):
        prefix = random.choice(COMPANY_PREFIXES)
        suffix = random.choice(COMPANY_SUFFIXES)
        name = f"{prefix} {suffix} BV"
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


def generate_trades(individuals: Sequence[Individual]) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    if not individuals:
        return [], []
    sample_size = min(len(individuals), max(100, len(individuals) // 10))
    random_investors = random.sample(individuals, k=sample_size)
    trade_rows: list[dict[str, object]] = []
    trade_id = IdFactory("TR-", start=1)
    for person in random_investors:
        num_trades = random.randint(6, 18)
        holdings: dict[str, float] = {}
        for _ in range(num_trades):
            instrument = random.choice(INSTRUMENTS)
            qty = round(random.uniform(1, 25), 2)
            price = round(random.uniform(20, 500), 2)
            side_options = ["BUY", "SELL"] if holdings.get(instrument["ext_id"], 0) > 2 else ["BUY"]
            side = random.choice(side_options)
            if side == "SELL":
                qty = min(qty, holdings.get(instrument["ext_id"], qty))
            trade_time = datetime(2024, random.randint(1, 12), random.randint(1, 28), random.randint(10, 16), random.randint(0, 59))
            trade_rows.append(
                {
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
                }
            )
            holdings[instrument["ext_id"]] = holdings.get(instrument["ext_id"], 0) + (qty if side == "BUY" else -qty)
    price_rows = [
        {
            "instrument_ext_id": instrument_id,
            "price_date": price_date,
            "close_price": f"{price:.2f}",
            "currency": next(inst["currency"] for inst in INSTRUMENTS if inst["ext_id"] == instrument_id),
        }
        for price_date, snapshot in PRICE_POINTS
        for instrument_id, price in snapshot.items()
    ]
    return trade_rows, price_rows


def generate_fx_rows() -> list[dict[str, object]]:
    return [
        {"base": base, "quote": quote, "rate_date": rate_date, "rate": f"{rate:.4f}"}
        for rate_date, base, quote, rate in FX_POINTS
    ]


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
    progress = ProgressTracker("Generating transactions", unit="rows", min_interval=0.2)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=TXN_HEADERS)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
            count += 1
            progress.advance()
    progress.finish()
    logger.info("Generated %s transactions", count)
    return count


def write_trades(trades: Sequence[dict[str, object]], prices: Sequence[dict[str, object]]) -> None:
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
    write_csv(
        SEED_DIR / "price_daily.csv",
        ["instrument_ext_id", "price_date", "close_price", "currency"],
        prices,
    )
    write_csv(
        SEED_DIR / "fx_rate_daily.csv",
        ["base", "quote", "rate_date", "rate"],
        generate_fx_rows(),
    )


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

    trades, prices = generate_trades(individuals)
    logger.info("Writing trade and market data CSVs")
    write_trades(trades, prices)

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
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    main()
