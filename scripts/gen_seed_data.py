#!/usr/bin/env python3
"""Generate rich synthetic data for the finance dashboard."""
from __future__ import annotations

import argparse
import csv
import itertools
import random
import unicodedata
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Iterable, Iterator, Optional, Sequence
from decimal import Decimal
import sys

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

SEED_DIR = Path("data/seed")

from app.log import get_logger, init_logging, log_context, progress_manager, timeit
from scripts.name_data import random_company_name, random_person_name
from scripts.job_titles import get_job_titles_for_tier

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

SECTIONS = ("income", "expense", "transfer")

INSTRUMENTS = [
    {
        "ext_id": "I-AAPL",
        "symbol": "AAPL",
        "yf_symbol": "AAPL",
        "name": "Apple Inc.",
        "instrument_type_code": "EQUITY",
        "primary_currency_code": "USD",
        "primary_market_mic": "XNAS",
        "identifiers": {"ISIN": "US0378331005", "TICKER": "AAPL"},
    },
    {
        "ext_id": "I-MSFT",
        "symbol": "MSFT",
        "yf_symbol": "MSFT",
        "name": "Microsoft Corporation",
        "instrument_type_code": "EQUITY",
        "primary_currency_code": "USD",
        "primary_market_mic": "XNAS",
        "identifiers": {"ISIN": "US5949181045", "TICKER": "MSFT"},
    },
    {
        "ext_id": "I-NVDA",
        "symbol": "NVDA",
        "yf_symbol": "NVDA",
        "name": "NVIDIA Corporation",
        "instrument_type_code": "EQUITY",
        "primary_currency_code": "USD",
        "primary_market_mic": "XNAS",
        "identifiers": {"ISIN": "US67066G1040", "TICKER": "NVDA"},
    },
    {
        "ext_id": "I-TSLA",
        "symbol": "TSLA",
        "yf_symbol": "TSLA",
        "name": "Tesla Inc.",
        "instrument_type_code": "EQUITY",
        "primary_currency_code": "USD",
        "primary_market_mic": "XNAS",
        "identifiers": {"ISIN": "US88160R1014", "TICKER": "TSLA"},
    },
    {
        "ext_id": "I-VWRL",
        "symbol": "VWRL",
        "yf_symbol": "VWRL.L",
        "name": "Vanguard FTSE All-World UCITS",
        "instrument_type_code": "ETF",
        "primary_currency_code": "USD",
        "primary_market_mic": "XLON",
        "identifiers": {"ISIN": "IE00B3RBWM25", "TICKER": "VWRL"},
    },
    {
        "ext_id": "I-ASM",
        "symbol": "ASML",
        "yf_symbol": "ASML.AS",
        "name": "ASML Holding N.V.",
        "instrument_type_code": "EQUITY",
        "primary_currency_code": "EUR",
        "primary_market_mic": "XAMS",
        "identifiers": {"ISIN": "NL0010273215", "TICKER": "ASML"},
    },
]

REFERENCE_CURRENCIES = {
    "EUR": {"name": "Euro", "exponent": 2},
    "USD": {"name": "US Dollar", "exponent": 2},
    "GBP": {"name": "British Pound Sterling", "exponent": 2},
}

ACCOUNT_TYPES = [
    {"code": "checking", "description": "Checking / current account", "is_cash": True, "is_brokerage": False},
    {"code": "savings", "description": "Savings deposit account", "is_cash": True, "is_brokerage": False},
    {"code": "brokerage", "description": "Brokerage / investment account", "is_cash": False, "is_brokerage": True},
    {"code": "operating", "description": "Corporate operating account", "is_cash": True, "is_brokerage": False},
]

ACCOUNT_ROLES = [
    {"code": "OWNER", "description": "Primary owner of the account"},
    {"code": "MANAGER", "description": "May manage the account"},
    {"code": "VIEWER", "description": "Read-only access"},
    {"code": "EMPLOYEE_CARDHOLDER", "description": "Issued employee payment card"},
    {"code": "BANK_ADMIN", "description": "Bank administrator access"},
]

TRANSACTION_CHANNELS = [
    {"code": "SEPA", "description": "SEPA payment"},
    {"code": "CARD", "description": "Card payment"},
    {"code": "WIRE", "description": "Wire transfer"},
    {"code": "CASH", "description": "Cash movement"},
    {"code": "INTERNAL", "description": "Internal transfer"},
]

TRADE_SIDES = [
    {"code": "BUY", "description": "Purchase of an instrument"},
    {"code": "SELL", "description": "Sale of an instrument"},
]

INSTRUMENT_TYPES = [
    {"code": "EQUITY", "description": "Equity security"},
    {"code": "ETF", "description": "Exchange-traded fund"},
]

MARKETS = [
    {"mic": "XNAS", "name": "Nasdaq Stock Market", "timezone": "America/New_York", "country_code": "US"},
    {"mic": "XLON", "name": "London Stock Exchange", "timezone": "Europe/London", "country_code": "GB"},
    {"mic": "XAMS", "name": "Euronext Amsterdam", "timezone": "Europe/Amsterdam", "country_code": "NL"},
]

# Price and FX data are now fetched from real sources via fetch_stock_prices.py


@dataclass
class Company:
    ext_id: str
    name: str
    industry: str
    account_ext_id: str
    iban: str
    size_tier: str
    base_margin: float
    margin_trend: float


@dataclass
class Individual:
    ext_id: str
    given_name: str
    family_name: str
    email: str
    employer: Company
    salary: float
    rent: float
    checking_account: str
    savings_account: str
    landlord: str
    wealth_tier: str
    job_title: str
    residency_country: str
    birth_date: date

    @property
    def name(self) -> str:
        return f"{self.given_name} {self.family_name}"


class IdFactory:
    def __init__(self, prefix: str, start: int = 1):
        self.prefix = prefix
        self.counter = start

    def next(self) -> str:
        value = f"{self.prefix}{self.counter:07d}"
        self.counter += 1
        return value


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


def normalize_display_key(value: str) -> str:
    """Produce an accent-insensitive lowercase key for uniqueness checks."""
    normalized = unicodedata.normalize("NFKD", value)
    without_accents = "".join(ch for ch in normalized if not unicodedata.combining(ch))
    return without_accents.lower()


def random_name() -> tuple[str, str]:
    """Return a first/last combination using the shared name pools."""
    return random_person_name()


def sanitize_email(name: str, idx: int) -> str:
    normalized = unicodedata.normalize("NFKD", name)
    ascii_name = "".join(ch for ch in normalized if not unicodedata.combining(ch))
    slug = ascii_name.lower().replace(" ", ".").replace("'", "").replace("-", ".")
    return f"{slug}{idx:04d}@example.com"


def build_companies(n: int) -> list[Company]:
    # Define size tiers with power-law distribution (more small, fewer large)
    SIZE_TIERS = {
        "micro": {"weight": 0.35, "revenue_multiplier": 1.0, "employee_multiplier": 1.0},
        "small": {"weight": 0.30, "revenue_multiplier": 2.5, "employee_multiplier": 2.0},
        "medium": {"weight": 0.20, "revenue_multiplier": 5.0, "employee_multiplier": 4.0},
        "large": {"weight": 0.12, "revenue_multiplier": 7.5, "employee_multiplier": 6.0},
        "enterprise": {"weight": 0.03, "revenue_multiplier": 10.0, "employee_multiplier": 8.0},
    }
    
    # Create weighted list for random selection
    tier_weights = list(SIZE_TIERS.keys())
    tier_probs = [SIZE_TIERS[tier]["weight"] for tier in tier_weights]
    
    companies: list[Company] = []
    seen_names: set[str] = set()
    seen_name_keys: set[str] = set()
    for i in range(1, n + 1):
        name = random_company_name()
        name_key = normalize_display_key(name)
        while name in seen_names or name_key in seen_name_keys:
            name = random_company_name()
            name_key = normalize_display_key(name)
        seen_names.add(name)
        seen_name_keys.add(name_key)
        industry = random.choice(INDUSTRIES)
        ext_id = f"C-{i:05d}"
        account_ext_id = f"A-C{i:05d}-OP"
        iban = f"NL{i % 90 + 10:02d}BANK{1000000000 + i:010d}"
        
        # Assign size tier using power-law distribution
        size_tier = random.choices(tier_weights, weights=tier_probs)[0]
        
        # Assign base profit margin (-5% to 20%)
        base_margin = random.uniform(-0.05, 0.20)
        
        # Assign margin trend (-0.02 to +0.02 per month, representing improving/declining companies)
        margin_trend = random.uniform(-0.02, 0.02)
        
        companies.append(Company(ext_id, name, industry, account_ext_id, iban, size_tier, base_margin, margin_trend))
    return companies


def build_individuals(n: int, companies: Sequence[Company]) -> list[Individual]:
    """Create individuals with wealth-tier-based distribution and size-weighted employer assignment."""
    
    # Define wealth tiers with their proportions and characteristics
    WEALTH_TIERS = {
        "low_income": {"pct": 0.30, "salary_range": (2200, 3800), "rent_pct": (0.28, 0.35)},
        "small_investor": {"pct": 0.28, "salary_range": (3200, 5500), "rent_pct": (0.24, 0.30)},
        "medium_investor": {"pct": 0.28, "salary_range": (4800, 8500), "rent_pct": (0.20, 0.26)},
        "high_investor": {"pct": 0.14, "salary_range": (15000, 50000), "rent_pct": (0.10, 0.18)},
    }
    
    # Create company weights based on size tiers (larger companies get more employees)
    SIZE_TIERS = {
        "micro": 1.0,
        "small": 2.0, 
        "medium": 4.0,
        "large": 6.0,
        "enterprise": 8.0,
    }
    
    company_weights = [SIZE_TIERS[company.size_tier] for company in companies]
    
    # Track used job titles per company to ensure uniqueness
    company_used_titles = {company.ext_id: set() for company in companies}
    
    individuals = []
    user_id = 1
    seen_names: set[str] = set()
    seen_name_keys: set[str] = set()
    max_name_attempts = 100
    
    for tier_name, config in WEALTH_TIERS.items():
        count = int(n * config["pct"])
        min_salary, max_salary = config["salary_range"]
        min_rent_pct, max_rent_pct = config["rent_pct"]
        
        for _ in range(count):
            attempts = 0
            while True:
                first, last = random_name()
                full_name = f"{first} {last}"
                key = normalize_display_key(full_name)
                if full_name not in seen_names and key not in seen_name_keys:
                    seen_names.add(full_name)
                    seen_name_keys.add(key)
                    break
                attempts += 1
                if attempts >= max_name_attempts:
                    raise RuntimeError("Unable to generate a unique individual name after multiple attempts")
            salary = round(random.uniform(min_salary, max_salary), 2)
            rent = round(salary * random.uniform(min_rent_pct, max_rent_pct), 2)

            birth_year = random.randint(1960, 1998)
            birth_month = random.randint(1, 12)
            birth_day = random.randint(1, 28)
            birth_dt = date(birth_year, birth_month, birth_day)

            # Assign employer based on company size weights
            employer = random.choices(companies, weights=company_weights)[0]

            # Assign job title based on wealth tier, ensuring uniqueness within company
            available_titles = get_job_titles_for_tier(tier_name)
            used_titles = company_used_titles[employer.ext_id]
            unused_titles = [title for title in available_titles if title not in used_titles]

            if unused_titles:
                job_title = random.choice(unused_titles)
            else:
                # If all titles for this tier are used, pick any available title
                job_title = random.choice(available_titles)

            # Track the used title
            company_used_titles[employer.ext_id].add(job_title)

            individuals.append(
                Individual(
                    ext_id=f"U-{user_id:05d}",
                    given_name=first,
                    family_name=last,
                    email=sanitize_email(f"{first} {last}", user_id),
                    employer=employer,
                    salary=salary,
                    rent=rent,
                    checking_account=f"A-U{user_id:05d}-CHK",
                    savings_account=f"A-U{user_id:05d}-SAV",
                    landlord=random.choice(PROPERTY_MANAGERS),
                    wealth_tier=tier_name,
                    job_title=job_title,
                    residency_country="NL",
                    birth_date=birth_dt,
                )
            )
            user_id += 1
    
    # Ensure every company has at least one employee
    company_employee_counts = {}
    for individual in individuals:
        company_id = individual.employer.ext_id
        company_employee_counts[company_id] = company_employee_counts.get(company_id, 0) + 1
    
    # Find companies with no employees and assign one
    for company in companies:
        if company_employee_counts.get(company.ext_id, 0) == 0:
            # Find a random individual to reassign
            individual_to_reassign = random.choice(individuals)
            individual_to_reassign.employer = company
            company_employee_counts[company.ext_id] = 1
    
    random.shuffle(individuals)
    return individuals


def write_csv(path: Path, headers: Sequence[str], rows: Iterable[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def write_reference_tables(base_currency: str) -> None:
    """Emit lookup CSVs so the database can be rebuilt without enums."""
    currencies = dict(REFERENCE_CURRENCIES)
    if base_currency not in currencies:
        currencies[base_currency] = {"name": base_currency, "exponent": 2}

    write_csv(
        SEED_DIR / "currencies.csv",
        ["code", "name", "exponent"],
        [
            {"code": code, "name": data["name"], "exponent": data["exponent"]}
            for code, data in sorted(currencies.items())
        ],
    )

    def bool_to_int(value: bool) -> int:
        return 1 if value else 0

    write_csv(
        SEED_DIR / "account_types.csv",
        ["code", "description", "is_cash", "is_brokerage"],
        [
            {
                "code": row["code"],
                "description": row["description"],
                "is_cash": bool_to_int(row["is_cash"]),
                "is_brokerage": bool_to_int(row["is_brokerage"]),
            }
            for row in ACCOUNT_TYPES
        ],
    )

    write_csv(
        SEED_DIR / "account_roles.csv",
        ["code", "description"],
        ACCOUNT_ROLES,
    )

    write_csv(
        SEED_DIR / "transaction_channels.csv",
        ["code", "description"],
        TRANSACTION_CHANNELS,
    )

    write_csv(
        SEED_DIR / "trade_sides.csv",
        ["code", "description"],
        TRADE_SIDES,
    )

    write_csv(
        SEED_DIR / "instrument_types.csv",
        ["code", "description"],
        INSTRUMENT_TYPES,
    )

    write_csv(
        SEED_DIR / "markets.csv",
        ["mic", "name", "timezone", "country_code"],
        MARKETS,
    )


def write_instruments() -> None:
    """Emit core instrument metadata and identifiers."""
    instrument_rows: list[dict[str, object]] = []
    identifier_rows: list[dict[str, object]] = []

    for instrument in INSTRUMENTS:
        instrument_rows.append(
            {
                "ext_id": instrument["ext_id"],
                "symbol": instrument["symbol"],
                "name": instrument["name"],
                "instrument_type_code": instrument["instrument_type_code"],
                "primary_currency_code": instrument["primary_currency_code"],
                "primary_market_mic": instrument.get("primary_market_mic") or "",
            }
        )

        for id_type, value in instrument.get("identifiers", {}).items():
            if not value:
                continue
            identifier_rows.append(
                {
                    "instrument_ext_id": instrument["ext_id"],
                    "identifier_type": id_type,
                    "identifier_value": value,
                }
            )

        yf_symbol = instrument.get("yf_symbol")
        if yf_symbol:
            identifier_rows.append(
                {
                    "instrument_ext_id": instrument["ext_id"],
                    "identifier_type": "YF_SYMBOL",
                    "identifier_value": yf_symbol,
                }
            )

    write_csv(
        SEED_DIR / "instruments.csv",
        ["ext_id", "symbol", "name", "instrument_type_code", "primary_currency_code", "primary_market_mic"],
        instrument_rows,
    )
    write_csv(
        SEED_DIR / "instrument_identifiers.csv",
        ["instrument_ext_id", "identifier_type", "identifier_value"],
        identifier_rows,
    )


def write_journal_tables(entries: Sequence[dict[str, object]], lines: Sequence[dict[str, object]]) -> None:
    """Persist journal entries and lines to CSV."""
    write_csv(
        SEED_DIR / "journal_entries.csv",
        ["entry_code", "txn_date", "posted_at", "description", "channel_code", "counterparty_party_ext_id", "transfer_reference", "external_reference"],
        entries,
    )
    write_csv(
        SEED_DIR / "journal_lines.csv",
        ["entry_code", "account_ext_id", "party_ext_id", "amount", "currency_code", "category_name", "section_name", "line_memo"],
        lines,
    )


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
                "counterparty_party_ext_id": person.employer.ext_id,
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
                "counterparty_party_ext_id": person.ext_id,
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
    individuals: Sequence[Individual],
    txn_ids: IdFactory,
    start_month: date,
    months: int,
    currency: str,
    stats: dict[str, int],
) -> Iterator[dict[str, object]]:
    # Size tier multipliers for revenue and expenses
    SIZE_TIERS = {
        "micro": {"revenue_multiplier": 1.0, "expense_multiplier": 1.0, "base_revenue": 50000, "base_expenses": 40000},
        "small": {"revenue_multiplier": 2.5, "expense_multiplier": 2.0, "base_revenue": 125000, "base_expenses": 100000},
        "medium": {"revenue_multiplier": 5.0, "expense_multiplier": 4.0, "base_revenue": 250000, "base_expenses": 200000},
        "large": {"revenue_multiplier": 7.5, "expense_multiplier": 6.0, "base_revenue": 375000, "base_expenses": 300000},
        "enterprise": {"revenue_multiplier": 10.0, "expense_multiplier": 8.0, "base_revenue": 500000, "base_expenses": 400000},
    }
    
    # Calculate payroll costs per company
    company_payroll = {}
    for company in companies:
        company_employees = [person for person in individuals if person.employer.ext_id == company.ext_id]
        monthly_payroll = sum(person.salary for person in company_employees)
        company_payroll[company.ext_id] = monthly_payroll
    
    for month_start in month_sequence(start_month, months):
        month_finish = month_end(month_start)
        for company in companies:
            # Calculate current margin with trend over time
            months_elapsed = (month_start.year - start_month.year) * 12 + (month_start.month - start_month.month)
            current_margin = company.base_margin + (company.margin_trend * months_elapsed)
            current_margin = max(-0.10, min(0.25, current_margin))  # Clamp between -10% and 25%
            
            # Get size tier configuration
            tier_config = SIZE_TIERS[company.size_tier]
            
            # Calculate base revenue and expenses for this month
            base_revenue = tier_config["base_revenue"] * tier_config["revenue_multiplier"]
            base_expenses = tier_config["base_expenses"] * tier_config["expense_multiplier"]
            
            # Add some monthly variation (±20%)
            revenue_variation = random.uniform(0.8, 1.2)
            expense_variation = random.uniform(0.8, 1.2)
            
            monthly_revenue = base_revenue * revenue_variation
            monthly_expenses = base_expenses * expense_variation
            
            # Get payroll costs for this company
            monthly_payroll = company_payroll[company.ext_id]
            
            # Calculate target total expenses (including payroll) to achieve margin
            target_total_expenses = monthly_revenue * (1 - current_margin)
            target_non_payroll_expenses = target_total_expenses - monthly_payroll
            
            # Adjust non-payroll expenses to achieve target margin
            if target_non_payroll_expenses > 0:
                expense_adjustment = target_non_payroll_expenses / monthly_expenses if monthly_expenses > 0 else 1.0
                monthly_expenses *= expense_adjustment
            else:
                # If payroll alone exceeds target expenses, set non-payroll to minimum
                monthly_expenses = max(1000, monthly_revenue * 0.05)  # Minimum 5% of revenue
            
            # Generate revenue transactions (customer payments)
            num_customers = random.randint(8, 20)  # Vary customer count
            customer_payments = []
            remaining_revenue = monthly_revenue
            
            for i in range(num_customers):
                if i == num_customers - 1:  # Last customer gets remaining revenue
                    amount = remaining_revenue
                else:
                    # Distribute revenue among customers
                    max_amount = remaining_revenue / (num_customers - i)
                    amount = round(random.uniform(1000, max_amount), 2)
                    remaining_revenue -= amount
                
                if amount > 0:
                    client = random.choice(CUSTOMERS)
                    invoice_day = month_start + timedelta(days=random.randint(1, month_finish.day - 1))
                    customer_payments.append({
                        "amount": amount,
                        "client": client,
                        "day": invoice_day
                    })
            
            # Generate expense transactions
            expenses = []
            remaining_expenses = monthly_expenses
            
            # Rent (20-30% of expenses)
            rent_pct = random.uniform(0.20, 0.30)
            rent_amount = round(remaining_expenses * rent_pct, 2)
            if rent_amount > 0:
                expenses.append({
                    "amount": rent_amount,
                    "category": "Rent",
                    "description": "Office lease",
                    "counterparty": random.choice(PROPERTY_MANAGERS),
                    "day": month_start + timedelta(days=random.randint(1, 5))
                })
                remaining_expenses -= rent_amount
            
            # Vendors (15-25% of expenses)
            vendor_pct = random.uniform(0.15, 0.25)
            vendor_amount = round(remaining_expenses * vendor_pct, 2)
            if vendor_amount > 0:
                num_vendors = random.randint(2, 5)
                vendor_per_vendor = vendor_amount / num_vendors
                for vendor in random.sample(BUSINESS_VENDORS, k=num_vendors):
                    expenses.append({
                        "amount": round(vendor_per_vendor, 2),
                        "category": "Vendors",
                        "description": f"Payment to {vendor}",
                        "counterparty": vendor,
                        "day": month_start + timedelta(days=random.randint(8, 20))
                    })
                remaining_expenses -= vendor_amount
            
            # Other expenses (remaining)
            if remaining_expenses > 0:
                expenses.append({
                    "amount": round(remaining_expenses, 2),
                    "category": "Other",
                    "description": "Miscellaneous business expenses",
                    "counterparty": "Various",
                    "day": month_start + timedelta(days=random.randint(10, 25))
                })
            
            # Quarterly taxes
            if (month_start.month - 1) % 3 == 0:
                tax_amount = round(monthly_revenue * random.uniform(0.15, 0.25), 2)  # 15-25% of revenue
                expenses.append({
                    "amount": tax_amount,
                    "category": "Taxes",
                    "description": "Quarterly VAT remittance",
                    "counterparty": "Belastingdienst",
                    "day": month_start + timedelta(days=random.randint(20, 27))
                })
            
            # Yield revenue transactions
            for payment in customer_payments:
                stats["company"] += 1
                yield {
                    "ext_id": txn_ids.next(),
                    "account_ext_id": company.account_ext_id,
                    "posted_at": iso_datetime(payment["day"], random.randint(8, 17), random.randint(0, 59)),
                    "txn_date": payment["day"].isoformat(),
                    "amount": fmt_amount(payment["amount"]),
                    "currency": currency,
                    "direction": "CREDIT",
                    "section_name": "income",
                    "category_name": "Customer payment",
                    "channel": "SEPA",
                    "description": f"Invoice settlement from {payment['client']}",
                    "counterparty_name": payment["client"],
                    "counterparty_account": "",
                    "counterparty_bic": "",
                    "transfer_ref": "",
                    "ext_reference": "",
                }
            
            # Yield expense transactions
            for expense in expenses:
                if expense["amount"] > 0:
                    stats["company"] += 1
                    yield {
                        "ext_id": txn_ids.next(),
                        "account_ext_id": company.account_ext_id,
                        "posted_at": iso_datetime(expense["day"], random.randint(9, 14), random.randint(0, 59)),
                        "txn_date": expense["day"].isoformat(),
                        "amount": fmt_amount(expense["amount"]),
                        "currency": currency,
                        "direction": "DEBIT",
                        "section_name": "expense",
                        "category_name": expense["category"],
                        "channel": "SEPA",
                        "description": expense["description"],
                        "counterparty_name": expense["counterparty"],
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
        # Generate historical trades based on wealth tier
        if person.wealth_tier == "small_investor":
            num_historical = random.randint(1, 3)
        elif person.wealth_tier == "medium_investor":
            num_historical = random.randint(2, 4)
        elif person.wealth_tier == "high_investor":
            num_historical = random.randint(4, 8)
        else:
            num_historical = 0  # No historical trades for low_income tier
        
        historical_start = start_month - timedelta(days=random.randint(180, 540))  # 6-18 months before
        
        holdings: dict[str, float] = {}
        for _ in range(num_historical):
            instrument = random.choice(INSTRUMENTS)
            # Use wealth-tier based quantities for historical trades
            if person.wealth_tier == "small_investor":
                qty = round(random.uniform(1, 8), 2)
            elif person.wealth_tier == "medium_investor":
                qty = round(random.uniform(3, 15), 2)
            elif person.wealth_tier == "high_investor":
                qty = round(random.uniform(10, 500), 2)
            else:
                qty = round(random.uniform(1, 5), 2)
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
                "currency": instrument["primary_currency_code"],
            })
            holdings[instrument["ext_id"]] = holdings.get(instrument["ext_id"], 0) + qty
    
    return historical_trades


def generate_trades(individuals: Sequence[Individual], start_month: date, months: int) -> list[dict[str, object]]:
    """Generate trades for the simulation period with wealth-tier-based portfolio distribution."""
    if not individuals:
        return []
    
    # Define trading characteristics by wealth tier
    TRADING_CONFIG = {
        "small_investor": {"trades": (2, 5), "stocks": (1, 2), "qty_range": (1, 8)},
        "medium_investor": {"trades": (6, 12), "stocks": (2, 4), "qty_range": (3, 15)},
        "high_investor": {"trades": (13, 25), "stocks": (3, 6), "qty_range": (10, 500)},
    }
    
    # Price ranges for different instruments
    PRICE_RANGES = {
        "I-AAPL": (120, 200), "I-MSFT": (300, 450), "I-NVDA": (200, 600),
        "I-TSLA": (150, 300), "I-VWRL": (80, 130), "I-ASM": (400, 800)
    }
    
    # Get investors (exclude low_income tier)
    investors = [p for p in individuals if p.wealth_tier in TRADING_CONFIG]
    
    trade_rows: list[dict[str, object]] = []
    trade_id = IdFactory("TR-", start=1)
    
    # Generate historical positions first
    historical_trades = generate_historical_positions(investors, start_month)
    trade_rows.extend(historical_trades)
    
    # Generate trades for each investor
    for person in investors:
        config = TRADING_CONFIG[person.wealth_tier]
        num_trades = random.randint(*config["trades"])
        num_stocks = random.randint(*config["stocks"])
        selected_stocks = random.sample(INSTRUMENTS, k=num_stocks)
        
        holdings: dict[str, float] = {}
        
        for _ in range(num_trades):
            instrument = random.choice(selected_stocks)
            qty = round(random.uniform(*config["qty_range"]), 2)
            
            # Get price based on instrument
            price_range = PRICE_RANGES.get(instrument["ext_id"], (50, 300))
            price = round(random.uniform(*price_range), 2)
            
            # Determine if this should be a BUY or SELL
            current_holding = holdings.get(instrument["ext_id"], 0)
            side = "SELL" if current_holding > qty * 2 and random.random() < 0.3 else "BUY"
            if side == "SELL":
                qty = min(qty, current_holding)
            
            # Generate trade time within simulation period
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
                "currency": instrument["primary_currency_code"],
            })
            holdings[instrument["ext_id"]] = holdings.get(instrument["ext_id"], 0) + (qty if side == "BUY" else -qty)
    
    return trade_rows


# FX data is now handled by fetch_stock_prices.py


def write_core_tables(
    individuals: list[Individual], companies: list[Company], currency: str
) -> tuple[dict[str, str], dict[str, str], dict[str, str]]:
    contract_ids = IdFactory("EC-")
    relationship_ids = IdFactory("REL-")

    party_rows: list[dict[str, object]] = [
        {
            "ext_id": "U-ADMIN",
            "party_type": "INDIVIDUAL",
            "display_name": "Bank Admin",
        }
    ]
    individual_profile_rows: list[dict[str, object]] = [
        {
            "party_ext_id": "U-ADMIN",
            "given_name": "Bank",
            "family_name": "Admin",
            "primary_email": "admin@simulatedbank.dev",
            "residency_country": "NL",
            "birth_date": "1980-01-01",
        }
    ]
    company_profile_rows: list[dict[str, object]] = []
    app_user_rows: list[dict[str, object]] = [
        {
            "ext_id": "U-ADMIN",
            "username": "admin",
            "email": "admin@simulatedbank.dev",
            "password_hash": "pbkdf2:mock:admin",
            "is_active": 1,
            "party_ext_id": "U-ADMIN",
        }
    ]
    app_user_role_rows: list[dict[str, object]] = [{"app_user_ext_id": "U-ADMIN", "role_code": "ADMIN"}]
    employment_contract_rows: list[dict[str, object]] = []
    company_access_rows: list[dict[str, object]] = []
    relationship_rows: list[dict[str, object]] = []
    contract_ext_by_employee: dict[str, str] = {}

    users_rows = [
        {"ext_id": "U-ADMIN", "name": "Bank Admin", "email": "admin@simulatedbank.dev", "job_title": "System Administrator"},
    ] + [
        {"ext_id": person.ext_id, "name": person.name, "email": person.email, "job_title": person.job_title}
        for person in individuals
    ]
    org_rows = [
        {
            "ext_id": company.ext_id, 
            "name": company.name, 
            "industry": company.industry,
            "size_tier": company.size_tier,
            "base_margin": company.base_margin,
            "margin_trend": company.margin_trend
        }
        for company in companies
    ]

    def industry_code_for(company: Company) -> str:
        cleaned = "".join(ch for ch in company.industry.upper() if ch.isalnum())
        return cleaned[:8] or "GENERAL"

    account_rows: list[dict[str, object]] = []
    account_party_role_rows: list[dict[str, object]] = []
    account_owner_map: dict[str, str] = {}

    for company in companies:
        party_rows.append(
            {
                "ext_id": company.ext_id,
                "party_type": "COMPANY",
                "display_name": company.name,
            }
        )
        incorporation_year = random.randint(1995, 2021)
        incorporation_month = random.randint(1, 12)
        incorporation_day = random.randint(1, 28)
        company_profile_rows.append(
            {
                "party_ext_id": company.ext_id,
                "legal_name": company.name,
                "registration_number": f"REG-{company.ext_id.split('-')[1]}",
                "tax_identifier": f"TAX-{company.ext_id.split('-')[1]}",
                "industry_code": industry_code_for(company),
                "incorporation_date": date(incorporation_year, incorporation_month, incorporation_day).isoformat(),
            }
        )
        account_rows.append(
            {
                "ext_id": company.account_ext_id,
                "party_ext_id": company.ext_id,
                "account_type_code": "operating",
                "currency_code": currency,
                "iban": company.iban,
                "opened_at": "2022-01-01T00:00:00",
                "closed_at": "",
                "name": f"{company.name} Operating",
            }
        )
        account_owner_map[company.account_ext_id] = company.ext_id
        account_party_role_rows.extend(
            [
                {
                    "account_ext_id": company.account_ext_id,
                    "party_ext_id": company.ext_id,
                    "role_code": "OWNER",
                    "start_date": "2022-01-01",
                    "end_date": "",
                    "is_primary": 1,
                },
                {
                    "account_ext_id": company.account_ext_id,
                    "party_ext_id": "U-ADMIN",
                    "role_code": "BANK_ADMIN",
                    "start_date": "2022-01-01",
                    "end_date": "",
                    "is_primary": 0,
                },
            ]
        )

    for person in individuals:
        party_rows.append(
            {
                "ext_id": person.ext_id,
                "party_type": "INDIVIDUAL",
                "display_name": person.name,
            }
        )
        individual_profile_rows.append(
            {
                "party_ext_id": person.ext_id,
                "given_name": person.given_name,
                "family_name": person.family_name,
                "primary_email": person.email,
                "residency_country": person.residency_country,
                "birth_date": person.birth_date.isoformat(),
            }
        )
        app_user_rows.append(
            {
                "ext_id": person.ext_id,
                "username": person.email,
                "email": person.email,
                "password_hash": "pbkdf2:mock:user",
                "is_active": 1,
                "party_ext_id": person.ext_id,
            }
        )
        app_user_role_rows.append({"app_user_ext_id": person.ext_id, "role_code": "EMPLOYEE"})

        contract_ext_id = contract_ids.next()
        start_date = "2023-01-01"
        employment_contract_rows.append(
            {
                "ext_id": contract_ext_id,
                "employee_party_ext_id": person.ext_id,
                "employer_party_ext_id": person.employer.ext_id,
                "position_title": person.job_title,
                "start_date": start_date,
                "end_date": "",
                "is_primary": 1,
            }
        )
        contract_ext_by_employee[person.ext_id] = contract_ext_id
        company_access_rows.append(
            {
                "contract_ext_id": contract_ext_id,
                "app_user_ext_id": person.ext_id,
                "role_code": "EMPLOYEE",
                "granted_at": f"{start_date}T08:00:00",
                "revoked_at": "",
            }
        )
        relationship_rows.append(
            {
                "ext_id": relationship_ids.next(),
                "from_party_ext_id": person.employer.ext_id,
                "to_party_ext_id": person.ext_id,
                "relationship_type": "EMPLOYMENT",
                "start_date": start_date,
                "end_date": "",
            }
        )

        identifier = int(person.ext_id.split("-")[1])
        account_rows.extend(
            [
                {
                    "ext_id": person.checking_account,
                    "party_ext_id": person.ext_id,
                    "account_type_code": "checking",
                    "currency_code": currency,
                    "iban": f"NL91SIMU{100000000 + identifier:010d}",
                    "opened_at": "2023-01-01T00:00:00",
                    "closed_at": "",
                    "name": f"{person.given_name} Checking",
                },
                {
                    "ext_id": person.savings_account,
                    "party_ext_id": person.ext_id,
                    "account_type_code": "savings",
                    "currency_code": currency,
                    "iban": "",
                    "opened_at": "2023-01-01T00:00:00",
                    "closed_at": "",
                    "name": f"{person.given_name} Savings",
                },
                {
                    "ext_id": person.checking_account.replace("-CHK", "-BRK"),
                    "party_ext_id": person.ext_id,
                    "account_type_code": "brokerage",
                    "currency_code": "USD",
                    "iban": "",
                    "opened_at": "2023-01-01T00:00:00",
                    "closed_at": "",
                    "name": f"{person.given_name} Brokerage",
                },
            ]
        )
        account_owner_map[person.checking_account] = person.ext_id
        account_owner_map[person.savings_account] = person.ext_id
        account_owner_map[person.checking_account.replace("-CHK", "-BRK")] = person.ext_id

        account_party_role_rows.extend(
            [
                {
                    "account_ext_id": person.checking_account,
                    "party_ext_id": person.ext_id,
                    "role_code": "OWNER",
                    "start_date": "2023-01-01",
                    "end_date": "",
                    "is_primary": 1,
                },
                {
                    "account_ext_id": person.savings_account,
                    "party_ext_id": person.ext_id,
                    "role_code": "OWNER",
                    "start_date": "2023-01-01",
                    "end_date": "",
                    "is_primary": 1,
                },
                {
                    "account_ext_id": person.checking_account.replace("-CHK", "-BRK"),
                    "party_ext_id": person.ext_id,
                    "role_code": "OWNER",
                    "start_date": "2023-01-01",
                    "end_date": "",
                    "is_primary": 1,
                },
            ]
        )

    clearing_party_ext = "GL-CLEAR"
    party_rows.append(
        {
            "ext_id": clearing_party_ext,
            "party_type": "COMPANY",
            "display_name": "Ledger Clearing",
        }
    )
    company_profile_rows.append(
        {
            "party_ext_id": clearing_party_ext,
            "legal_name": "Ledger Clearing",
            "registration_number": "REG-GL",
            "tax_identifier": "TAX-GL",
            "industry_code": "CLEARING",
            "incorporation_date": "2000-01-01",
        }
    )
    clearing_accounts: dict[str, str] = {}
    for section in SECTIONS:
        account_ext = f"A-GL-{section.upper()}"
        account_rows.append(
            {
                "ext_id": account_ext,
                "party_ext_id": clearing_party_ext,
                "account_type_code": "operating",
                "currency_code": currency,
                "iban": "",
                "opened_at": "2000-01-01T00:00:00",
                "closed_at": "",
                "name": f"Clearing {section.title()}",
            }
        )
        account_party_role_rows.append(
            {
                "account_ext_id": account_ext,
                "party_ext_id": clearing_party_ext,
                "role_code": "OWNER",
                "start_date": "2000-01-01",
                "end_date": "",
                "is_primary": 1,
            }
        )
        account_owner_map[account_ext] = clearing_party_ext
        clearing_accounts[section] = account_ext

    write_csv(
        SEED_DIR / "parties.csv",
        ["ext_id", "party_type", "display_name"],
        party_rows,
    )
    write_csv(
        SEED_DIR / "individual_profiles.csv",
        ["party_ext_id", "given_name", "family_name", "primary_email", "residency_country", "birth_date"],
        individual_profile_rows,
    )
    write_csv(
        SEED_DIR / "company_profiles.csv",
        ["party_ext_id", "legal_name", "registration_number", "tax_identifier", "industry_code", "incorporation_date"],
        company_profile_rows,
    )
    write_csv(
        SEED_DIR / "app_users.csv",
        ["ext_id", "username", "email", "password_hash", "is_active", "party_ext_id"],
        app_user_rows,
    )
    write_csv(
        SEED_DIR / "app_user_roles.csv",
        ["app_user_ext_id", "role_code"],
        app_user_role_rows,
    )
    write_csv(
        SEED_DIR / "employment_contracts.csv",
        ["ext_id", "employee_party_ext_id", "employer_party_ext_id", "position_title", "start_date", "end_date", "is_primary"],
        employment_contract_rows,
    )
    write_csv(
        SEED_DIR / "company_access_grants.csv",
        ["contract_ext_id", "app_user_ext_id", "role_code", "granted_at", "revoked_at"],
        company_access_rows,
    )
    write_csv(
        SEED_DIR / "party_relationships.csv",
        ["ext_id", "from_party_ext_id", "to_party_ext_id", "relationship_type", "start_date", "end_date"],
        relationship_rows,
    )

    write_csv(
        SEED_DIR / "users.csv",
        ["ext_id", "name", "email", "job_title"],
        users_rows,
    )
    write_csv(
        SEED_DIR / "orgs.csv",
        ["ext_id", "name", "industry", "size_tier", "base_margin", "margin_trend"],
        org_rows,
    )
    write_csv(
        SEED_DIR / "accounts.csv",
        ["ext_id", "party_ext_id", "account_type_code", "currency_code", "iban", "opened_at", "closed_at", "name"],
        account_rows,
    )
    write_csv(
        SEED_DIR / "account_party_roles.csv",
        ["account_ext_id", "party_ext_id", "role_code", "start_date", "end_date", "is_primary"],
        account_party_role_rows,
    )

    # Employment memberships (user -> org)
    employment_rows = [
        {
            "user_ext_id": person.ext_id,
            "org_ext_id": person.employer.ext_id,
            "role": "employee",
            "is_primary": True,
            "start_date": "2023-01-01",
            "end_date": "",
        }
        for person in individuals
    ]
    write_csv(
        SEED_DIR / "memberships.csv",
        ["user_ext_id", "org_ext_id", "role", "is_primary", "start_date", "end_date"],
        employment_rows,
    )
    return clearing_accounts, account_owner_map, contract_ext_by_employee


def build_journal_documents(
    rows: Iterator[dict[str, object]],
    clearing_accounts: dict[str, str],
    account_owner_map: dict[str, str],
) -> tuple[int, list[dict[str, object]], list[dict[str, object]]]:
    count = 0
    journal_entries: dict[str, dict[str, object]] = {}
    journal_lines: list[dict[str, object]] = []
    entry_totals: dict[str, Decimal] = {}

    with timeit("journal document build", logger=logger, unit="rows") as timer, progress_manager.task(
        "Generating journal documents", unit="rows"
    ) as task:
        for row in rows:
            count += 1
            timer.add()
            task.advance()

            entry_code = row["ext_id"]
            txn_date = row["txn_date"]
            posted_at = row["posted_at"]
            description = row.get("description") or ""
            channel_code = (row.get("channel") or "").upper() or ""
            transfer_ref = row.get("transfer_ref") or ""
            external_ref = row.get("ext_reference") or ""
            counterparty_party_ext_id = row.get("counterparty_party_ext_id") or ""

            entry = journal_entries.setdefault(
                entry_code,
                {
                    "entry_code": entry_code,
                    "txn_date": txn_date,
                    "posted_at": posted_at,
                    "description": description,
                    "channel_code": channel_code,
                    "counterparty_party_ext_id": counterparty_party_ext_id,
                    "transfer_reference": transfer_ref,
                    "external_reference": external_ref,
                },
            )

            if not entry["counterparty_party_ext_id"] and counterparty_party_ext_id:
                entry["counterparty_party_ext_id"] = counterparty_party_ext_id

            section_name = (row.get("section_name") or "transfer").lower()
            clearing_account_ext = clearing_accounts.get(section_name, clearing_accounts["transfer"])

            amount_value = Decimal(row["amount"])
            direction = (row.get("direction") or "CREDIT").upper()
            signed_amount = amount_value if direction == "CREDIT" else -amount_value

            account_ext_id = row["account_ext_id"]
            account_party_ext = account_owner_map.get(account_ext_id, "")
            currency_code = row["currency"]
            category_name = row.get("category_name") or ""
            section_value = row.get("section_name") or ""

            journal_lines.append(
                {
                    "entry_code": entry_code,
                    "account_ext_id": account_ext_id,
                    "party_ext_id": account_party_ext,
                    "amount": f"{signed_amount}",
                    "currency_code": currency_code,
                    "category_name": category_name,
                    "section_name": section_value,
                    "line_memo": description,
                }
            )

            entry_totals[entry_code] = entry_totals.get(entry_code, Decimal("0")) + signed_amount

            clearing_party_ext = account_owner_map.get(clearing_account_ext, "")
            journal_lines.append(
                {
                    "entry_code": entry_code,
                    "account_ext_id": clearing_account_ext,
                    "party_ext_id": clearing_party_ext,
                    "amount": f"{-signed_amount}",
                    "currency_code": currency_code,
                    "category_name": "",
                    "section_name": section_value,
                    "line_memo": description,
                }
            )

            entry_totals[entry_code] += -signed_amount

    for entry_code, balance in entry_totals.items():
        if balance != Decimal("0"):
            raise ValueError(f"Journal entry {entry_code} is not balanced ({balance})")

    ordered_entries = [journal_entries[key] for key in sorted(journal_entries.keys())]
    logger.info(
        "Generated %s journal entries from %s source events",
        len(ordered_entries),
        count,
    )
    return count, ordered_entries, journal_lines


def write_trades(trades: Sequence[dict[str, object]]) -> None:
    write_csv(
        SEED_DIR / "trades.csv",
        ["ext_id", "account_ext_id", "instrument_ext_id", "side", "qty", "price", "fees", "tax", "trade_time", "settle_dt", "currency"],
        trades,
    )
    # Price and FX data are now handled by fetch_stock_prices.py


def write_payroll_fact(
    individuals: Sequence[Individual],
    contract_ext_by_employee: dict[str, str],
    start_month: date,
    months: int,
) -> None:
    rows: list[dict[str, object]] = []
    for month_start in month_sequence(start_month, months):
        label = f"{month_start.year}-{month_start.month:02d}"
        for person in individuals:
            contract_ext = contract_ext_by_employee.get(person.ext_id)
            if not contract_ext:
                continue

            gross = person.salary
            net = round(gross * 0.75, 2)
            taxes = round(gross - net, 2)

            rows.append(
                {
                    "reporting_period_label": label,
                    "contract_ext_id": contract_ext,
                    "gross_amount": fmt_amount(gross),
                    "net_amount": fmt_amount(net),
                    "taxes_withheld": fmt_amount(taxes),
                }
            )

    write_csv(
        SEED_DIR / "payroll_fact.csv",
        ["reporting_period_label", "contract_ext_id", "gross_amount", "net_amount", "taxes_withheld"],
        rows,
    )

def main() -> None:
    args = parse_args()
    random.seed(args.seed)

    start_year, start_month = map(int, args.start.split("-"))
    start_month_date = date(start_year, start_month, 1)

    write_reference_tables(args.currency)
    write_instruments()

    companies = build_companies(args.companies)
    individuals = build_individuals(args.individuals, companies)

    logger.info(
        "Generating dataset for %s individuals and %s companies", len(individuals), len(companies)
    )
    clearing_accounts, account_owner_map, contract_ext_by_employee = write_core_tables(
        individuals, companies, args.currency
    )

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
            individuals=individuals,
            txn_ids=txn_ids,
            start_month=start_month_date,
            months=args.months,
            currency=args.currency,
            stats=stats,
        ),
    )
    source_events, journal_entries, journal_lines = build_journal_documents(
        txn_iter, clearing_accounts, account_owner_map
    )
    write_journal_tables(journal_entries, journal_lines)

    trades = generate_trades(individuals, start_month_date, args.months)
    logger.info("Writing trade data CSV")
    write_trades(trades)
    write_payroll_fact(individuals, contract_ext_by_employee, start_month_date, args.months)

    logger.info(
        "Generated %s users, %s organisations, %s source events",
        len(individuals),
        len(companies),
        source_events,
    )
    logger.info(
        "Breakdown — individual accounts: %s rows, corporate accounts: %s rows",
        f"{stats['individual']:,}",
        f"{stats['company']:,}",
    )

if __name__ == "__main__":
    init_logging(app_name="seed-data")
    log_context.bind(job="generate_seed", seed_dir=str(SEED_DIR))
    main()
