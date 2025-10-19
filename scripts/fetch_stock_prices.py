#!/usr/bin/env python3
"""Fetch real historical stock prices and FX rates for the finance simulation."""
from __future__ import annotations

import argparse
import csv
import os
import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import yfinance as yf
from app.log import get_logger, init_logging, log_context, progress_manager, timeit

logger = get_logger(__name__)

# Instruments to fetch (matching gen_seed_data.py)
INSTRUMENTS = [
    {"ext_id": "I-AAPL", "symbol": "AAPL", "name": "Apple Inc.", "type": "EQUITY", "currency": "USD", "isin": "US0378331005", "mic": "XNAS"},
    {"ext_id": "I-MSFT", "symbol": "MSFT", "name": "Microsoft Corporation", "type": "EQUITY", "currency": "USD", "isin": "US5949181045", "mic": "XNAS"},
    {"ext_id": "I-NVDA", "symbol": "NVDA", "name": "NVIDIA Corporation", "type": "EQUITY", "currency": "USD", "isin": "US67066G1040", "mic": "XNAS"},
    {"ext_id": "I-TSLA", "symbol": "TSLA", "name": "Tesla Inc.", "type": "EQUITY", "currency": "USD", "isin": "US88160R1014", "mic": "XNAS"},
    {"ext_id": "I-VWRL", "symbol": "VWRL.L", "name": "Vanguard FTSE All-World UCITS", "type": "ETF", "currency": "USD", "isin": "IE00B3RBWM25", "mic": "XLON"},
    {"ext_id": "I-ASM", "symbol": "ASML.AS", "name": "ASML Holding N.V.", "type": "EQUITY", "currency": "EUR", "isin": "NL0010273215", "mic": "XAMS"},
]

SEED_DIR = Path("data/seed")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start-date", type=str, default="2021-01-01", help="Start date for historical data (YYYY-MM-DD)")
    parser.add_argument("--end-date", type=str, default=None, help="End date for historical data (YYYY-MM-DD). Defaults to today.")
    parser.add_argument("--force", action="store_true", help="Force re-download even if files exist")
    parser.add_argument("--max-age-days", type=int, default=7, help="Maximum age of existing files in days before re-download")
    return parser.parse_args()


def should_refresh_files(max_age_days: int) -> bool:
    """Check if price files need to be refreshed based on age."""
    price_file = SEED_DIR / "price_daily.csv"
    fx_file = SEED_DIR / "fx_rate_daily.csv"
    
    if not price_file.exists() or not fx_file.exists():
        return True
    
    # Check if files are older than max_age_days
    price_age = datetime.now() - datetime.fromtimestamp(price_file.stat().st_mtime)
    fx_age = datetime.now() - datetime.fromtimestamp(fx_file.stat().st_mtime)
    
    return price_age.days > max_age_days or fx_age.days > max_age_days


def fetch_stock_prices(start_date: str, end_date: str) -> List[Dict[str, str]]:
    """Fetch historical prices for all instruments."""
    logger.info("Fetching stock prices from %s to %s", start_date, end_date)
    
    all_prices = []
    
    with progress_manager.task("Fetching stock prices", total=len(INSTRUMENTS), unit="stocks") as task:
        for instrument in INSTRUMENTS:
            symbol = instrument["symbol"]
            ext_id = instrument["ext_id"]
            currency = instrument["currency"]
            
            logger.debug("Fetching data for %s (%s)", symbol, ext_id)
            
            try:
                ticker = yf.Ticker(symbol)
                hist = ticker.history(start=start_date, end=end_date)
                
                if hist.empty:
                    logger.warning("No data found for %s (%s)", symbol, ext_id)
                    continue
                
                for date_idx, row in hist.iterrows():
                    # Convert pandas timestamp to date string
                    price_date = date_idx.date().isoformat()
                    close_price = f"{row['Close']:.2f}"
                    
                    all_prices.append({
                        "instrument_ext_id": ext_id,
                        "price_date": price_date,
                        "close_price": close_price,
                        "currency": currency,
                    })
                
                logger.debug("Fetched %s price points for %s", len(hist), symbol)
                
            except Exception as e:
                logger.error("Failed to fetch data for %s (%s): %s", symbol, ext_id, e)
                continue
            
            task.advance()
    
    logger.info("Fetched %s total price points", len(all_prices))
    return all_prices


def fetch_fx_rates(start_date: str, end_date: str) -> List[Dict[str, str]]:
    """Fetch USD/EUR exchange rates."""
    logger.info("Fetching USD/EUR exchange rates from %s to %s", start_date, end_date)
    
    fx_rates = []
    
    try:
        # Use EURUSD=X for USD/EUR rate (1 USD = X EUR)
        ticker = yf.Ticker("EURUSD=X")
        hist = ticker.history(start=start_date, end=end_date)
        
        if hist.empty:
            logger.warning("No FX data found for EURUSD=X")
            return fx_rates
        
        for date_idx, row in hist.iterrows():
            price_date = date_idx.date().isoformat()
            # EURUSD=X gives us 1 USD = X EUR, so we need 1 EUR = 1/X USD
            rate = 1.0 / row['Close']
            fx_rates.append({
                "base": "USD",
                "quote": "EUR", 
                "rate_date": price_date,
                "rate": f"{rate:.6f}",
            })
        
        logger.info("Fetched %s FX rate points", len(fx_rates))
        
    except Exception as e:
        logger.error("Failed to fetch FX data: %s", e)
    
    return fx_rates


def write_csv_files(prices: List[Dict[str, str]], fx_rates: List[Dict[str, str]]) -> None:
    """Write price and FX data to CSV files."""
    SEED_DIR.mkdir(parents=True, exist_ok=True)
    
    # Write price data
    price_file = SEED_DIR / "price_daily.csv"
    logger.info("Writing %s price points to %s", len(prices), price_file)
    
    with timeit("price csv write", logger=logger, unit="rows") as timer:
        with price_file.open("w", newline="", encoding="utf-8") as f:
            if prices:
                writer = csv.DictWriter(f, fieldnames=["instrument_ext_id", "price_date", "close_price", "currency"])
                writer.writeheader()
                for row in prices:
                    writer.writerow(row)
                    timer.add()
    
    # Write FX data
    fx_file = SEED_DIR / "fx_rate_daily.csv"
    logger.info("Writing %s FX rate points to %s", len(fx_rates), fx_file)
    
    with timeit("fx csv write", logger=logger, unit="rows") as timer:
        with fx_file.open("w", newline="", encoding="utf-8") as f:
            if fx_rates:
                writer = csv.DictWriter(f, fieldnames=["base", "quote", "rate_date", "rate"])
                writer.writeheader()
                for row in fx_rates:
                    writer.writerow(row)
                    timer.add()


def main() -> None:
    args = parse_args()
    
    # Set default end date to today if not provided
    end_date = args.end_date or date.today().isoformat()
    
    # Check if we need to refresh files
    if not args.force and not should_refresh_files(args.max_age_days):
        logger.info("Price files are recent (within %s days), skipping download. Use --force to override.", args.max_age_days)
        return
    
    logger.info("Starting price data fetch...")
    logger.info("Date range: %s to %s", args.start_date, end_date)
    
    # Fetch stock prices
    with timeit("stock price fetch", logger=logger):
        prices = fetch_stock_prices(args.start_date, end_date)
    
    # Fetch FX rates
    with timeit("fx rate fetch", logger=logger):
        fx_rates = fetch_fx_rates(args.start_date, end_date)
    
    # Write to CSV files
    with timeit("csv write", logger=logger):
        write_csv_files(prices, fx_rates)
    
    logger.info("Price data fetch complete!")
    logger.info("Generated %s price points and %s FX rate points", len(prices), len(fx_rates))


if __name__ == "__main__":
    init_logging(app_name="fetch-prices")
    log_context.bind(job="fetch_prices", seed_dir=str(SEED_DIR))
    main()
