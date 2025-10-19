#!/usr/bin/env python3
"""Clear all data from the database tables."""
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from sqlalchemy import create_engine, text
from app.config import get_settings
from app.log import get_logger, init_logging

logger = get_logger(__name__)

def clear_database():
    """Clear all data from database tables in dependency order."""
    settings = get_settings()
    engine = create_engine(settings.database.sqlalchemy_url, future=True)
    
    # Tables in dependency order (child tables first)
    tables_to_clear = [
        "transfer_link",
        "lot", 
        "holding",
        "trade",
        "transaction",
        "account_membership",
        "account",
        "counterparty",
        "category",
        "section",
        "price_daily",
        "fx_rate_daily",
        "instrument",
        "org",
        "user"
    ]
    
    with engine.begin() as connection:
        for table in tables_to_clear:
            try:
                result = connection.execute(text(f"DELETE FROM {table}"))
                logger.info(f"Cleared {result.rowcount} rows from {table}")
            except Exception as e:
                logger.warning(f"Could not clear table {table}: {e}")
    
    logger.info("Database clearing complete")

if __name__ == "__main__":
    init_logging(app_name="clear-database")
    clear_database()
