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

def is_database_empty():
    """Check if the database has any data."""
    settings = get_settings()
    engine = create_engine(settings.database.sqlalchemy_url, future=True)
    
    # Tables to check for data
    tables_to_check = [
        "user",
        "org", 
        "account",
        "transaction",
        "trade",
        "holding",
        "instrument",
        "price_daily",
        "fx_rate_daily"
    ]
    
    with engine.connect() as connection:
        for table in tables_to_check:
            try:
                result = connection.execute(text(f"SELECT COUNT(*) FROM {table}"))
                count = result.scalar()
                if count > 0:
                    logger.info(f"Found {count} rows in {table}, database is not empty")
                    return False
            except Exception as e:
                # Table might not exist yet, which is fine for an empty database
                logger.debug(f"Could not check table {table}: {e}")
                continue
    
    logger.info("Database appears to be empty")
    return True

def clear_database():
    """Clear all data from database tables in dependency order."""
    # Check if database is empty first
    if is_database_empty():
        logger.info("Database is empty, skipping clear operation")
        return
    
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
        "user_salary_monthly",  # References user and org
        "membership",           # References user and org
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
