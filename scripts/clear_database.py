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
        "party",
        "user",
        "org",
        "app_user",
        "account",
        "account_party_role",
        "journal_entry",
        "cash_flow_fact",
        "holding_performance_fact",
        "payroll_fact",
        "trade",
        "holding",
        "instrument",
        "price_quote",
        "fx_rate_daily",
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
        "journal_line",
        "journal_entry",
        "lot",
        "holding",
        "trade",
        "price_quote",
        "instrument_identifier",
        "fx_rate_daily",
        "holding_performance_fact",
        "cash_flow_fact",
        "payroll_fact",
        "account_party_role",
        "account",
        "user_party_map",
        "org_party_map",
        "user_salary_monthly",
        "membership",
        "company_access_grant",
        "party_relationship",
        "employment_contract",
        "app_user_role",
        "app_user",
        "category",
        "section",
        "reporting_period",
        "instrument",
        "company_profile",
        "individual_profile",
        "party",
        "org",
        "user",
    ]
    
    with engine.begin() as connection:
        for table in tables_to_clear:
            try:
                result = connection.execute(text(f"DELETE FROM {table}"))
                logger.info(f"Cleared {result.rowcount} rows from {table}")
            except Exception as e:
                logger.warning(f"Could not clear table {table}: {e}")
        
        # Reset AUTO_INCREMENT counters for tables that use them
        auto_increment_tables = [
            "party",
            "user",
            "org",
            "app_user",
            "account",
            "account_party_role",
            "company_access_grant",
            "employment_contract",
            "journal_entry",
            "journal_line",
            "trade",
            "holding",
            "instrument",
            "instrument_identifier",
            "reporting_period",
        ]
        for table in auto_increment_tables:
            try:
                connection.execute(text(f"ALTER TABLE {table} AUTO_INCREMENT = 1"))
                logger.info(f"Reset AUTO_INCREMENT for {table}")
            except Exception as e:
                logger.warning(f"Could not reset AUTO_INCREMENT for {table}: {e}")
    
    logger.info("Database clearing complete")

if __name__ == "__main__":
    init_logging(app_name="clear-database")
    clear_database()
