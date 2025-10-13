import os
import pandas as pd
from dotenv import load_dotenv
from db import get_engine
from sqlalchemy import text
from logger import log

load_dotenv()

CSV = "dataset.csv"
DB  = os.getenv("DB_NAME", "finance")
TABLE = "transactions"

log.info("RUNNING FILE: ingest-1.py")

def main():
    log.info("Starting ingestion.")
    # Read CSV
    df = pd.read_csv(CSV)
    # Light cleanup (kept — harmless for MySQL, helpful generally)
    df.columns = [c.strip().replace(" ", "_") for c in df.columns]

    engine = get_engine()

    # Count existing rows (0 if table doesn't exist)
    with engine.connect() as con:
        try:
            count = con.execute(text(f"SELECT COUNT(*) FROM `{TABLE}`")).scalar()
        except Exception:
            count = 0

    # Replace table with fresh data
    # Use method="multi" + chunksize for speed on larger files
    with engine.begin() as con:
        df.to_sql(
            TABLE,
            con=con,
            if_exists="replace",
            index=False,
            method="multi",
            chunksize=1000,
        )

    log.info(f"Database {DB} previously contained {count} rows in `{TABLE}`.")
    log.info(f"`{TABLE}` has been replaced with {len(df)} rows from {CSV}.")

if __name__ == "__main__":
    main()