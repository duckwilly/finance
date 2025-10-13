# Simple connectivity check.
import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")

# Default to PyMySQL; override with DB_DRIVER if you prefer mysqlclient.
DRIVER = os.getenv("DB_DRIVER", "mysql+pymysql")
url = f"{DRIVER}://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

engine = create_engine(url, echo=os.getenv("SQLALCHEMY_ECHO", "false") == "true", future=True)

def main():
    with engine.connect() as conn:
        conn.execute(text("SELECT 1"))
        version = conn.execute(text("SELECT VERSION()")).scalar()
        db = conn.execute(text("SELECT DATABASE()")).scalar()
        print(f"✅ Connected to {db} ({version}) via {DRIVER}")

if __name__ == "__main__":
    main()
