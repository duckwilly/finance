# db/engine.py
import os
from functools import lru_cache
from urllib.parse import quote_plus

from dotenv import load_dotenv
from sqlalchemy import create_engine

from logger import log

load_dotenv()  

def _mysql_url() -> str:
    url = os.getenv("DATABASE_URL")
    if url:
        return url

    host = os.getenv("DB_HOST", "localhost")
    port = int(os.getenv("DB_PORT", "3306"))
    user = os.getenv("DB_USER", "")
    pwd  = os.getenv("DB_PASSWORD", "")
    db   = os.getenv("DB_NAME", "finance")
    # URL-encode password for special chars
    return f"mysql+pymysql://{user}:{quote_plus(pwd)}@{host}:{port}/{db}?charset=utf8mb4"

@lru_cache(maxsize=1)
def get_engine():
    log.info("Connecting to database.")
    """
    Return a singleton SQLAlchemy Engine.
    Using LRU cache ensures all imports reuse the same engine/process.
    """
    echo = os.getenv("SQLALCHEMY_ECHO", "0") in ("1", "true", "True")
    return create_engine(_mysql_url(), pool_pre_ping=True, echo=echo)