"""Simple database connectivity check."""
from __future__ import annotations

import sys
from pathlib import Path

from sqlalchemy import text

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.config import get_settings  # noqa: E402  (import after sys.path manipulation)
from app.db.engine import create_sync_engine  # noqa: E402

settings = get_settings()
engine = create_sync_engine()


def main() -> None:
    with engine.connect() as conn:
        conn.execute(text("SELECT 1"))
        version = conn.execute(text("SELECT VERSION()"))
        db = conn.execute(text("SELECT DATABASE()"))
        print(
            "✅ Connected to {db} ({version}) via {driver}".format(
                db=db.scalar(),
                version=version.scalar(),
                driver=settings.database.driver,
            )
        )
        masked_password = "***" if settings.database.password else ""
        print(
            "Connection details: {user}:{pwd}@{host}:{port}/{name}".format(
                user=settings.database.user,
                pwd=masked_password,
                host=settings.database.host,
                port=settings.database.port,
                name=settings.database.name,
            )
        )


if __name__ == "__main__":
    main()
