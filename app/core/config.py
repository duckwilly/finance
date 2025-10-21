"""Minimal configuration system used by the application scaffold."""
from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache
import os
from pathlib import Path

from dotenv import load_dotenv

# Load .env file from project root
env_path = Path(__file__).parent.parent.parent / ".env"
load_dotenv(dotenv_path=env_path)


@dataclass(slots=True)
class DatabaseSettings:
    """Connection details for the transactional database."""

    driver: str
    host: str
    port: int
    user: str
    password: str
    name: str

    @property
    def sqlalchemy_url(self) -> str:
        """Build a SQLAlchemy compatible URL."""

        if self.password:
            credentials = f"{self.user}:{self.password}"
        else:
            credentials = self.user
        return f"{self.driver}://{credentials}@{self.host}:{self.port}/{self.name}"


@dataclass(slots=True)
class Settings:
    """Top-level application configuration container."""

    database: DatabaseSettings
    sqlalchemy_echo: bool = False

    @classmethod
    def from_env(cls) -> "Settings":
        """Load configuration values from environment variables."""

        def _get_env(name: str, default: str) -> str:
            return os.getenv(name, default)

        db = DatabaseSettings(
            driver=_get_env("DB_DRIVER", "mysql+pymysql"),
            host=_get_env("DB_HOST", "127.0.0.1"),
            port=int(_get_env("DB_PORT", "3306")),
            user=_get_env("DB_USER", "finance"),
            password=_get_env("DB_PASSWORD", "finance"),
            name=_get_env("DB_NAME", "finance"),
        )
        echo_flag = _get_env("SQLALCHEMY_ECHO", "0")
        return cls(database=db, sqlalchemy_echo=echo_flag not in {"0", "false", "False"})


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Return a cached ``Settings`` instance."""

    settings = Settings.from_env()

    # Import locally to avoid circular dependencies during module import time.
    from .logger import get_logger

    logger = get_logger(__name__)
    logger.debug(
        "Settings initialised",
        extra={
            "sqlalchemy_echo": settings.sqlalchemy_echo,
            "database": {
                "driver": settings.database.driver,
                "host": settings.database.host,
                "port": settings.database.port,
                "name": settings.database.name,
                "user": settings.database.user,
            },
        },
    )
    return settings
