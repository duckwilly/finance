"""Application configuration primitives."""
from __future__ import annotations

import os
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv


def _load_env(dotenv_path: Optional[Path] = None) -> None:
    """Load the .env file once for the process."""

    if getattr(_load_env, "_loaded", False):  # type: ignore[attr-defined]
        return

    load_dotenv(dotenv_path)
    setattr(_load_env, "_loaded", True)  # type: ignore[attr-defined]


@dataclass(frozen=True)
class DatabaseSettings:
    """Configuration for the relational database."""

    driver: str = "mysql+pymysql"
    user: str = "app"
    password: str = "apppwd"
    host: str = "127.0.0.1"
    port: int = 3306
    name: str = "finance"

    @classmethod
    def from_env(cls) -> "DatabaseSettings":
        """Instantiate settings using environment overrides when present."""

        defaults = cls()
        return cls(
            driver=os.getenv("DB_DRIVER", defaults.driver),
            user=os.getenv("DB_USER", defaults.user),
            password=os.getenv("DB_PASSWORD", defaults.password),
            host=os.getenv("DB_HOST", defaults.host),
            port=int(os.getenv("DB_PORT", defaults.port)),
            name=os.getenv("DB_NAME", defaults.name),
        )

    @property
    def sqlalchemy_url(self) -> str:
        """Return a SQLAlchemy compatible URL."""

        return f"{self.driver}://{self.user}:{self.password}@{self.host}:{self.port}/{self.name}"


@dataclass(frozen=True)
class Settings:
    """Container for application configuration."""

    database: DatabaseSettings
    sqlalchemy_echo: bool = False

    @classmethod
    def from_env(cls, dotenv_path: Optional[Path] = None) -> "Settings":
        """Build ``Settings`` using environment variables (optionally from ``.env``)."""

        _load_env(dotenv_path)

        database = DatabaseSettings.from_env()

        sqlalchemy_echo = os.getenv("SQLALCHEMY_ECHO", "false").lower() == "true"

        return cls(database=database, sqlalchemy_echo=sqlalchemy_echo)


@lru_cache()
def get_settings(dotenv_path: Optional[Path] = None) -> Settings:
    """Return a cached settings instance."""

    return Settings.from_env(dotenv_path=dotenv_path)
