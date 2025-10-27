"""Minimal configuration system used by the application scaffold."""
from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache
import os
from pathlib import Path
from typing import Tuple

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
class AuthAccount:
    """Configuration entry representing a demo account."""

    username: str
    subject_id: int


@dataclass(slots=True)
class AuthSettings:
    """Authentication settings loaded from environment variables."""

    secret_key: str
    algorithm: str
    access_token_expire_minutes: int
    admin_username: str
    admin_password: str
    demo_user_password: str
    individual_accounts: tuple[AuthAccount, ...]
    company_accounts: tuple[AuthAccount, ...]
    cookie_name: str = "access_token"
    enabled: bool = True


@dataclass(slots=True)
class Settings:
    """Top-level application configuration container."""

    database: DatabaseSettings
    auth: AuthSettings
    sqlalchemy_echo: bool = False

    @classmethod
    def from_env(cls) -> "Settings":
        """Load configuration values from environment variables."""

        def _get_env(name: str, default: str) -> str:
            return os.getenv(name, default)

        def _parse_accounts(value: str, default: str) -> Tuple[AuthAccount, ...]:
            raw_value = value or default
            accounts: list[AuthAccount] = []
            for item in raw_value.split(","):
                item = item.strip()
                if not item:
                    continue
                if ":" not in item:
                    raise ValueError(
                        "Account definitions must follow '<username>:<id>' format."
                    )
                username, raw_id = item.split(":", 1)
                username = username.strip()
                raw_id = raw_id.strip()
                if not username or not raw_id.isdigit():
                    raise ValueError(
                        "Invalid account definition: username and numeric id required."
                    )
                accounts.append(AuthAccount(username=username, subject_id=int(raw_id)))
            if not accounts:
                raise ValueError("At least one account must be configured.")
            return tuple(accounts)

        db = DatabaseSettings(
            driver=_get_env("DB_DRIVER", "mysql+pymysql"),
            host=_get_env("DB_HOST", "127.0.0.1"),
            port=int(_get_env("DB_PORT", "3306")),
            user=_get_env("DB_USER", "finance"),
            password=_get_env("DB_PASSWORD", "finance"),
            name=_get_env("DB_NAME", "finance"),
        )
        echo_flag = _get_env("SQLALCHEMY_ECHO", "0")
        auth = AuthSettings(
            secret_key=_get_env("JWT_SECRET_KEY", "change-me"),
            algorithm=_get_env("JWT_ALGORITHM", "HS256"),
            access_token_expire_minutes=int(_get_env("JWT_EXPIRE_MINUTES", "120")),
            admin_username=_get_env("ADMIN_USERNAME", "admin"),
            admin_password=_get_env("ADMIN_PASSWORD", "adminpass"),
            demo_user_password=_get_env("DEMO_USER_PASSWORD", "demo"),
            individual_accounts=_parse_accounts(
                _get_env("INDIVIDUAL_ACCOUNTS", ""),
                default="u1:1",
            ),
            company_accounts=_parse_accounts(
                _get_env("COMPANY_ACCOUNTS", ""),
                default="c1:1",
            ),
            enabled=_get_env("AUTH_ENABLED", "1") not in {"0", "false", "False"},
        )
        return cls(
            database=db,
            auth=auth,
            sqlalchemy_echo=echo_flag not in {"0", "false", "False"},
        )


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
            "auth": {
                "admin_username": settings.auth.admin_username,
                "individual_accounts": [
                    account.username for account in settings.auth.individual_accounts
                ],
                "company_accounts": [
                    account.username for account in settings.auth.company_accounts
                ],
                "token_ttl": settings.auth.access_token_expire_minutes,
                "enabled": settings.auth.enabled,
            },
        },
    )
    return settings
