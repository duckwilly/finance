"""Simple JWT-backed authentication helpers used by the demo."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from functools import lru_cache

import jwt
from fastapi import Depends, HTTPException, Request, status
from jwt import ExpiredSignatureError, InvalidTokenError

from app.core.config import AuthSettings, get_settings


class AuthenticationError(Exception):
    """Raised when authentication or token validation fails."""


@dataclass(frozen=True, slots=True)
class AuthenticatedUser:
    """Representation of the authenticated principal."""

    username: str
    role: str
    subject_id: int | None = None


class SecurityProvider:
    """Authenticate demo users and issue/verify JWT access tokens."""

    def __init__(self, settings: AuthSettings) -> None:
        self._settings = settings
        self._individual_accounts = {
            account.username: account.subject_id for account in settings.individual_accounts
        }
        self._company_accounts = {
            account.username: account.subject_id for account in settings.company_accounts
        }

    @property
    def cookie_name(self) -> str:
        """Return the cookie name used for the access token."""

        return self._settings.cookie_name

    @property
    def token_ttl_seconds(self) -> int:
        """Return the access token lifetime in seconds."""

        return int(self._settings.access_token_expire_minutes * 60)

    @property
    def admin_username(self) -> str:
        return self._settings.admin_username

    @property
    def demo_user_password(self) -> str:
        return self._settings.demo_user_password

    @property
    def is_enabled(self) -> bool:
        return self._settings.enabled

    def default_admin_user(self) -> AuthenticatedUser:
        return AuthenticatedUser(username=self._settings.admin_username, role="admin")

    def authenticate(self, username: str, password: str) -> AuthenticatedUser | None:
        """Validate the supplied credentials and return an ``AuthenticatedUser``."""

        if (
            username == self._settings.admin_username
            and password == self._settings.admin_password
        ):
            return AuthenticatedUser(username=username, role="admin")

        if password != self._settings.demo_user_password:
            return None

        if username in self._individual_accounts:
            return AuthenticatedUser(
                username=username,
                role="individual",
                subject_id=self._individual_accounts[username],
            )

        if username in self._company_accounts:
            return AuthenticatedUser(
                username=username,
                role="company",
                subject_id=self._company_accounts[username],
            )

        return None

    def create_access_token(self, user: AuthenticatedUser) -> str:
        """Create a signed JWT for the authenticated user."""

        now = datetime.now(tz=timezone.utc)
        expires = now + timedelta(minutes=self._settings.access_token_expire_minutes)
        payload: dict[str, object] = {
            "sub": user.username,
            "role": user.role,
            "iat": int(now.timestamp()),
            "exp": int(expires.timestamp()),
        }
        if user.subject_id is not None:
            payload["subject_id"] = user.subject_id
        token = jwt.encode(payload, self._settings.secret_key, algorithm=self._settings.algorithm)
        return token

    def decode_token(self, token: str) -> AuthenticatedUser:
        """Decode a JWT and return the corresponding ``AuthenticatedUser``."""

        try:
            payload = jwt.decode(
                token,
                self._settings.secret_key,
                algorithms=[self._settings.algorithm],
            )
        except ExpiredSignatureError as exc:  # pragma: no cover - runtime safeguard
            raise AuthenticationError("Token expired") from exc
        except InvalidTokenError as exc:  # pragma: no cover - runtime safeguard
            raise AuthenticationError("Invalid token") from exc

        username = payload.get("sub")
        role = payload.get("role")
        subject_id = payload.get("subject_id")
        if not isinstance(username, str) or not isinstance(role, str):
            raise AuthenticationError("Token payload missing required claims")

        resolved_subject: int | None = None
        if subject_id is not None:
            try:
                resolved_subject = int(subject_id)
            except (TypeError, ValueError) as exc:  # pragma: no cover - runtime safeguard
                raise AuthenticationError("Token subject claim invalid") from exc

        return AuthenticatedUser(username=username, role=role, subject_id=resolved_subject)


@lru_cache(maxsize=1)
def get_security_provider() -> SecurityProvider:
    """Return a cached security provider instance."""

    settings = get_settings()
    return SecurityProvider(settings.auth)


def get_authenticated_user(request: Request) -> AuthenticatedUser:
    """Retrieve the authenticated user from the request context."""

    security = get_security_provider()
    if not security.is_enabled:
        return security.default_admin_user()

    user = getattr(request.state, "user", None)
    if user is None:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Login required")
    return user


def require_admin_user(
    user: AuthenticatedUser = Depends(get_authenticated_user),
) -> AuthenticatedUser:
    """Ensure the current user has administrative privileges."""

    if user.role != "admin":
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Administrator access required",
        )
    return user


def require_individual_access(
    user_id: int,
    user: AuthenticatedUser = Depends(get_authenticated_user),
) -> AuthenticatedUser:
    """Ensure the user can access the requested individual dashboard."""

    if user.role == "admin":
        return user
    if user.role == "individual" and user.subject_id == user_id:
        return user
    raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Access denied")


def require_company_access(
    company_id: int,
    user: AuthenticatedUser = Depends(get_authenticated_user),
) -> AuthenticatedUser:
    """Ensure the user can access the requested company dashboard."""

    if user.role == "admin":
        return user
    if user.role == "company" and user.subject_id == company_id:
        return user
    raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Access denied")


__all__ = [
    "AuthenticatedUser",
    "AuthenticationError",
    "SecurityProvider",
    "get_security_provider",
    "get_authenticated_user",
    "require_admin_user",
    "require_company_access",
    "require_individual_access",
]
