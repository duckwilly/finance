"""Simple JWT-backed authentication helpers used by the demo."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from functools import lru_cache

import jwt
from fastapi import Depends, HTTPException, Request, status
from jwt import ExpiredSignatureError, InvalidTokenError

from app.core.config import AuthSettings, get_settings
from app.db.session import get_sessionmaker
from app.models import (
    AppUser,
    CompanyAccessGrant,
    EmploymentContract,
    OrgPartyMap,
    UserPartyMap,
)
from sqlalchemy import or_, select
from sqlalchemy.orm import selectinload


class AuthenticationError(Exception):
    """Raised when authentication or token validation fails."""

# Immutable dataclass for user authentication
@dataclass(frozen=True, slots=True)
class AuthenticatedUser:
    """Representation of the authenticated principal."""

    username: str
    role: str
    subject_id: int | None = None
    app_user_id: int | None = None
    party_id: int | None = None
    roles: tuple[str, ...] = ()
    company_ids: tuple[int, ...] = ()


class SecurityProvider:
    """Authenticate demo users and issue/verify JWT access tokens."""

    def __init__(self, settings: AuthSettings, session_factory=None) -> None:
        self._settings = settings
        self._session_factory = session_factory or get_sessionmaker()

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
        return AuthenticatedUser(
            username=self._settings.admin_username,
            role="admin",
            roles=("ADMIN",),
            company_ids=(),
        )

    def authenticate(self, username: str, password: str) -> AuthenticatedUser | None:
        """Validate the supplied credentials and return an ``AuthenticatedUser``."""

        # Check admin authentication first
        if (
            username == self._settings.admin_username
            and password == self._settings.admin_password
        ):
            return AuthenticatedUser(username=username, role="admin")

        # Check demo password for all other accounts
        if password != self._settings.demo_user_password:
            return None

        with self._session_factory() as session:
            base_query = select(AppUser).options(selectinload(AppUser.roles))
            app_user = session.execute(base_query.where(AppUser.username == username)).scalars().first()

            fallback_user_id: int | None = None

            if not app_user and username.startswith("u") and username[1:].isdigit():
                fallback_user_id = int(username[1:])
                mapped_party_id = (
                    session.execute(
                        select(UserPartyMap.party_id).where(UserPartyMap.user_id == fallback_user_id)
                    )
                    .scalar_one_or_none()
                )
                if mapped_party_id is not None:
                    app_user = (
                        session.execute(base_query.where(AppUser.party_id == mapped_party_id))
                        .scalars()
                        .first()
                    )

            if not app_user or not app_user.is_active:
                return None

            role_codes = tuple(sorted(role.role_code for role in app_user.roles))
            party_id = app_user.party_id

            individual_id = None
            if party_id is not None:
                individual_id = (
                    session.execute(
                        select(UserPartyMap.user_id).where(UserPartyMap.party_id == party_id)
                    ).scalar_one_or_none()
                )
            if individual_id is None and fallback_user_id is not None:
                individual_id = fallback_user_id

            company_ids: set[int] = set()
            if party_id is not None:
                employment_rows = session.execute(
                    select(
                        EmploymentContract.employer_party_id.label("employer_party_id"),
                        OrgPartyMap.org_id.label("company_id"),
                    )
                    .outerjoin(OrgPartyMap, OrgPartyMap.party_id == EmploymentContract.employer_party_id)
                    .where(
                        EmploymentContract.employee_party_id == party_id,
                        EmploymentContract.start_date <= date.today(),
                        or_(
                            EmploymentContract.end_date.is_(None),
                            EmploymentContract.end_date >= date.today(),
                        ),
                    )
                ).all()
                for row in employment_rows:
                    if row.company_id is not None:
                        company_ids.add(int(row.company_id))
                    if row.employer_party_id is not None:
                        company_ids.add(int(row.employer_party_id))

            if "ADMIN" not in role_codes:
                grant_rows = session.execute(
                    select(OrgPartyMap.org_id)
                    .join(
                        EmploymentContract,
                        EmploymentContract.employer_party_id == OrgPartyMap.party_id,
                    )
                    .join(
                        CompanyAccessGrant,
                        CompanyAccessGrant.contract_id == EmploymentContract.id,
                    )
                    .where(
                        CompanyAccessGrant.app_user_id == app_user.id,
                        CompanyAccessGrant.revoked_at.is_(None),
                    )
                ).scalars()
                company_ids.update(grant_rows)

            # Fall back to individual role when no admin privileges present
            resolved_role = "admin" if "ADMIN" in role_codes else "individual"
            subject_id = individual_id

            return AuthenticatedUser(
                username=app_user.username,
                role=resolved_role,
                subject_id=subject_id,
                app_user_id=app_user.id,
                party_id=party_id,
                roles=role_codes,
                company_ids=tuple(sorted(company_ids)),
            )

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
        if user.app_user_id is not None:
            payload["app_user_id"] = user.app_user_id
        if user.party_id is not None:
            payload["party_id"] = user.party_id
        if user.roles:
            payload["roles"] = list(user.roles)
        if user.company_ids:
            payload["company_ids"] = list(user.company_ids)
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
        app_user_id = payload.get("app_user_id")
        party_id = payload.get("party_id")
        roles_payload = payload.get("roles")
        company_ids_payload = payload.get("company_ids")

        resolved_app_user: int | None = None
        if app_user_id is not None:
            try:
                resolved_app_user = int(app_user_id)
            except (TypeError, ValueError) as exc:  # pragma: no cover - runtime safeguard
                raise AuthenticationError("Token app_user_id invalid") from exc

        resolved_party: int | None = None
        if party_id is not None:
            try:
                resolved_party = int(party_id)
            except (TypeError, ValueError) as exc:  # pragma: no cover - runtime safeguard
                raise AuthenticationError("Token party_id invalid") from exc

        resolved_roles: tuple[str, ...] = ()
        if isinstance(roles_payload, list) and all(isinstance(r, str) for r in roles_payload):
            resolved_roles = tuple(roles_payload)

        resolved_company_ids: tuple[int, ...] = ()
        if isinstance(company_ids_payload, list):
            company_values: list[int] = []
            for value in company_ids_payload:
                try:
                    company_values.append(int(value))
                except (TypeError, ValueError) as exc:  # pragma: no cover - runtime safeguard
                    raise AuthenticationError("Token company_ids invalid") from exc
            resolved_company_ids = tuple(sorted(set(company_values)))

        return AuthenticatedUser(
            username=username,
            role=role,
            subject_id=resolved_subject,
            app_user_id=resolved_app_user,
            party_id=resolved_party,
            roles=resolved_roles,
            company_ids=resolved_company_ids,
        )


@lru_cache(maxsize=1)
def get_security_provider() -> SecurityProvider:
    """Return a cached security provider instance."""

    settings = get_settings()
    session_factory = get_sessionmaker()
    return SecurityProvider(settings.auth, session_factory)


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

    if "ADMIN" in user.roles or user.role == "admin":
        return user
    if user.subject_id == user_id:
        return user
    raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Access denied")


def require_company_access(
    company_id: int,
    user: AuthenticatedUser = Depends(get_authenticated_user),
) -> AuthenticatedUser:
    """Ensure the user can access the requested company dashboard."""

    if "ADMIN" in user.roles or user.role == "admin":
        return user
    if company_id in user.company_ids:
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
