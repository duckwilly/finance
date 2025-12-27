"""Application middleware to enforce login requirements."""
from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Iterable

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import RedirectResponse, Response

from app.core.logger import get_logger
from app.core.security import AuthenticationError, AuthenticatedUser, SecurityProvider

LOGGER = get_logger(__name__)


class AuthMiddleware(BaseHTTPMiddleware):
    """Redirect unauthenticated requests to the login page."""

    def __init__(
        self,
        app,
        security_provider: SecurityProvider,
        *,
        login_path: str = "/login",
        logout_path: str = "/logout",
        exempt_paths: Iterable[str] | None = None,
        exempt_prefixes: Iterable[str] | None = None,
    ) -> None:
        super().__init__(app)
        self._security_provider = security_provider
        self._login_path = login_path
        self._logout_path = logout_path
        self._exempt_paths = set(exempt_paths or ()) | {login_path, logout_path}
        self._exempt_prefixes = tuple(exempt_prefixes or ("/static",))

    def _is_exempt(self, path: str) -> bool:
        """Return ``True`` when the request path should bypass authentication."""

        if path in self._exempt_paths:
            return True
        for prefix in self._exempt_prefixes:
            if path.startswith(prefix):
                return True
        return path in {"/openapi.json", "/docs", "/redoc", "/favicon.ico"}

    async def dispatch(
        self, request: Request, call_next: Callable[[Request], Awaitable[Response]]
    ) -> Response:
        token = request.cookies.get(self._security_provider.cookie_name)
        user: AuthenticatedUser | None = None
        invalid_token = False

        if token:
            try:
                user = self._security_provider.decode_token(token)
            except AuthenticationError as exc:
                LOGGER.info("Failed to decode access token", extra={"reason": str(exc)})
                invalid_token = True

        request.state.user = user
        path = request.url.path

        if self._is_exempt(path):
            if invalid_token:
                response = RedirectResponse(self._login_path, status_code=303)
                response.delete_cookie(self._security_provider.cookie_name)
                return response
            return await call_next(request)

        if user is None:
            response = RedirectResponse(self._login_path, status_code=303)
            if token:
                response.delete_cookie(self._security_provider.cookie_name)
            return response

        return await call_next(request)


__all__ = ["AuthMiddleware"]
