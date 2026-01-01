"""Middleware to handle X-Forwarded-Prefix header for proxy support."""
from __future__ import annotations

from starlette.types import ASGIApp, Message, Receive, Scope, Send


class ForwardedPrefixMiddleware:
    """ASGI middleware that sets root_path from X-Forwarded-Prefix header.

    When the application runs behind a reverse proxy that mounts it at a
    subpath (e.g., /finance), this middleware reads the X-Forwarded-Prefix
    header and sets the ASGI root_path accordingly. This ensures that
    request.url_for() generates URLs with the correct prefix.
    """

    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] in ("http", "websocket"):
            headers = dict(scope.get("headers", []))
            prefix = headers.get(b"x-forwarded-prefix", b"").decode("latin-1")
            if prefix:
                scope = dict(scope)
                scope["root_path"] = prefix + scope.get("root_path", "")
        await self.app(scope, receive, send)


__all__ = ["ForwardedPrefixMiddleware"]
