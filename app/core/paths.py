from __future__ import annotations

from starlette.requests import Request


def root_path(request: Request) -> str:
    """Return the ASGI root path without a trailing slash."""

    value = request.scope.get("root_path", "") or ""
    return value.rstrip("/") if value != "/" else ""


def with_root_path(request: Request, path: str) -> str:
    """Prefix a path with the root path when running behind a proxy."""

    root = root_path(request)
    if not root:
        return path
    if path == root or path.startswith(f"{root}/"):
        return path
    if not path.startswith("/"):
        path = f"/{path}"
    return f"{root}{path}"


def cookie_path(request: Request) -> str:
    """Scope cookies to the app root when mounted under a prefix."""

    root = root_path(request)
    return root or "/"
