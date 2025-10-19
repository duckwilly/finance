"""Placeholder security utilities for future authentication work."""
from __future__ import annotations


class SecurityProvider:
    """Stub implementation of an authentication/authorization provider."""

    def authenticate(self, *_args: object, **_kwargs: object) -> None:
        raise NotImplementedError("Authentication has not been implemented yet.")

    def authorize(self, *_args: object, **_kwargs: object) -> None:
        raise NotImplementedError("Authorization has not been implemented yet.")


__all__ = ["SecurityProvider"]
