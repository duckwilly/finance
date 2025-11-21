"""Typed helpers shared across chatbot analytics tools."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping, Optional


def _coerce_optional_int(value: Any) -> Optional[int]:
    """Convert loosely-typed identifiers into integers when possible."""
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


@dataclass(frozen=True)
class UserScope:
    """Represents the caller's authorization context for tool execution."""

    role: str
    person_id: Optional[int] = None
    company_id: Optional[int] = None

    @classmethod
    def from_context(cls, context: Mapping[str, Any]) -> "UserScope":
        """Create a scope from the user_context structure placed on requests."""
        return cls(
            role=str(context.get("role") or "user"),
            person_id=_coerce_optional_int(context.get("person_id")),
            company_id=_coerce_optional_int(context.get("company_id")),
        )

    def resolve_party_id(self, requested_party_id: Optional[int] = None) -> Optional[int]:
        """
        Determine the party_id that should be used for a query.

        Admins may pass through a requested party id (or none for aggregate tools).
        Non-admin users are forced to their own party id and cannot override it.
        """
        if self.role == "admin":
            return requested_party_id

        scoped_id = self.company_id or self.person_id
        if scoped_id is None:
            raise PermissionError("User scope is missing a party_id for this request")

        if requested_party_id is not None and requested_party_id != scoped_id:
            raise PermissionError("You are not allowed to query data for another party")

        return scoped_id

    def require_admin(self) -> None:
        """Raise if the caller is not an admin."""
        if self.role != "admin":
            raise PermissionError("This tool is only available to admins")


@dataclass(frozen=True)
class ToolResult:
    """Normalized structure returned by analytics tools."""

    keyword: str
    title: str
    rows: list[dict[str, Any]]
    chart_type: Optional[str] = None
    x_axis: Optional[str] = None
    y_axis: Optional[Any] = None  # str or list[str]
    stack_by: Optional[str] = None
    unit: Optional[str] = None
    sort: Optional[str] = None

    @property
    def has_data(self) -> bool:
        return bool(self.rows)


__all__ = ["ToolResult", "UserScope"]
