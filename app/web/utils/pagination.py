"""Shared helpers for pagination and query parameter parsing."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Iterable, Mapping, Sequence

from starlette.datastructures import QueryParams
from urllib.parse import urlencode


DEFAULT_PAGE_SIZE_OPTIONS: tuple[int, ...] = (10, 20, 50)


@dataclass(frozen=True)
class PaginationLinks:
    """Navigation metadata for paginated views."""

    has_previous: bool
    has_next: bool
    prev_url: str | None
    next_url: str | None


def parse_positive_int(value: str | None, *, default: int) -> int:
    """Parse a positive integer from the provided string.

    Any invalid or non-positive values will fall back to ``default``.
    """

    try:
        parsed = int(value) if value is not None else default
    except (TypeError, ValueError):
        return default
    return parsed if parsed > 0 else default


def normalize_page_size(
    value: str | None,
    *,
    options: Sequence[int] | None = None,
) -> int:
    """Return the closest allowed page size for the requested value."""

    allowed: Sequence[int] = options or DEFAULT_PAGE_SIZE_OPTIONS
    try:
        parsed = int(value) if value is not None else allowed[1]
    except (TypeError, ValueError):
        return allowed[1]

    for option in allowed:
        if parsed <= option:
            return option
    return allowed[-1]


def parse_iso_date(value: str | None) -> date | None:
    """Parse an ISO formatted date string into a ``date`` instance."""

    if value is None or value == "":
        return None
    try:
        return date.fromisoformat(value)
    except ValueError:
        return None


def build_pagination_links(
    query_params: QueryParams | Mapping[str, str] | Iterable[tuple[str, str]],
    *,
    page: int,
    total_pages: int,
    page_param: str = "page",
) -> PaginationLinks:
    """Construct navigation URLs for paginated listings."""

    if isinstance(query_params, QueryParams):
        items = query_params.multi_items()
    elif isinstance(query_params, Mapping):
        items = list(query_params.items())
    else:
        items = list(query_params)

    preserved_params = {key: value for key, value in items if key != page_param}

    has_previous = page > 1
    has_next = page < total_pages

    prev_url = None
    if has_previous:
        prev_params = dict(preserved_params)
        prev_params[page_param] = str(page - 1)
        prev_url = f"?{urlencode(prev_params)}"

    next_url = None
    if has_next:
        next_params = dict(preserved_params)
        next_params[page_param] = str(page + 1)
        next_url = f"?{urlencode(next_params)}"

    return PaginationLinks(
        has_previous=has_previous,
        has_next=has_next,
        prev_url=prev_url,
        next_url=next_url,
    )
