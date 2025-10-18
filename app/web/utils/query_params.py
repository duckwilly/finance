from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Mapping, Sequence

from starlette.datastructures import QueryParams

from .pagination import DEFAULT_PAGE_SIZE_OPTIONS

ParamsMapping = Mapping[str, str] | QueryParams


@dataclass(frozen=True)
class PaginationParams:
    page: int
    page_size: int


def parse_positive_int(value: str | None, *, default: int) -> int:
    try:
        parsed = int(value) if value is not None else default
    except (TypeError, ValueError):
        return default
    return parsed if parsed > 0 else default


def normalize_page_size(
    value: str | None,
    *,
    options: Sequence[int] = DEFAULT_PAGE_SIZE_OPTIONS,
) -> int:
    try:
        parsed = int(value) if value is not None else options[1]
    except (TypeError, ValueError):
        return options[1]

    for option in options:
        if parsed <= option:
            return option
    return options[-1]


def parse_iso_date(value: str | None) -> date | None:
    if not value:
        return None
    try:
        return date.fromisoformat(value)
    except ValueError:
        return None


def extract_pagination(
    params: ParamsMapping,
    *,
    page_param: str = "page",
    page_size_param: str = "page_size",
    default_page: int = 1,
    page_size_options: Sequence[int] = DEFAULT_PAGE_SIZE_OPTIONS,
) -> PaginationParams:
    page = parse_positive_int(params.get(page_param), default=default_page)
    page_size = normalize_page_size(
        params.get(page_size_param), options=page_size_options
    )
    return PaginationParams(page=page, page_size=page_size)


def extract_search_term(
    params: ParamsMapping,
    *,
    key: str = "search",
) -> str | None:
    value = params.get(key)
    if not value:
        return None
    stripped = value.strip()
    return stripped or None
