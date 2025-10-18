from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Mapping

from starlette.datastructures import QueryParams
from urllib.parse import urlencode

DEFAULT_PAGE_SIZE_OPTIONS: tuple[int, ...] = (10, 20, 50)


@dataclass(frozen=True)
class PaginationLinks:
    has_previous: bool
    has_next: bool
    prev_url: str | None
    next_url: str | None


def build_pagination_links(
    query_params: QueryParams | Mapping[str, str] | Iterable[tuple[str, str]],
    *,
    page: int,
    total_pages: int,
    page_param: str = "page",
) -> PaginationLinks:
    if isinstance(query_params, QueryParams):
        items = query_params.multi_items()
    elif isinstance(query_params, Mapping):
        items = query_params.items()
    else:
        items = query_params

    preserved = {key: value for key, value in items if key != page_param}

    has_previous = page > 1
    has_next = page < total_pages

    prev_url = None
    if has_previous:
        prev_params = dict(preserved)
        prev_params[page_param] = str(page - 1)
        prev_url = f"?{urlencode(prev_params)}"

    next_url = None
    if has_next:
        next_params = dict(preserved)
        next_params[page_param] = str(page + 1)
        next_url = f"?{urlencode(next_params)}"

    return PaginationLinks(
        has_previous=has_previous,
        has_next=has_next,
        prev_url=prev_url,
        next_url=next_url,
    )
