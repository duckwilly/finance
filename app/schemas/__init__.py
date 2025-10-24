"""Pydantic schemas for request and response payloads."""

__all__ = [
    "CompanySchema",
    "IndividualSchema",
    "StockSchema",
    "TransactionSchema",
    "AdminSchema",
]


class _PlaceholderSchema:
    """Base class for schema placeholders."""

    def __init__(self, *_args: object, **_kwargs: object) -> None:
        raise NotImplementedError("Schemas must be implemented during development.")


class CompanySchema(_PlaceholderSchema):
    """Stub company schema."""


class IndividualSchema(_PlaceholderSchema):
    """Stub individual schema."""


class StockSchema(_PlaceholderSchema):
    """Stub stock schema."""


class TransactionSchema(_PlaceholderSchema):
    """Stub transaction schema."""


class AdminSchema(_PlaceholderSchema):
    """Stub admin schema."""
