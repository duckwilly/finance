"""Database models for the finance domain."""

__all__ = [
    "Company",
    "Individual",
    "StockHolding",
    "Transaction",
    "AdminUser",
]


class _Placeholder:
    def __init_subclass__(cls) -> None:
        raise TypeError("Model stubs must be replaced with real SQLAlchemy models.")


class Company(_Placeholder):
    """Stub company model."""


class Individual(_Placeholder):
    """Stub individual model."""


class StockHolding(_Placeholder):
    """Stub stock holding model."""


class Transaction(_Placeholder):
    """Stub transaction model."""


class AdminUser(_Placeholder):
    """Stub admin model."""
