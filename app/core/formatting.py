"""Helper functions for formatting numbers and currencies."""

from __future__ import annotations
from decimal import Decimal

_UNITS = [
    (Decimal("1e12"), "trillion", "T"),
    (Decimal("1e9"), "billion", "B"),
    (Decimal("1e6"), "million", "M"),
    (Decimal("1e3"), "thousand", "k"),
]

def humanize_number(
    value: int | float | Decimal,
    short: bool = False,
    decimals: int = 1
) -> str:
    """Format a number with human-readable units.
    
    Args:
        value: The number to format
        short: If True, use short suffixes (k, M, B, T) instead of full words
        decimals: Number of decimal places to show
    """
    d = Decimal(str(value))
    sign = "-" if d < 0 else ""
    d = abs(d)

    def _format_plain_number() -> str:
        if d == d.to_integral():
            whole = format(d, "f")
            if "." in whole:
                whole = whole.rstrip("0").rstrip(".") or "0"
            return f"{sign}{whole}"
        return f"{sign}{d:.{decimals}f}"

    if d < Decimal("1e4"):
        return _format_plain_number()

    for threshold, long_name, short_name in _UNITS:
        if d >= threshold:
            if short:
                return f"{sign}{(d / threshold):.{decimals}f}{short_name}"
            return f"{sign}{(d / threshold):.{decimals}f} {long_name}"

    return _format_plain_number()
    
def humanize_currency(
    value: int | float | Decimal, 
    symbol: str = "€", 
    short: bool = False, 
    decimals: int = 1
) -> str:
    """Format a currency value with human-readable units.
    
    Args:
        value: The currency amount to format
        symbol: Currency symbol to use (default: €)
        short: If True, use short suffixes (k, M, B, T) instead of full words
        decimals: Number of decimal places to show
    """
    return f"{symbol} {humanize_number(value, short=short, decimals=decimals)}"
