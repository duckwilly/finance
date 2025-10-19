"""Helper functions for formatting numbers and currencies."""

from __future__ import annotations
from decimal import Decimal

_UNITS = [
    (Decimal("1e12"), "trillion"),
    (Decimal("1e9"), "billion"),
    (Decimal("1e6"), "million"),
    (Decimal("1e3"), "thousand"),
]

def humanize_number(value: int | float | Decimal) -> str:
    d = Decimal(str(value))
    sign = "-" if d < 0 else ""
    d = abs(d)

    for threshold, name in _UNITS:
        if d >= threshold:
            return f"{sign}{(d / threshold):.1f} {name}"
    return f"{sign}{d.normalize() if d == d.to_integral() else d:.0f}"
    
def humanize_currency(value: int | float | Decimal, symbol: str = "$") -> str:
    return f"{symbol}{humanize_number(value)}"