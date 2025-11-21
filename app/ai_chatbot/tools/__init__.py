"""Analytics tool entrypoints exposed to the chatbot."""
from .analytics import (
    expenses_by_category,
    income_by_category,
    monthly_cash_flow_comparison,
    spending_trend,
    top_spenders,
)
from .types import ToolResult, UserScope

__all__ = [
    "expenses_by_category",
    "income_by_category",
    "monthly_cash_flow_comparison",
    "spending_trend",
    "top_spenders",
    "ToolResult",
    "UserScope",
]
