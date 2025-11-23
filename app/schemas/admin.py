"""Schemas for administrator metrics responses."""
from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Any, Literal

from pydantic import BaseModel, field_serializer


class AdminMetrics(BaseModel):
    """Aggregate metrics for the admin dashboard overview."""

    total_individuals: int
    total_companies: int
    total_transactions: int
    first_transaction_at: datetime | None
    last_transaction_at: datetime | None
    total_cash: Decimal = Decimal("0")
    total_holdings: Decimal = Decimal("0")
    total_aum: Decimal

    @field_serializer("total_aum")
    def serialize_total_aum(self, value: Decimal) -> str:
        return str(value)


class ListViewColumn(BaseModel):
    """Configuration for a reusable list-view column."""

    key: str
    title: str
    column_type: Literal["text", "currency"] = "text"
    align: Literal["left", "center", "right"] = "left"


class ListViewRow(BaseModel):
    """One row of data for a list view."""

    key: str
    values: dict[str, Any]
    search_text: str | None = None
    links: dict[str, str] | None = None  # Maps column key to URL


class ListView(BaseModel):
    """Renderable list view payload for templates."""

    title: str
    columns: list[ListViewColumn]
    rows: list[ListViewRow]
    search_placeholder: str = "Search"
    empty_message: str = "No records found."


class PieChartData(BaseModel):
    """Simple representation for pie or doughnut charts."""

    title: str
    labels: list[str]
    values: list[float]
    hint: str | None = None


class LineChartData(BaseModel):
    """Representation of a single-series line chart."""

    title: str
    labels: list[str]
    values: list[float]
    series_label: str
    hint: str | None = None


class ChartSeries(BaseModel):
    """One dataset in a multi-series chart."""

    label: str
    values: list[float]


class MultiSeriesLineChartData(BaseModel):
    """Representation of a multi-series line chart."""

    title: str
    labels: list[str]
    datasets: list[ChartSeries]
    hint: str | None = None


class DashboardCharts(BaseModel):
    """Container for admin dashboard chart payloads."""

    individuals_income: PieChartData
    companies_profit_margin: PieChartData
    transactions_amounts: PieChartData
    stock_price_trend: LineChartData
