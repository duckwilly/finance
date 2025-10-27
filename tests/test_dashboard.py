"""Smoke tests for the dashboard router template rendering."""
from __future__ import annotations

from decimal import Decimal

from fastapi.testclient import TestClient

from app.main import app
from app.routers.dashboard import get_admin_service, get_db_session
from app.schemas.admin import (
    AdminMetrics,
    DashboardCharts,
    LineChartData,
    ListView,
    ListViewColumn,
    ListViewRow,
    PieChartData,
)


class _StubAdminService:
    """Return a deterministic metrics payload for template rendering."""

    def __init__(self) -> None:
        self._individuals = ListView(
            title="Individual users",
            columns=[
                ListViewColumn(key="name", title="Name"),
                ListViewColumn(key="employer", title="Employer"),
                ListViewColumn(
                    key="monthly_income",
                    title="Monthly income",
                    column_type="currency",
                    align="right",
                ),
                ListViewColumn(
                    key="checking_aum",
                    title="Checking AUM",
                    column_type="currency",
                    align="right",
                ),
                ListViewColumn(
                    key="savings_aum",
                    title="Savings AUM",
                    column_type="currency",
                    align="right",
                ),
                ListViewColumn(
                    key="brokerage_aum",
                    title="Brokerage AUM",
                    column_type="currency",
                    align="right",
                ),
            ],
            rows=[
                ListViewRow(
                    key="1",
                    values={
                        "name": "Jane Example",
                        "employer": "Acme Corp",
                        "monthly_income": Decimal("4200.00"),
                        "checking_aum": Decimal("15234.56"),
                        "savings_aum": Decimal("50123.45"),
                        "brokerage_aum": Decimal("7890.12"),
                    },
                    search_text="jane example acme corp",
                )
            ],
            search_placeholder="Search individuals",
            empty_message="No individual users found.",
        )
        self._transactions = ListView(
            title="Recent transactions",
            columns=[
                ListViewColumn(key="date", title="Date"),
                ListViewColumn(key="payer", title="Payer"),
                ListViewColumn(key="payee", title="Payee"),
                ListViewColumn(key="amount", title="Amount", column_type="currency", align="right"),
                ListViewColumn(key="category", title="Category"),
                ListViewColumn(key="description", title="Description"),
            ],
            rows=[
                ListViewRow(
                    key="txn-1",
                    values={
                        "date": "2024-04-01",
                        "payer": "Jane Example",
                        "payee": "Sushi Palace",
                        "amount": Decimal("72.30"),
                        "category": "Dining",
                        "description": "Team dinner",
                    },
                    search_text="2024-04-01 jane example sushi palace dining team dinner",
                )
            ],
            search_placeholder="Search transactions",
            empty_message="No transactions found.",
        )
        self._companies = ListView(
            title="Corporate users",
            columns=[
                ListViewColumn(key="name", title="Company name"),
                ListViewColumn(key="employee_count", title="Employees", align="right"),
            ],
            rows=[
                ListViewRow(
                    key="c1",
                    values={
                        "name": "Acme Corp",
                        "employee_count": 120,
                    },
                    search_text="acme corp",
                )
            ],
            search_placeholder="Search companies",
            empty_message="No corporate users found.",
        )
        self._stock_holdings = ListView(
            title="Stock holdings",
            columns=[
                ListViewColumn(key="symbol", title="Symbol"),
                ListViewColumn(key="owner", title="Owner"),
                ListViewColumn(
                    key="market_value",
                    title="Market value",
                    column_type="currency",
                    align="right",
                ),
            ],
            rows=[
                ListViewRow(
                    key="stk-1",
                    values={
                        "symbol": "ACM",
                        "owner": "Jane Example",
                        "market_value": Decimal("4321.09"),
                    },
                    search_text="acm jane example",
                )
            ],
            search_placeholder="Search stock holdings",
            empty_message="No stock positions found.",
        )
        self._charts = DashboardCharts(
            individuals_income=PieChartData(
                title="Income distribution",
                labels=["Under €50k", "€50k-€150k", "€150k+"],
                values=[10, 20, 12],
            ),
            companies_profit_margin=PieChartData(
                title="Profit margin",
                labels=["Loss", "0-10%", "10%+"],
                values=[3, 4, 5],
            ),
            transactions_amounts=PieChartData(
                title="Transaction sizes",
                labels=["<€1k", "€1k-€5k", "€5k+"],
                values=[30, 12, 4],
            ),
            stock_price_trend=LineChartData(
                title="ACM price trend",
                labels=["Jan", "Feb", "Mar"],
                values=[10.0, 10.5, 11.2],
                series_label="Price",
            ),
        )

    def get_metrics(self, session) -> AdminMetrics:  # pragma: no cover - exercised via endpoint
        return AdminMetrics(
            total_individuals=42,
            total_companies=5,
            total_transactions=9876,
            first_transaction_at=None,
            last_transaction_at=None,
            total_aum=Decimal("1234567.89"),
        )

    def get_individual_overview(self, session) -> ListView:  # pragma: no cover - exercised via endpoint
        return self._individuals

    def get_company_overview(self, session) -> ListView:  # pragma: no cover - exercised via endpoint
        return self._companies

    def get_transaction_overview(self, session) -> ListView:  # pragma: no cover - exercised via endpoint
        return self._transactions

    def get_stock_holdings_overview(self, session) -> ListView:  # pragma: no cover - exercised via endpoint
        return self._stock_holdings

    def get_dashboard_charts(self, session) -> DashboardCharts:  # pragma: no cover - exercised via endpoint
        return self._charts


def _override_get_db_session():  # pragma: no cover - exercised via dependency
    yield None


def test_dashboard_template_renders() -> None:
    """The dashboard endpoint should respond with HTML populated by metrics."""

    app.dependency_overrides[get_admin_service] = lambda: _StubAdminService()
    app.dependency_overrides[get_db_session] = _override_get_db_session

    client = TestClient(app)
    try:
        response = client.get("/dashboard/")
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    assert "text/html" in response.headers["content-type"].lower()
    body = response.text
    assert "Finance dashboard" in body
    assert "42" in body
    assert "€ 1.2 million" in body
    assert "Jane Example" in body
    assert "Acme Corp" in body
    assert "Recent transactions" in body
    assert "Sushi Palace" in body
