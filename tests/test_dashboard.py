"""Smoke tests for the dashboard router template rendering."""
from __future__ import annotations

from decimal import Decimal

from fastapi.testclient import TestClient

from app.main import app
from app.routers.dashboard import get_admin_service, get_db_session
from app.schemas.admin import AdminMetrics


class _StubAdminService:
    """Return a deterministic metrics payload for template rendering."""

    def get_metrics(self, session) -> AdminMetrics:  # pragma: no cover - exercised via endpoint
        return AdminMetrics(
            total_individuals=42,
            total_companies=5,
            total_transactions=9876,
            first_transaction_at=None,
            last_transaction_at=None,
            total_aum=Decimal("1234567.89"),
        )


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
    assert "Operations overview" in body
    assert "42" in body
    assert "$1234567.89" in body
