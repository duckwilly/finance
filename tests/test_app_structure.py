from fastapi import FastAPI

from app.core.config import get_settings
from app.main import create_app


def test_create_app_registers_dashboard_router() -> None:
    app = create_app()
    assert isinstance(app, FastAPI)
    paths = {route.path for route in app.routes}
    assert "/dashboard/" in paths


def test_get_settings_uses_default_configuration(monkeypatch) -> None:
    monkeypatch.delenv("DB_HOST", raising=False)
    monkeypatch.delenv("DB_PORT", raising=False)
    monkeypatch.delenv("DB_USER", raising=False)
    monkeypatch.delenv("DB_PASSWORD", raising=False)
    monkeypatch.delenv("DB_NAME", raising=False)
    monkeypatch.delenv("SQLALCHEMY_ECHO", raising=False)

    settings = get_settings()

    assert settings.database.host == "127.0.0.1"
    assert settings.database.port == 3306
    assert settings.database.user == "finance"
    assert settings.database.password == "finance"
    assert settings.database.name == "finance"
    assert settings.sqlalchemy_echo is False
