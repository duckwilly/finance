import pytest

from app.ai_chatbot.chatbot_core import ToolRegistry, ToolSpec
from app.ai_chatbot.tools import ToolResult, UserScope


def test_user_scope_rejects_cross_party():
    scope = UserScope(role="person", person_id=5)

    assert scope.resolve_party_id() == 5
    with pytest.raises(PermissionError):
        scope.resolve_party_id(9)


def test_user_scope_admin_passthrough():
    scope = UserScope(role="admin")

    assert scope.resolve_party_id(42) == 42


def test_tool_registry_coerces_numeric_arguments():
    args = ToolRegistry._coerce_arguments({"days": "15", "limit": "3", "label": "foo"})

    assert args["days"] == 15
    assert args["limit"] == 3
    assert args["label"] == "foo"


def test_tool_registry_handles_permission_error():
    registry = ToolRegistry()
    registry._tools = {
        "blocked": ToolSpec(
            name="blocked",
            description="",
            handler=lambda session, scope, **kwargs: (_ for _ in ()).throw(
                PermissionError("nope")
            ),
        )
    }

    results = registry.execute_calls(
        [{"tool": "blocked", "arguments": {}}],
        {"role": "person", "person_id": 1},
        db_session=None,
    )

    assert results == []


def test_tool_registry_merges_defaults_and_arguments():
    captured: dict = {}

    def _handler(session, scope, **kwargs) -> ToolResult:
        captured.update(kwargs)
        return ToolResult(keyword="demo", title="Demo", rows=[])

    registry = ToolRegistry()
    registry._tools = {
        "demo": ToolSpec(
            name="demo",
            description="",
            handler=_handler,
            default_args={"days": 30, "limit": 5},
        )
    }

    registry.execute_calls(
        [{"tool": "demo", "arguments": {"days": "10"}}],
        {"role": "person", "person_id": 2},
        db_session=None,
    )

    assert captured["days"] == 10
    assert captured["limit"] == 5
