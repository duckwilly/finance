import pytest

from ai_chatbot.backend.sql_generator import QuickTemplateManager, SQLGenerator


@pytest.fixture()
def sql_generator():
    return SQLGenerator()


@pytest.fixture()
def template_manager():
    return QuickTemplateManager()


def test_enforce_scope_appends_filter_without_where(sql_generator):
    sql = "SELECT * FROM account a"
    user_context = {"role": "person", "person_id": 7}

    scoped_sql = sql_generator.enforce_scope_constraints(sql, user_context)

    assert scoped_sql.endswith("WHERE a.party_id = 7")


def test_enforce_scope_appends_filter_with_existing_where(sql_generator):
    sql = "SELECT * FROM account a WHERE a.name = 'Checking'"
    user_context = {"role": "person", "person_id": 7}

    scoped_sql = sql_generator.enforce_scope_constraints(sql, user_context)

    assert scoped_sql.endswith("WHERE a.name = 'Checking' AND a.party_id = 7")


def test_enforce_scope_respects_existing_party_filter(sql_generator):
    sql = "SELECT * FROM account a WHERE a.party_id = 7"
    user_context = {"role": "person", "person_id": 7}

    scoped_sql = sql_generator.enforce_scope_constraints(sql, user_context)

    assert scoped_sql == sql


def test_enforce_scope_rejects_mismatched_selector(sql_generator):
    sql = "SELECT * FROM account a WHERE party_id = 9"
    user_context = {"role": "person", "person_id": 7}

    with pytest.raises(ValueError):
        sql_generator.enforce_scope_constraints(sql, user_context)


def test_enforce_scope_requires_identifiers(sql_generator):
    sql = "SELECT * FROM account a"
    user_context = {"role": "person"}

    with pytest.raises(ValueError):
        sql_generator.enforce_scope_constraints(sql, user_context)


def test_admin_allows_other_party_selectors(sql_generator):
    sql = "SELECT * FROM account a WHERE party_id = 9"
    user_context = {"role": "admin"}

    scoped_sql = sql_generator.enforce_scope_constraints(sql, user_context)

    assert scoped_sql == sql


def test_category_template_injects_scope_and_category(template_manager):
    template = template_manager.render_template(
        "Show my spend on groceries", {"role": "person", "person_id": 5}
    )

    assert template is not None
    assert "c.name LIKE :category_name" in template["sql"]
    assert template["params"]["category_name"] == "%groceries%"
    assert "a.party_id = 5" in template["sql"]


def test_trend_narrative_reports_direction(template_manager):
    narrative = template_manager.build_trend_narrative(
        "monthly_expense_trend",
        [
            {"month": "2024-05", "monthly_total": 1000},
            {"month": "2024-06", "monthly_total": 1250},
        ],
    )

    assert narrative is not None
    assert "increased" in narrative.lower()
