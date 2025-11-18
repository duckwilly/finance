import pytest

from ai_chatbot.backend.sql_generator import SQLGenerator


@pytest.fixture()
def sql_generator():
    return SQLGenerator()


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
