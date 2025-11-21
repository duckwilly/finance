"""
SQL Query Generation and Validation Module
Converts natural language to SQL using LLMs with security validation
"""
import re
import json
import logging
from decimal import Decimal
from typing import Any, Dict, Optional
from sqlalchemy import text
from sqlalchemy.orm import Session

from .llm_providers import LLMProviderFactory
from .config import chatbot_config

logger = logging.getLogger(__name__)


class SQLGenerator:
    """Generates and validates SQL queries from natural language"""

    # Default database schema for financial applications
    DEFAULT_SCHEMA = """
Authoritative Database Schema (MariaDB/MySQL)
- party: id PK, party_type ENUM('INDIVIDUAL','COMPANY'), display_name, created_at
- individual_profile: party_id PK/FK->party, given_name, family_name, primary_email
- company_profile: party_id PK/FK->party, legal_name, registration_number, tax_identifier
- app_user: id PK, party_id FK->party, username (UNIQUE), email (UNIQUE), is_active; app_user_role links to app_role
- account: id PK, party_id FK->party (owner), account_type_code FK, currency_code FK, name, opened_at, closed_at
- journal_entry: id PK, entry_code UNIQUE, txn_date, posted_at, description, channel_code FK->txn_channel, counterparty_party_id FK->party
- journal_line: id PK, entry_id FK->journal_entry, account_id FK->account, party_id FK->party (line-level counterparty), amount DECIMAL, currency_code FK, category_id FK->category, created_at
- category: id PK, section_id FK->section (income/expense/transfer), name; section has values income/expense/transfer
- Access & roles: account_party_role links account_id to additional party_id with role_code; employment_contract joins employee_party_id to employer_party_id; company_access_grant grants app_user_id to contract_id with role_code

Identity & Join Paths
- User identity: app_user.party_id -> party.id; individual vs company determined by party.party_type
- Company identity: company_profile.party_id -> party.id (party_type='COMPANY')
- Account ownership: account.party_id -> party.id (owner). Additional access via account_party_role.party_id
- Transactions: journal_line.account_id -> account.id -> account.party_id -> party (owner)
- Category classification: journal_line.category_id -> category.id -> section.id/name
"""

    def __init__(self, database_schema: str = None):
        """
        Initialize SQL Generator

        Args:
            database_schema: Custom database schema description (uses default if None)
        """
        self.database_schema = database_schema or self.DEFAULT_SCHEMA

    def build_system_prompt(self, user_context: Dict[str, Any]) -> str:
        """
        Build system prompt for LLM with schema and security context

        Args:
            user_context: Dict with 'role', 'person_id', 'company_id'

        Returns:
            System prompt string
        """
        role = user_context.get("role", "user")
        person_id = user_context.get("person_id")
        company_id = user_context.get("company_id")

        # Build role-based filter guidance
        filter_guidance = ""
        if role == "person" and person_id:
            filter_guidance = (
                "\nSelf-scope: Add 'AND a.party_id = {person_id}' to restrict to the"
                " signed-in individual's accounts."
            )
        elif role == "company" and company_id:
            filter_guidance = (
                "\nSelf-scope: Add 'AND a.party_id = {company_id}' to restrict to the"
                " active company accounts."
            )
        elif role == "admin":
            filter_guidance = (
                "\nAdmin scope: You may omit account-party filters when explicitly"
                " looking up other users/companies."
            )

        return f"""You are a SQL query generator for a MariaDB/MySQL financial database.

{self.database_schema}

Allowed lookup patterns (keep queries to SELECT only):
- Admin user lookup: query app_user au joined to party p on au.party_id = p.id and filter by id/username/email (e.g. "WHERE au.id = <id>" or "WHERE au.username LIKE '%<name>%'" or "WHERE au.email LIKE '%<name>%'")
- Admin company lookup: query party p with p.party_type = 'COMPANY' (optionally JOIN company_profile cp ON cp.party_id = p.id) and filter by "p.id = <company_id>" or name matches like "p.display_name LIKE '%<name>%'" or "cp.legal_name LIKE '%<name>%'"
- Self individual scope: filter owned accounts with "AND a.party_id = {person_id}" (party_type='INDIVIDUAL')
- Self company scope: filter owned accounts with "AND a.party_id = {company_id}" (party_type='COMPANY')
- Insight templates the assistant may answer directly: 30-day and quarter-to-date summaries, category-level spend/earn questions (with c.name filters), and monthly trends that include rolling averages for income or expenses.

CRITICAL Rules:
1. Generate ONLY valid SELECT queries
2. This is MariaDB/MySQL - use CURDATE(), DATE_SUB(), DATE_FORMAT(), YEAR(), MONTH()
3. NEVER use SQLite date functions like date('now') or date('now', '-1 month')
4. Use proper table aliases (jl for journal_line, je for journal_entry, a for account, c for category, s for section)
5. Join tables properly with foreign keys as shown in schema
6. Return results in JSON format with 'sql' and 'explanation' keys
7. For amounts, use SUM(ABS(jl.amount)) for totals
8. ORDER BY most relevant column (usually total DESC or date DESC)
9. LIMIT results to 100 unless user requests more
{filter_guidance}

Response format:
{{
    "sql": "SELECT ... FROM ...",
    "explanation": "Brief description of what the query returns"
}}"""

    async def generate_sql(
        self,
        question: str,
        provider_name: str,
        user_context: Dict[str, Any],
        financial_summary: Optional[str] = None,
        conversation_history: Optional[list] = None
    ) -> Dict[str, Any]:
        """
        Generate SQL query from natural language question

        Args:
            question: User's natural language question
            provider_name: LLM provider to use (e.g., 'claude-haiku-4-5-20251001')
            user_context: User's role, person_id, company_id
            financial_summary: Optional RAG context about user's finances
            conversation_history: Previous conversation messages

        Returns:
            Dict with 'sql', 'explanation', 'provider', 'model'
        """
        # Create LLM provider
        provider = LLMProviderFactory.create(provider_name)

        # Build system prompt
        system_prompt = self.build_system_prompt(user_context)

        # Build user prompt with optional RAG context
        user_prompt = f"Question: {question}"
        if financial_summary:
            user_prompt = f"User's Financial Context:\n{financial_summary}\n\n{user_prompt}"

        # Query LLM
        try:
            response = await provider.query(
                system_prompt=system_prompt,
                user_prompt=user_prompt,
                conversation_history=conversation_history,
                json_mode=True
            )

            # Parse JSON response
            content = response["content"]
            try:
                result = json.loads(content)
            except json.JSONDecodeError:
                # Try to extract JSON from response
                json_match = re.search(r'\{.*\}', content, re.DOTALL)
                if json_match:
                    result = json.loads(json_match.group())
                else:
                    raise ValueError("LLM did not return valid JSON")

            sql_query = result.get("sql", "")
            explanation = result.get("explanation", "")

            # Validate SQL
            self.validate_sql(sql_query)

            # Fix SQL parameter syntax for SQLAlchemy
            sql_query = self.fix_sql_parameters(sql_query, user_context)

            # Enforce scope-based filtering and selector validation
            sql_query = self.enforce_scope_constraints(sql_query, user_context)

            return {
                "sql": sql_query,
                "explanation": explanation,
                "provider": response.get("provider"),
                "model": response.get("model")
            }

        except Exception as e:
            logger.error(f"SQL generation failed: {str(e)}")
            raise

    def validate_sql(self, sql: str) -> None:
        """
        Validate SQL query for security

        Args:
            sql: SQL query string

        Raises:
            ValueError: If SQL contains dangerous operations
        """
        sql_upper = sql.upper()

        # Check only SELECT allowed
        if not sql_upper.strip().startswith("SELECT"):
            raise ValueError("Only SELECT queries are allowed")

        # Check for dangerous keywords
        for keyword in chatbot_config.blocked_sql_keywords:
            if keyword in sql_upper:
                raise ValueError(f"Dangerous SQL keyword detected: {keyword}")

    def fix_sql_parameters(self, sql: str, user_context: Dict[str, Any]) -> str:
        """
        Fix SQL parameter syntax and apply security filters

        Args:
            sql: Raw SQL query
            user_context: User's role and IDs

        Returns:
            Fixed SQL query
        """
        # Fix parameter syntax: {param} -> :param
        sql = re.sub(r'\{(\w+)\}', r':\1', sql)

        # Remove admin filters for admin users
        if user_context.get("role") == "admin":
            sql = re.sub(r'\s+AND\s+a\.party_id\s*=\s*:?\w+', '', sql, flags=re.IGNORECASE)
            sql = re.sub(r'\s+AND\s+a\.party_id\s*=\s*\d+', '', sql, flags=re.IGNORECASE)

        return sql

    def enforce_scope_constraints(self, sql: str, user_context: Dict[str, Any]) -> str:
        """
        Apply required party filters and validate selector usage based on role.

        Non-admin users must be restricted to their own party scope. Queries that
        attempt to select other parties or omit the required party filter will be
        rejected or amended to include the current user's scope.
        """

        role = user_context.get("role")

        if role == "admin":
            return sql

        scope_id = None
        if role == "person":
            scope_id = user_context.get("person_id")
        elif role == "company":
            scope_id = user_context.get("company_id")

        if not scope_id:
            raise ValueError("User scope could not be determined for this query")

        self._validate_selector_scope(sql, scope_id)

        if self._has_party_filter(sql, scope_id):
            return sql

        return self._append_party_filter(sql, scope_id)

    def _validate_selector_scope(self, sql: str, scope_id: Any) -> None:
        """Reject queries that target other parties for non-admin users."""

        selector_patterns = [
            r"party_id\s*(=|IN)\s*([^\s;]+)",
            r"person_id\s*(=|IN)\s*([^\s;]+)",
            r"company_id\s*(=|IN)\s*([^\s;]+)",
            r"p\.id\s*(=|IN)\s*([^\s;]+)",
        ]

        allowed_placeholders = re.compile(r":(person_id|company_id|party_id)", re.IGNORECASE)

        for pattern in selector_patterns:
            for match in re.finditer(pattern, sql, re.IGNORECASE):
                value_fragment = match.group(2)
                if str(scope_id) in value_fragment:
                    continue
                if allowed_placeholders.search(value_fragment):
                    continue
                raise ValueError("Query contains unauthorized person/company selector")

    def _has_party_filter(self, sql: str, scope_id: Any) -> bool:
        """Check if the query already filters on the current party scope."""

        for match in re.finditer(r"a\.party_id\s*(=|IN)\s*([^\s;]+)", sql, re.IGNORECASE):
            value_fragment = match.group(2)
            if str(scope_id) in value_fragment:
                return True
            if re.search(r":(person_id|company_id|party_id)", value_fragment, re.IGNORECASE):
                return True
        return False

    def _append_party_filter(self, sql: str, scope_id: Any) -> str:
        """Append a party filter to the query, preserving existing WHERE clauses."""

        if re.search(r"\bWHERE\b", sql, re.IGNORECASE):
            return f"{sql} AND a.party_id = {scope_id}"

        return f"{sql} WHERE a.party_id = {scope_id}"

    def execute_sql(
        self,
        sql: str,
        db_session: Session,
        params: Optional[Dict[str, Any]] = None
    ) -> list:
        """
        Execute SQL query against database

        Args:
            sql: SQL query string
            db_session: SQLAlchemy database session
            params: Optional query parameters

        Returns:
            List of result rows as dictionaries
        """
        try:
            result = db_session.execute(text(sql), params or {})
            rows = result.fetchall()

            # Convert to list of dicts
            if rows:
                columns = result.keys()
                return [self._convert_decimals(dict(zip(columns, row))) for row in rows]
            return []

        except Exception as e:
            logger.error(f"SQL execution failed: {str(e)}")
            raise ValueError(f"Database query failed: {str(e)}")

    def _convert_decimals(self, row: Dict[str, Any]) -> Dict[str, Any]:
        """
        Convert Decimal objects to float for JSON serialization

        Args:
            row: Dictionary that may contain Decimal values

        Returns:
            Dictionary with Decimals converted to floats
        """
        return {
            key: float(value) if isinstance(value, Decimal) else value
            for key, value in row.items()
        }


class QuickTemplateManager:
    """Manages quick SQL templates for common questions"""

    def __init__(self):
        self.templates = self._build_templates()

    def describe_templates(self) -> list[dict]:
        """Return keyword/description pairs for prompt construction."""

        return [
            {
                "keyword": name,
                "description": template.get("explanation", ""),
            }
            for name, template in self.templates.items()
        ]

    def _build_templates(self) -> Dict[str, Dict[str, Any]]:
        """Build quick template dictionary"""
        return {
            "expenses_by_category": {
                "keywords": ["expenses by category", "spending by category", "where did i spend"],
                "sql": """
                    SELECT c.name as category, SUM(ABS(jl.amount)) as total
                    FROM journal_line jl
                    JOIN journal_entry je ON jl.entry_id = je.id
                    JOIN account a ON jl.account_id = a.id
                    LEFT JOIN category c ON jl.category_id = c.id
                    LEFT JOIN section s ON c.section_id = s.id
                    WHERE s.name = 'expense' AND c.name IS NOT NULL {filter}
                    GROUP BY c.name
                    ORDER BY total DESC
                    LIMIT 10
                """,
                "explanation": "Total expenses grouped by category",
            },
            "income_by_category": {
                "keywords": ["income by category", "revenue by category", "where did my money come from"],
                "sql": """
                    SELECT c.name as category, SUM(ABS(jl.amount)) as total
                    FROM journal_line jl
                    JOIN journal_entry je ON jl.entry_id = je.id
                    JOIN account a ON jl.account_id = a.id
                    LEFT JOIN category c ON jl.category_id = c.id
                    LEFT JOIN section s ON c.section_id = s.id
                    WHERE s.name = 'income' AND c.name IS NOT NULL {filter}
                    GROUP BY c.name
                    ORDER BY total DESC
                    LIMIT 10
                """,
                "explanation": "Total income grouped by category",
            },
            "monthly_comparison": {
                "keywords": ["monthly income vs expenses", "income vs expenses by month"],
                "sql": """
                    SELECT
                        DATE_FORMAT(je.txn_date, '%Y-%m') as month,
                        SUM(CASE WHEN s.name = 'income' THEN ABS(jl.amount) ELSE 0 END) as income_total,
                        SUM(CASE WHEN s.name = 'expense' THEN ABS(jl.amount) ELSE 0 END) as expenses
                    FROM journal_line jl
                    JOIN journal_entry je ON jl.entry_id = je.id
                    JOIN account a ON jl.account_id = a.id
                    LEFT JOIN category c ON jl.category_id = c.id
                    LEFT JOIN section s ON c.section_id = s.id
                    WHERE s.name IN ('income', 'expense') {filter}
                    GROUP BY month
                    ORDER BY month DESC
                    LIMIT 12
                """,
                "explanation": "Monthly income vs expenses comparison",
                "trend_value_key": "income_total",
                "trend_time_key": "month",
            },
            "thirty_day_summary": {
                "keywords": ["last 30 days", "past month summary", "30 day summary"],
                "sql": """
                    SELECT 'Income' as metric, SUM(ABS(jl.amount)) as total
                    FROM journal_line jl
                    JOIN journal_entry je ON jl.entry_id = je.id
                    JOIN account a ON jl.account_id = a.id
                    LEFT JOIN category c ON jl.category_id = c.id
                    LEFT JOIN section s ON c.section_id = s.id
                    WHERE s.name = 'income' AND je.txn_date >= DATE_SUB(CURDATE(), INTERVAL 30 DAY) {filter}
                    UNION ALL
                    SELECT 'Expenses' as metric, SUM(ABS(jl.amount)) as total
                    FROM journal_line jl
                    JOIN journal_entry je ON jl.entry_id = je.id
                    JOIN account a ON jl.account_id = a.id
                    LEFT JOIN category c ON jl.category_id = c.id
                    LEFT JOIN section s ON c.section_id = s.id
                    WHERE s.name = 'expense' AND je.txn_date >= DATE_SUB(CURDATE(), INTERVAL 30 DAY) {filter}
                """,
                "explanation": "Income and expenses over the last 30 days",
            },
            "quarter_to_date_summary": {
                "keywords": ["quarter to date", "this quarter", "quarter summary"],
                "sql": """
                    SELECT 'Income' as metric, SUM(ABS(jl.amount)) as total
                    FROM journal_line jl
                    JOIN journal_entry je ON jl.entry_id = je.id
                    JOIN account a ON jl.account_id = a.id
                    LEFT JOIN category c ON jl.category_id = c.id
                    LEFT JOIN section s ON c.section_id = s.id
                    WHERE s.name = 'income'
                      AND QUARTER(je.txn_date) = QUARTER(CURDATE())
                      AND YEAR(je.txn_date) = YEAR(CURDATE()) {filter}
                    UNION ALL
                    SELECT 'Expenses' as metric, SUM(ABS(jl.amount)) as total
                    FROM journal_line jl
                    JOIN journal_entry je ON jl.entry_id = je.id
                    JOIN account a ON jl.account_id = a.id
                    LEFT JOIN category c ON jl.category_id = c.id
                    LEFT JOIN section s ON c.section_id = s.id
                    WHERE s.name = 'expense'
                      AND QUARTER(je.txn_date) = QUARTER(CURDATE())
                      AND YEAR(je.txn_date) = YEAR(CURDATE()) {filter}
                """,
                "explanation": "Quarter-to-date income and expenses",
            },
            "category_spend": {
                "keywords": ["spend on", "spent on", "expenses for"],
                "sql": """
                    SELECT DATE_FORMAT(je.txn_date, '%Y-%m') as month,
                           SUM(ABS(jl.amount)) as category_spend
                    FROM journal_line jl
                    JOIN journal_entry je ON jl.entry_id = je.id
                    JOIN account a ON jl.account_id = a.id
                    LEFT JOIN category c ON jl.category_id = c.id
                    LEFT JOIN section s ON c.section_id = s.id
                    WHERE s.name = 'expense' {filter} {category_clause}
                    GROUP BY DATE_FORMAT(je.txn_date, '%Y-%m')
                    ORDER BY month DESC
                    LIMIT 12
                """,
                "explanation": "Monthly spending for the requested category",
                "dynamic_category": True,
                "trend_value_key": "category_spend",
                "trend_time_key": "month",
            },
            "category_income": {
                "keywords": ["income from", "earnings from", "revenue from"],
                "sql": """
                    SELECT DATE_FORMAT(je.txn_date, '%Y-%m') as month,
                           SUM(ABS(jl.amount)) as category_income
                    FROM journal_line jl
                    JOIN journal_entry je ON jl.entry_id = je.id
                    JOIN account a ON jl.account_id = a.id
                    LEFT JOIN category c ON jl.category_id = c.id
                    LEFT JOIN section s ON c.section_id = s.id
                    WHERE s.name = 'income' {filter} {category_clause}
                    GROUP BY DATE_FORMAT(je.txn_date, '%Y-%m')
                    ORDER BY month DESC
                    LIMIT 12
                """,
                "explanation": "Monthly income for the requested category",
                "dynamic_category": True,
                "trend_value_key": "category_income",
                "trend_time_key": "month",
            },
            "monthly_expense_trend": {
                "keywords": ["expense trend", "monthly expenses", "spending trend"],
                "sql": """
                    SELECT month,
                           monthly_total,
                           ROUND(AVG(monthly_total) OVER (ORDER BY month ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) as rolling_3_month_avg
                    FROM (
                        SELECT DATE_FORMAT(je.txn_date, '%Y-%m') as month,
                               SUM(ABS(jl.amount)) as monthly_total
                        FROM journal_line jl
                        JOIN journal_entry je ON jl.entry_id = je.id
                        JOIN account a ON jl.account_id = a.id
                        LEFT JOIN category c ON jl.category_id = c.id
                        LEFT JOIN section s ON c.section_id = s.id
                        WHERE s.name = 'expense' {filter}
                        GROUP BY DATE_FORMAT(je.txn_date, '%Y-%m')
                    ) monthly
                    ORDER BY month DESC
                    LIMIT 12
                """,
                "explanation": "Expense trend with month-over-month totals and 3-month rolling average",
                "trend_value_key": "monthly_total",
                "trend_time_key": "month",
            },
            "monthly_income_trend": {
                "keywords": ["income trend", "revenue trend", "earnings trend"],
                "sql": """
                    SELECT month,
                           monthly_total,
                           ROUND(AVG(monthly_total) OVER (ORDER BY month ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) as rolling_3_month_avg
                    FROM (
                        SELECT DATE_FORMAT(je.txn_date, '%Y-%m') as month,
                               SUM(ABS(jl.amount)) as monthly_total
                        FROM journal_line jl
                        JOIN journal_entry je ON jl.entry_id = je.id
                        JOIN account a ON jl.account_id = a.id
                        LEFT JOIN category c ON jl.category_id = c.id
                        LEFT JOIN section s ON c.section_id = s.id
                        WHERE s.name = 'income' {filter}
                        GROUP BY DATE_FORMAT(je.txn_date, '%Y-%m')
                    ) monthly
                    ORDER BY month DESC
                    LIMIT 12
                """,
                "explanation": "Income trend with month-over-month totals and 3-month rolling average",
                "trend_value_key": "monthly_total",
                "trend_time_key": "month",
            },
            "spending_this_month": {
                "keywords": ["spending this month", "expenses this month", "how much spent this month"],
                "sql": """
                    SELECT c.name as category, SUM(ABS(jl.amount)) as total
                    FROM journal_line jl
                    JOIN journal_entry je ON jl.entry_id = je.id
                    JOIN account a ON jl.account_id = a.id
                    LEFT JOIN category c ON jl.category_id = c.id
                    LEFT JOIN section s ON c.section_id = s.id
                    WHERE s.name = 'expense'
                      AND YEAR(je.txn_date) = YEAR(CURDATE())
                      AND MONTH(je.txn_date) = MONTH(CURDATE())
                      AND c.name IS NOT NULL {filter}
                    GROUP BY c.name
                    ORDER BY total DESC
                    LIMIT 10
                """,
                "explanation": "Your spending this month by category",
            },
            "spending_last_month": {
                "keywords": ["spending last month", "expenses last month", "how much spent last month"],
                "sql": """
                    SELECT c.name as category, SUM(ABS(jl.amount)) as total
                    FROM journal_line jl
                    JOIN journal_entry je ON jl.entry_id = je.id
                    JOIN account a ON jl.account_id = a.id
                    LEFT JOIN category c ON jl.category_id = c.id
                    LEFT JOIN section s ON c.section_id = s.id
                    WHERE s.name = 'expense'
                      AND YEAR(je.txn_date) = YEAR(DATE_SUB(CURDATE(), INTERVAL 1 MONTH))
                      AND MONTH(je.txn_date) = MONTH(DATE_SUB(CURDATE(), INTERVAL 1 MONTH))
                      AND c.name IS NOT NULL {filter}
                    GROUP BY c.name
                    ORDER BY total DESC
                    LIMIT 10
                """,
                "explanation": "Your spending last month by category",
            },
            "total_income": {
                "keywords": ["total income", "how much income", "my income"],
                "sql": """
                    SELECT SUM(ABS(jl.amount)) as total_income
                    FROM journal_line jl
                    JOIN journal_entry je ON jl.entry_id = je.id
                    JOIN account a ON jl.account_id = a.id
                    LEFT JOIN category c ON jl.category_id = c.id
                    LEFT JOIN section s ON c.section_id = s.id
                    WHERE s.name = 'income' {filter}
                """,
                "explanation": "Your total income",
            },
            "total_expenses": {
                "keywords": ["total expenses", "total spending", "how much spent"],
                "sql": """
                    SELECT SUM(ABS(jl.amount)) as total_expenses
                    FROM journal_line jl
                    JOIN journal_entry je ON jl.entry_id = je.id
                    JOIN account a ON jl.account_id = a.id
                    LEFT JOIN category c ON jl.category_id = c.id
                    LEFT JOIN section s ON c.section_id = s.id
                    WHERE s.name = 'expense' {filter}
                """,
                "explanation": "Your total expenses",
            }
        }

    def match_template(self, question: str) -> Optional[Dict[str, Any]]:
        """
        Check if question matches a quick template

        Args:
            question: User's question (lowercase)

        Returns:
            Template dict with 'sql' and 'explanation' or None
        """
        if not chatbot_config.enable_quick_templates:
            return None

        question_lower = question.lower()

        for template_name, template_data in self.templates.items():
            for keyword in template_data["keywords"]:
                if keyword in question_lower:
                    prepared_sql, params = self._prepare_template_sql(
                        template_name, template_data, question_lower
                    )
                    return {
                        "name": template_name,
                        "sql": prepared_sql,
                        "explanation": template_data["explanation"],
                        "params": params,
                    }

        return None

    def _prepare_template_sql(
        self, template_name: str, template_data: Dict[str, Any], question_lower: str
    ) -> tuple[str, Dict[str, Any]]:
        sql = template_data["sql"]
        params: Dict[str, Any] = {}

        if template_data.get("dynamic_category"):
            category_name = self._extract_category_name(question_lower)
            if category_name:
                params["category_name"] = f"%{category_name}%"
                category_clause = "AND c.name LIKE :category_name"
            else:
                category_clause = "AND c.name IS NOT NULL"
            sql = sql.replace("{category_clause}", category_clause)
        else:
            sql = sql.replace("{category_clause}", "")

        return sql, params

    def _extract_category_name(self, question_lower: str) -> Optional[str]:
        patterns = [
            r"spend on (?P<category>[a-zA-Z\s]+)",
            r"spent on (?P<category>[a-zA-Z\s]+)",
            r"expenses for (?P<category>[a-zA-Z\s]+)",
            r"income from (?P<category>[a-zA-Z\s]+)",
            r"revenue from (?P<category>[a-zA-Z\s]+)",
            r"earnings from (?P<category>[a-zA-Z\s]+)",
        ]

        for pattern in patterns:
            match = re.search(pattern, question_lower)
            if match:
                return match.group("category").strip()

        return None

    def render_template(
        self, question: str, user_context: Dict[str, Any]
    ) -> Optional[Dict[str, Any]]:
        template = self.match_template(question)
        if not template:
            return None

        sql_with_filter = self.apply_filter(template["sql"], user_context)
        return {
            **template,
            "sql": sql_with_filter,
        }

    def render_template_by_keyword(
        self, keyword: str, user_context: Dict[str, Any]
    ) -> Optional[Dict[str, Any]]:
        """Render a template by its keyword name with role-based filtering."""

        template = self.templates.get(keyword)
        if not template:
            return None

        sql_with_filter = self.apply_filter(template["sql"], user_context)
        return {
            "name": keyword,
            "sql": sql_with_filter,
            "explanation": template.get("explanation", ""),
            "params": template.get("params", {}),
        }

    def apply_filter(self, sql: str, user_context: Dict[str, Any]) -> str:
        """
        Apply role-based filter to template SQL

        Args:
            sql: Template SQL with {filter} placeholder
            user_context: User's role and IDs

        Returns:
            SQL with filter applied
        """
        role = user_context.get("role")
        person_id = user_context.get("person_id")
        company_id = user_context.get("company_id")

        if role == "person" and person_id:
            filter_clause = f"AND a.party_id = {person_id}"
        elif role == "company" and company_id:
            filter_clause = f"AND a.party_id = {company_id}"
        else:
            filter_clause = ""

        return sql.replace("{filter}", filter_clause)

    def build_trend_narrative(
        self, template_name: str, results: list[Dict[str, Any]]
    ) -> Optional[str]:
        template = self.templates.get(template_name)
        if not template or not results:
            return None

        value_key = template.get("trend_value_key")
        time_key = template.get("trend_time_key")
        if not value_key or not time_key:
            return None

        ordered = sorted(
            [row for row in results if value_key in row and time_key in row],
            key=lambda row: row[time_key],
        )

        if len(ordered) < 2:
            return None

        latest, previous = ordered[-1], ordered[-2]
        latest_value = latest[value_key]
        previous_value = previous[value_key]

        if previous_value == 0:
            change_text = "from zero previously"
        else:
            change_pct = ((latest_value - previous_value) / previous_value) * 100
            direction = "increased" if change_pct >= 0 else "decreased"
            change_text = f"{direction} by {abs(change_pct):.1f}% since {previous[time_key]}"

        return (
            f"Latest value {latest_value:,.2f} for {latest[time_key]} {change_text}."
        )
