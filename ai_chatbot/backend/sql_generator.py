"""
SQL Query Generation and Validation Module
Converts natural language to SQL using LLMs with security validation
"""
import re
import json
import logging
from decimal import Decimal
from typing import Dict, Any, Optional
from sqlalchemy import text
from sqlalchemy.orm import Session

from .llm_providers import LLMProviderFactory
from .config import chatbot_config

logger = logging.getLogger(__name__)


class SQLGenerator:
    """Generates and validates SQL queries from natural language"""

    # Default database schema for financial applications
    DEFAULT_SCHEMA = """
Database Schema:
- transactions table: id, account_id, category_id, posted_at, transaction_date, amount, direction (CREDIT/DEBIT), channel, description
- transaction_categories table: id, section_name (income/expense/transfer), category_name
- accounts table: id, account_type, person_id, company_id
- persons table: id, first_name, last_name
- companies table: id, name, industry_category
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
            filter_guidance = f"\nIMPORTANT: Always include 'AND a.person_id = {person_id}' to filter by user's data only."
        elif role == "company" and company_id:
            filter_guidance = f"\nIMPORTANT: Always include 'AND a.company_id = {company_id}' to filter by company data only."
        elif role == "admin":
            filter_guidance = "\nYou have admin access - no filtering required unless specifically requested."

        return f"""You are a SQL query generator for a MariaDB/MySQL financial database.

{self.database_schema}

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
            provider_name: LLM provider to use (e.g., 'ollama', 'claude')
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
            sql = re.sub(r'\s+AND\s+a\.person_id\s*=\s*:?person_id', '', sql, flags=re.IGNORECASE)
            sql = re.sub(r'\s+AND\s+a\.company_id\s*=\s*:?company_id', '', sql, flags=re.IGNORECASE)
            sql = re.sub(r'\s+AND\s+a\.person_id\s*=\s*\d+', '', sql, flags=re.IGNORECASE)
            sql = re.sub(r'\s+AND\s+a\.company_id\s*=\s*\d+', '', sql, flags=re.IGNORECASE)

        return sql

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

    def _build_templates(self) -> Dict[str, Dict[str, str]]:
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
                "explanation": "Total expenses grouped by category"
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
                "explanation": "Total income grouped by category"
            },
            "monthly_comparison": {
                "keywords": ["monthly income vs expenses", "income vs expenses by month"],
                "sql": """
                    SELECT
                        DATE_FORMAT(je.txn_date, '%Y-%m') as month,
                        SUM(CASE WHEN s.name = 'income' THEN ABS(jl.amount) ELSE 0 END) as income,
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
                "explanation": "Monthly income vs expenses comparison"
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
                "explanation": "Your spending this month by category"
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
                "explanation": "Your spending last month by category"
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
                "explanation": "Your total income"
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
                "explanation": "Your total expenses"
            }
        }

    def match_template(self, question: str) -> Optional[Dict[str, str]]:
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
                    return {
                        "sql": template_data["sql"],
                        "explanation": template_data["explanation"]
                    }

        return None

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
            filter_clause = f"AND a.person_id = {person_id}"
        elif role == "company" and company_id:
            filter_clause = f"AND a.company_id = {company_id}"
        else:
            filter_clause = ""

        return sql.replace("{filter}", filter_clause)
