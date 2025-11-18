"""
Core Chatbot Orchestration Module
Main entry point for chatbot functionality
"""
import json
import logging
from typing import Dict, Any, Optional, List, Literal
from sqlalchemy.orm import Session

from .sql_generator import SQLGenerator, QuickTemplateManager
from .chart_generator import ChartGenerator
from .llm_providers import LLMProviderFactory

logger = logging.getLogger(__name__)


class FinancialChatbot:
    """Main chatbot class for financial queries"""

    def __init__(self, database_schema: Optional[str] = None):
        """
        Initialize chatbot

        Args:
            database_schema: Custom database schema description
        """
        self.sql_generator = SQLGenerator(database_schema)
        self.chart_generator = ChartGenerator()
        self.template_manager = QuickTemplateManager()

    async def process_query(
        self,
        question: str,
        provider_name: str,
        user_context: Dict[str, Any],
        db_session: Session,
        conversation_history: Optional[List[Dict[str, str]]] = None,
        response_mode: Optional[Literal["visualization", "conversational"]] = None,
        financial_summary: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Process a chatbot query end-to-end

        Args:
            question: User's natural language question
            provider_name: LLM provider (e.g., 'ollama', 'claude', 'chatgpt')
            user_context: Dict with 'role', 'person_id', 'company_id', 'username'
            db_session: SQLAlchemy database session
            conversation_history: Previous messages for context
            response_mode: Force 'visualization' or 'conversational' mode
            financial_summary: Optional RAG context about user's finances

        Returns:
            Dict with response data:
            {
                "response": "Text response",
                "chart_config": {...} or None,
                "chart_title": "..." or None,
                "table_data": [...] or None,
                "sql_query": "..." or None,
                "mode": "visualization" or "conversational"
            }
        """
        try:
            # Check if model is too small for SQL generation
            small_models = ["llama3.2:1b", "llama3.2"]
            is_small_model = any(small in provider_name.lower() for small in small_models)

            # Force conversational mode for small models
            if is_small_model and not response_mode:
                logger.info(f"Forcing conversational mode for small model: {provider_name}")
                response_mode = "conversational"

            # Step 1: Determine response mode if not specified
            if not response_mode:
                response_mode = await self._detect_response_mode(question, provider_name)

            # Step 2: Handle based on mode
            if response_mode == "visualization" and not is_small_model:
                return await self._handle_visualization_mode(
                    question, provider_name, user_context, db_session,
                    conversation_history, financial_summary
                )
            elif response_mode == "visualization" and is_small_model:
                # Small model requested visualization - inform user
                return {
                    "response": "Note: The selected model (llama3.2:1b) is optimized for conversation only and cannot generate charts or query data. Please select a larger model (Llama 3 or DeepSeek R1) for data visualization and analysis.",
                    "chart_config": None,
                    "chart_title": None,
                    "table_data": None,
                    "sql_query": None,
                    "mode": "conversational"
                }
            else:
                return await self._handle_conversational_mode(
                    question, provider_name, user_context,
                    conversation_history, financial_summary
                )

        except Exception as e:
            logger.error(f"Chatbot query failed: {str(e)}")
            return {
                "response": f"Sorry, I encountered an error: {str(e)}",
                "chart_config": None,
                "chart_title": None,
                "table_data": None,
                "sql_query": None,
                "mode": "error"
            }

    async def _handle_visualization_mode(
        self,
        question: str,
        provider_name: str,
        user_context: Dict[str, Any],
        db_session: Session,
        conversation_history: Optional[List[Dict[str, str]]],
        financial_summary: Optional[str]
    ) -> Dict[str, Any]:
        """Handle visualization mode (SQL + Charts)"""

        # Check for quick template match
        template = self.template_manager.match_template(question)

        if template:
            # Use quick template
            sql_query = self.template_manager.apply_filter(
                template["sql"], user_context
            )
            explanation = template["explanation"]
            logger.info("Using quick template for query")
        else:
            # Generate SQL using LLM
            sql_result = await self.sql_generator.generate_sql(
                question=question,
                provider_name=provider_name,
                user_context=user_context,
                financial_summary=financial_summary,
                conversation_history=conversation_history
            )
            sql_query = sql_result["sql"]
            explanation = sql_result["explanation"]

        # Execute SQL
        logger.info(f"Executing SQL: {sql_query}")
        results = self.sql_generator.execute_sql(sql_query, db_session)
        logger.info(f"SQL returned {len(results)} rows")

        if not results:
            return {
                "response": "No data found for your query.",
                "chart_config": None,
                "chart_title": None,
                "table_data": None,
                "sql_query": sql_query,
                "mode": "visualization"
            }

        # Detect if multi-series data (e.g., income vs expenses)
        first_row = results[0]
        has_multiple_value_fields = len([
            k for k in first_row.keys()
            if self.chart_generator._is_currency_field(k)
        ]) > 1

        # Generate chart config
        if has_multiple_value_fields:
            # Multi-series chart
            x_field = next(
                (k for k in first_row.keys() if k.lower() in ["month", "year", "date", "category"]),
                list(first_row.keys())[0]
            )
            y_fields = [k for k in first_row.keys() if k != x_field]

            chart_config = self.chart_generator.generate_multi_series_chart(
                data=results,
                x_field=x_field,
                y_fields=y_fields,
                title=explanation
            )
        else:
            # Single-series chart
            chart_config = self.chart_generator.generate_chart_config(
                data=results,
                title=explanation
            )

        # Build response text
        response_text = f"Here's what I found: {explanation}"

        return {
            "response": response_text,
            "chart_config": chart_config,
            "chart_title": explanation,
            "table_data": results,
            "sql_query": sql_query,
            "mode": "visualization"
        }

    async def _handle_conversational_mode(
        self,
        question: str,
        provider_name: str,
        user_context: Dict[str, Any],
        conversation_history: Optional[List[Dict[str, str]]],
        financial_summary: Optional[str]
    ) -> Dict[str, Any]:
        """Handle conversational mode (no SQL, just chat)"""

        # Check if small model (simpler system prompt for better performance)
        small_models = ["llama3.2:1b", "llama3.2"]
        is_small_model = any(small in provider_name.lower() for small in small_models)

        if is_small_model:
            # Very simple system prompt for small models (no financial context to avoid complexity)
            system_prompt = "You are a helpful financial assistant. Provide clear, concise advice in plain text (no markdown formatting)."
        else:
            # Full system prompt for larger models
            # Skip financial summary if it caused errors
            try:
                financial_context = f"Financial Context: {financial_summary}" if financial_summary else ""
            except:
                financial_context = ""

            system_prompt = f"""You are a helpful financial assistant providing advice and insights.

User: {user_context.get('username', 'User')}
Role: {user_context.get('role', 'user')}

Capabilities:
- Admins can look up users by id/username/email and companies by id or legal/display name.
- Individuals can only reference their own accounts/party_id.
- Company representatives can only reference their company's accounts/party_id.

{financial_context}

IMPORTANT: Write in plain text only. Do NOT use markdown formatting (no **, *, #, -, or other markdown symbols).
Use simple line breaks and natural language. For lists, just use numbers like "1." or simple sentences.
Provide helpful, concise advice in a friendly tone. Focus on actionable insights."""

        # Query LLM
        provider = LLMProviderFactory.create(provider_name)

        response = await provider.query(
            system_prompt=system_prompt,
            user_prompt=question,
            conversation_history=conversation_history,
            json_mode=False
        )

        return {
            "response": response["content"],
            "chart_config": None,
            "chart_title": None,
            "table_data": None,
            "sql_query": None,
            "mode": "conversational"
        }

    async def _detect_response_mode(
        self,
        question: str,
        provider_name: str
    ) -> Literal["visualization", "conversational"]:
        """
        Detect whether question needs visualization or conversational response

        Args:
            question: User's question
            provider_name: LLM provider

        Returns:
            'visualization' or 'conversational'
        """
        # Keywords indicating visualization
        viz_keywords = [
            "show", "display", "chart", "graph", "plot",
            "breakdown", "list", "summary", "report",
            "how much", "what are", "top"
        ]

        # Keywords indicating conversational
        conv_keywords = [
            "should i", "what do you think", "advice",
            "recommend", "help me", "explain", "why",
            "how can i", "is it good", "is it bad"
        ]

        question_lower = question.lower()

        # Check for strong visualization signals
        if any(kw in question_lower for kw in viz_keywords):
            return "visualization"

        # Check for strong conversational signals
        if any(kw in question_lower for kw in conv_keywords):
            return "conversational"

        # Default to visualization for ambiguous queries
        return "visualization"

    def get_financial_summary(
        self,
        user_context: Dict[str, Any],
        db_session: Session
    ) -> str:
        """
        Generate financial summary for RAG context

        Args:
            user_context: User's role and IDs
            db_session: Database session

        Returns:
            Formatted financial summary string
        """
        role = user_context.get("role")
        person_id = user_context.get("person_id")
        company_id = user_context.get("company_id")

        # Build filter
        if role == "person" and person_id:
            filter_clause = f"AND a.party_id = {person_id}"
        elif role == "company" and company_id:
            filter_clause = f"AND a.party_id = {company_id}"
        else:
            filter_clause = ""

        try:
            # Get total income (from journal lines)
            income_sql = f"""
                SELECT SUM(ABS(jl.amount)) as total
                FROM journal_line jl
                JOIN journal_entry je ON jl.entry_id = je.id
                JOIN account a ON jl.account_id = a.id
                LEFT JOIN category c ON jl.category_id = c.id
                LEFT JOIN section s ON c.section_id = s.id
                WHERE s.name = 'income' {filter_clause}
            """
            income_result = self.sql_generator.execute_sql(income_sql, db_session)
            total_income = income_result[0]["total"] if income_result and income_result[0]["total"] else 0

            # Get total expenses (from journal lines)
            expense_sql = f"""
                SELECT SUM(ABS(jl.amount)) as total
                FROM journal_line jl
                JOIN journal_entry je ON jl.entry_id = je.id
                JOIN account a ON jl.account_id = a.id
                LEFT JOIN category c ON jl.category_id = c.id
                LEFT JOIN section s ON c.section_id = s.id
                WHERE s.name = 'expense' {filter_clause}
            """
            expense_result = self.sql_generator.execute_sql(expense_sql, db_session)
            total_expenses = expense_result[0]["total"] if expense_result and expense_result[0]["total"] else 0

            # Get top expense categories
            top_expenses_sql = f"""
                SELECT c.name as category_name, SUM(ABS(jl.amount)) as total
                FROM journal_line jl
                JOIN journal_entry je ON jl.entry_id = je.id
                JOIN account a ON jl.account_id = a.id
                LEFT JOIN category c ON jl.category_id = c.id
                LEFT JOIN section s ON c.section_id = s.id
                WHERE s.name = 'expense' AND c.name IS NOT NULL {filter_clause}
                GROUP BY c.name
                ORDER BY total DESC
                LIMIT 5
            """
            top_expenses = self.sql_generator.execute_sql(top_expenses_sql, db_session)

            # Format summary
            summary = f"""
Total Income: €{total_income:,.2f}
Total Expenses: €{total_expenses:,.2f}
Net Position: €{(total_income - total_expenses):,.2f}

Top Expense Categories:
"""
            for i, cat in enumerate(top_expenses, 1):
                summary += f"{i}. {cat['category_name']}: €{cat['total']:,.2f}\n"

            return summary.strip()

        except Exception as e:
            logger.error(f"Failed to generate financial summary: {str(e)}")
            return "Financial summary unavailable."
