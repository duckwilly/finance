"""
Core Chatbot Orchestration Module
Main entry point for chatbot functionality
"""
import json
import logging
import re
from typing import Dict, Any, Optional, List, Literal
from sqlalchemy.orm import Session

from .sql_generator import SQLGenerator, QuickTemplateManager
from .chart_generator import ChartGenerator
from .llm_providers import LLMProviderFactory
from .prompt_builder import PromptBuilder

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
        self.prompt_builder = PromptBuilder(
            self.sql_generator.database_schema,
            self.template_manager.describe_templates(),
        )

    async def process_query(
        self,
        question: str,
        provider_name: str,
        user_context: Dict[str, Any],
        db_session: Session,
        conversation_history: Optional[List[Dict[str, str]]] = None,
        response_mode: Optional[Literal["visualization", "conversational"]] = None,
        financial_summary: Optional[str] = None,
        page_context: str = "Dashboard"
    ) -> Dict[str, Any]:
        """
        Process a chatbot query end-to-end

        Args:
            question: User's natural language question
            provider_name: LLM provider (e.g., 'claude-haiku-4-5-20251001')
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
            # Step 1: Determine response mode if not specified
            if not response_mode:
                response_mode = await self._detect_response_mode(question)

            # Step 2: Handle based on mode
            if response_mode == "visualization":
                return await self._handle_visualization_mode(
                    question,
                    provider_name,
                    user_context,
                    db_session,
                    conversation_history,
                    financial_summary,
                    page_context,
                )
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
        financial_summary: Optional[str],
        page_context: str,
    ) -> Dict[str, Any]:
        """Handle visualization mode (SQL + Charts)"""
        plan = await self._generate_visualization_plan(
            question,
            provider_name,
            user_context,
            conversation_history,
            page_context,
        )

        visualizations = self._materialize_visualizations(
            plan.get("visualizations", []),
            user_context,
            db_session,
        )

        if not visualizations:
            # Fallback to legacy template and SQL generation paths
            visualizations = await self._fallback_visualizations(
                question,
                provider_name,
                user_context,
                db_session,
                conversation_history,
                financial_summary,
            )

        primary = visualizations[0] if visualizations else None
        if not primary:
            return {
                "response": plan.get("reply", "No data found for your query."),
                "chart_config": None,
                "chart_title": None,
                "table_data": None,
                "sql_query": None,
                "visualizations": [],
                "mode": "visualization",
            }

        return {
            "response": plan.get("reply", "Here's what I found."),
            "chart_config": primary.get("chart_config"),
            "chart_title": primary.get("chart_title"),
            "table_data": primary.get("table_data"),
            "sql_query": primary.get("sql_query"),
            "visualizations": visualizations,
            "mode": "visualization",
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

        # Full system prompt for supported models
        try:
            financial_context = f"Financial Context: {financial_summary}" if financial_summary else ""
        except Exception:
            financial_context = ""

        system_prompt = f"""You are a helpful financial assistant providing advice and insights.

User: {user_context.get('username', 'User')}
Role: {user_context.get('role', 'user')}

Capabilities:
- Admins can look up users by id/username/email and companies by id or legal/display name.
- Individuals can only reference their own accounts/party_id.
- Company representatives can only reference their company's accounts/party_id.
- Quick insights include 30-day and quarter-to-date summaries, category-specific spend/earn reports, and monthly trend charts with rolling averages.

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
            "visualizations": [],
            "mode": "conversational"
        }

    async def _detect_response_mode(
        self,
        question: str
    ) -> Literal["visualization", "conversational"]:
        """
        Detect whether question needs visualization or conversational response

        Args:
            question: User's question

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

        if role == "admin":
            filter_clause = ""
        else:
            scope_id = (
                user_context.get("person_id")
                or user_context.get("company_id")
            )
            if not scope_id:
                raise ValueError(
                    "User scope could not be determined for the financial summary"
                )

            filter_clause = f"AND a.party_id = {scope_id}"

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

    async def _generate_visualization_plan(
        self,
        question: str,
        provider_name: str,
        user_context: Dict[str, Any],
        conversation_history: Optional[List[Dict[str, str]]],
        page_context: str,
    ) -> Dict[str, Any]:
        """Ask the LLM for a structured visualization plan."""

        response_schema = """{
    "reply": "plain text response for the chat window",
    "visualizations": [
        {
            "keyword": "one of the allowed keywords",
            "title": "human readable chart title",
            "chart_type": "bar|line|pie|doughnut",
            "kind": "chart|table"
        }
    ]
}"""



        system_prompt = self.prompt_builder.build_system_prompt(
            user_context=user_context,
            page_context=page_context,
            response_schema=response_schema,
        )
        user_prompt = self.prompt_builder.build_user_prompt(
            question=question,
            conversation_history=conversation_history,
        )

        provider = LLMProviderFactory.create(provider_name)
        response = await provider.query(
            system_prompt=system_prompt,
            user_prompt=user_prompt,
            conversation_history=conversation_history,
            json_mode=True,
        )

        content = response.get("content", "{}")
        parsed = self._parse_json_response(content)
        if parsed is None:
            logger.warning("Visualization plan was not valid JSON; returning fallback plan")
            parsed = {}

        visualizations = parsed.get("visualizations") or []
        # Keep only the first three allowed keywords
        allowed_keywords = {t["keyword"] for t in self.template_manager.describe_templates()}
        filtered_visualizations = []
        for item in visualizations:
            keyword = item.get("keyword")
            if keyword in allowed_keywords and len(filtered_visualizations) < 3:
                filtered_visualizations.append(item)

        return {
            "reply": parsed.get("reply", "Here's what I found."),
            "visualizations": filtered_visualizations,
        }

    def _parse_json_response(self, content: Optional[str]) -> Optional[Dict[str, Any]]:
        """
        Attempt to parse JSON content with common LLM formatting quirks handled.

        This trims code fences like ```json blocks and tries to extract the first
        balanced JSON object when extra prose slips into the response.
        """
        if not content:
            return None

        candidates: List[str] = []
        stripped = content.strip()
        fenced_match = re.search(r"```(?:json)?\s*(.*?)```", stripped, re.DOTALL | re.IGNORECASE)
        if fenced_match:
            candidates.append(fenced_match.group(1).strip())
        candidates.append(stripped)

        extracted_object = self._extract_first_json_object(stripped)
        if extracted_object:
            candidates.append(extracted_object)

        for candidate in candidates:
            if not candidate:
                continue
            try:
                return json.loads(candidate)
            except json.JSONDecodeError:
                continue

        return None

    @staticmethod
    def _extract_first_json_object(text: str) -> Optional[str]:
        """Extract the first balanced JSON object from the text."""
        start = text.find("{")
        if start == -1:
            return None

        depth = 0
        in_string = False
        escape = False
        for index in range(start, len(text)):
            char = text[index]
            if in_string:
                if escape:
                    escape = False
                elif char == "\\":
                    escape = True
                elif char == '"':
                    in_string = False
                continue

            if char == '"':
                in_string = True
                continue

            if char == "{":
                depth += 1
            elif char == "}":
                depth -= 1
                if depth == 0:
                    return text[start:index + 1]

        return None

    def _materialize_visualizations(
        self,
        descriptors: List[Dict[str, Any]],
        user_context: Dict[str, Any],
        db_session: Session,
    ) -> List[Dict[str, Any]]:
        """Convert visualization descriptors into executed chart/table payloads."""

        rendered: List[Dict[str, Any]] = []
        for descriptor in descriptors:
            template = self.template_manager.render_template_by_keyword(
                descriptor.get("keyword", ""),
                user_context,
            )

            if not template:
                continue

            sql_query = self.sql_generator.enforce_scope_constraints(
                template["sql"],
                user_context,
            )

            logger.info(f"Executing templated SQL for keyword {descriptor.get('keyword')}")
            results = self.sql_generator.execute_sql(
                sql_query,
                db_session,
                template.get("params") or {},
            )

            payload = self._build_visualization_payload(
                results,
                descriptor.get("title") or template.get("explanation"),
                sql_query,
                descriptor.get("chart_type"),
            )

            if payload:
                payload["keyword"] = descriptor.get("keyword")
                rendered.append(payload)

        return rendered

    async def _fallback_visualizations(
        self,
        question: str,
        provider_name: str,
        user_context: Dict[str, Any],
        db_session: Session,
        conversation_history: Optional[List[Dict[str, str]]],
        financial_summary: Optional[str],
    ) -> List[Dict[str, Any]]:
        """Use legacy template/SQL generation when the plan is empty."""

        template = self.template_manager.render_template(question, user_context)

        if template:
            sql_query = template["sql"]
            explanation = template["explanation"]
            template_params = template.get("params") or {}
        else:
            sql_result = await self.sql_generator.generate_sql(
                question=question,
                provider_name=provider_name,
                user_context=user_context,
                financial_summary=financial_summary,
                conversation_history=conversation_history,
            )
            sql_query = sql_result["sql"]
            explanation = sql_result["explanation"]
            template_params = {}

        sql_query = self.sql_generator.enforce_scope_constraints(sql_query, user_context)
        logger.info(f"Executing SQL fallback: {sql_query}")
        results = self.sql_generator.execute_sql(sql_query, db_session, template_params)

        if not results:
            return []

        payload = self._build_visualization_payload(
            results,
            explanation,
            sql_query,
            None,
        )

        if not payload:
            return []

        payload["keyword"] = template.get("name") if template else "generated_sql"
        return [payload]

    def _build_visualization_payload(
        self,
        results: List[Dict[str, Any]],
        explanation: Optional[str],
        sql_query: str,
        chart_type: Optional[str],
    ) -> Optional[Dict[str, Any]]:
        if not results:
            return None

        first_row = results[0]
        has_multiple_value_fields = len([
            k for k in first_row.keys()
            if self.chart_generator._is_currency_field(k)
        ]) > 1

        if has_multiple_value_fields:
            x_field = next(
                (k for k in first_row.keys() if k.lower() in ["month", "year", "date", "category"]),
                list(first_row.keys())[0],
            )
            y_fields = [k for k in first_row.keys() if k != x_field]

            chart_config = self.chart_generator.generate_multi_series_chart(
                data=results,
                x_field=x_field,
                y_fields=y_fields,
                title=explanation,
            )
        else:
            chart_config = self.chart_generator.generate_chart_config(
                data=results,
                chart_type=chart_type,
                title=explanation,
            )

        return {
            "chart_config": chart_config,
            "chart_title": explanation,
            "table_data": results,
            "sql_query": sql_query,
        }
