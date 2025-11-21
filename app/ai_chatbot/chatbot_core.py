"""
Core Chatbot Orchestration Module
Main entry point for chatbot functionality
"""
import json
import re
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Literal, Optional, Sequence

from app.core.logger import get_logger
from sqlalchemy.orm import Session

from .tools import (
    ToolResult,
    UserScope,
    expenses_by_category,
    income_by_category,
    leaderboard,
    monthly_cash_flow_comparison,
    party_insights,
    spending_trend,
    top_spenders,
)
from .sql_generator import SQLGenerator, QuickTemplateManager
from .chart_generator import ChartGenerator
from .llm_providers import LLMProviderFactory
from .prompt_builder import PromptBuilder

logger = get_logger(__name__)


@dataclass
class ToolSpec:
    """Registry entry describing a callable analytics tool."""

    name: str
    description: str
    handler: Callable[..., ToolResult]
    default_args: Dict[str, Any] | None = None


class ToolRegistry:
    """Central registry for chatbot tool/function calls."""

    def __init__(self) -> None:
        self._tools: Dict[str, ToolSpec] = {
            "expenses_by_category": ToolSpec(
                name="expenses_by_category",
                description="Sum of expenses grouped by category for a recent window",
                handler=expenses_by_category,
                default_args={"days": 30, "limit": 8},
            ),
            "income_by_category": ToolSpec(
                name="income_by_category",
                description="Sum of income grouped by category for a recent window",
                handler=income_by_category,
                default_args={"days": 30, "limit": 8},
            ),
            "monthly_comparison": ToolSpec(
                name="monthly_comparison",
                description="Income vs expense totals per month",
                handler=monthly_cash_flow_comparison,
                default_args={"months": 6},
            ),
            "monthly_expense_trend": ToolSpec(
                name="monthly_expense_trend",
                description="Expense trend over time with monthly granularity",
                handler=spending_trend,
                default_args={"days": 180},
            ),
            "leaderboard": ToolSpec(
                name="leaderboard",
                description=(
                    "Admin-only leaderboard by metric. "
                    "Args: metric=expenses|income|net_stock_gains|category_expenses:<name>, "
                    "direction=top|bottom, party_type=company|individual|all, days, limit."
                ),
                handler=leaderboard,
                default_args={
                    "metric": "expenses",
                    "direction": "top",
                    "party_type": "all",
                    "days": 30,
                    "limit": 5,
                },
            ),
            "party_insights": ToolSpec(
                name="party_insights",
                description=(
                    "Admin-only snapshot for a specific party (resolve by id or name). "
                    "Args: party_id|party_name, metric=summary|income|expenses|net_cash_flow|category_expenses:<name>, "
                    "granularity=total|monthly, days, party_type=individual|company (defaults to individual)."
                ),
                handler=party_insights,
                default_args={
                    "metric": "summary",
                    "granularity": "total",
                    "days": 365,
                    "party_type": "individual",
                },
            ),
            "top_spenders": ToolSpec(
                name="top_spenders",
                description=(
                    "Alias of leaderboard (defaults to top expenses across all parties). "
                    "Supports the same args as leaderboard."
                ),
                handler=top_spenders,
                default_args={
                    "metric": "expenses",
                    "direction": "top",
                    "party_type": "all",
                    "days": 30,
                    "limit": 5,
                },
            ),
        }

    def describe_keywords(self) -> list[dict[str, str]]:
        """Return keyword + description pairs for prompt construction."""
        return [
            {"keyword": spec.name, "description": spec.description}
            for spec in self._tools.values()
        ]

    def describe_for_prompt(self) -> str:
        """Human-readable list of tools and argument hints."""
        lines = []
        for spec in self._tools.values():
            default_args = spec.default_args or {}
            arg_hint = (
                f" (args: {', '.join(f'{k}={v}' for k, v in default_args.items())})"
                if default_args
                else ""
            )
            lines.append(f"- {spec.name}: {spec.description}{arg_hint}")
        lines.append(
            "Leaderboard metrics: expenses, income, net_stock_gains, category_expenses:<category>."
        )
        lines.append(
            "party_insights resolves a party by id or name and supports metrics summary|income|expenses|net_cash_flow|category_expenses:<category> with granularity=total|monthly."
        )
        lines.append(
            "Examples: leaderboard metric=expenses direction=top party_type=company limit=5; "
            "leaderboard metric=net_stock_gains direction=bottom party_type=individual limit=10; "
            "leaderboard metric=category_expenses:travel direction=top days=90; "
            "party_insights party_name=alex metric=income granularity=monthly."
        )
        return "\n".join(lines)

    def build_calls_from_keywords(
        self, descriptors: Sequence[Dict[str, Any]]
    ) -> list[Dict[str, Any]]:
        """Convert visualization descriptors into tool calls with default args."""
        calls: list[Dict[str, Any]] = []
        for descriptor in descriptors:
            keyword = descriptor.get("keyword")
            if keyword and keyword in self._tools:
                calls.append({"tool": keyword, "arguments": {}})
        return calls

    def execute_calls(
        self,
        calls: Sequence[Dict[str, Any]],
        user_context: Dict[str, Any],
        db_session: Session,
    ) -> list[ToolResult]:
        """Run tool calls with scope enforcement and error handling."""
        scope = UserScope.from_context(user_context)
        results: list[ToolResult] = []

        for call in calls:
            name = str(call.get("tool") or call.get("name") or "").strip()
            if not name:
                continue

            spec = self._tools.get(name)
            if not spec:
                logger.warning("Unknown tool requested by model: %s", name)
                continue

            provided_args = self._coerce_arguments(call.get("arguments") or {})
            args = {**(spec.default_args or {}), **provided_args}

            try:
                logger.info("Executing tool=%s args=%s", name, args)
                result = spec.handler(db_session, scope, **args)
            except PermissionError as exc:
                logger.warning("Tool %s blocked by permission: %s", name, exc)
                continue
            except Exception as exc:
                logger.error("Tool %s failed: %s", name, exc, exc_info=True)
                continue

            if isinstance(result, (list, tuple)):
                results.extend(r for r in result if isinstance(r, ToolResult))
            else:
                results.append(result)

        return results

    @staticmethod
    def _coerce_arguments(raw_args: Dict[str, Any]) -> Dict[str, Any]:
        """Best-effort coercion of simple argument types from model output."""
        coerced: Dict[str, Any] = {}
        for key, value in raw_args.items():
            if key in {"metric", "direction", "party_type"}:
                coerced[key] = value
            elif isinstance(value, str) and value.strip().isdigit():
                coerced[key] = int(value.strip())
            else:
                coerced[key] = value
        return coerced


class FinancialChatbot:
    """Main chatbot class for financial queries"""

    def __init__(self, database_schema: Optional[str] = None, enable_sql_fallback: bool = False):
        """
        Initialize chatbot

        Args:
            database_schema: Custom database schema description
        """
        self.sql_generator = SQLGenerator(database_schema)
        self.chart_generator = ChartGenerator()
        self.template_manager = QuickTemplateManager()
        self.tool_registry = ToolRegistry()
        self.enable_sql_fallback = enable_sql_fallback
        self.prompt_builder = PromptBuilder(
            self.sql_generator.database_schema,
            self.tool_registry.describe_keywords(),
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

        tool_calls = plan.get("tool_calls") or []
        if not tool_calls and plan.get("visualizations"):
            tool_calls = self.tool_registry.build_calls_from_keywords(
                plan.get("visualizations", [])
            )

        logger.info(
            "Visualization plan -> tools: %s",
            [call.get("tool") or call.get("name") for call in tool_calls],
        )

        if not tool_calls:
            logger.warning("No tool calls produced; returning reply without visualizations")
            return {
                "response": plan.get("reply", "No visualization available."),
                "chart_config": None,
                "chart_title": None,
                "table_data": None,
                "sql_query": None,
                "visualizations": [],
                "mode": "visualization",
            }

        tool_results = self.tool_registry.execute_calls(
            tool_calls,
            user_context,
            db_session,
        )
        logger.info(
            "Executed %d tool calls; %d returned data",
            len(tool_results),
            sum(1 for r in tool_results if r.has_data),
        )
        visualizations = self._render_tool_results(tool_results)

        if not visualizations and self.enable_sql_fallback:
            logger.info("Tool results empty; falling back to legacy SQL path (deprecated)")
            visualizations = await self._fallback_visualizations(
                question,
                provider_name,
                user_context,
                db_session,
                conversation_history,
                financial_summary,
            )

        error_notes = [viz["chart_error"] for viz in visualizations if viz.get("chart_error")]

        reply_text = plan.get("reply", "Here's what I found.")
        if error_notes:
            reply_text = f"{reply_text}\nChart issues: {'; '.join(error_notes)}"

        primary = visualizations[0] if visualizations else None
        if not primary:
            return {
                "response": reply_text or "No data found for your query.",
                "chart_config": None,
                "chart_title": None,
                "table_data": None,
                "sql_query": None,
                "visualizations": [],
                "mode": "visualization",
            }

        return {
            "response": reply_text,
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
        self._log_llm_exchange(
            stage="conversational_reply",
            provider_name=provider_name,
            system_prompt=system_prompt,
            user_prompt=question,
            response_content=response.get("content", ""),
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

    def _log_llm_exchange(
        self,
        *,
        stage: str,
        provider_name: str,
        system_prompt: str,
        user_prompt: str,
        response_content: str,
    ) -> None:
        """Log prompts and responses for observability/troubleshooting."""
        logger.info("LLM exchange stage=%s provider=%s", stage, provider_name)
        logger.debug("System prompt [%s]: %s", stage, system_prompt)
        logger.debug("User prompt [%s]: %s", stage, user_prompt)
        logger.debug("Response [%s]: %s", stage, response_content)

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
    "tool_calls": [
        {
            "tool": "one of the allowed tools",
            "arguments": {"days": 30, "limit": 5}
        }
    ],
    "visualizations": [
        {
            "keyword": "one of the allowed keywords",
            "title": "human readable chart title",
            "chart_type": "bar|line|pie|doughnut",
            "kind": "chart|table",
            "x_axis": "field name for labels/time",
            "y_axis": "field name or list of numeric fields",
            "stack_by": "field name to create stacked series | null",
            "unit": "currency|count|other",
            "sort": "asc|desc|null"
        }
    ]
}"""

        tools_block = self.tool_registry.describe_for_prompt()
        system_prompt = self.prompt_builder.build_system_prompt(
            user_context=user_context,
            page_context=page_context,
            response_schema=response_schema,
        )
        system_prompt = (
            f"{system_prompt}\n\nAvailable tools for data retrieval:\n"
            f"{tools_block}\n\n"
            "When charts or tables are required, include tool_calls with the"
            " appropriate tool name and arguments. Prefer tool_calls over SQL"
            " generation. The visualizations array can act as a summary of the"
            " requested outputs."
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
        self._log_llm_exchange(
            stage="visualization_plan",
            provider_name=provider_name,
            system_prompt=system_prompt,
            user_prompt=user_prompt,
            response_content=response.get("content", ""),
        )

        content = response.get("content", "{}")
        parsed = self._parse_json_response(content)
        if parsed is None:
            logger.warning("Visualization plan was not valid JSON; returning fallback plan")
            parsed = {}

        visualizations = parsed.get("visualizations") or []
        # Keep only the first three allowed keywords
        allowed_keywords = {t["keyword"] for t in self.tool_registry.describe_keywords()}
        filtered_visualizations = []
        for item in visualizations:
            keyword = item.get("keyword")
            if keyword in allowed_keywords and len(filtered_visualizations) < 3:
                filtered_visualizations.append(item)

        tool_calls = parsed.get("tool_calls") or []
        filtered_tool_calls = []
        for call in tool_calls:
            call_name = call.get("tool") or call.get("name")
            if call_name in allowed_keywords:
                filtered_tool_calls.append(call)
            if len(filtered_tool_calls) >= 3:
                break

        return {
            "reply": parsed.get("reply", "Here's what I found."),
            "visualizations": filtered_visualizations,
            "tool_calls": filtered_tool_calls,
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
                descriptor,
            )

            if payload:
                payload["keyword"] = descriptor.get("keyword")
                rendered.append(payload)

        return rendered

    def _render_tool_results(self, results: Sequence[ToolResult]) -> List[Dict[str, Any]]:
        """Convert tool outputs into the visualization payload format."""
        rendered: list[Dict[str, Any]] = []
        deferred: list[Dict[str, Any]] = []

        for result in results:
            descriptor: Dict[str, Any] = {}
            if result.chart_type:
                descriptor["chart_type"] = result.chart_type
            if result.x_axis:
                descriptor["x_axis"] = result.x_axis
            if result.y_axis:
                descriptor["y_axis"] = result.y_axis
            if result.stack_by:
                descriptor["stack_by"] = result.stack_by
            if result.unit:
                descriptor["unit"] = result.unit
            if result.sort:
                descriptor["sort"] = result.sort
            payload = self._build_visualization_payload(
                result.rows,
                result.title,
                sql_query=None,
                chart_type=result.chart_type,
                descriptor=descriptor,
            )
            if payload:
                payload["keyword"] = result.keyword
                if result.keyword.startswith("company_employees_"):
                    deferred.append(payload)
                else:
                    rendered.append(payload)

        # Append any deferred employee tables at the end (once per keyword)
        seen_keywords = set()
        for payload in deferred:
            if payload["keyword"] in seen_keywords:
                continue
            seen_keywords.add(payload["keyword"])
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
        sql_query: Optional[str],
        chart_type: Optional[str],
        descriptor: Optional[Dict[str, Any]] = None,
    ) -> Optional[Dict[str, Any]]:
        if not results:
            return None

        try:
            chart_config = self.chart_generator.generate_chart_config_enforced(
                data=results,
                descriptor=descriptor or {},
                fallback_chart_type=chart_type,
                title=explanation,
            )
            chart_error = None
        except Exception as exc:
            chart_error = str(exc)
            logger.warning("Chart rendering failed: %s", exc)
            chart_config = None

        return {
            "chart_config": chart_config,
            "chart_title": explanation,
            "table_data": results,
            "sql_query": sql_query,
            "chart_error": chart_error,
        }
