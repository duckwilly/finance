"""Prompt assembly utilities for the AI chatbot.

This module centralizes construction of LLM prompts so that each
request includes consistent application context, schema guidance,
and the required JSON response contract described in CHATBOTPLAN.md.
"""
from __future__ import annotations

from typing import Dict, List, Optional, Tuple

from app.chatbot_schema import DATABASE_SCHEMA


class ChatbotPromptBuilder:
    """Build structured prompts for chatbot interactions."""

    APP_HEADER = (
        "Finance Dashboard AI Assistant — respond with insights and data-driven visuals\n"
        "You help users explore company and personal finance data through concise text and up to "
        "three visualizations per reply."
    )

    ALLOWED_QUERY_GUIDANCE = """
Allowed SQLAlchemy-driven visualizations (up to 3):
- expenses_by_category: expenses grouped by category
- income_by_category: income grouped by category
- monthly_comparison: monthly income vs expenses
- thirty_day_summary: 30-day income/expense snapshot
- quarter_to_date_summary: quarter-to-date income/expense
- category_spend: monthly spend for a category
- category_income: monthly income for a category
- monthly_expense_trend: monthly expenses with rolling averages
- monthly_income_trend: monthly income with rolling averages
When in person/company scope you must respect that scope; admin can see all data.
"""

    RESPONSE_SCHEMA = """
Return JSON only with this shape:
{
  "message": "Plaintext response for the chat window.",
  "visualizations": [
    {
      "keyword": "one of the allowed keywords above",
      "title": "Short chart/table title",
      "chart_type": "bar|line|pie|doughnut|auto",
      "focus": "Optional category or metric focus",
      "limit": 0-100 (optional)
    }
  ]
}
Rules:
- message must be plaintext (no markdown).
- visualizations length between 0 and 3.
- If the best reply is conversational only, return an empty list.
"""

    EXAMPLE_EXCHANGE = """
Example user request: "Show me my expenses by category for this month"
Example JSON response:
{
  "message": "Here is your spending by category for the current month.",
  "visualizations": [
    {"keyword": "expenses_by_category", "title": "Expenses by Category", "chart_type": "doughnut"}
  ]
}
"""

    def build_prompts(
        self,
        question: str,
        user_context: Dict[str, Optional[str]],
        page_context: Optional[str] = None,
        financial_summary: Optional[str] = None,
        conversation_history: Optional[List[Dict[str, str]]] = None,
    ) -> Tuple[str, str]:
        """Construct system and user prompts for the LLM.

        Args:
            question: User question.
            user_context: Role/identity details for the signed-in user.
            page_context: Optional string describing the current dashboard page.
            financial_summary: Optional recent financial summary for RAG context.
            conversation_history: Prior conversation exchanges.

        Returns:
            Tuple of (system_prompt, user_prompt).
        """
        page_line = page_context or "You are on the main finance dashboard."

        system_prompt = "\n".join(
            [
                self.APP_HEADER,
                page_line,
                f"Current user context: {user_context}",
                "Database schema:\n" + DATABASE_SCHEMA.strip(),
                self.ALLOWED_QUERY_GUIDANCE.strip(),
                self.RESPONSE_SCHEMA.strip(),
                self.EXAMPLE_EXCHANGE.strip(),
            ]
        )

        history_text = ""
        if conversation_history:
            formatted = [
                f"{msg.get('role','user')}: {msg.get('content','')}"
                for msg in conversation_history
            ]
            history_text = "Previous conversation:\n" + "\n".join(formatted) + "\n"

        user_parts = []
        if financial_summary:
            user_parts.append(f"Financial summary:\n{financial_summary}\n")
        user_parts.append(history_text)
        user_parts.append(f"User request: {question}")

        user_prompt = "\n".join(part for part in user_parts if part)

        return system_prompt, user_prompt
