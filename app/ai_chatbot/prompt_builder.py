"""Prompt assembly utilities for the AI chatbot.

This module centralizes the long-form prompt construction required to
produce page-aware, JSON-only responses from the LLM. The prompt builder
injects context about the application, active page, signed-in user, and
available visualization templates so models reliably return the contract
expected by the backend.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional


class PromptBuilder:
    """Construct structured prompts for chatbot calls."""

    APP_HEADER = (
        "You are the AI assistant for the Finance dashboard."
        " The dashboard shows cards at the top and a central area that can"
        " render charts or tables."
    )

    def __init__(self, database_schema: str, allowed_visualizations: List[Dict[str, str]]):
        self.database_schema = database_schema.strip()
        self.allowed_visualizations = allowed_visualizations

    def build_system_prompt(
        self,
        user_context: Dict[str, Any],
        page_context: str,
        response_schema: str,
    ) -> str:
        """Build the system prompt with app, page, and schema context."""

        user_line = self._format_user_context(user_context)
        visualization_lines = "\n".join(
            f"- {item['keyword']}: {item['description']}" for item in self.allowed_visualizations
        )

        return (
            f"{self.APP_HEADER}\n"
            f"Current page: {page_context}.\n"
            f"Current user: {user_line}.\n"
            "You must always respond with a single JSON object that follows the"
            " specified contract and never include markdown or prose outside of"
            " the JSON body."
            "\n\nDatabase schema (authoritative):\n"
            f"{self.database_schema}\n\n"
            "Allowed visualization keywords (choose up to three distinct entries):\n"
            f"{visualization_lines}\n\n"
            f"Response contract:\n{response_schema}"
        )

    def build_user_prompt(
        self,
        question: str,
        conversation_history: Optional[List[Dict[str, str]]],
    ) -> str:
        """Build the user prompt with the question and prior history."""

        history_block = self._format_history(conversation_history)
        example_block = self._example_response()

        return (
            f"User question: {question}\n\n"
            f"Conversation so far:{history_block}\n\n"
            "Generate a JSON response with a plain-text reply and zero to three"
            " visualization descriptors using the allowed keywords."
            f"\n\nExample:{example_block}"
        )

    def _format_user_context(self, context: Dict[str, Any]) -> str:
        parts = [f"role={context.get('role', 'user')}"]
        if context.get("person_id"):
            parts.append(f"person_id={context['person_id']}")
        if context.get("company_id"):
            parts.append(f"company_id={context['company_id']}")
        if context.get("username"):
            parts.append(f"username={context['username']}")
        return ", ".join(parts)

    def _format_history(self, history: Optional[List[Dict[str, str]]]) -> str:
        if not history:
            return " (no prior messages)"

        formatted = []
        for message in history[-6:]:
            role = message.get("role", "user")
            content = message.get("content", "")
            formatted.append(f"- {role}: {content}")
        return "\n" + "\n".join(formatted)

    def _example_response(self) -> str:
        return """{
    "reply": "Here is a quick overview of your spending trends.",
    "visualizations": [
        {
            "keyword": "monthly_expense_trend",
            "title": "Monthly expenses with rolling average",
            "chart_type": "line",
            "kind": "chart"
        },
        {
            "keyword": "expenses_by_category",
            "title": "Top expense categories",
            "chart_type": "bar",
            "kind": "table"
        }
    ]
}"""

