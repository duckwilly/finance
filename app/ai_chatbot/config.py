"""
AI Chatbot Configuration Module
Centralized configuration for LLM providers and chatbot settings
"""
import os
from typing import Literal
from pydantic import BaseModel, Field


class LLMProviderConfig(BaseModel):
    """Configuration for LLM providers"""

    # Ollama Configuration
    ollama_base_url: str = Field(
        default_factory=lambda: os.getenv("OLLAMA_BASE_URL", "http://localhost:11434")
    )
    ollama_default_model: str = "llama3:latest"
    ollama_timeout: int = 600  # 10 minutes

    # Claude Configuration
    claude_api_key: str = Field(
        default_factory=lambda: os.getenv("CLAUDE_API_KEY", "")
    )
    claude_model: str = "claude-sonnet-4-20250514"
    claude_max_tokens: int = 2000

    # OpenAI Configuration
    openai_api_key: str = Field(
        default_factory=lambda: os.getenv("OPENAI_API_KEY", "")
    )
    openai_model: str = "gpt-4o"
    openai_max_tokens: int = 2000


class ChatbotConfig(BaseModel):
    """General chatbot configuration"""

    # Response settings
    max_conversation_history: int = 6
    max_sql_results: int = 1000

    # Chart settings
    chart_color_palette: list[str] = [
        "#5f6afc",  # Primary Blue
        "#60a5fa",  # Sky Blue
        "#3b82f6",  # Medium Blue
        "#a7b4ff",  # Light Purple Blue
        "#93c5fd",  # Soft Blue
    ]
    chart_default_type: Literal["bar", "line", "pie", "doughnut"] = "bar"

    # Security settings
    allowed_sql_operations: list[str] = ["SELECT"]
    blocked_sql_keywords: list[str] = [
        "DROP", "DELETE", "UPDATE", "INSERT", "ALTER",
        "CREATE", "TRUNCATE", "EXEC", "EXECUTE"
    ]

    # Quick template settings
    enable_quick_templates: bool = True
    quick_template_keywords: dict = {
        "expenses_by_category": ["expenses by category", "spending by category"],
        "income_by_category": ["income by category", "revenue by category"],
        "monthly_comparison": ["monthly income vs expenses", "income vs expenses monthly"],
        "yearly_comparison": ["yearly income vs expenses", "income vs expenses yearly"],
    }


# Global config instances
llm_config = LLMProviderConfig()
chatbot_config = ChatbotConfig()
