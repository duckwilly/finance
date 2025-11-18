"""
AI Chatbot Package
Financial chatbot with LLM integration, SQL generation, and chart visualization
"""
from .chatbot_core import FinancialChatbot
from .router import router, configure_dependencies, configure_templates
from .config import llm_config, chatbot_config, LLMProviderConfig, ChatbotConfig
from .llm_providers import LLMProviderFactory, OllamaProvider, ClaudeProvider, ChatGPTProvider
from .sql_generator import SQLGenerator, QuickTemplateManager
from .chart_generator import ChartGenerator

__version__ = "1.0.0"

__all__ = [
    "FinancialChatbot",
    "router",
    "configure_dependencies",
    "configure_templates",
    "llm_config",
    "chatbot_config",
    "LLMProviderConfig",
    "ChatbotConfig",
    "LLMProviderFactory",
    "OllamaProvider",
    "ClaudeProvider",
    "ChatGPTProvider",
    "SQLGenerator",
    "QuickTemplateManager",
    "ChartGenerator",
]
