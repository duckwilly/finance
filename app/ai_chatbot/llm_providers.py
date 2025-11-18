"""
LLM Provider Abstraction Layer
Supports Ollama (local), Claude, and ChatGPT
"""
import httpx
import json
import logging
from typing import Optional, Dict, Any
from abc import ABC, abstractmethod

from .config import llm_config

logger = logging.getLogger(__name__)


class LLMProvider(ABC):
    """Base class for LLM providers"""

    @abstractmethod
    async def query(
        self,
        system_prompt: str,
        user_prompt: str,
        conversation_history: Optional[list] = None,
        json_mode: bool = True
    ) -> Dict[str, Any]:
        """
        Query the LLM with a prompt

        Args:
            system_prompt: System instructions for the LLM
            user_prompt: User's question/request
            conversation_history: Previous messages for context
            json_mode: Whether to enforce JSON response format

        Returns:
            Dict with 'content' key containing response, and optional 'usage' stats
        """
        pass


class OllamaProvider(LLMProvider):
    """Ollama local LLM provider"""

    def __init__(self, model: str = None):
        self.base_url = llm_config.ollama_base_url
        self.model = model or llm_config.ollama_default_model
        self.timeout = llm_config.ollama_timeout

    async def query(
        self,
        system_prompt: str,
        user_prompt: str,
        conversation_history: Optional[list] = None,
        json_mode: bool = True
    ) -> Dict[str, Any]:
        """Query Ollama API"""

        messages = []

        # Add system prompt
        if system_prompt:
            messages.append({"role": "system", "content": system_prompt})

        # Add conversation history
        if conversation_history:
            messages.extend(conversation_history)

        # Add current user prompt
        messages.append({"role": "user", "content": user_prompt})

        payload = {
            "model": self.model,
            "messages": messages,
            "stream": False
        }

        # Enforce JSON format if requested
        if json_mode:
            payload["format"] = "json"

        try:
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                response = await client.post(
                    f"{self.base_url}/api/chat",
                    json=payload
                )
                response.raise_for_status()
                data = response.json()

                content = data.get("message", {}).get("content", "")

                return {
                    "content": content,
                    "model": self.model,
                    "provider": "ollama"
                }

        except httpx.TimeoutException:
            logger.error(f"Ollama request timeout after {self.timeout}s")
            raise Exception("LLM request timed out. Please try again.")
        except httpx.HTTPError as e:
            logger.error(f"Ollama HTTP error: {str(e)}")
            if "out of memory" in str(e).lower():
                raise Exception("GPU out of memory. Try using llama3.2:1b model or restart Ollama.")
            raise Exception(f"Failed to connect to Ollama: {str(e)}")


class ClaudeProvider(LLMProvider):
    """Anthropic Claude API provider"""

    def __init__(self):
        self.api_key = llm_config.claude_api_key
        self.model = llm_config.claude_model
        self.max_tokens = llm_config.claude_max_tokens

        if not self.api_key:
            raise ValueError("Claude API key not configured. Set CLAUDE_API_KEY environment variable.")

    async def query(
        self,
        system_prompt: str,
        user_prompt: str,
        conversation_history: Optional[list] = None,
        json_mode: bool = True
    ) -> Dict[str, Any]:
        """Query Claude API"""

        messages = []

        # Add conversation history
        if conversation_history:
            messages.extend(conversation_history)

        # Add current user prompt
        user_content = user_prompt
        if json_mode:
            user_content += "\n\nIMPORTANT: Respond with valid JSON only."

        messages.append({"role": "user", "content": user_content})

        payload = {
            "model": self.model,
            "max_tokens": self.max_tokens,
            "system": system_prompt,
            "messages": messages
        }

        try:
            async with httpx.AsyncClient(timeout=60.0) as client:
                response = await client.post(
                    "https://api.anthropic.com/v1/messages",
                    headers={
                        "x-api-key": self.api_key,
                        "anthropic-version": "2023-06-01",
                        "content-type": "application/json"
                    },
                    json=payload
                )
                response.raise_for_status()
                data = response.json()

                content = data.get("content", [{}])[0].get("text", "")

                return {
                    "content": content,
                    "model": self.model,
                    "provider": "claude",
                    "usage": data.get("usage", {})
                }

        except httpx.HTTPError as e:
            logger.error(f"Claude API error: {str(e)}")
            raise Exception(f"Claude API request failed: {str(e)}")


class ChatGPTProvider(LLMProvider):
    """OpenAI ChatGPT API provider"""

    def __init__(self):
        self.api_key = llm_config.openai_api_key
        self.model = llm_config.openai_model
        self.max_tokens = llm_config.openai_max_tokens

        if not self.api_key:
            raise ValueError("OpenAI API key not configured. Set OPENAI_API_KEY environment variable.")

    async def query(
        self,
        system_prompt: str,
        user_prompt: str,
        conversation_history: Optional[list] = None,
        json_mode: bool = True
    ) -> Dict[str, Any]:
        """Query OpenAI ChatGPT API"""

        messages = []

        # Add system prompt
        if system_prompt:
            messages.append({"role": "system", "content": system_prompt})

        # Add conversation history
        if conversation_history:
            messages.extend(conversation_history)

        # Add current user prompt
        user_content = user_prompt
        if json_mode:
            user_content += "\n\nIMPORTANT: Respond with valid JSON only."

        messages.append({"role": "user", "content": user_content})

        payload = {
            "model": self.model,
            "messages": messages,
            "max_tokens": self.max_tokens
        }

        # Add JSON mode if supported
        if json_mode:
            payload["response_format"] = {"type": "json_object"}

        try:
            async with httpx.AsyncClient(timeout=60.0) as client:
                response = await client.post(
                    "https://api.openai.com/v1/chat/completions",
                    headers={
                        "Authorization": f"Bearer {self.api_key}",
                        "Content-Type": "application/json"
                    },
                    json=payload
                )
                response.raise_for_status()
                data = response.json()

                content = data.get("choices", [{}])[0].get("message", {}).get("content", "")

                return {
                    "content": content,
                    "model": self.model,
                    "provider": "chatgpt",
                    "usage": data.get("usage", {})
                }

        except httpx.HTTPError as e:
            logger.error(f"OpenAI API error: {str(e)}")
            raise Exception(f"OpenAI API request failed: {str(e)}")


class LLMProviderFactory:
    """Factory to create appropriate LLM provider"""

    @staticmethod
    def create(provider_name: str) -> LLMProvider:
        """
        Create LLM provider instance

        Args:
            provider_name: One of 'ollama', 'claude', 'chatgpt', or 'ollama:model_name'

        Returns:
            Configured LLM provider instance
        """
        if provider_name.startswith("ollama"):
            # Extract model name if specified (e.g., "ollama:llama3.2:1b")
            parts = provider_name.split(":", 1)
            model = parts[1] if len(parts) > 1 else None
            return OllamaProvider(model=model)

        elif provider_name == "claude":
            return ClaudeProvider()

        elif provider_name == "chatgpt":
            return ChatGPTProvider()

        else:
            raise ValueError(f"Unknown LLM provider: {provider_name}")
