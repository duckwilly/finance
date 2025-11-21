"""
LLM Provider Abstraction Layer
Supports Anthropic Claude and OpenAI GPT models
"""
import httpx
import logging
from typing import Optional, Dict, Any, Callable
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
        """Query the LLM with a prompt"""
        raise NotImplementedError


class ClaudeProvider(LLMProvider):
    """Anthropic Claude API provider"""

    def __init__(self, model: Optional[str] = None):
        self.api_key = llm_config.claude_api_key
        self.model = model or llm_config.claude_model
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

        user_content = user_prompt
        if json_mode:
            user_content += "\n\nIMPORTANT: Respond with valid JSON only."

        messages.append({"role": "user", "content": user_content})

        payload = {
            "model": self.model,
            "max_tokens": self.max_tokens,
            "system": system_prompt,
            "messages": messages,
        }

        try:
            async with httpx.AsyncClient(timeout=60.0) as client:
                response = await client.post(
                    "https://api.anthropic.com/v1/messages",
                    headers={
                        "x-api-key": self.api_key,
                        "anthropic-version": "2023-06-01",
                        "content-type": "application/json",
                    },
                    json=payload,
                )
                response.raise_for_status()
                data = response.json()

                content = data.get("content", [{}])[0].get("text", "")

                return {
                    "content": content,
                    "model": self.model,
                    "provider": "claude",
                    "usage": data.get("usage", {}),
                }

        except httpx.HTTPError as e:
            logger.error(f"Claude API error: {str(e)}")
            raise Exception(f"Claude API request failed: {str(e)}")


class ChatGPTProvider(LLMProvider):
    """OpenAI GPT API provider"""

    def __init__(self, model: Optional[str] = None):
        self.api_key = llm_config.openai_api_key
        self.model = model or llm_config.openai_model
        self.max_tokens = llm_config.openai_max_tokens

        if not self.api_key:
            raise ValueError("OpenAI API key not configured. Set OPENAI_API_KEY environment variable.")

    def _use_responses_api(self) -> bool:
        """Decide whether to call the newer Responses API."""
        if not self.model:
            return False
        lowered = self.model.lower()
        return any(token in lowered for token in ("gpt-5", "gpt-4.1"))

    def _supports_response_format(self) -> bool:
        """Only some Chat Completions models honor response_format."""
        if not self.model:
            return True
        lowered = self.model.lower()
        return not any(token in lowered for token in ("gpt-5", "gpt-4.1"))

    async def query(
        self,
        system_prompt: str,
        user_prompt: str,
        conversation_history: Optional[list] = None,
        json_mode: bool = True
    ) -> Dict[str, Any]:
        """Query OpenAI API using the appropriate endpoint"""

        def _build_history(as_blocks: bool = False):
            history = []
            if system_prompt:
                history.append({"role": "system", "content": system_prompt})
            if conversation_history:
                history.extend(conversation_history)

            user_content = user_prompt
            if json_mode:
                user_content += "\n\nIMPORTANT: Respond with valid JSON only."
            history.append({"role": "user", "content": user_content})

            if not as_blocks:
                return history

            block_messages = []
            for msg in history:
                block_messages.append({
                    "role": msg.get("role"),
                    "content": [{"type": "text", "text": msg.get("content", "")}],
                })
            return block_messages

        use_responses_api = self._use_responses_api()
        include_response_format = json_mode and not use_responses_api and self._supports_response_format()

        if use_responses_api:
            payload = {
                "model": self.model,
                "input": _build_history(as_blocks=True),
                "max_output_tokens": self.max_tokens,
            }
            endpoint = "https://api.openai.com/v1/responses"
        else:
            payload = {
                "model": self.model,
                "messages": _build_history(),
                "max_tokens": self.max_tokens,
            }
            if include_response_format:
                payload["response_format"] = {"type": "json_object"}
            endpoint = "https://api.openai.com/v1/chat/completions"

        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }

        try:
            async with httpx.AsyncClient(timeout=60.0) as client:
                response = await client.post(endpoint, headers=headers, json=payload)
                if response.status_code == 400 and include_response_format:
                    # Retry once without response_format for models that silently dropped support
                    payload.pop("response_format", None)
                    include_response_format = False
                    response = await client.post(endpoint, headers=headers, json=payload)

                response.raise_for_status()
                data = response.json()

                if use_responses_api:
                    content = ChatGPTProvider._extract_responses_text(data)
                else:
                    content = data.get("choices", [{}])[0].get("message", {}).get("content", "")

                return {
                    "content": content,
                    "model": self.model,
                    "provider": "chatgpt",
                    "usage": data.get("usage", {}),
                }

        except httpx.HTTPError as e:
            logger.error(f"OpenAI API error: {str(e)}")
            raise Exception(f"OpenAI API request failed: {str(e)}")

    @staticmethod
    def _extract_responses_text(payload: Dict[str, Any]) -> str:
        """Flatten the `output` array from the Responses API."""
        outputs = payload.get("output", [])
        if not outputs:
            return ""

        texts: list[str] = []
        for item in outputs:
            if item.get("type") != "message":
                continue
            for content in item.get("content", []):
                if content.get("type") == "text":
                    texts.append(content.get("text", ""))
        return "\n".join(texts).strip()


class LLMProviderFactory:
    """Factory to create appropriate LLM provider"""

    FRIENDLY_ALIASES: Dict[str, tuple[str, Callable[[], Optional[str]]]] = {
        "claude-haiku-4.5": ("claude", lambda: llm_config.claude_model),
        "claude-sonnet-4.5-mini": ("claude", lambda: llm_config.claude_model),
        "gpt-4o-mini": ("chatgpt", lambda: llm_config.openai_model),
        "gpt-4o": ("chatgpt", lambda: llm_config.openai_model),
    }

    LEGACY_NAMES = {
        "claude": "claude-haiku-4.5",
        "chatgpt": "gpt-4o-mini",
        "gpt-5-mini": "gpt-4o-mini",
        "gpt-5.1-mini": "gpt-4o-mini",
    }

    @staticmethod
    def create(provider_name: str) -> LLMProvider:
        """
        Create LLM provider instance

        Args:
            provider_name: Supported model identifier or friendly alias

        Returns:
            Configured LLM provider instance
        """
        normalized = (provider_name or "").strip().lower()

        alias_entry = LLMProviderFactory.FRIENDLY_ALIASES.get(normalized)
        if alias_entry:
            provider_key, resolver = alias_entry
            resolved_model = resolver()
            return LLMProviderFactory._build_provider(provider_key, resolved_model)

        if normalized in LLMProviderFactory.LEGACY_NAMES:
            normalized = LLMProviderFactory.LEGACY_NAMES[normalized]

        if normalized in {"", "claude-haiku-4.5"}:
            return ClaudeProvider()
        if normalized in {"chatgpt", "gpt-4o-mini"}:
            return ChatGPTProvider()

        if normalized.startswith("claude"):
            return ClaudeProvider(model=provider_name)
        if normalized.startswith("gpt"):
            return ChatGPTProvider(model=provider_name)

        raise ValueError(f"Unknown LLM provider: {provider_name}")

    @staticmethod
    def _build_provider(provider_key: str, model_override: Optional[str]) -> LLMProvider:
        if provider_key == "claude":
            return ClaudeProvider(model=model_override)
        if provider_key == "chatgpt":
            return ChatGPTProvider(model=model_override)
        raise ValueError(f"Unsupported provider key: {provider_key}")
