"""Azure AI Foundry LLMプロバイダー。"""

import os
from typing import Any

from loguru import logger

from src.llm_gateway.gateway import BaseLLMProvider, LLMResponse


class AzureProvider(BaseLLMProvider):
    """Azure AI Foundry経由のLLMプロバイダー。"""

    def __init__(self, config: dict[str, Any]) -> None:
        self._config = config
        self._endpoint = config.get("endpoint", os.environ.get("AZURE_OPENAI_ENDPOINT", ""))
        self._api_key = os.environ.get("AZURE_OPENAI_API_KEY", "")
        self._api_version = config.get("api_version", "2024-12-01-preview")

    def name(self) -> str:
        return "azure"

    def is_available(self) -> bool:
        return bool(self._endpoint and self._api_key)

    async def generate(
        self,
        prompt: str,
        model: str,
        system_prompt: str = "",
        temperature: float = 0.7,
        max_tokens: int = 4096,
    ) -> LLMResponse:
        """Azure AI Foundry経由でテキスト生成を実行する。"""
        from openai import AsyncAzureOpenAI

        client = AsyncAzureOpenAI(
            azure_endpoint=self._endpoint,
            api_key=self._api_key,
            api_version=self._api_version,
        )

        messages: list[dict[str, str]] = []
        if system_prompt:
            messages.append({"role": "system", "content": system_prompt})
        messages.append({"role": "user", "content": prompt})

        response = await client.chat.completions.create(
            model=model,
            messages=messages,
            temperature=temperature,
            max_tokens=max_tokens,
        )

        content = response.choices[0].message.content or ""
        usage = {
            "prompt_tokens": response.usage.prompt_tokens if response.usage else 0,
            "completion_tokens": response.usage.completion_tokens if response.usage else 0,
        }

        logger.debug(f"Azure応答: model={model}, tokens={usage}")

        return LLMResponse(
            content=content,
            model=model,
            provider="azure",
            usage=usage,
            raw_response=response,
        )
