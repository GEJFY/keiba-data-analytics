"""Azure AI Foundry LLMプロバイダー。"""

import os
from typing import Any

import requests
from loguru import logger

from src.llm_gateway.gateway import BaseLLMProvider, LLMResponse


class AzureProvider(BaseLLMProvider):
    """Azure AI Foundry経由のLLMプロバイダー。

    REST APIを直接使用し、openai SDKへの依存を排除。
    """

    def __init__(self, config: dict[str, Any]) -> None:
        self._config = config
        self._endpoint = config.get("endpoint", os.environ.get("AZURE_OPENAI_ENDPOINT", ""))
        self._api_key = os.environ.get("AZURE_OPENAI_API_KEY", "")
        self._api_version = config.get("api_version", "2024-12-01-preview")
        self._timeout = config.get("timeout", 60)

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
        """Azure AI Foundry REST API経由でテキスト生成を実行する。"""
        endpoint = self._endpoint.rstrip("/")
        url = (
            f"{endpoint}/openai/deployments/{model}"
            f"/chat/completions?api-version={self._api_version}"
        )
        headers = {
            "Content-Type": "application/json",
            "api-key": self._api_key,
        }

        messages: list[dict[str, str]] = []
        if system_prompt:
            messages.append({"role": "system", "content": system_prompt})
        messages.append({"role": "user", "content": prompt})

        body = {
            "messages": messages,
            "temperature": temperature,
            "max_tokens": max_tokens,
        }

        resp = requests.post(url, headers=headers, json=body, timeout=self._timeout)
        resp.raise_for_status()
        data = resp.json()

        content = data["choices"][0]["message"]["content"]
        usage_data = data.get("usage", {})
        usage = {
            "prompt_tokens": usage_data.get("prompt_tokens", 0),
            "completion_tokens": usage_data.get("completion_tokens", 0),
        }

        logger.debug(f"Azure応答: model={model}, tokens={usage}")

        return LLMResponse(
            content=content,
            model=model,
            provider="azure",
            usage=usage,
            raw_response=data,
        )
