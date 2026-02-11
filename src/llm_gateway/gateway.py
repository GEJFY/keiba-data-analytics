"""LLM Gateway本体 — プロバイダー抽象化層。

Azure AI FoundryとGCP Vertex AIを統一的に利用する
ゲートウェイを提供する。
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any

from loguru import logger


@dataclass
class LLMResponse:
    """LLMの応答データ。"""

    content: str
    model: str
    provider: str
    usage: dict[str, int]  # prompt_tokens, completion_tokens等
    raw_response: Any = None


class BaseLLMProvider(ABC):
    """LLMプロバイダーの基底クラス。"""

    @abstractmethod
    def name(self) -> str:
        """プロバイダー名を返す。"""
        ...

    @abstractmethod
    async def generate(
        self,
        prompt: str,
        model: str,
        system_prompt: str = "",
        temperature: float = 0.7,
        max_tokens: int = 4096,
    ) -> LLMResponse:
        """テキスト生成を実行する。"""
        ...

    @abstractmethod
    def is_available(self) -> bool:
        """プロバイダーが利用可能か確認する。"""
        ...


class LLMGateway:
    """LLM Gateway — マルチプロバイダー対応の統合ゲートウェイ。"""

    def __init__(self, config: dict[str, Any]) -> None:
        self._config = config
        self._providers: dict[str, BaseLLMProvider] = {}

    def register_provider(self, provider: BaseLLMProvider) -> None:
        """プロバイダーを登録する。"""
        self._providers[provider.name()] = provider
        logger.info(f"LLMプロバイダー登録: {provider.name()}")

    async def generate(
        self,
        use_case: str,
        prompt: str,
        system_prompt: str = "",
        temperature: float = 0.7,
        max_tokens: int = 4096,
    ) -> LLMResponse:
        """用途に応じたモデルでテキスト生成を実行する。

        プライマリモデルが失敗した場合、フォールバックモデルに自動切替する。
        """
        routing = self._config.get("model_routing", {}).get(use_case, {})
        primary = routing.get("primary", "")
        fallback = routing.get("fallback", "")

        # プライマリモデルで試行
        if primary:
            result = await self._try_generate(primary, prompt, system_prompt, temperature, max_tokens)
            if result:
                return result
            logger.warning(f"プライマリモデル {primary} 失敗。フォールバックに切替")

        # フォールバックモデルで試行
        if fallback:
            result = await self._try_generate(fallback, prompt, system_prompt, temperature, max_tokens)
            if result:
                return result
            logger.error(f"フォールバックモデル {fallback} も失敗")

        raise RuntimeError(f"用途 '{use_case}' で利用可能なモデルがありません")

    async def _try_generate(
        self,
        model_path: str,
        prompt: str,
        system_prompt: str,
        temperature: float,
        max_tokens: int,
    ) -> LLMResponse | None:
        """指定モデルでの生成を試行する。"""
        # model_pathの形式: "provider/model_key"
        parts = model_path.split("/", 1)
        if len(parts) != 2:
            logger.error(f"不正なモデルパス: {model_path}")
            return None

        provider_name, model_key = parts
        provider = self._providers.get(provider_name)
        if not provider:
            logger.error(f"未登録プロバイダー: {provider_name}")
            return None

        if not provider.is_available():
            logger.warning(f"プロバイダー {provider_name} は現在利用不可")
            return None

        # プロバイダー設定からモデルIDを解決
        provider_config = self._config.get(provider_name, {})
        models = provider_config.get("models", {})
        model_id = models.get(model_key, model_key)

        try:
            return await provider.generate(
                prompt=prompt,
                model=model_id,
                system_prompt=system_prompt,
                temperature=temperature,
                max_tokens=max_tokens,
            )
        except Exception as e:
            logger.error(f"モデル {model_path} でエラー: {e}")
            return None
