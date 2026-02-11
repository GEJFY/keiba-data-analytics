"""LLM Gateway本体 — プロバイダー抽象化層。

Azure AI FoundryとGCP Vertex AIを統一的に利用する
ゲートウェイを提供する。

ルーティング:
    config.yamlのmodel_routingセクションで、用途(use_case)ごとに
    プライマリ/フォールバックのモデルパス("provider/model_key")を指定。
    プライマリが失敗した場合、自動的にフォールバックに切替。
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any

from loguru import logger


@dataclass
class LLMResponse:
    """LLMの応答データ。

    Attributes:
        content: 生成テキスト
        model: 使用されたモデルID
        provider: プロバイダー名（azure / vertex）
        usage: トークン使用量（prompt_tokens, completion_tokens）
        raw_response: プロバイダー固有のレスポンスオブジェクト
    """

    content: str
    model: str
    provider: str
    usage: dict[str, int]
    raw_response: Any = None


class BaseLLMProvider(ABC):
    """LLMプロバイダーの基底クラス。

    新しいプロバイダーを追加する場合はこのクラスを継承し、
    name(), generate(), is_available() を実装する。
    """

    @abstractmethod
    def name(self) -> str:
        """プロバイダー名を返す（azure, vertex等）。"""
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
        """テキスト生成を実行する。

        Args:
            prompt: ユーザープロンプト
            model: モデルID
            system_prompt: システムプロンプト（省略可）
            temperature: 生成温度（0.0〜2.0）
            max_tokens: 最大出力トークン数

        Returns:
            LLMResponse
        """
        ...

    @abstractmethod
    def is_available(self) -> bool:
        """プロバイダーが利用可能か確認する（認証情報の有無等）。"""
        ...


class LLMGateway:
    """LLM Gateway — マルチプロバイダー対応の統合ゲートウェイ。

    model_routingの設定に基づき、用途ごとに最適なモデルを選択。
    プライマリ失敗時は自動的にフォールバックに切替。
    """

    def __init__(self, config: dict[str, Any]) -> None:
        self._config = config
        self._providers: dict[str, BaseLLMProvider] = {}

    def register_provider(self, provider: BaseLLMProvider) -> None:
        """プロバイダーを登録する。

        Args:
            provider: BaseLLMProviderの実装インスタンス
        """
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

        Args:
            use_case: 用途キー（factor_generation, race_analysis等）
            prompt: ユーザープロンプト
            system_prompt: システムプロンプト
            temperature: 生成温度
            max_tokens: 最大出力トークン数

        Returns:
            LLMResponse

        Raises:
            RuntimeError: プライマリ・フォールバック両方失敗した場合
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
        """指定モデルでの生成を試行する。

        model_pathの形式: "provider_name/model_key"
        （例: "azure/claude", "vertex/gemini"）
        """
        parts = model_path.split("/", 1)
        if len(parts) != 2:
            logger.error(f"不正なモデルパス（'provider/model_key' 形式が必要）: {model_path}")
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
            logger.error(f"モデル {model_path} (ID: {model_id}) でエラー: {e}")
            return None
