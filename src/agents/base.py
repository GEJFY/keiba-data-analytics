"""AIエージェント基底クラス。

全エージェントはBaseAgentを継承し、run()メソッドを実装する。
LLM Gatewayが未設定の場合はフォールバック応答を返す。
"""

from abc import ABC, abstractmethod
from typing import Any

from loguru import logger

from src.llm_gateway.gateway import LLMGateway, LLMResponse


class BaseAgent(ABC):
    """AIエージェントの基底クラス。"""

    def __init__(self, gateway: LLMGateway | None = None) -> None:
        self._gateway = gateway

    @abstractmethod
    def agent_name(self) -> str:
        """エージェント名を返す。"""
        ...

    @abstractmethod
    def use_case(self) -> str:
        """LLM Gatewayのuse_caseキーを返す。"""
        ...

    @abstractmethod
    def build_prompt(self, context: dict[str, Any]) -> tuple[str, str]:
        """プロンプトを構築する。

        Args:
            context: エージェント固有の入力データ

        Returns:
            (system_prompt, user_prompt) のタプル
        """
        ...

    @abstractmethod
    def fallback_response(self, context: dict[str, Any]) -> str:
        """LLM未設定時のフォールバック応答を生成する。"""
        ...

    async def run(self, context: dict[str, Any]) -> str:
        """エージェントを実行し、テキスト応答を返す。

        LLM Gatewayが未設定の場合はfallback_response()を使用する。

        Args:
            context: エージェント固有の入力データ

        Returns:
            生成されたテキスト応答
        """
        if not self._gateway:
            logger.info(f"{self.agent_name()}: LLM未設定 → フォールバック応答")
            return self.fallback_response(context)

        system_prompt, user_prompt = self.build_prompt(context)

        try:
            response: LLMResponse = await self._gateway.generate(
                use_case=self.use_case(),
                prompt=user_prompt,
                system_prompt=system_prompt,
                temperature=0.3,
                max_tokens=2048,
            )
            return response.content
        except Exception as e:
            logger.warning(f"{self.agent_name()}: LLM呼び出し失敗 ({e}) → フォールバック応答")
            return self.fallback_response(context)
