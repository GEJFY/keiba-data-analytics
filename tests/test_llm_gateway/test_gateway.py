"""LLM Gatewayの単体テスト。"""

import pytest

from src.llm_gateway.gateway import BaseLLMProvider, LLMGateway, LLMResponse


class MockProvider(BaseLLMProvider):
    """テスト用モックプロバイダー。"""

    def __init__(self, name: str = "mock", available: bool = True, fail: bool = False) -> None:
        self._name = name
        self._available = available
        self._fail = fail

    def name(self) -> str:
        return self._name

    def is_available(self) -> bool:
        return self._available

    async def generate(
        self,
        prompt: str,
        model: str,
        system_prompt: str = "",
        temperature: float = 0.7,
        max_tokens: int = 4096,
    ) -> LLMResponse:
        if self._fail:
            raise RuntimeError("Mock failure")
        return LLMResponse(
            content=f"Mock response: {prompt[:50]}",
            model=model,
            provider=self._name,
            usage={"prompt_tokens": 10, "completion_tokens": 20},
        )


@pytest.mark.unit
class TestLLMGateway:
    """LLMGatewayクラスのテスト。"""

    def test_register_provider(self) -> None:
        """プロバイダーが正常に登録されること。"""
        gateway = LLMGateway(config={})
        provider = MockProvider("test_provider")
        gateway.register_provider(provider)
        assert "test_provider" in gateway._providers

    @pytest.mark.asyncio
    async def test_generate_with_primary(self) -> None:
        """プライマリモデルで正常に生成されること。"""
        config = {
            "model_routing": {
                "test_case": {
                    "primary": "mock/test_model",
                    "fallback": "mock/fallback_model",
                },
            },
            "mock": {"models": {"test_model": "actual-model-id"}},
        }
        gateway = LLMGateway(config)
        gateway.register_provider(MockProvider("mock"))

        result = await gateway.generate("test_case", "Hello")
        assert result.content.startswith("Mock response:")
        assert result.provider == "mock"

    @pytest.mark.asyncio
    async def test_fallback_on_primary_failure(self) -> None:
        """プライマリ失敗時にフォールバックに切り替わること。"""
        config = {
            "model_routing": {
                "test_case": {
                    "primary": "failing/model",
                    "fallback": "working/model",
                },
            },
            "failing": {"models": {"model": "fail-model"}},
            "working": {"models": {"model": "work-model"}},
        }
        gateway = LLMGateway(config)
        gateway.register_provider(MockProvider("failing", fail=True))
        gateway.register_provider(MockProvider("working"))

        result = await gateway.generate("test_case", "Hello")
        assert result.provider == "working"

    @pytest.mark.asyncio
    async def test_error_when_no_provider_available(self) -> None:
        """利用可能なプロバイダーがない場合にエラーが発生すること。"""
        config = {
            "model_routing": {
                "test_case": {
                    "primary": "unavailable/model",
                },
            },
        }
        gateway = LLMGateway(config)
        gateway.register_provider(MockProvider("unavailable", available=False))

        with pytest.raises(RuntimeError, match="利用可能なモデルがありません"):
            await gateway.generate("test_case", "Hello")
