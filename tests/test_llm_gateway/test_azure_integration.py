"""Azure AI Foundry接続統合テスト。

Gateway初期化フロー（dotenvロード → config読込 → プロバイダー登録）をテストする。
実際のAzure APIは呼ばない。
"""

import os
from pathlib import Path
from unittest.mock import patch

import pytest

from src.llm_gateway.azure_provider import AzureProvider
from src.llm_gateway.config import create_gateway
from src.llm_gateway.gateway import LLMGateway


@pytest.fixture
def azure_config_file(tmp_path: Path) -> Path:
    """Azure AI Foundry優先のconfig.yamlを生成する。"""
    config_content = """
llm_gateway:
  default_provider: "azure"
  azure:
    endpoint: "https://test-resource.openai.azure.com/"
    api_version: "2024-12-01-preview"
    models:
      gpt4o: "gpt-4o"
      gpt4o_mini: "gpt-4o-mini"
  model_routing:
    race_analysis:
      primary: "azure/gpt4o"
      fallback: "azure/gpt4o_mini"
    factor_generation:
      primary: "azure/gpt4o"
      fallback: "azure/gpt4o_mini"
"""
    config_path = tmp_path / "config.yaml"
    config_path.write_text(config_content, encoding="utf-8")
    return config_path


@pytest.mark.unit
class TestAzureGatewayInitialization:
    """Azure AI Foundry Gateway初期化フローのテスト。"""

    def test_gateway_created_with_azure_key(self, azure_config_file: Path) -> None:
        """API key設定時にAzureプロバイダーが登録されること。"""
        with patch.dict(os.environ, {"AZURE_OPENAI_API_KEY": "test-key-123"}):
            gateway = create_gateway(str(azure_config_file))
            assert isinstance(gateway, LLMGateway)
            assert "azure" in gateway._providers
            assert isinstance(gateway._providers["azure"], AzureProvider)

    def test_gateway_skips_azure_without_key(self, azure_config_file: Path) -> None:
        """API key未設定時にAzureプロバイダーがスキップされること。"""
        env = {k: v for k, v in os.environ.items()
               if k not in ("AZURE_OPENAI_API_KEY", "AZURE_OPENAI_ENDPOINT")}
        with patch.dict(os.environ, env, clear=True):
            gateway = create_gateway(str(azure_config_file))
            assert "azure" not in gateway._providers

    def test_azure_provider_endpoint_from_config(self) -> None:
        """endpointがconfig設定から取得されること。"""
        provider = AzureProvider({
            "endpoint": "https://my-resource.openai.azure.com/",
            "api_version": "2024-12-01-preview",
        })
        assert provider._endpoint == "https://my-resource.openai.azure.com/"

    def test_azure_provider_endpoint_from_env(self) -> None:
        """endpointが環境変数からフォールバックされること。"""
        with patch.dict(os.environ, {
            "AZURE_OPENAI_ENDPOINT": "https://env-resource.openai.azure.com/",
        }):
            provider = AzureProvider({})
            assert provider._endpoint == "https://env-resource.openai.azure.com/"

    @pytest.mark.asyncio
    async def test_gateway_routing_azure_only(self, azure_config_file: Path) -> None:
        """Azure優先ルーティングでプライマリが選択されること。"""
        from tests.test_llm_gateway.test_gateway import MockProvider

        gateway = create_gateway(str(azure_config_file))
        # Mockプロバイダーに差し替え
        gateway._providers["azure"] = MockProvider("azure")

        result = await gateway.generate("race_analysis", "テストプロンプト")
        assert result.provider == "azure"
        assert result.content.startswith("Mock response:")

    def test_gateway_has_no_providers_without_credentials(self, azure_config_file: Path) -> None:
        """認証情報なしの場合、プロバイダー数が0であること。"""
        env = {k: v for k, v in os.environ.items()
               if k not in ("AZURE_OPENAI_API_KEY", "AZURE_OPENAI_ENDPOINT", "GCP_PROJECT_ID")}
        with patch.dict(os.environ, env, clear=True):
            gateway = create_gateway(str(azure_config_file))
            assert len(gateway._providers) == 0
