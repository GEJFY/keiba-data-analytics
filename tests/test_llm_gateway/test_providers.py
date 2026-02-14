"""LLMプロバイダーの単体テスト。"""

import os
from unittest.mock import patch

from src.llm_gateway.azure_provider import AzureProvider
from src.llm_gateway.vertex_provider import VertexProvider


class TestAzureProvider:
    """AzureProviderクラスのテスト。"""

    def test_name(self) -> None:
        """プロバイダー名がazureであること。"""
        provider = AzureProvider({"endpoint": "https://test.openai.azure.com"})
        assert provider.name() == "azure"

    def test_is_available_with_credentials(self) -> None:
        """endpointとAPI keyが設定されている場合Trueを返すこと。"""
        with patch.dict(os.environ, {"AZURE_OPENAI_API_KEY": "test-key"}):
            provider = AzureProvider({"endpoint": "https://test.openai.azure.com"})
            assert provider.is_available() is True

    def test_is_available_without_key(self) -> None:
        """API keyがない場合Falseを返すこと。"""
        with patch.dict(os.environ, {}, clear=True):
            # 環境変数をクリア
            env = {k: v for k, v in os.environ.items() if k != "AZURE_OPENAI_API_KEY"}
            with patch.dict(os.environ, env, clear=True):
                provider = AzureProvider({"endpoint": "https://test.openai.azure.com"})
                assert provider.is_available() is False

    def test_is_available_without_endpoint(self) -> None:
        """endpointがない場合Falseを返すこと。"""
        with patch.dict(os.environ, {"AZURE_OPENAI_API_KEY": "test-key"}):
            provider = AzureProvider({})
            assert provider.is_available() is False

    def test_config_api_version(self) -> None:
        """API versionがconfigから設定されること。"""
        provider = AzureProvider({
            "endpoint": "https://test.openai.azure.com",
            "api_version": "2025-01-01",
        })
        assert provider._api_version == "2025-01-01"

    def test_config_default_api_version(self) -> None:
        """デフォルトのAPI versionが正しいこと。"""
        provider = AzureProvider({"endpoint": "https://test.openai.azure.com"})
        assert provider._api_version == "2024-12-01-preview"


class TestVertexProvider:
    """VertexProviderクラスのテスト。"""

    def test_name(self) -> None:
        """プロバイダー名がvertexであること。"""
        provider = VertexProvider({"project_id": "test-project"})
        assert provider.name() == "vertex"

    def test_is_available_with_project_id(self) -> None:
        """project_idが設定されている場合Trueを返すこと。"""
        provider = VertexProvider({"project_id": "test-project"})
        assert provider.is_available() is True

    def test_is_available_without_project_id(self) -> None:
        """project_idがない場合Falseを返すこと。"""
        with patch.dict(os.environ, {}, clear=True):
            env = {k: v for k, v in os.environ.items() if k != "GCP_PROJECT_ID"}
            with patch.dict(os.environ, env, clear=True):
                provider = VertexProvider({})
                assert provider.is_available() is False

    def test_location_default(self) -> None:
        """デフォルトのlocationがus-central1であること。"""
        provider = VertexProvider({"project_id": "test"})
        assert provider._location == "us-central1"

    def test_location_custom(self) -> None:
        """カスタムlocationが設定されること。"""
        provider = VertexProvider({"project_id": "test", "location": "asia-northeast1"})
        assert provider._location == "asia-northeast1"
