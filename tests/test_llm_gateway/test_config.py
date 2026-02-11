"""LLM Gateway設定管理の単体テスト。"""

import os
from pathlib import Path
from unittest.mock import patch

import pytest

from src.llm_gateway.config import _load_config, create_gateway
from src.llm_gateway.gateway import LLMGateway


@pytest.fixture
def config_file(tmp_path: Path) -> Path:
    """テスト用config.yamlを生成する。"""
    config_content = """
llm_gateway:
  model_routing:
    factor_generation:
      primary: azure/claude
      fallback: vertex/gemini
  azure:
    endpoint: https://test.openai.azure.com
    api_version: "2024-12-01-preview"
    models:
      claude: claude-opus-4-6
  vertex:
    project_id: test-project
    location: us-central1
    models:
      gemini: gemini-3.0-pro
"""
    config_path = tmp_path / "config.yaml"
    config_path.write_text(config_content, encoding="utf-8")
    return config_path


class TestLoadConfig:
    """_load_config関数のテスト。"""

    def test_load_existing_config(self, config_file: Path) -> None:
        """存在するconfigファイルを読み込めること。"""
        config = _load_config(str(config_file))
        assert "llm_gateway" in config
        assert "model_routing" in config["llm_gateway"]

    def test_load_nonexistent_config(self) -> None:
        """存在しないファイルの場合、空dictを返すこと。"""
        config = _load_config("/nonexistent/path/config.yaml")
        assert config == {}

    def test_load_default_path(self) -> None:
        """config_pathがNoneの場合、デフォルトパスを使用すること。"""
        # デフォルトパスは通常存在しないので空dictが返る
        config = _load_config(None)
        assert isinstance(config, dict)


class TestCreateGateway:
    """create_gateway関数のテスト。"""

    def test_create_gateway_returns_instance(self, config_file: Path) -> None:
        """LLMGatewayインスタンスが返ること。"""
        with patch.dict(os.environ, {"AZURE_OPENAI_API_KEY": "test-key"}):
            gateway = create_gateway(str(config_file))
            assert isinstance(gateway, LLMGateway)

    def test_create_gateway_registers_available_providers(self, config_file: Path) -> None:
        """利用可能なプロバイダーが登録されること。"""
        with patch.dict(os.environ, {"AZURE_OPENAI_API_KEY": "test-key"}):
            gateway = create_gateway(str(config_file))
            # Azure (key設定済み) + Vertex (project_id設定済み) = 2つ登録
            assert "azure" in gateway._providers
            assert "vertex" in gateway._providers

    def test_create_gateway_skips_unavailable_providers(self, tmp_path: Path) -> None:
        """認証情報がないプロバイダーがスキップされること。"""
        config_content = """
llm_gateway:
  azure:
    endpoint: ""
  vertex: {}
"""
        config_path = tmp_path / "config.yaml"
        config_path.write_text(config_content, encoding="utf-8")

        env = {k: v for k, v in os.environ.items()
               if k not in ("AZURE_OPENAI_API_KEY", "GCP_PROJECT_ID", "AZURE_OPENAI_ENDPOINT")}
        with patch.dict(os.environ, env, clear=True):
            gateway = create_gateway(str(config_path))
            assert len(gateway._providers) == 0

    def test_create_gateway_with_no_config(self) -> None:
        """設定ファイルなしでも空のGatewayが返ること。"""
        gateway = create_gateway("/nonexistent/config.yaml")
        assert isinstance(gateway, LLMGateway)
        assert len(gateway._providers) == 0
