"""LLM Gatewayの設定管理モジュール。"""

from pathlib import Path
from typing import Any

import yaml
from loguru import logger

from src.llm_gateway.azure_provider import AzureProvider
from src.llm_gateway.gateway import LLMGateway
from src.llm_gateway.vertex_provider import VertexProvider


def create_gateway(config_path: str | None = None) -> LLMGateway:
    """設定ファイルからLLM Gatewayを構築する。"""
    config = _load_config(config_path)
    llm_config = config.get("llm_gateway", {})

    gateway = LLMGateway(llm_config)

    # Azure AI Foundryプロバイダー
    azure_config = llm_config.get("azure", {})
    if azure_config:
        azure_provider = AzureProvider(azure_config)
        if azure_provider.is_available():
            gateway.register_provider(azure_provider)
        else:
            logger.info("Azure AI Foundry: API key未設定のためスキップ")

    # GCP Vertex AIプロバイダー
    vertex_config = llm_config.get("vertex", {})
    if vertex_config:
        vertex_provider = VertexProvider(vertex_config)
        if vertex_provider.is_available():
            gateway.register_provider(vertex_provider)
        else:
            logger.info("GCP Vertex AI: プロジェクトID未設定のためスキップ")

    return gateway


def _load_config(config_path: str | None = None) -> dict[str, Any]:
    """設定ファイルをロードする。"""
    if config_path is None:
        config_path = "config/config.yaml"

    path = Path(config_path)
    if not path.exists():
        logger.warning(f"設定ファイルが見つかりません: {path}")
        return {}

    with open(path, encoding="utf-8") as f:
        return yaml.safe_load(f) or {}
