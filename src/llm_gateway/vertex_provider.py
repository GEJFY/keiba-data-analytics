"""GCP Vertex AI LLMプロバイダー。"""

import os
from typing import Any

from loguru import logger

from src.llm_gateway.gateway import BaseLLMProvider, LLMResponse


class VertexProvider(BaseLLMProvider):
    """GCP Vertex AI経由のLLMプロバイダー。"""

    def __init__(self, config: dict[str, Any]) -> None:
        self._config = config
        self._project_id = config.get("project_id", os.environ.get("GCP_PROJECT_ID", ""))
        self._location = config.get("location", "us-central1")

    def name(self) -> str:
        return "vertex"

    def is_available(self) -> bool:
        return bool(self._project_id) and not self._project_id.startswith("your-")

    async def generate(
        self,
        prompt: str,
        model: str,
        system_prompt: str = "",
        temperature: float = 0.7,
        max_tokens: int = 4096,
    ) -> LLMResponse:
        """Vertex AI経由でテキスト生成を実行する。"""
        import vertexai
        from vertexai.generative_models import GenerativeModel

        vertexai.init(project=self._project_id, location=self._location)

        generative_model = GenerativeModel(
            model_name=model,
            system_instruction=system_prompt if system_prompt else None,
        )

        response = generative_model.generate_content(
            prompt,
            generation_config={
                "temperature": temperature,
                "max_output_tokens": max_tokens,
            },
        )

        content = response.text if response.text else ""
        usage = {
            "prompt_tokens": getattr(response.usage_metadata, "prompt_token_count", 0),
            "completion_tokens": getattr(response.usage_metadata, "candidates_token_count", 0),
        }

        logger.debug(f"Vertex AI応答: model={model}, tokens={usage}")

        return LLMResponse(
            content=content,
            model=model,
            provider="vertex",
            usage=usage,
            raw_response=response,
        )
