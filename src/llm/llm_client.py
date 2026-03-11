import os
import json
import time
from typing import Dict, Optional

from src.utils.logger import get_logger

logger = get_logger("llm_client")


class LLMClient:
    def __init__(self, config: Dict):
        llm_config = config.get("llm", {})
        self.api_key = os.environ.get(llm_config.get("api_key_env", "OPENAI_API_KEY"), "")
        self.base_url = llm_config.get("base_url", "https://api.openai.com/v1")
        self.models = llm_config.get("models", {
            "fast": "gpt-4o-mini",
            "balanced": "gpt-4o",
            "powerful": "gpt-4o",
        })
        self.max_retries = 3

    @property
    def is_available(self) -> bool:
        return bool(self.api_key)

    def call(self, prompt: str, system_prompt: Optional[str] = None,
             model_tier: str = "balanced", temperature: float = 0.3,
             max_tokens: int = 4000) -> str:
        if not self.is_available:
            logger.warning("LLM not available (no API key)")
            return ""

        model = self.models.get(model_tier, self.models.get("balanced", "gpt-4o"))
        messages = []
        if system_prompt:
            messages.append({"role": "system", "content": system_prompt})
        messages.append({"role": "user", "content": prompt})

        for attempt in range(self.max_retries):
            try:
                import requests
                headers = {
                    "Authorization": f"Bearer {self.api_key}",
                    "Content-Type": "application/json",
                }
                payload = {
                    "model": model,
                    "messages": messages,
                    "temperature": temperature,
                    "max_tokens": max_tokens,
                }
                resp = requests.post(
                    f"{self.base_url}/chat/completions",
                    headers=headers,
                    json=payload,
                    timeout=120,
                )
                resp.raise_for_status()
                data = resp.json()
                return data["choices"][0]["message"]["content"]
            except Exception as e:
                logger.warning("LLM call attempt %d failed: %s", attempt + 1, e)
                if attempt < self.max_retries - 1:
                    time.sleep(2 ** attempt)

        logger.error("All LLM call attempts failed")
        return ""
