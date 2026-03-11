import os
import re
import copy
from typing import Dict, Any, Optional

import yaml

from src.utils.logger import get_logger

logger = get_logger("config_loader")

_ENV_VAR_PATTERN = re.compile(r'\$\{(\w+)\}')

REQUIRED_FIELDS = [
    "project.name",
    "sas_environment.code_paths",
]


class ConfigLoader:
    def __init__(self, config_path: str, override_path: Optional[str] = None):
        self.config_path = config_path
        self.override_path = override_path
        self._config: Dict[str, Any] = {}

    def load(self) -> Dict[str, Any]:
        with open(self.config_path, "r", encoding="utf-8") as f:
            self._config = yaml.safe_load(f) or {}

        if self.override_path:
            with open(self.override_path, "r", encoding="utf-8") as f:
                override = yaml.safe_load(f) or {}
            self._config = self._deep_merge(self._config, override)

        self._expand_env_vars(self._config)
        self._validate()
        logger.info("Configuration loaded from %s", self.config_path)
        return self._config

    def _deep_merge(self, base: Dict, override: Dict) -> Dict:
        result = copy.deepcopy(base)
        for key, value in override.items():
            if key in result and isinstance(result[key], dict) and isinstance(value, dict):
                result[key] = self._deep_merge(result[key], value)
            else:
                result[key] = copy.deepcopy(value)
        return result

    def _expand_env_vars(self, obj: Any, parent: Any = None, key: Any = None):
        if isinstance(obj, str):
            def replacer(match):
                var_name = match.group(1)
                return os.environ.get(var_name, match.group(0))
            expanded = _ENV_VAR_PATTERN.sub(replacer, obj)
            if parent is not None and key is not None:
                parent[key] = expanded
        elif isinstance(obj, dict):
            for k, v in obj.items():
                self._expand_env_vars(v, obj, k)
        elif isinstance(obj, list):
            for i, v in enumerate(obj):
                self._expand_env_vars(v, obj, i)

    def _validate(self):
        for field_path in REQUIRED_FIELDS:
            parts = field_path.split(".")
            current = self._config
            for part in parts:
                if not isinstance(current, dict) or part not in current:
                    raise ValueError(f"Missing required config field: {field_path}")
                current = current[part]
        logger.info("Configuration validation passed")

    @property
    def config(self) -> Dict[str, Any]:
        return self._config
