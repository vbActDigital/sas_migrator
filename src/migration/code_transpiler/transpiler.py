from typing import Dict, List

from src.utils.logger import get_logger

logger = get_logger("transpiler")


class PySparkTranspiler:
    """Transpiles SAS code to PySpark (Databricks). Stub for MVP2."""

    def __init__(self, config: Dict):
        self.config = config
        self.library_mapping = config.get("library_mapping", {})

    def transpile(self, parsed_program: Dict) -> Dict:
        logger.info("PySpark transpilation is a stub")
        return {
            "pyspark_code": "# PySpark transpilation not yet implemented",
            "gaps": ["Full PySpark transpilation pending"],
            "warnings": [],
            "coverage_pct": 0.0,
        }
