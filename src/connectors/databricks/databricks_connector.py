from typing import Dict, List, Optional

from src.utils.logger import get_logger

logger = get_logger("databricks_connector")


class DatabricksConnector:
    """Stub for Databricks connection."""

    def __init__(self, config: Dict):
        target = config.get("target", {})
        self.workspace_url = target.get("workspace_url", "")
        self.token = target.get("token", "")
        self.catalog = target.get("catalog", "")

    def connect(self) -> bool:
        logger.warning("Databricks connector is a stub - not connecting")
        return False

    def execute(self, sql: str) -> List[Dict]:
        logger.info("Stub execute: %s", sql[:100])
        return []
