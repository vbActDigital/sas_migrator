from typing import Dict, List, Optional

from src.utils.logger import get_logger

logger = get_logger("snowflake_connector")


class SnowflakeConnector:
    """Stub for Snowflake connection. Generates correct code but does not execute."""

    def __init__(self, config: Dict):
        target = config.get("target", {})
        self.account = target.get("account", "")
        self.user = target.get("user", "")
        self.warehouse = target.get("warehouse", "")
        self.database = target.get("database", "")
        self.role = target.get("role", "")
        self._connected = False

    def connect(self) -> bool:
        logger.warning("Snowflake connector is a stub - not connecting")
        return False

    def execute(self, sql: str) -> List[Dict]:
        logger.info("Stub execute: %s", sql[:100])
        return []

    def disconnect(self):
        self._connected = False
