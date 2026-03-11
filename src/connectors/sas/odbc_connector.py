from typing import Dict, List, Optional

from src.utils.logger import get_logger

logger = get_logger("odbc_connector")


class SASODBCConnector:
    """Stub for SAS ODBC connection. Future implementation."""

    def __init__(self, config: Dict):
        self.dsn = config.get("odbc", {}).get("dsn", "")
        self._connected = False

    def connect(self) -> bool:
        logger.warning("SAS ODBC connector is a stub - not connecting")
        return False

    def execute_query(self, query: str) -> List[Dict]:
        logger.warning("Stub: cannot execute query")
        return []

    def disconnect(self):
        self._connected = False
