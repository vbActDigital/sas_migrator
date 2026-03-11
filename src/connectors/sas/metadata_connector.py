from typing import Dict, List, Optional

from src.utils.logger import get_logger

logger = get_logger("metadata_connector")


class SASMetadataConnector:
    """Stub for SAS Metadata Server connection. Future implementation."""

    def __init__(self, config: Dict):
        self.host = config.get("metadata_server", {}).get("host", "")
        self.port = config.get("metadata_server", {}).get("port", 8561)
        self._connected = False

    def connect(self) -> bool:
        logger.warning("SAS Metadata Server connector is a stub - not connecting")
        return False

    def get_libraries(self) -> List[Dict]:
        logger.warning("Stub: returning empty library list")
        return []

    def get_datasets(self, library: str) -> List[Dict]:
        logger.warning("Stub: returning empty dataset list for library %s", library)
        return []

    def disconnect(self):
        self._connected = False
