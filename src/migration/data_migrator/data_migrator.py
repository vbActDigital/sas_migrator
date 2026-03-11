from typing import Dict, List

from src.utils.logger import get_logger

logger = get_logger("data_migrator")


class DataMigrator:
    """Databricks data migrator stub."""

    def __init__(self, config: Dict):
        self.config = config

    def generate_migration_plan(self, datasets_metadata: List[Dict]) -> Dict:
        logger.info("Databricks data migration is a stub")
        return {"plan": "Databricks migration not yet implemented", "datasets": []}
