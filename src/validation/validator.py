from typing import Dict, List

from src.utils.logger import get_logger

logger = get_logger("validator")


class MigrationValidator:
    def generate_validation_scripts(self, source_meta: Dict, target_config: Dict) -> Dict:
        ds_name = source_meta.get("dataset_name", "unknown")
        row_count = source_meta.get("row_count", -1)
        columns = source_meta.get("columns", [])
        database = target_config.get("database", "SAS_MIGRATION")
        schema = target_config.get("schema", "RAW")
        target_table = f"{database}.{schema}.{ds_name.upper()}"

        scripts = {
            "dataset": ds_name,
            "target_table": target_table,
            "row_count": (
                f"-- Row count validation\n"
                f"SELECT '{ds_name}' AS dataset,\n"
                f"  {row_count} AS source_count,\n"
                f"  COUNT(*) AS target_count,\n"
                f"  CASE WHEN COUNT(*) = {row_count} THEN 'PASS' ELSE 'FAIL' END AS status\n"
                f"FROM {target_table};"
            ),
            "schema_match": self._schema_match_sql(ds_name, columns, target_table),
            "column_stats": self._column_stats_sql(ds_name, columns, target_table),
            "checksum": (
                f"-- Checksum validation\n"
                f"SELECT HASH_AGG(*) AS checksum FROM {target_table};"
            ),
        }
        return scripts

    def _schema_match_sql(self, ds_name: str, columns: List[Dict], target_table: str) -> str:
        expected = len(columns)
        return (
            f"-- Schema match validation\n"
            f"SELECT '{ds_name}' AS dataset,\n"
            f"  {expected} AS expected_columns,\n"
            f"  COUNT(*) AS actual_columns,\n"
            f"  CASE WHEN COUNT(*) = {expected} THEN 'PASS' ELSE 'FAIL' END AS status\n"
            f"FROM INFORMATION_SCHEMA.COLUMNS\n"
            f"WHERE TABLE_CATALOG || '.' || TABLE_SCHEMA || '.' || TABLE_NAME = '{target_table}';"
        )

    def _column_stats_sql(self, ds_name: str, columns: List[Dict], target_table: str) -> str:
        numeric_cols = [c for c in columns if c.get("type", "").lower() == "num"]
        if not numeric_cols:
            return f"-- No numeric columns to validate for {ds_name}"
        col = numeric_cols[0]["name"]
        return (
            f"-- Column stats validation for {col}\n"
            f"SELECT\n"
            f"  '{ds_name}' AS dataset,\n"
            f"  '{col}' AS column_name,\n"
            f"  MIN({col}) AS min_val,\n"
            f"  MAX({col}) AS max_val,\n"
            f"  AVG({col}) AS avg_val,\n"
            f"  COUNT(*) - COUNT({col}) AS null_count\n"
            f"FROM {target_table};"
        )
