from typing import Dict, List

from src.utils.logger import get_logger

logger = get_logger("databricks_migrator")

TYPE_MAP = {
    "num": "DOUBLE",
    "char": "STRING",
}

DATE_FORMATS = {"date", "ddmmyy", "mmddyy", "yymmdd", "date9", "date7", "dateampm"}
DATETIME_FORMATS = {"datetime", "datetime20", "datetime18", "e8601dt", "dateampm"}


class DatabricksMigrator:
    """Generates Delta Lake DDL and data load scripts for Databricks."""

    def __init__(self, config: Dict):
        self.config = config
        target = config.get("target", {})
        self.catalog = target.get("catalog", "sas_migration")
        self.schema_bronze = target.get("schema_bronze", "bronze_sas")
        self.schema_silver = target.get("schema_silver", "silver_sas")
        self.volume_path = target.get("volume_path", "/Volumes/sas_migration/staging")

    def generate_migration_plan(self, datasets_metadata: List[Dict]) -> Dict:
        plan = {
            "catalog": self.catalog,
            "total_datasets": len(datasets_metadata),
            "datasets": [],
        }
        for ds in datasets_metadata:
            plan["datasets"].append({
                "name": ds.get("dataset_name", ""),
                "row_count": ds.get("row_count", -1),
                "ddl": self.generate_ddl(ds),
                "copy_into": self.generate_copy_into(ds),
            })
        return plan

    def generate_ddl(self, dataset_meta: Dict) -> str:
        ds_name = dataset_meta.get("dataset_name", "unknown")
        columns = dataset_meta.get("columns", [])
        col_defs = []
        for col in columns:
            db_type = self._map_type(col)
            col_name = col.get("name", "col").lower()
            label = col.get("label", "")
            comment = f' COMMENT "{label}"' if label else ""
            col_defs.append(f"  {col_name} {db_type}{comment}")

        cols_str = ",\n".join(col_defs) if col_defs else "  -- No columns defined"
        return (
            f"CREATE OR REPLACE TABLE "
            f"{self.catalog}.{self.schema_bronze}.{ds_name} (\n"
            f"{cols_str}\n"
            f")\n"
            f"USING DELTA\n"
            f"COMMENT 'Migrated from SAS dataset {ds_name}';"
        )

    def generate_copy_into(self, dataset_meta: Dict) -> str:
        ds_name = dataset_meta.get("dataset_name", "unknown")
        target_table = (
            f"{self.catalog}.{self.schema_bronze}.{ds_name}"
        )
        return (
            f"COPY INTO {target_table}\n"
            f"FROM '{self.volume_path}/{ds_name}.csv'\n"
            f"FILEFORMAT = CSV\n"
            f"FORMAT_OPTIONS (\n"
            f"  'header' = 'true',\n"
            f"  'inferSchema' = 'true',\n"
            f"  'delimiter' = ','\n"
            f")\n"
            f"COPY_OPTIONS ('mergeSchema' = 'true');"
        )

    def generate_autoloader(self, dataset_meta: Dict) -> str:
        ds_name = dataset_meta.get("dataset_name", "unknown")
        target_table = (
            f"{self.catalog}.{self.schema_bronze}.{ds_name}"
        )
        return (
            f"# Auto Loader (Structured Streaming) for {ds_name}\n"
            f'df = (spark.readStream\n'
            f'    .format("cloudFiles")\n'
            f'    .option("cloudFiles.format", "csv")\n'
            f'    .option("cloudFiles.schemaLocation", '
            f'"{self.volume_path}/_schema/{ds_name}")\n'
            f'    .load("{self.volume_path}/{ds_name}/"))\n\n'
            f'(df.writeStream\n'
            f'    .format("delta")\n'
            f'    .option("checkpointLocation", '
            f'"{self.volume_path}/_checkpoint/{ds_name}")\n'
            f'    .trigger(availableNow=True)\n'
            f'    .toTable("{target_table}"))'
        )

    def _map_type(self, col: Dict) -> str:
        col_type = col.get("type", "").lower()
        col_format = col.get("format", "").lower()
        col_length = col.get("length", 0)

        if col_type == "num":
            if any(fmt in col_format for fmt in DATE_FORMATS):
                return "DATE"
            if any(fmt in col_format for fmt in DATETIME_FORMATS):
                return "TIMESTAMP"
            if "commax" in col_format or "dollar" in col_format:
                return "DECIMAL(20,2)"
            return "DOUBLE"
        elif col_type == "char":
            return "STRING"
        return "STRING"
