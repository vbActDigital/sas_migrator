from typing import Dict, List

from src.utils.logger import get_logger

logger = get_logger("snowflake_migrator")

TYPE_MAP = {
    "num": "NUMBER(38,10)",
    "char": "VARCHAR",
}

DATE_FORMATS = {"date", "ddmmyy", "mmddyy", "yymmdd", "date9", "date7", "dateampm"}
DATETIME_FORMATS = {"datetime", "datetime20", "datetime18", "e8601dt", "dateampm"}


class SnowflakeMigrator:
    def __init__(self, config: Dict):
        self.config = config
        target = config.get("target", {})
        self.database = target.get("database", "SAS_MIGRATION")
        self.warehouse = target.get("warehouse", "MIGRATION_WH")
        aws = target.get("aws", {})
        self.s3_bucket = aws.get("s3_bucket", "")
        self.s3_prefix = aws.get("s3_prefix", "")

    def generate_migration_plan(self, datasets_metadata: List[Dict]) -> Dict:
        plan = {
            "database": self.database,
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
            sf_type = self._map_type(col)
            col_name = col.get("name", "col").upper()
            col_defs.append(f"  {col_name} {sf_type}")

        cols_str = ",\n".join(col_defs) if col_defs else "  -- No columns defined"
        return (
            f"CREATE OR REPLACE TABLE {self.database}.RAW.{ds_name.upper()} (\n"
            f"{cols_str}\n"
            f");"
        )

    def generate_copy_into(self, dataset_meta: Dict) -> str:
        ds_name = dataset_meta.get("dataset_name", "unknown")
        return (
            f"COPY INTO {self.database}.RAW.{ds_name.upper()}\n"
            f"FROM @sas_migration_stage/{ds_name}.csv\n"
            f"FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '\"' SKIP_HEADER = 1)\n"
            f"ON_ERROR = 'CONTINUE';"
        )

    def generate_snowpipe(self, dataset_meta: Dict) -> str:
        ds_name = dataset_meta.get("dataset_name", "unknown")
        return (
            f"CREATE OR REPLACE PIPE {self.database}.RAW.pipe_{ds_name.upper()}\n"
            f"AUTO_INGEST = TRUE\n"
            f"AS\n"
            f"COPY INTO {self.database}.RAW.{ds_name.upper()}\n"
            f"FROM @sas_migration_stage/{ds_name}/\n"
            f"FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '\"' SKIP_HEADER = 1);"
        )

    def _map_type(self, col: Dict) -> str:
        col_type = col.get("type", "").lower()
        col_format = col.get("format", "").lower()
        col_length = col.get("length", 0)

        if col_type == "num":
            if any(fmt in col_format for fmt in DATE_FORMATS):
                return "DATE"
            if any(fmt in col_format for fmt in DATETIME_FORMATS):
                return "TIMESTAMP_NTZ"
            return "NUMBER(38,10)"
        elif col_type == "char":
            length = col_length if isinstance(col_length, int) and col_length > 0 else 256
            return f"VARCHAR({length})"
        return "VARCHAR(256)"
