from typing import Dict, List, Optional

from src.utils.logger import get_logger

logger = get_logger("databricks_connector")

try:
    import databricks.sql as dbsql
    _SDK_AVAILABLE = True
except ImportError:
    dbsql = None
    _SDK_AVAILABLE = False


class DatabricksConnector:
    """Connects to Databricks via databricks-sql-connector.

    Requires: pip install databricks-sql-connector
    Falls back to stub mode if SDK is not installed.
    """

    def __init__(self, config: Dict):
        target = config.get("target", {})
        self.workspace_url = target.get("workspace_url", "")
        self.token = target.get("token", "")
        self.catalog = target.get("catalog", "")
        self.schema_bronze = target.get("schema_bronze", "bronze_sas")
        self.http_path = target.get("http_path", "")
        self._connection = None
        self._cursor = None

    # ------------------------------------------------------------------
    # Connection
    # ------------------------------------------------------------------

    def connect(self) -> bool:
        if not _SDK_AVAILABLE:
            logger.warning(
                "databricks-sql-connector not installed. "
                "Run: pip install databricks-sql-connector"
            )
            return False

        if not self.workspace_url or not self.token:
            logger.error(
                "DATABRICKS_HOST or DATABRICKS_TOKEN not configured."
            )
            return False

        if not self.http_path:
            logger.error(
                "target.http_path not configured "
                "(e.g. /sql/1.0/warehouses/<id>)."
            )
            return False

        try:
            host = self.workspace_url.replace("https://", "").rstrip("/")
            self._connection = dbsql.connect(
                server_hostname=host,
                http_path=self.http_path,
                access_token=self.token,
            )
            self._cursor = self._connection.cursor()
            logger.info("Connected to Databricks workspace: %s", host)
            return True
        except Exception as exc:
            logger.error("Databricks connection failed: %s", exc)
            return False

    def disconnect(self):
        try:
            if self._cursor:
                self._cursor.close()
            if self._connection:
                self._connection.close()
        except Exception as exc:
            logger.warning("Error during disconnect: %s", exc)
        finally:
            self._cursor = None
            self._connection = None

    @property
    def is_connected(self) -> bool:
        return self._connection is not None and self._cursor is not None

    # ------------------------------------------------------------------
    # Query execution
    # ------------------------------------------------------------------

    def execute(self, sql: str) -> List[Dict]:
        """Execute a SQL statement and return rows as list of dicts."""
        if not self.is_connected:
            logger.warning("Not connected — call connect() first.")
            return []

        try:
            self._cursor.execute(sql)
            if self._cursor.description is None:
                return []
            columns = [col[0] for col in self._cursor.description]
            return [
                dict(zip(columns, row))
                for row in self._cursor.fetchall()
            ]
        except Exception as exc:
            logger.error("execute() failed: %s\nSQL: %.200s", exc, sql)
            return []

    def execute_ddl(self, sql: str) -> bool:
        """Execute a DDL statement (CREATE TABLE, COPY INTO, etc.)."""
        if not self.is_connected:
            logger.warning("Not connected — call connect() first.")
            return False

        try:
            self._cursor.execute(sql)
            logger.info("DDL executed successfully: %.80s...", sql)
            return True
        except Exception as exc:
            logger.error("execute_ddl() failed: %s\nSQL: %.200s", exc, sql)
            return False

    def set_catalog_schema(self, catalog: Optional[str] = None,
                           schema: Optional[str] = None) -> bool:
        """Switch active catalog and/or schema."""
        catalog = catalog or self.catalog
        schema = schema or self.schema_bronze
        return (
            self.execute_ddl(f"USE CATALOG {catalog}")
            and self.execute_ddl(f"USE SCHEMA {schema}")
        )

    # ------------------------------------------------------------------
    # Context manager support
    # ------------------------------------------------------------------

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, *_):
        self.disconnect()
