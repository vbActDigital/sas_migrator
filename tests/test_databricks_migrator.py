"""Unit tests for DatabricksMigrator and DatabricksConnector."""
import os
import sys
from unittest.mock import MagicMock, patch

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

from src.migration.data_migrator.databricks_migrator import DatabricksMigrator
from src.connectors.databricks.databricks_connector import DatabricksConnector


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def config():
    return {
        "target": {
            "platform": "databricks",
            "workspace_url": "https://adb-123.azuredatabricks.net",
            "token": "dapi_test_token",
            "http_path": "/sql/1.0/warehouses/abc123",
            "catalog": "sas_migration",
            "schema_bronze": "bronze_sas",
            "schema_silver": "silver_sas",
            "volume_path": "/Volumes/sas_migration/staging",
        }
    }


@pytest.fixture
def migrator(config):
    return DatabricksMigrator(config)


@pytest.fixture
def ds_numeric():
    return {
        "dataset_name": "pendente_analitico",
        "row_count": 150000,
        "column_count": 5,
        "columns": [
            {"name": "APOLICE", "type": "num", "format": "", "label": "Apolice ID"},
            {"name": "VALOR", "type": "num", "format": "commax20.2", "label": "Valor"},
            {"name": "DATA_REF", "type": "num", "format": "date9.", "label": "Data Ref"},
            {"name": "TIMESTAMP_IN", "type": "num", "format": "datetime20.", "label": ""},
            {"name": "NOME", "type": "char", "format": "$30.", "label": "Nome"},
        ],
    }


@pytest.fixture
def ds_minimal():
    return {
        "dataset_name": "fatores_rvr",
        "row_count": 500,
        "column_count": 2,
        "columns": [
            {"name": "CODIGO", "type": "char", "format": "", "label": ""},
            {"name": "FATOR", "type": "num", "format": "", "label": "Fator RVR"},
        ],
    }


# ---------------------------------------------------------------------------
# DatabricksMigrator — DDL
# ---------------------------------------------------------------------------

class TestGenerateDDL:
    def test_ddl_creates_delta_table(self, migrator, ds_numeric):
        ddl = migrator.generate_ddl(ds_numeric)
        assert "CREATE OR REPLACE TABLE" in ddl
        assert "USING DELTA" in ddl

    def test_ddl_uses_catalog_and_schema(self, migrator, ds_numeric):
        ddl = migrator.generate_ddl(ds_numeric)
        assert "sas_migration.bronze_sas.pendente_analitico" in ddl

    def test_ddl_column_names_lowercase(self, migrator, ds_numeric):
        ddl = migrator.generate_ddl(ds_numeric)
        assert "apolice" in ddl
        assert "valor" in ddl
        assert "APOLICE" not in ddl

    def test_ddl_includes_comment_for_label(self, migrator, ds_numeric):
        ddl = migrator.generate_ddl(ds_numeric)
        assert 'COMMENT "Apolice ID"' in ddl
        assert 'COMMENT "Valor"' in ddl

    def test_ddl_no_comment_for_empty_label(self, migrator, ds_numeric):
        ddl = migrator.generate_ddl(ds_numeric)
        # TIMESTAMP_IN has no label — should not have a COMMENT clause for it
        lines = [l for l in ddl.splitlines() if "timestamp_in" in l]
        assert lines
        assert "COMMENT" not in lines[0]

    def test_ddl_empty_columns_fallback(self, migrator):
        ddl = migrator.generate_ddl({"dataset_name": "empty_ds", "columns": []})
        assert "-- No columns defined" in ddl


# ---------------------------------------------------------------------------
# DatabricksMigrator — Type mapping
# ---------------------------------------------------------------------------

class TestTypeMapping:
    @pytest.mark.parametrize("fmt,expected", [
        ("date9.", "DATE"),
        ("ddmmyy10.", "DATE"),
        ("yymmdd.", "DATE"),
        ("datetime20.", "TIMESTAMP"),
        ("e8601dt.", "TIMESTAMP"),
        ("commax20.2", "DECIMAL(20,2)"),
        ("dollar12.2", "DECIMAL(20,2)"),
        ("", "DOUBLE"),
        ("best12.", "DOUBLE"),
    ])
    def test_num_type_mapping(self, migrator, fmt, expected):
        col = {"type": "num", "format": fmt, "length": 8}
        assert migrator._map_type(col) == expected

    def test_char_maps_to_string(self, migrator):
        assert migrator._map_type({"type": "char", "format": "", "length": 30}) == "STRING"

    def test_unknown_type_defaults_to_string(self, migrator):
        assert migrator._map_type({"type": "unknown", "format": ""}) == "STRING"


# ---------------------------------------------------------------------------
# DatabricksMigrator — COPY INTO
# ---------------------------------------------------------------------------

class TestGenerateCopyInto:
    def test_copy_into_references_table(self, migrator, ds_minimal):
        copy_sql = migrator.generate_copy_into(ds_minimal)
        assert "COPY INTO sas_migration.bronze_sas.fatores_rvr" in copy_sql

    def test_copy_into_references_volume(self, migrator, ds_minimal):
        copy_sql = migrator.generate_copy_into(ds_minimal)
        assert "/Volumes/sas_migration/staging/fatores_rvr.csv" in copy_sql

    def test_copy_into_fileformat_csv(self, migrator, ds_minimal):
        copy_sql = migrator.generate_copy_into(ds_minimal)
        assert "FILEFORMAT = CSV" in copy_sql

    def test_copy_into_merge_schema(self, migrator, ds_minimal):
        copy_sql = migrator.generate_copy_into(ds_minimal)
        assert "mergeSchema" in copy_sql


# ---------------------------------------------------------------------------
# DatabricksMigrator — Auto Loader
# ---------------------------------------------------------------------------

class TestGenerateAutoLoader:
    def test_autoloader_uses_cloudfiles(self, migrator, ds_minimal):
        al = migrator.generate_autoloader(ds_minimal)
        assert '"cloudFiles"' in al

    def test_autoloader_writes_to_delta(self, migrator, ds_minimal):
        al = migrator.generate_autoloader(ds_minimal)
        assert '.format("delta")' in al

    def test_autoloader_has_checkpoint(self, migrator, ds_minimal):
        al = migrator.generate_autoloader(ds_minimal)
        assert "checkpointLocation" in al

    def test_autoloader_trigger_available_now(self, migrator, ds_minimal):
        al = migrator.generate_autoloader(ds_minimal)
        assert "availableNow=True" in al

    def test_autoloader_target_table(self, migrator, ds_minimal):
        al = migrator.generate_autoloader(ds_minimal)
        assert "sas_migration.bronze_sas.fatores_rvr" in al


# ---------------------------------------------------------------------------
# DatabricksMigrator — Migration Plan
# ---------------------------------------------------------------------------

class TestGenerateMigrationPlan:
    def test_plan_contains_all_datasets(self, migrator, ds_numeric, ds_minimal):
        plan = migrator.generate_migration_plan([ds_numeric, ds_minimal])
        assert plan["total_datasets"] == 2
        assert len(plan["datasets"]) == 2

    def test_plan_dataset_has_ddl_and_copy(self, migrator, ds_minimal):
        plan = migrator.generate_migration_plan([ds_minimal])
        ds = plan["datasets"][0]
        assert "ddl" in ds
        assert "copy_into" in ds
        assert "USING DELTA" in ds["ddl"]
        assert "COPY INTO" in ds["copy_into"]

    def test_plan_catalog_field(self, migrator, ds_minimal):
        plan = migrator.generate_migration_plan([ds_minimal])
        assert plan["catalog"] == "sas_migration"

    def test_plan_row_count_preserved(self, migrator, ds_minimal):
        plan = migrator.generate_migration_plan([ds_minimal])
        assert plan["datasets"][0]["row_count"] == 500


# ---------------------------------------------------------------------------
# DatabricksConnector — unit tests (no real network)
# ---------------------------------------------------------------------------

class TestDatabricksConnectorStubMode:
    def test_connect_returns_false_without_sdk(self, config):
        """When databricks-sql-connector is not installed, connect() is False."""
        conn = DatabricksConnector(config)
        with patch(
            "src.connectors.databricks.databricks_connector._SDK_AVAILABLE",
            False,
        ):
            assert conn.connect() is False

    def test_connect_returns_false_without_token(self):
        conn = DatabricksConnector({"target": {}})
        with patch(
            "src.connectors.databricks.databricks_connector._SDK_AVAILABLE",
            False,
        ):
            assert conn.connect() is False

    def test_execute_without_connect_returns_empty(self, config):
        conn = DatabricksConnector(config)
        # Not connected — should return []
        result = conn.execute("SELECT 1")
        assert result == []

    def test_execute_ddl_without_connect_returns_false(self, config):
        conn = DatabricksConnector(config)
        assert conn.execute_ddl("CREATE TABLE test (id INT)") is False

    def test_is_connected_false_by_default(self, config):
        conn = DatabricksConnector(config)
        assert conn.is_connected is False


class TestDatabricksConnectorWithMockSDK:
    def _make_connected_connector(self, config):
        """Return a connector wired to a mock databricks.sql connection."""
        mock_cursor = MagicMock()
        mock_cursor.description = [("id",), ("name",)]
        mock_cursor.fetchall.return_value = [(1, "Alice"), (2, "Bob")]

        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        conn = DatabricksConnector(config)
        conn._connection = mock_conn
        conn._cursor = mock_cursor
        return conn, mock_cursor

    def test_execute_returns_list_of_dicts(self, config):
        conn, _ = self._make_connected_connector(config)
        rows = conn.execute("SELECT id, name FROM users")
        assert rows == [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]

    def test_execute_handles_no_description(self, config):
        conn, mock_cursor = self._make_connected_connector(config)
        mock_cursor.description = None
        rows = conn.execute("INSERT INTO t VALUES (1)")
        assert rows == []

    def test_execute_ddl_returns_true_on_success(self, config):
        conn, _ = self._make_connected_connector(config)
        assert conn.execute_ddl("CREATE TABLE t (id INT) USING DELTA") is True

    def test_execute_ddl_returns_false_on_exception(self, config):
        conn, mock_cursor = self._make_connected_connector(config)
        mock_cursor.execute.side_effect = Exception("SQL error")
        assert conn.execute_ddl("BAD SQL") is False

    def test_execute_returns_empty_on_exception(self, config):
        conn, mock_cursor = self._make_connected_connector(config)
        mock_cursor.execute.side_effect = Exception("timeout")
        rows = conn.execute("SELECT 1")
        assert rows == []

    def test_disconnect_clears_connection(self, config):
        conn, _ = self._make_connected_connector(config)
        assert conn.is_connected is True
        conn.disconnect()
        assert conn.is_connected is False

    def test_context_manager_disconnects(self, config):
        with patch(
            "src.connectors.databricks.databricks_connector._SDK_AVAILABLE",
            False,
        ):
            conn = DatabricksConnector(config)
            with conn:
                pass  # connect() returns False but no exception
            assert conn.is_connected is False
