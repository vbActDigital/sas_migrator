"""Unit tests for Snowflake Transpiler."""
import os
import sys
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

from src.migration.code_transpiler.snowflake_transpiler import SnowflakeTranspiler


@pytest.fixture
def config():
    return {
        "library_mapping": {
            "rawdata": {"database": "SAS_MIGRATION", "schema": "RAW"},
            "dw": {"database": "SAS_MIGRATION", "schema": "REFINED"},
        }
    }


@pytest.fixture
def transpiler(config):
    return SnowflakeTranspiler(config)


class TestSnowflakeTranspiler:
    def test_transpile_returns_dict(self, transpiler):
        result = transpiler.transpile({
            "filename": "test.sas",
            "libnames": [],
            "data_steps": [],
            "procs_used": [],
            "datasets_read": [],
            "datasets_written": [],
            "macro_definitions": [],
            "macro_calls": [],
            "merge_statements": [],
            "includes": [],
            "has_hash_objects": False,
            "has_dynamic_sql": False,
        })
        assert "sql_code" in result
        assert "snowpark_code" in result
        assert "gaps" in result
        assert "coverage_pct" in result

    def test_libname_mapping(self, transpiler):
        result = transpiler.transpile({
            "filename": "test.sas",
            "libnames": [{"name": "rawdata", "path_or_engine": "/data/raw"}],
            "data_steps": [], "procs_used": [], "datasets_read": [],
            "datasets_written": [], "macro_definitions": [], "macro_calls": [],
            "merge_statements": [], "includes": [],
            "has_hash_objects": False, "has_dynamic_sql": False,
        })
        assert "USE DATABASE SAS_MIGRATION" in result["sql_code"]
        assert "USE SCHEMA RAW" in result["sql_code"]

    def test_proc_sort_transpilation(self, transpiler):
        result = transpiler.transpile({
            "filename": "test.sas",
            "libnames": [], "data_steps": [],
            "procs_used": ["SORT"],
            "datasets_read": [], "datasets_written": [],
            "macro_definitions": [], "macro_calls": [],
            "merge_statements": [], "includes": [],
            "has_hash_objects": False, "has_dynamic_sql": False,
        })
        assert "ROW_NUMBER" in result["sql_code"]

    def test_proc_freq_transpilation(self, transpiler):
        result = transpiler.transpile({
            "filename": "test.sas",
            "libnames": [], "data_steps": [],
            "procs_used": ["FREQ"],
            "datasets_read": [], "datasets_written": [],
            "macro_definitions": [], "macro_calls": [],
            "merge_statements": [], "includes": [],
            "has_hash_objects": False, "has_dynamic_sql": False,
        })
        assert "GROUP BY" in result["sql_code"]
        assert "COUNT" in result["sql_code"]

    def test_proc_means_transpilation(self, transpiler):
        result = transpiler.transpile({
            "filename": "test.sas",
            "libnames": [], "data_steps": [],
            "procs_used": ["MEANS"],
            "datasets_read": [], "datasets_written": [],
            "macro_definitions": [], "macro_calls": [],
            "merge_statements": [], "includes": [],
            "has_hash_objects": False, "has_dynamic_sql": False,
        })
        assert "AVG" in result["sql_code"]
        assert "STDDEV" in result["sql_code"]

    def test_hash_objects_gap(self, transpiler):
        result = transpiler.transpile({
            "filename": "test.sas",
            "libnames": [], "data_steps": [], "procs_used": [],
            "datasets_read": [], "datasets_written": [],
            "macro_definitions": [], "macro_calls": [],
            "merge_statements": [], "includes": [],
            "has_hash_objects": True, "has_dynamic_sql": False,
        })
        assert any("Hash" in g for g in result["gaps"])

    def test_dynamic_sql_gap(self, transpiler):
        result = transpiler.transpile({
            "filename": "test.sas",
            "libnames": [], "data_steps": [], "procs_used": [],
            "datasets_read": [], "datasets_written": [],
            "macro_definitions": [], "macro_calls": [],
            "merge_statements": [], "includes": [],
            "has_hash_objects": False, "has_dynamic_sql": True,
        })
        assert any("CALL EXECUTE" in g or "dynamic" in g.lower() for g in result["gaps"])

    def test_statistical_proc_gap(self, transpiler):
        result = transpiler.transpile({
            "filename": "test.sas",
            "libnames": [], "data_steps": [],
            "procs_used": ["LOGISTIC"],
            "datasets_read": [], "datasets_written": [],
            "macro_definitions": [], "macro_calls": [],
            "merge_statements": [], "includes": [],
            "has_hash_objects": False, "has_dynamic_sql": False,
        })
        assert any("LOGISTIC" in g for g in result["gaps"])

    def test_macro_to_stored_proc(self, transpiler):
        result = transpiler.transpile({
            "filename": "test.sas",
            "libnames": [], "data_steps": [], "procs_used": [],
            "datasets_read": [], "datasets_written": [],
            "macro_definitions": ["my_macro"], "macro_calls": [],
            "merge_statements": [], "includes": [],
            "has_hash_objects": False, "has_dynamic_sql": False,
        })
        assert "PROCEDURE" in result["sql_code"]
        assert "my_macro" in result["sql_code"]

    def test_coverage_100_for_empty(self, transpiler):
        result = transpiler.transpile({
            "filename": "test.sas",
            "libnames": [], "data_steps": [], "procs_used": [],
            "datasets_read": [], "datasets_written": [],
            "macro_definitions": [], "macro_calls": [],
            "merge_statements": [], "includes": [],
            "has_hash_objects": False, "has_dynamic_sql": False,
        })
        assert result["coverage_pct"] == 100.0

    def test_data_step_transpilation(self, transpiler):
        result = transpiler.transpile({
            "filename": "test.sas",
            "libnames": [],
            "data_steps": ["work.output"],
            "procs_used": [],
            "datasets_read": ["rawdata.input"],
            "datasets_written": ["work.output"],
            "macro_definitions": [], "macro_calls": [],
            "merge_statements": [], "includes": [],
            "has_hash_objects": False, "has_dynamic_sql": False,
        })
        assert "CREATE OR REPLACE TABLE" in result["sql_code"]

    def test_merge_transpilation(self, transpiler):
        result = transpiler.transpile({
            "filename": "test.sas",
            "libnames": [], "data_steps": [], "procs_used": [],
            "datasets_read": [], "datasets_written": [],
            "macro_definitions": [], "macro_calls": [],
            "merge_statements": ["table_a", "table_b"],
            "includes": [],
            "has_hash_objects": False, "has_dynamic_sql": False,
        })
        assert "JOIN" in result["sql_code"]
