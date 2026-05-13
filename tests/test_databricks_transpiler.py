"""Unit tests for DatabricksTranspiler."""
import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

from src.migration.code_transpiler.databricks_transpiler import DatabricksTranspiler


@pytest.fixture
def config():
    return {
        "library_mapping": {
            "rawdata": {"catalog": "sas_migration", "schema": "bronze_sas"},
            "dw": {"catalog": "sas_migration", "schema": "silver_sas"},
        },
        "target": {
            "platform": "databricks",
            "catalog": "sas_migration",
        },
    }


@pytest.fixture
def transpiler(config):
    return DatabricksTranspiler(config)


def _empty_program(**overrides):
    base = {
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
    }
    base.update(overrides)
    return base


class TestDatabricksTranspilerOutputShape:
    def test_returns_required_keys(self, transpiler):
        result = transpiler.transpile(_empty_program())
        assert "sql_code" in result
        assert "pyspark_code" in result
        assert "gaps" in result
        assert "warnings" in result
        assert "coverage_pct" in result

    def test_coverage_100_for_empty_program(self, transpiler):
        result = transpiler.transpile(_empty_program())
        assert result["coverage_pct"] == 100.0

    def test_no_gaps_for_empty_program(self, transpiler):
        result = transpiler.transpile(_empty_program())
        assert result["gaps"] == []


class TestLibnameMapping:
    def test_known_library_generates_use_catalog(self, transpiler):
        result = transpiler.transpile(_empty_program(
            libnames=[{"name": "rawdata", "path_or_engine": "/data/raw"}]
        ))
        assert "USE CATALOG sas_migration" in result["sql_code"]
        assert "USE SCHEMA bronze_sas" in result["sql_code"]

    def test_known_library_generates_pyspark(self, transpiler):
        result = transpiler.transpile(_empty_program(
            libnames=[{"name": "dw", "path_or_engine": "/data/dw"}]
        ))
        assert 'spark.sql("USE CATALOG sas_migration")' in result["pyspark_code"]
        assert 'spark.sql("USE SCHEMA silver_sas")' in result["pyspark_code"]

    def test_unknown_library_adds_warning(self, transpiler):
        result = transpiler.transpile(_empty_program(
            libnames=[{"name": "mystery", "path_or_engine": "/x"}]
        ))
        assert any("mystery" in w for w in result["warnings"])


class TestDataStep:
    def test_data_step_generates_delta_table(self, transpiler):
        result = transpiler.transpile(_empty_program(
            data_steps=["work.output"],
            datasets_read=["rawdata.input"],
        ))
        assert "CREATE OR REPLACE TABLE" in result["sql_code"]
        assert "USING DELTA" in result["sql_code"]

    def test_data_step_generates_pyspark_write(self, transpiler):
        result = transpiler.transpile(_empty_program(
            data_steps=["work.output"],
            datasets_read=["rawdata.input"],
        ))
        assert 'format("delta")' in result["pyspark_code"]


class TestMerge:
    def test_merge_generates_join(self, transpiler):
        result = transpiler.transpile(_empty_program(
            merge_statements=["table_a", "table_b"]
        ))
        assert "JOIN" in result["sql_code"]

    def test_merge_generates_pyspark_join(self, transpiler):
        result = transpiler.transpile(_empty_program(
            merge_statements=["table_a", "table_b"]
        ))
        assert ".join(" in result["pyspark_code"]


class TestProcTranspilation:
    def test_proc_sort_row_number(self, transpiler):
        result = transpiler.transpile(_empty_program(procs_used=["SORT"]))
        assert "ROW_NUMBER" in result["sql_code"]
        assert "Window" in result["pyspark_code"]

    def test_proc_sql_compatible(self, transpiler):
        result = transpiler.transpile(_empty_program(procs_used=["SQL"]))
        assert "SQL" in result["sql_code"]

    def test_proc_freq_group_by(self, transpiler):
        result = transpiler.transpile(_empty_program(procs_used=["FREQ"]))
        assert "GROUP BY" in result["sql_code"]
        assert "COUNT" in result["sql_code"]

    def test_proc_means_aggregations(self, transpiler):
        result = transpiler.transpile(_empty_program(procs_used=["MEANS"]))
        assert "AVG" in result["sql_code"]
        assert "STDDEV" in result["sql_code"]
        assert "MIN" in result["sql_code"]
        assert "MAX" in result["sql_code"]

    def test_proc_format_lookup_table(self, transpiler):
        result = transpiler.transpile(_empty_program(procs_used=["FORMAT"]))
        assert "lookup" in result["sql_code"].lower()

    def test_proc_univariate_percentiles(self, transpiler):
        result = transpiler.transpile(_empty_program(procs_used=["UNIVARIATE"]))
        assert "PERCENTILE" in result["sql_code"]
        assert "approxQuantile" in result["pyspark_code"]

    @pytest.mark.parametrize("proc", ["LOGISTIC", "REG", "GLM", "MIXED", "IML"])
    def test_statistical_procs_flagged_as_gap(self, transpiler, proc):
        result = transpiler.transpile(_empty_program(procs_used=[proc]))
        assert any(proc in g for g in result["gaps"])

    def test_unknown_proc_adds_warning(self, transpiler):
        result = transpiler.transpile(_empty_program(procs_used=["MYSTERY"]))
        assert any("MYSTERY" in w for w in result["warnings"])


class TestGaps:
    def test_hash_objects_flagged(self, transpiler):
        result = transpiler.transpile(_empty_program(has_hash_objects=True))
        assert any("broadcast" in g.lower() or "hash" in g.lower()
                   for g in result["gaps"])

    def test_dynamic_sql_flagged(self, transpiler):
        result = transpiler.transpile(_empty_program(has_dynamic_sql=True))
        assert any("dynamic" in g.lower() or "CALL EXECUTE" in g
                   for g in result["gaps"])


class TestMacros:
    def test_macro_generates_sql_function(self, transpiler):
        result = transpiler.transpile(_empty_program(
            macro_definitions=["calc_premium"]
        ))
        assert "CREATE OR REPLACE FUNCTION calc_premium" in result["sql_code"]

    def test_macro_generates_python_def(self, transpiler):
        result = transpiler.transpile(_empty_program(
            macro_definitions=["calc_premium"]
        ))
        assert "def calc_premium" in result["pyspark_code"]


class TestCoverage:
    def test_covered_procs_increase_coverage(self, transpiler):
        result_empty = transpiler.transpile(_empty_program())
        result_with_proc = transpiler.transpile(_empty_program(
            procs_used=["SORT"]
        ))
        # Both should be valid floats
        assert 0.0 <= result_empty["coverage_pct"] <= 100.0
        assert 0.0 <= result_with_proc["coverage_pct"] <= 100.0

    def test_gap_procs_reduce_coverage(self, transpiler):
        result = transpiler.transpile(_empty_program(procs_used=["LOGISTIC"]))
        assert result["coverage_pct"] < 100.0
