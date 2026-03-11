"""Unit tests for SAS Code Parser."""
import os
import sys
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

from src.parsers.sas.sas_code_parser import SASCodeParser

FIXTURES_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "fixtures", "sample_programs")


@pytest.fixture
def parser():
    return SASCodeParser()


@pytest.fixture
def etl_customers_path():
    return os.path.join(FIXTURES_DIR, "etl_load_customers.sas")


@pytest.fixture
def risk_model_path():
    return os.path.join(FIXTURES_DIR, "risk_model_scoring.sas")


class TestSASCodeParser:
    def test_parse_returns_dict(self, parser, etl_customers_path):
        if not os.path.exists(etl_customers_path):
            pytest.skip("Fixture not found - run create_mock_environment.py first")
        result = parser.parse_file(etl_customers_path)
        assert isinstance(result, dict)
        assert "filename" in result
        assert "complexity_score" in result

    def test_extracts_libnames(self, parser, etl_customers_path):
        if not os.path.exists(etl_customers_path):
            pytest.skip("Fixture not found")
        result = parser.parse_file(etl_customers_path)
        lib_names = [lib["name"] for lib in result["libnames"]]
        assert "RAWDATA" in lib_names
        assert "DW" in lib_names

    def test_extracts_procs(self, parser, etl_customers_path):
        if not os.path.exists(etl_customers_path):
            pytest.skip("Fixture not found")
        result = parser.parse_file(etl_customers_path)
        procs = [p.upper() for p in result["procs_used"]]
        assert "SORT" in procs
        assert "SQL" in procs
        assert "FREQ" in procs
        assert "MEANS" in procs

    def test_extracts_macro_definitions(self, parser, etl_customers_path):
        if not os.path.exists(etl_customers_path):
            pytest.skip("Fixture not found")
        result = parser.parse_file(etl_customers_path)
        assert "log_step" in result["macro_definitions"]

    def test_extracts_macro_calls(self, parser, etl_customers_path):
        if not os.path.exists(etl_customers_path):
            pytest.skip("Fixture not found")
        result = parser.parse_file(etl_customers_path)
        assert "log_step" in result["macro_calls"]

    def test_extracts_datasets_read(self, parser, etl_customers_path):
        if not os.path.exists(etl_customers_path):
            pytest.skip("Fixture not found")
        result = parser.parse_file(etl_customers_path)
        reads = [ds.lower() for ds in result["datasets_read"]]
        assert any("customers" in r for r in reads)

    def test_extracts_datasets_written(self, parser, etl_customers_path):
        if not os.path.exists(etl_customers_path):
            pytest.skip("Fixture not found")
        result = parser.parse_file(etl_customers_path)
        writes = [ds.lower() for ds in result["datasets_written"]]
        assert any("dim_customer" in w for w in writes) or any("customer" in w for w in writes)

    def test_detects_merge(self, parser, etl_customers_path):
        if not os.path.exists(etl_customers_path):
            pytest.skip("Fixture not found")
        result = parser.parse_file(etl_customers_path)
        assert len(result["merge_statements"]) > 0

    def test_complexity_score_positive(self, parser, etl_customers_path):
        if not os.path.exists(etl_customers_path):
            pytest.skip("Fixture not found")
        result = parser.parse_file(etl_customers_path)
        assert result["complexity_score"] > 0

    def test_complexity_level_valid(self, parser, etl_customers_path):
        if not os.path.exists(etl_customers_path):
            pytest.skip("Fixture not found")
        result = parser.parse_file(etl_customers_path)
        assert result["complexity_level"] in ("LOW", "MEDIUM", "HIGH", "VERY_HIGH")

    def test_risk_model_has_statistical_procs(self, parser, risk_model_path):
        if not os.path.exists(risk_model_path):
            pytest.skip("Fixture not found")
        result = parser.parse_file(risk_model_path)
        procs = [p.upper() for p in result["procs_used"]]
        assert "LOGISTIC" in procs
        assert "UNIVARIATE" in procs

    def test_risk_model_has_macro_definitions(self, parser, risk_model_path):
        if not os.path.exists(risk_model_path):
            pytest.skip("Fixture not found")
        result = parser.parse_file(risk_model_path)
        assert "score_segment" in result["macro_definitions"]

    def test_risk_model_high_complexity(self, parser, risk_model_path):
        if not os.path.exists(risk_model_path):
            pytest.skip("Fixture not found")
        result = parser.parse_file(risk_model_path)
        assert result["complexity_level"] in ("MEDIUM", "HIGH", "VERY_HIGH")

    def test_parse_inline_code(self, parser, tmp_path):
        code = """\
LIBNAME mylib '/data/test';
DATA work.output;
  SET mylib.input;
RUN;
PROC SORT DATA=work.output; BY id; RUN;
"""
        fpath = tmp_path / "test.sas"
        fpath.write_text(code)
        result = parser.parse_file(str(fpath))
        assert len(result["libnames"]) == 1
        assert result["libnames"][0]["name"] == "MYLIB"
        assert "SORT" in [p.upper() for p in result["procs_used"]]

    def test_hash_detection(self, parser, tmp_path):
        code = """\
DATA output;
  IF _N_ = 1 THEN DO;
    DECLARE HASH h(dataset: 'lookup');
    h.defineKey('id');
    h.defineDone();
  END;
  SET input;
RUN;
"""
        fpath = tmp_path / "hash_test.sas"
        fpath.write_text(code)
        result = parser.parse_file(str(fpath))
        assert result["has_hash_objects"] is True

    def test_dynamic_sql_detection(self, parser, tmp_path):
        code = """\
DATA _null_;
  CALL EXECUTE('DATA out; SET in; RUN;');
RUN;
"""
        fpath = tmp_path / "dynamic.sas"
        fpath.write_text(code)
        result = parser.parse_file(str(fpath))
        assert result["has_dynamic_sql"] is True

    def test_include_detection(self, parser, tmp_path):
        code = """%INCLUDE '/macros/utils.sas';\nDATA a; SET b; RUN;\n"""
        fpath = tmp_path / "inc.sas"
        fpath.write_text(code)
        result = parser.parse_file(str(fpath))
        assert len(result["includes"]) == 1

    def test_empty_file(self, parser, tmp_path):
        fpath = tmp_path / "empty.sas"
        fpath.write_text("/* empty file */\n")
        result = parser.parse_file(str(fpath))
        assert result["complexity_score"] == 0
        assert result["complexity_level"] == "LOW"
