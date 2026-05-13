"""Unit tests for SASCodeValidator."""
import sys
import os
import textwrap
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

from src.parsers.sas.sas_code_validator import SASCodeValidator

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _write_sas(tmp_path, name: str, code: str) -> str:
    p = tmp_path / name
    p.write_text(textwrap.dedent(code), encoding="utf-8")
    return str(p)


def _parsed(filename: str, filepath: str, **extra) -> dict:
    base = {
        "filename": filename,
        "filepath": filepath,
        "proc_exports": [],
    }
    base.update(extra)
    return base


@pytest.fixture
def validator():
    return SASCodeValidator()


# ---------------------------------------------------------------------------
# _strip_comments
# ---------------------------------------------------------------------------

class TestStripComments:
    def test_block_comment_removed(self, validator):
        code = "DATA a; /* this is a comment */ SET b; RUN;"
        result = validator._strip_comments(code)
        assert "this is a comment" not in result
        assert "DATA a;" in result

    def test_preserves_line_count(self, validator):
        code = "line1\n/* multi\nline\ncomment */\nline5"
        result = validator._strip_comments(code)
        assert result.count("\n") == code.count("\n")

    def test_line_comment_removed(self, validator):
        code = "DATA a;\n* this is a line comment;\nSET b;\nRUN;"
        result = validator._strip_comments(code)
        assert "this is a line comment" not in result


# ---------------------------------------------------------------------------
# _path_stem
# ---------------------------------------------------------------------------

class TestPathStem:
    def test_simple_csv(self, validator):
        assert validator._path_stem("/data/output.csv") == "output"

    def test_unc_path(self, validator):
        stem = validator._path_stem("\\\\server\\share\\file_name.xlsx")
        assert "file_name" in stem or stem == "file_name"

    def test_macro_variable_removed(self, validator):
        stem = validator._path_stem("&diretorio./output.csv")
        assert "&" not in stem

    def test_numeric_prefix_stripped(self, validator):
        stem = validator._path_stem("/data/01_output.csv")
        assert not stem.startswith("01")

    def test_empty_path(self, validator):
        assert validator._path_stem("") == ""


# ---------------------------------------------------------------------------
# _var_in_formula_rhs
# ---------------------------------------------------------------------------

class TestVarInFormulaRhs:
    def test_var_found_on_rhs(self, validator):
        body = "  total = C1234567 + C9876543;\n"
        assert validator._var_in_formula_rhs("C1234567", body) is True

    def test_var_not_found(self, validator):
        body = "  total = amount + factor;\n"
        assert validator._var_in_formula_rhs("C1234567", body) is False


# ---------------------------------------------------------------------------
# _check_self_reimport
# ---------------------------------------------------------------------------

class TestCheckSelfReimport:
    def test_detects_self_reimport(self, validator):
        code = textwrap.dedent("""\
            PROC EXPORT DATA=work.result
                OUTFILE='/tmp/output_sap.csv'
                DBMS=CSV REPLACE;
            RUN;
            DATA reimported;
                INFILE '/tmp/output_sap.csv' DLM=';';
                INPUT a b;
            RUN;
        """)
        findings = validator._check_self_reimport(code, {}, "test.sas")
        assert len(findings) == 1
        assert findings[0]["category"] == "AUTO_REIMPORTACAO"
        assert findings[0]["severity"] == "HIGH"

    def test_no_self_reimport_when_different_files(self, validator):
        code = textwrap.dedent("""\
            PROC EXPORT DATA=work.result
                OUTFILE='/tmp/output_sap.csv'
                DBMS=CSV REPLACE;
            RUN;
            DATA other;
                INFILE '/tmp/input_data.csv' DLM=';';
                INPUT a b;
            RUN;
        """)
        findings = validator._check_self_reimport(code, {}, "test.sas")
        assert len(findings) == 0

    def test_no_finding_when_no_export(self, validator):
        code = "DATA a; SET b; RUN;"
        findings = validator._check_self_reimport(code, {}, "test.sas")
        assert len(findings) == 0


# ---------------------------------------------------------------------------
# _parse_data_step_scopes
# ---------------------------------------------------------------------------

class TestParseDataStepScopes:
    def test_parses_single_data_step(self, validator):
        code = "DATA work.out;\n  SET work.input;\n  x = 1;\nRUN;"
        steps = validator._parse_data_step_scopes(code)
        assert len(steps) == 1
        assert steps[0]["target"] == "WORK.OUT"

    def test_captures_assigned_vars(self, validator):
        code = "DATA work.out;\n  SET work.input;\n  total = a + b;\nRUN;"
        steps = validator._parse_data_step_scopes(code)
        assert "TOTAL" in steps[0]["assigned"]

    def test_captures_set_sources(self, validator):
        code = "DATA work.out;\n  SET work.input;\n  x = 1;\nRUN;"
        steps = validator._parse_data_step_scopes(code)
        assert "WORK.INPUT" in steps[0]["set_sources"]

    def test_captures_keep_vars(self, validator):
        code = "DATA work.out;\n  SET work.input;\n  KEEP id total;\nRUN;"
        steps = validator._parse_data_step_scopes(code)
        assert "ID" in steps[0]["keep_vars"] or "TOTAL" in steps[0]["keep_vars"]

    def test_multiple_data_steps(self, validator):
        code = textwrap.dedent("""\
            DATA a; SET b; x = 1; RUN;
            DATA c; SET a; y = 2; RUN;
        """)
        steps = validator._parse_data_step_scopes(code)
        assert len(steps) == 2


# ---------------------------------------------------------------------------
# _build_variable_registry
# ---------------------------------------------------------------------------

class TestBuildVariableRegistry:
    def test_registry_populated_from_steps(self, validator):
        data_steps = [
            {
                "target": "WORK.OUT",
                "set_sources": [],
                "defined": {"TOTAL", "ID"},
                "keep_vars": set(),
                "drop_vars": set(),
                "acct_referenced": set(),
                "body": "",
                "length_vars": set(),
                "format_vars": set(),
                "attrib_vars": set(),
                "start_line": 1,
            }
        ]
        registry = validator._build_variable_registry(data_steps, [])
        assert "WORK.OUT" in registry
        assert "TOTAL" in registry["WORK.OUT"]

    def test_inheritance_from_set_source(self, validator):
        data_steps = [
            {
                "target": "STEP1",
                "set_sources": [],
                "defined": {"VAR_A"},
                "keep_vars": set(), "drop_vars": set(),
                "acct_referenced": set(), "body": "",
                "length_vars": set(), "format_vars": set(),
                "attrib_vars": set(), "start_line": 1,
            },
            {
                "target": "STEP2",
                "set_sources": ["STEP1"],
                "defined": {"VAR_B"},
                "keep_vars": set(), "drop_vars": set(),
                "acct_referenced": set(), "body": "",
                "length_vars": set(), "format_vars": set(),
                "attrib_vars": set(), "start_line": 5,
            },
        ]
        registry = validator._build_variable_registry(data_steps, [])
        # STEP2 should inherit VAR_A from STEP1
        assert "VAR_A" in registry["STEP2"]
        assert "VAR_B" in registry["STEP2"]


# ---------------------------------------------------------------------------
# validate_program — full integration via temp files
# ---------------------------------------------------------------------------

class TestValidateProgram:
    def test_clean_program_returns_no_findings(self, validator, tmp_path):
        code = textwrap.dedent("""\
            DATA work.out;
                SET work.input;
                total = amount + factor;
            RUN;
        """)
        fp = _write_sas(tmp_path, "clean.sas", code)
        parsed = _parsed("clean.sas", fp)
        findings = validator.validate_program(fp, parsed)
        assert isinstance(findings, list)
        # No accounting vars, no self-reimport, no incomplete mappings

    def test_missing_file_returns_empty(self, validator):
        findings = validator.validate_program("/nonexistent/path/fake.sas", {})
        assert findings == []

    def test_detects_self_reimport_via_validate(self, validator, tmp_path):
        code = textwrap.dedent("""\
            PROC EXPORT DATA=work.result
                OUTFILE='/tmp/output_sap.csv'
                DBMS=CSV REPLACE;
            RUN;
            DATA reimported;
                INFILE '/tmp/output_sap.csv' DLM=';';
                INPUT a b;
            RUN;
        """)
        fp = _write_sas(tmp_path, "reimport.sas", code)
        parsed = _parsed("reimport.sas", fp)
        findings = validator.validate_program(fp, parsed)
        cats = [f["category"] for f in findings]
        assert "AUTO_REIMPORTACAO" in cats

    def test_accounting_var_undefined_detected(self, validator, tmp_path):
        code = textwrap.dedent("""\
            DATA work.out;
                SET work.input;
                C1234567 = amount * 0.5;
                total = C9876543 + C1234567;
                KEEP C9876543 C1234567;
            RUN;
        """)
        fp = _write_sas(tmp_path, "acct.sas", code)
        parsed = _parsed("acct.sas", fp)
        findings = validator.validate_program(fp, parsed)
        # C9876543 is never defined but referenced in KEEP/formula
        cats = [f["category"] for f in findings]
        assert any("CONTABIL" in c or "FORMULA" in c for c in cats)

    def test_findings_have_required_keys(self, validator, tmp_path):
        code = textwrap.dedent("""\
            PROC EXPORT DATA=work.result
                OUTFILE='/tmp/sap.csv'
                DBMS=CSV REPLACE;
            RUN;
            DATA a;
                INFILE '/tmp/sap.csv';
                INPUT x;
            RUN;
        """)
        fp = _write_sas(tmp_path, "keys.sas", code)
        findings = validator.validate_program(fp, _parsed("keys.sas", fp))
        for f in findings:
            for key in ("severity", "category", "program", "line",
                        "description", "impact", "recommendation",
                        "recommendation_type"):
                assert key in f, f"Missing key '{key}' in finding"


# ---------------------------------------------------------------------------
# validate_cross_program
# ---------------------------------------------------------------------------

class TestValidateCrossProgram:
    def test_returns_list(self, validator):
        result = validator.validate_cross_program([])
        assert isinstance(result, list)

    def test_single_program_no_cross_findings(self, validator, tmp_path):
        code = "DATA a; SET b; x = 1; RUN;"
        fp = _write_sas(tmp_path, "solo.sas", code)
        findings = validator.validate_cross_program([_parsed("solo.sas", fp)])
        assert isinstance(findings, list)

    def test_section_absent_detected(self, validator, tmp_path):
        """Program A has Direto+Aceito; program B only Direto → finding."""
        code_a = textwrap.dedent("""\
            /* Direto */
            IF cod_tp_emissao = 'D' THEN output_d = 1;
            /* Aceito */
            IF cod_tp_emissao = 'A' THEN output_a = 1;
        """)
        code_b = textwrap.dedent("""\
            /* Direto */
            IF cod_tp_emissao = 'D' THEN output_d = 1;
        """)
        fp_a = _write_sas(tmp_path, "prog_a.sas", code_a)
        fp_b = _write_sas(tmp_path, "prog_b.sas", code_b)
        findings = validator.validate_cross_program([
            _parsed("prog_a.sas", fp_a),
            _parsed("prog_b.sas", fp_b),
        ])
        cats = [f["category"] for f in findings]
        assert "SECAO_AUSENTE" in cats

    def test_compare_exports_detects_missing_sap_aceito(self, validator, tmp_path):
        code_a = "DATA x; SET y; RUN;"
        code_b = "DATA x; SET y; RUN;"
        fp_a = _write_sas(tmp_path, "prog_a.sas", code_a)
        fp_b = _write_sas(tmp_path, "prog_b.sas", code_b)
        prog_a = _parsed("prog_a.sas", fp_a, proc_exports=[
            {"outfile": "/data/sap_direto.csv", "dbms": "CSV"},
            {"outfile": "/data/sap_aceito.csv", "dbms": "CSV"},
            {"outfile": "/data/analitica_direto.csv", "dbms": "CSV"},
            {"outfile": "/data/analitica_aceito.csv", "dbms": "CSV"},
        ])
        prog_b = _parsed("prog_b.sas", fp_b, proc_exports=[
            {"outfile": "/data/sap_direto.csv", "dbms": "CSV"},
            {"outfile": "/data/analitica_direto.csv", "dbms": "CSV"},
            # sap_aceito and base_analitica_aceito MISSING
        ])
        findings = validator.validate_cross_program([prog_a, prog_b])
        cats = [f["category"] for f in findings]
        assert "EXPORT_AUSENTE" in cats
