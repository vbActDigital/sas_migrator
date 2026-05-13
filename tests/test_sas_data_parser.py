"""Unit tests for SASDataParser."""
import sys
import os
import json
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

from src.parsers.sas.sas_data_parser import SASDataParser

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

META_FULL = {
    "dataset_name": "customers",
    "row_count": 150000,
    "column_count": 3,
    "size_bytes": 25_000_000,
    "columns": [
        {"name": "id", "type": "num", "length": 8, "format": "BEST12.", "label": "Customer ID"},
        {"name": "cpf", "type": "char", "length": 11, "format": "$CHAR11.", "label": "CPF"},
        {"name": "age", "type": "num", "length": 8, "format": "BEST3.", "label": "Age"},
    ],
}


@pytest.fixture
def parser():
    return SASDataParser()


def _write_meta(tmp_path, data: dict, basename="customers.sas7bdat") -> tuple:
    """Write a .sas7bdat (dummy) and a .meta.json. Returns (sas_path, meta_path)."""
    sas_path = tmp_path / basename
    sas_path.write_bytes(b"\x00" * 64)  # dummy binary
    meta_path = tmp_path / (basename + ".meta.json")
    meta_path.write_text(json.dumps(data), encoding="utf-8")
    return str(sas_path), str(meta_path)


# ---------------------------------------------------------------------------
# meta.json loading
# ---------------------------------------------------------------------------

class TestParseMetaJson:
    def test_returns_dict(self, parser, tmp_path):
        sas_path, _ = _write_meta(tmp_path, META_FULL)
        result = parser.parse_file(sas_path)
        assert isinstance(result, dict)

    def test_dataset_name_preserved(self, parser, tmp_path):
        sas_path, _ = _write_meta(tmp_path, META_FULL)
        result = parser.parse_file(sas_path)
        assert result.get("dataset_name") == "customers"

    def test_row_count_from_meta(self, parser, tmp_path):
        sas_path, _ = _write_meta(tmp_path, META_FULL)
        result = parser.parse_file(sas_path)
        assert result["row_count"] == 150000

    def test_column_count_from_meta(self, parser, tmp_path):
        sas_path, _ = _write_meta(tmp_path, META_FULL)
        result = parser.parse_file(sas_path)
        assert result["column_count"] == 3

    def test_columns_list_populated(self, parser, tmp_path):
        sas_path, _ = _write_meta(tmp_path, META_FULL)
        result = parser.parse_file(sas_path)
        assert len(result["columns"]) == 3

    def test_filepath_set_to_sas_path(self, parser, tmp_path):
        sas_path, _ = _write_meta(tmp_path, META_FULL)
        result = parser.parse_file(sas_path)
        assert result["filepath"] == sas_path

    def test_filename_is_basename(self, parser, tmp_path):
        sas_path, _ = _write_meta(tmp_path, META_FULL)
        result = parser.parse_file(sas_path)
        assert result["filename"] == "customers.sas7bdat"

    def test_parse_meta_json_directly(self, parser, tmp_path):
        _, meta_path = _write_meta(tmp_path, META_FULL)
        result = parser.parse_file(meta_path)
        assert result["row_count"] == 150000

    def test_columns_have_name_key(self, parser, tmp_path):
        sas_path, _ = _write_meta(tmp_path, META_FULL)
        result = parser.parse_file(sas_path)
        for col in result["columns"]:
            assert "name" in col

    def test_columns_have_type_key(self, parser, tmp_path):
        sas_path, _ = _write_meta(tmp_path, META_FULL)
        result = parser.parse_file(sas_path)
        for col in result["columns"]:
            assert "type" in col

    def test_size_bytes_from_meta(self, parser, tmp_path):
        sas_path, _ = _write_meta(tmp_path, META_FULL)
        result = parser.parse_file(sas_path)
        assert result["size_bytes"] == 25_000_000


# ---------------------------------------------------------------------------
# Filesystem fallback (no meta.json, no pyreadstat)
# ---------------------------------------------------------------------------

class TestFilesystemFallback:
    def test_nonexistent_file_returns_zeroed_dict(self, parser):
        result = parser.parse_file("/nonexistent/path/fake.sas7bdat")
        assert result["row_count"] == -1
        assert result["column_count"] == -1
        assert result["columns"] == []
        assert result["size_bytes"] == 0

    def test_fallback_has_filename(self, parser):
        result = parser.parse_file("/nonexistent/path/fake.sas7bdat")
        assert result["filename"] == "fake.sas7bdat"

    def test_dummy_file_without_meta_gives_fallback(self, parser, tmp_path):
        # A .sas7bdat with no .meta.json and no pyreadstat → filesystem fallback
        sas_path = tmp_path / "bare.sas7bdat"
        sas_path.write_bytes(b"\x00" * 64)
        result = parser.parse_file(str(sas_path))
        assert result["filename"] == "bare.sas7bdat"
        # row_count is -1 (fallback) or >=0 (if pyreadstat happens to be installed)
        assert "row_count" in result
        assert "columns" in result
