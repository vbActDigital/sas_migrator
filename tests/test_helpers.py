"""Unit tests for src/utils/helpers.py."""
import sys
import os
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

from src.utils.helpers import format_bytes, safe_filename, hash_content, normalize_sas_name


class TestFormatBytes:
    def test_zero_bytes(self):
        assert format_bytes(0) == "0 B"

    def test_exact_bytes(self):
        assert format_bytes(512) == "512 B"

    def test_exactly_one_kb(self):
        result = format_bytes(1024)
        assert "1.0 KB" == result

    def test_megabyte(self):
        result = format_bytes(1024 * 1024)
        assert "1.0 MB" == result

    def test_gigabyte(self):
        result = format_bytes(1024 ** 3)
        assert "1.0 GB" == result

    def test_terabyte(self):
        result = format_bytes(1024 ** 4)
        assert "1.0 TB" == result

    def test_partial_kb(self):
        result = format_bytes(1536)  # 1.5 KB
        assert "1.5 KB" == result

    def test_25_mb(self):
        result = format_bytes(25 * 1024 * 1024)
        assert "25.0 MB" == result

    def test_returns_string(self):
        assert isinstance(format_bytes(100), str)


class TestSafeFilename:
    def test_clean_name_unchanged(self):
        assert safe_filename("my_file-v1.sas") == "my_file-v1.sas"

    def test_spaces_replaced(self):
        assert safe_filename("my file") == "my_file"

    def test_special_chars_replaced(self):
        result = safe_filename("file@name#2!")
        assert "@" not in result
        assert "#" not in result
        assert "!" not in result

    def test_leading_underscore_stripped(self):
        result = safe_filename("_leading")
        assert not result.startswith("_")

    def test_trailing_underscore_stripped(self):
        result = safe_filename("trailing_")
        assert not result.endswith("_")

    def test_empty_string(self):
        # Edge case — just ensure no exception
        result = safe_filename("")
        assert isinstance(result, str)

    def test_returns_string(self):
        assert isinstance(safe_filename("test"), str)


class TestHashContent:
    def test_returns_16_char_hex(self):
        result = hash_content("hello")
        assert len(result) == 16
        assert all(c in "0123456789abcdef" for c in result)

    def test_same_input_same_output(self):
        assert hash_content("test") == hash_content("test")

    def test_different_input_different_output(self):
        assert hash_content("foo") != hash_content("bar")

    def test_empty_string(self):
        result = hash_content("")
        assert len(result) == 16

    def test_unicode_content(self):
        result = hash_content("conteúdo com acentos: ção")
        assert len(result) == 16


class TestNormalizeSasName:
    def test_lowercase(self):
        assert normalize_sas_name("RAWDATA") == "rawdata"

    def test_strips_whitespace(self):
        assert normalize_sas_name("  dw  ") == "dw"

    def test_mixed_case_with_spaces(self):
        assert normalize_sas_name("  MyLibrary  ") == "mylibrary"

    def test_already_normalized(self):
        assert normalize_sas_name("work") == "work"

    def test_empty_string(self):
        assert normalize_sas_name("") == ""
