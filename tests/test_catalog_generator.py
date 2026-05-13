"""Unit tests for DataCatalogGenerator."""
import sys
import os
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

from src.catalog.catalog_generator import DataCatalogGenerator

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

CONFIG_DEFAULT = {"catalog": {"detect_pii": True, "infer_domains": True}}
CONFIG_NO_PII = {"catalog": {"detect_pii": False, "infer_domains": True}}
CONFIG_NO_DOMAINS = {"catalog": {"detect_pii": True, "infer_domains": False}}

DS_CUSTOMERS = {
    "dataset_name": "customers",
    "row_count": 150000,
    "column_count": 3,
    "columns": [
        {"name": "cpf", "type": "char"},
        {"name": "email", "type": "char"},
        {"name": "age", "type": "num"},
    ],
}

DS_FATORES = {
    "dataset_name": "fatores_rvr",
    "row_count": 500,
    "column_count": 4,
    "columns": [
        {"name": "risk_score", "type": "num"},
        {"name": "product_code", "type": "char"},
        {"name": "factor", "type": "num"},
        {"name": "reference_date", "type": "num"},
    ],
}

DS_LOOKUP = {
    "dataset_name": "lookup_codes",
    "row_count": 50,
    "column_count": 2,
    "columns": [
        {"name": "code", "type": "char"},
        {"name": "description", "type": "char"},
    ],
}

DS_SINISTRO = {
    "dataset_name": "sinistro_analitico",
    "row_count": 80000,
    "column_count": 3,
    "columns": [
        {"name": "claim_id", "type": "num"},
        {"name": "sinistro_status", "type": "char"},
        {"name": "dt_ocorrencia", "type": "num"},
    ],
}


@pytest.fixture
def gen():
    return DataCatalogGenerator(CONFIG_DEFAULT)


# ---------------------------------------------------------------------------
# Catalog structure
# ---------------------------------------------------------------------------

class TestCatalogStructure:
    def test_returns_dict(self, gen):
        result = gen.generate_catalog([DS_CUSTOMERS])
        assert isinstance(result, dict)

    def test_top_level_keys(self, gen):
        result = gen.generate_catalog([DS_CUSTOMERS])
        assert "metadata" in result
        assert "summary" in result
        assert "datasets" in result
        assert "columns" in result

    def test_metadata_has_timestamp(self, gen):
        result = gen.generate_catalog([DS_CUSTOMERS])
        assert "timestamp" in result["metadata"]

    def test_total_datasets_count(self, gen):
        result = gen.generate_catalog([DS_CUSTOMERS, DS_FATORES])
        assert result["summary"]["total_datasets"] == 2

    def test_total_columns_sum(self, gen):
        result = gen.generate_catalog([DS_CUSTOMERS, DS_FATORES])
        # customers=3, fatores=4
        assert result["summary"]["total_columns"] == 7

    def test_columns_list_length(self, gen):
        result = gen.generate_catalog([DS_CUSTOMERS])
        assert len(result["columns"]) == 3

    def test_empty_datasets(self, gen):
        result = gen.generate_catalog([])
        assert result["summary"]["total_datasets"] == 0
        assert len(result["datasets"]) == 0


# ---------------------------------------------------------------------------
# PII detection
# ---------------------------------------------------------------------------

class TestPIIDetection:
    def test_detects_cpf_as_pii(self, gen):
        result = gen.generate_catalog([DS_CUSTOMERS])
        ds = result["datasets"][0]
        assert "cpf" in ds["pii_columns"]

    def test_detects_email_as_pii(self, gen):
        result = gen.generate_catalog([DS_CUSTOMERS])
        ds = result["datasets"][0]
        assert "email" in ds["pii_columns"]

    def test_non_pii_column_not_flagged(self, gen):
        result = gen.generate_catalog([DS_CUSTOMERS])
        ds = result["datasets"][0]
        assert "age" not in ds["pii_columns"]

    def test_dataset_with_pii_is_restricted(self, gen):
        result = gen.generate_catalog([DS_CUSTOMERS])
        ds = result["datasets"][0]
        assert ds["sensitivity"] == "Restricted"

    def test_pii_disabled_returns_empty_pii_list(self):
        gen = DataCatalogGenerator(CONFIG_NO_PII)
        result = gen.generate_catalog([DS_CUSTOMERS])
        ds = result["datasets"][0]
        assert ds["pii_columns"] == []

    def test_pii_disabled_dataset_not_restricted(self):
        gen = DataCatalogGenerator(CONFIG_NO_PII)
        result = gen.generate_catalog([DS_CUSTOMERS])
        ds = result["datasets"][0]
        assert ds["sensitivity"] != "Restricted"

    def test_datasets_with_pii_count(self, gen):
        result = gen.generate_catalog([DS_CUSTOMERS, DS_FATORES])
        # only customers has PII
        assert result["summary"]["datasets_with_pii"] == 1

    def test_partial_keyword_match(self, gen):
        # "telefone_principal" contains "telefone"
        ds = {
            "dataset_name": "contacts",
            "row_count": 100,
            "column_count": 1,
            "columns": [{"name": "telefone_principal", "type": "char"}],
        }
        result = gen.generate_catalog([ds])
        ds_out = result["datasets"][0]
        assert "telefone_principal" in ds_out["pii_columns"]


# ---------------------------------------------------------------------------
# Sensitivity classification
# ---------------------------------------------------------------------------

class TestSensitivity:
    def test_reference_table_is_public(self, gen):
        result = gen.generate_catalog([DS_LOOKUP])
        ds = result["datasets"][0]
        assert ds["sensitivity"] == "Public"

    def test_large_non_pii_is_internal(self, gen):
        result = gen.generate_catalog([DS_FATORES])
        ds = result["datasets"][0]
        assert ds["sensitivity"] == "Internal"

    def test_sensitivity_distribution_keys(self, gen):
        result = gen.generate_catalog([DS_CUSTOMERS, DS_FATORES, DS_LOOKUP])
        dist = result["summary"]["sensitivity_distribution"]
        assert "Restricted" in dist
        assert "Internal" in dist
        assert "Public" in dist


# ---------------------------------------------------------------------------
# Domain inference
# ---------------------------------------------------------------------------

class TestDomainInference:
    def test_customers_domain(self, gen):
        result = gen.generate_catalog([DS_CUSTOMERS])
        ds = result["datasets"][0]
        assert ds["domain"] == "Customer"

    def test_risk_domain_from_columns(self, gen):
        result = gen.generate_catalog([DS_FATORES])
        ds = result["datasets"][0]
        assert ds["domain"] in ("Risk", "Product", "General")

    def test_sinistro_is_claims(self, gen):
        result = gen.generate_catalog([DS_SINISTRO])
        ds = result["datasets"][0]
        # "sinistro" maps to Claims, "claim_id" also
        assert ds["domain"] == "Claims"

    def test_domain_disabled_returns_unknown(self):
        gen = DataCatalogGenerator(CONFIG_NO_DOMAINS)
        result = gen.generate_catalog([DS_CUSTOMERS])
        ds = result["datasets"][0]
        assert ds["domain"] == "Unknown"

    def test_unknown_domain_fallback(self, gen):
        ds = {
            "dataset_name": "xyz_data",
            "row_count": 1000,
            "column_count": 2,
            "columns": [{"name": "id", "type": "num"}, {"name": "value", "type": "num"}],
        }
        result = gen.generate_catalog([ds])
        assert result["datasets"][0]["domain"] == "General"

    def test_domains_summary_populated(self, gen):
        result = gen.generate_catalog([DS_CUSTOMERS, DS_SINISTRO])
        domains = result["summary"]["domains"]
        assert len(domains) > 0


# ---------------------------------------------------------------------------
# Reference table detection
# ---------------------------------------------------------------------------

class TestReferenceTableDetection:
    def test_small_row_count_is_reference(self, gen):
        ds = {
            "dataset_name": "grupo_rvr",
            "row_count": 30,
            "column_count": 2,
            "columns": [],
        }
        result = gen.generate_catalog([ds])
        assert result["datasets"][0]["sensitivity"] == "Public"

    def test_name_with_lookup_is_reference(self, gen):
        ds = {
            "dataset_name": "lookup_products",
            "row_count": 5000,
            "column_count": 3,
            "columns": [],
        }
        result = gen.generate_catalog([ds])
        assert result["datasets"][0]["sensitivity"] == "Public"

    def test_row_count_101_is_not_reference(self, gen):
        ds = {
            "dataset_name": "borderline_table",
            "row_count": 101,
            "column_count": 2,
            "columns": [],
        }
        result = gen.generate_catalog([ds])
        # >100 rows and no lookup keyword → Internal (no PII either)
        assert result["datasets"][0]["sensitivity"] == "Internal"


# ---------------------------------------------------------------------------
# LLM enrichment (stub/skip)
# ---------------------------------------------------------------------------

class TestLLMEnrichment:
    def test_no_llm_advisor_skips_enrichment(self, gen):
        result = gen.generate_catalog([DS_CUSTOMERS], enrich_with_llm=True)
        ds = result["datasets"][0]
        assert "llm_description" not in ds

    def test_llm_exception_does_not_crash(self):
        class BadLLM:
            def enrich_catalog_entry(self, *a, **kw):
                raise RuntimeError("LLM unavailable")

        gen = DataCatalogGenerator(CONFIG_DEFAULT, llm_advisor=BadLLM())
        # Should not raise
        result = gen.generate_catalog([DS_CUSTOMERS], enrich_with_llm=True)
        assert len(result["datasets"]) == 1
