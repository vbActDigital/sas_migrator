"""Unit tests for ReportGenerator."""
import os
import sys
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

from src.reporting.report_generator import ReportGenerator

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

CONFIG = {
    "project": {"name": "SAS Migration Test", "client": "MAPFRE"},
}

PROGRAMS_SIMPLE = [
    {
        "filename": "etl_customers.sas",
        "line_count": 300,
        "complexity_score": 8,
        "complexity_level": "MEDIUM",
        "complexity_dimensions": {
            "PLP": 3, "PLP_nivel": "Media",
            "PDI": 2, "PDI_nivel": "Baixa",
            "PVD": 1, "PVD_nivel": "Baixa",
            "PRS": 2, "PRS_nivel": "Baixa",
            "esforco_hh": "16h",
        },
        "procs_used": ["SORT", "SQL", "FREQ"],
        "datasets_read": ["rawdata.customers"],
        "datasets_written": ["dw.dim_customer"],
        "macro_definitions": ["log_step"],
        "macro_calls": ["log_step"],
        "merge_statements": ["merge_1"],
        "join_count": 2,
        "if_then_chains": 5,
        "has_hash_objects": False,
        "has_dynamic_sql": False,
        "includes": [],
    },
    {
        "filename": "risk_model.sas",
        "line_count": 900,
        "complexity_score": 22,
        "complexity_level": "HIGH",
        "complexity_dimensions": {
            "PLP": 8, "PLP_nivel": "Alta",
            "PDI": 4, "PDI_nivel": "Media",
            "PVD": 5, "PVD_nivel": "Media",
            "PRS": 5, "PRS_nivel": "Media",
            "esforco_hh": "80h",
        },
        "procs_used": ["LOGISTIC", "UNIVARIATE", "SQL"],
        "datasets_read": ["rawdata.claims", "rawdata.policies"],
        "datasets_written": ["dw.risk_scores"],
        "macro_definitions": ["score_segment", "validate_input"],
        "macro_calls": ["score_segment"],
        "merge_statements": ["merge_1", "merge_2"],
        "join_count": 5,
        "if_then_chains": 20,
        "has_hash_objects": True,
        "has_dynamic_sql": False,
        "includes": ["/macros/utils.sas"],
    },
]

DATASETS_SIMPLE = [
    {
        "dataset_name": "customers",
        "row_count": 150000,
        "column_count": 45,
        "size_bytes": 25_000_000,
    },
    {
        "dataset_name": "fatores_rvr",
        "row_count": 500,
        "column_count": 12,
        "size_bytes": 120_000,
    },
]

LINEAGE_SIMPLE = {
    "nodes": [
        {"id": "etl_customers.sas", "type": "program"},
        {"id": "rawdata.customers", "type": "dataset"},
        {"id": "dw.dim_customer", "type": "dataset"},
    ],
    "edges": [
        {"source": "rawdata.customers", "target": "etl_customers.sas"},
        {"source": "etl_customers.sas", "target": "dw.dim_customer"},
    ],
}

VALIDATION_FINDINGS = [
    {
        "program": "calculo_rvr_msg.sas",
        "category": "UNC_PATH",
        "severity": "CRITICAL",
        "description": "Caminho UNC nao acessivel em ambiente Snowflake.",
        "impact": "Arquivo nao sera carregado sem intervencao manual.",
        "recommendation": "Substituir caminho UNC por S3 bucket path.",
        "recommendation_type": "EXTRACAO_MANUAL",
        "line": 42,
    },
    {
        "program": "base_financeiro.sas",
        "category": "PROC_IMPORT_XLSX",
        "severity": "HIGH",
        "description": "PROC IMPORT de Excel nao suportado diretamente.",
        "impact": "Dados de Excel precisam de conversao previa.",
        "recommendation": "Converter para CSV e usar COPY INTO no Snowflake.",
        "recommendation_type": "CONFIGURACAO_EXPORT",
        "line": 15,
    },
    {
        "program": "etl_customers.sas",
        "category": "MACRO_UNDEFINED",
        "severity": "MEDIUM",
        "description": "Macro %validate_date chamada mas nao definida no arquivo.",
        "impact": "Possivel erro de execucao em runtime.",
        "recommendation": "Verificar se macro esta em arquivo de include.",
        "recommendation_type": "REVISAO_MANUAL",
        "line": 0,
    },
    {
        "program": "risk_model.sas",
        "category": "HASH_OBJECT",
        "severity": "LOW",
        "description": "Hash object detectado — padrao avancado SAS.",
        "impact": "Requer traducao manual para Snowflake.",
        "recommendation": "Avaliar uso de JOIN ou temp table equivalente.",
        "recommendation_type": "CORRECAO_CODIGO",
        "line": 200,
    },
]


@pytest.fixture
def generator():
    return ReportGenerator(CONFIG)


@pytest.fixture
def programs():
    return PROGRAMS_SIMPLE


@pytest.fixture
def datasets():
    return DATASETS_SIMPLE


@pytest.fixture
def lineage():
    return LINEAGE_SIMPLE


# ---------------------------------------------------------------------------
# generate_discovery_report
# ---------------------------------------------------------------------------

class TestGenerateDiscoveryReport:
    def test_creates_file(self, generator, programs, datasets, lineage, tmp_path):
        path = generator.generate_discovery_report(
            programs, datasets, lineage, output_dir=str(tmp_path)
        )
        assert os.path.exists(path)

    def test_returns_correct_path(self, generator, programs, datasets, lineage, tmp_path):
        path = generator.generate_discovery_report(
            programs, datasets, lineage, output_dir=str(tmp_path)
        )
        assert path.endswith("discovery_report.md")

    def test_file_not_empty(self, generator, programs, datasets, lineage, tmp_path):
        path = generator.generate_discovery_report(
            programs, datasets, lineage, output_dir=str(tmp_path)
        )
        assert os.path.getsize(path) > 0

    def test_creates_output_dir_if_missing(self, generator, programs, datasets, lineage, tmp_path):
        new_dir = os.path.join(str(tmp_path), "novo_subdir")
        path = generator.generate_discovery_report(
            programs, datasets, lineage, output_dir=new_dir
        )
        assert os.path.exists(new_dir)
        assert os.path.exists(path)

    def test_report_contains_all_section_headers(self, generator, programs, datasets, lineage, tmp_path):
        path = generator.generate_discovery_report(
            programs, datasets, lineage, output_dir=str(tmp_path)
        )
        content = open(path, encoding="utf-8").read()
        for header in [
            "## 1. Executive Summary",
            "## 2. Program Inventory",
            "## 3. Dataset Inventory",
            "## 4. Dependencies and Lineage",
            "## 5. Complexity Analysis",
            "## 6. Analise de Integridade",
            "## 7. Limitations",
            "## 8. Next Steps",
        ]:
            assert header in content, f"Header ausente: {header}"

    def test_report_header_contains_project_and_client(self, generator, programs, datasets, lineage, tmp_path):
        path = generator.generate_discovery_report(
            programs, datasets, lineage, output_dir=str(tmp_path)
        )
        content = open(path, encoding="utf-8").read()
        assert "SAS Migration Test" in content
        assert "MAPFRE" in content

    def test_report_with_validation_findings(self, generator, programs, datasets, lineage, tmp_path):
        path = generator.generate_discovery_report(
            programs, datasets, lineage,
            validation_findings=VALIDATION_FINDINGS,
            output_dir=str(tmp_path),
        )
        content = open(path, encoding="utf-8").read()
        assert "UNC_PATH" in content
        assert "CRITICAL" in content


# ---------------------------------------------------------------------------
# Section 1: Executive Summary
# ---------------------------------------------------------------------------

class TestSectionExecutiveSummary:
    def _get_content(self, generator, programs, datasets, lineage, findings=None):
        return generator._section_executive_summary(programs, datasets, lineage, findings)

    def test_contains_total_programs(self, generator, programs, datasets, lineage):
        content = self._get_content(generator, programs, datasets, lineage)
        assert "Total Programs" in content
        assert "2" in content

    def test_contains_total_datasets(self, generator, programs, datasets, lineage):
        content = self._get_content(generator, programs, datasets, lineage)
        assert "Total Datasets" in content
        assert str(len(datasets)) in content

    def test_contains_recommended_strategy(self, generator, programs, datasets, lineage):
        content = self._get_content(generator, programs, datasets, lineage)
        assert "Recommended Strategy" in content

    def test_strategy_phased_when_high_complexity(self, generator, programs, datasets, lineage):
        # risk_model.sas has HIGH complexity
        content = self._get_content(generator, programs, datasets, lineage)
        assert "Phased migration" in content

    def test_strategy_straightforward_when_all_low(self, generator, datasets, lineage):
        low_programs = [
            {**PROGRAMS_SIMPLE[0], "complexity_level": "LOW"},
            {**PROGRAMS_SIMPLE[1], "complexity_level": "LOW"},
        ]
        content = generator._section_executive_summary(low_programs, datasets, lineage)
        assert "Straightforward" in content

    def test_validation_summary_shown_when_findings_present(self, generator, programs, datasets, lineage):
        content = self._get_content(generator, programs, datasets, lineage, VALIDATION_FINDINGS)
        assert "Achados de Validacao" in content

    def test_validation_summary_absent_when_no_findings(self, generator, programs, datasets, lineage):
        content = self._get_content(generator, programs, datasets, lineage)
        assert "Achados de Validacao" not in content

    def test_lineage_node_count(self, generator, programs, datasets, lineage):
        content = self._get_content(generator, programs, datasets, lineage)
        assert "Lineage Nodes" in content
        assert "3" in content

    def test_lineage_edge_count(self, generator, programs, datasets, lineage):
        content = self._get_content(generator, programs, datasets, lineage)
        assert "Lineage Edges" in content
        assert "2" in content


# ---------------------------------------------------------------------------
# Section 2: Program Inventory
# ---------------------------------------------------------------------------

class TestSectionProgramInventory:
    def test_contains_all_programs(self, generator, programs):
        content = generator._section_program_inventory(programs)
        for p in programs:
            assert p["filename"] in content

    def test_sorted_by_complexity_descending(self, generator, programs):
        content = generator._section_program_inventory(programs)
        idx_high = content.index("risk_model.sas")
        idx_medium = content.index("etl_customers.sas")
        assert idx_high < idx_medium

    def test_header_row_present(self, generator, programs):
        content = generator._section_program_inventory(programs)
        assert "Complexity" in content
        assert "Lines" in content

    def test_shows_proc_names(self, generator, programs):
        content = generator._section_program_inventory(programs)
        assert "SORT" in content
        assert "LOGISTIC" in content

    def test_shows_section_number(self, generator, programs):
        content = generator._section_program_inventory(programs)
        assert "## 2. Program Inventory" in content


# ---------------------------------------------------------------------------
# Section 3: Dataset Inventory
# ---------------------------------------------------------------------------

class TestSectionDatasetInventory:
    def test_contains_all_datasets(self, generator, datasets):
        content = generator._section_dataset_inventory(datasets)
        assert "customers" in content
        assert "fatores_rvr" in content

    def test_sorted_by_row_count_descending(self, generator, datasets):
        content = generator._section_dataset_inventory(datasets)
        idx_large = content.index("customers")
        idx_small = content.index("fatores_rvr")
        assert idx_large < idx_small

    def test_shows_row_counts(self, generator, datasets):
        content = generator._section_dataset_inventory(datasets)
        assert "150000" in content or "150,000" in content

    def test_shows_section_number(self, generator, datasets):
        content = generator._section_dataset_inventory(datasets)
        assert "## 3. Dataset Inventory" in content


# ---------------------------------------------------------------------------
# Section 4: Lineage
# ---------------------------------------------------------------------------

class TestSectionLineage:
    def test_total_nodes(self, generator, lineage):
        content = generator._section_lineage(lineage)
        assert "3" in content

    def test_total_edges(self, generator, lineage):
        content = generator._section_lineage(lineage)
        assert "2" in content

    def test_root_and_leaf_detected(self, generator, lineage):
        content = generator._section_lineage(lineage)
        assert "Root Nodes" in content
        assert "Leaf Nodes" in content

    def test_empty_lineage(self, generator):
        content = generator._section_lineage({"nodes": [], "edges": []})
        assert "0" in content

    def test_section_header(self, generator, lineage):
        content = generator._section_lineage(lineage)
        assert "## 4. Dependencies and Lineage" in content


# ---------------------------------------------------------------------------
# Section 5: Complexity Analysis
# ---------------------------------------------------------------------------

class TestSectionComplexity:
    def test_contains_dimension_matrix_header(self, generator, programs):
        content = generator._section_complexity(programs)
        assert "Matriz de Complexidade" in content

    def test_all_four_dimensions_listed(self, generator, programs):
        content = generator._section_complexity(programs)
        for dim in ["PLP", "PDI", "PVD", "PRS"]:
            assert dim in content

    def test_shows_program_names(self, generator, programs):
        content = generator._section_complexity(programs)
        assert "risk_model.sas" in content
        assert "etl_customers.sas" in content

    def test_shows_effort_estimate(self, generator, programs):
        content = generator._section_complexity(programs)
        assert "80h" in content or "16h" in content

    def test_shows_detailed_factors_table(self, generator, programs):
        content = generator._section_complexity(programs)
        assert "Fatores Detalhados" in content

    def test_hash_object_shown_as_sim(self, generator, programs):
        content = generator._section_complexity(programs)
        assert "Sim" in content  # risk_model.sas tem has_hash_objects=True

    def test_section_header(self, generator, programs):
        content = generator._section_complexity(programs)
        assert "## 5. Complexity Analysis" in content


# ---------------------------------------------------------------------------
# Section 6: Validation Findings
# ---------------------------------------------------------------------------

class TestSectionValidationFindings:
    def test_no_findings_message(self, generator):
        content = generator._section_validation_findings([])
        assert "Nenhum problema" in content

    def test_shows_finding_count_in_summary(self, generator):
        content = generator._section_validation_findings(VALIDATION_FINDINGS)
        assert "Resumo dos Achados" in content
        assert "CRITICAL" in content
        assert "HIGH" in content

    def test_shows_finding_programs(self, generator):
        content = generator._section_validation_findings(VALIDATION_FINDINGS)
        assert "calculo_rvr_msg.sas" in content
        assert "base_financeiro.sas" in content

    def test_finding_numbered_sequentially(self, generator):
        content = generator._section_validation_findings(VALIDATION_FINDINGS)
        assert "Achado #1" in content
        assert "Achado #2" in content
        assert "Achado #3" in content
        assert "Achado #4" in content

    def test_shows_impact_and_recommendation(self, generator):
        content = generator._section_validation_findings(VALIDATION_FINDINGS)
        assert "Impacto" in content
        assert "Recomendacao" in content

    def test_severity_order_critical_first(self, generator):
        content = generator._section_validation_findings(VALIDATION_FINDINGS)
        idx_critical = content.index("CRITICO")
        idx_low = content.index("BAIXO")
        assert idx_critical < idx_low

    def test_line_info_shown_when_nonzero(self, generator):
        content = generator._section_validation_findings(VALIDATION_FINDINGS)
        assert "linha ~42" in content

    def test_line_info_absent_when_zero(self, generator):
        content = generator._section_validation_findings(VALIDATION_FINDINGS)
        # MACRO_UNDEFINED has line=0, so "linha ~0" must NOT appear
        assert "linha ~0" not in content

    def test_recommendation_type_label_shown(self, generator):
        content = generator._section_validation_findings(VALIDATION_FINDINGS)
        assert "Extracao Manual" in content
        assert "Configuracao de Export" in content

    def test_section_header(self, generator):
        content = generator._section_validation_findings(VALIDATION_FINDINGS)
        assert "## 6. Analise de Integridade" in content


# ---------------------------------------------------------------------------
# Section 7: Limitations
# ---------------------------------------------------------------------------

class TestSectionLimitations:
    def test_section_header(self, generator):
        content = generator._section_limitations()
        assert "## 7. Limitations" in content

    def test_mentions_regex_parser(self, generator):
        content = generator._section_limitations()
        assert "regex" in content.lower()

    def test_mentions_hash_objects(self, generator):
        content = generator._section_limitations()
        assert "Hash" in content


# ---------------------------------------------------------------------------
# Section 8: Next Steps
# ---------------------------------------------------------------------------

class TestSectionNextSteps:
    def test_section_header(self, generator):
        content = generator._section_next_steps()
        assert "## 8. Next Steps" in content

    def test_contains_urgente(self, generator):
        content = generator._section_next_steps()
        assert "URGENTE" in content

    def test_mentions_mvp2(self, generator):
        content = generator._section_next_steps()
        assert "MVP2" in content
