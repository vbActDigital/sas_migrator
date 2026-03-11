#!/usr/bin/env python3
"""
Gera PDF profissional com resultados do MVP1 Discovery.
"""
import os
import sys
import json
from datetime import datetime
from collections import Counter

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

from fpdf import FPDF

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.join(BASE_DIR, "..")
MOCK_DIR = os.path.join(BASE_DIR, "mock_sas_environment")
CONFIG_PATH = os.path.join(BASE_DIR, "mock_config.yaml")
OUTPUT_PDF = os.path.join(PROJECT_ROOT, "report_output", "MVP1_Discovery_Report_MAPFRE.pdf")


# ============================================================
# COLETA DE DADOS
# ============================================================
def collect_data():
    import yaml
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)

    from src.connectors.sas.filesystem_scanner import SASFilesystemScanner
    from src.parsers.sas.sas_code_parser import SASCodeParser
    from src.parsers.sas.lineage_builder import LineageBuilder
    from src.catalog.catalog_generator import DataCatalogGenerator

    # Scan
    scanner = SASFilesystemScanner(config)
    programs = scanner.scan_programs()
    datasets = scanner.scan_datasets()

    # Parse
    parser = SASCodeParser()
    parsed = [parser.parse_file(p["absolute_path"]) for p in programs]

    # Metadata
    data_dir = config["sas_environment"]["data_paths"][0]
    ds_meta = []
    for fname in sorted(os.listdir(data_dir)):
        if fname.endswith(".meta.json"):
            with open(os.path.join(data_dir, fname), "r", encoding="utf-8") as f:
                ds_meta.append(json.load(f))

    # Lineage
    builder = LineageBuilder()
    lineage = builder.build_from_parsed_programs(parsed)

    # Catalog
    cat_gen = DataCatalogGenerator(config=config, llm_advisor=None)
    catalog = cat_gen.generate_catalog(ds_meta, parsed, lineage, enrich_with_llm=False)

    return config, programs, datasets, parsed, ds_meta, lineage, builder, catalog


# ============================================================
# PDF
# ============================================================
class MigrationPDF(FPDF):
    BG_DARK = (23, 37, 63)
    BG_ACCENT = (41, 98, 255)
    BG_LIGHT = (240, 243, 249)
    BG_WHITE = (255, 255, 255)
    TXT_DARK = (30, 30, 30)
    TXT_WHITE = (255, 255, 255)
    TXT_ACCENT = (41, 98, 255)
    GREEN = (34, 139, 34)
    RED = (200, 50, 50)
    ORANGE = (220, 140, 20)
    YELLOW = (180, 160, 30)

    def __init__(self):
        super().__init__(orientation="P", unit="mm", format="A4")
        self.set_auto_page_break(auto=True, margin=20)

    def header(self):
        if self.page_no() == 1:
            return
        self.set_fill_color(*self.BG_DARK)
        self.rect(0, 0, 210, 12, "F")
        self.set_font("Helvetica", "B", 7)
        self.set_text_color(*self.TXT_WHITE)
        self.set_xy(8, 3)
        self.cell(0, 5, "SAS-to-Snowflake Migration Toolkit  |  MVP1 Discovery Report  |  MAPFRE", align="L")
        self.set_xy(0, 3)
        self.cell(202, 5, f"Pag. {self.page_no()}", align="R")
        self.set_y(16)

    def footer(self):
        self.set_y(-12)
        self.set_font("Helvetica", "I", 7)
        self.set_text_color(130, 130, 130)
        self.cell(0, 5, f"Gerado em {datetime.now().strftime('%d/%m/%Y %H:%M')}  |  Confidencial  |  act digital", align="C")

    def cover_page(self, config):
        self.add_page()
        # Background
        self.set_fill_color(*self.BG_DARK)
        self.rect(0, 0, 210, 297, "F")

        # Accent bar
        self.set_fill_color(*self.BG_ACCENT)
        self.rect(0, 100, 210, 4, "F")

        # Title
        self.set_font("Helvetica", "B", 32)
        self.set_text_color(*self.TXT_WHITE)
        self.set_xy(20, 50)
        self.cell(0, 14, "MVP1 Discovery Report")

        self.set_font("Helvetica", "", 18)
        self.set_xy(20, 68)
        self.cell(0, 10, "SAS-to-Snowflake Migration Toolkit")

        # Project info
        self.set_font("Helvetica", "", 13)
        self.set_text_color(180, 195, 220)
        self.set_xy(20, 115)
        self.cell(0, 8, f"Cliente: {config['project']['client']}")
        self.set_xy(20, 126)
        self.cell(0, 8, f"Projeto: {config['project']['name']}")
        self.set_xy(20, 137)
        self.cell(0, 8, f"Data: {datetime.now().strftime('%d de %B de %Y')}")
        self.set_xy(20, 148)
        self.cell(0, 8, "Plataforma alvo: Snowflake + AWS")

        # Bottom
        self.set_fill_color(*self.BG_ACCENT)
        self.rect(0, 270, 210, 27, "F")
        self.set_font("Helvetica", "B", 10)
        self.set_text_color(*self.TXT_WHITE)
        self.set_xy(20, 276)
        self.cell(0, 6, "CONFIDENCIAL  |  act digital  |  SAS Migration Practice")

    def section_title(self, number, title):
        self.ln(4)
        self.set_fill_color(*self.BG_ACCENT)
        self.rect(10, self.get_y(), 190, 9, "F")
        self.set_font("Helvetica", "B", 11)
        self.set_text_color(*self.TXT_WHITE)
        self.set_x(14)
        self.cell(0, 9, f"{number}. {title}")
        self.ln(12)
        self.set_text_color(*self.TXT_DARK)

    def sub_title(self, text):
        self.set_font("Helvetica", "B", 10)
        self.set_text_color(*self.TXT_ACCENT)
        self.set_x(12)
        self.cell(0, 7, text)
        self.ln(7)
        self.set_text_color(*self.TXT_DARK)

    def body_text(self, text):
        self.set_font("Helvetica", "", 9)
        self.set_text_color(*self.TXT_DARK)
        self.set_x(12)
        self.multi_cell(186, 5, text)
        self.ln(1)

    def kv_line(self, key, value, bold_value=False):
        self.set_font("Helvetica", "", 9)
        self.set_x(16)
        self.cell(60, 5, key, new_x="END")
        self.set_font("Helvetica", "B" if bold_value else "", 9)
        self.cell(0, 5, str(value))
        self.ln(5)

    def add_table(self, headers, rows, col_widths=None, zebra=True):
        if col_widths is None:
            col_widths = [186 / len(headers)] * len(headers)

        # Header
        self.set_fill_color(*self.BG_DARK)
        self.set_text_color(*self.TXT_WHITE)
        self.set_font("Helvetica", "B", 8)
        x_start = 12
        self.set_x(x_start)
        for i, h in enumerate(headers):
            self.cell(col_widths[i], 7, h, border=0, fill=True, align="C")
        self.ln(7)

        # Rows
        self.set_text_color(*self.TXT_DARK)
        self.set_font("Helvetica", "", 8)
        for row_idx, row in enumerate(rows):
            if self.get_y() > 265:
                self.add_page()
                # Re-draw header
                self.set_fill_color(*self.BG_DARK)
                self.set_text_color(*self.TXT_WHITE)
                self.set_font("Helvetica", "B", 8)
                self.set_x(x_start)
                for i, h in enumerate(headers):
                    self.cell(col_widths[i], 7, h, border=0, fill=True, align="C")
                self.ln(7)
                self.set_text_color(*self.TXT_DARK)
                self.set_font("Helvetica", "", 8)

            if zebra and row_idx % 2 == 0:
                self.set_fill_color(*self.BG_LIGHT)
                fill = True
            else:
                self.set_fill_color(*self.BG_WHITE)
                fill = True
            self.set_x(x_start)
            for i, val in enumerate(row):
                align = "L" if i == 0 else "C"
                self.cell(col_widths[i], 6, str(val), border=0, fill=fill, align=align)
            self.ln(6)
        self.ln(2)

    def metric_box(self, label, value, x, y, w=40, h=22, color=None):
        if color is None:
            color = self.BG_ACCENT
        self.set_fill_color(*color)
        self.rect(x, y, w, h, "F")
        # Value
        self.set_font("Helvetica", "B", 16)
        self.set_text_color(*self.TXT_WHITE)
        self.set_xy(x, y + 2)
        self.cell(w, 10, str(value), align="C")
        # Label
        self.set_font("Helvetica", "", 7)
        self.set_xy(x, y + 12)
        self.cell(w, 6, label, align="C")

    def badge(self, text, color):
        self.set_fill_color(*color)
        self.set_text_color(*self.TXT_WHITE)
        self.set_font("Helvetica", "B", 7)
        tw = self.get_string_width(text) + 6
        x = self.get_x()
        y = self.get_y()
        self.rect(x, y, tw, 5, "F")
        self.set_xy(x + 1, y)
        self.cell(tw - 2, 5, text)
        self.set_text_color(*self.TXT_DARK)
        return tw

    def horizontal_bar(self, label, value, max_val, width=120, color=None):
        if color is None:
            color = self.BG_ACCENT
        bar_w = (value / max_val * width) if max_val > 0 else 0
        self.set_x(16)
        self.set_font("Helvetica", "", 8)
        self.cell(40, 6, label)
        y = self.get_y() + 1
        x = self.get_x()
        # Background
        self.set_fill_color(220, 220, 220)
        self.rect(x, y, width, 4, "F")
        # Bar
        self.set_fill_color(*color)
        self.rect(x, y, bar_w, 4, "F")
        # Value
        self.set_font("Helvetica", "B", 8)
        self.set_xy(x + width + 2, self.get_y())
        self.cell(20, 6, str(value))
        self.ln(7)


def generate_pdf():
    print("Coletando dados do MVP1...")
    config, programs, datasets, parsed, ds_meta, lineage, builder, catalog = collect_data()

    pdf = MigrationPDF()

    # ==========================================
    # CAPA
    # ==========================================
    pdf.cover_page(config)

    # ==========================================
    # PAGINA 2: SUMARIO EXECUTIVO
    # ==========================================
    pdf.add_page()
    pdf.section_title("1", "SUMARIO EXECUTIVO")

    pdf.body_text(
        "Este relatorio apresenta os resultados da fase de Discovery (MVP1) do projeto de "
        "migracao do ecossistema SAS para Snowflake + AWS da MAPFRE. A analise abrange o "
        "inventario completo de programas, datasets, dependencias, complexidade e "
        "classificacao de dados do ambiente SAS existente."
    )
    pdf.ln(2)

    # KPI boxes
    y = pdf.get_y()
    pdf.metric_box("Programas SAS", len(parsed), 12, y, 35, 22, pdf.BG_ACCENT)
    pdf.metric_box("Datasets", len(ds_meta), 52, y, 35, 22, pdf.BG_ACCENT)
    total_rows = sum(d["row_count"] for d in ds_meta)
    pdf.metric_box("Linhas", f"{total_rows:,}", 92, y, 35, 22, pdf.BG_ACCENT)
    pdf.metric_box("Nos Lineage", len(lineage["nodes"]), 132, y, 35, 22, pdf.BG_DARK)
    pdf.metric_box("Arestas", len(lineage["edges"]), 172, y, 28, 22, pdf.BG_DARK)
    pdf.set_y(y + 28)

    pdf.sub_title("Distribuicao de Complexidade")
    dist = Counter(p["complexity_level"] for p in parsed)
    max_c = max(dist.values()) if dist else 1
    colors = {"LOW": pdf.GREEN, "MEDIUM": pdf.YELLOW, "HIGH": pdf.ORANGE, "VERY_HIGH": pdf.RED}
    for level in ["LOW", "MEDIUM", "HIGH", "VERY_HIGH"]:
        count = dist.get(level, 0)
        pdf.horizontal_bar(level, count, max_c, 100, colors.get(level, pdf.BG_ACCENT))

    pdf.ln(2)
    pdf.sub_title("Recomendacao Estrategica")
    has_complex = any(p["complexity_level"] in ("HIGH", "VERY_HIGH") for p in parsed)
    strategy = (
        "Migracao faseada recomendada. O ambiente possui programas de alta complexidade "
        "(hash objects, PROC LOGISTIC, SQL dinamico) que requerem revisao manual. "
        "Sugerimos 3 ondas: Quick Wins (LOW), Core ETL (MEDIUM/HIGH) e Modelos Avancados (VERY_HIGH)."
    ) if has_complex else "Migracao direta viavel para todos os programas."
    pdf.body_text(strategy)

    # ==========================================
    # PAGINA 3: INVENTARIO DE PROGRAMAS
    # ==========================================
    pdf.add_page()
    pdf.section_title("2", "INVENTARIO DE PROGRAMAS SAS")

    pdf.body_text(
        "Foram escaneados os diretorios configurados do ambiente SAS, excluindo pastas de "
        "backup conforme os exclude_patterns. Cada programa foi analisado via parser regex "
        "para extracao de LIBNAMEs, PROCs, DATA steps, macros e features especiais."
    )
    pdf.ln(1)

    pdf.sub_title("2.1  Programas Encontrados")

    headers = ["Programa", "Linhas", "Score", "Nivel", "PROCs", "Datasets R/W"]
    rows = []
    for p in sorted(parsed, key=lambda x: x["complexity_score"], reverse=True):
        procs = ", ".join(p["procs_used"][:4])
        rw = f"{len(p['datasets_read'])}/{len(p['datasets_written'])}"
        rows.append([p["filename"], str(p["line_count"]), str(p["complexity_score"]),
                      p["complexity_level"], procs, rw])
    pdf.add_table(headers, rows, [52, 14, 14, 20, 60, 26])

    pdf.sub_title("2.2  Features Especiais Detectadas")
    features = [
        ("Hash Objects", [p["filename"] for p in parsed if p["has_hash_objects"]]),
        ("SQL Dinamico / CALL EXECUTE", [p["filename"] for p in parsed if p["has_dynamic_sql"]]),
        ("MERGE Statements", [p["filename"] for p in parsed if p["merge_statements"]]),
    ]
    for feat_name, progs in features:
        pdf.set_font("Helvetica", "B", 9)
        pdf.set_x(16)
        pdf.cell(60, 5, feat_name + ":")
        pdf.set_font("Helvetica", "", 9)
        pdf.cell(0, 5, ", ".join(progs) if progs else "Nenhum")
        pdf.ln(6)

    pdf.ln(2)
    pdf.sub_title("2.3  Macros Customizadas")
    macro_data = [(p["filename"], ", ".join(p["macro_definitions"])) for p in parsed if p["macro_definitions"]]
    if macro_data:
        pdf.add_table(["Programa", "Macros Definidas"], macro_data, [60, 126])
    else:
        pdf.body_text("Nenhuma macro customizada encontrada.")

    pdf.sub_title("2.4  PROCs Utilizados no Ambiente")
    all_procs = Counter()
    for p in parsed:
        all_procs.update(p["procs_used"])
    gap_procs = {"LOGISTIC", "REG", "GLM", "MIXED", "IML", "REPORT", "TABULATE"}
    proc_rows = []
    for proc, count in all_procs.most_common():
        traduzivel = "Sim" if proc.upper() not in gap_procs else "GAP"
        proc_rows.append([f"PROC {proc}", str(count), traduzivel])
    pdf.add_table(["PROC", "Ocorrencias", "Traduzivel"], proc_rows, [60, 40, 86])

    # ==========================================
    # PAGINA: INVENTARIO DE DATASETS
    # ==========================================
    pdf.add_page()
    pdf.section_title("3", "INVENTARIO DE DATASETS")

    pdf.body_text(
        "Metadados extraidos de cada dataset SAS (.sas7bdat) via fallback chain: "
        "pyreadstat -> sas7bdat lib -> .meta.json. Inclui contagem de linhas, colunas, "
        "tipos de dados e formatos SAS."
    )
    pdf.ln(1)

    pdf.sub_title("3.1  Visao Geral")
    ds_headers = ["Dataset", "Linhas", "Colunas", "Tamanho", "Tipo Dominio"]
    ds_rows = []
    for ds in sorted(ds_meta, key=lambda x: x["row_count"], reverse=True):
        sz = ds.get("size_bytes", 0)
        size_str = f"{sz/1024:.1f} KB" if sz >= 1024 else f"{sz} B"
        domain = "Customer" if "customer" in ds["dataset_name"] or "address" in ds["dataset_name"] else \
                 "Financial" if "claim" in ds["dataset_name"] else \
                 "Policy" if "polic" in ds["dataset_name"] else \
                 "Reference" if ds["row_count"] <= 100 else "General"
        ds_rows.append([ds["dataset_name"], f"{ds['row_count']:,}", str(ds["column_count"]), size_str, domain])
    pdf.add_table(ds_headers, ds_rows, [40, 25, 20, 25, 76])

    pdf.sub_title("3.2  Detalhamento de Colunas por Dataset")
    pii_keywords = {"cpf", "email", "phone", "telefone", "salary", "salario", "ssn", "rg", "nome", "name", "address", "endereco"}

    for ds in ds_meta:
        pdf.set_font("Helvetica", "B", 9)
        pdf.set_text_color(*pdf.TXT_ACCENT)
        pdf.set_x(12)
        pdf.cell(0, 6, f"{ds['dataset_name']}  ({ds['row_count']:,} linhas, {ds['column_count']} colunas)")
        pdf.ln(6)
        pdf.set_text_color(*pdf.TXT_DARK)

        col_headers = ["Coluna", "Tipo", "Formato", "Label", "PII"]
        col_rows = []
        for col in ds.get("columns", []):
            is_pii = col["name"].lower() in pii_keywords
            col_rows.append([col["name"], col["type"], col.get("format", "") or "-",
                             (col.get("label", "") or "-")[:25], "SIM" if is_pii else ""])
        pdf.add_table(col_headers, col_rows, [36, 14, 30, 76, 16])

        if pdf.get_y() > 255:
            pdf.add_page()

    # ==========================================
    # PAGINA: LINEAGE
    # ==========================================
    pdf.add_page()
    pdf.section_title("4", "DEPENDENCIAS E LINEAGE")

    pdf.body_text(
        "Grafo dirigido de dependencias construido a partir do parsing de codigo. "
        "Relaciona programas, datasets, macros e includes com arestas de reads/writes/calls/defines."
    )
    pdf.ln(1)

    nodes = lineage["nodes"]
    edges = lineage["edges"]
    types = Counter(n["type"] for n in nodes)
    rel_types = Counter(e["relationship"] for e in edges)
    targets_set = {e["target"] for e in edges}
    sources_set = {e["source"] for e in edges}
    all_ids = {n["id"] for n in nodes}
    roots = all_ids - targets_set
    leaves = all_ids - sources_set

    # Metricas
    y = pdf.get_y()
    pdf.metric_box("Nos", len(nodes), 12, y, 30, 22, pdf.BG_ACCENT)
    pdf.metric_box("Arestas", len(edges), 47, y, 30, 22, pdf.BG_ACCENT)
    pdf.metric_box("Raizes", len(roots), 82, y, 30, 22, pdf.BG_DARK)
    pdf.metric_box("Folhas", len(leaves), 117, y, 30, 22, pdf.BG_DARK)
    pdf.set_y(y + 28)

    pdf.sub_title("4.1  Composicao do Grafo")
    pdf.set_font("Helvetica", "", 9)
    for t, c in types.most_common():
        pdf.horizontal_bar(f"Nos: {t}", c, max(types.values()), 80, pdf.BG_ACCENT)
    pdf.ln(2)
    for r, c in rel_types.most_common():
        pdf.horizontal_bar(f"Arestas: {r}", c, max(rel_types.values()), 80, pdf.BG_DARK)

    pdf.ln(4)
    pdf.sub_title("4.2  Impacto por Programa")
    pdf.body_text("Ranking de programas pelo numero total de conexoes (upstream + downstream):")
    prog_nodes = [n for n in nodes if n["type"] == "program"]
    impact_rows = []
    for pn in sorted(prog_nodes, key=lambda x: len(builder.get_upstream(x["id"])) + len(builder.get_downstream(x["id"])), reverse=True):
        up = len(builder.get_upstream(pn["id"]))
        down = len(builder.get_downstream(pn["id"]))
        impact_rows.append([pn["label"], str(up), str(down), str(up + down)])
    pdf.add_table(["Programa", "Upstream", "Downstream", "Total"], impact_rows, [70, 30, 30, 30])

    pdf.sub_title("4.3  Nos Folha (saidas finais)")
    leaf_nodes = sorted([n for n in nodes if n["id"] in leaves], key=lambda x: x["label"])
    leaf_rows = [[n["label"], n["type"]] for n in leaf_nodes]
    pdf.add_table(["No", "Tipo"], leaf_rows, [130, 56])

    # ==========================================
    # PAGINA: CATALOGO DE DADOS
    # ==========================================
    pdf.add_page()
    pdf.section_title("5", "CATALOGO DE DADOS E CLASSIFICACAO")

    pdf.body_text(
        "Catalogo gerado com classificacao automatica de dominio, deteccao de PII "
        "por nome de coluna e classificacao de sensibilidade (Restricted/Internal/Public)."
    )
    pdf.ln(1)

    summary = catalog["summary"]

    # Metricas
    y = pdf.get_y()
    pdf.metric_box("Datasets", summary["total_datasets"], 12, y, 35, 22, pdf.BG_ACCENT)
    pdf.metric_box("Colunas", summary["total_columns"], 52, y, 35, 22, pdf.BG_ACCENT)
    pdf.metric_box("Com PII", summary["datasets_with_pii"], 92, y, 35, 22, pdf.RED)
    pdf.metric_box("Dominios", len(summary["domains"]), 132, y, 35, 22, pdf.BG_DARK)
    pdf.set_y(y + 28)

    pdf.sub_title("5.1  Classificacao de Sensibilidade")
    sens_colors = {"Restricted": pdf.RED, "Internal": pdf.ORANGE, "Public": pdf.GREEN}
    sens_descs = {
        "Restricted": "Contem PII - requer masking/encryption e controle de acesso rigoroso",
        "Internal": "Dados de negocio sem PII - acesso restrito por role",
        "Public": "Dados de referencia / lookup - acesso geral permitido",
    }
    for level in ["Restricted", "Internal", "Public"]:
        count = summary["sensitivity_distribution"].get(level, 0)
        pdf.horizontal_bar(level, count, max(summary["sensitivity_distribution"].values()), 80,
                          sens_colors.get(level, pdf.BG_ACCENT))
    pdf.ln(1)
    for level, desc in sens_descs.items():
        count = summary["sensitivity_distribution"].get(level, 0)
        pdf.set_font("Helvetica", "I", 7)
        pdf.set_x(16)
        pdf.cell(0, 4, f"  {level}: {desc}")
        pdf.ln(4)

    pdf.ln(3)
    pdf.sub_title("5.2  Classificacao por Dominio")
    for domain, count in summary["domains"].items():
        pdf.horizontal_bar(domain, count, max(summary["domains"].values()), 80, pdf.BG_ACCENT)

    pdf.ln(3)
    pdf.sub_title("5.3  Detalhamento de PII")
    for ds in catalog["datasets"]:
        pii = ds.get("pii_columns", [])
        if pii:
            pdf.set_font("Helvetica", "B", 9)
            pdf.set_text_color(*pdf.RED)
            pdf.set_x(16)
            pdf.cell(0, 6, f"ATENCAO: {ds['dataset_name']}")
            pdf.ln(6)
            pdf.set_text_color(*pdf.TXT_DARK)
            pdf.set_font("Helvetica", "", 9)
            pdf.set_x(20)
            pdf.cell(0, 5, f"Colunas PII encontradas: {', '.join(pii)}")
            pdf.ln(5)
            pdf.set_x(20)
            pdf.cell(0, 5, f"Sensibilidade: {ds['sensitivity']}  |  Dominio: {ds['domain']}")
            pdf.ln(5)
            pdf.set_x(20)
            pdf.cell(0, 5, "Recomendacao: Aplicar data masking antes da migracao. Revisar LGPD compliance.")
            pdf.ln(7)

    pdf.sub_title("5.4  Resumo por Dataset")
    cat_rows = []
    for ds in catalog["datasets"]:
        pii = ", ".join(ds.get("pii_columns", [])) or "-"
        cat_rows.append([ds["dataset_name"], ds["domain"], ds["sensitivity"],
                          str(ds["row_count"]), pii])
    pdf.add_table(["Dataset", "Dominio", "Sensibilidade", "Linhas", "PII"],
                   cat_rows, [38, 28, 28, 22, 70])

    # ==========================================
    # PAGINA: COMPLEXIDADE E GAPS
    # ==========================================
    pdf.add_page()
    pdf.section_title("6", "ANALISE DE COMPLEXIDADE E GAPS")

    pdf.body_text(
        "Score de complexidade calculado aditivamente: DATA steps (+1), MERGE (+5/tabela), "
        "PROC SQL (+3), PROCs estatisticos (+5), macros (+2), hash objects (+5), "
        "SQL dinamico (+5), includes (+1). Classificacao: LOW (0-10), MEDIUM (11-25), "
        "HIGH (26-50), VERY_HIGH (50+)."
    )
    pdf.ln(1)

    pdf.sub_title("6.1  Ranking de Complexidade")
    for p in sorted(parsed, key=lambda x: x["complexity_score"], reverse=True):
        color = colors.get(p["complexity_level"], pdf.BG_ACCENT)
        pdf.horizontal_bar(
            p["filename"][:30],
            p["complexity_score"],
            max(pp["complexity_score"] for pp in parsed),
            100,
            color
        )

    pdf.ln(3)
    pdf.sub_title("6.2  Composicao do Score por Programa")
    for p in sorted(parsed, key=lambda x: x["complexity_score"], reverse=True)[:5]:
        pdf.set_font("Helvetica", "B", 8)
        pdf.set_x(16)
        pdf.cell(0, 5, f"{p['filename']} (Score: {p['complexity_score']}, {p['complexity_level']})")
        pdf.ln(5)
        pdf.set_font("Helvetica", "", 8)

        factors = []
        if p["data_steps"]:
            factors.append(f"DATA steps ({len(p['data_steps'])}) = +{len(p['data_steps'])}")
        if p["merge_statements"]:
            factors.append(f"MERGE ({len(p['merge_statements'])} tabelas) = +{len(p['merge_statements']) * 5}")
        for proc in p["procs_used"]:
            if proc.upper() == "SQL":
                factors.append("PROC SQL = +3")
            elif proc.upper() in {"LOGISTIC", "REG", "GLM", "MIXED", "UNIVARIATE"}:
                factors.append(f"PROC {proc} (estatistico) = +5")
        if p["macro_definitions"]:
            factors.append(f"Macros ({len(p['macro_definitions'])}) = +{len(p['macro_definitions']) * 2}")
        if p["has_hash_objects"]:
            factors.append("Hash objects = +5")
        if p["has_dynamic_sql"]:
            factors.append("SQL dinamico = +5")

        for f in factors:
            pdf.set_x(24)
            pdf.cell(0, 4, f"- {f}")
            pdf.ln(4)
        pdf.ln(2)

    pdf.ln(2)
    pdf.sub_title("6.3  Gaps Identificados para Migracao")
    pdf.body_text(
        "Os seguintes padroes SAS nao possuem traducao direta para Snowflake SQL "
        "e requerem abordagens alternativas:"
    )
    gaps = [
        ("PROC LOGISTIC / REG / GLM", "Usar Snowpark ML, Python UDF ou ferramenta externa (SageMaker, Databricks ML)", "ALTO"),
        ("PROC REPORT / TABULATE", "Substituir por BI tool (Tableau, PowerBI) ou Snowsight dashboards", "MEDIO"),
        ("Hash Objects", "Substituir por tabelas temporarias + JOIN ou Snowpark Python", "MEDIO"),
        ("CALL EXECUTE / SQL Dinamico", "Usar Snowflake Scripting (Snowflake SQL Procedures) ou Tasks", "MEDIO"),
    ]
    pdf.add_table(["Gap", "Resolucao Sugerida", "Esforco"],
                   gaps, [55, 105, 26])

    # ==========================================
    # PAGINA: LIMITACOES E PROXIMOS PASSOS
    # ==========================================
    pdf.add_page()
    pdf.section_title("7", "LIMITACOES E PONTOS DE ATENCAO")

    limitations = [
        "O parser SAS e baseado em regex, nao e um parser AST completo. Cobertura estimada de ~80% dos padroes comuns.",
        "Metadados de datasets extraidos via .meta.json (mock). Em producao, usar pyreadstat para leitura nativa.",
        "Lineage inferido estaticamente do codigo. Dependencias de runtime (CALL EXECUTE) podem gerar caminhos adicionais.",
        "PROCs estatisticos (LOGISTIC, REG, GLM) nao possuem equivalente direto em Snowflake SQL.",
        "Integracao com LLM (OpenAI) desabilitada neste teste. Em producao, habilitar para enriquecimento do catalogo.",
        "Analise de encoding (Latin1 vs UTF-8) nao realizada no mock - verificar em ambiente real.",
    ]
    for i, lim in enumerate(limitations, 1):
        pdf.set_font("Helvetica", "", 9)
        pdf.set_x(16)
        pdf.multi_cell(176, 5, f"{i}. {lim}")
        pdf.ln(1)

    pdf.ln(4)
    pdf.section_title("8", "PROXIMOS PASSOS")

    steps = [
        ("Semana 1-2", "Validar inventario com stakeholders MAPFRE e DBA. Confirmar lineage com Product Owners."),
        ("Semana 2-3", "Executar MVP2 para gerar DDLs Snowflake, scripts COPY INTO e codigo transpilado."),
        ("Semana 3", "Configurar ambiente Snowflake (warehouse, databases, schemas, stages S3)."),
        ("Semana 3-4", "Habilitar revisao LLM para validacao de transpilacao e gap resolution."),
        ("Semana 4", "Teste de paridade de dados: carregar amostra e validar row counts, checksums."),
        ("Semana 5", "Definir ondas de migracao: Quick Wins (LOW), Core ETL (MEDIUM/HIGH), Modelos (VERY_HIGH)."),
        ("Semana 6", "Workshop de handover com equipe MAPFRE e apresentacao do Business Case."),
    ]
    pdf.add_table(["Timeline", "Acao"], steps, [30, 156])

    # ==========================================
    # PAGINA: APENDICE - MAPEAMENTO PLANILHA
    # ==========================================
    pdf.add_page()
    pdf.section_title("A", "APENDICE: MAPEAMENTO COM PLANILHA MAPFRE")

    pdf.body_text(
        "Correlacao entre as atividades da planilha 'Migracao Ecossistema SAS-SNOWFLAKES & AWS' "
        "e os modulos do toolkit executados neste MVP1:"
    )
    pdf.ln(1)

    mapping = [
        ["1.1", "Mapeamento de Repositorios", "FilesystemScanner", "Fase 1"],
        ["1.1.1", "Varredura de File Systems", "scan_programs()", "Fase 1"],
        ["1.1.6", "Validacao de Backup", "exclude_patterns", "Fase 1"],
        ["1.2", "Extracao de Metadados", "DataCatalogGenerator", "Fase 5"],
        ["1.3", "Conectividade Externa", "LIBNAME parser", "Fase 2"],
        ["1.4", "Catalogacao de Macros", "macro_definitions", "Fase 2"],
        ["1.4.6", "Dependencia Cruzada", "LineageBuilder", "Fase 4"],
        ["2.2", "Formatos e Informatas", "SASDataParser", "Fase 3"],
        ["2.3", "Lineage / Precedencia", "LineageBuilder", "Fase 4"],
        ["2.4", "Hard-Codings", "SASCodeParser", "Fase 2"],
        ["3.1", "Classificacao Complexidade", "complexity_score", "Fase 6"],
        ["3.2", "De-Para AWS/Snowflake", "SnowflakeTranspiler", "MVP2"],
        ["3.3", "Seguranca (PII)", "detect_pii", "Fase 5"],
    ]
    pdf.add_table(["ID", "Atividade Planilha", "Modulo Toolkit", "Fase"],
                   mapping, [14, 60, 56, 56])

    # Save
    os.makedirs(os.path.dirname(OUTPUT_PDF), exist_ok=True)
    pdf.output(OUTPUT_PDF)
    print(f"\nPDF gerado: {OUTPUT_PDF}")
    print(f"Tamanho: {os.path.getsize(OUTPUT_PDF):,} bytes")
    print(f"Paginas: {pdf.page_no()}")


if __name__ == "__main__":
    generate_pdf()
