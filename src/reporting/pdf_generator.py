"""
PDF Report Generator for SAS Migration Discovery.
Generates a professional PDF with all analysis results.
"""
import os
from datetime import datetime
from collections import Counter
from typing import Dict, List, Optional

from fpdf import FPDF

from src.utils.logger import get_logger

logger = get_logger("pdf_generator")


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

    def __init__(self, target_platform: str = "Snowflake"):
        super().__init__(orientation="P", unit="mm", format="A4")
        self.set_auto_page_break(auto=True, margin=20)
        self.target_platform = target_platform

    def header(self):
        if self.page_no() == 1:
            return
        self.set_fill_color(*self.BG_DARK)
        self.rect(0, 0, 210, 12, "F")
        self.set_font("Helvetica", "B", 7)
        self.set_text_color(*self.TXT_WHITE)
        self.set_xy(8, 3)
        self.cell(
            0, 5,
            f"SAS Migration Toolkit  |  Discovery Report  |  "
            f"Target: {self.target_platform}",
            align="L",
        )
        self.set_xy(0, 3)
        self.cell(202, 5, f"Pag. {self.page_no()}", align="R")
        self.set_y(16)

    def footer(self):
        self.set_y(-12)
        self.set_font("Helvetica", "I", 7)
        self.set_text_color(130, 130, 130)
        self.cell(
            0, 5,
            f"Gerado em {datetime.now().strftime('%d/%m/%Y %H:%M')}  "
            f"|  Confidencial  |  act digital",
            align="C",
        )

    def cover_page(self, config: Dict):
        self.add_page()
        self.set_fill_color(*self.BG_DARK)
        self.rect(0, 0, 210, 297, "F")

        self.set_fill_color(*self.BG_ACCENT)
        self.rect(0, 100, 210, 4, "F")

        self.set_font("Helvetica", "B", 32)
        self.set_text_color(*self.TXT_WHITE)
        self.set_xy(20, 50)
        self.cell(0, 14, "Discovery Report")

        self.set_font("Helvetica", "", 18)
        self.set_xy(20, 68)
        self.cell(0, 10, "SAS Migration Toolkit")

        self.set_font("Helvetica", "", 13)
        self.set_text_color(180, 195, 220)
        client = config.get("project", {}).get("client", "")
        project = config.get("project", {}).get("name", "")
        self.set_xy(20, 115)
        self.cell(0, 8, f"Cliente: {client}")
        self.set_xy(20, 126)
        self.cell(0, 8, f"Projeto: {project}")
        self.set_xy(20, 137)
        self.cell(0, 8, f"Data: {datetime.now().strftime('%d/%m/%Y')}")
        self.set_xy(20, 148)
        self.cell(0, 8, f"Plataforma alvo: {self.target_platform}")

        self.set_fill_color(*self.BG_ACCENT)
        self.rect(0, 270, 210, 27, "F")
        self.set_font("Helvetica", "B", 10)
        self.set_text_color(*self.TXT_WHITE)
        self.set_xy(20, 276)
        self.cell(
            0, 6,
            "CONFIDENCIAL  |  act digital  |  SAS Migration Practice",
        )

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

    def add_table(self, headers, rows, col_widths=None):
        if col_widths is None:
            col_widths = [186 / len(headers)] * len(headers)

        x_start = 12
        self._draw_table_header(headers, col_widths, x_start)

        self.set_text_color(*self.TXT_DARK)
        self.set_font("Helvetica", "", 8)
        for row_idx, row in enumerate(rows):
            if self.get_y() > 265:
                self.add_page()
                self._draw_table_header(headers, col_widths, x_start)
                self.set_text_color(*self.TXT_DARK)
                self.set_font("Helvetica", "", 8)

            if row_idx % 2 == 0:
                self.set_fill_color(*self.BG_LIGHT)
            else:
                self.set_fill_color(*self.BG_WHITE)
            self.set_x(x_start)
            for i, val in enumerate(row):
                align = "L" if i == 0 else "C"
                self.cell(
                    col_widths[i], 6, str(val),
                    border=0, fill=True, align=align,
                )
            self.ln(6)
        self.ln(2)

    def _draw_table_header(self, headers, col_widths, x_start):
        self.set_fill_color(*self.BG_DARK)
        self.set_text_color(*self.TXT_WHITE)
        self.set_font("Helvetica", "B", 8)
        self.set_x(x_start)
        for i, h in enumerate(headers):
            self.cell(
                col_widths[i], 7, h,
                border=0, fill=True, align="C",
            )
        self.ln(7)

    def metric_box(self, label, value, x, y, w=40, h=22, color=None):
        if color is None:
            color = self.BG_ACCENT
        self.set_fill_color(*color)
        self.rect(x, y, w, h, "F")
        self.set_font("Helvetica", "B", 16)
        self.set_text_color(*self.TXT_WHITE)
        self.set_xy(x, y + 2)
        self.cell(w, 10, str(value), align="C")
        self.set_font("Helvetica", "", 7)
        self.set_xy(x, y + 12)
        self.cell(w, 6, label, align="C")

    def horizontal_bar(self, label, value, max_val, width=120, color=None):
        if color is None:
            color = self.BG_ACCENT
        bar_w = (value / max_val * width) if max_val > 0 else 0
        self.set_x(16)
        self.set_font("Helvetica", "", 8)
        self.cell(40, 6, label)
        y = self.get_y() + 1
        x = self.get_x()
        self.set_fill_color(220, 220, 220)
        self.rect(x, y, width, 4, "F")
        self.set_fill_color(*color)
        self.rect(x, y, bar_w, 4, "F")
        self.set_font("Helvetica", "B", 8)
        self.set_xy(x + width + 2, self.get_y())
        self.cell(20, 6, str(value))
        self.ln(7)


class PDFReportGenerator:
    """Generates a complete PDF discovery report."""

    PII_KEYWORDS = {
        "cpf", "email", "phone", "telefone", "salary", "salario",
        "ssn", "rg", "nome", "name", "address", "endereco",
    }

    def __init__(self, config: Dict):
        self.config = config
        target = config.get("target", {})
        platform = target.get("platform", "snowflake")
        self.target_platform = (
            "Snowflake + AWS" if platform == "snowflake"
            else "Databricks" if platform == "databricks"
            else platform
        )

    def generate(
        self,
        parsed_programs: List[Dict],
        datasets_metadata: List[Dict],
        lineage: Dict,
        catalog_data: Optional[Dict] = None,
        validation_findings: Optional[List[Dict]] = None,
        output_path: str = "discovery_report.pdf",
    ) -> str:
        os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)

        pdf = MigrationPDF(self.target_platform)

        # Cover
        pdf.cover_page(self.config)

        # 1. Executive Summary
        self._page_executive_summary(
            pdf, parsed_programs, datasets_metadata, lineage,
            validation_findings=validation_findings,
        )

        # 2. Program Inventory
        self._page_program_inventory(pdf, parsed_programs)

        # 3. Dataset Inventory
        self._page_dataset_inventory(pdf, datasets_metadata)

        # 4. Lineage
        self._page_lineage(pdf, lineage)

        # 5. Data Catalog
        if catalog_data:
            self._page_catalog(pdf, catalog_data)

        # 6. Complexity (4 dimensions)
        self._page_complexity(pdf, parsed_programs)

        # 7. Validation Findings
        self._page_validation_findings(pdf, validation_findings or [])

        # 8. Limitations & Next Steps
        self._page_limitations(pdf)

        pdf.output(output_path)
        size = os.path.getsize(output_path)
        logger.info(
            "PDF report generated: %s (%d pages, %s bytes)",
            output_path, pdf.page_no(), f"{size:,}",
        )
        return output_path

    def _page_executive_summary(self, pdf, parsed, ds_meta, lineage,
                                validation_findings=None):
        pdf.add_page()
        pdf.section_title("1", "SUMARIO EXECUTIVO")

        client = self.config.get("project", {}).get("client", "")
        pdf.body_text(
            f"Este relatorio apresenta os resultados da fase de Discovery do projeto "
            f"de migracao do ecossistema SAS para {self.target_platform} "
            f"{'de ' + client if client else ''}. A analise abrange o inventario "
            f"completo de programas, datasets, dependencias, complexidade, "
            f"integridade de codigo e classificacao de dados do ambiente SAS existente."
        )
        pdf.ln(2)

        y = pdf.get_y()
        pdf.metric_box("Programas", len(parsed), 12, y, 35, 22, pdf.BG_ACCENT)
        pdf.metric_box("Datasets", len(ds_meta), 52, y, 35, 22, pdf.BG_ACCENT)
        total_lines = sum(p.get("line_count", 0) for p in parsed)
        pdf.metric_box("Linhas", f"{total_lines:,}", 92, y, 35, 22, pdf.BG_ACCENT)
        pdf.metric_box(
            "Nos Lineage", len(lineage.get("nodes", [])),
            132, y, 35, 22, pdf.BG_DARK,
        )
        findings = validation_findings or []
        n_critical = len([f for f in findings if f.get("severity") == "CRITICAL"])
        finding_color = pdf.RED if n_critical > 0 else pdf.GREEN
        pdf.metric_box(
            "Achados", len(findings),
            172, y, 28, 22, finding_color,
        )
        pdf.set_y(y + 28)

        pdf.sub_title("Distribuicao de Complexidade")
        dist = Counter(p.get("complexity_level", "UNKNOWN") for p in parsed)
        max_c = max(dist.values()) if dist else 1
        colors = {
            "LOW": pdf.GREEN, "MEDIUM": pdf.YELLOW,
            "HIGH": pdf.ORANGE, "VERY_HIGH": pdf.RED,
        }
        for level in ["LOW", "MEDIUM", "HIGH", "VERY_HIGH"]:
            count = dist.get(level, 0)
            pdf.horizontal_bar(
                level, count, max_c, 100,
                colors.get(level, pdf.BG_ACCENT),
            )

        pdf.ln(2)
        pdf.sub_title("Recomendacao Estrategica")
        has_complex = any(
            p.get("complexity_level") in ("HIGH", "VERY_HIGH") for p in parsed
        )
        if has_complex:
            strategy = (
                "Migracao faseada recomendada. O ambiente possui programas de alta "
                "complexidade que requerem revisao manual. Sugerimos 3 ondas: "
                "Quick Wins (LOW), Core ETL (MEDIUM/HIGH) e Modelos Avancados "
                "(VERY_HIGH)."
            )
        else:
            strategy = "Migracao direta viavel para todos os programas."
        pdf.body_text(strategy)

    def _page_program_inventory(self, pdf, parsed):
        pdf.add_page()
        pdf.section_title("2", "INVENTARIO DE PROGRAMAS SAS")

        headers = ["Programa", "Linhas", "Score", "Nivel", "PROCs", "R/W"]
        rows = []
        for p in sorted(parsed, key=lambda x: x.get("complexity_score", 0), reverse=True):
            procs = ", ".join(p.get("procs_used", [])[:4])
            rw = f"{len(p.get('datasets_read', []))}/{len(p.get('datasets_written', []))}"
            rows.append([
                p.get("filename", ""),
                str(p.get("line_count", 0)),
                str(p.get("complexity_score", 0)),
                p.get("complexity_level", ""),
                procs, rw,
            ])
        pdf.add_table(headers, rows, [52, 14, 14, 20, 60, 26])

        # Special features
        pdf.sub_title("Features Especiais Detectadas")
        features = [
            ("Hash Objects", [p.get("filename", "") for p in parsed if p.get("has_hash_objects")]),
            ("SQL Dinamico", [p.get("filename", "") for p in parsed if p.get("has_dynamic_sql")]),
            ("MERGE", [p.get("filename", "") for p in parsed if p.get("merge_statements")]),
        ]
        for feat_name, progs in features:
            pdf.set_font("Helvetica", "B", 9)
            pdf.set_x(16)
            pdf.cell(60, 5, feat_name + ":")
            pdf.set_font("Helvetica", "", 9)
            pdf.cell(0, 5, ", ".join(progs) if progs else "Nenhum")
            pdf.ln(6)

    def _page_dataset_inventory(self, pdf, ds_meta):
        pdf.add_page()
        pdf.section_title("3", "INVENTARIO DE DATASETS")

        headers = ["Dataset", "Linhas", "Colunas", "Tamanho"]
        rows = []
        for ds in sorted(ds_meta, key=lambda x: x.get("row_count", 0), reverse=True):
            sz = ds.get("size_bytes", 0)
            size_str = f"{sz/1024:.1f} KB" if sz >= 1024 else f"{sz} B"
            rows.append([
                ds.get("dataset_name", ""),
                f"{ds.get('row_count', 0):,}",
                str(ds.get("column_count", 0)),
                size_str,
            ])
        pdf.add_table(headers, rows, [50, 30, 25, 81])

        # Column detail for top datasets
        pdf.sub_title("Detalhamento de Colunas")
        for ds in ds_meta[:3]:
            pdf.set_font("Helvetica", "B", 9)
            pdf.set_text_color(*pdf.TXT_ACCENT)
            pdf.set_x(12)
            pdf.cell(
                0, 6,
                f"{ds.get('dataset_name', '')} "
                f"({ds.get('row_count', 0):,} linhas, "
                f"{ds.get('column_count', 0)} colunas)",
            )
            pdf.ln(6)
            pdf.set_text_color(*pdf.TXT_DARK)

            col_headers = ["Coluna", "Tipo", "Formato", "Label", "PII"]
            col_rows = []
            for col in ds.get("columns", [])[:15]:
                is_pii = col.get("name", "").lower() in self.PII_KEYWORDS
                col_rows.append([
                    col.get("name", ""),
                    col.get("type", ""),
                    col.get("format", "") or "-",
                    (col.get("label", "") or "-")[:25],
                    "SIM" if is_pii else "",
                ])
            pdf.add_table(col_headers, col_rows, [36, 14, 30, 76, 16])

            if pdf.get_y() > 255:
                pdf.add_page()

    def _page_lineage(self, pdf, lineage):
        pdf.add_page()
        pdf.section_title("4", "DEPENDENCIAS E LINEAGE")

        nodes = lineage.get("nodes", [])
        edges = lineage.get("edges", [])
        types = Counter(n.get("type", "unknown") for n in nodes)

        y = pdf.get_y()
        pdf.metric_box("Nos", len(nodes), 12, y, 40, 22, pdf.BG_ACCENT)
        pdf.metric_box("Arestas", len(edges), 57, y, 40, 22, pdf.BG_ACCENT)

        targets_set = {e["target"] for e in edges}
        sources_set = {e["source"] for e in edges}
        all_ids = {n["id"] for n in nodes}
        roots = all_ids - targets_set
        leaves = all_ids - sources_set

        pdf.metric_box("Raizes", len(roots), 102, y, 40, 22, pdf.BG_DARK)
        pdf.metric_box("Folhas", len(leaves), 147, y, 40, 22, pdf.BG_DARK)
        pdf.set_y(y + 28)

        pdf.sub_title("Composicao do Grafo")
        max_type = max(types.values()) if types else 1
        for t, c in types.most_common():
            pdf.horizontal_bar(f"Nos: {t}", c, max_type, 80, pdf.BG_ACCENT)

    def _page_catalog(self, pdf, catalog):
        pdf.add_page()
        pdf.section_title("5", "CATALOGO DE DADOS E CLASSIFICACAO")

        summary = catalog.get("summary", {})

        y = pdf.get_y()
        pdf.metric_box(
            "Datasets", summary.get("total_datasets", 0),
            12, y, 35, 22, pdf.BG_ACCENT,
        )
        pdf.metric_box(
            "Colunas", summary.get("total_columns", 0),
            52, y, 35, 22, pdf.BG_ACCENT,
        )
        pdf.metric_box(
            "Com PII", summary.get("datasets_with_pii", 0),
            92, y, 35, 22, pdf.RED,
        )
        pdf.metric_box(
            "Dominios", len(summary.get("domains", {})),
            132, y, 35, 22, pdf.BG_DARK,
        )
        pdf.set_y(y + 28)

        pdf.sub_title("Classificacao de Sensibilidade")
        sens_dist = summary.get("sensitivity_distribution", {})
        sens_colors = {
            "Restricted": pdf.RED,
            "Internal": pdf.ORANGE,
            "Public": pdf.GREEN,
        }
        max_sens = max(sens_dist.values()) if sens_dist else 1
        for level in ["Restricted", "Internal", "Public"]:
            count = sens_dist.get(level, 0)
            pdf.horizontal_bar(
                level, count, max_sens, 80,
                sens_colors.get(level, pdf.BG_ACCENT),
            )

        pdf.ln(3)
        pdf.sub_title("Resumo por Dataset")
        cat_rows = []
        for ds in catalog.get("datasets", []):
            pii = ", ".join(ds.get("pii_columns", [])) or "-"
            cat_rows.append([
                ds.get("dataset_name", ""),
                ds.get("domain", ""),
                ds.get("sensitivity", ""),
                str(ds.get("row_count", 0)),
                pii,
            ])
        pdf.add_table(
            ["Dataset", "Dominio", "Sensibilidade", "Linhas", "PII"],
            cat_rows, [38, 28, 28, 22, 70],
        )

    def _page_complexity(self, pdf, parsed):
        pdf.add_page()
        pdf.section_title("6", "ANALISE DE COMPLEXIDADE (CT = PLP + PDI + PVD + PRS)")

        pdf.body_text(
            "Complexidade calculada em 4 dimensoes: "
            "PLP (Logica de Programacao), PDI (Dependencias e Integracao), "
            "PVD (Volume e Variedade de Dados), PRS (Recursos Especificos SAS). "
            "Nivel por dimensao: Baixa (1-2), Media (3-5), Alta (>5)."
        )
        pdf.ln(2)

        # Complexity bars
        colors = {
            "LOW": pdf.GREEN, "MEDIUM": pdf.YELLOW,
            "HIGH": pdf.ORANGE, "VERY_HIGH": pdf.RED,
        }
        max_score = max(
            (p.get("complexity_score", 0) for p in parsed), default=1
        )
        for p in sorted(parsed, key=lambda x: x.get("complexity_score", 0), reverse=True):
            color = colors.get(p.get("complexity_level", ""), pdf.BG_ACCENT)
            pdf.horizontal_bar(
                p.get("filename", "")[:30],
                p.get("complexity_score", 0),
                max_score, 100, color,
            )

        pdf.ln(3)
        pdf.sub_title("Detalhamento por Dimensao")

        headers = ["Programa", "CT", "Nivel", "PLP", "PDI", "PVD", "PRS", "Esforco"]
        rows = []
        for p in sorted(parsed, key=lambda x: x.get("complexity_score", 0), reverse=True):
            dims = p.get("complexity_dimensions", {})
            rows.append([
                p.get("filename", "")[:28],
                str(dims.get("CT", p.get("complexity_score", 0))),
                p.get("complexity_level", ""),
                f"{dims.get('PLP', '?')} ({dims.get('PLP_nivel', '?')[:1]})",
                f"{dims.get('PDI', '?')} ({dims.get('PDI_nivel', '?')[:1]})",
                f"{dims.get('PVD', '?')} ({dims.get('PVD_nivel', '?')[:1]})",
                f"{dims.get('PRS', '?')} ({dims.get('PRS_nivel', '?')[:1]})",
                dims.get("esforco_hh", "N/A"),
            ])
        pdf.add_table(headers, rows, [38, 12, 22, 20, 20, 20, 20, 34])

        pdf.ln(2)
        pdf.sub_title("Fatores Detalhados")
        det_headers = ["Programa", "Hash", "Dyn SQL", "Macros", "Joins", "IF/THEN", "Linhas"]
        det_rows = []
        for p in sorted(parsed, key=lambda x: x.get("complexity_score", 0), reverse=True):
            det_rows.append([
                p.get("filename", "")[:28],
                "Sim" if p.get("has_hash_objects") else "-",
                "Sim" if p.get("has_dynamic_sql") else "-",
                str(len(p.get("macro_definitions", []))),
                str(p.get("join_count", 0)),
                str(p.get("if_then_chains", 0)),
                str(p.get("line_count", 0)),
            ])
        pdf.add_table(det_headers, det_rows, [38, 18, 22, 22, 22, 24, 40])

        # Gaps
        if pdf.get_y() > 220:
            pdf.add_page()
        pdf.ln(2)
        pdf.sub_title("Gaps Identificados para Migracao")
        target_short = (
            "Snowflake" if "snowflake" in self.target_platform.lower()
            else "Databricks"
        )
        if target_short == "Snowflake":
            gaps = [
                ("PROC LOGISTIC/REG/GLM", "Snowpark ML ou SageMaker", "ALTO"),
                ("PROC REPORT/TABULATE", "BI tool (Tableau, PowerBI)", "MEDIO"),
                ("Hash Objects", "Tabelas temporarias + JOIN", "MEDIO"),
                ("CALL EXECUTE", "Snowflake Scripting / Tasks", "MEDIO"),
            ]
        else:
            gaps = [
                ("PROC LOGISTIC/REG/GLM", "Spark MLlib ou MLflow", "ALTO"),
                ("PROC REPORT/TABULATE", "Databricks SQL Dashboards", "MEDIO"),
                ("Hash Objects", "Broadcast joins / Delta temp tables", "MEDIO"),
                ("CALL EXECUTE", "Databricks Workflows / Notebooks", "MEDIO"),
            ]
        pdf.add_table(
            ["Gap", "Resolucao Sugerida", "Esforco"],
            gaps, [55, 105, 26],
        )

    def _page_validation_findings(self, pdf, findings: List[Dict]):
        pdf.add_page()
        pdf.section_title("7", "ANALISE DE INTEGRIDADE E QUALIDADE DO CODIGO")

        if not findings:
            pdf.body_text("Nenhum problema de integridade detectado.")
            return

        # Summary boxes
        severity_counts = Counter(f.get("severity", "?") for f in findings)
        y = pdf.get_y()
        n_crit = severity_counts.get("CRITICAL", 0)
        n_high = severity_counts.get("HIGH", 0)
        n_med = severity_counts.get("MEDIUM", 0)
        n_low = severity_counts.get("LOW", 0)
        pdf.metric_box("CRITICO", n_crit, 12, y, 42, 22, pdf.RED if n_crit else pdf.GREEN)
        pdf.metric_box("ALTO", n_high, 59, y, 42, 22, pdf.ORANGE if n_high else pdf.GREEN)
        pdf.metric_box("MEDIO", n_med, 106, y, 42, 22, pdf.YELLOW if n_med else pdf.GREEN)
        pdf.metric_box("BAIXO", n_low, 153, y, 42, 22, pdf.BG_DARK)
        pdf.set_y(y + 28)

        # Summary table
        pdf.sub_title("Resumo dos Achados")
        summary_rows = []
        for sev in ["CRITICAL", "HIGH", "MEDIUM", "LOW"]:
            items = [f for f in findings if f.get("severity") == sev]
            if items:
                categories = ", ".join(sorted(set(f.get("category", "") for f in items)))
                programs = ", ".join(sorted(set(f.get("program", "")[:25] for f in items)))
                summary_rows.append([sev, str(len(items)), categories[:45], programs[:40]])
        pdf.add_table(
            ["Severidade", "Qtde", "Categorias", "Programas"],
            summary_rows, [24, 12, 80, 70],
        )

        # Recommendation type labels
        rec_labels = {
            "EXTRACAO_MANUAL": "Extracao Manual",
            "CORRECAO_CODIGO": "Correcao de Codigo",
            "CONFIGURACAO_EXPORT": "Config. Export",
            "REVISAO_MANUAL": "Revisao Manual",
        }

        # Detailed findings
        pdf.sub_title("Detalhamento dos Achados")
        for i, f in enumerate(findings, 1):
            if pdf.get_y() > 235:
                pdf.add_page()

            sev = f.get("severity", "?")
            sev_colors = {
                "CRITICAL": pdf.RED, "HIGH": pdf.ORANGE,
                "MEDIUM": pdf.YELLOW, "LOW": pdf.BG_DARK,
            }
            color = sev_colors.get(sev, pdf.BG_ACCENT)

            # Finding header bar
            pdf.set_fill_color(*color)
            pdf.rect(12, pdf.get_y(), 186, 7, "F")
            pdf.set_font("Helvetica", "B", 8)
            pdf.set_text_color(*pdf.TXT_WHITE)
            pdf.set_x(14)
            rec_type = rec_labels.get(f.get("recommendation_type", ""), f.get("recommendation_type", ""))
            line_info = f" (linha ~{f['line']})" if f.get("line", 0) > 0 else ""
            header_text = f"#{i} [{sev}] {f.get('category', '')} | {f.get('program', '')}{line_info}"
            pdf.cell(182, 7, header_text[:90])
            pdf.ln(9)
            pdf.set_text_color(*pdf.TXT_DARK)

            # Description
            pdf.set_font("Helvetica", "B", 8)
            pdf.set_x(14)
            pdf.cell(22, 5, "Erro:")
            pdf.set_font("Helvetica", "", 8)
            pdf.set_x(14)
            pdf.ln(5)
            pdf.set_x(16)
            pdf.multi_cell(180, 4, f.get("description", "")[:200])

            # Impact
            pdf.set_font("Helvetica", "B", 8)
            pdf.set_x(14)
            pdf.cell(22, 5, "Impacto:")
            pdf.set_font("Helvetica", "", 8)
            pdf.ln(5)
            pdf.set_x(16)
            pdf.multi_cell(180, 4, f.get("impact", "")[:200])

            # Recommendation
            pdf.set_font("Helvetica", "B", 8)
            pdf.set_x(14)
            pdf.cell(22, 5, f"Acao ({rec_type}):")
            pdf.set_font("Helvetica", "", 8)
            pdf.ln(5)
            pdf.set_x(16)
            pdf.multi_cell(180, 4, f.get("recommendation", "")[:250])

            pdf.ln(3)

    def _page_limitations(self, pdf):
        pdf.add_page()
        pdf.section_title("8", "LIMITACOES E PROXIMOS PASSOS")

        limitations = [
            "Parser SAS baseado em regex (~80% de cobertura dos padroes comuns).",
            "Metadados de datasets extraidos via .meta.json (fallback).",
            "Lineage inferido estaticamente do codigo.",
            "PROCs estatisticos nao possuem equivalente direto.",
            "Integracao LLM opcional para enriquecimento do catalogo.",
        ]
        for i, lim in enumerate(limitations, 1):
            pdf.set_font("Helvetica", "", 9)
            pdf.set_x(16)
            pdf.multi_cell(176, 5, f"{i}. {lim}")
            pdf.ln(1)

        pdf.ln(4)
        pdf.section_title("8", "PROXIMOS PASSOS")

        steps = [
            ("Semana 1-2", "Validar inventario com stakeholders e DBA."),
            ("Semana 2-3", "Gerar DDLs, COPY INTO e codigo transpilado."),
            ("Semana 3", "Configurar ambiente de destino."),
            ("Semana 3-4", "Habilitar revisao LLM para gap resolution."),
            ("Semana 4", "Teste de paridade de dados."),
            ("Semana 5", "Definir ondas de migracao."),
            ("Semana 6", "Workshop de handover com equipe."),
        ]
        pdf.add_table(["Timeline", "Acao"], steps, [30, 156])
