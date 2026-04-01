import os
from datetime import datetime
from typing import Dict, List, Optional

from src.utils.logger import get_logger

logger = get_logger("report_generator")


class ReportGenerator:
    def __init__(self, config: Dict):
        self.config = config
        self.project_name = config.get("project", {}).get("name", "SAS Migration")
        self.client = config.get("project", {}).get("client", "")

    def generate_discovery_report(self, programs: List[Dict], datasets: List[Dict],
                                   lineage: Dict, complexity_report: Optional[Dict] = None,
                                   validation_findings: Optional[List[Dict]] = None,
                                   output_dir: str = "output",
                                   format: str = "markdown") -> str:
        os.makedirs(output_dir, exist_ok=True)
        report = self._build_markdown(programs, datasets, lineage, complexity_report,
                                       validation_findings=validation_findings)
        filepath = os.path.join(output_dir, "discovery_report.md")
        with open(filepath, "w", encoding="utf-8") as f:
            f.write(report)
        logger.info("Discovery report generated: %s", filepath)
        return filepath

    def _build_markdown(self, programs, datasets, lineage, complexity_report,
                        validation_findings=None) -> str:
        sections = []

        # 1. Executive Summary
        sections.append(self._section_executive_summary(programs, datasets, lineage,
                                                         validation_findings=validation_findings))
        # 2. Program Inventory
        sections.append(self._section_program_inventory(programs))
        # 3. Dataset Inventory
        sections.append(self._section_dataset_inventory(datasets))
        # 4. Dependencies and Lineage
        sections.append(self._section_lineage(lineage))
        # 5. Complexity Analysis
        sections.append(self._section_complexity(programs))
        # 6. Code Integrity and Validation Findings
        sections.append(self._section_validation_findings(validation_findings or []))
        # 7. Limitations
        sections.append(self._section_limitations())
        # 8. Next Steps
        sections.append(self._section_next_steps())

        header = (
            f"# SAS Environment Discovery Report\n\n"
            f"**Project:** {self.project_name}  \n"
            f"**Client:** {self.client}  \n"
            f"**Generated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}  \n\n"
            f"---\n\n"
        )
        return header + "\n\n".join(sections)

    def _section_executive_summary(self, programs, datasets, lineage,
                                    validation_findings=None) -> str:
        complexity_dist = {}
        for p in programs:
            level = p.get("complexity_level", "UNKNOWN")
            complexity_dist[level] = complexity_dist.get(level, 0) + 1

        total_procs = set()
        for p in programs:
            total_procs.update(p.get("procs_used", []))

        strategy = "Phased migration recommended" if any(
            p.get("complexity_level") in ("HIGH", "VERY_HIGH") for p in programs
        ) else "Straightforward migration feasible"

        # Validation findings summary
        findings = validation_findings or []
        severity_counts = {}
        for f in findings:
            sev = f.get("severity", "UNKNOWN")
            severity_counts[sev] = severity_counts.get(sev, 0) + 1

        validation_line = ""
        if findings:
            validation_line = (
                f"| Achados de Validacao | {len(findings)} "
                f"({', '.join(f'{k}: {v}' for k, v in sorted(severity_counts.items()))}) |\n"
            )

        return (
            f"## 1. Executive Summary\n\n"
            f"| Metric | Value |\n"
            f"|--------|-------|\n"
            f"| Total Programs | {len(programs)} |\n"
            f"| Total Datasets | {len(datasets)} |\n"
            f"| Lineage Nodes | {len(lineage.get('nodes', []))} |\n"
            f"| Lineage Edges | {len(lineage.get('edges', []))} |\n"
            f"| Unique PROCs | {len(total_procs)} |\n"
            f"| Complexity Distribution | {complexity_dist} |\n"
            f"{validation_line}\n"
            f"**Recommended Strategy:** {strategy}\n"
        )

    def _section_program_inventory(self, programs) -> str:
        lines = ["## 2. Program Inventory\n"]
        lines.append("| Program | Lines | Complexity | PROCs | Datasets Read | Datasets Written |")
        lines.append("|---------|-------|------------|-------|---------------|------------------|")
        for p in sorted(programs, key=lambda x: x.get("complexity_score", 0), reverse=True):
            lines.append(
                f"| {p.get('filename', '')} | {p.get('line_count', 0)} | "
                f"{p.get('complexity_level', '')} ({p.get('complexity_score', 0)}) | "
                f"{', '.join(p.get('procs_used', []))} | "
                f"{len(p.get('datasets_read', []))} | "
                f"{len(p.get('datasets_written', []))} |"
            )
        return "\n".join(lines)

    def _section_dataset_inventory(self, datasets) -> str:
        lines = ["## 3. Dataset Inventory\n"]
        lines.append("| Dataset | Rows | Columns | Size |")
        lines.append("|---------|------|---------|------|")
        for ds in sorted(datasets, key=lambda x: x.get("row_count", 0), reverse=True):
            from src.utils.helpers import format_bytes
            size = format_bytes(ds.get("size_bytes", 0)) if "size_bytes" in ds else "N/A"
            lines.append(
                f"| {ds.get('dataset_name', ds.get('filename', ''))} | "
                f"{ds.get('row_count', 'N/A')} | {ds.get('column_count', 'N/A')} | {size} |"
            )
        return "\n".join(lines)

    def _section_lineage(self, lineage) -> str:
        nodes = lineage.get("nodes", [])
        edges = lineage.get("edges", [])
        node_types = {}
        for n in nodes:
            t = n.get("type", "unknown")
            node_types[t] = node_types.get(t, 0) + 1

        # Find roots (no incoming edges) and leaves (no outgoing edges)
        targets = {e["target"] for e in edges}
        sources = {e["source"] for e in edges}
        all_ids = {n["id"] for n in nodes}
        roots = all_ids - targets
        leaves = all_ids - sources

        return (
            f"## 4. Dependencies and Lineage\n\n"
            f"| Metric | Value |\n"
            f"|--------|-------|\n"
            f"| Total Nodes | {len(nodes)} |\n"
            f"| Total Edges | {len(edges)} |\n"
            f"| Node Types | {node_types} |\n"
            f"| Root Nodes | {len(roots)} |\n"
            f"| Leaf Nodes | {len(leaves)} |\n"
        )

    def _section_complexity(self, programs) -> str:
        lines = ["## 5. Complexity Analysis\n"]
        lines.append("### Top 20 Most Complex Programs\n")
        lines.append("| Rank | Program | Score | Level | Key Factors |")
        lines.append("|------|---------|-------|-------|-------------|")
        sorted_progs = sorted(programs, key=lambda x: x.get("complexity_score", 0), reverse=True)[:20]
        for i, p in enumerate(sorted_progs, 1):
            factors = []
            if p.get("has_hash_objects"):
                factors.append("Hash Objects")
            if p.get("has_dynamic_sql"):
                factors.append("Dynamic SQL")
            if p.get("merge_statements"):
                factors.append(f"MERGE ({len(p['merge_statements'])} tables)")
            if p.get("macro_definitions"):
                factors.append(f"Macros ({len(p['macro_definitions'])})")
            lines.append(
                f"| {i} | {p.get('filename', '')} | {p.get('complexity_score', 0)} | "
                f"{p.get('complexity_level', '')} | {', '.join(factors) or 'Standard'} |"
            )
        return "\n".join(lines)

    def _section_validation_findings(self, findings: List[Dict]) -> str:
        """Generate detailed validation findings section."""
        lines = ["## 6. Analise de Integridade e Qualidade do Codigo\n"]

        if not findings:
            lines.append("Nenhum problema de integridade detectado.\n")
            return "\n".join(lines)

        # Group by severity
        severity_order = ["CRITICAL", "HIGH", "MEDIUM", "LOW"]
        severity_labels = {
            "CRITICAL": "CRITICO - Requer atencao imediata",
            "HIGH": "ALTO - Impacto significativo nos resultados",
            "MEDIUM": "MEDIO - Revisar antes da migracao",
            "LOW": "BAIXO - Verificar se e intencional",
        }
        recommendation_labels = {
            "EXTRACAO_MANUAL": "Extracao Manual",
            "CORRECAO_CODIGO": "Correcao de Codigo",
            "CONFIGURACAO_EXPORT": "Configuracao de Export",
            "REVISAO_MANUAL": "Revisao Manual",
        }

        grouped = {}
        for f in findings:
            sev = f.get("severity", "UNKNOWN")
            if sev not in grouped:
                grouped[sev] = []
            grouped[sev].append(f)

        # Summary table
        lines.append("### Resumo dos Achados\n")
        lines.append("| Severidade | Quantidade | Categorias |")
        lines.append("|------------|------------|------------|")
        for sev in severity_order:
            if sev in grouped:
                items = grouped[sev]
                categories = set(f.get("category", "") for f in items)
                lines.append(
                    f"| **{sev}** | {len(items)} | "
                    f"{', '.join(sorted(categories))} |"
                )
        lines.append("")

        # Detailed findings
        finding_num = 0
        for sev in severity_order:
            if sev not in grouped:
                continue

            lines.append(f"### {severity_labels.get(sev, sev)}\n")

            for f in grouped[sev]:
                finding_num += 1
                rec_type = f.get("recommendation_type", "REVISAO_MANUAL")
                rec_label = recommendation_labels.get(rec_type, rec_type)
                line_info = f" (linha ~{f['line']})" if f.get("line", 0) > 0 else ""

                lines.append(f"#### Achado #{finding_num}: [{sev}] {f['category']}\n")
                lines.append(f"**Programa:** `{f.get('program', 'N/A')}`{line_info}\n")
                lines.append(f"**Erro encontrado:**  ")
                lines.append(f"{f.get('description', 'N/A')}\n")
                lines.append(f"**Por que nao foi processado / Impacto:**  ")
                lines.append(f"{f.get('impact', 'N/A')}\n")
                lines.append(f"**Recomendacao ({rec_label}):**  ")
                lines.append(f"{f.get('recommendation', 'N/A')}\n")
                lines.append("---\n")

        return "\n".join(lines)

    def _section_limitations(self) -> str:
        return (
            "## 7. Limitations and Notes\n\n"
            "- SAS code parser is regex-based, not a full AST parser. Coverage ~80% of common patterns.\n"
            "- Dataset metadata extracted from .meta.json fallback files (pyreadstat may not read all formats).\n"
            "- PROC LOGISTIC, REG, GLM, MIXED have no direct Snowflake SQL equivalent.\n"
            "- Hash objects and CALL EXECUTE require manual review.\n"
            "- Lineage is inferred from code; runtime dependencies may differ.\n"
            "- Validacao de integridade detecta padroes comuns de erro mas nao substitui revisao manual completa.\n"
        )

    def _section_next_steps(self) -> str:
        return (
            "## 8. Next Steps\n\n"
            "1. **URGENTE:** Resolver todos os achados CRITICOS da secao 6 antes de prosseguir.\n"
            "2. Review complexity analysis and prioritize programs for migration.\n"
            "3. Validate lineage graph with business stakeholders.\n"
            "4. Run MVP2 to generate Snowflake DDL, COPY INTO, and transpiled code.\n"
            "5. Enable LLM review for architecture recommendations.\n"
            "6. Set up staging environment and test data loads.\n"
            "7. Plan UAT with business users.\n"
        )
