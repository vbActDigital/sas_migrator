#!/usr/bin/env python3
"""
MVP1 Discovery -- Docker Runner with Mock SAS + Snowflake
Outputs: mvp1_outputs/ with MD reports, JSON artifacts, DDL/SQL samples.
Terminal: live spinners, progress bars, and colored status.
"""
import os
import sys
import io
import json
import time
import threading
import itertools
import shutil
from datetime import datetime

# Force UTF-8 stdout on Windows to support Unicode chars
if sys.platform == "win32":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.join(BASE_DIR, "..")
OUTPUT_DIR = os.path.join(PROJECT_ROOT, "mvp1_outputs")
MOCK_DIR = os.path.join(BASE_DIR, "mock_sas_environment")
CONFIG_PATH = os.path.join(BASE_DIR, "mock_config.yaml")

# --- ANSI Colors ---
RESET   = "\033[0m"
BOLD    = "\033[1m"
DIM     = "\033[2m"
GREEN   = "\033[92m"
YELLOW  = "\033[93m"
RED     = "\033[91m"
CYAN    = "\033[96m"
MAGENTA = "\033[95m"
WHITE   = "\033[97m"
BG_BLUE = "\033[44m"


# --- Spinner / Progress helpers ---
class Spinner:
    """Animated spinner that runs in a background thread."""
    FRAMES = ["|", "/", "-", "\\"]

    def __init__(self, message: str):
        self.message = message
        self._stop = threading.Event()
        self._thread = None

    def __enter__(self):
        self._thread = threading.Thread(target=self._spin, daemon=True)
        self._thread.start()
        return self

    def __exit__(self, *_):
        self._stop.set()
        self._thread.join()
        # Clear the spinner line
        sys.stdout.write(f"\r\033[K")
        sys.stdout.flush()

    def _spin(self):
        for frame in itertools.cycle(self.FRAMES):
            if self._stop.is_set():
                break
            sys.stdout.write(f"\r  {CYAN}{frame}{RESET} {self.message}")
            sys.stdout.flush()
            time.sleep(0.08)


def progress_bar(current, total, width=30, label=""):
    pct = current / total if total else 1
    filled = int(width * pct)
    bar = f"{'#' * filled}{'.' * (width - filled)}"
    sys.stdout.write(f"\r  {CYAN}[{bar}]{RESET} {current}/{total} {DIM}{label}{RESET}")
    sys.stdout.flush()
    if current == total:
        sys.stdout.write("\n")


def banner(text):
    w = max(len(text) + 6, 64)
    print(f"\n{CYAN}{'=' * w}{RESET}")
    print(f"{CYAN}|{RESET} {BOLD}{WHITE}{text}{RESET}{' ' * (w - len(text) - 3)}{CYAN}|{RESET}")
    print(f"{CYAN}{'=' * w}{RESET}")


def step_header(num, total, text):
    print(f"\n  {BG_BLUE}{WHITE}{BOLD} STEP {num}/{total} {RESET} {BOLD}{text}{RESET}")
    print(f"  {'-' * 56}")


def ok(text):
    print(f"  {GREEN}[OK]{RESET} {text}")


def warn(text):
    print(f"  {YELLOW}[!]{RESET} {text}")


def fail(text):
    print(f"  {RED}[FAIL]{RESET} {text}")


def info(text):
    print(f"  {DIM}|{RESET} {text}")


def table_row(cols, widths):
    parts = []
    for val, w in zip(cols, widths):
        parts.append(f"{str(val):<{w}}")
    print(f"  {DIM}|{RESET} {'  '.join(parts)}")


def table_sep(widths):
    parts = [f"{'-' * w}" for w in widths]
    print(f"  {DIM}|{RESET} {'--'.join(parts)}")


# ─── Environment ───────────────────────────────────────────
def ensure_mock():
    import subprocess
    if not os.path.exists(CONFIG_PATH):
        with Spinner("Generating mock SAS environment..."):
            subprocess.run(
                [sys.executable, os.path.join(BASE_DIR, "create_mock_environment.py")],
                check=True, capture_output=True,
            )
        ok("Mock SAS environment generated")


def load_config():
    import yaml
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


# ─── MVP1 Pipeline ─────────────────────────────────────────
def run_pipeline(config):
    TOTAL_STEPS = 8
    results = {}
    errors = []
    timings = {}

    # Prepare output dirs
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    os.makedirs(os.path.join(OUTPUT_DIR, "snowflake"), exist_ok=True)
    os.makedirs(os.path.join(OUTPUT_DIR, "analysis"), exist_ok=True)

    # ── STEP 1: Filesystem Scan ────────────────────────────
    step_header(1, TOTAL_STEPS, "Filesystem Scan")
    t0 = time.time()
    try:
        from src.connectors.sas.filesystem_scanner import SASFilesystemScanner

        with Spinner("Scanning SAS file system..."):
            scanner = SASFilesystemScanner(config)
            programs = scanner.scan_programs()
            datasets = scanner.scan_datasets()
            time.sleep(0.3)  # let spinner show

        ok(f"Found {GREEN}{len(programs)}{RESET} programs, {GREEN}{len(datasets)}{RESET} datasets")
        print()
        W = [35, 8, 6]
        table_row(["FILE", "SIZE", "LINES"], W)
        table_sep(W)
        for p in programs:
            table_row([p["filename"], f"{p['size_bytes']}B", p["line_count"]], W)
        print()
        for d in datasets:
            info(f"{d['filename']}  lib={CYAN}{d['inferred_library']}{RESET}")

        results["scan"] = {"programs": len(programs), "datasets": len(datasets)}
        timings["scan"] = time.time() - t0
    except Exception as e:
        fail(f"Filesystem scan: {e}")
        errors.append(("Step 1: Filesystem Scan", str(e)))
        programs, datasets = [], []

    # ── STEP 2: Code Parsing ───────────────────────────────
    step_header(2, TOTAL_STEPS, "SAS Code Parsing & Complexity")
    t0 = time.time()
    parsed_programs = []
    try:
        from src.parsers.sas.sas_code_parser import SASCodeParser

        parser = SASCodeParser()
        for i, prog in enumerate(programs):
            progress_bar(i + 1, len(programs), label=prog["filename"])
            parsed = parser.parse_file(prog["absolute_path"])
            parsed_programs.append(parsed)

        print()
        W = [30, 6, 6, 10, 30]
        table_row(["PROGRAM", "LINES", "SCORE", "LEVEL", "PROCs"], W)
        table_sep(W)
        for p in sorted(parsed_programs, key=lambda x: x["complexity_score"], reverse=True):
            procs = ", ".join(p["procs_used"][:5])
            table_row([p["filename"], p["line_count"], p["complexity_score"], p["complexity_level"], procs], W)

        dist = {}
        for p in parsed_programs:
            dist[p["complexity_level"]] = dist.get(p["complexity_level"], 0) + 1

        print()
        ok(f"Complexity: {' | '.join(f'{k}={v}' for k, v in sorted(dist.items()))}")

        hash_progs = [p["filename"] for p in parsed_programs if p["has_hash_objects"]]
        dyn_progs = [p["filename"] for p in parsed_programs if p["has_dynamic_sql"]]
        if hash_progs:
            warn(f"Hash objects: {', '.join(hash_progs)}")
        if dyn_progs:
            warn(f"Dynamic SQL:  {', '.join(dyn_progs)}")

        results["parsing"] = {"programs_parsed": len(parsed_programs), "complexity": dist}
        timings["parsing"] = time.time() - t0
    except Exception as e:
        fail(f"Code parsing: {e}")
        errors.append(("Step 2: Code Parsing", str(e)))

    # ── STEP 3: Dataset Metadata ───────────────────────────
    step_header(3, TOTAL_STEPS, "Dataset Metadata Extraction")
    t0 = time.time()
    datasets_metadata = []
    try:
        data_dir = config["sas_environment"]["data_paths"][0]

        with Spinner("Reading .meta.json files..."):
            for fname in sorted(os.listdir(data_dir)):
                if fname.endswith(".meta.json"):
                    with open(os.path.join(data_dir, fname), "r", encoding="utf-8") as f:
                        meta = json.load(f)
                    datasets_metadata.append(meta)
            time.sleep(0.2)

        pii_keywords = {
            "cpf", "email", "phone", "telefone", "salary", "salario",
            "ssn", "rg", "nome", "name", "address", "endereco",
        }
        print()
        W = [18, 6, 4, 10, 30]
        table_row(["DATASET", "ROWS", "COLS", "SIZE", "PII COLUMNS"], W)
        table_sep(W)
        for ds in datasets_metadata:
            pii = [c["name"] for c in ds.get("columns", []) if c["name"].lower() in pii_keywords]
            size_kb = f"{ds.get('size_bytes', 0) / 1024:.1f}KB"
            pii_str = f"{RED}{', '.join(pii)}{RESET}" if pii else f"{GREEN}-{RESET}"
            table_row([ds["dataset_name"], ds["row_count"], ds["column_count"], size_kb, pii_str], W)

        total_rows = sum(d["row_count"] for d in datasets_metadata)
        ok(f"{len(datasets_metadata)} datasets, {total_rows:,} total rows")
        results["metadata"] = {"datasets": len(datasets_metadata), "total_rows": total_rows}
        timings["metadata"] = time.time() - t0
    except Exception as e:
        fail(f"Metadata extraction: {e}")
        errors.append(("Step 3: Metadata", str(e)))

    # ── STEP 4: Lineage Graph ──────────────────────────────
    step_header(4, TOTAL_STEPS, "Lineage Graph Construction")
    t0 = time.time()
    lineage = {"nodes": [], "edges": []}
    try:
        from src.parsers.sas.lineage_builder import LineageBuilder

        with Spinner("Building dependency graph..."):
            builder = LineageBuilder()
            lineage = builder.build_from_parsed_programs(parsed_programs)
            time.sleep(0.2)

        nodes = lineage["nodes"]
        edges = lineage["edges"]
        types = {}
        for n in nodes:
            types[n["type"]] = types.get(n["type"], 0) + 1

        targets = {e["target"] for e in edges}
        sources = {e["source"] for e in edges}
        all_ids = {n["id"] for n in nodes}
        roots = all_ids - targets
        leaves = all_ids - sources

        ok(f"Nodes: {CYAN}{len(nodes)}{RESET}  Edges: {CYAN}{len(edges)}{RESET}")
        info(f"Types: {types}")
        info(f"Roots (sources): {len(roots)}  |  Leaves (sinks): {len(leaves)}")

        for ds_node in [n for n in nodes if n["type"] == "dataset"][:3]:
            up = builder.get_upstream(ds_node["id"])
            down = builder.get_downstream(ds_node["id"])
            info(f"{ds_node['label']}: {len(up)} upstream -> {len(down)} downstream")

        results["lineage"] = {"nodes": len(nodes), "edges": len(edges), "roots": len(roots), "leaves": len(leaves)}
        timings["lineage"] = time.time() - t0
    except Exception as e:
        fail(f"Lineage: {e}")
        errors.append(("Step 4: Lineage", str(e)))

    # ── STEP 5: Data Catalog ───────────────────────────────
    step_header(5, TOTAL_STEPS, "Data Catalog (PII + Sensitivity)")
    t0 = time.time()
    catalog = {}
    try:
        from src.catalog.catalog_generator import DataCatalogGenerator

        with Spinner("Generating data catalog..."):
            catalog_gen = DataCatalogGenerator(config=config, llm_advisor=None)
            catalog = catalog_gen.generate_catalog(
                datasets_metadata, parsed_programs, lineage, enrich_with_llm=False,
            )
            time.sleep(0.2)

        summary = catalog["summary"]
        ok(f"Datasets: {summary['total_datasets']}  Columns: {summary['total_columns']}")
        ok(f"PII detected: {RED}{summary['datasets_with_pii']}{RESET} dataset(s)")
        info(f"Sensitivity: {summary['sensitivity_distribution']}")
        info(f"Domains: {summary['domains']}")

        print()
        W = [18, 12, 12, 25]
        table_row(["DATASET", "DOMAIN", "SENSITIVITY", "PII"], W)
        table_sep(W)
        for ds in catalog["datasets"]:
            pii = ", ".join(ds.get("pii_columns", [])) or "-"
            sens = ds.get("sensitivity", "")
            sens_col = RED if sens == "high" else (YELLOW if sens == "medium" else GREEN)
            table_row([ds["dataset_name"], ds["domain"], f"{sens_col}{sens}{RESET}", pii], W)

        catalog_path = os.path.join(OUTPUT_DIR, "analysis", "data_catalog.json")
        with open(catalog_path, "w", encoding="utf-8") as f:
            json.dump(catalog, f, indent=2, default=str)

        results["catalog"] = summary
        timings["catalog"] = time.time() - t0
    except Exception as e:
        fail(f"Catalog: {e}")
        errors.append(("Step 5: Catalog", str(e)))

    # ── STEP 6: Discovery Report (MD) ──────────────────────
    step_header(6, TOTAL_STEPS, "Discovery Report (Markdown)")
    t0 = time.time()
    try:
        from src.reporting.report_generator import ReportGenerator

        with Spinner("Writing discovery report..."):
            report_gen = ReportGenerator(config)
            report_path = report_gen.generate_discovery_report(
                parsed_programs, datasets_metadata, lineage, output_dir=OUTPUT_DIR,
            )
            time.sleep(0.2)

        with open(report_path, "r", encoding="utf-8") as f:
            report_content = f.read()

        expected = [
            "1. Executive Summary", "2. Program Inventory",
            "3. Dataset Inventory", "4. Dependencies and Lineage",
            "5. Complexity Analysis", "6. Limitations", "7. Next Steps",
        ]
        found = [s for s in expected if s in report_content]
        ok(f"Report: {os.path.basename(report_path)} ({len(report_content):,} chars)")
        ok(f"Sections: {GREEN}{len(found)}/{len(expected)}{RESET}")
        results["report"] = report_path
        timings["report"] = time.time() - t0
    except Exception as e:
        fail(f"Report: {e}")
        errors.append(("Step 6: Report", str(e)))

    # ── STEP 7: Snowflake DDL + COPY INTO ──────────────────
    step_header(7, TOTAL_STEPS, "Snowflake Mock -- DDL & Data Load")
    t0 = time.time()
    try:
        from src.migration.data_migrator.snowflake_migrator import SnowflakeMigrator

        sf_dir = os.path.join(OUTPUT_DIR, "snowflake")
        migrator = SnowflakeMigrator(config)

        for i, ds in enumerate(datasets_metadata):
            progress_bar(i + 1, len(datasets_metadata), label=ds["dataset_name"])

            # DDL
            ddl = migrator.generate_ddl(ds)
            ddl_path = os.path.join(sf_dir, f"ddl_{ds['dataset_name']}.sql")
            with open(ddl_path, "w", encoding="utf-8") as f:
                f.write(f"-- Auto-generated DDL for {ds['dataset_name']}\n")
                f.write(f"-- Source: SAS .sas7bdat ({ds['row_count']} rows, {ds['column_count']} cols)\n\n")
                f.write(ddl)

            # COPY INTO
            copy_sql = migrator.generate_copy_into(ds)
            pipe_sql = migrator.generate_snowpipe(ds)
            load_path = os.path.join(sf_dir, f"load_{ds['dataset_name']}.sql")
            with open(load_path, "w", encoding="utf-8") as f:
                f.write(f"-- Data load for {ds['dataset_name']}\n\n")
                f.write(f"-- COPY INTO\n{copy_sql}\n\n")
                f.write(f"-- Snowpipe (auto-ingest)\n{pipe_sql}\n")

        print()
        # Migration plan
        plan = migrator.generate_migration_plan(datasets_metadata)
        plan_path = os.path.join(sf_dir, "migration_plan.json")
        with open(plan_path, "w", encoding="utf-8") as f:
            json.dump(plan, f, indent=2, default=str)

        ok(f"DDL files: {len(datasets_metadata)}")
        ok(f"Load scripts: {len(datasets_metadata)}")
        ok(f"Migration plan: migration_plan.json")
        info(f"Database: {plan['database']}  |  Datasets: {plan['total_datasets']}")
        results["snowflake_ddl"] = {"files": len(datasets_metadata) * 2}
        timings["snowflake_ddl"] = time.time() - t0
    except Exception as e:
        fail(f"Snowflake DDL: {e}")
        errors.append(("Step 7: Snowflake DDL", str(e)))

    # ── STEP 8: Code Transpilation (SAS -> Snowflake SQL) ───
    step_header(8, TOTAL_STEPS, "Snowflake Mock -- Code Transpilation")
    t0 = time.time()
    try:
        from src.migration.code_transpiler.snowflake_transpiler import SnowflakeTranspiler

        transpiler = SnowflakeTranspiler(config)
        all_gaps = []
        total_coverage = []
        transpile_dir = os.path.join(OUTPUT_DIR, "snowflake", "transpiled")
        os.makedirs(transpile_dir, exist_ok=True)

        for i, prog in enumerate(parsed_programs):
            progress_bar(i + 1, len(parsed_programs), label=prog["filename"])
            result = transpiler.transpile(prog)

            sql_path = os.path.join(transpile_dir, f"{prog['filename'].replace('.sas', '')}.sql")
            with open(sql_path, "w", encoding="utf-8") as f:
                f.write(f"-- Transpiled from: {prog['filename']}\n")
                f.write(f"-- Complexity: {prog['complexity_level']} (score {prog['complexity_score']})\n")
                f.write(f"-- Coverage: {result['coverage_pct']}%\n")
                f.write(f"-- Gaps: {len(result['gaps'])}\n\n")
                f.write(result["sql_code"])

            if result.get("snowpark_code") and result["snowpark_code"] != "# No Snowpark code generated":
                py_path = os.path.join(transpile_dir, f"{prog['filename'].replace('.sas', '')}_snowpark.py")
                with open(py_path, "w", encoding="utf-8") as f:
                    f.write(f"# Snowpark from: {prog['filename']}\n\n")
                    f.write(result["snowpark_code"])

            total_coverage.append(result["coverage_pct"])
            all_gaps.extend(result["gaps"])

        print()
        avg_coverage = sum(total_coverage) / len(total_coverage) if total_coverage else 0
        unique_gaps = list(set(all_gaps))

        W = [30, 10]
        table_row(["PROGRAM", "COVERAGE"], W)
        table_sep(W)
        for prog, cov in zip(parsed_programs, total_coverage):
            cov_color = GREEN if cov >= 80 else (YELLOW if cov >= 50 else RED)
            table_row([prog["filename"], f"{cov_color}{cov:.0f}%{RESET}"], W)

        print()
        ok(f"Average coverage: {BOLD}{avg_coverage:.1f}%{RESET}")
        if unique_gaps:
            warn(f"Gaps ({len(unique_gaps)}):")
            for g in unique_gaps:
                info(f"  -> {g}")

        # Gap report
        gap_report = {
            "timestamp": datetime.now().isoformat(),
            "total_programs": len(parsed_programs),
            "avg_coverage": avg_coverage,
            "gaps": unique_gaps,
        }
        gap_path = os.path.join(OUTPUT_DIR, "analysis", "gap_report.json")
        with open(gap_path, "w", encoding="utf-8") as f:
            json.dump(gap_report, f, indent=2)

        results["transpilation"] = {"avg_coverage": avg_coverage, "total_gaps": len(all_gaps), "unique_gaps": unique_gaps}
        timings["transpilation"] = time.time() - t0
    except Exception as e:
        fail(f"Transpilation: {e}")
        errors.append(("Step 8: Transpilation", str(e)))

    # ── Save inventory ─────────────────────────────────────
    inventory = {
        "programs": [{
            "filename": p["filename"],
            "filepath": p["filepath"],
            "complexity_score": p["complexity_score"],
            "complexity_level": p["complexity_level"],
            "procs_used": p["procs_used"],
            "libnames": p["libnames"],
            "data_steps": p["data_steps"],
            "datasets_read": p["datasets_read"],
            "datasets_written": p["datasets_written"],
            "macro_definitions": p["macro_definitions"],
            "macro_calls": p["macro_calls"],
            "merge_statements": p["merge_statements"],
            "includes": p["includes"],
            "has_hash_objects": p["has_hash_objects"],
            "has_dynamic_sql": p["has_dynamic_sql"],
        } for p in parsed_programs],
        "datasets": datasets_metadata,
        "lineage": lineage,
    }
    inv_path = os.path.join(OUTPUT_DIR, "analysis", "inventory.json")
    with open(inv_path, "w", encoding="utf-8") as f:
        json.dump(inventory, f, indent=2, default=str)

    return results, errors, timings


# ─── Summary MD ────────────────────────────────────────────
def write_summary_md(results, errors, timings, total_elapsed):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    scan = results.get("scan", {})
    parsing = results.get("parsing", {})
    lineage = results.get("lineage", {})
    catalog = results.get("catalog", {})
    transp = results.get("transpilation", {})

    lines = [
        "# MVP1 Discovery -- Analysis Report",
        "",
        f"> **Generated:** {ts}",
        f"> **Total time:** {total_elapsed:.1f}s",
        f"> **Status:** {'ALL PASSED' if not errors else f'PARTIAL -- {len(errors)} error(s)'}",
        f"> **Mode:** Docker + Mock Data (SAS + Snowflake)",
        "",
        "---",
        "",
        "## 1. SAS Environment Scan",
        "",
        f"| Metric | Value |",
        f"|--------|-------|",
        f"| SAS Programs | {scan.get('programs', 0)} |",
        f"| SAS Datasets | {scan.get('datasets', 0)} |",
        f"| Scan time | {timings.get('scan', 0):.2f}s |",
        "",
        "## 2. Code Complexity",
        "",
        "| Level | Count |",
        "|-------|-------|",
    ]
    for level, count in sorted(parsing.get("complexity", {}).items()):
        lines.append(f"| {level} | {count} |")

    lines += [
        "",
        f"Programs parsed: {parsing.get('programs_parsed', 0)}",
        "",
        "## 3. Lineage Graph",
        "",
        "| Metric | Value |",
        "|--------|-------|",
        f"| Nodes | {lineage.get('nodes', 0)} |",
        f"| Edges | {lineage.get('edges', 0)} |",
        f"| Root nodes (sources) | {lineage.get('roots', 0)} |",
        f"| Leaf nodes (sinks) | {lineage.get('leaves', 0)} |",
        "",
        "## 4. Data Catalog & Sensitivity",
        "",
        "| Metric | Value |",
        "|--------|-------|",
        f"| Total datasets | {catalog.get('total_datasets', 0)} |",
        f"| Total columns | {catalog.get('total_columns', 0)} |",
        f"| Datasets with PII | {catalog.get('datasets_with_pii', 0)} |",
        f"| Domains | {catalog.get('domains', {})} |",
        f"| Sensitivity | {catalog.get('sensitivity_distribution', {})} |",
        "",
        "## 5. Snowflake Migration Preview",
        "",
        "| Metric | Value |",
        "|--------|-------|",
        f"| Avg transpilation coverage | {transp.get('avg_coverage', 0):.1f}% |",
        f"| Total gaps | {transp.get('total_gaps', 0)} |",
        f"| Unique gap types | {len(transp.get('unique_gaps', []))} |",
        "",
    ]

    if transp.get("unique_gaps"):
        lines.append("### Gaps requiring manual review")
        lines.append("")
        for g in transp["unique_gaps"]:
            lines.append(f"- {g}")
        lines.append("")

    if errors:
        lines.append("## Errors")
        lines.append("")
        for step, err in errors:
            lines.append(f"- **{step}:** {err}")
        lines.append("")

    lines += [
        "---",
        "",
        "## Output Files",
        "",
        "```",
        "mvp1_outputs/",
        "  analysis/",
        "    data_catalog.json      # Full catalog with PII + sensitivity",
        "    inventory.json          # Programs + datasets inventory",
        "    gap_report.json         # Transpilation gaps",
        "  snowflake/",
        "    ddl_*.sql               # CREATE TABLE statements",
        "    load_*.sql              # COPY INTO + Snowpipe",
        "    migration_plan.json     # Migration strategy",
        "    transpiled/",
        "      *.sql                 # Transpiled Snowflake SQL",
        "      *_snowpark.py         # Snowpark Python stubs",
        "  discovery_report.md       # Full 7-section report",
        "  analysis_summary.md       # This file",
        "  results.json              # Raw results",
        "```",
    ]

    md_path = os.path.join(OUTPUT_DIR, "analysis_summary.md")
    with open(md_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")
    return md_path


# ─── MAIN ──────────────────────────────────────────────────
def main():
    start = time.time()

    print()
    print(f"  {MAGENTA}+----------------------------------------------------------+{RESET}")
    print(f"  {MAGENTA}|{RESET}  {BOLD}SAS -> Snowflake Migration Toolkit{RESET}                       {MAGENTA}|{RESET}")
    print(f"  {MAGENTA}|{RESET}  {DIM}MVP1 Discovery - Docker - Mock Data{RESET}                     {MAGENTA}|{RESET}")
    print(f"  {MAGENTA}+----------------------------------------------------------+{RESET}")
    print()
    info(f"Timestamp:  {datetime.now().isoformat()}")
    info(f"Python:     {sys.version.split()[0]}")
    info(f"Output:     {OUTPUT_DIR}")
    print()

    # Setup
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    ensure_mock()
    config = load_config()

    # Run pipeline
    results, errors, timings = run_pipeline(config)

    total_elapsed = time.time() - start

    # Write summary MD
    banner("Generating Summary")
    md_path = write_summary_md(results, errors, timings, total_elapsed)
    ok(f"analysis_summary.md")

    # Save raw results
    full = {
        "timestamp": datetime.now().isoformat(),
        "elapsed_seconds": total_elapsed,
        "timings": timings,
        "mvp1": results,
        "errors": [{"step": s, "error": e} for s, e in errors],
    }
    results_path = os.path.join(OUTPUT_DIR, "results.json")
    with open(results_path, "w", encoding="utf-8") as f:
        json.dump(full, f, indent=2, default=str)

    # ── Final Status ───────────────────────────────────────
    print()
    print(f"  {MAGENTA}+----------------------------------------------------------+{RESET}")
    status_icon = GREEN + "[OK] ALL PASSED" if not errors else RED + f"[!] {len(errors)} ERROR(S)"
    print(f"  {MAGENTA}|{RESET}  Status:  {BOLD}{status_icon}{RESET}")
    print(f"  {MAGENTA}|{RESET}  Time:    {BOLD}{total_elapsed:.1f}s{RESET}")
    print(f"  {MAGENTA}+----------------------------------------------------------+{RESET}")
    print()
    info("Output files:")
    for root, _, files in os.walk(OUTPUT_DIR):
        for fname in sorted(files):
            fpath = os.path.join(root, fname)
            size = os.path.getsize(fpath)
            rel = os.path.relpath(fpath, OUTPUT_DIR).replace("\\", "/")
            size_str = f"{size:,}B" if size < 1024 else f"{size/1024:.1f}KB"
            print(f"  {DIM}|{RESET}   {rel:<45} {DIM}{size_str}{RESET}")

    print()
    sys.exit(0 if not errors else 1)


if __name__ == "__main__":
    main()
