#!/usr/bin/env python3
"""
MVP1 Discovery + MVP2 Migration -- Full Report Runner
Outputs: mvp1_mvp2_outputs/ with MD reports, JSON artifacts, DDL/SQL, validation scripts.
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

# Force UTF-8 stdout on Windows
if sys.platform == "win32":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.join(BASE_DIR, "..")
OUTPUT_DIR = os.path.join(PROJECT_ROOT, "mvp1_mvp2_outputs")
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
BG_MAG  = "\033[45m"


# --- Spinner / Progress helpers ---
class Spinner:
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


def banner(text, color=CYAN):
    w = max(len(text) + 6, 64)
    print(f"\n{color}{'=' * w}{RESET}")
    print(f"{color}|{RESET} {BOLD}{WHITE}{text}{RESET}{' ' * (w - len(text) - 3)}{color}|{RESET}")
    print(f"{color}{'=' * w}{RESET}")


def step_header(num, total, text, phase_color=BG_BLUE):
    print(f"\n  {phase_color}{WHITE}{BOLD} STEP {num}/{total} {RESET} {BOLD}{text}{RESET}")
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


# --- Environment ---
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


# =====================================================================
#  MVP1 -- DISCOVERY PIPELINE (Steps 1-8)
# =====================================================================
def run_mvp1(config):
    TOTAL = 8
    results = {}
    errors = []
    timings = {}

    os.makedirs(os.path.join(OUTPUT_DIR, "mvp1", "snowflake", "transpiled"), exist_ok=True)
    os.makedirs(os.path.join(OUTPUT_DIR, "mvp1", "analysis"), exist_ok=True)

    mvp1_dir = os.path.join(OUTPUT_DIR, "mvp1")

    # -- STEP 1: Filesystem Scan --
    step_header(1, TOTAL, "Filesystem Scan", BG_BLUE)
    t0 = time.time()
    programs, datasets = [], []
    try:
        from src.connectors.sas.filesystem_scanner import SASFilesystemScanner
        with Spinner("Scanning SAS file system..."):
            scanner = SASFilesystemScanner(config)
            programs = scanner.scan_programs()
            datasets = scanner.scan_datasets()
            time.sleep(0.2)

        ok(f"Found {GREEN}{len(programs)}{RESET} programs, {GREEN}{len(datasets)}{RESET} datasets")
        print()
        W = [35, 8, 6]
        table_row(["FILE", "SIZE", "LINES"], W)
        table_sep(W)
        for p in programs:
            table_row([p["filename"], f"{p['size_bytes']}B", p["line_count"]], W)
        results["scan"] = {"programs": len(programs), "datasets": len(datasets)}
        timings["scan"] = time.time() - t0
    except Exception as e:
        fail(f"Filesystem scan: {e}")
        errors.append(("Step 1: Filesystem Scan", str(e)))

    # -- STEP 2: Code Parsing --
    step_header(2, TOTAL, "SAS Code Parsing & Complexity", BG_BLUE)
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

    # -- STEP 3: Dataset Metadata --
    step_header(3, TOTAL, "Dataset Metadata Extraction", BG_BLUE)
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

        pii_keywords = {"cpf", "email", "phone", "telefone", "salary", "salario",
                        "ssn", "rg", "nome", "name", "address", "endereco"}
        print()
        W = [22, 8, 4, 10, 30]
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

    # -- STEP 4: Lineage Graph --
    step_header(4, TOTAL, "Lineage Graph Construction", BG_BLUE)
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

        results["lineage"] = {"nodes": len(nodes), "edges": len(edges), "roots": len(roots), "leaves": len(leaves)}
        timings["lineage"] = time.time() - t0
    except Exception as e:
        fail(f"Lineage: {e}")
        errors.append(("Step 4: Lineage", str(e)))

    # -- STEP 5: Data Catalog --
    step_header(5, TOTAL, "Data Catalog (PII + Sensitivity)", BG_BLUE)
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

        catalog_path = os.path.join(mvp1_dir, "analysis", "data_catalog.json")
        with open(catalog_path, "w", encoding="utf-8") as f:
            json.dump(catalog, f, indent=2, default=str)

        results["catalog"] = summary
        timings["catalog"] = time.time() - t0
    except Exception as e:
        fail(f"Catalog: {e}")
        errors.append(("Step 5: Catalog", str(e)))

    # -- STEP 6: Discovery Report --
    step_header(6, TOTAL, "Discovery Report (Markdown)", BG_BLUE)
    t0 = time.time()
    try:
        from src.reporting.report_generator import ReportGenerator
        with Spinner("Writing discovery report..."):
            report_gen = ReportGenerator(config)
            report_path = report_gen.generate_discovery_report(
                parsed_programs, datasets_metadata, lineage, output_dir=mvp1_dir,
            )
            time.sleep(0.2)

        with open(report_path, "r", encoding="utf-8") as f:
            report_content = f.read()

        expected = ["1. Executive Summary", "2. Program Inventory",
                    "3. Dataset Inventory", "4. Dependencies and Lineage",
                    "5. Complexity Analysis", "6. Limitations", "7. Next Steps"]
        found = [s for s in expected if s in report_content]
        ok(f"Report: {os.path.basename(report_path)} ({len(report_content):,} chars)")
        ok(f"Sections: {GREEN}{len(found)}/{len(expected)}{RESET}")
        results["report"] = report_path
        timings["report"] = time.time() - t0
    except Exception as e:
        fail(f"Report: {e}")
        errors.append(("Step 6: Report", str(e)))

    # -- STEP 7: Snowflake DDL + COPY INTO --
    step_header(7, TOTAL, "Snowflake DDL & Data Load Scripts", BG_BLUE)
    t0 = time.time()
    try:
        from src.migration.data_migrator.snowflake_migrator import SnowflakeMigrator
        sf_dir = os.path.join(mvp1_dir, "snowflake")
        migrator = SnowflakeMigrator(config)

        for i, ds in enumerate(datasets_metadata):
            progress_bar(i + 1, len(datasets_metadata), label=ds["dataset_name"])
            ddl = migrator.generate_ddl(ds)
            ddl_path = os.path.join(sf_dir, f"ddl_{ds['dataset_name']}.sql")
            with open(ddl_path, "w", encoding="utf-8") as f:
                f.write(f"-- DDL for {ds['dataset_name']}\n")
                f.write(f"-- Source: SAS ({ds['row_count']} rows, {ds['column_count']} cols)\n\n")
                f.write(ddl)

            copy_sql = migrator.generate_copy_into(ds)
            pipe_sql = migrator.generate_snowpipe(ds)
            load_path = os.path.join(sf_dir, f"load_{ds['dataset_name']}.sql")
            with open(load_path, "w", encoding="utf-8") as f:
                f.write(f"-- Data load for {ds['dataset_name']}\n\n")
                f.write(f"-- COPY INTO\n{copy_sql}\n\n")
                f.write(f"-- Snowpipe\n{pipe_sql}\n")

        print()
        plan = migrator.generate_migration_plan(datasets_metadata)
        plan_path = os.path.join(sf_dir, "migration_plan.json")
        with open(plan_path, "w", encoding="utf-8") as f:
            json.dump(plan, f, indent=2, default=str)

        ok(f"DDL files: {len(datasets_metadata)}")
        ok(f"Load scripts: {len(datasets_metadata)}")
        ok(f"Migration plan: migration_plan.json")
        results["snowflake_ddl"] = {"files": len(datasets_metadata) * 2}
        timings["snowflake_ddl"] = time.time() - t0
    except Exception as e:
        fail(f"Snowflake DDL: {e}")
        errors.append(("Step 7: Snowflake DDL", str(e)))

    # -- STEP 8: Code Transpilation --
    step_header(8, TOTAL, "Code Transpilation (SAS -> Snowflake SQL)", BG_BLUE)
    t0 = time.time()
    transpile_results = []
    try:
        from src.migration.code_transpiler.snowflake_transpiler import SnowflakeTranspiler
        transpiler = SnowflakeTranspiler(config)
        all_gaps = []
        total_coverage = []
        transpile_dir = os.path.join(mvp1_dir, "snowflake", "transpiled")

        for i, prog in enumerate(parsed_programs):
            progress_bar(i + 1, len(parsed_programs), label=prog["filename"])
            result = transpiler.transpile(prog)
            transpile_results.append(result)

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

        gap_report = {
            "timestamp": datetime.now().isoformat(),
            "total_programs": len(parsed_programs),
            "avg_coverage": avg_coverage,
            "gaps": unique_gaps,
        }
        gap_path = os.path.join(mvp1_dir, "analysis", "gap_report.json")
        with open(gap_path, "w", encoding="utf-8") as f:
            json.dump(gap_report, f, indent=2)

        results["transpilation"] = {"avg_coverage": avg_coverage, "total_gaps": len(all_gaps), "unique_gaps": unique_gaps}
        timings["transpilation"] = time.time() - t0
    except Exception as e:
        fail(f"Transpilation: {e}")
        errors.append(("Step 8: Transpilation", str(e)))

    # Save inventory
    inventory = {
        "programs": [{
            "filename": p["filename"], "filepath": p["filepath"],
            "complexity_score": p["complexity_score"], "complexity_level": p["complexity_level"],
            "procs_used": p["procs_used"], "libnames": p["libnames"],
            "data_steps": p["data_steps"], "datasets_read": p["datasets_read"],
            "datasets_written": p["datasets_written"], "macro_definitions": p["macro_definitions"],
            "macro_calls": p["macro_calls"], "merge_statements": p["merge_statements"],
            "includes": p["includes"], "has_hash_objects": p["has_hash_objects"],
            "has_dynamic_sql": p["has_dynamic_sql"],
        } for p in parsed_programs],
        "datasets": datasets_metadata,
        "lineage": lineage,
    }
    inv_path = os.path.join(mvp1_dir, "analysis", "inventory.json")
    with open(inv_path, "w", encoding="utf-8") as f:
        json.dump(inventory, f, indent=2, default=str)

    return results, errors, timings, parsed_programs, datasets_metadata, lineage, inventory


# =====================================================================
#  MVP2 -- MIGRATION PIPELINE (Steps 9-12)
# =====================================================================
def run_mvp2(config, parsed_programs, datasets_metadata, lineage, inventory):
    results = {}
    errors = []
    timings = {}
    TOTAL = 4
    step_offset = 8  # continues from MVP1

    mvp2_dir = os.path.join(OUTPUT_DIR, "mvp2")
    os.makedirs(os.path.join(mvp2_dir, "ddl"), exist_ok=True)
    os.makedirs(os.path.join(mvp2_dir, "data_load"), exist_ok=True)
    os.makedirs(os.path.join(mvp2_dir, "transpiled"), exist_ok=True)
    os.makedirs(os.path.join(mvp2_dir, "validation"), exist_ok=True)

    # -- STEP 9: Migration Plan & DDL --
    step_header(step_offset + 1, step_offset + TOTAL, "Migration Plan & Snowflake DDL", BG_MAG)
    t0 = time.time()
    try:
        from src.migration.data_migrator.snowflake_migrator import SnowflakeMigrator
        migrator = SnowflakeMigrator(config)

        with Spinner("Generating migration plan..."):
            plan = migrator.generate_migration_plan(datasets_metadata)
            time.sleep(0.2)

        plan_path = os.path.join(mvp2_dir, "migration_plan.json")
        with open(plan_path, "w", encoding="utf-8") as f:
            json.dump(plan, f, indent=2, default=str)

        for i, ds in enumerate(datasets_metadata):
            progress_bar(i + 1, len(datasets_metadata), label=ds["dataset_name"])
            ddl = migrator.generate_ddl(ds)
            ddl_path = os.path.join(mvp2_dir, "ddl", f"{ds['dataset_name']}.sql")
            with open(ddl_path, "w", encoding="utf-8") as f:
                f.write(f"-- Snowflake DDL for {ds['dataset_name']}\n")
                f.write(f"-- Rows: {ds['row_count']} | Cols: {ds['column_count']}\n\n")
                f.write(ddl)

            # COPY INTO + Snowpipe
            copy_sql = migrator.generate_copy_into(ds)
            pipe_sql = migrator.generate_snowpipe(ds)
            load_path = os.path.join(mvp2_dir, "data_load", f"{ds['dataset_name']}.sql")
            with open(load_path, "w", encoding="utf-8") as f:
                f.write(f"-- Data load scripts for {ds['dataset_name']}\n\n")
                f.write(f"-- 1. COPY INTO (batch)\n{copy_sql}\n\n")
                f.write(f"-- 2. Snowpipe (streaming)\n{pipe_sql}\n")

        print()
        ok(f"Migration plan: {plan['total_datasets']} datasets -> {plan['database']}")
        ok(f"DDL scripts: {len(datasets_metadata)}")
        ok(f"Data load scripts: {len(datasets_metadata)} (COPY INTO + Snowpipe)")

        results["migration_plan"] = {
            "database": plan["database"],
            "total_datasets": plan["total_datasets"],
            "ddl_files": len(datasets_metadata),
            "load_files": len(datasets_metadata),
        }
        timings["migration_plan"] = time.time() - t0
    except Exception as e:
        fail(f"Migration plan: {e}")
        errors.append(("Step 9: Migration Plan", str(e)))

    # -- STEP 10: Full Code Transpilation --
    step_header(step_offset + 2, step_offset + TOTAL, "Full Code Transpilation", BG_MAG)
    t0 = time.time()
    try:
        from src.migration.code_transpiler.snowflake_transpiler import SnowflakeTranspiler
        transpiler = SnowflakeTranspiler(config)

        all_gaps = []
        all_warnings = []
        coverage_list = []

        for i, prog in enumerate(parsed_programs):
            progress_bar(i + 1, len(parsed_programs), label=prog["filename"])
            result = transpiler.transpile(prog)

            # SQL output
            sql_path = os.path.join(mvp2_dir, "transpiled", f"{prog['filename'].replace('.sas', '')}.sql")
            with open(sql_path, "w", encoding="utf-8") as f:
                f.write(f"-- ============================================\n")
                f.write(f"-- Transpiled: {prog['filename']}\n")
                f.write(f"-- Source: {prog['complexity_level']} complexity (score {prog['complexity_score']})\n")
                f.write(f"-- Coverage: {result['coverage_pct']}%\n")
                f.write(f"-- Gaps: {len(result['gaps'])}\n")
                f.write(f"-- Warnings: {len(result['warnings'])}\n")
                f.write(f"-- ============================================\n\n")
                f.write(result["sql_code"])

            # Snowpark output
            if result.get("snowpark_code") and result["snowpark_code"] != "# No Snowpark code generated":
                py_path = os.path.join(mvp2_dir, "transpiled", f"{prog['filename'].replace('.sas', '')}_snowpark.py")
                with open(py_path, "w", encoding="utf-8") as f:
                    f.write(f"# Snowpark Python from: {prog['filename']}\n\n")
                    f.write(result["snowpark_code"])

            coverage_list.append({"program": prog["filename"], "coverage": result["coverage_pct"],
                                   "gaps": result["gaps"], "warnings": result["warnings"]})
            all_gaps.extend(result["gaps"])
            all_warnings.extend(result["warnings"])

        print()
        avg_cov = sum(c["coverage"] for c in coverage_list) / len(coverage_list) if coverage_list else 0
        unique_gaps = list(set(all_gaps))
        unique_warnings = list(set(all_warnings))

        W = [30, 10, 6, 6]
        table_row(["PROGRAM", "COVERAGE", "GAPS", "WARNS"], W)
        table_sep(W)
        for c in sorted(coverage_list, key=lambda x: x["coverage"]):
            cov_color = GREEN if c["coverage"] >= 80 else (YELLOW if c["coverage"] >= 50 else RED)
            table_row([c["program"], f"{cov_color}{c['coverage']:.0f}%{RESET}",
                        len(c["gaps"]), len(c["warnings"])], W)

        print()
        ok(f"Average coverage: {BOLD}{avg_cov:.1f}%{RESET}")
        ok(f"Total gaps: {len(unique_gaps)} unique types")
        if unique_gaps:
            for g in unique_gaps:
                info(f"  {RED}GAP{RESET} {g}")
        if unique_warnings:
            for w in unique_warnings[:5]:
                info(f"  {YELLOW}WARN{RESET} {w}")

        # Gap report
        gap_report = {
            "timestamp": datetime.now().isoformat(),
            "total_programs": len(parsed_programs),
            "avg_coverage": avg_cov,
            "per_program": coverage_list,
            "unique_gaps": unique_gaps,
            "unique_warnings": unique_warnings,
        }
        gap_path = os.path.join(mvp2_dir, "gap_report.json")
        with open(gap_path, "w", encoding="utf-8") as f:
            json.dump(gap_report, f, indent=2)

        results["transpilation"] = {
            "avg_coverage": avg_cov,
            "unique_gaps": len(unique_gaps),
            "unique_warnings": len(unique_warnings),
            "per_program": coverage_list,
        }
        timings["transpilation"] = time.time() - t0
    except Exception as e:
        fail(f"Transpilation: {e}")
        errors.append(("Step 10: Transpilation", str(e)))

    # -- STEP 11: Validation Scripts --
    step_header(step_offset + 3, step_offset + TOTAL, "Post-Migration Validation Scripts", BG_MAG)
    t0 = time.time()
    try:
        from src.validation.validator import MigrationValidator
        validator = MigrationValidator()
        target_config = {
            "database": config.get("target", {}).get("database", "SAS_MIGRATION"),
            "schema": "RAW",
        }

        for i, ds in enumerate(datasets_metadata):
            progress_bar(i + 1, len(datasets_metadata), label=ds["dataset_name"])
            scripts = validator.generate_validation_scripts(ds, target_config)
            val_path = os.path.join(mvp2_dir, "validation", f"validate_{ds['dataset_name']}.sql")
            with open(val_path, "w", encoding="utf-8") as f:
                f.write(f"-- Post-migration validation for {ds['dataset_name']}\n")
                f.write(f"-- Expected rows: {ds['row_count']}\n\n")
                for key, sql in scripts.items():
                    if key not in ("dataset", "target_table"):
                        f.write(f"\n-- {key.upper()}\n{sql}\n")

        print()
        ok(f"Validation scripts: {len(datasets_metadata)}")
        info(f"Checks per dataset: row_count, schema_match, column_stats, checksum")

        results["validation"] = {"scripts": len(datasets_metadata), "checks_per_dataset": 4}
        timings["validation"] = time.time() - t0
    except Exception as e:
        fail(f"Validation: {e}")
        errors.append(("Step 11: Validation", str(e)))

    # -- STEP 12: Migration Summary --
    step_header(step_offset + 4, step_offset + TOTAL, "Migration Summary Report", BG_MAG)
    t0 = time.time()
    try:
        migration_summary = {
            "timestamp": datetime.now().isoformat(),
            "source": "SAS Environment (Mock)",
            "target": "Snowflake",
            "mvp2_results": results,
            "total_programs": len(parsed_programs),
            "total_datasets": len(datasets_metadata),
            "total_rows": sum(d["row_count"] for d in datasets_metadata),
        }
        summary_path = os.path.join(mvp2_dir, "migration_summary.json")
        with open(summary_path, "w", encoding="utf-8") as f:
            json.dump(migration_summary, f, indent=2, default=str)

        ok(f"Migration summary exported")
        results["summary"] = summary_path
        timings["summary"] = time.time() - t0
    except Exception as e:
        fail(f"Summary: {e}")
        errors.append(("Step 12: Summary", str(e)))

    return results, errors, timings


# =====================================================================
#  FULL REPORT GENERATION (MD)
# =====================================================================
def write_full_report(mvp1_results, mvp1_errors, mvp1_timings,
                      mvp2_results, mvp2_errors, mvp2_timings,
                      total_elapsed):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    all_errors = mvp1_errors + mvp2_errors
    scan = mvp1_results.get("scan", {})
    parsing = mvp1_results.get("parsing", {})
    lineage_r = mvp1_results.get("lineage", {})
    catalog_r = mvp1_results.get("catalog", {})
    mvp1_transp = mvp1_results.get("transpilation", {})
    mvp2_plan = mvp2_results.get("migration_plan", {})
    mvp2_transp = mvp2_results.get("transpilation", {})
    mvp2_val = mvp2_results.get("validation", {})

    lines = [
        "# SAS -> Snowflake Migration Report",
        "## MVP1 Discovery + MVP2 Migration",
        "",
        f"> **Generated:** {ts}",
        f"> **Total time:** {total_elapsed:.1f}s",
        f"> **Status:** {'ALL 12 STEPS PASSED' if not all_errors else f'PARTIAL -- {len(all_errors)} error(s)'}",
        f"> **Mode:** Mock Data (SAS + Snowflake)",
        "",
        "---",
        "",
        "# PART 1: MVP1 -- DISCOVERY",
        "",
        "## 1.1 SAS Environment Scan",
        "",
        "| Metric | Value |",
        "|--------|-------|",
        f"| SAS Programs | {scan.get('programs', 0)} |",
        f"| SAS Datasets | {scan.get('datasets', 0)} |",
        f"| Scan time | {mvp1_timings.get('scan', 0):.2f}s |",
        "",
        "## 1.2 Code Complexity Analysis",
        "",
        "| Level | Count |",
        "|-------|-------|",
    ]
    for level, count in sorted(parsing.get("complexity", {}).items()):
        lines.append(f"| {level} | {count} |")

    lines += [
        "",
        f"Total programs parsed: {parsing.get('programs_parsed', 0)}",
        "",
        "## 1.3 Lineage Graph",
        "",
        "| Metric | Value |",
        "|--------|-------|",
        f"| Nodes | {lineage_r.get('nodes', 0)} |",
        f"| Edges | {lineage_r.get('edges', 0)} |",
        f"| Root nodes (sources) | {lineage_r.get('roots', 0)} |",
        f"| Leaf nodes (sinks) | {lineage_r.get('leaves', 0)} |",
        "",
        "## 1.4 Data Catalog & Sensitivity",
        "",
        "| Metric | Value |",
        "|--------|-------|",
        f"| Total datasets | {catalog_r.get('total_datasets', 0)} |",
        f"| Total columns | {catalog_r.get('total_columns', 0)} |",
        f"| Datasets with PII | {catalog_r.get('datasets_with_pii', 0)} |",
        f"| Domains | {catalog_r.get('domains', {})} |",
        f"| Sensitivity | {catalog_r.get('sensitivity_distribution', {})} |",
        "",
        "## 1.5 Initial Transpilation Preview",
        "",
        "| Metric | Value |",
        "|--------|-------|",
        f"| Avg transpilation coverage | {mvp1_transp.get('avg_coverage', 0):.1f}% |",
        f"| Total gaps | {mvp1_transp.get('total_gaps', 0)} |",
        f"| Unique gap types | {len(mvp1_transp.get('unique_gaps', []))} |",
        "",
    ]

    if mvp1_transp.get("unique_gaps"):
        lines.append("### Gaps identified")
        lines.append("")
        for g in mvp1_transp["unique_gaps"]:
            lines.append(f"- {g}")
        lines.append("")

    lines += [
        "---",
        "",
        "# PART 2: MVP2 -- MIGRATION",
        "",
        "## 2.1 Migration Plan",
        "",
        "| Metric | Value |",
        "|--------|-------|",
        f"| Target database | {mvp2_plan.get('database', 'N/A')} |",
        f"| Total datasets | {mvp2_plan.get('total_datasets', 0)} |",
        f"| DDL scripts | {mvp2_plan.get('ddl_files', 0)} |",
        f"| Data load scripts | {mvp2_plan.get('load_files', 0)} |",
        "",
        "## 2.2 Code Transpilation (Full)",
        "",
        "| Metric | Value |",
        "|--------|-------|",
        f"| Avg coverage | {mvp2_transp.get('avg_coverage', 0):.1f}% |",
        f"| Unique gaps | {mvp2_transp.get('unique_gaps', 0)} |",
        f"| Unique warnings | {mvp2_transp.get('unique_warnings', 0)} |",
        "",
    ]

    per_prog = mvp2_transp.get("per_program", [])
    if per_prog:
        lines.append("### Per-Program Coverage")
        lines.append("")
        lines.append("| Program | Coverage | Gaps | Warnings |")
        lines.append("|---------|----------|------|----------|")
        for p in sorted(per_prog, key=lambda x: x["coverage"]):
            lines.append(f"| {p['program']} | {p['coverage']:.0f}% | {len(p['gaps'])} | {len(p['warnings'])} |")
        lines.append("")

    lines += [
        "## 2.3 Post-Migration Validation",
        "",
        "| Metric | Value |",
        "|--------|-------|",
        f"| Validation scripts | {mvp2_val.get('scripts', 0)} |",
        f"| Checks per dataset | {mvp2_val.get('checks_per_dataset', 0)} |",
        f"| Check types | row_count, schema_match, column_stats, checksum |",
        "",
        "---",
        "",
        "# EXECUTION TIMINGS",
        "",
        "| Phase | Step | Time |",
        "|-------|------|------|",
    ]
    for step_name, t in sorted(mvp1_timings.items()):
        lines.append(f"| MVP1 | {step_name} | {t:.2f}s |")
    for step_name, t in sorted(mvp2_timings.items()):
        lines.append(f"| MVP2 | {step_name} | {t:.2f}s |")
    lines.append(f"| **TOTAL** | **all** | **{total_elapsed:.1f}s** |")

    if all_errors:
        lines += ["", "---", "", "# ERRORS", ""]
        for step, err in all_errors:
            lines.append(f"- **{step}:** {err}")

    lines += [
        "",
        "---",
        "",
        "# OUTPUT FILES",
        "",
        "```",
        "mvp1_mvp2_outputs/",
        "  mvp1/                          # Discovery phase",
        "    analysis/",
        "      data_catalog.json          # PII + sensitivity catalog",
        "      inventory.json             # Programs + datasets inventory",
        "      gap_report.json            # Transpilation gaps",
        "    snowflake/",
        "      ddl_*.sql                  # CREATE TABLE statements",
        "      load_*.sql                 # COPY INTO + Snowpipe",
        "      migration_plan.json        # Migration strategy",
        "      transpiled/                # Transpiled SQL + Snowpark",
        "    discovery_report.md          # 7-section discovery report",
        "  mvp2/                          # Migration phase",
        "    ddl/                         # Snowflake DDL scripts",
        "    data_load/                   # COPY INTO + Snowpipe scripts",
        "    transpiled/                  # SQL + Snowpark Python",
        "    validation/                  # Post-migration validation SQL",
        "    migration_plan.json          # Full migration plan",
        "    migration_summary.json       # Execution summary",
        "    gap_report.json              # Detailed gap analysis",
        "  full_report.md                 # This report",
        "  results.json                   # Raw JSON results",
        "```",
    ]

    md_path = os.path.join(OUTPUT_DIR, "full_report.md")
    with open(md_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")
    return md_path


# =====================================================================
#  MAIN
# =====================================================================
def main():
    start = time.time()

    print()
    print(f"  {MAGENTA}+----------------------------------------------------------+{RESET}")
    print(f"  {MAGENTA}|{RESET}  {BOLD}SAS -> Snowflake Migration Toolkit{RESET}                       {MAGENTA}|{RESET}")
    print(f"  {MAGENTA}|{RESET}  {DIM}MVP1 Discovery + MVP2 Migration -- Full Report{RESET}          {MAGENTA}|{RESET}")
    print(f"  {MAGENTA}+----------------------------------------------------------+{RESET}")
    print()
    info(f"Timestamp:  {datetime.now().isoformat()}")
    info(f"Python:     {sys.version.split()[0]}")
    info(f"Output:     {OUTPUT_DIR}")
    print()

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    ensure_mock()
    config = load_config()

    # ── MVP1 ──
    banner("PHASE 1: MVP1 -- DISCOVERY", CYAN)
    mvp1_results, mvp1_errors, mvp1_timings, parsed_programs, datasets_metadata, lineage, inventory = run_mvp1(config)

    # ── MVP2 ──
    banner("PHASE 2: MVP2 -- MIGRATION", MAGENTA)
    mvp2_results, mvp2_errors, mvp2_timings = run_mvp2(config, parsed_programs, datasets_metadata, lineage, inventory)

    total_elapsed = time.time() - start
    all_errors = mvp1_errors + mvp2_errors

    # ── Generate full report ──
    banner("GENERATING FULL REPORT")
    md_path = write_full_report(mvp1_results, mvp1_errors, mvp1_timings,
                                 mvp2_results, mvp2_errors, mvp2_timings,
                                 total_elapsed)
    ok(f"full_report.md")

    # Save raw results JSON
    full_json = {
        "timestamp": datetime.now().isoformat(),
        "elapsed_seconds": total_elapsed,
        "mvp1": {"results": mvp1_results, "timings": mvp1_timings,
                  "errors": [{"step": s, "error": e} for s, e in mvp1_errors]},
        "mvp2": {"results": mvp2_results, "timings": mvp2_timings,
                  "errors": [{"step": s, "error": e} for s, e in mvp2_errors]},
    }
    results_path = os.path.join(OUTPUT_DIR, "results.json")
    with open(results_path, "w", encoding="utf-8") as f:
        json.dump(full_json, f, indent=2, default=str)

    # ── Final Status ──
    print()
    print(f"  {MAGENTA}+----------------------------------------------------------+{RESET}")
    mvp1_status = f"{GREEN}PASSED{RESET}" if not mvp1_errors else f"{RED}{len(mvp1_errors)} ERROR(S){RESET}"
    mvp2_status = f"{GREEN}PASSED{RESET}" if not mvp2_errors else f"{RED}{len(mvp2_errors)} ERROR(S){RESET}"
    overall = f"{GREEN}[OK] ALL 12 STEPS PASSED{RESET}" if not all_errors else f"{RED}[!] {len(all_errors)} ERROR(S){RESET}"
    print(f"  {MAGENTA}|{RESET}  MVP1 Discovery:  {BOLD}{mvp1_status}{RESET}")
    print(f"  {MAGENTA}|{RESET}  MVP2 Migration:  {BOLD}{mvp2_status}{RESET}")
    print(f"  {MAGENTA}|{RESET}  Overall:         {BOLD}{overall}{RESET}")
    print(f"  {MAGENTA}|{RESET}  Time:            {BOLD}{total_elapsed:.1f}s{RESET}")
    print(f"  {MAGENTA}+----------------------------------------------------------+{RESET}")
    print()

    # List output files
    info("Output files:")
    file_count = 0
    for root, _, files in os.walk(OUTPUT_DIR):
        for fname in sorted(files):
            fpath = os.path.join(root, fname)
            size = os.path.getsize(fpath)
            rel = os.path.relpath(fpath, OUTPUT_DIR).replace("\\", "/")
            size_str = f"{size:,}B" if size < 1024 else f"{size/1024:.1f}KB"
            print(f"  {DIM}|{RESET}   {rel:<50} {DIM}{size_str}{RESET}")
            file_count += 1

    print()
    info(f"Total: {file_count} files")
    print()
    sys.exit(0 if not all_errors else 1)


if __name__ == "__main__":
    main()
