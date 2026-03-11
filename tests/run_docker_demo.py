#!/usr/bin/env python3
"""
Full Docker Demo: MVP1 Discovery + MVP2 Migration + Unit Tests
Generates all artifacts in /app/docker_output for volume mount.
"""
import os
import sys
import json
import time
import subprocess
from datetime import datetime

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.join(BASE_DIR, "..")
OUTPUT_DIR = os.path.join(PROJECT_ROOT, "docker_output")
MOCK_DIR = os.path.join(BASE_DIR, "mock_sas_environment")
CONFIG_PATH = os.path.join(BASE_DIR, "mock_config.yaml")

SEPARATOR = "=" * 72
SUBSEP = "-" * 72


def banner(text):
    print(f"\n{SEPARATOR}")
    print(f"  {text}")
    print(SEPARATOR)


def section(text):
    print(f"\n{SUBSEP}")
    print(f"  {text}")
    print(SUBSEP)


def ensure_mock():
    if not os.path.exists(CONFIG_PATH):
        print("Mock environment not found. Generating...")
        subprocess.run([sys.executable, os.path.join(BASE_DIR, "create_mock_environment.py")], check=True)


def load_config():
    import yaml
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


# ============================================================
# PHASE 0: UNIT TESTS
# ============================================================
def run_unit_tests():
    banner("PHASE 0: UNIT TESTS (pytest)")
    result = subprocess.run(
        [sys.executable, "-m", "pytest", "tests/", "-v", "--tb=short", "-q"],
        cwd=PROJECT_ROOT,
        capture_output=True, text=True
    )
    print(result.stdout)
    if result.returncode != 0:
        print(result.stderr)
    return result.returncode == 0


# ============================================================
# PHASE 1: MVP1 DISCOVERY
# ============================================================
def run_mvp1_discovery(config):
    banner("PHASE 1: MVP1 DISCOVERY")
    discovery_dir = os.path.join(OUTPUT_DIR, "mvp1_discovery")
    os.makedirs(discovery_dir, exist_ok=True)

    from src.connectors.sas.filesystem_scanner import SASFilesystemScanner
    from src.parsers.sas.sas_code_parser import SASCodeParser
    from src.parsers.sas.sas_data_parser import SASDataParser
    from src.parsers.sas.lineage_builder import LineageBuilder
    from src.catalog.catalog_generator import DataCatalogGenerator
    from src.reporting.report_generator import ReportGenerator

    results = {}

    # Step 1: Filesystem Scan
    section("Step 1: Filesystem Scan")
    scanner = SASFilesystemScanner(config)
    programs = scanner.scan_programs()
    datasets = scanner.scan_datasets()
    print(f"  Programs found: {len(programs)}")
    for p in programs:
        print(f"    [{p['size_bytes']:>6} bytes] {p['filename']} ({p['line_count']} lines)")
    print(f"  Datasets found: {len(datasets)}")
    for d in datasets:
        print(f"    [{d['size_bytes']:>6} bytes] {d['filename']} (lib: {d['inferred_library']})")
    results["scan"] = {"programs": len(programs), "datasets": len(datasets)}

    # Step 2: Code Parsing
    section("Step 2: SAS Code Parsing")
    parser = SASCodeParser()
    parsed_programs = []
    for prog in programs:
        parsed = parser.parse_file(prog["absolute_path"])
        parsed_programs.append(parsed)

    # Pretty table
    print(f"\n  {'Program':<35} {'Lines':>5} {'Score':>5} {'Level':<10} {'PROCs'}")
    print(f"  {'-'*35} {'-'*5} {'-'*5} {'-'*10} {'-'*30}")
    for p in sorted(parsed_programs, key=lambda x: x["complexity_score"], reverse=True):
        procs = ", ".join(p["procs_used"][:5])
        print(f"  {p['filename']:<35} {p['line_count']:>5} {p['complexity_score']:>5} {p['complexity_level']:<10} {procs}")

    # Complexity distribution
    dist = {}
    for p in parsed_programs:
        dist[p["complexity_level"]] = dist.get(p["complexity_level"], 0) + 1
    print(f"\n  Complexity Distribution: {dist}")

    # Special features
    hash_progs = [p["filename"] for p in parsed_programs if p["has_hash_objects"]]
    dyn_progs = [p["filename"] for p in parsed_programs if p["has_dynamic_sql"]]
    merge_progs = [p["filename"] for p in parsed_programs if p["merge_statements"]]
    macro_progs = [p["filename"] for p in parsed_programs if p["macro_definitions"]]
    print(f"  Hash Objects:  {hash_progs or 'None'}")
    print(f"  Dynamic SQL:   {dyn_progs or 'None'}")
    print(f"  MERGE stmts:   {merge_progs or 'None'}")
    print(f"  Macro defs:    {macro_progs or 'None'}")
    results["parsing"] = {"programs_parsed": len(parsed_programs), "complexity": dist}

    # Step 3: Dataset Metadata
    section("Step 3: Dataset Metadata Extraction")
    data_dir = config["sas_environment"]["data_paths"][0]
    datasets_metadata = []
    for fname in sorted(os.listdir(data_dir)):
        if fname.endswith(".meta.json"):
            with open(os.path.join(data_dir, fname), "r", encoding="utf-8") as f:
                meta = json.load(f)
            datasets_metadata.append(meta)

    print(f"\n  {'Dataset':<20} {'Rows':>6} {'Cols':>4} {'Size':>10} {'PII Columns'}")
    print(f"  {'-'*20} {'-'*6} {'-'*4} {'-'*10} {'-'*30}")
    pii_keywords = {"cpf", "email", "phone", "telefone", "salary", "salario", "ssn", "rg", "nome", "name", "address", "endereco"}
    for ds in datasets_metadata:
        pii = [c["name"] for c in ds.get("columns", []) if c["name"].lower() in pii_keywords]
        size_kb = ds.get("size_bytes", 0) / 1024
        print(f"  {ds['dataset_name']:<20} {ds['row_count']:>6} {ds['column_count']:>4} {size_kb:>8.1f}KB {', '.join(pii) or '-'}")
    results["metadata"] = {"datasets": len(datasets_metadata), "total_rows": sum(d["row_count"] for d in datasets_metadata)}

    # Step 4: Lineage
    section("Step 4: Lineage Graph Construction")
    builder = LineageBuilder()
    lineage = builder.build_from_parsed_programs(parsed_programs)
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

    print(f"  Total Nodes: {len(nodes)}")
    print(f"  Total Edges: {len(edges)}")
    print(f"  Node Types:  {types}")
    print(f"  Root Nodes:  {len(roots)} (sources with no input)")
    print(f"  Leaf Nodes:  {len(leaves)} (outputs with no consumer)")

    # Show sample traversals
    for ds_node in [n for n in nodes if n["type"] == "dataset"][:3]:
        up = builder.get_upstream(ds_node["id"])
        down = builder.get_downstream(ds_node["id"])
        print(f"  {ds_node['label']}: {len(up)} upstream, {len(down)} downstream")
    results["lineage"] = {"nodes": len(nodes), "edges": len(edges), "roots": len(roots), "leaves": len(leaves)}

    # Step 5: Data Catalog
    section("Step 5: Data Catalog Generation")
    catalog_gen = DataCatalogGenerator(config=config, llm_advisor=None)
    catalog = catalog_gen.generate_catalog(datasets_metadata, parsed_programs, lineage, enrich_with_llm=False)
    summary = catalog["summary"]

    print(f"  Total Datasets:       {summary['total_datasets']}")
    print(f"  Total Columns:        {summary['total_columns']}")
    print(f"  Domains:              {summary['domains']}")
    print(f"  Sensitivity:          {summary['sensitivity_distribution']}")
    print(f"  Datasets with PII:    {summary['datasets_with_pii']}")

    print(f"\n  {'Dataset':<20} {'Domain':<12} {'Sensitivity':<12} {'PII'}")
    print(f"  {'-'*20} {'-'*12} {'-'*12} {'-'*20}")
    for ds in catalog["datasets"]:
        pii = ", ".join(ds.get("pii_columns", [])) or "-"
        print(f"  {ds['dataset_name']:<20} {ds['domain']:<12} {ds['sensitivity']:<12} {pii}")

    # Save catalog
    catalog_path = os.path.join(discovery_dir, "data_catalog.json")
    with open(catalog_path, "w", encoding="utf-8") as f:
        json.dump(catalog, f, indent=2, default=str)
    results["catalog"] = summary

    # Step 6: Report
    section("Step 6: Discovery Report Generation")
    report_gen = ReportGenerator(config)
    report_path = report_gen.generate_discovery_report(
        parsed_programs, datasets_metadata, lineage, output_dir=discovery_dir
    )
    with open(report_path, "r", encoding="utf-8") as f:
        report_content = f.read()

    sections_found = [s for s in ["1. Executive Summary", "2. Program Inventory", "3. Dataset Inventory",
                                   "4. Dependencies and Lineage", "5. Complexity Analysis",
                                   "6. Limitations", "7. Next Steps"] if s in report_content]
    print(f"  Report: {report_path}")
    print(f"  Size: {len(report_content)} chars")
    print(f"  Sections: {len(sections_found)}/7 present")
    results["report"] = report_path

    # Save inventory for MVP2
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
    inv_path = os.path.join(discovery_dir, "inventory.json")
    with open(inv_path, "w", encoding="utf-8") as f:
        json.dump(inventory, f, indent=2, default=str)
    results["inventory"] = inv_path

    return results, inv_path


# ============================================================
# PHASE 2: MVP2 MIGRATION
# ============================================================
def run_mvp2_migration(config, inventory_path):
    banner("PHASE 2: MVP2 MIGRATION ARTIFACTS")
    migration_dir = os.path.join(OUTPUT_DIR, "mvp2_migration")
    os.makedirs(migration_dir, exist_ok=True)

    with open(inventory_path, "r", encoding="utf-8") as f:
        inventory = json.load(f)

    from src.migration.data_migrator.snowflake_migrator import SnowflakeMigrator
    from src.migration.code_transpiler.snowflake_transpiler import SnowflakeTranspiler
    from src.validation.validator import MigrationValidator

    results = {}

    # Step 1: DDL Generation
    section("Step 1: Snowflake DDL Generation")
    migrator = SnowflakeMigrator(config)
    ddl_dir = os.path.join(migration_dir, "ddl")
    os.makedirs(ddl_dir, exist_ok=True)

    for ds in inventory["datasets"]:
        ddl = migrator.generate_ddl(ds)
        ddl_path = os.path.join(ddl_dir, f"{ds['dataset_name']}.sql")
        with open(ddl_path, "w", encoding="utf-8") as f:
            f.write(ddl)
        print(f"  DDL: {ds['dataset_name']}.sql")
        # Print first few lines
        for line in ddl.split("\n")[:3]:
            print(f"    {line}")
        print(f"    ... ({len(ddl.split(chr(10)))} lines)")

    # Step 2: COPY INTO + Snowpipe
    section("Step 2: Data Load Scripts (COPY INTO + Snowpipe)")
    load_dir = os.path.join(migration_dir, "data_load")
    os.makedirs(load_dir, exist_ok=True)

    for ds in inventory["datasets"]:
        copy_sql = migrator.generate_copy_into(ds)
        pipe_sql = migrator.generate_snowpipe(ds)
        load_path = os.path.join(load_dir, f"load_{ds['dataset_name']}.sql")
        with open(load_path, "w", encoding="utf-8") as f:
            f.write(f"-- COPY INTO for {ds['dataset_name']}\n{copy_sql}\n\n")
            f.write(f"-- Snowpipe for {ds['dataset_name']}\n{pipe_sql}\n")
        print(f"  Load script: load_{ds['dataset_name']}.sql")

    # Step 3: Code Transpilation
    section("Step 3: SAS -> Snowflake Code Transpilation")
    transpile_dir = os.path.join(migration_dir, "transpiled")
    os.makedirs(transpile_dir, exist_ok=True)
    transpiler = SnowflakeTranspiler(config)

    all_gaps = []
    total_coverage = []
    for prog in inventory["programs"]:
        result = transpiler.transpile(prog)
        sql_path = os.path.join(transpile_dir, f"{prog['filename'].replace('.sas', '')}.sql")
        with open(sql_path, "w", encoding="utf-8") as f:
            f.write(f"-- Transpiled from: {prog['filename']}\n")
            f.write(f"-- Coverage: {result['coverage_pct']}%\n")
            f.write(f"-- Gaps: {len(result['gaps'])}\n\n")
            f.write(result["sql_code"])

        if result.get("snowpark_code") and result["snowpark_code"] != "# No Snowpark code generated":
            py_path = os.path.join(transpile_dir, f"{prog['filename'].replace('.sas', '')}_snowpark.py")
            with open(py_path, "w", encoding="utf-8") as f:
                f.write(f"# Snowpark transpilation from: {prog['filename']}\n\n")
                f.write(result["snowpark_code"])

        coverage = result["coverage_pct"]
        total_coverage.append(coverage)
        gaps_str = f" | Gaps: {', '.join(result['gaps'][:2])}" if result["gaps"] else ""
        print(f"  {prog['filename']:<35} Coverage: {coverage:>5.1f}%{gaps_str}")
        all_gaps.extend(result["gaps"])

    avg_coverage = sum(total_coverage) / len(total_coverage) if total_coverage else 0
    print(f"\n  Average Coverage: {avg_coverage:.1f}%")
    print(f"  Total Gaps: {len(all_gaps)}")
    if all_gaps:
        unique_gaps = list(set(all_gaps))
        print(f"  Unique Gaps:")
        for g in unique_gaps:
            print(f"    - {g}")
    results["transpilation"] = {"avg_coverage": avg_coverage, "total_gaps": len(all_gaps), "unique_gaps": list(set(all_gaps))}

    # Step 4: Validation Scripts
    section("Step 4: Post-Migration Validation Scripts")
    val_dir = os.path.join(migration_dir, "validation")
    os.makedirs(val_dir, exist_ok=True)
    validator = MigrationValidator()
    target_config = {"database": "SAS_MIGRATION", "schema": "RAW"}

    for ds in inventory["datasets"]:
        scripts = validator.generate_validation_scripts(ds, target_config)
        val_path = os.path.join(val_dir, f"validate_{ds['dataset_name']}.sql")
        with open(val_path, "w", encoding="utf-8") as f:
            f.write(f"-- Validation scripts for {ds['dataset_name']}\n")
            f.write(f"-- Source rows: {ds.get('row_count', 'N/A')}\n\n")
            for key, sql in scripts.items():
                if key not in ("dataset", "target_table"):
                    f.write(f"\n-- === {key.upper()} ===\n{sql}\n")
        print(f"  Validation: validate_{ds['dataset_name']}.sql (checks: row_count, schema, stats, checksum)")

    # Step 5: Migration Plan Summary
    section("Step 5: Migration Plan")
    plan = migrator.generate_migration_plan(inventory["datasets"])
    plan_path = os.path.join(migration_dir, "migration_plan.json")
    with open(plan_path, "w", encoding="utf-8") as f:
        json.dump(plan, f, indent=2)

    print(f"  Database: {plan['database']}")
    print(f"  Total datasets: {plan['total_datasets']}")
    print(f"  Plan saved: {plan_path}")
    results["plan"] = plan_path

    # Save gap report
    gap_report = {
        "timestamp": datetime.now().isoformat(),
        "total_programs": len(inventory["programs"]),
        "avg_coverage": avg_coverage,
        "gaps": list(set(all_gaps)),
        "programs_with_gaps": [
            p["filename"] for p in inventory["programs"]
            if p.get("has_hash_objects") or p.get("has_dynamic_sql") or
            any(proc in ("LOGISTIC", "REG", "GLM", "MIXED", "IML", "REPORT", "TABULATE")
                for proc in p.get("procs_used", []))
        ],
    }
    gap_path = os.path.join(migration_dir, "gap_report.json")
    with open(gap_path, "w", encoding="utf-8") as f:
        json.dump(gap_report, f, indent=2)
    print(f"\n  Gap report saved: {gap_path}")
    results["gap_report"] = gap_report

    return results


# ============================================================
# FINAL SUMMARY
# ============================================================
def print_final_summary(tests_ok, mvp1_results, mvp2_results, elapsed):
    banner("FINAL SUMMARY")

    print(f"\n  Execution time: {elapsed:.1f}s")
    print(f"  Output directory: {OUTPUT_DIR}")
    print()

    print("  UNIT TESTS:")
    print(f"    Status: {'PASSED' if tests_ok else 'FAILED'}")
    print()

    print("  MVP1 DISCOVERY:")
    print(f"    Programs scanned:    {mvp1_results.get('scan', {}).get('programs', 0)}")
    print(f"    Datasets cataloged:  {mvp1_results.get('scan', {}).get('datasets', 0)}")
    print(f"    Complexity:          {mvp1_results.get('parsing', {}).get('complexity', {})}")
    lineage = mvp1_results.get("lineage", {})
    print(f"    Lineage:             {lineage.get('nodes', 0)} nodes, {lineage.get('edges', 0)} edges")
    catalog = mvp1_results.get("catalog", {})
    print(f"    PII detected:        {catalog.get('datasets_with_pii', 0)} dataset(s)")
    print(f"    Sensitivity:         {catalog.get('sensitivity_distribution', {})}")
    print(f"    Report:              {mvp1_results.get('report', 'N/A')}")
    print()

    print("  MVP2 MIGRATION:")
    transp = mvp2_results.get("transpilation", {})
    print(f"    Avg coverage:        {transp.get('avg_coverage', 0):.1f}%")
    print(f"    Unique gaps:         {len(transp.get('unique_gaps', []))}")
    for g in transp.get("unique_gaps", []):
        print(f"      - {g}")
    print(f"    Plan:                {mvp2_results.get('plan', 'N/A')}")
    print()

    # List output files
    print("  OUTPUT FILES:")
    for root, dirs, files in os.walk(OUTPUT_DIR):
        level = root.replace(OUTPUT_DIR, "").count(os.sep)
        indent = "    " + "  " * level
        print(f"{indent}{os.path.basename(root)}/")
        subindent = "    " + "  " * (level + 1)
        for f in sorted(files):
            fpath = os.path.join(root, f)
            size = os.path.getsize(fpath)
            print(f"{subindent}{f} ({size:,} bytes)")

    print(f"\n{SEPARATOR}")
    status = "ALL PASSED" if tests_ok else "PARTIAL (unit tests failed)"
    print(f"  FINAL STATUS: {status}")
    print(SEPARATOR)


# ============================================================
# MAIN
# ============================================================
def main():
    start = time.time()

    banner("SAS-TO-SNOWFLAKE MIGRATION TOOLKIT - DOCKER DEMO")
    print(f"  Timestamp: {datetime.now().isoformat()}")
    print(f"  Python:    {sys.version}")
    print(f"  Output:    {OUTPUT_DIR}")

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    ensure_mock()
    config = load_config()

    # Phase 0: Unit Tests
    tests_ok = run_unit_tests()

    # Phase 1: MVP1
    mvp1_results, inventory_path = run_mvp1_discovery(config)

    # Phase 2: MVP2
    mvp2_results = run_mvp2_migration(config, inventory_path)

    elapsed = time.time() - start
    print_final_summary(tests_ok, mvp1_results, mvp2_results, elapsed)

    # Save full results JSON
    full_results = {
        "timestamp": datetime.now().isoformat(),
        "elapsed_seconds": elapsed,
        "tests_passed": tests_ok,
        "mvp1": mvp1_results,
        "mvp2": mvp2_results,
    }
    results_path = os.path.join(OUTPUT_DIR, "demo_results.json")
    with open(results_path, "w", encoding="utf-8") as f:
        json.dump(full_results, f, indent=2, default=str)


if __name__ == "__main__":
    main()
