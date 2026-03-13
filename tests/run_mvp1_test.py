"""
End-to-end test for MVP1 (Discovery).
Runs all 6 steps in sequence, reports pass/fail for each.
"""
import os
import sys
import json

# Add project root to path
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
MOCK_DIR = os.path.join(BASE_DIR, "mock_sas_environment")
OUTPUT_DIR = os.path.join(BASE_DIR, "output")
CONFIG_PATH = os.path.join(BASE_DIR, "mock_config.yaml")


def load_config():
    import yaml
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def step1_filesystem_scan(config):
    """Step 1: Filesystem scan"""
    from src.connectors.sas.filesystem_scanner import SASFilesystemScanner

    scanner = SASFilesystemScanner(config)
    programs = scanner.scan_programs()
    datasets = scanner.scan_datasets()

    print(f"  Programs found: {len(programs)}")
    for p in programs:
        print(f"    - {p['filename']} ({p['line_count']} lines, {p['size_bytes']} bytes)")

    print(f"  Datasets found: {len(datasets)}")
    for d in datasets:
        print(f"    - {d['filename']} (library: {d['inferred_library']})")

    # Validate backup was excluded
    backup_files = [p for p in programs if "backup" in p["absolute_path"].replace("\\", "/")]
    assert len(backup_files) == 0, f"Backup files should be excluded, found: {backup_files}"
    assert len(programs) >= 7, f"Expected at least 7 programs, found {len(programs)}"
    assert len(datasets) >= 5, f"Expected at least 5 datasets, found {len(datasets)}"

    print("  [PASS] Backup excluded, correct counts")
    return programs, datasets


def step2_code_parsing(programs):
    """Step 2: Parse SAS code"""
    from src.parsers.sas.sas_code_parser import SASCodeParser

    parser = SASCodeParser()
    parsed = []

    for prog in programs:
        result = parser.parse_file(prog["absolute_path"])
        parsed.append(result)
        print(f"  {result['filename']}:")
        print(f"    Complexity: {result['complexity_score']} ({result['complexity_level']})")
        print(f"    PROCs: {result['procs_used']}")
        print(f"    Datasets read: {result['datasets_read'][:5]}")
        print(f"    Datasets written: {result['datasets_written'][:5]}")
        print(f"    Macros defined: {result['macro_definitions']}")
        print(f"    Macros called: {result['macro_calls']}")
        if result['has_hash_objects']:
            print(f"    ** Has hash objects")
        if result['has_dynamic_sql']:
            print(f"    ** Has dynamic SQL")

    # Check complexity distribution
    levels = [p["complexity_level"] for p in parsed]
    print(f"\n  Complexity distribution: {dict((l, levels.count(l)) for l in set(levels))}")
    print("  [PASS] All programs parsed successfully")
    return parsed


def step3_data_metadata(config):
    """Step 3: Parse dataset metadata from .meta.json"""
    data_dir = config["sas_environment"]["data_paths"][0]
    datasets_metadata = []

    for fname in os.listdir(data_dir):
        if fname.endswith(".meta.json"):
            meta_path = os.path.join(data_dir, fname)
            with open(meta_path, "r", encoding="utf-8") as f:
                meta = json.load(f)
            datasets_metadata.append(meta)
            print(f"  {meta['dataset_name']}: {meta['row_count']} rows, {meta['column_count']} columns")
            for col in meta.get("columns", [])[:3]:
                print(f"    - {col['name']} ({col['type']}, {col.get('format', '')})")

    assert len(datasets_metadata) >= 5, f"Expected at least 5 datasets, found {len(datasets_metadata)}"
    print("  [PASS] All dataset metadata loaded")
    return datasets_metadata


def step4_lineage(parsed_programs):
    """Step 4: Build lineage graph"""
    from src.parsers.sas.lineage_builder import LineageBuilder

    builder = LineageBuilder()
    lineage = builder.build_from_parsed_programs(parsed_programs)

    nodes = lineage["nodes"]
    edges = lineage["edges"]
    print(f"  Nodes: {len(nodes)}")
    print(f"  Edges: {len(edges)}")

    # Show node types
    types = {}
    for n in nodes:
        t = n["type"]
        types[t] = types.get(t, 0) + 1
    print(f"  Node types: {types}")

    # Test upstream/downstream
    # Find a dataset node to test
    dataset_nodes = [n for n in nodes if n["type"] == "dataset"]
    if dataset_nodes:
        test_node = dataset_nodes[0]["id"]
        upstream = builder.get_upstream(test_node)
        downstream = builder.get_downstream(test_node)
        print(f"  Test node: {test_node}")
        print(f"    Upstream: {len(upstream)} nodes")
        print(f"    Downstream: {len(downstream)} nodes")

    assert len(nodes) >= 20, f"Expected 20+ nodes, got {len(nodes)}"
    assert len(edges) >= 15, f"Expected 15+ edges, got {len(edges)}"
    print("  [PASS] Lineage built successfully")
    return lineage


def step5_catalog(config, datasets_metadata, parsed_programs, lineage):
    """Step 5: Generate data catalog"""
    from src.catalog.catalog_generator import DataCatalogGenerator

    generator = DataCatalogGenerator(config=config, llm_advisor=None)
    catalog = generator.generate_catalog(
        datasets_metadata,
        programs_metadata=parsed_programs,
        lineage_data=lineage,
        enrich_with_llm=False,
    )

    summary = catalog["summary"]
    print(f"  Total datasets: {summary['total_datasets']}")
    print(f"  Total columns: {summary['total_columns']}")
    print(f"  Domains: {summary['domains']}")
    print(f"  Sensitivity: {summary['sensitivity_distribution']}")
    print(f"  Datasets with PII: {summary['datasets_with_pii']}")

    for ds in catalog["datasets"]:
        pii = ds.get("pii_columns", [])
        if pii:
            print(f"  PII in {ds['dataset_name']}: {pii}")

    assert summary["datasets_with_pii"] > 0, "Expected at least 1 dataset with PII"
    print("  [PASS] Catalog generated with PII detection")
    return catalog


def step6_report(config, parsed_programs, datasets_metadata, lineage):
    """Step 6: Generate discovery report"""
    from src.reporting.report_generator import ReportGenerator

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    generator = ReportGenerator(config)
    report_path = generator.generate_discovery_report(
        parsed_programs, datasets_metadata, lineage, output_dir=OUTPUT_DIR
    )

    assert os.path.exists(report_path), f"Report not found: {report_path}"

    with open(report_path, "r", encoding="utf-8") as f:
        content = f.read()

    expected_sections = [
        "1. Executive Summary",
        "2. Program Inventory",
        "3. Dataset Inventory",
        "4. Dependencies and Lineage",
        "5. Complexity Analysis",
        "6. Limitations",
        "7. Next Steps",
    ]
    for section in expected_sections:
        assert section in content, f"Missing section: {section}"

    print(f"  Report generated: {report_path}")
    print(f"  Report size: {len(content)} chars")
    print(f"  All 7 sections present: YES")
    print("  [PASS] Report generated successfully")
    return report_path


def main():
    print("=" * 70)
    print("SAS Migration Toolkit - MVP1 End-to-End Test")
    print("=" * 70)

    if not os.path.exists(CONFIG_PATH):
        print(f"\nERROR: Config not found at {CONFIG_PATH}")
        print("Run: python tests/create_mock_environment.py first")
        sys.exit(1)

    config = load_config()
    results = {}
    steps = [
        ("Step 1: Filesystem Scan", lambda: step1_filesystem_scan(config)),
        ("Step 2: Code Parsing", None),  # depends on step1
        ("Step 3: Data Metadata", lambda: step3_data_metadata(config)),
        ("Step 4: Lineage", None),  # depends on step2
        ("Step 5: Data Catalog", None),  # depends on step3, step4
        ("Step 6: Report Generation", None),  # depends on all
    ]

    errors = []

    # Step 1
    print(f"\n{'='*50}")
    print("Step 1: Filesystem Scan")
    print(f"{'='*50}")
    try:
        programs, datasets = step1_filesystem_scan(config)
        results["programs"] = programs
        results["datasets"] = datasets
    except Exception as e:
        print(f"  [FAIL] {e}")
        errors.append(("Step 1", str(e)))
        results["programs"] = []
        results["datasets"] = []

    # Step 2
    print(f"\n{'='*50}")
    print("Step 2: Code Parsing")
    print(f"{'='*50}")
    try:
        parsed_programs = step2_code_parsing(results["programs"])
        results["parsed"] = parsed_programs
    except Exception as e:
        print(f"  [FAIL] {e}")
        errors.append(("Step 2", str(e)))
        results["parsed"] = []

    # Step 3
    print(f"\n{'='*50}")
    print("Step 3: Data Metadata")
    print(f"{'='*50}")
    try:
        datasets_metadata = step3_data_metadata(config)
        results["datasets_meta"] = datasets_metadata
    except Exception as e:
        print(f"  [FAIL] {e}")
        errors.append(("Step 3", str(e)))
        results["datasets_meta"] = []

    # Step 4
    print(f"\n{'='*50}")
    print("Step 4: Lineage")
    print(f"{'='*50}")
    try:
        lineage = step4_lineage(results["parsed"])
        results["lineage"] = lineage
    except Exception as e:
        print(f"  [FAIL] {e}")
        errors.append(("Step 4", str(e)))
        results["lineage"] = {"nodes": [], "edges": []}

    # Step 5
    print(f"\n{'='*50}")
    print("Step 5: Data Catalog")
    print(f"{'='*50}")
    try:
        catalog = step5_catalog(config, results["datasets_meta"], results["parsed"], results["lineage"])
        results["catalog"] = catalog
    except Exception as e:
        print(f"  [FAIL] {e}")
        errors.append(("Step 5", str(e)))

    # Step 6
    print(f"\n{'='*50}")
    print("Step 6: Report Generation")
    print(f"{'='*50}")
    try:
        report_path = step6_report(config, results["parsed"], results["datasets_meta"], results["lineage"])
        results["report"] = report_path
    except Exception as e:
        print(f"  [FAIL] {e}")
        errors.append(("Step 6", str(e)))

    # Final summary
    print(f"\n{'='*70}")
    print("FINAL RESULTS")
    print(f"{'='*70}")
    if not errors:
        print("ALL STEPS PASSED")
    else:
        print(f"PARTIAL - {len(errors)} error(s):")
        for step, err in errors:
            print(f"  {step}: {err}")

    sys.exit(0 if not errors else 1)


if __name__ == "__main__":
    main()
