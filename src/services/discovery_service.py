import os
import json
from typing import Dict, Optional

from src.utils.logger import get_logger
from src.connectors.sas.filesystem_scanner import SASFilesystemScanner
from src.parsers.sas.sas_code_parser import SASCodeParser
from src.parsers.sas.sas_data_parser import SASDataParser
from src.parsers.sas.lineage_builder import LineageBuilder
from src.catalog.catalog_generator import DataCatalogGenerator
from src.reporting.report_generator import ReportGenerator

logger = get_logger("discovery_service")


class DiscoveryService:
    def __init__(self, config: Dict, llm_advisor=None):
        self.config = config
        self.llm_advisor = llm_advisor

    def run(self, output_dir: str, catalog: bool = False,
            llm_validate: bool = False, llm_architecture: bool = False) -> Dict:
        os.makedirs(output_dir, exist_ok=True)
        results = {}

        # Step 1: Filesystem scan
        logger.info("Step 1: Scanning filesystem...")
        scanner = SASFilesystemScanner(self.config)
        programs_files = scanner.scan_programs()
        datasets_files = scanner.scan_datasets()
        logs_files = scanner.scan_logs()
        results["scan"] = {
            "programs": len(programs_files),
            "datasets": len(datasets_files),
            "logs": len(logs_files),
        }

        # Step 2: Parse SAS code
        logger.info("Step 2: Parsing SAS programs...")
        code_parser = SASCodeParser()
        parsed_programs = []
        for prog_file in programs_files:
            try:
                parsed = code_parser.parse_file(prog_file["absolute_path"])
                parsed_programs.append(parsed)
            except Exception as e:
                logger.error("Failed to parse %s: %s", prog_file["filename"], e)
        results["parsed_programs"] = len(parsed_programs)

        # Step 3: Parse dataset metadata
        logger.info("Step 3: Parsing dataset metadata...")
        data_parser = SASDataParser()
        datasets_metadata = []
        for ds_file in datasets_files:
            try:
                meta = data_parser.parse_file(ds_file["absolute_path"])
                meta["dataset_name"] = ds_file.get("dataset_name", "")
                datasets_metadata.append(meta)
            except Exception as e:
                logger.error("Failed to parse dataset %s: %s", ds_file["filename"], e)
        results["datasets_metadata"] = len(datasets_metadata)

        # Step 4: Build lineage
        logger.info("Step 4: Building lineage...")
        lineage_builder = LineageBuilder()
        lineage = lineage_builder.build_from_parsed_programs(parsed_programs)
        results["lineage"] = {
            "nodes": len(lineage["nodes"]),
            "edges": len(lineage["edges"]),
        }

        # Step 5: Generate catalog (optional)
        catalog_data = None
        if catalog:
            logger.info("Step 5: Generating data catalog...")
            catalog_gen = DataCatalogGenerator(self.config, self.llm_advisor)
            catalog_data = catalog_gen.generate_catalog(
                datasets_metadata, parsed_programs, lineage,
                enrich_with_llm=(llm_validate and self.llm_advisor is not None)
            )
            catalog_path = os.path.join(output_dir, "data_catalog.json")
            with open(catalog_path, "w", encoding="utf-8") as f:
                json.dump(catalog_data, f, indent=2, default=str)
            results["catalog"] = catalog_path

        # Step 6: LLM validation (optional)
        if llm_validate and self.llm_advisor:
            logger.info("Step 5b: Running LLM validation...")
            for parsed in parsed_programs[:5]:
                try:
                    with open(parsed["filepath"], "r", encoding="utf-8", errors="replace") as f:
                        code = f.read()
                    validation = self.llm_advisor.validate_parser_output(code, parsed)
                    parsed["llm_validation"] = validation
                except Exception as e:
                    logger.warning("LLM validation failed for %s: %s", parsed["filename"], e)

        # Step 6b: LLM architecture review (optional)
        if llm_architecture and self.llm_advisor:
            logger.info("Step 5c: Running LLM architecture review...")
            try:
                complexity = {
                    p["filename"]: {"score": p["complexity_score"], "level": p["complexity_level"]}
                    for p in parsed_programs
                }
                arch_review = self.llm_advisor.review_architecture(results["scan"], lineage, complexity)
                results["architecture_review"] = arch_review
                arch_path = os.path.join(output_dir, "architecture_review.json")
                with open(arch_path, "w", encoding="utf-8") as f:
                    json.dump(arch_review, f, indent=2, default=str)
            except Exception as e:
                logger.warning("LLM architecture review failed: %s", e)

        # Step 7: Generate report
        logger.info("Step 6: Generating report...")
        report_gen = ReportGenerator(self.config)
        report_path = report_gen.generate_discovery_report(
            parsed_programs, datasets_metadata, lineage, output_dir=output_dir
        )
        results["report"] = report_path

        # Save inventory JSON
        inventory_path = os.path.join(output_dir, "inventory.json")
        inventory = {
            "programs": [{"filename": p["filename"], "complexity_score": p["complexity_score"],
                          "complexity_level": p["complexity_level"], "procs_used": p["procs_used"]}
                         for p in parsed_programs],
            "datasets": [{"dataset_name": d.get("dataset_name", ""), "row_count": d.get("row_count", -1),
                          "column_count": d.get("column_count", 0)} for d in datasets_metadata],
            "lineage": lineage,
        }
        with open(inventory_path, "w", encoding="utf-8") as f:
            json.dump(inventory, f, indent=2, default=str)
        results["inventory"] = inventory_path

        logger.info("Discovery complete. Results: %s", results)
        return results
