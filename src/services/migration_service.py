import os
import json
from typing import Dict, Optional

from src.utils.logger import get_logger
from src.migration.code_transpiler.snowflake_transpiler import SnowflakeTranspiler
from src.migration.data_migrator.snowflake_migrator import SnowflakeMigrator
from src.validation.validator import MigrationValidator

logger = get_logger("migration_service")


class MigrationService:
    def __init__(self, config: Dict, llm_advisor=None):
        self.config = config
        self.llm_advisor = llm_advisor

    def run(self, inventory_path: str, output_dir: str,
            llm_review: bool = False, llm_gaps: bool = False,
            validate_only: bool = False) -> Dict:
        os.makedirs(output_dir, exist_ok=True)

        with open(inventory_path, "r", encoding="utf-8") as f:
            inventory = json.load(f)

        results = {}

        if validate_only:
            return self._run_validation_only(inventory, output_dir)

        # Step 1: Generate DDL and data load scripts
        logger.info("Step 1: Generating DDL and data load scripts...")
        migrator = SnowflakeMigrator(self.config)
        migration_plan = migrator.generate_migration_plan(inventory.get("datasets", []))
        plan_path = os.path.join(output_dir, "migration_plan.json")
        with open(plan_path, "w", encoding="utf-8") as f:
            json.dump(migration_plan, f, indent=2)
        results["migration_plan"] = plan_path

        # Save DDLs
        ddl_dir = os.path.join(output_dir, "ddl")
        os.makedirs(ddl_dir, exist_ok=True)
        for ds in migration_plan.get("datasets", []):
            ddl_path = os.path.join(ddl_dir, f"{ds['name']}.sql")
            with open(ddl_path, "w", encoding="utf-8") as f:
                f.write(ds.get("ddl", ""))

        # Step 2: Transpile code
        logger.info("Step 2: Transpiling SAS code...")
        transpiler = SnowflakeTranspiler(self.config)
        transpile_dir = os.path.join(output_dir, "transpiled")
        os.makedirs(transpile_dir, exist_ok=True)

        for prog in inventory.get("programs", []):
            try:
                result = transpiler.transpile(prog)
                sql_path = os.path.join(transpile_dir, f"{prog['filename'].replace('.sas', '')}.sql")
                with open(sql_path, "w", encoding="utf-8") as f:
                    f.write(result.get("sql_code", ""))

                if llm_review and self.llm_advisor:
                    review = self.llm_advisor.review_transpiled_code("", result["sql_code"], "Snowflake")
                    result["llm_review"] = review
            except Exception as e:
                logger.error("Transpilation failed for %s: %s", prog.get("filename", ""), e)

        # Step 3: Generate validation scripts
        logger.info("Step 3: Generating validation scripts...")
        validator = MigrationValidator()
        val_dir = os.path.join(output_dir, "validation")
        os.makedirs(val_dir, exist_ok=True)

        target_config = {"database": self.config.get("target", {}).get("database", "SAS_MIGRATION"), "schema": "RAW"}
        for ds in inventory.get("datasets", []):
            scripts = validator.generate_validation_scripts(ds, target_config)
            val_path = os.path.join(val_dir, f"validate_{ds.get('dataset_name', 'unknown')}.sql")
            with open(val_path, "w", encoding="utf-8") as f:
                for key, sql in scripts.items():
                    if key not in ("dataset", "target_table"):
                        f.write(f"\n-- {key}\n{sql}\n")

        # Step 4: LLM gap resolution (optional)
        if llm_gaps and self.llm_advisor:
            logger.info("Step 4: LLM gap resolution...")
            # Collect all gaps
            all_gaps = []
            for prog in inventory.get("programs", []):
                # gaps would come from transpilation results
                pass
            if all_gaps:
                suggestions = self.llm_advisor.suggest_gap_resolution({"gaps": all_gaps})
                gap_path = os.path.join(output_dir, "gap_suggestions.json")
                with open(gap_path, "w", encoding="utf-8") as f:
                    json.dump(suggestions, f, indent=2)

        logger.info("Migration artifacts generated in %s", output_dir)
        return results

    def _run_validation_only(self, inventory: Dict, output_dir: str) -> Dict:
        validator = MigrationValidator()
        val_dir = os.path.join(output_dir, "validation")
        os.makedirs(val_dir, exist_ok=True)
        target_config = {"database": self.config.get("target", {}).get("database", "SAS_MIGRATION"), "schema": "RAW"}
        for ds in inventory.get("datasets", []):
            scripts = validator.generate_validation_scripts(ds, target_config)
            val_path = os.path.join(val_dir, f"validate_{ds.get('dataset_name', 'unknown')}.sql")
            with open(val_path, "w", encoding="utf-8") as f:
                for key, sql in scripts.items():
                    if key not in ("dataset", "target_table"):
                        f.write(f"\n-- {key}\n{sql}\n")
        return {"validation_dir": val_dir}
