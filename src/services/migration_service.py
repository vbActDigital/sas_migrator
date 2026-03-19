import os
import json
from typing import Dict

from src.utils.logger import get_logger

logger = get_logger("migration_service")


class MigrationService:
    def __init__(self, config: Dict, llm_advisor=None):
        self.config = config
        self.llm_advisor = llm_advisor
        self.platform = config.get("target", {}).get("platform", "snowflake")

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
        logger.info("Step 1: Generating DDL and data load scripts for %s...", self.platform)
        migrator = self._get_data_migrator()
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

        # Save data load scripts
        load_dir = os.path.join(output_dir, "data_load")
        os.makedirs(load_dir, exist_ok=True)
        for ds in migration_plan.get("datasets", []):
            load_path = os.path.join(load_dir, f"load_{ds['name']}.sql")
            with open(load_path, "w", encoding="utf-8") as f:
                f.write(ds.get("copy_into", ""))

        # Step 2: Transpile code
        logger.info("Step 2: Transpiling SAS code to %s...", self.platform)
        transpiler = self._get_transpiler()
        transpile_dir = os.path.join(output_dir, "transpiled")
        os.makedirs(transpile_dir, exist_ok=True)

        all_gaps = []
        manual_interventions = []
        converted_count = 0

        for prog in inventory.get("programs", []):
            try:
                result = transpiler.transpile(prog)
                base_name = prog.get("filename", "unknown").replace(".sas", "")
                sql_path = os.path.join(transpile_dir, f"{base_name}.sql")
                with open(sql_path, "w", encoding="utf-8") as f:
                    f.write(result.get("sql_code", ""))

                # Write platform-specific code
                if self.platform == "databricks":
                    pyspark_code = result.get("pyspark_code", "")
                    if pyspark_code and pyspark_code != "# No PySpark code generated":
                        py_path = os.path.join(transpile_dir, f"{base_name}_pyspark.py")
                        with open(py_path, "w", encoding="utf-8") as f:
                            f.write(pyspark_code)
                else:
                    snowpark_code = result.get("snowpark_code", "")
                    if snowpark_code and snowpark_code != "# No Snowpark code generated":
                        py_path = os.path.join(transpile_dir, f"{base_name}_snowpark.py")
                        with open(py_path, "w", encoding="utf-8") as f:
                            f.write(snowpark_code)

                prog_gaps = result.get("gaps", [])
                all_gaps.extend(prog_gaps)

                # Track manual interventions per program
                self._check_manual_interventions(
                    prog, prog_gaps, manual_interventions
                )

                if not prog_gaps:
                    converted_count += 1

                if llm_review and self.llm_advisor:
                    review = self.llm_advisor.review_transpiled_code(
                        "", result.get("sql_code", ""), self.platform,
                    )
                    result["llm_review"] = review
            except Exception as e:
                logger.error("Transpilation failed for %s: %s", prog.get("filename", ""), e)
                manual_interventions.append({
                    "severity": "CRITICAL",
                    "program": prog.get("filename", "unknown"),
                    "reason": f"Falha na transpilacao: {e}",
                })

        results["converted"] = converted_count
        results["total_programs"] = len(inventory.get("programs", []))
        results["manual_interventions"] = manual_interventions

        # Save gap report
        if all_gaps:
            gap_path = os.path.join(output_dir, "gap_report.json")
            with open(gap_path, "w", encoding="utf-8") as f:
                json.dump({
                    "platform": self.platform,
                    "gaps": list(set(all_gaps)),
                    "manual_interventions": manual_interventions,
                }, f, indent=2)

        # Step 3: Generate validation scripts
        logger.info("Step 3: Generating validation scripts...")
        self._generate_validations(inventory, output_dir)

        # Step 4: LLM gap resolution (optional)
        if llm_gaps and self.llm_advisor and all_gaps:
            logger.info("Step 4: LLM gap resolution...")
            suggestions = self.llm_advisor.suggest_gap_resolution(
                {"platform": self.platform, "gaps": list(set(all_gaps))},
            )
            gap_path = os.path.join(output_dir, "gap_suggestions.json")
            with open(gap_path, "w", encoding="utf-8") as f:
                json.dump(suggestions, f, indent=2)

        # Step 5: LLM suggestions for manual interventions
        if llm_gaps and self.llm_advisor and manual_interventions:
            logger.info("Step 5: Generating LLM suggestions for %d manual intervention(s)...",
                        len(manual_interventions))
            programs_by_name = {
                p.get("filename", ""): p for p in inventory.get("programs", [])
            }
            for intervention in manual_interventions:
                prog_info = programs_by_name.get(intervention["program"], {})
                try:
                    suggestion = self.llm_advisor.suggest_manual_intervention(
                        prog_info, intervention, self.platform,
                    )
                    intervention["llm_suggestion"] = suggestion
                except Exception as e:
                    logger.warning("LLM suggestion failed for %s: %s",
                                   intervention["program"], e)

            # Save enriched interventions
            interventions_path = os.path.join(output_dir, "manual_interventions.json")
            with open(interventions_path, "w", encoding="utf-8") as f:
                json.dump({
                    "platform": self.platform,
                    "total": len(manual_interventions),
                    "interventions": manual_interventions,
                }, f, indent=2, default=str)
            results["manual_interventions_report"] = interventions_path

        logger.info(
            "Migration complete: %d/%d programs converted, %d need manual intervention",
            converted_count, len(inventory.get("programs", [])), len(manual_interventions),
        )
        return results

    def _check_manual_interventions(self, prog: Dict, gaps: list,
                                     interventions: list):
        """Check a program for patterns that require manual intervention."""
        filename = prog.get("filename", "unknown")

        if prog.get("has_hash_objects"):
            interventions.append({
                "severity": "HIGH",
                "program": filename,
                "reason": "Hash objects detectados - requer reescrita manual "
                          "(usar JOIN/broadcast join na plataforma alvo)",
            })

        if prog.get("has_dynamic_sql"):
            interventions.append({
                "severity": "HIGH",
                "program": filename,
                "reason": "SQL dinamico / CALL EXECUTE - logica dinamica requer analise manual",
            })

        statistical_procs = {"logistic", "reg", "glm", "mixed", "genmod",
                             "phreg", "lifetest", "nlin", "iml"}
        stat_found = [p for p in prog.get("procs_used", [])
                      if p.lower() in statistical_procs]
        if stat_found:
            interventions.append({
                "severity": "MEDIUM",
                "program": filename,
                "reason": f"PROCs estatisticos ({', '.join(stat_found)}) - "
                          f"requer migracao para framework ML da plataforma alvo",
            })

        if prog.get("complexity_level") == "VERY_HIGH":
            interventions.append({
                "severity": "MEDIUM",
                "program": filename,
                "reason": f"Complexidade MUITO ALTA (score={prog.get('complexity_score', 0)}) - "
                          f"revisao manual recomendada",
            })

    def _get_data_migrator(self):
        if self.platform == "databricks":
            from src.migration.data_migrator.databricks_migrator import DatabricksMigrator
            return DatabricksMigrator(self.config)
        else:
            from src.migration.data_migrator.snowflake_migrator import SnowflakeMigrator
            return SnowflakeMigrator(self.config)

    def _get_transpiler(self):
        if self.platform == "databricks":
            from src.migration.code_transpiler.databricks_transpiler import DatabricksTranspiler
            return DatabricksTranspiler(self.config)
        else:
            from src.migration.code_transpiler.snowflake_transpiler import SnowflakeTranspiler
            return SnowflakeTranspiler(self.config)

    def _generate_validations(self, inventory, output_dir):
        from src.validation.validator import MigrationValidator
        validator = MigrationValidator()
        val_dir = os.path.join(output_dir, "validation")
        os.makedirs(val_dir, exist_ok=True)

        if self.platform == "databricks":
            target_config = {
                "database": self.config.get("target", {}).get("catalog", "sas_migration"),
                "schema": self.config.get("target", {}).get("schema_bronze", "bronze_sas"),
            }
        else:
            target_config = {
                "database": self.config.get("target", {}).get("database", "SAS_MIGRATION"),
                "schema": "RAW",
            }

        for ds in inventory.get("datasets", []):
            scripts = validator.generate_validation_scripts(ds, target_config)
            val_path = os.path.join(val_dir, f"validate_{ds.get('dataset_name', 'unknown')}.sql")
            with open(val_path, "w", encoding="utf-8") as f:
                for key, sql in scripts.items():
                    if key not in ("dataset", "target_table"):
                        f.write(f"\n-- {key}\n{sql}\n")

    def _run_validation_only(self, inventory: Dict, output_dir: str) -> Dict:
        self._generate_validations(inventory, output_dir)
        return {"validation_dir": os.path.join(output_dir, "validation")}
