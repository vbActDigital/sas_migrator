import re
from typing import Dict, List

from src.utils.logger import get_logger

logger = get_logger("snowflake_transpiler")

GAP_PROCS = {"LOGISTIC", "REG", "GLM", "MIXED", "IML", "REPORT", "TABULATE"}


class SnowflakeTranspiler:
    def __init__(self, config: Dict):
        self.config = config
        self.library_mapping = config.get("library_mapping", {})

    def transpile(self, parsed_program: Dict) -> Dict:
        sql_parts = []
        snowpark_parts = []
        gaps = []
        warnings = []
        total_patterns = 0
        covered_patterns = 0

        # LIBNAME -> USE SCHEMA
        for lib in parsed_program.get("libnames", []):
            total_patterns += 1
            mapping = self.library_mapping.get(lib["name"].lower(), {})
            if mapping:
                db = mapping.get("database", "")
                schema = mapping.get("schema", "")
                sql_parts.append(f"USE DATABASE {db};")
                sql_parts.append(f"USE SCHEMA {schema};")
                covered_patterns += 1
            else:
                sql_parts.append(f"-- LIBNAME {lib['name']} -> no mapping found")
                warnings.append(f"No library mapping for {lib['name']}")

        # DATA step SET -> CREATE TABLE AS SELECT
        for ds in parsed_program.get("data_steps", []):
            total_patterns += 1
            reads = parsed_program.get("datasets_read", [])
            if reads:
                source = reads[0]
                sql_parts.append(f"CREATE OR REPLACE TABLE {ds} AS\nSELECT *\nFROM {source};")
                snowpark_parts.append(
                    f'df_{ds} = session.table("{source}")\n'
                    f'df_{ds}.write.mode("overwrite").save_as_table("{ds}")'
                )
                covered_patterns += 1

        # MERGE -> JOIN
        merges = parsed_program.get("merge_statements", [])
        if merges and len(merges) >= 2:
            total_patterns += 1
            t1, t2 = merges[0], merges[1]
            sql_parts.append(
                f"-- MERGE transpiled to JOIN\n"
                f"CREATE OR REPLACE TABLE merged_{t1}_{t2} AS\n"
                f"SELECT a.*, b.*\n"
                f"FROM {t1} a\n"
                f"LEFT JOIN {t2} b ON a.id = b.id;  -- Review join key"
            )
            covered_patterns += 1

        # PROCs
        for proc in parsed_program.get("procs_used", []):
            total_patterns += 1
            proc_upper = proc.upper()

            if proc_upper == "SORT":
                sql_parts.append(
                    "-- PROC SORT NODUPKEY equivalent\n"
                    "CREATE OR REPLACE TABLE sorted_output AS\n"
                    "SELECT * FROM (\n"
                    "  SELECT *, ROW_NUMBER() OVER (PARTITION BY key_col ORDER BY key_col) AS rn\n"
                    "  FROM input_table\n"
                    ") WHERE rn = 1;"
                )
                covered_patterns += 1

            elif proc_upper == "SQL":
                sql_parts.append("-- PROC SQL: Snowflake SQL is largely compatible")
                covered_patterns += 1

            elif proc_upper == "FREQ":
                sql_parts.append(
                    "-- PROC FREQ equivalent\n"
                    "SELECT column_name, COUNT(*) AS frequency,\n"
                    "       COUNT(*) * 100.0 / SUM(COUNT(*)) OVER () AS pct\n"
                    "FROM input_table\n"
                    "GROUP BY column_name\n"
                    "ORDER BY frequency DESC;"
                )
                covered_patterns += 1

            elif proc_upper == "MEANS":
                sql_parts.append(
                    "-- PROC MEANS equivalent\n"
                    "SELECT\n"
                    "  COUNT(*) AS n,\n"
                    "  AVG(numeric_col) AS mean,\n"
                    "  STDDEV(numeric_col) AS std,\n"
                    "  MIN(numeric_col) AS min,\n"
                    "  MAX(numeric_col) AS max\n"
                    "FROM input_table;"
                )
                covered_patterns += 1

            elif proc_upper == "FORMAT":
                sql_parts.append(
                    "-- PROC FORMAT equivalent: lookup table + LEFT JOIN\n"
                    "CREATE OR REPLACE TABLE format_lookup (\n"
                    "  code VARCHAR(50),\n"
                    "  label VARCHAR(200)\n"
                    ");\n"
                    "-- Then: SELECT t.*, f.label FROM table t LEFT JOIN format_lookup f ON t.code = f.code;"
                )
                covered_patterns += 1

            elif proc_upper == "UNIVARIATE":
                sql_parts.append(
                    "-- PROC UNIVARIATE equivalent\n"
                    "SELECT\n"
                    "  COUNT(*) AS n, AVG(col) AS mean, MEDIAN(col) AS median,\n"
                    "  STDDEV(col) AS std, MIN(col) AS min, MAX(col) AS max,\n"
                    "  PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY col) AS p25,\n"
                    "  PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY col) AS p75\n"
                    "FROM input_table;"
                )
                covered_patterns += 1

            elif proc_upper in GAP_PROCS:
                gaps.append(f"PROC {proc_upper} - no direct Snowflake equivalent; consider Snowpark ML or external tool")

            else:
                warnings.append(f"PROC {proc_upper} - not explicitly handled")

        # Macros -> Stored Procedure stubs
        for macro in parsed_program.get("macro_definitions", []):
            total_patterns += 1
            sql_parts.append(
                f"-- %MACRO {macro} -> Stored Procedure stub\n"
                f"CREATE OR REPLACE PROCEDURE {macro}()\n"
                f"RETURNS VARCHAR\n"
                f"LANGUAGE SQL\n"
                f"AS\n"
                f"$$\n"
                f"  -- TODO: Implement {macro} logic\n"
                f"  RETURN 'OK';\n"
                f"$$;"
            )
            covered_patterns += 1

        # Hash objects -> gap
        if parsed_program.get("has_hash_objects"):
            total_patterns += 1
            gaps.append("Hash objects - consider Snowpark Python or temporary tables for lookups")

        # Dynamic SQL -> gap
        if parsed_program.get("has_dynamic_sql"):
            total_patterns += 1
            gaps.append("CALL EXECUTE / dynamic SQL - consider Snowflake Scripting or Snowpark")

        coverage = (covered_patterns / total_patterns * 100) if total_patterns > 0 else 100.0

        return {
            "sql_code": "\n\n".join(sql_parts),
            "snowpark_code": "\n\n".join(snowpark_parts) if snowpark_parts else "# No Snowpark code generated",
            "gaps": gaps,
            "warnings": warnings,
            "coverage_pct": round(coverage, 1),
        }
