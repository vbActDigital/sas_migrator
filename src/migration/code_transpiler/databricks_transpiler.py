import re
from typing import Dict, List

from src.utils.logger import get_logger

logger = get_logger("databricks_transpiler")

GAP_PROCS = {"LOGISTIC", "REG", "GLM", "MIXED", "IML", "REPORT", "TABULATE"}


class DatabricksTranspiler:
    """Transpiles SAS code to PySpark / Databricks SQL."""

    def __init__(self, config: Dict):
        self.config = config
        self.library_mapping = config.get("library_mapping", {})
        target = config.get("target", {})
        self.catalog = target.get("catalog", "sas_migration")

    def transpile(self, parsed_program: Dict) -> Dict:
        sql_parts = []
        pyspark_parts = []
        gaps = []
        warnings = []
        total_patterns = 0
        covered_patterns = 0

        # LIBNAME -> USE CATALOG/SCHEMA
        for lib in parsed_program.get("libnames", []):
            total_patterns += 1
            mapping = self.library_mapping.get(lib["name"].lower(), {})
            if mapping:
                catalog = mapping.get("catalog", self.catalog)
                schema = mapping.get("schema", "")
                sql_parts.append(f"USE CATALOG {catalog};")
                sql_parts.append(f"USE SCHEMA {schema};")
                pyspark_parts.append(
                    f'spark.sql("USE CATALOG {catalog}")\n'
                    f'spark.sql("USE SCHEMA {schema}")'
                )
                covered_patterns += 1
            else:
                sql_parts.append(f"-- LIBNAME {lib['name']} -> no mapping found")
                warnings.append(f"No library mapping for {lib['name']}")

        # DATA step SET -> CREATE TABLE / PySpark DataFrame
        for ds in parsed_program.get("data_steps", []):
            total_patterns += 1
            reads = parsed_program.get("datasets_read", [])
            if reads:
                source = reads[0]
                sql_parts.append(
                    f"CREATE OR REPLACE TABLE {ds}\n"
                    f"USING DELTA\n"
                    f"AS SELECT * FROM {source};"
                )
                pyspark_parts.append(
                    f'df_{ds} = spark.table("{source}")\n'
                    f'df_{ds}.write.format("delta").mode("overwrite")'
                    f'.saveAsTable("{ds}")'
                )
                covered_patterns += 1

        # MERGE -> JOIN (PySpark)
        merges = parsed_program.get("merge_statements", [])
        if merges and len(merges) >= 2:
            total_patterns += 1
            t1, t2 = merges[0], merges[1]
            sql_parts.append(
                f"-- MERGE transpiled to JOIN\n"
                f"CREATE OR REPLACE TABLE merged_{t1}_{t2}\n"
                f"USING DELTA AS\n"
                f"SELECT a.*, b.*\n"
                f"FROM {t1} a\n"
                f"LEFT JOIN {t2} b ON a.id = b.id;  -- Review join key"
            )
            pyspark_parts.append(
                f'df_{t1} = spark.table("{t1}")\n'
                f'df_{t2} = spark.table("{t2}")\n'
                f'df_merged = df_{t1}.join(df_{t2}, on="id", how="left")  '
                f'# Review join key\n'
                f'df_merged.write.format("delta").mode("overwrite")'
                f'.saveAsTable("merged_{t1}_{t2}")'
            )
            covered_patterns += 1

        # PROCs
        for proc in parsed_program.get("procs_used", []):
            total_patterns += 1
            proc_upper = proc.upper()

            if proc_upper == "SORT":
                sql_parts.append(
                    "-- PROC SORT NODUPKEY equivalent\n"
                    "CREATE OR REPLACE TABLE sorted_output\n"
                    "USING DELTA AS\n"
                    "SELECT * FROM (\n"
                    "  SELECT *, ROW_NUMBER() OVER "
                    "(PARTITION BY key_col ORDER BY key_col) AS rn\n"
                    "  FROM input_table\n"
                    ") WHERE rn = 1;"
                )
                pyspark_parts.append(
                    'from pyspark.sql import Window\n'
                    'from pyspark.sql.functions import row_number\n\n'
                    'w = Window.partitionBy("key_col").orderBy("key_col")\n'
                    'df_sorted = df.withColumn("rn", row_number().over(w))'
                    '.filter("rn = 1").drop("rn")'
                )
                covered_patterns += 1

            elif proc_upper == "SQL":
                sql_parts.append(
                    "-- PROC SQL: Databricks SQL is largely compatible"
                )
                covered_patterns += 1

            elif proc_upper == "FREQ":
                sql_parts.append(
                    "-- PROC FREQ equivalent\n"
                    "SELECT column_name, COUNT(*) AS frequency,\n"
                    "       COUNT(*) * 100.0 / SUM(COUNT(*)) "
                    "OVER () AS pct\n"
                    "FROM input_table\n"
                    "GROUP BY column_name\n"
                    "ORDER BY frequency DESC;"
                )
                pyspark_parts.append(
                    'from pyspark.sql.functions import count, col\n\n'
                    'df_freq = df.groupBy("column_name")'
                    '.agg(count("*").alias("frequency"))\n'
                    'df_freq = df_freq.orderBy(col("frequency").desc())'
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
                pyspark_parts.append(
                    'from pyspark.sql.functions import count, avg, '
                    'stddev, min as min_, max as max_\n\n'
                    'df.select(\n'
                    '    count("*").alias("n"),\n'
                    '    avg("numeric_col").alias("mean"),\n'
                    '    stddev("numeric_col").alias("std"),\n'
                    '    min_("numeric_col").alias("min"),\n'
                    '    max_("numeric_col").alias("max")\n'
                    ').show()'
                )
                covered_patterns += 1

            elif proc_upper == "FORMAT":
                sql_parts.append(
                    "-- PROC FORMAT equivalent: lookup table + LEFT JOIN\n"
                    "CREATE OR REPLACE TABLE format_lookup (\n"
                    "  code STRING,\n"
                    "  label STRING\n"
                    ") USING DELTA;\n"
                    "-- Then: SELECT t.*, f.label FROM table t "
                    "LEFT JOIN format_lookup f ON t.code = f.code;"
                )
                covered_patterns += 1

            elif proc_upper == "UNIVARIATE":
                sql_parts.append(
                    "-- PROC UNIVARIATE equivalent\n"
                    "SELECT\n"
                    "  COUNT(*) AS n, AVG(col) AS mean, "
                    "MEDIAN(col) AS median,\n"
                    "  STDDEV(col) AS std, MIN(col) AS min, "
                    "MAX(col) AS max,\n"
                    "  PERCENTILE(col, 0.25) AS p25,\n"
                    "  PERCENTILE(col, 0.75) AS p75\n"
                    "FROM input_table;"
                )
                pyspark_parts.append(
                    'df.describe().show()\n'
                    'df.approxQuantile("col", [0.25, 0.5, 0.75], 0.01)'
                )
                covered_patterns += 1

            elif proc_upper in GAP_PROCS:
                gaps.append(
                    f"PROC {proc_upper} - consider Databricks ML Runtime, "
                    f"MLflow, or Spark MLlib"
                )

            else:
                warnings.append(
                    f"PROC {proc_upper} - not explicitly handled"
                )

        # Macros -> Databricks notebook widgets / functions
        for macro in parsed_program.get("macro_definitions", []):
            total_patterns += 1
            sql_parts.append(
                f"-- %MACRO {macro} -> Databricks SQL function\n"
                f"CREATE OR REPLACE FUNCTION {macro}()\n"
                f"RETURNS STRING\n"
                f"RETURN 'OK';  -- TODO: Implement {macro} logic"
            )
            pyspark_parts.append(
                f'def {macro}():\n'
                f'    """Converted from SAS %MACRO {macro}"""\n'
                f'    # TODO: Implement {macro} logic\n'
                f'    pass'
            )
            covered_patterns += 1

        # Hash objects -> gap
        if parsed_program.get("has_hash_objects"):
            total_patterns += 1
            gaps.append(
                "Hash objects - use PySpark broadcast joins or "
                "Delta Lake temporary tables"
            )

        # Dynamic SQL -> gap
        if parsed_program.get("has_dynamic_sql"):
            total_patterns += 1
            gaps.append(
                "CALL EXECUTE / dynamic SQL - use Databricks "
                "Workflows or notebook widgets"
            )

        coverage = (
            (covered_patterns / total_patterns * 100)
            if total_patterns > 0 else 100.0
        )

        return {
            "sql_code": "\n\n".join(sql_parts),
            "pyspark_code": (
                "\n\n".join(pyspark_parts)
                if pyspark_parts
                else "# No PySpark code generated"
            ),
            "gaps": gaps,
            "warnings": warnings,
            "coverage_pct": round(coverage, 1),
        }
