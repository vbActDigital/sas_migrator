"""
Creates a synthetic SAS environment for testing the migration toolkit.
Generates: 8 SAS programs, 5 datasets (dummy .sas7bdat + .meta.json), config YAML.
"""
import os
import json
import sys

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
MOCK_DIR = os.path.join(BASE_DIR, "mock_sas_environment")
PROGRAMS_DIR = os.path.join(MOCK_DIR, "programs")
MACROS_DIR = os.path.join(MOCK_DIR, "macros")
DATA_DIR = os.path.join(MOCK_DIR, "data")
BACKUP_DIR = os.path.join(PROGRAMS_DIR, "backup")
CONFIG_DIR = os.path.join(BASE_DIR, "..")


def create_directories():
    for d in [PROGRAMS_DIR, MACROS_DIR, DATA_DIR, BACKUP_DIR]:
        os.makedirs(d, exist_ok=True)
    print("[OK] Directories created")


def create_sas_programs():
    programs = {
        os.path.join(PROGRAMS_DIR, "etl_load_customers.sas"): '''\
/* ETL: Load and transform customer data */
LIBNAME rawdata '/sas/data/raw';
LIBNAME dw '/sas/data/dw';

%MACRO log_step(step_name);
  %PUT NOTE: Step &step_name started at %SYSFUNC(datetime(), datetime20.);
%MEND log_step;

%log_step(load_customers);

/* Dedup customers */
PROC SORT DATA=rawdata.customers_raw OUT=work.customers_sorted NODUPKEY;
  BY customer_id;
RUN;

/* Remove duplicates using FIRST. */
DATA work.customers_dedup;
  SET work.customers_sorted;
  BY customer_id;
  IF FIRST.customer_id;
RUN;

/* Merge with addresses */
DATA work.customers_with_addr;
  MERGE work.customers_dedup (IN=a)
        rawdata.addresses (IN=b);
  BY customer_id;
  IF a;
RUN;

/* Create dim_customer */
PROC SQL;
  CREATE TABLE dw.dim_customer AS
  SELECT
    c.customer_id,
    c.customer_name,
    c.email,
    c.cpf,
    c.phone,
    c.birth_date,
    c.gender,
    c.income,
    c.segment,
    c.risk_score,
    a.city,
    a.state,
    a.zip_code,
    c.registration_date,
    c.status
  FROM work.customers_dedup c
  LEFT JOIN rawdata.addresses a ON c.customer_id = a.customer_id;
QUIT;

PROC FREQ DATA=dw.dim_customer;
  TABLES segment * status / NOCUM;
RUN;

PROC MEANS DATA=dw.dim_customer N MEAN STD MIN MAX;
  VAR income risk_score;
RUN;

%log_step(load_customers_done);
''',
        os.path.join(PROGRAMS_DIR, "etl_load_policies.sas"): '''\
/* ETL: Load policies and create fact table */
LIBNAME rawdata '/sas/data/raw';
LIBNAME dw '/sas/data/dw';

PROC SQL;
  CREATE TABLE work.policies_enriched AS
  SELECT
    p.policy_id,
    p.customer_id,
    p.product_code,
    p.premium_amount,
    p.start_date,
    p.end_date,
    p.status,
    c.customer_name,
    c.segment
  FROM rawdata.policies p
  INNER JOIN dw.dim_customer c ON p.customer_id = c.customer_id;
QUIT;

DATA dw.dim_policy;
  SET work.policies_enriched;
RUN;

/* Create customer-policy summary */
DATA dw.fact_customer_policies;
  MERGE work.policies_enriched (IN=a)
        rawdata.products (IN=b);
  BY product_code;
  IF a;
  total_premium = premium_amount;
RUN;

PROC SORT DATA=dw.fact_customer_policies;
  BY customer_id product_code;
RUN;
''',
        os.path.join(PROGRAMS_DIR, "etl_claims_processing.sas"): '''\
/* ETL: Claims processing with hash lookup */
LIBNAME rawdata '/sas/data/raw';
LIBNAME dw '/sas/data/dw';

DATA dw.fact_claims;
  LENGTH policy_id 8 claim_id 8;

  /* Hash object for policy lookup */
  IF _N_ = 1 THEN DO;
    DECLARE HASH h_policies(dataset: 'rawdata.policies');
    h_policies.defineKey('policy_id');
    h_policies.defineData('customer_id', 'product_code', 'premium_amount');
    h_policies.defineDone();
  END;

  SET rawdata.claims;

  rc = h_policies.find();
  IF rc = 0 THEN DO;
    claim_to_premium_ratio = claim_amount / premium_amount;
    output;
  END;
RUN;

PROC MEANS DATA=dw.fact_claims N MEAN STD MIN MAX;
  VAR claim_amount claim_to_premium_ratio;
  CLASS claim_type;
RUN;

PROC FREQ DATA=dw.fact_claims;
  TABLES claim_type * status / NOCUM NOPERCENT;
RUN;
''',
        os.path.join(PROGRAMS_DIR, "etl_dynamic_loader.sas"): '''\
/* Dynamic loader: generates DATA steps at runtime */
LIBNAME rawdata '/sas/data/raw';
LIBNAME dw '/sas/data/dw';

/* Get list of tables to process */
PROC SQL NOPRINT;
  SELECT DISTINCT memname
  INTO :table_list SEPARATED BY ' '
  FROM dictionary.tables
  WHERE libname = 'RAWDATA';
QUIT;

/* Dynamic DATA step generation */
%MACRO dynamic_load;
  %LET i = 1;
  %DO %WHILE(%SCAN(&table_list, &i, ' ') NE );
    %LET tbl = %SCAN(&table_list, &i, ' ');

    CALL EXECUTE(
      "DATA dw.&tbl; SET rawdata.&tbl; RUN;"
    );

    %LET i = %EVAL(&i + 1);
  %END;
%MEND dynamic_load;

%dynamic_load;

PROC SQL;
  CREATE TABLE work.load_summary AS
  SELECT memname, nobs
  FROM dictionary.tables
  WHERE libname = 'DW';
QUIT;
''',
        os.path.join(PROGRAMS_DIR, "risk_model_scoring.sas"): '''\
/* Risk Model Scoring across segments */
LIBNAME dw '/sas/data/dw';
LIBNAME output '/sas/data/output';

%MACRO score_segment(segment);
  /* Filter segment */
  DATA work.seg_&segment;
    SET dw.dim_customer;
    WHERE segment = "&segment";
  RUN;

  /* Univariate analysis */
  PROC UNIVARIATE DATA=work.seg_&segment;
    VAR income risk_score;
  RUN;

  /* Logistic regression */
  PROC LOGISTIC DATA=work.seg_&segment;
    MODEL status(EVENT='ACTIVE') = income risk_score / SELECTION=STEPWISE;
    OUTPUT OUT=work.scored_&segment PREDICTED=pred_prob;
  RUN;

  /* Frequency analysis */
  PROC FREQ DATA=work.scored_&segment;
    TABLES status / NOCUM;
  RUN;
%MEND score_segment;

%score_segment(RETAIL);
%score_segment(CORPORATE);
%score_segment(SME);

/* Combine all scored segments */
DATA output.risk_scores;
  SET work.scored_RETAIL
      work.scored_CORPORATE
      work.scored_SME;
RUN;

PROC MEANS DATA=output.risk_scores N MEAN STD;
  VAR pred_prob;
  CLASS segment;
RUN;
''',
        os.path.join(PROGRAMS_DIR, "report_monthly_kpi.sas"): '''\
/* Monthly KPI Report */
LIBNAME dw '/sas/data/dw';

PROC SQL;
  CREATE TABLE work.monthly_kpi AS
  SELECT
    YEAR(start_date) AS yr,
    MONTH(start_date) AS mo,
    COUNT(DISTINCT customer_id) AS unique_customers,
    COUNT(*) AS total_policies,
    SUM(premium_amount) AS total_premium,
    AVG(premium_amount) AS avg_premium
  FROM dw.fact_customer_policies
  GROUP BY CALCULATED yr, CALCULATED mo;
QUIT;

PROC TABULATE DATA=work.monthly_kpi;
  CLASS yr mo;
  VAR total_policies total_premium avg_premium;
  TABLE yr * mo, total_policies * SUM total_premium * SUM avg_premium * MEAN;
RUN;

PROC REPORT DATA=work.monthly_kpi NOWD;
  COLUMNS yr mo unique_customers total_policies total_premium avg_premium;
  DEFINE yr / GROUP 'Year';
  DEFINE mo / GROUP 'Month';
  DEFINE unique_customers / ANALYSIS SUM 'Customers';
  DEFINE total_policies / ANALYSIS SUM 'Policies';
  DEFINE total_premium / ANALYSIS SUM FORMAT=DOLLAR12.2 'Total Premium';
  DEFINE avg_premium / ANALYSIS MEAN FORMAT=DOLLAR12.2 'Avg Premium';
RUN;
''',
        os.path.join(MACROS_DIR, "log_utils.sas"): '''\
/* Utility macros for logging and validation */

%MACRO log_step(step_name);
  %PUT NOTE: ====================================;
  %PUT NOTE: Step: &step_name;
  %PUT NOTE: Time: %SYSFUNC(datetime(), datetime20.);
  %PUT NOTE: ====================================;
%MEND log_step;

%MACRO log_rowcount(dataset);
  PROC SQL NOPRINT;
    SELECT COUNT(*) INTO :nrows FROM &dataset;
  QUIT;
  %PUT NOTE: Dataset &dataset has &nrows rows;
%MEND log_rowcount;

%MACRO check_exists(dataset);
  %IF %SYSFUNC(EXIST(&dataset)) %THEN %DO;
    %PUT NOTE: Dataset &dataset exists;
  %END;
  %ELSE %DO;
    %PUT WARNING: Dataset &dataset does NOT exist;
    CALL EXECUTE('%ABORT CANCEL;');
  %END;
%MEND check_exists;
''',
        os.path.join(BACKUP_DIR, "old_etl.sas"): '''\
/* Old ETL - should be excluded by scanner */
DATA work.old_customers;
  SET rawdata.customers_raw;
RUN;
''',
    }

    for path, content in programs.items():
        with open(path, "w", encoding="utf-8") as f:
            f.write(content)
    print(f"[OK] {len(programs)} SAS programs created")


def create_datasets():
    datasets = {
        "customers_raw": {
            "dataset_name": "customers_raw",
            "row_count": 1000,
            "column_count": 16,
            "size_bytes": 256000,
            "columns": [
                {"name": "customer_id", "type": "num", "length": 8, "format": "", "label": "Customer ID"},
                {"name": "customer_name", "type": "char", "length": 50, "format": "", "label": "Customer Name"},
                {"name": "email", "type": "char", "length": 80, "format": "", "label": "Email Address"},
                {"name": "cpf", "type": "char", "length": 14, "format": "", "label": "CPF"},
                {"name": "phone", "type": "char", "length": 20, "format": "", "label": "Phone Number"},
                {"name": "birth_date", "type": "num", "length": 8, "format": "DATE9.", "label": "Birth Date"},
                {"name": "gender", "type": "char", "length": 1, "format": "", "label": "Gender"},
                {"name": "income", "type": "num", "length": 8, "format": "DOLLAR12.2", "label": "Monthly Income"},
                {"name": "segment", "type": "char", "length": 20, "format": "", "label": "Customer Segment"},
                {"name": "risk_score", "type": "num", "length": 8, "format": "8.2", "label": "Risk Score"},
                {"name": "registration_date", "type": "num", "length": 8, "format": "DATE9.", "label": "Registration Date"},
                {"name": "status", "type": "char", "length": 10, "format": "", "label": "Status"},
                {"name": "city", "type": "char", "length": 40, "format": "", "label": "City"},
                {"name": "state", "type": "char", "length": 2, "format": "", "label": "State"},
                {"name": "zip_code", "type": "char", "length": 10, "format": "", "label": "ZIP Code"},
                {"name": "last_update", "type": "num", "length": 8, "format": "DATETIME20.", "label": "Last Update"},
            ],
        },
        "policies": {
            "dataset_name": "policies",
            "row_count": 2000,
            "column_count": 7,
            "size_bytes": 160000,
            "columns": [
                {"name": "policy_id", "type": "num", "length": 8, "format": "", "label": "Policy ID"},
                {"name": "customer_id", "type": "num", "length": 8, "format": "", "label": "Customer ID"},
                {"name": "product_code", "type": "char", "length": 10, "format": "", "label": "Product Code"},
                {"name": "premium_amount", "type": "num", "length": 8, "format": "DOLLAR12.2", "label": "Premium Amount"},
                {"name": "start_date", "type": "num", "length": 8, "format": "DATE9.", "label": "Start Date"},
                {"name": "end_date", "type": "num", "length": 8, "format": "DATE9.", "label": "End Date"},
                {"name": "status", "type": "char", "length": 10, "format": "", "label": "Status"},
            ],
        },
        "claims": {
            "dataset_name": "claims",
            "row_count": 500,
            "column_count": 6,
            "size_bytes": 48000,
            "columns": [
                {"name": "claim_id", "type": "num", "length": 8, "format": "", "label": "Claim ID"},
                {"name": "policy_id", "type": "num", "length": 8, "format": "", "label": "Policy ID"},
                {"name": "claim_amount", "type": "num", "length": 8, "format": "DOLLAR12.2", "label": "Claim Amount"},
                {"name": "claim_date", "type": "num", "length": 8, "format": "DATE9.", "label": "Claim Date"},
                {"name": "claim_type", "type": "char", "length": 20, "format": "", "label": "Claim Type"},
                {"name": "status", "type": "char", "length": 10, "format": "", "label": "Status"},
            ],
        },
        "addresses": {
            "dataset_name": "addresses",
            "row_count": 800,
            "column_count": 5,
            "size_bytes": 64000,
            "columns": [
                {"name": "customer_id", "type": "num", "length": 8, "format": "", "label": "Customer ID"},
                {"name": "street", "type": "char", "length": 100, "format": "", "label": "Street"},
                {"name": "city", "type": "char", "length": 40, "format": "", "label": "City"},
                {"name": "state", "type": "char", "length": 2, "format": "", "label": "State"},
                {"name": "zip_code", "type": "char", "length": 10, "format": "", "label": "ZIP Code"},
            ],
        },
        "products": {
            "dataset_name": "products",
            "row_count": 5,
            "column_count": 3,
            "size_bytes": 1024,
            "columns": [
                {"name": "product_code", "type": "char", "length": 10, "format": "", "label": "Product Code"},
                {"name": "product_name", "type": "char", "length": 50, "format": "", "label": "Product Name"},
                {"name": "product_category", "type": "char", "length": 30, "format": "", "label": "Product Category"},
            ],
        },
    }

    for name, meta in datasets.items():
        # Create dummy .sas7bdat (proportional size)
        sas_path = os.path.join(DATA_DIR, f"{name}.sas7bdat")
        with open(sas_path, "wb") as f:
            f.write(b'\x00' * min(meta["size_bytes"], 4096))

        # Create .meta.json with full metadata
        meta_path = os.path.join(DATA_DIR, f"{name}.sas7bdat.meta.json")
        with open(meta_path, "w", encoding="utf-8") as f:
            json.dump(meta, f, indent=2)

    # Try to create .xpt files with pyreadstat if available
    try:
        import pandas as pd
        import numpy as np
        import pyreadstat

        for name, meta in datasets.items():
            n = min(meta["row_count"], 50)  # small sample for .xpt
            data = {}
            for col in meta["columns"]:
                if col["type"] == "num":
                    data[col["name"][:8]] = np.random.randint(1, 1000, n).astype(float)
                else:
                    data[col["name"][:8]] = [f"val_{i}" for i in range(n)]
            df = pd.DataFrame(data)
            xpt_path = os.path.join(DATA_DIR, f"{name}.xpt")
            pyreadstat.write_xport(df, xpt_path)
        print("[OK] .xpt files created with pyreadstat")
    except Exception as e:
        print(f"[WARN] Could not create .xpt files: {e}")

    print(f"[OK] {len(datasets)} datasets created (.sas7bdat + .meta.json)")


def create_test_config():
    config = {
        "project": {
            "name": "MVP1 Mock Test",
            "client": "MAPFRE",
        },
        "sas_environment": {
            "code_paths": [
                os.path.join(MOCK_DIR, "programs").replace("\\", "/"),
                os.path.join(MOCK_DIR, "macros").replace("\\", "/"),
            ],
            "data_paths": [
                os.path.join(MOCK_DIR, "data").replace("\\", "/"),
            ],
            "log_paths": [],
            "exclude_patterns": ["backup"],
        },
        "target": {
            "platform": "snowflake",
        },
        "library_mapping": {
            "rawdata": {"database": "SAS_MIGRATION", "schema": "RAW"},
            "dw": {"database": "SAS_MIGRATION", "schema": "REFINED"},
        },
        "catalog": {
            "output_format": ["json", "markdown"],
            "detect_pii": True,
            "infer_domains": True,
        },
    }

    config_path = os.path.join(BASE_DIR, "mock_config.yaml")
    try:
        import yaml
        with open(config_path, "w", encoding="utf-8") as f:
            yaml.dump(config, f, default_flow_style=False, sort_keys=False)
    except ImportError:
        # Fallback: write YAML manually
        with open(config_path, "w", encoding="utf-8") as f:
            f.write(f"""project:
  name: "MVP1 Mock Test"
  client: "MAPFRE"

sas_environment:
  code_paths:
    - {os.path.join(MOCK_DIR, 'programs').replace(chr(92), '/')}
    - {os.path.join(MOCK_DIR, 'macros').replace(chr(92), '/')}
  data_paths:
    - {os.path.join(MOCK_DIR, 'data').replace(chr(92), '/')}
  log_paths: []
  exclude_patterns:
    - backup

target:
  platform: snowflake

library_mapping:
  rawdata:
    database: SAS_MIGRATION
    schema: RAW
  dw:
    database: SAS_MIGRATION
    schema: REFINED

catalog:
  output_format:
    - json
    - markdown
  detect_pii: true
  infer_domains: true
""")
    print(f"[OK] Test config created: {config_path}")
    return config_path


def create_fixture_programs():
    """Also copy key programs to fixtures for unit tests."""
    fixtures_dir = os.path.join(BASE_DIR, "fixtures", "sample_programs")
    os.makedirs(fixtures_dir, exist_ok=True)

    # Copy from programs dir
    import shutil
    for name in ["etl_load_customers.sas", "risk_model_scoring.sas"]:
        src = os.path.join(PROGRAMS_DIR, name)
        dst = os.path.join(fixtures_dir, name)
        if os.path.exists(src):
            shutil.copy2(src, dst)
    print("[OK] Fixture programs copied")


if __name__ == "__main__":
    print("=" * 60)
    print("Creating Mock SAS Environment")
    print("=" * 60)

    create_directories()
    create_sas_programs()
    create_datasets()
    config_path = create_test_config()
    create_fixture_programs()

    print()
    print("=" * 60)
    print("Mock environment ready!")
    print(f"  Programs: {PROGRAMS_DIR}")
    print(f"  Macros:   {MACROS_DIR}")
    print(f"  Data:     {DATA_DIR}")
    print(f"  Config:   {config_path}")
    print("=" * 60)
