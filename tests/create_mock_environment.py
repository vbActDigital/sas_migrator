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
        "pendente_analitico": {
            "dataset_name": "pendente_analitico",
            "row_count": 150000,
            "column_count": 45,
            "size_bytes": 52500000,
            "description": "Base analitica de premios pendentes - importada de arquivo texto delimitado",
            "columns": [
                {"name": "ano_mes", "type": "num", "length": 8, "format": "BEST6.", "label": "Ano/Mes Referencia"},
                {"name": "cod_seguradora_susep", "type": "num", "length": 8, "format": "BEST5.", "label": "Codigo Seguradora SUSEP"},
                {"name": "cod_apolice", "type": "num", "length": 8, "format": "BEST20.", "label": "Codigo Apolice"},
                {"name": "num_endosso", "type": "char", "length": 10, "format": "$CHAR10.", "label": "Numero Endosso"},
                {"name": "num_certificado", "type": "char", "length": 15, "format": "$CHAR15.", "label": "Numero Certificado"},
                {"name": "cod_produto", "type": "num", "length": 8, "format": "BEST5.", "label": "Codigo Produto"},
                {"name": "cod_agencia", "type": "num", "length": 8, "format": "BEST5.", "label": "Codigo Agencia"},
                {"name": "cod_tp_emissao", "type": "char", "length": 1, "format": "$CHAR1.", "label": "Tipo Emissao (D=Direto, A=Aceito)"},
                {"name": "cod_moeda", "type": "num", "length": 8, "format": "BEST3.", "label": "Codigo Moeda"},
                {"name": "dt_inicio_vigencia", "type": "num", "length": 8, "format": "DDMMYY10.", "label": "Data Inicio Vigencia"},
                {"name": "dt_fim_vigencia", "type": "num", "length": 8, "format": "DDMMYY10.", "label": "Data Fim Vigencia"},
                {"name": "dt_emissao_doc", "type": "num", "length": 8, "format": "DDMMYY10.", "label": "Data Emissao Documento"},
                {"name": "qtde_parcelas", "type": "num", "length": 8, "format": "BEST3.", "label": "Quantidade Parcelas"},
                {"name": "dt_vencimento", "type": "num", "length": 8, "format": "DDMMYY10.", "label": "Data Vencimento"},
                {"name": "dt_inicio_cobertura_parcela", "type": "num", "length": 8, "format": "DDMMYY10.", "label": "Inicio Cobertura Parcela"},
                {"name": "dt_fim_cobertura_parcela", "type": "num", "length": 8, "format": "DDMMYY10.", "label": "Fim Cobertura Parcela"},
                {"name": "num_parcela", "type": "num", "length": 8, "format": "BEST3.", "label": "Numero Parcela"},
                {"name": "grupo_ramo_contabil", "type": "num", "length": 8, "format": "BEST2.", "label": "Grupo Ramo Contabil"},
                {"name": "cod_ramo_contabil", "type": "num", "length": 8, "format": "BEST2.", "label": "Codigo Ramo Contabil"},
                {"name": "cod_ramo_emitido", "type": "num", "length": 8, "format": "BEST2.", "label": "Codigo Ramo Emitido"},
                {"name": "val_cobranca", "type": "num", "length": 8, "format": "COMMAX20.2", "label": "Valor Cobranca"},
                {"name": "val_iof", "type": "num", "length": 8, "format": "COMMAX20.2", "label": "Valor IOF"},
                {"name": "val_custo_apolice", "type": "num", "length": 8, "format": "COMMAX20.2", "label": "Custo Apolice"},
                {"name": "val_desconto", "type": "num", "length": 8, "format": "COMMAX20.2", "label": "Valor Desconto"},
                {"name": "val_adic_fracionamento", "type": "num", "length": 8, "format": "COMMAX20.2", "label": "Adicional Fracionamento"},
                {"name": "val_comissao", "type": "num", "length": 8, "format": "COMMAX20.2", "label": "Valor Comissao"},
                {"name": "val_estipulante", "type": "num", "length": 8, "format": "COMMAX20.2", "label": "Valor Estipulante"},
                {"name": "val_cobranca_cosseguro", "type": "num", "length": 8, "format": "COMMAX20.2", "label": "Cobranca Cosseguro"},
                {"name": "val_comissao_cosseguro", "type": "num", "length": 8, "format": "COMMAX20.2", "label": "Comissao Cosseguro"},
                {"name": "val_cobranca_resseguro", "type": "num", "length": 8, "format": "COMMAX20.2", "label": "Cobranca Resseguro"},
                {"name": "val_comissao_resseguro", "type": "num", "length": 8, "format": "COMMAX20.2", "label": "Comissao Resseguro"},
                {"name": "val_direito_creditorio", "type": "num", "length": 8, "format": "COMMAX20.2", "label": "Direito Creditorio"},
                {"name": "val_comissao_agenciamento", "type": "num", "length": 8, "format": "COMMAX20.2", "label": "Comissao Agenciamento"},
                {"name": "val_remuneracao_representante", "type": "num", "length": 8, "format": "COMMAX20.2", "label": "Remuneracao Representante"},
                {"name": "dt_inicio_vigencia_ori", "type": "num", "length": 8, "format": "DDMMYY10.", "label": "Inicio Vigencia Original"},
                {"name": "dt_fim_vigencia_ori", "type": "num", "length": 8, "format": "DDMMYY10.", "label": "Fim Vigencia Original"},
                {"name": "cod_sistema_origem", "type": "char", "length": 14, "format": "$CHAR14.", "label": "Sistema Origem"},
                {"name": "cpf_cnpj_segurado", "type": "num", "length": 8, "format": "BEST14.", "label": "CPF/CNPJ Segurado"},
                {"name": "num_proposta", "type": "num", "length": 8, "format": "BEST20.", "label": "Numero Proposta"},
                {"name": "IDLG", "type": "char", "length": 50, "format": "$CHAR50.", "label": "ID Legado"},
                {"name": "numero_externo", "type": "char", "length": 40, "format": "$CHAR40.", "label": "Numero Externo"},
                {"name": "val_comissao_VC", "type": "num", "length": 8, "format": "COMMAX20.2", "label": "Comissao VC"},
                {"name": "val_estipulante_VC", "type": "num", "length": 8, "format": "COMMAX20.2", "label": "Estipulante VC"},
                {"name": "val_cobranca_VC", "type": "num", "length": 8, "format": "COMMAX20.2", "label": "Cobranca VC"},
                {"name": "LETRA_SISTEMA_ORIGEM", "type": "char", "length": 4, "format": "$CHAR4.", "label": "Letra Sistema Origem"},
            ],
        },
        "percentual_resseguro": {
            "dataset_name": "percentual_resseguro",
            "row_count": 85,
            "column_count": 4,
            "size_bytes": 6800,
            "description": "Percentuais de cessao de resseguro por ramo e produto",
            "columns": [
                {"name": "COD_RAMO_CONTABIL", "type": "num", "length": 8, "format": "BEST2.", "label": "Codigo Ramo Contabil"},
                {"name": "COD_PRODUTO", "type": "num", "length": 8, "format": "BEST5.", "label": "Codigo Produto"},
                {"name": "Percen_Cessao_utilizado", "type": "num", "length": 8, "format": "COMMAX20.4", "label": "Percentual Cessao Utilizado"},
                {"name": "Percentual_SAS", "type": "num", "length": 8, "format": "COMMAX20.4", "label": "Percentual SAS"},
            ],
        },
        "grupo_rvr": {
            "dataset_name": "grupo_rvr",
            "row_count": 30,
            "column_count": 3,
            "size_bytes": 2400,
            "description": "Grupos de calculo dos fatores de RVR por ramo contabil",
            "columns": [
                {"name": "grupo_ramo_contabil", "type": "num", "length": 8, "format": "BEST2.", "label": "Grupo Ramo Contabil"},
                {"name": "cod_ramo_contabil", "type": "num", "length": 8, "format": "BEST2.", "label": "Codigo Ramo Contabil"},
                {"name": "Grupo_Calculo", "type": "char", "length": 20, "format": "", "label": "Grupo de Calculo RVR"},
            ],
        },
        "fatores_rvr": {
            "dataset_name": "fatores_rvr",
            "row_count": 500,
            "column_count": 9,
            "size_bytes": 36000,
            "description": "Fatores atuariais de RVR por grupo de calculo, aging e status",
            "columns": [
                {"name": "cod_seguradora_susep", "type": "num", "length": 8, "format": "BEST5.", "label": "Codigo Seguradora SUSEP"},
                {"name": "cod_tp_emissao", "type": "char", "length": 1, "format": "$CHAR1.", "label": "Tipo Emissao"},
                {"name": "Status_Vigencia", "type": "char", "length": 20, "format": "", "label": "Status Vigencia"},
                {"name": "Status_Cliente", "type": "char", "length": 20, "format": "", "label": "Status Cliente"},
                {"name": "Status_Apolice", "type": "char", "length": 20, "format": "", "label": "Status Apolice"},
                {"name": "Status_Parcela", "type": "char", "length": 20, "format": "", "label": "Status Parcela"},
                {"name": "Agging_meses", "type": "num", "length": 8, "format": "BEST3.", "label": "Aging em Meses"},
                {"name": "Grupo_Calculo", "type": "char", "length": 20, "format": "", "label": "Grupo de Calculo"},
                {"name": "FATOR_ATUARIAL", "type": "num", "length": 8, "format": "COMMAX20.18", "label": "Fator Atuarial RVR"},
            ],
        },
        "base_rvr_resultado": {
            "dataset_name": "base_rvr_resultado",
            "row_count": 150000,
            "column_count": 65,
            "size_bytes": 78000000,
            "description": "Resultado final do calculo de RVR com aging, condicoes e provisoes",
            "columns": [
                {"name": "ano_mes", "type": "num", "length": 8, "format": "BEST6.", "label": "Ano/Mes"},
                {"name": "cod_apolice", "type": "num", "length": 8, "format": "BEST20.", "label": "Codigo Apolice"},
                {"name": "cod_produto", "type": "num", "length": 8, "format": "BEST5.", "label": "Codigo Produto"},
                {"name": "cod_ramo_contabil", "type": "num", "length": 8, "format": "BEST2.", "label": "Ramo Contabil"},
                {"name": "cpf_cnpj_segurado", "type": "num", "length": 8, "format": "BEST14.", "label": "CPF/CNPJ Segurado"},
                {"name": "val_cobranca", "type": "num", "length": 8, "format": "COMMAX20.2", "label": "Valor Cobranca"},
                {"name": "val_comissao", "type": "num", "length": 8, "format": "COMMAX20.2", "label": "Valor Comissao"},
                {"name": "val_iof", "type": "num", "length": 8, "format": "COMMAX20.2", "label": "Valor IOF"},
                {"name": "base_ppng", "type": "num", "length": 8, "format": "COMMAX20.2", "label": "Base PPNG"},
                {"name": "dcd_base", "type": "num", "length": 8, "format": "COMMAX20.2", "label": "DCD Base"},
                {"name": "fator_decorrer", "type": "num", "length": 8, "format": "COMMAX20.18", "label": "Fator Decorrer"},
                {"name": "calc_ppng_dec", "type": "num", "length": 8, "format": "COMMAX20.2", "label": "PPNG a Decorrer"},
                {"name": "calc_dcd_dec", "type": "num", "length": 8, "format": "COMMAX20.2", "label": "DCD a Decorrer"},
                {"name": "premio_resseguro", "type": "num", "length": 8, "format": "COMMAX20.4", "label": "Premio Resseguro"},
                {"name": "ppng_resseguro", "type": "num", "length": 8, "format": "COMMAX20.2", "label": "PPNG Resseguro"},
                {"name": "RVR", "type": "num", "length": 8, "format": "COMMAX20.2", "label": "RVR Calculada"},
                {"name": "RVR_ATUARIAL", "type": "num", "length": 8, "format": "COMMAX20.2", "label": "RVR Atuarial"},
                {"name": "FATOR_ATUARIAL", "type": "num", "length": 8, "format": "COMMAX20.18", "label": "Fator Atuarial"},
                {"name": "CONDICOES", "type": "char", "length": 10, "format": "$10.", "label": "Condicao (1-5)"},
                {"name": "aging_dias_nome", "type": "char", "length": 25, "format": "$25.", "label": "Aging Dias Nome"},
                {"name": "venc_vinc", "type": "char", "length": 25, "format": "$25.", "label": "Vencido/A Vencer"},
                {"name": "decorrer_e_decorrido", "type": "char", "length": 25, "format": "$25.", "label": "Decorrer/Decorrido"},
                {"name": "INCONSIST", "type": "char", "length": 1, "format": "$1.", "label": "Flag Inconsistencia"},
                {"name": "DIAS", "type": "num", "length": 8, "format": "BEST6.", "label": "Dias ate Vencimento"},
                {"name": "DIAS_VIGENTE", "type": "num", "length": 8, "format": "COMMAX20.2", "label": "Dias Vigente"},
                {"name": "DIAS_DECORRER", "type": "num", "length": 8, "format": "COMMAX20.2", "label": "Dias a Decorrer"},
            ],
        },
        "products": {
            "dataset_name": "products",
            "row_count": 120,
            "column_count": 3,
            "size_bytes": 9600,
            "description": "Tabela de produtos de seguro (auto, vida, patrimonial, etc.)",
            "columns": [
                {"name": "cod_produto", "type": "num", "length": 8, "format": "BEST5.", "label": "Codigo Produto"},
                {"name": "nome_produto", "type": "char", "length": 50, "format": "", "label": "Nome Produto"},
                {"name": "categoria_produto", "type": "char", "length": 30, "format": "", "label": "Categoria Produto"},
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


def copy_real_programs():
    """Copy real MAPFRE SAS programs into mock environment if available."""
    import shutil
    real_dir = os.path.join(BASE_DIR, "real_sas_programs")
    if not os.path.exists(real_dir):
        print("[SKIP] No real_sas_programs directory found")
        return

    count = 0
    for fname in os.listdir(real_dir):
        if fname.endswith(".sas"):
            src = os.path.join(real_dir, fname)
            dst = os.path.join(PROGRAMS_DIR, fname)
            shutil.copy2(src, dst)
            count += 1
    print(f"[OK] {count} real SAS programs copied to mock environment")


def create_fixture_programs():
    """Also copy key programs to fixtures for unit tests."""
    fixtures_dir = os.path.join(BASE_DIR, "fixtures", "sample_programs")
    os.makedirs(fixtures_dir, exist_ok=True)

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
    copy_real_programs()
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
