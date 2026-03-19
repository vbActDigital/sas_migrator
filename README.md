# SAS Migration Toolkit

Toolkit de producao para analise e migracao de ambientes SAS para **Snowflake** ou **Databricks**. Aponte para um diretorio real com programas SAS (ou forneça dados de conexao ODBC/Metadata Server) e o toolkit executa o fluxo completo: analise, conversao automatica e identificacao do que precisa de intervencao manual.

## Fluxo de trabalho

```
                    ┌─────────────────────┐
                    │   Ambiente SAS       │
                    │  (folder / ODBC /    │
                    │   Metadata Server)   │
                    └─────────┬───────────┘
                              │
                    ┌─────────▼───────────┐
                    │  FASE 1: DISCOVERY   │
                    │  - Scan de programas │
                    │  - Parse de codigo   │
                    │  - Lineage graph     │
                    │  - Catalogo de dados │
                    │  - Relatorio PDF     │
                    └─────────┬───────────┘
                              │
                    ┌─────────▼───────────┐
                    │  FASE 2: MIGRACAO    │
                    │  - DDL (CREATE TABLE)│
                    │  - Data load scripts │
                    │  - Transpilacao SAS  │
                    │    → SQL + Python    │
                    │  - Validacao scripts │
                    └─────────┬───────────┘
                              │
               ┌──────────────┼──────────────┐
               │                             │
    ┌──────────▼──────────┐     ┌────────────▼────────────┐
    │  CONVERTIDO         │     │  INTERVENCAO MANUAL      │
    │  Artefatos prontos  │     │  - Hash objects           │
    │  para deploy        │     │  - SQL dinamico           │
    │                     │     │  - PROCs estatisticos     │
    │                     │     │  - Complexidade muito alta│
    └─────────────────────┘     └──────────────────────────┘
```

## Funcionalidades

- **Discovery automatizado**: scan de programas SAS (.sas), datasets (.sas7bdat), macros e logs
- **Parser de codigo SAS**: extracao de LIBNAMEs, DATA steps, PROCs, macros, MERGE, hash objects, SQL dinamico
- **Score de complexidade**: classificacao automatica (LOW/MEDIUM/HIGH/VERY_HIGH)
- **Grafo de lineage**: dependencias entre programas, datasets, macros e includes
- **Catalogo de dados**: deteccao de PII, classificacao de dominio e sensibilidade
- **Transpilacao de codigo**: SAS -> Snowflake SQL + Snowpark OU Databricks SQL + PySpark
- **Geracao de DDL**: CREATE TABLE para Snowflake (com COPY INTO/Snowpipe) ou Databricks (Delta Lake/Auto Loader)
- **Deteccao de gaps**: identificacao explicita do que NAO pode ser convertido automaticamente
- **Validacao pos-migracao**: scripts de row count, schema match, column stats e checksum
- **Relatorio PDF**: relatorio profissional com graficos, tabelas e KPIs
- **Integracao LLM**: validacao de parsing, revisao de arquitetura, enriquecimento de catalogo, gap resolution
- **Multiplas fontes SAS**: filesystem local, ODBC ou SAS Metadata Server

## Instalacao

### Requisitos

- Python 3.9+
- pip

### Instalacao basica

```bash
git clone https://github.com/vbActDigital/sas_migrator.git
cd sas_migrator
pip install -r requirements.txt
```

### Instalacao completa (com suporte a leitura nativa de SAS e LLM)

```bash
pip install -r requirements.txt
pip install pyreadstat sas7bdat pandas numpy openai
```

### Instalacao como pacote

```bash
pip install -e .
```

## Uso rapido

### Apontar para um diretorio real de programas SAS

```bash
# Pipeline completo (discovery + migracao) em um comando
sas-migrator run --sas-path /caminho/para/programas/sas --out output --target snowflake

# Com datasets tambem
sas-migrator run --sas-path /sas/programs --data-path /sas/data --out output

# Com LLM habilitado para todas as etapas
sas-migrator run --sas-path /sas/programs --out output --llm
```

### Executar etapas separadamente

```bash
# 1. Discovery (analise do ambiente)
sas-migrator discover --sas-path /sas/programs --out output --target snowflake

# 2. Migracao (conversao e de-para)
sas-migrator migrate --inventory output/inventory.json --out output/migration
```

### Usando arquivo de configuracao

```bash
# Com config YAML completo
sas-migrator discover --config config/snowflake_aws_config.yaml --out output
sas-migrator migrate --inventory output/inventory.json --config config/snowflake_aws_config.yaml --out output/migration
```

## O que e convertido automaticamente vs. intervencao manual

O toolkit converte automaticamente a maioria dos padroes SAS comuns e **reporta explicitamente** o que nao pode ser convertido:

### Conversao automatica

| Padrao SAS | Snowflake | Databricks |
|------------|-----------|------------|
| LIBNAME | USE DATABASE/SCHEMA | USE CATALOG/SCHEMA |
| DATA step (SET) | CREATE TABLE AS SELECT | CREATE TABLE USING DELTA |
| MERGE | SQL JOIN | PySpark join |
| PROC SORT | ORDER BY | ORDER BY |
| PROC SQL | Snowflake SQL | Databricks SQL |
| PROC FREQ | GROUP BY + COUNT | GROUP BY + COUNT |
| PROC MEANS | AVG/MIN/MAX/STD | AVG/MIN/MAX/STDDEV |
| PROC FORMAT | CASE WHEN | CASE WHEN |
| %MACRO | Stored Procedure | SQL Function |

### Requer intervencao manual (reportado no gap_report.json)

| Padrao SAS | Severidade | Sugestao |
|------------|-----------|----------|
| Hash objects | HIGH | Reescrita como JOIN / broadcast join |
| CALL EXECUTE / SQL dinamico | HIGH | Analise manual da logica dinamica |
| PROC LOGISTIC/REG/GLM | MEDIUM | Framework ML da plataforma alvo |
| PROC MIXED/IML | MEDIUM | Spark MLlib / Snowpark ML |
| Complexidade VERY_HIGH | MEDIUM | Revisao manual recomendada |

## Configuracao

### 1. Via CLI (sem arquivo YAML)

O modo mais simples: aponte direto para o diretorio de scripts SAS.

```bash
sas-migrator run --sas-path /caminho/para/sas --out output --target snowflake
```

O toolkit cria a configuracao automaticamente com mapeamentos padrao.

### 2. Via arquivo YAML

Para configuracoes mais complexas, use um arquivo YAML:

- `config/snowflake_aws_config.yaml` - para migracao para **Snowflake + AWS**
- `config/databricks_config.yaml` - para migracao para **Databricks**

```yaml
project:
  name: "Meu Projeto de Migracao"
  client: "Nome do Cliente"

sas_environment:
  source_type: filesystem          # filesystem | odbc | metadata_server
  code_paths:
    - /caminho/para/programas/sas
    - /caminho/para/macros
  data_paths:
    - /caminho/para/datasets
  log_paths:
    - /caminho/para/logs
  exclude_patterns:
    - backup
    - archive
    - test

target:
  platform: snowflake              # snowflake | databricks
```

### 3. Conexao direta ao SAS (ODBC ou Metadata Server)

```yaml
# Opcao ODBC
sas_environment:
  source_type: odbc
  odbc:
    dsn: SAS_PROD
    user: ${SAS_USER}
    password: ${SAS_PASSWORD}
  code_paths:
    - /sas/programs

# Opcao Metadata Server
sas_environment:
  source_type: metadata_server
  metadata_server:
    host: ${SAS_META_HOST}
    port: 8561
    user: ${SAS_META_USER}
    password: ${SAS_META_PASSWORD}
  code_paths:
    - /sas/programs
```

### 4. Plataforma alvo

**Snowflake:**
```yaml
target:
  platform: snowflake
  account: ${SNOWFLAKE_ACCOUNT}
  user: ${SNOWFLAKE_USER}
  password: ${SNOWFLAKE_PASSWORD}
  warehouse: MIGRATION_WH
  database: SAS_MIGRATION

library_mapping:
  rawdata:
    database: SAS_MIGRATION
    schema: RAW
  dw:
    database: SAS_MIGRATION
    schema: REFINED
```

**Databricks:**
```yaml
target:
  platform: databricks
  workspace_url: ${DATABRICKS_HOST}
  token: ${DATABRICKS_TOKEN}
  catalog: sas_migration

library_mapping:
  rawdata:
    catalog: sas_migration
    schema: bronze_sas
  dw:
    catalog: sas_migration
    schema: silver_sas
```

### 5. LLM (opcional)

```yaml
llm:
  provider: openai
  api_key_env: OPENAI_API_KEY
  # base_url: https://...          # para Azure OpenAI ou outros providers
  models:
    fast: gpt-4o-mini
    balanced: gpt-4o
    powerful: gpt-4o
```

O LLM e usado para:
- Validar output do parser (encontrar elementos nao detectados)
- Revisar arquitetura de migracao
- Enriquecer catalogo de dados (descricoes, regras de qualidade)
- Revisar codigo transpilado
- Sugerir resolucoes para gaps de conversao

## Artefatos gerados

```
output/
├── inventory.json              # Inventario completo (programas + datasets + lineage)
├── discovery_report.md         # Relatorio Markdown (7 secoes)
├── discovery_report.pdf        # Relatorio PDF profissional
├── data_catalog.json           # Catalogo de dados (PII + dominios)
└── migration/
    ├── migration_plan.json     # Plano de migracao
    ├── ddl/                    # Scripts CREATE TABLE
    ├── data_load/              # Scripts COPY INTO / Auto Loader
    ├── transpiled/             # Codigo SQL + Python transpilado
    │   ├── *.sql               # SQL transpilado
    │   ├── *_snowpark.py       # Snowpark Python (Snowflake)
    │   └── *_pyspark.py        # PySpark (Databricks)
    ├── validation/             # Scripts de validacao pos-migracao
    ├── gap_report.json         # Gaps + itens de intervencao manual
    └── gap_suggestions.json    # Sugestoes LLM para gaps
```

## Relatorio PDF

O PDF gerado inclui:

1. **Sumario Executivo** - KPIs, distribuicao de complexidade, estrategia recomendada
2. **Inventario de Programas** - tabela com score, PROCs, features especiais
3. **Inventario de Datasets** - linhas, colunas, tamanho, detalhamento
4. **Dependencias e Lineage** - metricas do grafo, composicao, raizes e folhas
5. **Catalogo de Dados** - classificacao de sensibilidade, PII, dominios
6. **Analise de Complexidade** - ranking, gaps, intervencoes manuais necessarias
7. **Limitacoes e Proximos Passos** - timeline de implementacao

## Docker

```bash
# Build e run
docker compose up --build

# Os artefatos ficam em ./output/

# Com LLM habilitado
docker run --rm \
  -e OPENAI_API_KEY=$OPENAI_API_KEY \
  -v $(pwd)/output:/app/output \
  sas-migrator
```

## Testes

O toolkit inclui dados de teste para validacao do pipeline:

```bash
# Gerar ambiente de teste
python tests/create_mock_environment.py

# Rodar todos os testes
python -m pytest tests/ -v

# Teste end-to-end
python tests/run_mvp1_test.py
```

## Arquitetura

```
src/
├── cli.py                              # CLI (Click) - comandos discover, migrate, run
├── services/
│   ├── discovery_service.py            # Orquestrador do pipeline de discovery
│   └── migration_service.py            # Orquestrador do pipeline de migracao
├── connectors/
│   ├── sas/
│   │   ├── filesystem_scanner.py       # Scan recursivo de programas e datasets
│   │   ├── metadata_connector.py       # Conexao ao SAS Metadata Server
│   │   └── odbc_connector.py           # Conexao ODBC ao SAS
│   ├── snowflake/
│   │   └── snowflake_connector.py      # Conexao Snowflake
│   ├── databricks/
│   │   └── databricks_connector.py     # Conexao Databricks
│   └── aws/
│       └── aws_connector.py            # Integracao AWS/S3
├── parsers/sas/
│   ├── sas_code_parser.py              # Parser regex de codigo SAS
│   ├── sas_data_parser.py              # Parser de metadados de datasets
│   └── lineage_builder.py              # Construtor do grafo de dependencias
├── migration/
│   ├── code_transpiler/
│   │   ├── snowflake_transpiler.py     # SAS -> Snowflake SQL + Snowpark
│   │   └── databricks_transpiler.py    # SAS -> Databricks SQL + PySpark
│   └── data_migrator/
│       ├── snowflake_migrator.py       # DDL + COPY INTO + Snowpipe
│       └── databricks_migrator.py      # DDL Delta Lake + Auto Loader
├── catalog/
│   └── catalog_generator.py            # Catalogo com PII e dominios
├── reporting/
│   ├── report_generator.py             # Relatorio Markdown (7 secoes)
│   └── pdf_generator.py               # Relatorio PDF profissional
├── validation/
│   └── validator.py                    # Scripts de validacao pos-migracao
├── llm/
│   ├── llm_client.py                   # Cliente OpenAI (compativel com qualquer API)
│   └── llm_advisor.py                  # Advisor LLM para analise e revisao
└── utils/
    ├── config_loader.py                # Loader YAML com expansao de env vars
    ├── logger.py                       # Logging rotativo (console + arquivo)
    └── helpers.py                      # Funcoes utilitarias
```

## Variaveis de ambiente

| Variavel | Descricao | Obrigatoria |
|----------|-----------|-------------|
| `OPENAI_API_KEY` | API key para integracao LLM | Nao (opcional) |
| `SNOWFLAKE_ACCOUNT` | Conta Snowflake | Somente se target=snowflake |
| `SNOWFLAKE_USER` | Usuario Snowflake | Somente se target=snowflake |
| `SNOWFLAKE_PASSWORD` | Senha Snowflake | Somente se target=snowflake |
| `DATABRICKS_HOST` | URL do workspace Databricks | Somente se target=databricks |
| `DATABRICKS_TOKEN` | Token de acesso Databricks | Somente se target=databricks |
| `AWS_IAM_ROLE` | IAM Role para S3 staging | Somente se usando AWS |
| `SAS_USER` | Usuario SAS (modo ODBC) | Somente se source_type=odbc |
| `SAS_PASSWORD` | Senha SAS (modo ODBC) | Somente se source_type=odbc |

## Stack

- **Python** 3.9+
- **Click** - CLI framework
- **PyYAML** - configuracao
- **fpdf2** - geracao de PDF
- **requests** - chamadas HTTP (LLM)
- **pytest** - testes

### Opcionais

- **pyreadstat** - leitura nativa de .sas7bdat
- **pandas** / **numpy** - manipulacao de dados
- **openai** - SDK alternativo para LLM
