# SAS Migration Toolkit

Toolkit de producao para analise e migracao de ambientes SAS para **Snowflake** ou **Databricks**.

Aponte para um diretorio com programas SAS — ou deixe o toolkit varrer o servidor automaticamente — e ele executa o fluxo completo: analise, conversao automatica e identificacao do que precisa de intervencao manual.

**Versao 1.1.0**

---

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
                    │  - Varredura auto    │
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

---

## Funcionalidades

- **Varredura automatica**: descoberta de todos os arquivos SAS por extensao, sem necessidade de informar caminhos fixos
- **Busca paralela**: multiplas threads varrendo diretorios simultaneamente (padrao: 16 workers)
- **Deteccao automatica de encoding**: suporte a UTF-8, Latin-1, CP1252 e ISO-8859-1
- **Parser de codigo SAS**: extracao de LIBNAMEs, DATA steps, PROCs, macros, MERGE, hash objects, SQL dinamico
- **Validacao de integridade de codigo**: deteccao de variaveis indefinidas, padroes de auto-reimportacao, mapeamentos incompletos e lacunas de secao entre programas relacionados
- **Score de complexidade 4 dimensoes**: PLP (programacao), PDI (integracao), PVD (volume de dados), PRS (risco de reescrita)
- **Grafo de lineage**: dependencias entre programas, datasets, macros e includes
- **Catalogo de dados**: deteccao de PII, classificacao de dominio e sensibilidade
- **Transpilacao de codigo**: SAS → Snowflake SQL + Snowpark OU Databricks SQL + PySpark
- **Geracao de DDL**: CREATE TABLE para Snowflake (com COPY INTO/Snowpipe) ou Databricks (Delta Lake/Auto Loader)
- **Deteccao de gaps**: identificacao explicita do que NAO pode ser convertido automaticamente
- **Validacao pos-migracao**: scripts de row count, schema match, column stats e checksum
- **Relatorio PDF**: relatorio profissional com graficos, tabelas, KPIs e secao de validacao de integridade
- **Integracao LLM**: validacao de parsing, revisao de arquitetura, enriquecimento de catalogo, gap resolution
- **Multiplas fontes SAS**: filesystem local, ODBC ou SAS Metadata Server
- **Suporte RHEL 7.9 / SAS 9.4**: compativel com servidores Linux Red Hat sem caminho fixo configurado

---

## Formatos SAS suportados

O toolkit reconhece e classifica automaticamente todos os tipos de arquivo SAS:

| Extensao | Tipo | Descricao |
|----------|------|-----------|
| `.sas` | Codigo | Programa SAS (texto com codigo SAS) |
| `.sas7bpgm` | Codigo | DATA step compilado e armazenado |
| `.sas7bdat` | Dados | Dataset SAS binario |
| `.sas7bcat` | Dados | Catalogo SAS (formatos, informatos, macros) |
| `.xpt` | Dados | Arquivo de transporte SAS (cross-platform) |
| `.stx` | Dados | Arquivo de transporte alternativo |
| `.log` | Log | Log de execucao SAS |
| `.lst` | Log | Listagem de saida (PROC PRINT, PROC MEANS etc) |
| `.cfg` | Configuracao | Configuracao do SAS |
| `.spk` | Configuracao | Pacote SAS (SAS Management Console / DI Studio) |
| `.egp` | Projeto | Projeto SAS Enterprise Guide |
| `.ctp` | Projeto | Custom Task do Enterprise Guide |
| `.amx` | Projeto | Modelo SAS Add-In for Microsoft Office |
| `.smd` | Projeto | Metadados de servidor (Server Metadata) |
| `.djf` | Projeto | SAS DataFlux / Data Integration Studio |
| `.ddf` | Projeto | SAS DataFlux / Data Integration Studio |

---

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

### Instalacao completa (com suporte a leitura nativa de SAS)

```bash
pip install -r requirements.txt
pip install pyreadstat sas7bdat pandas numpy
```

### Instalacao como pacote

```bash
pip install -e .
```

### Instalacao em RHEL 7.9 (SAS 9.4)

O RHEL 7.9 instala Python 3.6 por padrao. O toolkit requer Python 3.9+. Instale via Software Collections:

```bash
# Instalar Python 3.9 via SCL
sudo yum install rh-python39 -y
scl enable rh-python39 bash

# Verificar versao
python --version   # Python 3.9.x

# Instalar o toolkit
pip install -r requirements.txt
```

---

## Uso rapido

### Varredura automatica (sem caminho fixo)

```bash
# Pipeline completo varrendo o servidor inteiro a partir de /
sas-migrator run --config config/bmg_config.yaml --out output --target snowflake

# Varredura a partir de diretorios especificos
sas-migrator discover --config config/bmg_config.yaml --out output
```

### Apontar para um diretorio especifico

```bash
# Pipeline completo (discovery + migracao)
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

---

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
| PROC IMPORT (Excel/CSV) | COPY INTO / Snowpipe | Auto Loader |
| PROC EXPORT (CSV/XLSX) | UNLOAD / GET_DDL | EXPORT TO CSV |
| %LET variaveis | Parametros de procedure | Parametros de procedure |

### Requer intervencao manual (reportado no gap_report.json)

| Padrao SAS | Severidade | Sugestao |
|------------|-----------|----------|
| Hash objects | HIGH | Reescrita como JOIN / broadcast join |
| CALL EXECUTE / SQL dinamico | HIGH | Analise manual da logica dinamica |
| PROC LOGISTIC/REG/GLM | MEDIUM | Framework ML da plataforma alvo |
| PROC MIXED/IML | MEDIUM | Spark MLlib / Snowpark ML |
| Complexidade VERY_HIGH | MEDIUM | Revisao manual recomendada |

---

## Configuracao

### 1. Varredura automatica (scan_roots) — modo recomendado para servidores sem caminho fixo

Quando o ambiente SAS nao tem um diretorio padrao conhecido, o toolkit varre automaticamente a partir das raizes configuradas:

```yaml
sas_environment:
  source_type: filesystem

  # Raizes de varredura — o toolkit encontra todos os arquivos SAS por extensao
  scan_roots:
    - /home        # diretorios de usuarios
    - /opt         # software instalado
    - /data        # volumes de dados

  # Encoding dos arquivos SAS — Latin-1 e comum em SAS 9.4 no Linux
  # Opcoes: utf-8 | latin-1 | iso-8859-1 | cp1252
  file_encoding: latin-1

  # Threads paralelas para varredura (I/O-bound: mais = mais rapido)
  scan_workers: 16

  # Pular diretorios ocultos (.git, .cache, .ssh etc)
  skip_hidden_dirs: true

  # Profundidade maxima de recursao
  max_scan_depth: 20

  # Padroes para excluir da varredura
  exclude_patterns:
    - backup
    - archive
    - ".git"
    - lost+found
```

O toolkit pula automaticamente os diretorios de sistema Linux (`/proc`, `/sys`, `/dev`, `/boot`, `/usr/lib`, `/bin`, `/sbin` etc), focando apenas em areas com conteudo real.

### 2. Caminhos explicitos (quando conhecidos)

```yaml
sas_environment:
  source_type: filesystem
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
  http_path: ${DATABRICKS_HTTP_PATH}

library_mapping:
  rawdata:
    catalog: sas_migration
    schema: bronze_sas
  dw:
    catalog: sas_migration
    schema: silver_sas
```

### 5. Configuracoes por cliente

Os arquivos de configuracao prontos estao em `config/`:

| Arquivo | Uso |
|---------|-----|
| `config/snowflake_aws_config.yaml` | Migracao para Snowflake + AWS |
| `config/databricks_config.yaml` | Migracao para Databricks |
| `config/bmg_config.yaml` | Template para RHEL 7.9 / SAS 9.4 (scan automatico) |

---

## Artefatos gerados

```
output/
├── inventory.json              # Inventario completo (programas + datasets + lineage)
├── discovery_report.md         # Relatorio Markdown (7 secoes)
├── discovery_report.pdf        # Relatorio PDF profissional
├── analysis_summary.md         # Sumario de analise
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
    └── manual_interventions.json # Intervencoes manuais com sugestoes
```

---

## Relatorio PDF

O PDF gerado inclui 7 secoes:

1. **Sumario Executivo** — KPIs, distribuicao de complexidade, estrategia recomendada
2. **Inventario de Programas** — tabela com score 4D, PROCs, features especiais
3. **Inventario de Datasets** — linhas, colunas, tamanho, detalhamento
4. **Dependencias e Lineage** — metricas do grafo, composicao, raizes e folhas
5. **Catalogo de Dados** — classificacao de sensibilidade, PII, dominios
6. **Analise de Complexidade** — ranking, gaps, intervencoes manuais necessarias
7. **Validacao de Integridade** — findings detalhados por programa (variaveis indefinidas, padroes problematicos, mapeamentos incompletos)

---

## Score de Complexidade (4 Dimensoes)

Cada programa SAS recebe um score calculado em 4 dimensoes:

| Dimensao | Sigla | O que avalia |
|----------|-------|--------------|
| Programacao e Logica | PLP | Macros, hash objects, SQL dinamico, estruturas de controle |
| Integracao de Dados | PDI | MERGEs, JOINs, includes, LIBNAME, PROC IMPORT/EXPORT |
| Volume e Diversidade | PVD | Variedade de PROCs, datasets lidos/escritos, formatos especiais |
| Risco de Reescrita | PRS | PROCs estatisticos, named literals, caminhos UNC, variaveis contabeis |

Classificacao final: **LOW** / **MEDIUM** / **HIGH** / **VERY_HIGH**

---

## Validacao de Integridade de Codigo

O toolkit executa analise profunda de cada programa SAS, detectando:

- **Variaveis indefinidas** em expressoes KEEP e formulas
- **Padroes de auto-reimportacao** (PROC EXPORT seguido de PROC IMPORT do mesmo arquivo)
- **Mapeamentos incompletos** em chains IF/ELSE IF
- **Lacunas de secao** entre programas relacionados (ex: falta segmento Aceito/Direto/Resumo)
- **Formula integrity**: variaveis usadas mas nunca atribuidas
- **Validacao cruzada** entre todos os programas do projeto

---

## Docker

```bash
# Build e run completo
docker compose up --build

# Os artefatos ficam em ./output/

# Somente MVP1 (discovery)
docker build -t sas-migrator -f Dockerfile .
docker run --rm \
  -v $(pwd)/output:/app/mvp1_outputs \
  -e PYTHONUNBUFFERED=1 \
  sas-migrator
```

---

## Testes

```bash
# Gerar ambiente de teste com dados mock
python tests/create_mock_environment.py

# Rodar todos os testes unitarios (271 testes)
python -m pytest tests/ -v

# Teste end-to-end com dados mock
python tests/run_mvp1_docker.py
```

---

## Arquitetura

```
src/
├── cli.py                              # CLI (Click) — comandos discover, migrate, run
├── services/
│   ├── discovery_service.py            # Orquestrador do pipeline de discovery
│   └── migration_service.py            # Orquestrador do pipeline de migracao
├── connectors/
│   ├── sas/
│   │   ├── filesystem_scanner.py       # Varredura paralela (BFS + 16 threads)
│   │   ├── metadata_connector.py       # Conexao ao SAS Metadata Server
│   │   └── odbc_connector.py           # Conexao ODBC ao SAS
│   ├── snowflake/
│   │   └── snowflake_connector.py      # Conexao Snowflake
│   ├── databricks/
│   │   └── databricks_connector.py     # Conexao Databricks (databricks-sql-connector)
│   └── aws/
│       └── aws_connector.py            # Integracao AWS/S3
├── parsers/sas/
│   ├── sas_code_parser.py              # Parser regex de codigo SAS
│   ├── sas_code_validator.py           # Validacao de integridade (single + cross-program)
│   ├── sas_data_parser.py              # Parser de metadados de datasets
│   └── lineage_builder.py              # Construtor do grafo de dependencias
├── migration/
│   ├── code_transpiler/
│   │   ├── snowflake_transpiler.py     # SAS → Snowflake SQL + Snowpark
│   │   └── databricks_transpiler.py    # SAS → Databricks SQL + PySpark
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
│   ├── llm_client.py                   # Cliente HTTP para APIs de linguagem
│   └── llm_advisor.py                  # Advisor para analise e revisao
└── utils/
    ├── config_loader.py                # Loader YAML com expansao de env vars
    ├── logger.py                       # Logging rotativo (console + arquivo)
    └── helpers.py                      # Funcoes utilitarias
```

---

## Variaveis de ambiente

| Variavel | Descricao | Obrigatoria |
|----------|-----------|-------------|
| `SNOWFLAKE_ACCOUNT` | Conta Snowflake | Somente se target=snowflake |
| `SNOWFLAKE_USER` | Usuario Snowflake | Somente se target=snowflake |
| `SNOWFLAKE_PASSWORD` | Senha Snowflake | Somente se target=snowflake |
| `DATABRICKS_HOST` | URL do workspace Databricks | Somente se target=databricks |
| `DATABRICKS_TOKEN` | Token de acesso Databricks | Somente se target=databricks |
| `DATABRICKS_HTTP_PATH` | HTTP Path do SQL Warehouse | Somente se target=databricks |
| `AWS_IAM_ROLE` | IAM Role para S3 staging | Somente se usando AWS |
| `SAS_USER` | Usuario SAS (modo ODBC) | Somente se source_type=odbc |
| `SAS_PASSWORD` | Senha SAS (modo ODBC) | Somente se source_type=odbc |
| `OPENAI_API_KEY` | API key para integracao LLM | Nao (opcional) |

---

## Stack

- **Python** 3.9+
- **Click** — CLI framework
- **PyYAML** — configuracao
- **fpdf2** — geracao de PDF
- **requests** — chamadas HTTP
- **pytest** — testes

### Opcionais

- **pyreadstat** — leitura nativa de .sas7bdat
- **pandas** / **numpy** — manipulacao de dados
- **databricks-sql-connector** — conexao real ao Databricks
- **openai** — SDK para integracao LLM

---

## Compatibilidade

| Ambiente | Suporte |
|----------|---------|
| Windows 10/11 | Sim |
| Linux (Ubuntu, Debian) | Sim |
| RHEL 7.9 / SAS 9.4 | Sim (ver instrucoes de instalacao) |
| Docker | Sim |
| Python 3.9+ | Sim |
| SAS 9.4 | Sim |
| SAS Viya | Compativel (filesystem) |
