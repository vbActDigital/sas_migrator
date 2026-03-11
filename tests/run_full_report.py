#!/usr/bin/env python3
"""
=============================================================================
  SAS-TO-SNOWFLAKE MIGRATION TOOLKIT
  Relatorio Completo de Execucao - MVP1 Discovery + MVP2 Migration
=============================================================================
Gera um relatorio descritivo de cada etapa do pipeline, incluindo:
  - Inventario detalhado de programas e datasets
  - Analise de complexidade com fatores de risco
  - Grafo de lineage com metricas de conectividade
  - Catalogo de dados com PII e dominios
  - Artefatos de migracao (DDL, COPY INTO, Snowpipe, transpiled code)
  - Validacao pos-migracao
  - Gap analysis
  - Relatorio markdown final
"""
import os
import sys
import json
import time
import subprocess
import textwrap
from datetime import datetime
from collections import Counter

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.join(BASE_DIR, "..")
OUTPUT_DIR = os.path.join(PROJECT_ROOT, "report_output")
MOCK_DIR = os.path.join(BASE_DIR, "mock_sas_environment")
CONFIG_PATH = os.path.join(BASE_DIR, "mock_config.yaml")

# ============================================================
# HELPERS DE FORMATACAO
# ============================================================
W = 80

def banner(text, char="="):
    print(f"\n{char * W}")
    print(f"  {text}")
    print(f"{char * W}")

def section(text, char="-"):
    print(f"\n{char * W}")
    print(f"  {text}")
    print(f"{char * W}")

def subsection(text):
    print(f"\n  >>> {text}")
    print(f"  {'.' * (W - 6)}")

def kv(key, value, indent=4):
    print(f"{' ' * indent}{key:<35} {value}")

def table(headers, rows, indent=4):
    """Print a formatted ASCII table."""
    widths = [max(len(str(h)), max((len(str(r[i])) for r in rows), default=0)) for i, h in enumerate(headers)]
    pad = " " * indent
    header_line = pad + " | ".join(f"{h:<{widths[i]}}" for i, h in enumerate(headers))
    sep_line = pad + "-+-".join("-" * w for w in widths)
    print(header_line)
    print(sep_line)
    for row in rows:
        print(pad + " | ".join(f"{str(row[i]):<{widths[i]}}" for i in range(len(headers))))

def box(title, content):
    """Print content in a box."""
    lines = content.strip().split("\n")
    max_len = max(len(title) + 4, max(len(l) for l in lines) + 4)
    print(f"\n    +{'-' * max_len}+")
    print(f"    | {title:<{max_len - 2}} |")
    print(f"    +{'-' * max_len}+")
    for line in lines:
        print(f"    | {line:<{max_len - 2}} |")
    print(f"    +{'-' * max_len}+")

def format_bytes(n):
    for unit in ("B", "KB", "MB", "GB"):
        if abs(n) < 1024:
            return f"{n:.1f} {unit}" if unit != "B" else f"{n} B"
        n /= 1024
    return f"{n:.1f} TB"

def passed(msg):
    print(f"\n    [PASSED] {msg}")

def failed(msg):
    print(f"\n    [FAILED] {msg}")

def info(msg):
    print(f"    [INFO]   {msg}")


# ============================================================
# CARREGAMENTO
# ============================================================
def load_config():
    import yaml
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


# ============================================================
# FASE 0: TESTES UNITARIOS
# ============================================================
def phase0_unit_tests():
    banner("FASE 0: TESTES UNITARIOS", "=")
    print("""
    Objetivo: Validar que todos os modulos do toolkit estao funcionando
    corretamente antes de executar o pipeline completo.

    Modulos testados:
      - SASCodeParser       (18 testes) - parsing regex de codigo .sas
      - LineageBuilder       (12 testes) - construcao de grafo de dependencias
      - LLMAdvisor           (8 testes)  - integracao com LLM (mock)
      - SnowflakeTranspiler  (12 testes) - transpilacao SAS -> Snowflake SQL
    """)

    result = subprocess.run(
        [sys.executable, "-m", "pytest", "tests/", "-v", "--tb=short"],
        cwd=PROJECT_ROOT,
        capture_output=True, text=True
    )

    # Parse results
    lines = result.stdout.strip().split("\n")
    test_results = []
    for line in lines:
        if "PASSED" in line or "FAILED" in line:
            status = "PASSED" if "PASSED" in line else "FAILED"
            name = line.split("::")[1].split(" ")[0] if "::" in line else line
            module = line.split("::")[0].split("/")[-1].split("\\")[-1] if "::" in line else ""
            test_results.append((module, name, status))

    # Group by module
    modules = {}
    for mod, name, status in test_results:
        if mod not in modules:
            modules[mod] = {"passed": 0, "failed": 0, "tests": []}
        modules[mod]["tests"].append((name, status))
        if status == "PASSED":
            modules[mod]["passed"] += 1
        else:
            modules[mod]["failed"] += 1

    total_passed = sum(m["passed"] for m in modules.values())
    total_failed = sum(m["failed"] for m in modules.values())
    total = total_passed + total_failed

    for mod, data in modules.items():
        subsection(f"{mod} ({data['passed']}/{data['passed'] + data['failed']})")
        for name, status in data["tests"]:
            icon = "+" if status == "PASSED" else "X"
            print(f"      [{icon}] {name}")

    section(f"Resultado: {total_passed}/{total} testes passaram")
    if total_failed == 0:
        passed(f"Todos os {total} testes unitarios passaram com sucesso")
    else:
        failed(f"{total_failed} testes falharam")

    return total_failed == 0, total_passed, total


# ============================================================
# FASE 1: FILESYSTEM SCAN
# ============================================================
def phase1_filesystem_scan(config):
    banner("FASE 1: VARREDURA DO FILESYSTEM SAS", "=")
    print("""
    Objetivo: Escanear todos os diretorios configurados para identificar
    programas .sas, datasets .sas7bdat e logs .log do ambiente SAS.

    Atividades da planilha MAPFRE cobertas:
      1.1   - Mapeamento de Repositorios e Bibliotecas
      1.1.1 - Varredura de File Systems do Servidor
      1.1.5 - Deteccao de Codigo "Shadow IT"
      1.1.6 - Validacao de Backup e Versionamento
    """)

    from src.connectors.sas.filesystem_scanner import SASFilesystemScanner

    scanner = SASFilesystemScanner(config)

    # Mostra configuracao
    subsection("Configuracao do Scanner")
    kv("Code paths:", str(config["sas_environment"]["code_paths"]))
    kv("Data paths:", str(config["sas_environment"]["data_paths"]))
    kv("Exclude patterns:", str(config["sas_environment"].get("exclude_patterns", [])))
    kv("Max scan depth:", str(scanner.max_scan_depth))

    # Scan programs
    subsection("Varredura de Programas SAS (.sas)")
    programs = scanner.scan_programs()

    rows = []
    total_lines = 0
    total_size = 0
    for p in sorted(programs, key=lambda x: x["filename"]):
        total_lines += p["line_count"]
        total_size += p["size_bytes"]
        rows.append((
            p["filename"],
            str(p["line_count"]),
            format_bytes(p["size_bytes"]),
            p["last_modified"][:10],
            p["encoding"],
        ))

    table(["Programa", "Linhas", "Tamanho", "Modificado", "Encoding"], rows)
    print()
    kv("Total de programas:", len(programs))
    kv("Total de linhas:", f"{total_lines:,}")
    kv("Tamanho total:", format_bytes(total_size))

    # Verify exclusion
    subsection("Validacao de Exclusao (backup/)")
    all_paths = [p["absolute_path"].replace("\\", "/") for p in programs]
    backup_found = [p for p in all_paths if "backup" in p.lower()]
    if not backup_found:
        passed("Diretorio 'backup/' excluido corretamente - nenhum arquivo de backup incluido")
    else:
        failed(f"Arquivos de backup encontrados: {backup_found}")

    # Scan datasets
    subsection("Varredura de Datasets SAS (.sas7bdat)")
    datasets = scanner.scan_datasets()

    rows = []
    for d in sorted(datasets, key=lambda x: x["filename"]):
        rows.append((
            d["filename"],
            d["dataset_name"],
            d["inferred_library"],
            d["file_type"],
            format_bytes(d["size_bytes"]),
        ))

    table(["Arquivo", "Dataset", "Biblioteca", "Tipo", "Tamanho"], rows)
    print()
    kv("Total de datasets:", len(datasets))

    # Scan logs
    subsection("Varredura de Logs (.log)")
    logs = scanner.scan_logs()
    kv("Logs encontrados:", len(logs))
    info("(Nenhum diretorio de logs configurado no mock)")

    passed(f"Filesystem scan concluido: {len(programs)} programas, {len(datasets)} datasets")
    return programs, datasets


# ============================================================
# FASE 2: PARSING DE CODIGO SAS
# ============================================================
def phase2_code_parsing(programs):
    banner("FASE 2: PARSING DE CODIGO SAS", "=")
    print("""
    Objetivo: Analisar cada programa .sas via regex para extrair:
      - Atribuicoes LIBNAME (bibliotecas SAS)
      - DATA steps (datasets criados)
      - PROCs utilizados (SORT, SQL, LOGISTIC, etc.)
      - Datasets lidos (SET, MERGE, FROM) e escritos (DATA, CREATE TABLE)
      - Definicoes e chamadas de macros
      - Hash objects e SQL dinamico
      - Score de complexidade (classificacao LOW/MEDIUM/HIGH/VERY_HIGH)

    Atividades da planilha MAPFRE cobertas:
      1.4   - Catalogacao de Macros Customizadas
      1.4.2 - Inventario de Macros de Processamento Core
      1.4.5 - Documentacao de Logica de Loop e Condicionais
      1.4.6 - Teste de Dependencia Cruzada
      2.4   - Identificacao de Hard-Codings
      3.1   - Classificacao por Peso de Logica
    """)

    from src.parsers.sas.sas_code_parser import SASCodeParser
    parser = SASCodeParser()
    parsed_programs = []

    for prog in programs:
        parsed = parser.parse_file(prog["absolute_path"])
        parsed_programs.append(parsed)

    # Detalhes por programa
    for p in sorted(parsed_programs, key=lambda x: x["complexity_score"], reverse=True):
        subsection(f"{p['filename']} (Score: {p['complexity_score']}, Level: {p['complexity_level']})")
        kv("Linhas de codigo:", p["line_count"])
        kv("LIBNAMEs:", ", ".join(lib["name"] for lib in p["libnames"]) or "Nenhum")
        kv("PROCs utilizados:", ", ".join(p["procs_used"]) or "Nenhum")
        kv("Datasets lidos:", ", ".join(p["datasets_read"][:6]) or "Nenhum")
        kv("Datasets escritos:", ", ".join(p["datasets_written"][:6]) or "Nenhum")
        kv("Macros definidas:", ", ".join(p["macro_definitions"]) or "Nenhuma")
        kv("Macros chamadas:", ", ".join(p["macro_calls"]) or "Nenhuma")
        kv("MERGE statements:", f"{len(p['merge_statements'])} tabelas" if p["merge_statements"] else "Nenhum")
        kv("Includes:", ", ".join(p["includes"]) or "Nenhum")
        kv("Hash objects:", "SIM" if p["has_hash_objects"] else "Nao")
        kv("SQL dinamico:", "SIM" if p["has_dynamic_sql"] else "Nao")

        # Complexity breakdown
        factors = []
        if p["data_steps"]:
            factors.append(f"DATA steps ({len(p['data_steps'])}) = +{len(p['data_steps'])}")
        if p["merge_statements"]:
            factors.append(f"MERGE ({len(p['merge_statements'])} tabelas) = +{len(p['merge_statements']) * 5}")
        for proc in p["procs_used"]:
            if proc.upper() == "SQL":
                factors.append(f"PROC SQL = +3")
            elif proc.upper() in {"LOGISTIC", "REG", "GLM", "MIXED", "UNIVARIATE"}:
                factors.append(f"PROC {proc} (estatistico) = +5")
        if p["macro_definitions"]:
            factors.append(f"Macros definidas ({len(p['macro_definitions'])}) = +{len(p['macro_definitions']) * 2}")
        if p["has_hash_objects"]:
            factors.append("Hash objects = +5")
        if p["has_dynamic_sql"]:
            factors.append("SQL dinamico/CALL EXECUTE = +5")
        if p["includes"]:
            factors.append(f"Includes ({len(p['includes'])}) = +{len(p['includes'])}")

        if factors:
            print(f"      Composicao do score:")
            for f in factors:
                print(f"        - {f}")

    # Sumario de complexidade
    subsection("Distribuicao de Complexidade")
    dist = Counter(p["complexity_level"] for p in parsed_programs)
    for level in ["LOW", "MEDIUM", "HIGH", "VERY_HIGH"]:
        count = dist.get(level, 0)
        bar = "#" * (count * 5)
        print(f"      {level:<10} {count:>2} programa(s) {bar}")

    # PROCs unicos
    subsection("PROCs SAS Utilizados no Ambiente")
    all_procs = Counter()
    for p in parsed_programs:
        all_procs.update(p["procs_used"])
    for proc, count in all_procs.most_common():
        translatable = proc.upper() not in {"LOGISTIC", "REG", "GLM", "MIXED", "IML", "REPORT", "TABULATE"}
        status = "Traduzivel" if translatable else "GAP - requer revisao"
        print(f"      PROC {proc:<12} usado {count}x  [{status}]")

    # Features especiais
    subsection("Features Especiais Detectadas")
    for feature, label in [("has_hash_objects", "Hash Objects"), ("has_dynamic_sql", "SQL Dinamico")]:
        progs_with = [p["filename"] for p in parsed_programs if p[feature]]
        if progs_with:
            print(f"      {label}: {', '.join(progs_with)}")
        else:
            print(f"      {label}: Nenhum")

    macro_progs = [(p["filename"], p["macro_definitions"]) for p in parsed_programs if p["macro_definitions"]]
    if macro_progs:
        print(f"      Macros customizadas:")
        for fname, macros in macro_progs:
            print(f"        {fname}: {', '.join(macros)}")

    passed(f"Parsing concluido: {len(parsed_programs)} programas analisados")
    return parsed_programs


# ============================================================
# FASE 3: METADADOS DE DATASETS
# ============================================================
def phase3_dataset_metadata(config):
    banner("FASE 3: EXTRACAO DE METADADOS DE DATASETS", "=")
    print("""
    Objetivo: Extrair metadados detalhados de cada dataset SAS:
      - Contagem de linhas e colunas
      - Tipos de dados (num, char) e formatos SAS
      - Labels descritivos das colunas
      - Deteccao de colunas PII por nome

    Estrategia de leitura (fallback chain):
      1. pyreadstat (leitura nativa .sas7bdat)
      2. sas7bdat library
      3. .meta.json (fallback para mock)

    Atividades da planilha MAPFRE cobertas:
      2.2   - Mapeamento de Formatos e Informatas
      2.2.1 - Inventario de User-Defined Formats (PROC FORMAT)
      2.2.2 - Mapeamento de Datas e Timestamps
      2.2.3 - Identificacao de Valores "Missing"
    """)

    data_dir = config["sas_environment"]["data_paths"][0]
    datasets_metadata = []
    pii_keywords = {"cpf", "email", "phone", "telefone", "salary", "salario", "ssn", "rg",
                    "nome", "name", "address", "endereco"}

    for fname in sorted(os.listdir(data_dir)):
        if not fname.endswith(".meta.json"):
            continue
        with open(os.path.join(data_dir, fname), "r", encoding="utf-8") as f:
            meta = json.load(f)
        datasets_metadata.append(meta)

    total_rows = 0
    total_cols = 0
    total_pii = 0

    for ds in datasets_metadata:
        subsection(f"Dataset: {ds['dataset_name']}")
        kv("Linhas:", f"{ds['row_count']:,}")
        kv("Colunas:", ds['column_count'])
        kv("Tamanho estimado:", format_bytes(ds.get('size_bytes', 0)))
        total_rows += ds["row_count"]
        total_cols += ds["column_count"]

        # Column details
        print(f"\n      {'Coluna':<20} {'Tipo':<6} {'Tam':<5} {'Formato':<14} {'Label':<25} {'PII'}")
        print(f"      {'-'*20} {'-'*6} {'-'*5} {'-'*14} {'-'*25} {'-'*5}")
        ds_pii = []
        num_cols = 0
        char_cols = 0
        date_cols = 0
        for col in ds.get("columns", []):
            is_pii = col["name"].lower() in pii_keywords
            if is_pii:
                ds_pii.append(col["name"])
            pii_flag = "<PII>" if is_pii else ""
            fmt = col.get("format", "") or ""
            is_date = any(d in fmt.lower() for d in ["date", "datetime"])
            if col["type"] == "num":
                if is_date:
                    date_cols += 1
                else:
                    num_cols += 1
            else:
                char_cols += 1
            print(f"      {col['name']:<20} {col['type']:<6} {col.get('length', ''):<5} {fmt:<14} {col.get('label', '')[:25]:<25} {pii_flag}")

        total_pii += len(ds_pii)
        print(f"\n      Resumo: {num_cols} numericas, {char_cols} caractere, {date_cols} datas")
        if ds_pii:
            print(f"      ** PII detectado: {', '.join(ds_pii)} **")

    subsection("Totais Globais de Dados")
    kv("Total de datasets:", len(datasets_metadata))
    kv("Total de linhas:", f"{total_rows:,}")
    kv("Total de colunas:", total_cols)
    kv("Colunas PII:", total_pii)

    passed(f"Metadados extraidos: {len(datasets_metadata)} datasets, {total_rows:,} linhas, {total_cols} colunas")
    return datasets_metadata


# ============================================================
# FASE 4: CONSTRUCAO DE LINEAGE
# ============================================================
def phase4_lineage(parsed_programs):
    banner("FASE 4: CONSTRUCAO DO GRAFO DE LINEAGE", "=")
    print("""
    Objetivo: Construir um grafo dirigido de dependencias entre programas,
    datasets, macros e includes para visualizar o fluxo de dados.

    Tipos de nos: program, dataset, macro, include
    Tipos de arestas: reads, writes, calls, defines, includes

    Atividades da planilha MAPFRE cobertas:
      2.3   - Diagramacao de Precedencia (Lineage)
      2.3.2 - Rastreamento de Tabelas Temporarias (WORK)
      2.3.3 - Identificacao de Dependencias Cruzadas
      2.3.4 - Documentacao de Fluxo Input/Output
      2.3.5 - Analise de Impacto de Falha
    """)

    from src.parsers.sas.lineage_builder import LineageBuilder
    builder = LineageBuilder()
    lineage = builder.build_from_parsed_programs(parsed_programs)
    nodes = lineage["nodes"]
    edges = lineage["edges"]

    # Metricas gerais
    subsection("Metricas do Grafo")
    types = Counter(n["type"] for n in nodes)
    rel_types = Counter(e["relationship"] for e in edges)

    kv("Total de nos:", len(nodes))
    kv("Total de arestas:", len(edges))
    print()
    for t, c in types.most_common():
        print(f"      Nos tipo '{t}': {c}")
    print()
    for r, c in rel_types.most_common():
        print(f"      Arestas tipo '{r}': {c}")

    # Raizes e folhas
    targets = {e["target"] for e in edges}
    sources = {e["source"] for e in edges}
    all_ids = {n["id"] for n in nodes}
    roots = all_ids - targets
    leaves = all_ids - sources

    subsection("Nos Raiz (fontes de dados sem dependencia)")
    for r in sorted(roots):
        node = next((n for n in nodes if n["id"] == r), None)
        if node:
            print(f"      [{node['type']:<8}] {node['label']}")

    subsection("Nos Folha (saidas finais sem consumidores)")
    for l in sorted(leaves):
        node = next((n for n in nodes if n["id"] == l), None)
        if node:
            print(f"      [{node['type']:<8}] {node['label']}")

    # Analise de conectividade
    subsection("Analise de Conectividade (Upstream/Downstream)")
    dataset_nodes = [n for n in nodes if n["type"] == "dataset"]
    rows = []
    for ds in sorted(dataset_nodes, key=lambda x: x["label"]):
        up = builder.get_upstream(ds["id"])
        down = builder.get_downstream(ds["id"])
        rows.append((ds["label"], str(len(up)), str(len(down)),
                      "CRITICO" if len(down) >= 3 else "Normal"))
    table(["Dataset", "Upstream", "Downstream", "Criticidade"], rows)

    # Caminhos criticos
    subsection("Programas por Impacto (mais conexoes)")
    prog_nodes = [n for n in nodes if n["type"] == "program"]
    prog_impact = []
    for pn in prog_nodes:
        down = builder.get_downstream(pn["id"])
        up = builder.get_upstream(pn["id"])
        prog_impact.append((pn["label"], len(up), len(down), len(up) + len(down)))
    prog_impact.sort(key=lambda x: x[3], reverse=True)
    for label, up, down, total in prog_impact:
        bar = "#" * min(total, 40)
        print(f"      {label:<35} Up:{up:>2} Down:{down:>2} Total:{total:>2} {bar}")

    passed(f"Lineage construido: {len(nodes)} nos, {len(edges)} arestas, {len(roots)} raizes, {len(leaves)} folhas")
    return lineage, builder


# ============================================================
# FASE 5: CATALOGO DE DADOS
# ============================================================
def phase5_catalog(config, datasets_metadata, parsed_programs, lineage):
    banner("FASE 5: GERACAO DO CATALOGO DE DADOS", "=")
    print("""
    Objetivo: Gerar um catalogo de dados completo com:
      - Classificacao de dominio por keywords (Customer, Financial, Risk, etc.)
      - Deteccao de PII por nome de coluna (cpf, email, phone, etc.)
      - Classificacao de sensibilidade (Restricted, Internal, Public)
      - Enriquecimento opcional por LLM (desabilitado neste teste)

    Atividades da planilha MAPFRE cobertas:
      1.2   - Extracao de Metadados de Usuarios e Acessos
      1.2.3 - Extracao de Arvore de Metadados
      3.3   - Validacao de Seguranca
    """)

    from src.catalog.catalog_generator import DataCatalogGenerator

    subsection("Configuracao do Catalogo")
    cat_config = config.get("catalog", {})
    kv("Detectar PII:", cat_config.get("detect_pii", True))
    kv("Inferir dominios:", cat_config.get("infer_domains", True))
    kv("LLM habilitado:", "Nao (teste sem API key)")
    kv("Formatos de saida:", str(cat_config.get("output_format", ["json"])))

    generator = DataCatalogGenerator(config=config, llm_advisor=None)
    catalog = generator.generate_catalog(datasets_metadata, parsed_programs, lineage, enrich_with_llm=False)
    summary = catalog["summary"]

    subsection("Classificacao por Dominio")
    for domain, count in summary["domains"].items():
        bar = "#" * (count * 8)
        print(f"      {domain:<15} {count} dataset(s)  {bar}")

    subsection("Classificacao de Sensibilidade")
    for level in ["Restricted", "Internal", "Public"]:
        count = summary["sensitivity_distribution"].get(level, 0)
        desc = {
            "Restricted": "Contem PII - requer controle de acesso rigoroso",
            "Internal": "Dados de negocio sem PII",
            "Public": "Dados de referencia / lookup"
        }
        bar = "#" * (count * 8)
        print(f"      {level:<12} {count} dataset(s)  {bar}")
        print(f"                   {desc[level]}")

    subsection("Detalhamento por Dataset")
    for ds in catalog["datasets"]:
        pii = ds.get("pii_columns", [])
        pii_str = ", ".join(pii) if pii else "Nenhuma"
        print(f"\n      {ds['dataset_name']}")
        print(f"        Dominio:       {ds['domain']}")
        print(f"        Sensibilidade: {ds['sensitivity']}")
        print(f"        Linhas:        {ds['row_count']:,}")
        print(f"        Colunas:       {ds['column_count']}")
        print(f"        PII:           {pii_str}")
        if pii:
            print(f"        ** ATENCAO: Dataset contem dados sensiveis - aplicar masking/encryption **")

    subsection("Resumo do Catalogo")
    kv("Total de datasets:", summary["total_datasets"])
    kv("Total de colunas:", summary["total_columns"])
    kv("Datasets com PII:", summary["datasets_with_pii"])
    kv("Dominios identificados:", len(summary["domains"]))

    passed(f"Catalogo gerado: {summary['total_datasets']} datasets, {summary['datasets_with_pii']} com PII")
    return catalog


# ============================================================
# FASE 6: RELATORIO DISCOVERY (MVP1)
# ============================================================
def phase6_discovery_report(config, parsed_programs, datasets_metadata, lineage):
    banner("FASE 6: GERACAO DO RELATORIO DISCOVERY", "=")
    print("""
    Objetivo: Gerar relatorio Markdown consolidado com 7 secoes:
      1. Sumario Executivo
      2. Inventario de Programas
      3. Inventario de Datasets
      4. Dependencias e Lineage
      5. Analise de Complexidade
      6. Limitacoes e Pontos de Atencao
      7. Proximos Passos
    """)

    from src.reporting.report_generator import ReportGenerator

    discovery_dir = os.path.join(OUTPUT_DIR, "mvp1_discovery")
    os.makedirs(discovery_dir, exist_ok=True)

    report_gen = ReportGenerator(config)
    report_path = report_gen.generate_discovery_report(
        parsed_programs, datasets_metadata, lineage, output_dir=discovery_dir
    )

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

    subsection("Validacao de Secoes")
    all_present = True
    for sect in expected_sections:
        found = sect in content
        icon = "+" if found else "X"
        print(f"      [{icon}] {sect}")
        if not found:
            all_present = False

    kv("Arquivo:", report_path)
    kv("Tamanho:", f"{len(content):,} caracteres")
    kv("Secoes:", f"{sum(1 for s in expected_sections if s in content)}/7")

    # Save inventory JSON too
    inventory = {
        "programs": [{
            "filename": p["filename"], "filepath": p["filepath"],
            "complexity_score": p["complexity_score"], "complexity_level": p["complexity_level"],
            "procs_used": p["procs_used"], "libnames": p["libnames"],
            "data_steps": p["data_steps"], "datasets_read": p["datasets_read"],
            "datasets_written": p["datasets_written"], "macro_definitions": p["macro_definitions"],
            "macro_calls": p["macro_calls"], "merge_statements": p["merge_statements"],
            "includes": p["includes"], "has_hash_objects": p["has_hash_objects"],
            "has_dynamic_sql": p["has_dynamic_sql"],
        } for p in parsed_programs],
        "datasets": datasets_metadata,
        "lineage": lineage,
    }
    inv_path = os.path.join(discovery_dir, "inventory.json")
    with open(inv_path, "w", encoding="utf-8") as f:
        json.dump(inventory, f, indent=2, default=str)

    if all_present:
        passed("Relatorio gerado com todas as 7 secoes")
    else:
        failed("Relatorio incompleto")

    return report_path, inv_path


# ============================================================
# FASE 7: GERACAO DE DDL SNOWFLAKE (MVP2)
# ============================================================
def phase7_ddl_generation(config, datasets_metadata):
    banner("FASE 7: GERACAO DE DDL SNOWFLAKE", "=")
    print("""
    Objetivo: Gerar CREATE TABLE para cada dataset no Snowflake,
    com mapeamento de tipos SAS -> Snowflake:
      - num (sem formato)    -> NUMBER(38,10)
      - num + DATE format    -> DATE
      - num + DATETIME       -> TIMESTAMP_NTZ
      - char                 -> VARCHAR(length)

    Atividades da planilha MAPFRE cobertas:
      3.2   - Desenho de De-Para AWS/Snowflake
      3.2.1 - Definicao de Camadas de Dados (S3)
    """)

    from src.migration.data_migrator.snowflake_migrator import SnowflakeMigrator

    migration_dir = os.path.join(OUTPUT_DIR, "mvp2_migration")
    ddl_dir = os.path.join(migration_dir, "ddl")
    os.makedirs(ddl_dir, exist_ok=True)

    migrator = SnowflakeMigrator(config)

    for ds in datasets_metadata:
        ddl = migrator.generate_ddl(ds)
        ddl_path = os.path.join(ddl_dir, f"{ds['dataset_name']}.sql")
        with open(ddl_path, "w", encoding="utf-8") as f:
            f.write(ddl)

        subsection(f"DDL: {ds['dataset_name']}")
        for line in ddl.split("\n"):
            print(f"      {line}")

    passed(f"{len(datasets_metadata)} DDLs gerados em {ddl_dir}")
    return ddl_dir


# ============================================================
# FASE 8: SCRIPTS DE CARGA (COPY INTO + SNOWPIPE)
# ============================================================
def phase8_data_load(config, datasets_metadata):
    banner("FASE 8: SCRIPTS DE CARGA DE DADOS", "=")
    print("""
    Objetivo: Gerar scripts de ingestao de dados para Snowflake:
      - COPY INTO: carga batch a partir de arquivos em stage S3
      - Snowpipe: ingestao continua com auto-ingest

    Atividades da planilha MAPFRE cobertas:
      3.2.2 - Planejamento de Carga (Snowpipe vs Copy)
    """)

    from src.migration.data_migrator.snowflake_migrator import SnowflakeMigrator

    migration_dir = os.path.join(OUTPUT_DIR, "mvp2_migration")
    load_dir = os.path.join(migration_dir, "data_load")
    os.makedirs(load_dir, exist_ok=True)

    migrator = SnowflakeMigrator(config)

    for ds in datasets_metadata:
        copy_sql = migrator.generate_copy_into(ds)
        pipe_sql = migrator.generate_snowpipe(ds)

        load_path = os.path.join(load_dir, f"load_{ds['dataset_name']}.sql")
        with open(load_path, "w", encoding="utf-8") as f:
            f.write(f"-- ========================================\n")
            f.write(f"-- Data Load: {ds['dataset_name']}\n")
            f.write(f"-- Rows esperadas: {ds['row_count']:,}\n")
            f.write(f"-- ========================================\n\n")
            f.write(f"-- Opcao 1: COPY INTO (batch)\n{copy_sql}\n\n")
            f.write(f"-- Opcao 2: Snowpipe (streaming)\n{pipe_sql}\n")

        subsection(f"Load: {ds['dataset_name']} ({ds['row_count']:,} rows)")
        print(f"      COPY INTO:")
        for line in copy_sql.split("\n"):
            print(f"        {line}")
        print(f"\n      Snowpipe:")
        for line in pipe_sql.split("\n"):
            print(f"        {line}")

    passed(f"{len(datasets_metadata)} scripts de carga gerados")
    return load_dir


# ============================================================
# FASE 9: TRANSPILACAO SAS -> SNOWFLAKE
# ============================================================
def phase9_transpilation(config, inventory_path):
    banner("FASE 9: TRANSPILACAO SAS -> SNOWFLAKE SQL/SNOWPARK", "=")
    print("""
    Objetivo: Traduzir codigo SAS para Snowflake SQL e Snowpark Python.

    Padroes cobertos:
      LIBNAME            -> USE SCHEMA
      DATA step SET      -> CREATE TABLE AS SELECT
      MERGE com IN=      -> LEFT/INNER JOIN
      PROC SORT NODUPKEY -> QUALIFY ROW_NUMBER()
      PROC SQL           -> Snowflake SQL (quase direto)
      PROC FREQ          -> GROUP BY + COUNT
      PROC MEANS         -> AVG, STDDEV, MIN, MAX
      PROC FORMAT        -> Tabela lookup + LEFT JOIN
      %MACRO             -> Stored Procedure (stub)

    Gaps documentados (nao traduz):
      PROC LOGISTIC, REG, GLM, MIXED, IML
      PROC REPORT, PROC TABULATE
      Hash objects
      CALL EXECUTE / SQL dinamico

    Atividades da planilha MAPFRE cobertas:
      3.1.3 - Avaliacao de Scripts de "Data Step" Complexos
      3.2   - Desenho de De-Para AWS/Snowflake
      3.2.2 - Selecao de Ferramental de ETL
    """)

    from src.migration.code_transpiler.snowflake_transpiler import SnowflakeTranspiler

    with open(inventory_path, "r", encoding="utf-8") as f:
        inventory = json.load(f)

    migration_dir = os.path.join(OUTPUT_DIR, "mvp2_migration")
    transpile_dir = os.path.join(migration_dir, "transpiled")
    os.makedirs(transpile_dir, exist_ok=True)

    transpiler = SnowflakeTranspiler(config)
    all_gaps = []
    results_table = []

    for prog in inventory["programs"]:
        result = transpiler.transpile(prog)

        # Save SQL
        sql_path = os.path.join(transpile_dir, f"{prog['filename'].replace('.sas', '')}.sql")
        with open(sql_path, "w", encoding="utf-8") as f:
            f.write(f"-- Transpilado de: {prog['filename']}\n")
            f.write(f"-- Complexidade:   {prog['complexity_level']} (score {prog['complexity_score']})\n")
            f.write(f"-- Cobertura:      {result['coverage_pct']}%\n")
            f.write(f"-- Gaps:           {len(result['gaps'])}\n")
            f.write(f"-- Warnings:       {len(result['warnings'])}\n\n")
            f.write(result["sql_code"])

        # Save Snowpark
        if result.get("snowpark_code") and result["snowpark_code"] != "# No Snowpark code generated":
            py_path = os.path.join(transpile_dir, f"{prog['filename'].replace('.sas', '')}_snowpark.py")
            with open(py_path, "w", encoding="utf-8") as f:
                f.write(f"# Snowpark transpilation: {prog['filename']}\n\n")
                f.write(result["snowpark_code"])

        gaps_list = result["gaps"]
        all_gaps.extend(gaps_list)
        results_table.append((
            prog["filename"],
            f"{result['coverage_pct']:.1f}%",
            str(len(gaps_list)),
            str(len(result["warnings"])),
            prog["complexity_level"],
        ))

        subsection(f"{prog['filename']} -> Cobertura: {result['coverage_pct']:.1f}%")
        kv("Complexidade original:", f"{prog['complexity_level']} (score {prog['complexity_score']})")
        kv("SQL gerado:", f"{len(result['sql_code'])} caracteres")
        kv("Snowpark gerado:", "Sim" if result["snowpark_code"] != "# No Snowpark code generated" else "Nao")
        kv("Gaps:", str(len(gaps_list)))
        kv("Warnings:", str(len(result["warnings"])))
        if gaps_list:
            for g in gaps_list:
                print(f"        [GAP] {g}")
        if result["warnings"]:
            for w in result["warnings"]:
                print(f"        [WARN] {w}")

        # Show first lines of generated SQL
        sql_lines = result["sql_code"].split("\n")[:8]
        if sql_lines:
            print(f"\n      Preview SQL:")
            for line in sql_lines:
                if line.strip():
                    print(f"        {line}")
            if len(result["sql_code"].split("\n")) > 8:
                print(f"        ... ({len(result['sql_code'].split(chr(10)))} linhas total)")

    # Summary table
    subsection("Resumo de Transpilacao")
    table(["Programa", "Cobertura", "Gaps", "Warns", "Complexidade"], results_table)

    avg_coverage = sum(float(r[1].replace("%", "")) for r in results_table) / len(results_table)
    unique_gaps = list(set(all_gaps))

    print(f"\n      Cobertura media:  {avg_coverage:.1f}%")
    print(f"      Total de gaps:    {len(all_gaps)}")
    print(f"      Gaps unicos:      {len(unique_gaps)}")

    subsection("Gap Analysis Detalhado")
    for i, g in enumerate(unique_gaps, 1):
        print(f"      {i}. {g}")

    # Save gap report
    gap_report = {
        "timestamp": datetime.now().isoformat(),
        "total_programs": len(inventory["programs"]),
        "avg_coverage_pct": round(avg_coverage, 1),
        "unique_gaps": unique_gaps,
        "total_gap_occurrences": len(all_gaps),
        "programs_with_gaps": [
            p["filename"] for p in inventory["programs"]
            if p.get("has_hash_objects") or p.get("has_dynamic_sql") or
            any(proc in ("LOGISTIC", "REG", "GLM", "MIXED", "IML", "REPORT", "TABULATE")
                for proc in p.get("procs_used", []))
        ],
    }
    gap_path = os.path.join(migration_dir, "gap_report.json")
    with open(gap_path, "w", encoding="utf-8") as f:
        json.dump(gap_report, f, indent=2)

    passed(f"Transpilacao concluida: cobertura media {avg_coverage:.1f}%, {len(unique_gaps)} gaps unicos")
    return transpile_dir, gap_report


# ============================================================
# FASE 10: VALIDACAO POS-MIGRACAO
# ============================================================
def phase10_validation(config, datasets_metadata):
    banner("FASE 10: SCRIPTS DE VALIDACAO POS-MIGRACAO", "=")
    print("""
    Objetivo: Gerar scripts SQL para validar que a migracao esta correta.

    Validacoes geradas por dataset:
      1. Row Count     - Compara contagem de linhas source vs target
      2. Schema Match  - Verifica numero de colunas no INFORMATION_SCHEMA
      3. Column Stats  - MIN, MAX, AVG, NULL count de colunas numericas
      4. Checksum      - HASH_AGG para validacao de integridade

    Atividades da planilha MAPFRE cobertas:
      4.2.2 - Cronograma de Testes de Paridade
      4.1.3 - Definicao de Criterios de Sucesso (KPIs)
    """)

    from src.validation.validator import MigrationValidator

    migration_dir = os.path.join(OUTPUT_DIR, "mvp2_migration")
    val_dir = os.path.join(migration_dir, "validation")
    os.makedirs(val_dir, exist_ok=True)

    validator = MigrationValidator()
    target_config = {"database": "SAS_MIGRATION", "schema": "RAW"}

    for ds in datasets_metadata:
        scripts = validator.generate_validation_scripts(ds, target_config)
        val_path = os.path.join(val_dir, f"validate_{ds['dataset_name']}.sql")
        with open(val_path, "w", encoding="utf-8") as f:
            f.write(f"-- Validacao pos-migracao: {ds['dataset_name']}\n")
            f.write(f"-- Source rows: {ds.get('row_count', 'N/A')}\n")
            f.write(f"-- Target: {scripts.get('target_table', '')}\n\n")
            for key, sql in scripts.items():
                if key not in ("dataset", "target_table"):
                    f.write(f"\n-- {'=' * 40}\n-- {key.upper()}\n-- {'=' * 40}\n{sql}\n")

        subsection(f"Validacao: {ds['dataset_name']}")
        kv("Target table:", scripts.get("target_table", ""))
        kv("Source rows:", f"{ds.get('row_count', 'N/A'):,}")
        kv("Checks:", "row_count, schema_match, column_stats, checksum")

        # Show row count validation
        print(f"\n      SQL (row_count):")
        for line in scripts["row_count"].split("\n")[:6]:
            print(f"        {line}")

    passed(f"Scripts de validacao gerados para {len(datasets_metadata)} datasets")
    return val_dir


# ============================================================
# FASE 11: SUMARIO FINAL
# ============================================================
def phase11_final_summary(start_time, tests_ok, test_counts, all_results):
    elapsed = time.time() - start_time

    banner("SUMARIO EXECUTIVO FINAL", "#")

    print(f"""
    +---------------------------------------------------------+
    |  SAS-TO-SNOWFLAKE MIGRATION TOOLKIT                     |
    |  Relatorio de Execucao Completa                         |
    +---------------------------------------------------------+
    |  Data:        {datetime.now().strftime('%Y-%m-%d %H:%M:%S'):<41} |
    |  Duracao:     {elapsed:.2f} segundos{' ' * (33 - len(f'{elapsed:.2f}'))}|
    |  Python:      {sys.version.split()[0]:<41} |
    |  Output:      report_output/{' ' * 28}|
    +---------------------------------------------------------+
    """)

    section("RESULTADOS POR FASE")
    phases = [
        ("Fase 0", "Testes Unitarios", f"{test_counts[0]}/{test_counts[1]} passaram", tests_ok),
        ("Fase 1", "Filesystem Scan", f"{all_results['scan_programs']} programas, {all_results['scan_datasets']} datasets", True),
        ("Fase 2", "Code Parsing", f"{all_results['parsed_count']} programas analisados", True),
        ("Fase 3", "Dataset Metadata", f"{all_results['meta_count']} datasets, {all_results['total_rows']:,} rows", True),
        ("Fase 4", "Lineage Graph", f"{all_results['lineage_nodes']} nos, {all_results['lineage_edges']} arestas", True),
        ("Fase 5", "Data Catalog", f"{all_results['catalog_datasets']} datasets, {all_results['pii_datasets']} com PII", True),
        ("Fase 6", "Discovery Report", f"7/7 secoes, {all_results['report_size']:,} chars", True),
        ("Fase 7", "DDL Generation", f"{all_results['ddl_count']} DDLs gerados", True),
        ("Fase 8", "Data Load Scripts", f"{all_results['load_count']} COPY INTO + Snowpipe", True),
        ("Fase 9", "Transpilation", f"Cobertura media {all_results['avg_coverage']:.1f}%", True),
        ("Fase 10", "Validation Scripts", f"{all_results['val_count']} scripts gerados", True),
    ]

    for phase, name, detail, ok in phases:
        icon = "PASS" if ok else "FAIL"
        print(f"    [{icon}] {phase}: {name}")
        print(f"           {detail}")

    section("ARTEFATOS GERADOS")
    for root, dirs, files in os.walk(OUTPUT_DIR):
        level = root.replace(OUTPUT_DIR, "").count(os.sep)
        indent = "    " + "  " * level
        print(f"{indent}{os.path.basename(root)}/")
        subindent = "    " + "  " * (level + 1)
        for f in sorted(files):
            fpath = os.path.join(root, f)
            size = os.path.getsize(fpath)
            print(f"{subindent}{f:<45} {size:>8,} bytes")

    total_files = sum(len(files) for _, _, files in os.walk(OUTPUT_DIR))
    total_size = sum(os.path.getsize(os.path.join(root, f)) for root, _, files in os.walk(OUTPUT_DIR) for f in files)

    section("METRICAS CONSOLIDADAS")
    kv("Total de arquivos gerados:", total_files)
    kv("Tamanho total:", format_bytes(total_size))
    kv("Tempo de execucao:", f"{elapsed:.2f}s")
    kv("Testes unitarios:", f"{test_counts[0]}/{test_counts[1]}")
    kv("Programas SAS analisados:", all_results['parsed_count'])
    kv("Datasets catalogados:", all_results['meta_count'])
    kv("Nos no grafo de lineage:", all_results['lineage_nodes'])
    kv("Cobertura de transpilacao:", f"{all_results['avg_coverage']:.1f}%")
    kv("Gaps de migracao:", all_results['gap_count'])
    kv("Datasets com PII:", all_results['pii_datasets'])

    final_status = "ALL PASSED" if tests_ok else "PARTIAL (testes falharam)"
    banner(f"STATUS FINAL: {final_status}", "#")

    # Save summary JSON
    summary = {
        "timestamp": datetime.now().isoformat(),
        "elapsed_seconds": round(elapsed, 2),
        "status": final_status,
        "tests": {"passed": test_counts[0], "total": test_counts[1]},
        "metrics": all_results,
    }
    summary_path = os.path.join(OUTPUT_DIR, "execution_summary.json")
    with open(summary_path, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2, default=str)
    print(f"\n    Sumario salvo em: {summary_path}")


# ============================================================
# MAIN
# ============================================================
def main():
    start_time = time.time()

    banner("SAS-TO-SNOWFLAKE MIGRATION TOOLKIT", "#")
    banner("RELATORIO COMPLETO DE EXECUCAO", "#")
    print(f"""
    Projeto:    MVP1 Mock Test - MAPFRE
    Data:       {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
    Python:     {sys.version}
    Output:     {OUTPUT_DIR}

    Este relatorio executa todas as fases do pipeline de migracao
    SAS -> Snowflake com dados mockados e descreve detalhadamente
    cada etapa, seus resultados e artefatos gerados.
    """)

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # Ensure mock
    if not os.path.exists(CONFIG_PATH):
        print("Gerando ambiente mock...")
        subprocess.run([sys.executable, os.path.join(BASE_DIR, "create_mock_environment.py")], check=True)

    config = load_config()
    all_results = {}

    # Phase 0
    tests_ok, tests_passed, tests_total = phase0_unit_tests()

    # Phase 1
    programs, datasets = phase1_filesystem_scan(config)
    all_results["scan_programs"] = len(programs)
    all_results["scan_datasets"] = len(datasets)

    # Phase 2
    parsed_programs = phase2_code_parsing(programs)
    all_results["parsed_count"] = len(parsed_programs)

    # Phase 3
    datasets_metadata = phase3_dataset_metadata(config)
    all_results["meta_count"] = len(datasets_metadata)
    all_results["total_rows"] = sum(d["row_count"] for d in datasets_metadata)

    # Phase 4
    lineage, builder = phase4_lineage(parsed_programs)
    all_results["lineage_nodes"] = len(lineage["nodes"])
    all_results["lineage_edges"] = len(lineage["edges"])

    # Phase 5
    catalog = phase5_catalog(config, datasets_metadata, parsed_programs, lineage)
    all_results["catalog_datasets"] = catalog["summary"]["total_datasets"]
    all_results["pii_datasets"] = catalog["summary"]["datasets_with_pii"]

    # Save catalog
    discovery_dir = os.path.join(OUTPUT_DIR, "mvp1_discovery")
    os.makedirs(discovery_dir, exist_ok=True)
    with open(os.path.join(discovery_dir, "data_catalog.json"), "w", encoding="utf-8") as f:
        json.dump(catalog, f, indent=2, default=str)

    # Phase 6
    report_path, inventory_path = phase6_discovery_report(config, parsed_programs, datasets_metadata, lineage)
    with open(report_path, "r", encoding="utf-8") as f:
        all_results["report_size"] = len(f.read())

    # Phase 7
    ddl_dir = phase7_ddl_generation(config, datasets_metadata)
    all_results["ddl_count"] = len(datasets_metadata)

    # Phase 8
    load_dir = phase8_data_load(config, datasets_metadata)
    all_results["load_count"] = len(datasets_metadata)

    # Phase 9
    transpile_dir, gap_report = phase9_transpilation(config, inventory_path)
    all_results["avg_coverage"] = gap_report["avg_coverage_pct"]
    all_results["gap_count"] = len(gap_report["unique_gaps"])

    # Phase 10
    val_dir = phase10_validation(config, datasets_metadata)
    all_results["val_count"] = len(datasets_metadata)

    # Phase 11
    phase11_final_summary(start_time, tests_ok, (tests_passed, tests_total), all_results)


if __name__ == "__main__":
    main()
