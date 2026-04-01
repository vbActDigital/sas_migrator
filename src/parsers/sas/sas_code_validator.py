"""
SAS Code Validator - Deep integrity analysis for SAS programs.

Detects:
- Undefined variables in KEEP/formulas (especially accounting vars C-prefix)
- Self-reimport patterns (export then reimport own data)
- Missing processing sections across related programs (Direto/Aceito/Resumo/Conciliacao)
- Incomplete value mappings in IF/ELSE IF chains
- Formula integrity issues (variables used but never assigned)
"""

import os
import re
from typing import Dict, List, Set, Tuple, Optional

from src.utils.logger import get_logger

logger = get_logger("sas_code_validator")

# ---------------------------------------------------------------------------
# Regex patterns for deep analysis
# ---------------------------------------------------------------------------
RE_DATA_BLOCK = re.compile(
    r'\bDATA\s+([^;]+);(.*?)\bRUN\s*;', re.IGNORECASE | re.DOTALL
)
RE_SET_STMT = re.compile(r'\bSET\s+([\w."&]+)', re.IGNORECASE)
RE_ASSIGNMENT = re.compile(r'^\s*([A-Za-z_]\w*)\s*=[^=]', re.MULTILINE)
RE_KEEP_STMT = re.compile(r'\bKEEP\s+([\w\s]+);', re.IGNORECASE)
RE_KEEP_OPTION = re.compile(r'\(\s*keep\s*=\s*([\w\s]+)\)', re.IGNORECASE)
RE_DROP_STMT = re.compile(r'\bDROP\s+([\w\s]+);', re.IGNORECASE)
RE_FORMAT_BLOCK = re.compile(r'\bFORMAT\b\s+(.*?);', re.IGNORECASE | re.DOTALL)
RE_LENGTH_BLOCK = re.compile(r'\bLENGTH\b\s+(.*?);', re.IGNORECASE | re.DOTALL)
RE_ATTRIB_BLOCK = re.compile(r'\bATTRIB\b\s+(.*?);', re.IGNORECASE | re.DOTALL)
RE_ACCOUNTING_VAR = re.compile(r'\bC\d{7,}\b')
RE_PROC_EXPORT_OUTFILE = re.compile(
    r'PROC\s+EXPORT.*?OUTFILE\s*=\s*[\'"]?([^;\'"]+)',
    re.IGNORECASE | re.DOTALL
)
RE_INFILE_PATH = re.compile(r'INFILE\s+[\'"]([^;\'"]+)', re.IGNORECASE)
RE_PROC_IMPORT_DATAFILE = re.compile(
    r'PROC\s+IMPORT.*?DATAFILE\s*=\s*[\'"]?([^;\'"]+)',
    re.IGNORECASE | re.DOTALL
)
RE_SQL_CREATE = re.compile(
    r'CREATE\s+TABLE\s+([\w.]+)\s+AS\s+SELECT', re.IGNORECASE
)
RE_IF_MAPPING = re.compile(
    r'(?:ELSE\s+)?IF\s+(\w+)\s*=\s*[\'"]([^\'"]+)[\'"]\s*THEN\s+(\w+)\s*=\s*[\'"]([^\'"]+)[\'"]',
    re.IGNORECASE
)
RE_SECTION_DIRETO = re.compile(
    r'cod_tp_emissao\s*=\s*[\'"]D[\'"]', re.IGNORECASE
)
RE_SECTION_ACEITO = re.compile(
    r'cod_tp_emissao\s*=\s*[\'"]A[\'"]', re.IGNORECASE
)
RE_SECTION_RESUMO = re.compile(
    r'resumo.*aging|RESUMO_AGING', re.IGNORECASE
)
RE_SECTION_CONCILIACAO = re.compile(
    r'concilia[çc][ãa]o', re.IGNORECASE
)
RE_FORMAT_SPEC_TOKEN = re.compile(
    r'(?:COMMAX|COMMA|DDMMYY|MMDDYY|YYMMDD|DATE|DATETIME|BEST|DOLLAR|PERCENT|'
    r'\$?CHAR|Z|FORMAT|INFORMAT)\w*\d*\.?\d*',
    re.IGNORECASE
)

# Known SAS keywords that are NOT variable names
SAS_KEYWORDS = {
    'IF', 'THEN', 'ELSE', 'DO', 'END', 'SET', 'MERGE', 'BY', 'WHERE', 'AND',
    'OR', 'NOT', 'IN', 'AS', 'FROM', 'SELECT', 'ON', 'OUTPUT', 'DELETE',
    'KEEP', 'DROP', 'RENAME', 'FORMAT', 'INFORMAT', 'LENGTH', 'ATTRIB',
    'LABEL', 'RUN', 'QUIT', 'DATA', 'PROC', 'NOTIN', 'CALCULATED',
}


class SASCodeValidator:
    """Deep integrity validator for SAS programs."""

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    def validate_program(self, filepath: str, parsed_data: Dict) -> List[Dict]:
        """Run all single-program validations. Returns list of findings."""
        findings: List[Dict] = []
        filename = os.path.basename(filepath)

        try:
            with open(filepath, "r", encoding="utf-8", errors="replace") as f:
                content = f.read()
        except Exception as e:
            logger.error("Cannot read %s for validation: %s", filepath, e)
            return findings

        # Strip SAS comments before structural parsing (preserving line numbers)
        clean_content = self._strip_comments(content)

        # Build variable flow map for the program
        data_steps = self._parse_data_step_scopes(clean_content)
        sql_tables = self._parse_sql_creates(clean_content)

        # Build cumulative variable registry (dataset -> known vars)
        var_registry = self._build_variable_registry(data_steps, sql_tables)

        # 1. Check accounting variable integrity
        findings.extend(
            self._check_accounting_vars(clean_content, data_steps, var_registry, filename)
        )

        # 2. Check KEEP integrity
        findings.extend(
            self._check_keep_integrity(data_steps, var_registry, filename)
        )

        # 3. Check self-reimport
        findings.extend(
            self._check_self_reimport(clean_content, parsed_data, filename)
        )

        # 4. Check incomplete value mappings
        findings.extend(
            self._check_incomplete_mappings(clean_content, filename)
        )

        # 5. Check formula integrity (variables in formula RHS never defined)
        findings.extend(
            self._check_formula_integrity(data_steps, var_registry, filename)
        )

        return findings

    def validate_cross_program(
        self, parsed_programs: List[Dict]
    ) -> List[Dict]:
        """Run cross-program comparisons. Returns list of findings."""
        findings: List[Dict] = []

        # Load content for each program
        contents: Dict[str, str] = {}
        for prog in parsed_programs:
            fp = prog.get("filepath", "")
            if fp and os.path.isfile(fp):
                try:
                    with open(fp, "r", encoding="utf-8", errors="replace") as f:
                        contents[prog["filename"]] = f.read()
                except Exception:
                    pass

        # 1. Compare processing sections (Direto/Aceito/Resumo/Conciliacao)
        findings.extend(
            self._compare_sections(parsed_programs, contents)
        )

        # 2. Compare accounting variable coverage
        findings.extend(
            self._compare_accounting_vars(parsed_programs, contents)
        )

        # 3. Compare value mapping coverage
        findings.extend(
            self._compare_mapping_coverage(parsed_programs, contents)
        )

        # 4. Compare export patterns
        findings.extend(
            self._compare_exports(parsed_programs)
        )

        return findings

    # ------------------------------------------------------------------
    # DATA step scope parsing
    # ------------------------------------------------------------------
    def _parse_data_step_scopes(self, content: str) -> List[Dict]:
        """Parse DATA...RUN blocks with variable definitions."""
        steps = []
        for m in RE_DATA_BLOCK.finditer(content):
            target_raw = m.group(1).strip()
            body = m.group(2)
            start_line = content[: m.start()].count("\n") + 1

            # Clean target name (remove options like (keep=...))
            target = re.sub(r'\(.*?\)', '', target_raw).strip().split()[0] if target_raw else ""

            # SET sources
            set_sources = [
                s.strip().strip('"').strip("'")
                for s in RE_SET_STMT.findall(body)
            ]

            # Variables explicitly assigned (left of =)
            assigned = {
                am.group(1).upper()
                for am in RE_ASSIGNMENT.finditer(body)
                if am.group(1).upper() not in SAS_KEYWORDS
            }

            # Variables from LENGTH declarations
            length_vars = self._extract_declared_vars_from_block(body, RE_LENGTH_BLOCK)

            # Variables from FORMAT declarations
            format_vars = self._extract_format_vars(body)

            # Variables from ATTRIB declarations
            attrib_vars = self._extract_declared_vars_from_block(body, RE_ATTRIB_BLOCK)

            # KEEP variables
            keep_vars: Set[str] = set()
            for km in RE_KEEP_STMT.finditer(body):
                for v in re.findall(r'\b([A-Za-z_]\w*)\b', km.group(1)):
                    if v.upper() not in SAS_KEYWORDS:
                        keep_vars.add(v.upper())
            # Also from DATA options: (keep=...)
            for km in RE_KEEP_OPTION.finditer(target_raw):
                for v in re.findall(r'\b([A-Za-z_]\w*)\b', km.group(1)):
                    if v.upper() not in SAS_KEYWORDS:
                        keep_vars.add(v.upper())

            # DROP variables
            drop_vars: Set[str] = set()
            for dm in RE_DROP_STMT.finditer(body):
                for v in re.findall(r'\b([A-Za-z_]\w*)\b', dm.group(1)):
                    if v.upper() not in SAS_KEYWORDS:
                        drop_vars.add(v.upper())

            # All locally defined variables
            defined = assigned | length_vars | format_vars | attrib_vars

            # Accounting variables referenced in body
            acct_referenced = {v.upper() for v in RE_ACCOUNTING_VAR.findall(body)}

            steps.append({
                "target": target.upper(),
                "start_line": start_line,
                "set_sources": [s.upper() for s in set_sources],
                "assigned": assigned,
                "length_vars": length_vars,
                "format_vars": format_vars,
                "attrib_vars": attrib_vars,
                "keep_vars": keep_vars,
                "drop_vars": drop_vars,
                "defined": defined,
                "acct_referenced": acct_referenced,
                "body": body,
            })
        return steps

    def _parse_sql_creates(self, content: str) -> List[Dict]:
        """Parse PROC SQL CREATE TABLE statements to identify created tables."""
        results = []
        for m in RE_SQL_CREATE.finditer(content):
            table_name = m.group(1).strip().upper()
            # Get the full SQL block to find selected columns
            start = m.start()
            end_match = re.search(r';\s*$|QUIT', content[start:], re.IGNORECASE | re.MULTILINE)
            sql_body = content[start: start + end_match.start()] if end_match else content[start: start + 2000]

            # Extract column aliases (AS alias_name)
            aliases = {
                am.group(1).upper()
                for am in re.finditer(r'\bAS\s+(\w+)', sql_body, re.IGNORECASE)
                if am.group(1).upper() not in SAS_KEYWORDS
            }
            results.append({
                "table": table_name,
                "start_line": content[:start].count("\n") + 1,
                "aliases": aliases,
            })
        return results

    # ------------------------------------------------------------------
    # Variable registry
    # ------------------------------------------------------------------
    def _build_variable_registry(
        self, data_steps: List[Dict], sql_tables: List[Dict]
    ) -> Dict[str, Set[str]]:
        """Build a map of dataset_name -> known variables."""
        registry: Dict[str, Set[str]] = {}

        for step in data_steps:
            target = step["target"]
            vars_set = set(step["defined"])
            # Inherit from SET sources
            for src in step["set_sources"]:
                src_clean = re.sub(r'\(.*?\)', '', src).strip().upper()
                if src_clean in registry:
                    vars_set |= registry[src_clean]
            registry[target] = vars_set

        for sql in sql_tables:
            table = sql["table"]
            if table not in registry:
                registry[table] = set()
            registry[table] |= sql["aliases"]

        return registry

    # ------------------------------------------------------------------
    # Validation checks
    # ------------------------------------------------------------------
    def _check_accounting_vars(
        self, content: str, data_steps: List[Dict],
        var_registry: Dict[str, Set[str]], filename: str
    ) -> List[Dict]:
        """Check accounting variables (C-prefix) are properly defined before use."""
        findings = []

        # Collect all accounting vars that are ASSIGNED anywhere in the program
        all_assigned_acct: Set[str] = set()
        for step in data_steps:
            for v in step["assigned"]:
                if RE_ACCOUNTING_VAR.match(v):
                    all_assigned_acct.add(v)

        for step in data_steps:
            # Accounting vars referenced in formulas but never assigned in this
            # step AND not assigned anywhere upstream
            for var in step["acct_referenced"]:
                if var in step["assigned"]:
                    continue  # defined here, OK

                # Check if inherited from SET source
                inherited = False
                for src in step["set_sources"]:
                    src_clean = re.sub(r'\(.*?\)', '', src).strip().upper()
                    if src_clean in var_registry and var in var_registry[src_clean]:
                        inherited = True
                        break

                if inherited:
                    continue

                # Check if it's assigned ANYWHERE in the program
                if var in all_assigned_acct:
                    # It exists somewhere but not reachable through SET chain
                    # Check if it's in KEEP or formula (right side of =)
                    if var in step.get("keep_vars", set()):
                        findings.append(self._make_finding(
                            severity="CRITICAL",
                            category="VARIAVEL_CONTABIL_INDEFINIDA",
                            program=filename,
                            line=step["start_line"],
                            description=(
                                f"Variavel contabil '{var}' esta no KEEP do DATA step "
                                f"'{step['target']}' mas NAO e calculada neste step nem "
                                f"herdada via SET de nenhuma fonte que a defina."
                            ),
                            impact=(
                                f"A variavel tera valor missing (.) no output, "
                                f"corrompendo calculos contabeis e arquivos SAP exportados."
                            ),
                            recommendation=(
                                f"Adicionar o calculo de '{var}' no DATA step "
                                f"'{step['target']}' ou em um step anterior que alimente "
                                f"este via SET. Verificar programas relacionados que "
                                f"possuem essa formula para referencia."
                            ),
                            recommendation_type="CORRECAO_CODIGO",
                        ))
                else:
                    # Variable never assigned anywhere in the program
                    if var in step.get("keep_vars", set()) or self._var_in_formula_rhs(var, step["body"]):
                        findings.append(self._make_finding(
                            severity="CRITICAL",
                            category="VARIAVEL_CONTABIL_INEXISTENTE",
                            program=filename,
                            line=step["start_line"],
                            description=(
                                f"Variavel contabil '{var}' e referenciada no DATA step "
                                f"'{step['target']}' (em formula ou KEEP) mas NAO e "
                                f"calculada em NENHUM ponto do programa."
                            ),
                            impact=(
                                f"Valor sera missing (.) em toda a cadeia de calculos. "
                                f"Formulas que dependem de '{var}' produzirao resultados "
                                f"incorretos. Arquivos SAP exportados terao valores errados."
                            ),
                            recommendation=(
                                f"EXTRACAO MANUAL: Este calculo precisa ser adicionado ao "
                                f"programa. Consultar programas relacionados (ex: MSG vs "
                                f"MVIDA) para obter a formula correta. Em geral, variaveis "
                                f"C-prefix sao contas contabeis que devem ser calculadas "
                                f"explicitamente."
                            ),
                            recommendation_type="EXTRACAO_MANUAL",
                        ))
        return findings

    def _check_keep_integrity(
        self, data_steps: List[Dict], var_registry: Dict[str, Set[str]],
        filename: str
    ) -> List[Dict]:
        """Check KEEP variables exist in the scope."""
        findings = []

        for step in data_steps:
            if not step["keep_vars"]:
                continue

            # Variables available = locally defined + inherited from SET
            available = set(step["defined"])
            for src in step["set_sources"]:
                src_clean = re.sub(r'\(.*?\)', '', src).strip().upper()
                if src_clean in var_registry:
                    available |= var_registry[src_clean]

            missing_in_keep = step["keep_vars"] - available
            # Filter out non-accounting variables (they might come from
            # datasets we can't trace, so only flag accounting vars and
            # vars that look like they should be calculated)
            suspicious = {
                v for v in missing_in_keep
                if RE_ACCOUNTING_VAR.match(v)
            }

            for var in suspicious:
                findings.append(self._make_finding(
                    severity="HIGH",
                    category="KEEP_VARIAVEL_INDEFINIDA",
                    program=filename,
                    line=step["start_line"],
                    description=(
                        f"Variavel '{var}' aparece no KEEP do DATA step "
                        f"'{step['target']}' mas nao foi encontrada como "
                        f"definida neste step nem nas fontes SET."
                    ),
                    impact=(
                        f"Coluna '{var}' sera exportada com valores missing, "
                        f"gerando dados incorretos no output."
                    ),
                    recommendation=(
                        f"Verificar se a variavel '{var}' deveria ser "
                        f"calculada neste DATA step. Comparar com programas "
                        f"similares para identificar a formula ausente."
                    ),
                    recommendation_type="CORRECAO_CODIGO",
                ))
        return findings

    def _check_formula_integrity(
        self, data_steps: List[Dict], var_registry: Dict[str, Set[str]],
        filename: str
    ) -> List[Dict]:
        """Check variables used in formula RHS that are never defined."""
        findings = []

        for step in data_steps:
            available = set(step["defined"])
            for src in step["set_sources"]:
                src_clean = re.sub(r'\(.*?\)', '', src).strip().upper()
                if src_clean in var_registry:
                    available |= var_registry[src_clean]

            # For each assignment, check RHS variable references
            for am in re.finditer(
                r'^\s*([A-Za-z_]\w*)\s*=\s*(.+?);\s*$', step["body"], re.MULTILINE
            ):
                lhs = am.group(1).upper()
                rhs = am.group(2)

                # Extract variable references from RHS
                rhs_vars = {
                    v.upper() for v in re.findall(r'\b([A-Za-z_]\w*)\b', rhs)
                    if v.upper() not in SAS_KEYWORDS
                    and not re.match(r'^\d', v)
                    and v.upper() != lhs
                    # Skip SAS functions
                    and v.upper() not in {
                        'IFN', 'IFC', 'SUM', 'MIN', 'MAX', 'ABS', 'PUT', 'INPUT',
                        'CATS', 'SUBSTR', 'YEAR', 'MONTH', 'MDY', 'CALCULATED',
                        'COMPRESS', 'STRIP', 'TRIM', 'LEFT', 'RIGHT', 'UPCASE',
                        'LOWCASE', 'INT', 'ROUND', 'FLOOR', 'CEIL', 'LOG', 'EXP',
                        'MEAN', 'STD', 'MISSING', 'COALESCE', 'SCAN', 'COUNT',
                        'INDEX', 'FIND', 'TRANWRD', 'TRANSLATE', 'VERIFY',
                    }
                }

                # Only flag accounting variables missing from RHS
                for var in rhs_vars:
                    if RE_ACCOUNTING_VAR.match(var) and var not in available:
                        findings.append(self._make_finding(
                            severity="CRITICAL",
                            category="FORMULA_REFERENCIA_INDEFINIDA",
                            program=filename,
                            line=step["start_line"],
                            description=(
                                f"Formula '{lhs} = ...' referencia variavel contabil "
                                f"'{var}' que nao esta definida neste escopo nem "
                                f"nas fontes SET do DATA step '{step['target']}'."
                            ),
                            impact=(
                                f"O calculo de '{lhs}' usara valor missing para "
                                f"'{var}', produzindo resultado incorreto em toda a "
                                f"cadeia contabil."
                            ),
                            recommendation=(
                                f"EXTRACAO MANUAL: Adicionar o calculo de '{var}' antes "
                                f"desta formula, ou no DATA step que alimenta "
                                f"'{step['target']}' via SET. Consultar programas "
                                f"relacionados para a formula correta."
                            ),
                            recommendation_type="EXTRACAO_MANUAL",
                        ))
        return findings

    def _check_self_reimport(
        self, content: str, parsed_data: Dict, filename: str
    ) -> List[Dict]:
        """Detect when a program exports data then reimports it."""
        findings = []

        # Collect export outfile stems
        export_stems: List[Tuple[str, str]] = []
        for m in RE_PROC_EXPORT_OUTFILE.finditer(content):
            path = m.group(1).strip()
            stem = self._path_stem(path)
            if stem:
                export_stems.append((stem, path))

        # Collect infile/import datafile stems
        import_stems: List[Tuple[str, str]] = []
        for m in RE_INFILE_PATH.finditer(content):
            path = m.group(1).strip()
            stem = self._path_stem(path)
            if stem:
                import_stems.append((stem, path))
        for m in RE_PROC_IMPORT_DATAFILE.finditer(content):
            path = m.group(1).strip()
            stem = self._path_stem(path)
            if stem:
                import_stems.append((stem, path))

        # Check overlap
        for exp_stem, exp_path in export_stems:
            for imp_stem, imp_path in import_stems:
                if exp_stem and imp_stem and exp_stem == imp_stem:
                    findings.append(self._make_finding(
                        severity="HIGH",
                        category="AUTO_REIMPORTACAO",
                        program=filename,
                        line=0,
                        description=(
                            f"O programa exporta dados para '{exp_path}' e depois "
                            f"reimporta o mesmo arquivo via INFILE/PROC IMPORT "
                            f"('{imp_path}'). Isso cria dependencia circular."
                        ),
                        impact=(
                            f"Risco de perda de precisao numerica na conversao "
                            f"para CSV/texto. Se o export falhar ou estiver "
                            f"desatualizado, o reimport usara dados incorretos. "
                            f"Variaveis calculadas (como _atu) podem nao estar "
                            f"disponiveis na reimportacao."
                        ),
                        recommendation=(
                            f"CONFIGURACAO EXPORT: Substituir a reimportacao do "
                            f"arquivo por uso direto da tabela WORK em memoria. "
                            f"Se a reimportacao e necessaria para conciliacao, "
                            f"garantir que o export use formato que preserve "
                            f"precisao (XLSX em vez de CSV) e que todas as "
                            f"variaveis necessarias estejam no export."
                        ),
                        recommendation_type="CONFIGURACAO_EXPORT",
                    ))
        return findings

    def _check_incomplete_mappings(
        self, content: str, filename: str
    ) -> List[Dict]:
        """Check IF/ELSE IF chains that map values but may miss cases."""
        findings = []

        # Find IF/ELSE IF chains that map a source variable to a target
        # Group by (source_var, target_var) to find all covered values
        mapping_chains: Dict[Tuple[str, str], List[Dict]] = {}

        for m in RE_IF_MAPPING.finditer(content):
            src_var = m.group(1).upper()
            src_val = m.group(2)
            tgt_var = m.group(3).upper()
            tgt_val = m.group(4)
            line = content[: m.start()].count("\n") + 1

            key = (src_var, tgt_var)
            if key not in mapping_chains:
                mapping_chains[key] = []
            mapping_chains[key].append({
                "src_val": src_val, "tgt_val": tgt_val, "line": line
            })

        # Check for COD_SISTEMA_ORIGEM mappings specifically
        for (src_var, tgt_var), mappings in mapping_chains.items():
            if "SISTEMA_ORIGEM" not in src_var:
                continue

            covered_values = {m["src_val"] for m in mappings}
            first_line = min(m["line"] for m in mappings)

            # Check if there's a catch-all ELSE (non-commented)
            # Look for ELSE without IF after the chain
            chain_end = max(m["line"] for m in mappings)
            lines = content.splitlines()
            has_else = False
            for i in range(chain_end, min(chain_end + 5, len(lines))):
                line_text = lines[i] if i < len(lines) else ""
                if re.search(r'^\s*ELSE\s+' + re.escape(tgt_var), line_text, re.IGNORECASE):
                    has_else = True
                    break
                # Check for commented ELSE
                if re.search(r'/\*.*ELSE.*\*/', line_text, re.IGNORECASE):
                    findings.append(self._make_finding(
                        severity="MEDIUM",
                        category="MAPEAMENTO_ELSE_COMENTADO",
                        program=filename,
                        line=i + 1,
                        description=(
                            f"O ELSE final da cadeia IF/ELSE IF para "
                            f"'{src_var}' -> '{tgt_var}' esta comentado. "
                            f"Valores nao mapeados: sem tratamento."
                        ),
                        impact=(
                            f"Registros com valores de '{src_var}' fora da "
                            f"lista [{', '.join(sorted(covered_values))}] "
                            f"terao '{tgt_var}' vazio, causando SEGMENTO "
                            f"incompleto no arquivo SAP."
                        ),
                        recommendation=(
                            f"REVISAO MANUAL: Descomentar o ELSE ou adicionar "
                            f"tratamento explicito para valores nao previstos. "
                            f"Considerar log/flag para novos sistemas de origem."
                        ),
                        recommendation_type="REVISAO_MANUAL",
                    ))
                    has_else = True  # counted as found (commented)
                    break

            if not has_else and len(covered_values) > 0:
                findings.append(self._make_finding(
                    severity="MEDIUM",
                    category="MAPEAMENTO_SEM_ELSE",
                    program=filename,
                    line=first_line,
                    description=(
                        f"Cadeia IF/ELSE IF para '{src_var}' -> '{tgt_var}' "
                        f"cobre {len(covered_values)} valores mas nao possui "
                        f"ELSE final para tratar valores inesperados."
                    ),
                    impact=(
                        f"Novos valores de '{src_var}' nao serao mapeados, "
                        f"resultando em '{tgt_var}' vazio."
                    ),
                    recommendation=(
                        f"CONFIGURACAO EXPORT: Adicionar ELSE com valor "
                        f"padrao ou flag de erro para identificar valores "
                        f"nao mapeados durante execucao."
                    ),
                    recommendation_type="CONFIGURACAO_EXPORT",
                ))
        return findings

    # ------------------------------------------------------------------
    # Cross-program validations
    # ------------------------------------------------------------------
    def _compare_sections(
        self, parsed_programs: List[Dict], contents: Dict[str, str]
    ) -> List[Dict]:
        """Compare processing sections across related programs."""
        findings = []

        # Identify which sections each program has
        program_sections: Dict[str, Dict[str, bool]] = {}
        for prog in parsed_programs:
            fn = prog["filename"]
            if fn not in contents:
                continue
            content = contents[fn]
            program_sections[fn] = {
                "direto": bool(RE_SECTION_DIRETO.search(content)),
                "aceito": bool(RE_SECTION_ACEITO.search(content)),
                "resumo": bool(RE_SECTION_RESUMO.search(content)),
                "conciliacao": bool(RE_SECTION_CONCILIACAO.search(content)),
            }

        if len(program_sections) < 2:
            return findings

        # Find related programs (share common structure: both have "direto")
        direto_programs = [
            fn for fn, secs in program_sections.items() if secs["direto"]
        ]

        if len(direto_programs) < 2:
            return findings

        # For each section type, check if all related programs have it
        section_labels = {
            "aceito": "RVR Aceito (emissao aceita, cod_tp_emissao='A')",
            "resumo": "Resumo Aging (tabela de resumo por aging/segmento)",
            "conciliacao": "Conciliacao (tabela de verificacao cruzada)",
        }

        for section_key, section_desc in section_labels.items():
            has_it = [fn for fn in direto_programs if program_sections[fn].get(section_key)]
            missing = [fn for fn in direto_programs if not program_sections[fn].get(section_key)]

            if has_it and missing:
                findings.append(self._make_finding(
                    severity="CRITICAL",
                    category="SECAO_AUSENTE",
                    program=", ".join(missing),
                    line=0,
                    description=(
                        f"Secao '{section_desc}' existe em "
                        f"[{', '.join(has_it)}] mas esta AUSENTE em "
                        f"[{', '.join(missing)}]. Ambos os programas "
                        f"processam RVR Direto, indicando que a secao "
                        f"deveria existir em todos."
                    ),
                    impact=(
                        f"Os programas sem esta secao NAO geram os outputs "
                        f"correspondentes (arquivo SAP, base analitica, "
                        f"conciliacao). Isso resulta em dados incompletos "
                        f"na contabilizacao."
                    ),
                    recommendation=(
                        f"EXTRACAO MANUAL: Implementar a secao '{section_key}' "
                        f"nos programas que a nao possuem, usando como "
                        f"referencia os programas que ja a tem. Atentar para "
                        f"diferencas de contas contabeis, percentuais de "
                        f"resseguro e codigos de empresa entre as entidades."
                    ),
                    recommendation_type="EXTRACAO_MANUAL",
                ))
        return findings

    def _compare_accounting_vars(
        self, parsed_programs: List[Dict], contents: Dict[str, str]
    ) -> List[Dict]:
        """Compare accounting variable coverage across related programs."""
        findings = []

        # For each program, find all accounting vars that are ASSIGNED
        program_acct_vars: Dict[str, Set[str]] = {}
        for prog in parsed_programs:
            fn = prog["filename"]
            if fn not in contents:
                continue
            content = contents[fn]
            assigned = set()
            for m in re.finditer(r'^\s*(C\d{7,})\s*=', content, re.MULTILINE):
                assigned.add(m.group(1).upper())
            if assigned:
                program_acct_vars[fn] = assigned

        # Only compare programs that have a significant number of accounting vars
        # AND process RVR (have direto section) - skip auxiliary programs
        direto_programs = set()
        for prog in parsed_programs:
            fn = prog["filename"]
            if fn in contents and RE_SECTION_DIRETO.search(contents[fn]):
                direto_programs.add(fn)

        main_acct = {
            fn: vs for fn, vs in program_acct_vars.items()
            if len(vs) >= 5 and fn in direto_programs
        }
        if len(main_acct) < 2:
            return findings

        # Find the union of all accounting vars across main programs
        all_vars = set()
        for vars_set in main_acct.values():
            all_vars |= vars_set

        # For each main program, check which vars are missing
        for fn, vars_set in main_acct.items():
            missing = all_vars - vars_set
            if missing:
                other_programs = [
                    ofn for ofn in main_acct if ofn != fn
                ]
                findings.append(self._make_finding(
                    severity="HIGH",
                    category="CONTA_CONTABIL_AUSENTE",
                    program=fn,
                    line=0,
                    description=(
                        f"Variaveis contabeis [{', '.join(sorted(missing))}] "
                        f"sao calculadas em [{', '.join(other_programs)}] "
                        f"mas NAO em '{fn}'."
                    ),
                    impact=(
                        f"Contas contabeis ausentes significam que parte da "
                        f"composicao da RVR nao sera gerada, resultando em "
                        f"arquivo SAP incompleto e divergencia na conciliacao."
                    ),
                    recommendation=(
                        f"REVISAO MANUAL: Verificar se as contas ausentes "
                        f"[{', '.join(sorted(missing))}] sao aplicaveis a "
                        f"este programa. Se sim, adicionar os calculos "
                        f"correspondentes. Se nao, documentar a razao da "
                        f"exclusao."
                    ),
                    recommendation_type="REVISAO_MANUAL",
                ))
        return findings

    def _compare_mapping_coverage(
        self, parsed_programs: List[Dict], contents: Dict[str, str]
    ) -> List[Dict]:
        """Compare value mapping coverage between related programs."""
        findings = []

        # For each program, extract COD_SISTEMA_ORIGEM mappings
        program_mappings: Dict[str, Dict[str, Set[str]]] = {}

        for prog in parsed_programs:
            fn = prog["filename"]
            if fn not in contents:
                continue
            content = contents[fn]

            # Find all sections with COD_SISTEMA_ORIGEM mappings
            sections: Dict[str, Set[str]] = {}
            current_section = "geral"

            for m in RE_IF_MAPPING.finditer(content):
                src_var = m.group(1).upper()
                if "SISTEMA_ORIGEM" not in src_var:
                    continue
                src_val = m.group(2)
                line = content[: m.start()].count("\n") + 1

                # Determine section context
                preceding = content[max(0, m.start() - 500): m.start()]
                if re.search(r'aceito', preceding, re.IGNORECASE):
                    section = "aceito"
                elif re.search(r'resumo', preceding, re.IGNORECASE):
                    section = "resumo"
                else:
                    section = "geral"

                if section not in sections:
                    sections[section] = set()
                sections[section].add(src_val)

            if sections:
                program_mappings[fn] = sections

        if len(program_mappings) < 2:
            return findings

        # Compare mapping coverage within the same program (geral vs aceito)
        for fn, sections in program_mappings.items():
            if "geral" in sections and "aceito" in sections:
                in_geral = sections["geral"]
                in_aceito = sections["aceito"]
                missing_in_aceito = in_geral - in_aceito
                if missing_in_aceito:
                    findings.append(self._make_finding(
                        severity="MEDIUM",
                        category="MAPEAMENTO_INCONSISTENTE_INTERNO",
                        program=fn,
                        line=0,
                        description=(
                            f"Mapeamento de COD_SISTEMA_ORIGEM na secao "
                            f"Direto cobre [{', '.join(sorted(in_geral))}] "
                            f"mas na secao Aceito falta: "
                            f"[{', '.join(sorted(missing_in_aceito))}]."
                        ),
                        impact=(
                            f"Registros aceitos com sistemas de origem "
                            f"[{', '.join(sorted(missing_in_aceito))}] "
                            f"terao SEGMENTO incompleto no arquivo SAP."
                        ),
                        recommendation=(
                            f"CORRECAO CODIGO: Adicionar os mapeamentos "
                            f"faltantes na secao Aceito para manter "
                            f"consistencia com a secao Direto."
                        ),
                        recommendation_type="CORRECAO_CODIGO",
                    ))

        # Compare across programs
        all_sections_geral: Dict[str, Set[str]] = {}
        for fn, sections in program_mappings.items():
            if "geral" in sections:
                all_sections_geral[fn] = sections["geral"]

        if len(all_sections_geral) >= 2:
            all_values = set()
            for vals in all_sections_geral.values():
                all_values |= vals
            for fn, vals in all_sections_geral.items():
                missing = all_values - vals
                if missing:
                    other = [ofn for ofn in all_sections_geral if ofn != fn]
                    findings.append(self._make_finding(
                        severity="LOW",
                        category="MAPEAMENTO_DIVERGENTE_ENTRE_PROGRAMAS",
                        program=fn,
                        line=0,
                        description=(
                            f"Mapeamento de COD_SISTEMA_ORIGEM em '{fn}' "
                            f"nao inclui valores "
                            f"[{', '.join(sorted(missing))}] que existem "
                            f"em [{', '.join(other)}]."
                        ),
                        impact=(
                            f"Pode ser intencional (sistemas diferentes por "
                            f"empresa), mas tambem pode indicar mapeamento "
                            f"incompleto."
                        ),
                        recommendation=(
                            f"REVISAO MANUAL: Confirmar se os valores "
                            f"[{', '.join(sorted(missing))}] sao aplicaveis "
                            f"a este programa. Se sim, adicionar. Se nao, "
                            f"documentar."
                        ),
                        recommendation_type="REVISAO_MANUAL",
                    ))
        return findings

    def _compare_exports(
        self, parsed_programs: List[Dict]
    ) -> List[Dict]:
        """Compare export patterns between related programs."""
        findings = []

        # Categorize exports by type
        program_export_types: Dict[str, Set[str]] = {}

        for prog in parsed_programs:
            fn = prog["filename"]
            exports = prog.get("proc_exports", [])
            if not exports:
                continue

            types = set()
            for exp in exports:
                outfile = exp.get("outfile", "").lower()
                if "sap" in outfile and "direto" in outfile:
                    types.add("sap_direto")
                elif "sap" in outfile and "aceito" in outfile:
                    types.add("sap_aceito")
                elif "analitica" in outfile and "direto" in outfile:
                    types.add("base_analitica_direto")
                elif "analitica" in outfile and "aceito" in outfile:
                    types.add("base_analitica_aceito")
                elif "analitica" in outfile and "vencido" in outfile:
                    types.add("base_analitica_vencido")
                elif "concilia" in outfile:
                    types.add("conciliacao")
                elif "resumo" in outfile or "aging" in outfile:
                    types.add("resumo_aging")
                elif "usd" in outfile or "moeda" in outfile:
                    types.add("moeda_estrangeira")
                else:
                    types.add("outro")

            if types:
                program_export_types[fn] = types

        if len(program_export_types) < 2:
            return findings

        # Only compare programs that have similar complexity / are "main"
        # programs (not auxiliaries like Base_Financeiro, Base_USD)
        main_programs = {
            fn: types for fn, types in program_export_types.items()
            if any(t in types for t in (
                "sap_direto", "sap_aceito", "base_analitica_direto",
                "base_analitica_aceito",
            ))
        }
        if len(main_programs) < 2:
            return findings

        # For main programs, check if export patterns match
        for fn, types in main_programs.items():
            others = {
                ofn: otypes
                for ofn, otypes in main_programs.items()
                if ofn != fn
            }

            for ofn, otypes in others.items():
                critical_missing = set()
                if "sap_aceito" in otypes and "sap_aceito" not in types:
                    critical_missing.add("sap_aceito")
                if "base_analitica_aceito" in otypes and "base_analitica_aceito" not in types:
                    critical_missing.add("base_analitica_aceito")

                if critical_missing:
                    findings.append(self._make_finding(
                        severity="HIGH",
                        category="EXPORT_AUSENTE",
                        program=fn,
                        line=0,
                        description=(
                            f"Programa '{ofn}' exporta arquivos de tipo "
                            f"[{', '.join(sorted(critical_missing))}] mas "
                            f"'{fn}' NAO gera esses exports."
                        ),
                        impact=(
                            f"Arquivos faltantes significam que parte "
                            f"dos lancamentos contabeis nao sera gerada "
                            f"para importacao no sistema financeiro."
                        ),
                        recommendation=(
                            f"EXTRACAO MANUAL: Implementar as secoes de "
                            f"processamento e export faltantes. Usar "
                            f"'{ofn}' como referencia, adaptando contas "
                            f"contabeis e parametros da empresa."
                        ),
                        recommendation_type="EXTRACAO_MANUAL",
                    ))
                    break  # only report once per program

        return findings

    # ------------------------------------------------------------------
    # Helper methods
    # ------------------------------------------------------------------
    def _strip_comments(self, content: str) -> str:
        """Remove SAS comments while preserving line numbers."""
        def _replace_keeping_newlines(match):
            text = match.group(0)
            return re.sub(r'[^\n]', ' ', text)

        # Remove block comments /* ... */
        result = re.sub(
            r'/\*.*?\*/', _replace_keeping_newlines, content, flags=re.DOTALL
        )
        # Remove line comments: lines starting with * ending with ;
        result = re.sub(
            r'^\s*\*[^;]*;', _replace_keeping_newlines, result, flags=re.MULTILINE
        )
        return result

    def _extract_format_vars(self, text: str) -> Set[str]:
        """Extract variable names from FORMAT declarations."""
        variables: Set[str] = set()
        for m in RE_FORMAT_BLOCK.finditer(text):
            block = m.group(1)
            tokens = re.findall(r'\S+', block)
            i = 0
            while i < len(tokens):
                token = tokens[i]
                # Check if this looks like a variable name
                if (re.match(r'^[A-Za-z_]\w*$', token)
                        and not RE_FORMAT_SPEC_TOKEN.match(token)
                        and token.upper() not in SAS_KEYWORDS):
                    # Next token should be a format spec
                    if i + 1 < len(tokens) and re.search(r'\d+\.', tokens[i + 1]):
                        variables.add(token.upper())
                        i += 2
                        continue
                i += 1
        return variables

    def _extract_declared_vars_from_block(
        self, text: str, pattern: re.Pattern
    ) -> Set[str]:
        """Extract variable names from LENGTH/ATTRIB blocks."""
        variables: Set[str] = set()
        for m in pattern.finditer(text):
            block = m.group(1)
            for vm in re.finditer(r'\b([A-Za-z_]\w*)\b', block):
                name = vm.group(1).upper()
                if (name not in SAS_KEYWORDS
                        and not RE_FORMAT_SPEC_TOKEN.match(name)
                        and not re.match(r'^\d', name)):
                    variables.add(name)
        return variables

    def _var_in_formula_rhs(self, var: str, body: str) -> bool:
        """Check if a variable appears on the right side of an assignment."""
        pattern = re.compile(
            r'^\s*\w+\s*=.*\b' + re.escape(var) + r'\b',
            re.MULTILINE | re.IGNORECASE
        )
        return bool(pattern.search(body))

    def _path_stem(self, path: str) -> str:
        """Extract distinctive stem from a file path, ignoring macros."""
        # Remove macro variables
        cleaned = re.sub(r'&\w+\.?', '', path)
        # Get basename
        basename = os.path.basename(cleaned.replace("\\", "/"))
        # Remove extension
        stem = os.path.splitext(basename)[0]
        # Remove leading/trailing underscores and pure digit prefixes
        stem = re.sub(r'^\d+_', '', stem)
        stem = re.sub(r'_\d+$', '', stem)
        return stem.strip("_").lower()

    def _make_finding(
        self,
        severity: str,
        category: str,
        program: str,
        line: int,
        description: str,
        impact: str,
        recommendation: str,
        recommendation_type: str,
    ) -> Dict:
        """Create a structured finding dict."""
        return {
            "severity": severity,
            "category": category,
            "program": program,
            "line": line,
            "description": description,
            "impact": impact,
            "recommendation": recommendation,
            "recommendation_type": recommendation_type,
        }
