import re
from typing import Dict, List

from src.utils.logger import get_logger

logger = get_logger("sas_code_parser")

# Regex patterns
RE_LIBNAME = re.compile(r"LIBNAME\s+(\w+)\s+['\"]?([^;'\"]+)", re.IGNORECASE)
RE_DATA_STEP = re.compile(r"DATA\s+([\w.]+(?:\s*\(.*?\))?(?:\s+[\w.]+(?:\s*\(.*?\))?)*);\s", re.IGNORECASE | re.DOTALL)
RE_SET = re.compile(r"\bSET\s+([\w.]+)", re.IGNORECASE)
RE_MERGE = re.compile(r"\bMERGE\s+([\w.]+(?:\s*\([^)]*\))?\s+[\w.]+(?:\s*\([^)]*\))?)", re.IGNORECASE)
RE_PROC = re.compile(r"\bPROC\s+(\w+)", re.IGNORECASE)
RE_MACRO_DEF = re.compile(r"%MACRO\s+(\w+)", re.IGNORECASE)
RE_MACRO_CALL = re.compile(r"%(\w+)\s*\(", re.IGNORECASE)
RE_SQL_CREATE = re.compile(r"CREATE\s+TABLE\s+([\w.]+)\s+AS", re.IGNORECASE)
RE_HASH = re.compile(r"DECLARE\s+HASH|_NEW_\s*HASH", re.IGNORECASE)
RE_DYNAMIC_SQL = re.compile(r"CALL\s+EXECUTE|%SYSFUNC", re.IGNORECASE)
RE_INCLUDE = re.compile(r"%INCLUDE\s+['\"]?([^;'\"]+)", re.IGNORECASE)

# New patterns for real-world SAS programs
RE_LET = re.compile(r"%LET\s+(\w+)\s*=\s*([^;]*);", re.IGNORECASE)
RE_PROC_IMPORT = re.compile(
    r"PROC\s+IMPORT\s+.*?DATAFILE\s*=\s*['\"]?([^;'\"]+)['\"]?.*?OUT\s*=\s*([\w.&]+).*?DBMS\s*=\s*(\w+)",
    re.IGNORECASE | re.DOTALL
)
RE_PROC_EXPORT = re.compile(
    r"PROC\s+EXPORT\s+.*?DATA\s*=\s*([\w.&]+).*?OUTFILE\s*=\s*['\"]?([^;'\"]+)['\"]?.*?DBMS\s*=\s*(\w+)",
    re.IGNORECASE | re.DOTALL
)
RE_FILENAME = re.compile(r"FILENAME\s+(\w+)\s+(?:ZIP\s+)?['\"]?([^;'\"]+)", re.IGNORECASE)
RE_INFILE = re.compile(r"INFILE\s+['\"]?([^;'\"]+)", re.IGNORECASE)
RE_FORMAT_STMT = re.compile(r"\bFORMAT\b\s+(.*?);", re.IGNORECASE | re.DOTALL)
RE_INFORMAT_STMT = re.compile(r"\bINFORMAT\b\s+(.*?);", re.IGNORECASE | re.DOTALL)
RE_LENGTH_STMT = re.compile(r"\bLENGTH\b\s+(.*?);", re.IGNORECASE | re.DOTALL)
RE_ATTRIB_STMT = re.compile(r"\bATTRIB\b\s+(.*?);", re.IGNORECASE | re.DOTALL)
RE_NAMED_LITERAL = re.compile(r'"[^"]+?"n', re.IGNORECASE)
RE_UNC_PATH = re.compile(r'\\\\[a-zA-Z0-9_\-]+\\[^\s;\'\"]+')
RE_FORMAT_SPEC = re.compile(r'\b(COMMAX|COMMA|DDMMYY|MMDDYY|YYMMDD|DATE|DATETIME|BEST|DOLLAR|PERCENT)\d*\.?\d*', re.IGNORECASE)
RE_LEFT_JOIN = re.compile(r'\bLEFT\s+JOIN\b', re.IGNORECASE)
RE_INNER_JOIN = re.compile(r'\bINNER\s+JOIN\b', re.IGNORECASE)
RE_IF_THEN = re.compile(r'\bIF\b.*?\bTHEN\b', re.IGNORECASE)
RE_KEEP = re.compile(r'\bKEEP\s+([\w\s]+);', re.IGNORECASE)
RE_ACCOUNTING_VAR = re.compile(r'\bC\d{7,}\b')
RE_VAR_ASSIGNMENT = re.compile(r'^\s*([A-Za-z_]\w*)\s*=[^=]', re.MULTILINE)

STATISTICAL_PROCS = {"logistic", "reg", "glm", "mixed", "univariate", "genmod", "phreg", "lifetest", "nlin"}
SAS_BUILTIN_MACROS = {"if", "then", "else", "do", "end", "let", "put", "str", "scan", "substr",
                       "eval", "sysevalf", "sysfunc", "nrstr", "bquote", "superq", "global", "local",
                       "mend", "macro", "include", "run", "quit", "length", "index", "upcase", "lowcase"}


class SASCodeParser:
    def parse_file(self, filepath: str) -> Dict:
        with open(filepath, "r", encoding="utf-8", errors="replace") as f:
            content = f.read()
        lines = content.splitlines()

        import os
        filename = os.path.basename(filepath)

        libnames = self._extract_libnames(content)
        data_steps = self._extract_data_steps(content)
        procs_used = self._extract_procs(content)
        datasets_read = self._extract_datasets_read(content)
        datasets_written = self._extract_datasets_written(content, data_steps)
        macro_definitions = self._extract_macro_definitions(content)
        macro_calls = self._extract_macro_calls(content)
        merge_statements = self._extract_merge_statements(content)
        includes = self._extract_includes(content)
        has_hash = bool(RE_HASH.search(content))
        has_dynamic_sql = bool(RE_DYNAMIC_SQL.search(content))

        # New extractions
        macro_variables = self._extract_macro_variables(content)
        proc_imports = self._extract_proc_imports(content)
        proc_exports = self._extract_proc_exports(content)
        filenames = self._extract_filenames(content)
        infile_stmts = self._extract_infiles(content)
        format_specs = self._extract_format_specs(content)
        named_literals = self._extract_named_literals(content)
        unc_paths = self._extract_unc_paths(content)
        join_count = len(RE_LEFT_JOIN.findall(content)) + len(RE_INNER_JOIN.findall(content))
        if_then_count = len(RE_IF_THEN.findall(content))
        has_zip_processing = bool(re.search(r'FILENAME\s+\w+\s+ZIP', content, re.IGNORECASE))
        has_infile_processing = len(infile_stmts) > 0
        keep_variables = self._extract_keep_variables(content)
        variable_assignments = self._extract_variable_assignments(content)
        accounting_variables = self._extract_accounting_variables(content)

        complexity_detail = self._compute_complexity_dimensions(
            data_steps=data_steps, procs_used=procs_used,
            macro_definitions=macro_definitions, merge_statements=merge_statements,
            has_hash=has_hash, has_dynamic_sql=has_dynamic_sql,
            includes=includes, macro_variables=macro_variables,
            proc_imports=proc_imports, proc_exports=proc_exports,
            join_count=join_count, if_then_count=if_then_count,
            has_zip_processing=has_zip_processing, line_count=len(lines),
            datasets_read=datasets_read, libnames=libnames,
            unc_paths=unc_paths, filenames=filenames,
            infile_stmts=infile_stmts, format_specs=format_specs,
            named_literals=named_literals,
        )

        return {
            "filename": filename,
            "filepath": filepath,
            "line_count": len(lines),
            "size_bytes": len(content.encode("utf-8")),
            "libnames": libnames,
            "data_steps": data_steps,
            "procs_used": procs_used,
            "datasets_read": datasets_read,
            "datasets_written": datasets_written,
            "macro_definitions": macro_definitions,
            "macro_calls": macro_calls,
            "macro_variables": macro_variables,
            "merge_statements": merge_statements,
            "includes": includes,
            "has_hash_objects": has_hash,
            "has_dynamic_sql": has_dynamic_sql,
            "complexity_score": complexity_detail["CT"],
            "complexity_level": self._complexity_level(complexity_detail["CT"]),
            "complexity_dimensions": complexity_detail,
            # New fields
            "proc_imports": proc_imports,
            "proc_exports": proc_exports,
            "filenames": filenames,
            "infile_statements": infile_stmts,
            "format_specs": format_specs,
            "named_literals": named_literals,
            "unc_paths": unc_paths,
            "join_count": join_count,
            "if_then_chains": if_then_count,
            "has_zip_processing": has_zip_processing,
            "has_infile_processing": has_infile_processing,
            "keep_variables": keep_variables,
            "variable_assignments": variable_assignments,
            "accounting_variables": accounting_variables,
        }

    def _extract_libnames(self, content: str) -> List[Dict]:
        results = []
        for m in RE_LIBNAME.finditer(content):
            results.append({"name": m.group(1).upper(), "path_or_engine": m.group(2).strip()})
        return results

    def _extract_data_steps(self, content: str) -> List[str]:
        results = []
        for m in RE_DATA_STEP.finditer(content):
            targets = m.group(1).strip()
            for part in re.split(r'\s+', targets):
                cleaned = re.sub(r'\(.*?\)', '', part).strip()
                if cleaned and not cleaned.startswith('('):
                    results.append(cleaned)
        return results

    def _extract_procs(self, content: str) -> List[str]:
        return list({m.group(1).upper() for m in RE_PROC.finditer(content)})

    def _extract_datasets_read(self, content: str) -> List[str]:
        read_set = set()
        for m in RE_SET.finditer(content):
            read_set.add(m.group(1))
        for m in RE_MERGE.finditer(content):
            for ds in re.findall(r'([\w.]+)', m.group(1)):
                read_set.add(ds)
        for m in re.finditer(r'\bFROM\s+([\w.]+)', content, re.IGNORECASE):
            read_set.add(m.group(1))
        # JOIN targets
        for m in re.finditer(r'\bJOIN\s+([\w.]+)', content, re.IGNORECASE):
            read_set.add(m.group(1))
        return list(read_set)

    def _extract_datasets_written(self, content: str, data_steps: List[str]) -> List[str]:
        written = set(data_steps)
        for m in RE_SQL_CREATE.finditer(content):
            written.add(m.group(1))
        return list(written)

    def _extract_macro_definitions(self, content: str) -> List[str]:
        return [m.group(1) for m in RE_MACRO_DEF.finditer(content)]

    def _extract_macro_calls(self, content: str) -> List[str]:
        calls = set()
        for m in RE_MACRO_CALL.finditer(content):
            name = m.group(1).lower()
            if name not in SAS_BUILTIN_MACROS:
                calls.add(m.group(1))
        return list(calls)

    def _extract_merge_statements(self, content: str) -> List[str]:
        results = []
        for m in RE_MERGE.finditer(content):
            datasets = re.findall(r'([\w.]+)', m.group(1))
            results.extend(datasets)
        return results

    def _extract_includes(self, content: str) -> List[str]:
        return [m.group(1).strip() for m in RE_INCLUDE.finditer(content)]

    def _extract_macro_variables(self, content: str) -> List[Dict]:
        results = []
        for m in RE_LET.finditer(content):
            results.append({"name": m.group(1), "value": m.group(2).strip()})
        return results

    def _extract_proc_imports(self, content: str) -> List[Dict]:
        results = []
        for m in RE_PROC_IMPORT.finditer(content):
            results.append({
                "datafile": m.group(1).strip(),
                "out_dataset": m.group(2).strip(),
                "dbms": m.group(3).strip().upper(),
            })
        return results

    def _extract_proc_exports(self, content: str) -> List[Dict]:
        results = []
        for m in RE_PROC_EXPORT.finditer(content):
            results.append({
                "data": m.group(1).strip(),
                "outfile": m.group(2).strip(),
                "dbms": m.group(3).strip().upper(),
            })
        return results

    def _extract_filenames(self, content: str) -> List[Dict]:
        results = []
        for m in RE_FILENAME.finditer(content):
            results.append({"fileref": m.group(1), "path": m.group(2).strip()})
        return results

    def _extract_infiles(self, content: str) -> List[str]:
        return [m.group(1).strip() for m in RE_INFILE.finditer(content)]

    def _extract_format_specs(self, content: str) -> List[str]:
        return list({m.group(0).upper() for m in RE_FORMAT_SPEC.finditer(content)})

    def _extract_named_literals(self, content: str) -> List[str]:
        return list({m.group(0) for m in RE_NAMED_LITERAL.finditer(content)})

    def _extract_unc_paths(self, content: str) -> List[str]:
        return list({m.group(0) for m in RE_UNC_PATH.finditer(content)})

    def _extract_keep_variables(self, content: str) -> List[str]:
        """Extract all variable names from KEEP statements."""
        keep_vars = set()
        for m in RE_KEEP.finditer(content):
            for v in re.findall(r'\b([A-Za-z_]\w*)\b', m.group(1)):
                if v.upper() not in SAS_BUILTIN_MACROS:
                    keep_vars.add(v.upper())
        return sorted(keep_vars)

    def _extract_variable_assignments(self, content: str) -> List[str]:
        """Extract variable names from assignment statements (left side of =)."""
        assigned = set()
        for m in RE_VAR_ASSIGNMENT.finditer(content):
            name = m.group(1).upper()
            if name not in SAS_BUILTIN_MACROS:
                assigned.add(name)
        return sorted(assigned)

    def _extract_accounting_variables(self, content: str) -> Dict:
        """Extract accounting variables (C-prefix) - both assigned and referenced."""
        assigned = set()
        referenced = set()
        for m in re.finditer(r'^\s*(C\d{7,})\s*=', content, re.MULTILINE):
            assigned.add(m.group(1).upper())
        for m in RE_ACCOUNTING_VAR.finditer(content):
            referenced.add(m.group(0).upper())
        return {
            "assigned": sorted(assigned),
            "referenced": sorted(referenced),
            "referenced_but_not_assigned": sorted(referenced - assigned),
        }

    def _compute_complexity_dimensions(self, **kw) -> Dict:
        """
        Compute complexity using 4 dimensions from the standard matrix:
          PLP = Logica de Programacao
          PDI = Dependencias e Integracao
          PVD = Volume e Variedade de Dados
          PRS = Recursos Especificos SAS
          CT  = PLP + PDI + PVD + PRS

        Each dimension scores: Baixa (1-2), Media (3-5), Alta (>5, capped at 8).
        CT range: 4 (all Baixa) to 32 (all Alta).
        """
        plp = self._score_plp(kw)
        pdi = self._score_pdi(kw)
        pvd = self._score_pvd(kw)
        prs = self._score_prs(kw)
        ct = plp + pdi + pvd + prs

        return {
            "PLP": plp, "PLP_nivel": self._dim_level(plp),
            "PDI": pdi, "PDI_nivel": self._dim_level(pdi),
            "PVD": pvd, "PVD_nivel": self._dim_level(pvd),
            "PRS": prs, "PRS_nivel": self._dim_level(prs),
            "CT": ct,
            "esforco_hh": self._estimate_effort(ct),
        }

    def _score_plp(self, kw) -> int:
        """PLP - Logica de Programacao (Codigo)."""
        score = 0
        macros = kw.get("macro_definitions", [])
        has_hash = kw.get("has_hash", False)
        has_dynamic_sql = kw.get("has_dynamic_sql", False)
        if_then_count = kw.get("if_then_count", 0)
        join_count = kw.get("join_count", 0)
        data_steps = kw.get("data_steps", [])
        line_count = kw.get("line_count", 0)
        macro_variables = kw.get("macro_variables") or []

        # Macros: 0=1, 1-5=3, >5=6
        n_macros = len(macros) + min(len(macro_variables), 5)
        if n_macros == 0:
            score += 1
        elif n_macros <= 5:
            score += 3
        else:
            score += 5

        # Hash objects / arrays complexos -> Alta
        if has_hash:
            score += 3

        # Dynamic SQL -> complexidade extra
        if has_dynamic_sql:
            score += 2

        # IF-THEN aninhamento
        if if_then_count > 20:
            score += 3
        elif if_then_count > 10:
            score += 2
        elif if_then_count > 5:
            score += 1

        # Joins
        if join_count > 6:
            score += 2
        elif join_count > 2:
            score += 1

        # Data steps encadeados (processos encadeados)
        if len(data_steps) > 10:
            score += 2
        elif len(data_steps) > 5:
            score += 1

        # Volume de codigo
        if line_count > 1000:
            score += 2
        elif line_count > 500:
            score += 1

        return min(score, 8)  # cap

    def _score_pdi(self, kw) -> int:
        """PDI - Dependencias e Integracao."""
        score = 0
        datasets_read = kw.get("datasets_read", [])
        includes = kw.get("includes", [])
        libnames = kw.get("libnames", [])
        unc_paths = kw.get("unc_paths", [])
        proc_imports = kw.get("proc_imports") or []
        proc_exports = kw.get("proc_exports") or []
        filenames = kw.get("filenames") or []
        has_dynamic_sql = kw.get("has_dynamic_sql", False)

        # Datasets lidos (fontes de dados)
        n_reads = len(datasets_read)
        if n_reads > 8:
            score += 4
        elif n_reads > 3:
            score += 2
        else:
            score += 1

        # Includes (dependencia de outros scripts)
        if len(includes) > 3:
            score += 3
        elif len(includes) > 0:
            score += 1

        # Libnames (integracao com sistemas)
        if len(libnames) > 3:
            score += 2
        elif len(libnames) > 0:
            score += 1

        # UNC paths (acesso a rede/sistemas de arquivos externos)
        if len(unc_paths) > 0:
            score += 1

        # PROC IMPORT/EXPORT (integracao de dados)
        n_io = len(proc_imports) + len(proc_exports)
        if n_io > 4:
            score += 2
        elif n_io > 0:
            score += 1

        # Filenames / acesso externo
        if len(filenames) > 2:
            score += 1

        # Dynamic SQL = dependencia complexa
        if has_dynamic_sql:
            score += 1

        return min(score, 8)

    def _score_pvd(self, kw) -> int:
        """PVD - Volume e Variedade de Dados."""
        score = 0
        merge_statements = kw.get("merge_statements", [])
        has_zip = kw.get("has_zip_processing", False)
        infile_stmts = kw.get("infile_stmts") or []
        format_specs = kw.get("format_specs") or []
        proc_imports = kw.get("proc_imports") or []

        # Merges (complexidade de juncao de dados)
        n_merges = len(merge_statements)
        if n_merges > 3:
            score += 3
        elif n_merges > 0:
            score += 2
        else:
            score += 1

        # ZIP processing (volume grande)
        if has_zip:
            score += 2

        # INFILE (processamento de arquivos brutos)
        if len(infile_stmts) > 0:
            score += 1

        # Variedade de formatos
        n_formats = len(format_specs)
        if n_formats > 10:
            score += 2
        elif n_formats > 5:
            score += 1

        # Diversidade de fontes (DBMS diferentes nos imports)
        dbms_types = set()
        for imp in proc_imports:
            dbms_types.add(imp.get("dbms", ""))
        if len(dbms_types) > 2:
            score += 2
        elif len(dbms_types) > 1:
            score += 1

        return min(score, 8)

    def _score_prs(self, kw) -> int:
        """PRS - Recursos Especificos SAS."""
        score = 0
        procs_used = kw.get("procs_used", [])
        has_hash = kw.get("has_hash", False)
        named_literals = kw.get("named_literals") or []

        procs_lower = {p.lower() for p in procs_used}

        # PROCs estatisticas especificas (Media)
        stat_medium = {"reg", "glm", "logistic", "univariate", "genmod", "phreg",
                        "lifetest", "nlin", "means", "freq"}
        stat_found = procs_lower & stat_medium
        # PROCs avancadas (Alta)
        stat_alta = {"iml", "graph", "ets", "report", "tabulate", "mixed"}
        alta_found = procs_lower & stat_alta

        if alta_found:
            score += 5
        elif stat_found:
            score += 3
        else:
            score += 1

        # Hash objects (recurso avancado)
        if has_hash:
            score += 2

        # Named literals (recurso especifico SAS)
        if len(named_literals) > 0:
            score += 1

        # PROC FORMAT (customizacao)
        if "format" in procs_lower:
            score += 1

        # Quantidade total de PROCs distintas
        n_procs = len(procs_lower)
        if n_procs > 7:
            score += 2
        elif n_procs > 3:
            score += 1

        return min(score, 8)

    @staticmethod
    def _dim_level(score: int) -> str:
        """Classify a single dimension score."""
        if score <= 2:
            return "Baixa"
        elif score <= 5:
            return "Media"
        else:
            return "Alta"

    @staticmethod
    def _estimate_effort(ct: int) -> str:
        """Estimate effort in person-hours based on CT."""
        if ct <= 8:
            return "8-24 HH"
        elif ct <= 14:
            return "32-80 HH"
        elif ct <= 20:
            return "80-160 HH"
        else:
            return ">160 HH"

    def _complexity_level(self, ct: int) -> str:
        if ct <= 8:
            return "LOW"
        elif ct <= 14:
            return "MEDIUM"
        elif ct <= 20:
            return "HIGH"
        else:
            return "VERY_HIGH"
