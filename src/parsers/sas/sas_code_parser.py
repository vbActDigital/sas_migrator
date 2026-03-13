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

        complexity = self._compute_complexity(
            data_steps, procs_used, macro_definitions, merge_statements,
            has_hash, has_dynamic_sql, includes,
            macro_variables=macro_variables,
            proc_imports=proc_imports,
            proc_exports=proc_exports,
            join_count=join_count,
            if_then_count=if_then_count,
            has_zip_processing=has_zip_processing,
            line_count=len(lines),
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
            "complexity_score": complexity,
            "complexity_level": self._complexity_level(complexity),
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

    def _compute_complexity(self, data_steps, procs, macros, merges,
                            has_hash, has_dynamic_sql, includes,
                            macro_variables=None, proc_imports=None,
                            proc_exports=None, join_count=0,
                            if_then_count=0, has_zip_processing=False,
                            line_count=0) -> int:
        score = 0
        score += len(data_steps) * 1
        score += len(merges) * 5
        for proc in procs:
            if proc.lower() == "sql":
                score += 3
            elif proc.lower() in STATISTICAL_PROCS:
                score += 5
            elif proc.lower() == "import":
                score += 2
            elif proc.lower() == "export":
                score += 1
        score += len(macros) * 2
        if has_hash:
            score += 5
        if has_dynamic_sql:
            score += 5
        score += len(includes) * 1
        # New scoring
        if macro_variables:
            score += min(len(macro_variables), 5)  # cap at 5
        if proc_imports:
            score += len(proc_imports) * 2
        if proc_exports:
            score += len(proc_exports) * 1
        score += join_count * 2
        if if_then_count > 10:
            score += 5
        elif if_then_count > 5:
            score += 3
        if has_zip_processing:
            score += 5
        # Lines-based bonus for very large programs
        if line_count > 500:
            score += 5
        if line_count > 1000:
            score += 5
        return score

    def _complexity_level(self, score: int) -> str:
        if score <= 10:
            return "LOW"
        elif score <= 25:
            return "MEDIUM"
        elif score <= 50:
            return "HIGH"
        else:
            return "VERY_HIGH"
