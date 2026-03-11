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

        complexity = self._compute_complexity(
            data_steps, procs_used, macro_definitions, merge_statements,
            has_hash, has_dynamic_sql, includes
        )

        return {
            "filename": filename,
            "filepath": filepath,
            "line_count": len(lines),
            "libnames": libnames,
            "data_steps": data_steps,
            "procs_used": procs_used,
            "datasets_read": datasets_read,
            "datasets_written": datasets_written,
            "macro_definitions": macro_definitions,
            "macro_calls": macro_calls,
            "merge_statements": merge_statements,
            "includes": includes,
            "has_hash_objects": has_hash,
            "has_dynamic_sql": has_dynamic_sql,
            "complexity_score": complexity,
            "complexity_level": self._complexity_level(complexity),
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
            # Split multiple targets and clean up options like (drop=...)
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
        # FROM in PROC SQL
        for m in re.finditer(r'\bFROM\s+([\w.]+)', content, re.IGNORECASE):
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

    def _compute_complexity(self, data_steps, procs, macros, merges,
                            has_hash, has_dynamic_sql, includes) -> int:
        score = 0
        score += len(data_steps) * 1
        score += len(merges) * 5
        for proc in procs:
            if proc.lower() == "sql":
                score += 3
            elif proc.lower() in STATISTICAL_PROCS:
                score += 5
        score += len(macros) * 2
        if has_hash:
            score += 5
        if has_dynamic_sql:
            score += 5
        score += len(includes) * 1
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
