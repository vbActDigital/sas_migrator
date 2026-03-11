import os
import json
from typing import Dict, Optional

from src.utils.logger import get_logger

logger = get_logger("sas_data_parser")


class SASDataParser:
    def parse_file(self, filepath: str) -> Dict:
        meta_path = filepath + ".meta.json" if not filepath.endswith(".meta.json") else filepath
        base_path = filepath.replace(".meta.json", "") if filepath.endswith(".meta.json") else filepath

        # Try .meta.json fallback first (works for mock environments)
        if os.path.exists(meta_path):
            return self._parse_meta_json(meta_path, base_path)

        # Try pyreadstat
        try:
            import pyreadstat
            df, meta = pyreadstat.read_sas7bdat(filepath)
            columns = []
            for i, col in enumerate(meta.column_names):
                columns.append({
                    "name": col,
                    "type": "num" if col in (meta.variable_value_labels or {}) or str(df[col].dtype).startswith(("int", "float")) else "char",
                    "length": meta.column_names_to_labels.get(col, ""),
                    "format": meta.column_formats.get(col, "") if hasattr(meta, 'column_formats') else "",
                    "label": meta.column_labels[i] if meta.column_labels and i < len(meta.column_labels) else "",
                })
            return {
                "filename": os.path.basename(filepath),
                "filepath": filepath,
                "row_count": len(df),
                "column_count": len(columns),
                "columns": columns,
                "size_bytes": os.path.getsize(filepath),
            }
        except Exception as e:
            logger.debug("pyreadstat failed for %s: %s", filepath, e)

        # Try sas7bdat lib
        try:
            from sas7bdat import SAS7BDAT
            with SAS7BDAT(filepath) as f:
                rows = list(f)
                header = rows[0] if rows else []
                return {
                    "filename": os.path.basename(filepath),
                    "filepath": filepath,
                    "row_count": len(rows) - 1,
                    "column_count": len(header),
                    "columns": [{"name": str(c), "type": "unknown", "length": 0, "format": "", "label": ""} for c in header],
                    "size_bytes": os.path.getsize(filepath),
                }
        except Exception as e:
            logger.debug("sas7bdat lib failed for %s: %s", filepath, e)

        # Filesystem fallback
        return {
            "filename": os.path.basename(filepath),
            "filepath": filepath,
            "row_count": -1,
            "column_count": -1,
            "columns": [],
            "size_bytes": os.path.getsize(filepath) if os.path.exists(filepath) else 0,
        }

    def _parse_meta_json(self, meta_path: str, base_path: str) -> Dict:
        with open(meta_path, "r", encoding="utf-8") as f:
            meta = json.load(f)
        meta["filepath"] = base_path
        meta["filename"] = os.path.basename(base_path)
        return meta
