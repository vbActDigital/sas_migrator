import os
import fnmatch
from datetime import datetime
from typing import Dict, List, Optional

from src.utils.logger import get_logger

logger = get_logger("filesystem_scanner")


class SASFilesystemScanner:
    def __init__(self, config: Dict):
        sas_env = config.get("sas_environment", {})
        self.code_paths: List[str] = sas_env.get("code_paths", [])
        self.data_paths: List[str] = sas_env.get("data_paths", [])
        self.log_paths: List[str] = sas_env.get("log_paths", [])
        self.exclude_patterns: List[str] = sas_env.get("exclude_patterns", [])
        self.max_scan_depth: int = sas_env.get("max_scan_depth", 10)

    def scan_programs(self) -> List[Dict]:
        results = []
        for base_path in self.code_paths:
            if not os.path.isdir(base_path):
                logger.warning("Code path does not exist: %s", base_path)
                continue
            for root, dirs, files in os.walk(base_path):
                depth = root.replace(base_path, "").count(os.sep)
                if depth >= self.max_scan_depth:
                    dirs.clear()
                    continue
                if self._should_exclude(root):
                    dirs.clear()
                    continue
                for fname in files:
                    if not fname.lower().endswith(".sas"):
                        continue
                    fpath = os.path.join(root, fname)
                    if self._should_exclude(fpath):
                        continue
                    try:
                        stat = os.stat(fpath)
                        with open(fpath, "r", encoding="utf-8", errors="replace") as f:
                            line_count = sum(1 for _ in f)
                        results.append({
                            "filename": fname,
                            "absolute_path": os.path.abspath(fpath),
                            "relative_path": os.path.relpath(fpath, base_path),
                            "size_bytes": stat.st_size,
                            "line_count": line_count,
                            "last_modified": datetime.fromtimestamp(stat.st_mtime).isoformat(),
                            "encoding": "utf-8",
                        })
                    except Exception as e:
                        logger.error("Error scanning %s: %s", fpath, e)
        logger.info("Scanned %d SAS programs", len(results))
        return results

    def scan_datasets(self) -> List[Dict]:
        results = []
        dataset_extensions = (".sas7bdat", ".xpt", ".sas7bcat")
        for base_path in self.data_paths:
            if not os.path.isdir(base_path):
                logger.warning("Data path does not exist: %s", base_path)
                continue
            for root, dirs, files in os.walk(base_path):
                depth = root.replace(base_path, "").count(os.sep)
                if depth >= self.max_scan_depth:
                    dirs.clear()
                    continue
                if self._should_exclude(root):
                    dirs.clear()
                    continue
                for fname in files:
                    if not any(fname.lower().endswith(ext) for ext in dataset_extensions):
                        continue
                    fpath = os.path.join(root, fname)
                    if self._should_exclude(fpath):
                        continue
                    try:
                        stat = os.stat(fpath)
                        dataset_name = os.path.splitext(fname)[0]
                        parent_dir = os.path.basename(root)
                        results.append({
                            "filename": fname,
                            "dataset_name": dataset_name,
                            "absolute_path": os.path.abspath(fpath),
                            "inferred_library": parent_dir,
                            "file_type": os.path.splitext(fname)[1].lstrip("."),
                            "size_bytes": stat.st_size,
                        })
                    except Exception as e:
                        logger.error("Error scanning dataset %s: %s", fpath, e)
        logger.info("Scanned %d SAS datasets", len(results))
        return results

    def scan_logs(self) -> List[Dict]:
        results = []
        for base_path in self.log_paths:
            if not os.path.isdir(base_path):
                continue
            for root, dirs, files in os.walk(base_path):
                depth = root.replace(base_path, "").count(os.sep)
                if depth >= self.max_scan_depth:
                    dirs.clear()
                    continue
                if self._should_exclude(root):
                    dirs.clear()
                    continue
                for fname in files:
                    if not fname.lower().endswith(".log"):
                        continue
                    fpath = os.path.join(root, fname)
                    try:
                        stat = os.stat(fpath)
                        results.append({
                            "filename": fname,
                            "absolute_path": os.path.abspath(fpath),
                            "size_bytes": stat.st_size,
                            "last_modified": datetime.fromtimestamp(stat.st_mtime).isoformat(),
                        })
                    except Exception as e:
                        logger.error("Error scanning log %s: %s", fpath, e)
        logger.info("Scanned %d SAS logs", len(results))
        return results

    def _should_exclude(self, path: str) -> bool:
        normalized = path.replace("\\", "/")
        for pattern in self.exclude_patterns:
            if pattern in normalized:
                return True
            if fnmatch.fnmatch(os.path.basename(path), pattern):
                return True
        return False
