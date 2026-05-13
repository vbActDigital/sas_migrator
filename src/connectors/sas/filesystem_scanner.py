import os
import sys
import fnmatch
from datetime import datetime
from typing import Dict, List, Optional, Tuple

from src.utils.logger import get_logger

logger = get_logger("filesystem_scanner")

_ENCODING_CANDIDATES = ["utf-8", "latin-1", "cp1252", "iso-8859-1"]

# Extensions for each category
_EXT_CODE    = {".sas", ".sas7bpgm"}
_EXT_DATA    = {".sas7bdat", ".sas7bcat", ".xpt", ".stx"}
_EXT_LOG     = {".log", ".lst"}
_EXT_CONFIG  = {".cfg", ".spk", ".egp", ".ctp", ".amx", ".smd", ".djf", ".ddf"}

# Linux virtual/system directories that are never worth scanning
_LINUX_SKIP = {
    "/proc", "/sys", "/dev", "/run", "/tmp",
    "/boot", "/lib", "/lib64", "/lib32",
    "/usr/lib", "/usr/lib64", "/usr/lib32",
    "/usr/share", "/usr/local/lib",
    "/bin", "/sbin", "/usr/bin", "/usr/sbin",
    "/snap", "/selinux", "/cgroup",
}


def _detect_encoding(fpath: str) -> Tuple[str, int]:
    """Try encodings in order; return (encoding, line_count)."""
    for enc in _ENCODING_CANDIDATES:
        try:
            with open(fpath, "r", encoding=enc) as f:
                lines = sum(1 for _ in f)
            return enc, lines
        except UnicodeDecodeError:
            continue
    with open(fpath, "r", encoding="utf-8", errors="replace") as f:
        lines = sum(1 for _ in f)
    return "utf-8", lines


class SASFilesystemScanner:
    def __init__(self, config: Dict):
        sas_env = config.get("sas_environment", {})
        # Legacy explicit-path mode
        self.code_paths: List[str] = sas_env.get("code_paths", [])
        self.data_paths: List[str] = sas_env.get("data_paths", [])
        self.log_paths:  List[str] = sas_env.get("log_paths", [])
        # Auto-discovery mode
        self.scan_roots: List[str] = sas_env.get("scan_roots", [])
        self.exclude_patterns: List[str] = sas_env.get("exclude_patterns", [])
        self.max_scan_depth: int = sas_env.get("max_scan_depth", 20)
        self._forced_encoding: Optional[str] = sas_env.get("file_encoding")

    # ------------------------------------------------------------------
    # Auto-discovery mode  (used when scan_roots is set in config)
    # ------------------------------------------------------------------
    def discover(self) -> Dict[str, List[Dict]]:
        """Walk scan_roots recursively and classify every SAS-related file.

        Returns a dict with keys:
          programs  — .sas / .sas7bpgm  (same shape as scan_programs())
          datasets  — .sas7bdat / .sas7bcat / .xpt / .stx (same shape as scan_datasets())
          logs      — .log / .lst  (same shape as scan_logs())
          configs   — .cfg / .spk / .egp / .ctp / .amx / .smd / .djf / .ddf
        """
        roots = self.scan_roots or (["/" ] if sys.platform != "win32" else ["C:\\"])
        result: Dict[str, List[Dict]] = {
            "programs": [], "datasets": [], "logs": [], "configs": []
        }

        for root_path in roots:
            if not os.path.isdir(root_path):
                logger.warning("scan_root does not exist: %s", root_path)
                continue
            logger.info("Auto-discovering SAS files from: %s", root_path)
            self._walk_and_classify(root_path, root_path, depth=0, result=result)

        logger.info(
            "Discovery complete — programs=%d datasets=%d logs=%d configs=%d",
            len(result["programs"]), len(result["datasets"]),
            len(result["logs"]), len(result["configs"]),
        )
        return result

    def _walk_and_classify(
        self, root_path: str, base_path: str, depth: int,
        result: Dict[str, List[Dict]]
    ):
        if depth > self.max_scan_depth:
            return

        try:
            entries = os.scandir(root_path)
        except PermissionError:
            return
        except Exception as e:
            logger.debug("Cannot scan %s: %s", root_path, e)
            return

        subdirs = []
        with entries:
            for entry in entries:
                if entry.is_symlink():
                    continue  # skip symlinks to avoid loops
                if entry.is_dir(follow_symlinks=False):
                    if self._should_skip_dir(entry.path):
                        continue
                    subdirs.append(entry.path)
                elif entry.is_file(follow_symlinks=False):
                    self._classify_file(entry, base_path, result)

        for subdir in subdirs:
            self._walk_and_classify(subdir, base_path, depth + 1, result)

    def _classify_file(
        self, entry, base_path: str, result: Dict[str, List[Dict]]
    ):
        fname = entry.name
        ext = os.path.splitext(fname)[1].lower()

        if ext not in (_EXT_CODE | _EXT_DATA | _EXT_LOG | _EXT_CONFIG):
            return
        if self._should_exclude(entry.path):
            return

        try:
            stat = entry.stat(follow_symlinks=False)
        except Exception:
            return

        fpath = entry.path
        abs_path = os.path.abspath(fpath)
        rel_path = os.path.relpath(fpath, base_path)

        if ext in _EXT_CODE:
            if self._forced_encoding:
                with open(fpath, "r", encoding=self._forced_encoding, errors="replace") as f:
                    line_count = sum(1 for _ in f)
                detected_enc = self._forced_encoding
            else:
                detected_enc, line_count = _detect_encoding(fpath)
            result["programs"].append({
                "filename": fname,
                "absolute_path": abs_path,
                "relative_path": rel_path,
                "size_bytes": stat.st_size,
                "line_count": line_count,
                "last_modified": datetime.fromtimestamp(stat.st_mtime).isoformat(),
                "encoding": detected_enc,
                "file_type": ext.lstrip("."),
            })

        elif ext in _EXT_DATA:
            result["datasets"].append({
                "filename": fname,
                "dataset_name": os.path.splitext(fname)[0],
                "absolute_path": abs_path,
                "inferred_library": os.path.basename(os.path.dirname(fpath)),
                "file_type": ext.lstrip("."),
                "size_bytes": stat.st_size,
            })

        elif ext in _EXT_LOG:
            result["logs"].append({
                "filename": fname,
                "absolute_path": abs_path,
                "size_bytes": stat.st_size,
                "last_modified": datetime.fromtimestamp(stat.st_mtime).isoformat(),
                "file_type": ext.lstrip("."),
            })

        elif ext in _EXT_CONFIG:
            result["configs"].append({
                "filename": fname,
                "absolute_path": abs_path,
                "size_bytes": stat.st_size,
                "last_modified": datetime.fromtimestamp(stat.st_mtime).isoformat(),
                "file_type": ext.lstrip("."),
            })

    def _should_skip_dir(self, path: str) -> bool:
        """Return True for dirs that should never be entered."""
        # Linux virtual/system directories
        normalized = path.replace("\\", "/").rstrip("/")
        if any(normalized == skip.rstrip("/") or normalized.startswith(skip.rstrip("/") + "/")
               for skip in _LINUX_SKIP):
            return True
        return self._should_exclude(path)

    # ------------------------------------------------------------------
    # Legacy explicit-path mode  (backward compat, used by mock/tests)
    # ------------------------------------------------------------------
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
                    ext = os.path.splitext(fname)[1].lower()
                    if ext not in _EXT_CODE:
                        continue
                    fpath = os.path.join(root, fname)
                    if self._should_exclude(fpath):
                        continue
                    try:
                        stat = os.stat(fpath)
                        if self._forced_encoding:
                            with open(fpath, "r", encoding=self._forced_encoding, errors="replace") as f:
                                line_count = sum(1 for _ in f)
                            detected_enc = self._forced_encoding
                        else:
                            detected_enc, line_count = _detect_encoding(fpath)
                        results.append({
                            "filename": fname,
                            "absolute_path": os.path.abspath(fpath),
                            "relative_path": os.path.relpath(fpath, base_path),
                            "size_bytes": stat.st_size,
                            "line_count": line_count,
                            "last_modified": datetime.fromtimestamp(stat.st_mtime).isoformat(),
                            "encoding": detected_enc,
                        })
                    except Exception as e:
                        logger.error("Error scanning %s: %s", fpath, e)
        logger.info("Scanned %d SAS programs", len(results))
        return results

    def scan_datasets(self) -> List[Dict]:
        results = []
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
                    ext = os.path.splitext(fname)[1].lower()
                    if ext not in _EXT_DATA:
                        continue
                    fpath = os.path.join(root, fname)
                    if self._should_exclude(fpath):
                        continue
                    try:
                        stat = os.stat(fpath)
                        results.append({
                            "filename": fname,
                            "dataset_name": os.path.splitext(fname)[0],
                            "absolute_path": os.path.abspath(fpath),
                            "inferred_library": os.path.basename(root),
                            "file_type": ext.lstrip("."),
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
                    if not any(fname.lower().endswith(ext) for ext in _EXT_LOG):
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
