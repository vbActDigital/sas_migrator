import os
import sys
import fnmatch
import queue
import threading
from datetime import datetime
from typing import Dict, List, Optional, Tuple

from src.utils.logger import get_logger

logger = get_logger("filesystem_scanner")

# O(1) extension lookups via frozenset
_EXT_CODE   = frozenset({".sas", ".sas7bpgm"})
_EXT_DATA   = frozenset({".sas7bdat", ".sas7bcat", ".xpt", ".stx"})
_EXT_LOG    = frozenset({".log", ".lst"})
_EXT_CONFIG = frozenset({".cfg", ".spk", ".egp", ".ctp", ".amx", ".smd", ".djf", ".ddf"})
_EXT_ALL    = _EXT_CODE | _EXT_DATA | _EXT_LOG | _EXT_CONFIG

# Linux virtual/system dirs — never worth entering
_LINUX_SKIP_EXACT = frozenset({
    "/proc", "/sys", "/dev", "/run", "/boot",
    "/bin", "/sbin",
})
_LINUX_SKIP_PREFIX = (
    "/lib", "/lib64", "/lib32",
    "/usr/lib", "/usr/lib64", "/usr/share",
    "/usr/bin", "/usr/sbin",
    "/snap", "/selinux", "/cgroup",
)

_IS_LINUX = sys.platform != "win32"
_SENTINEL  = object()   # poison pill to stop worker threads


def _count_lines_fast(fpath: str, forced_enc: Optional[str]) -> Tuple[str, int]:
    """Read binary once — count \\n and detect encoding from BOM/heuristic.

    Avoids re-opening the file multiple times and eliminates Python text-mode
    overhead.  Returns (encoding, line_count).
    """
    try:
        with open(fpath, "rb") as f:
            data = f.read()
        # Count lines: number of newlines + 1 if file doesn't end with one
        count = data.count(b"\n")
        if data and not data.endswith(b"\n"):
            count += 1
        if forced_enc:
            return forced_enc, count
        if data[:3] == b"\xef\xbb\xbf":
            return "utf-8-sig", count
        try:
            data.decode("utf-8")
            return "utf-8", count
        except UnicodeDecodeError:
            return "latin-1", count
    except Exception:
        return forced_enc or "utf-8", -1


class SASFilesystemScanner:
    def __init__(self, config: Dict):
        sas_env = config.get("sas_environment", {})
        # Auto-discovery mode
        self.scan_roots: List[str]       = sas_env.get("scan_roots", [])
        # Legacy explicit-path mode
        self.code_paths: List[str]       = sas_env.get("code_paths", [])
        self.data_paths: List[str]       = sas_env.get("data_paths", [])
        self.log_paths:  List[str]       = sas_env.get("log_paths", [])
        # Common options
        self.exclude_patterns: List[str] = sas_env.get("exclude_patterns", [])
        self.max_scan_depth: int         = sas_env.get("max_scan_depth", 20)
        self._forced_enc: Optional[str]  = sas_env.get("file_encoding")
        # Parallel workers — I/O-bound: more threads = more win
        self._workers: int               = sas_env.get("scan_workers", 16)
        # Skip .hidden dirs (e.g. .git, .cache) — almost never contain SAS files
        self._skip_hidden: bool          = sas_env.get("skip_hidden_dirs", True)

    # ----------------------------------------------------------------
    # Auto-discovery  (scan_roots config key)
    # ----------------------------------------------------------------
    def discover(self) -> Dict[str, List[Dict]]:
        """Parallel BFS from scan_roots, classifying every SAS file by extension.

        Returns:
          programs  — .sas / .sas7bpgm
          datasets  — .sas7bdat / .sas7bcat / .xpt / .stx
          logs      — .log / .lst
          configs   — .cfg / .spk / .egp / .ctp / .amx / .smd / .djf / .ddf
        """
        default_root = "/" if _IS_LINUX else "C:\\"
        roots = [r for r in (self.scan_roots or [default_root]) if os.path.isdir(r)]
        if not roots:
            logger.warning("No valid scan_roots found")
            return {"programs": [], "datasets": [], "logs": [], "configs": []}

        dir_queue: "queue.Queue[object]" = queue.Queue()
        shared: Dict[str, List] = {"programs": [], "datasets": [], "logs": [], "configs": []}
        lock = threading.Lock()

        for root in roots:
            dir_queue.put((root, 0))

        def worker():
            # Each thread accumulates locally; merges once at the end (fewer lock ops)
            local: Dict[str, List] = {"programs": [], "datasets": [], "logs": [], "configs": []}
            while True:
                item = dir_queue.get()
                if item is _SENTINEL:
                    if any(local[k] for k in local):
                        with lock:
                            for cat, items in local.items():
                                shared[cat].extend(items)
                    dir_queue.task_done()
                    return
                dir_path, depth = item
                try:
                    self._scan_dir(dir_path, depth, dir_queue, local)
                except Exception as e:
                    logger.debug("Worker error %s: %s", dir_path, e)
                finally:
                    dir_queue.task_done()

        threads = [threading.Thread(target=worker, daemon=True)
                   for _ in range(self._workers)]
        for t in threads:
            t.start()

        dir_queue.join()                    # wait for all dirs to be processed

        for _ in threads:                   # send poison pills
            dir_queue.put(_SENTINEL)
        for t in threads:
            t.join()

        logger.info(
            "Discovery — programs=%d  datasets=%d  logs=%d  configs=%d",
            len(shared["programs"]), len(shared["datasets"]),
            len(shared["logs"]),     len(shared["configs"]),
        )
        return shared

    def _scan_dir(
        self,
        dir_path: str,
        depth: int,
        dir_queue: queue.Queue,
        local: Dict[str, List],
    ):
        try:
            with os.scandir(dir_path) as it:
                for entry in it:
                    try:
                        if entry.is_symlink():
                            continue
                        if entry.is_dir(follow_symlinks=False):
                            if depth < self.max_scan_depth \
                                    and not self._should_skip_dir(entry.path):
                                dir_queue.put((entry.path, depth + 1))
                        elif entry.is_file(follow_symlinks=False):
                            ext = os.path.splitext(entry.name)[1].lower()
                            if ext in _EXT_ALL \
                                    and not self._should_exclude(entry.path):
                                rec = self._make_record(entry, ext)
                                if rec is not None:
                                    local[_category(ext)].append(rec)
                    except Exception:
                        continue
        except PermissionError:
            pass
        except Exception as e:
            logger.debug("Cannot open %s: %s", dir_path, e)

    def _make_record(self, entry, ext: str) -> Optional[Dict]:
        try:
            st = entry.stat(follow_symlinks=False)
        except Exception:
            return None
        fpath = entry.path
        rec: Dict = {
            "filename":      entry.name,
            "absolute_path": fpath,
            "size_bytes":    st.st_size,
            "last_modified": datetime.fromtimestamp(st.st_mtime).isoformat(),
            "file_type":     ext.lstrip("."),
        }
        if ext in _EXT_CODE:
            enc, lines = _count_lines_fast(fpath, self._forced_enc)
            rec["line_count"]    = lines
            rec["encoding"]      = enc
            rec["relative_path"] = fpath   # no fixed base in discover mode
        elif ext in _EXT_DATA:
            rec["dataset_name"]    = os.path.splitext(entry.name)[0]
            rec["inferred_library"] = os.path.basename(os.path.dirname(fpath))
        return rec

    def _should_skip_dir(self, path: str) -> bool:
        name = os.path.basename(path)
        if self._skip_hidden and name.startswith("."):
            return True
        if _IS_LINUX:
            norm = path.rstrip("/")
            if norm in _LINUX_SKIP_EXACT:
                return True
            for prefix in _LINUX_SKIP_PREFIX:
                if norm == prefix or norm.startswith(prefix + "/"):
                    return True
        return self._should_exclude(path)

    # ----------------------------------------------------------------
    # Legacy explicit-path mode  (backward compat — mock / tests)
    # ----------------------------------------------------------------
    def scan_programs(self) -> List[Dict]:
        results = []
        for base_path in self.code_paths:
            if not os.path.isdir(base_path):
                logger.warning("Code path does not exist: %s", base_path)
                continue
            for root, dirs, files in os.walk(base_path):
                depth = root.replace(base_path, "").count(os.sep)
                if depth >= self.max_scan_depth:
                    dirs.clear(); continue
                if self._should_exclude(root):
                    dirs.clear(); continue
                for fname in files:
                    ext = os.path.splitext(fname)[1].lower()
                    if ext not in _EXT_CODE:
                        continue
                    fpath = os.path.join(root, fname)
                    if self._should_exclude(fpath):
                        continue
                    try:
                        st = os.stat(fpath)
                        enc, lines = _count_lines_fast(fpath, self._forced_enc)
                        results.append({
                            "filename":      fname,
                            "absolute_path": os.path.abspath(fpath),
                            "relative_path": os.path.relpath(fpath, base_path),
                            "size_bytes":    st.st_size,
                            "line_count":    lines,
                            "last_modified": datetime.fromtimestamp(st.st_mtime).isoformat(),
                            "encoding":      enc,
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
                    dirs.clear(); continue
                if self._should_exclude(root):
                    dirs.clear(); continue
                for fname in files:
                    ext = os.path.splitext(fname)[1].lower()
                    if ext not in _EXT_DATA:
                        continue
                    fpath = os.path.join(root, fname)
                    if self._should_exclude(fpath):
                        continue
                    try:
                        st = os.stat(fpath)
                        results.append({
                            "filename":       fname,
                            "dataset_name":   os.path.splitext(fname)[0],
                            "absolute_path":  os.path.abspath(fpath),
                            "inferred_library": os.path.basename(root),
                            "file_type":      ext.lstrip("."),
                            "size_bytes":     st.st_size,
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
                    dirs.clear(); continue
                if self._should_exclude(root):
                    dirs.clear(); continue
                for fname in files:
                    if os.path.splitext(fname)[1].lower() not in _EXT_LOG:
                        continue
                    fpath = os.path.join(root, fname)
                    try:
                        st = os.stat(fpath)
                        results.append({
                            "filename":      fname,
                            "absolute_path": os.path.abspath(fpath),
                            "size_bytes":    st.st_size,
                            "last_modified": datetime.fromtimestamp(st.st_mtime).isoformat(),
                        })
                    except Exception as e:
                        logger.error("Error scanning log %s: %s", fpath, e)
        logger.info("Scanned %d SAS logs", len(results))
        return results

    def _should_exclude(self, path: str) -> bool:
        normalized = path.replace("\\", "/")
        base = os.path.basename(path)
        for pattern in self.exclude_patterns:
            if pattern in normalized or fnmatch.fnmatch(base, pattern):
                return True
        return False


def _category(ext: str) -> str:
    if ext in _EXT_CODE:   return "programs"
    if ext in _EXT_DATA:   return "datasets"
    if ext in _EXT_LOG:    return "logs"
    return "configs"
