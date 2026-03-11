import hashlib
import re


def format_bytes(n: int) -> str:
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if abs(n) < 1024:
            return f"{n:.1f} {unit}" if unit != "B" else f"{n} B"
        n /= 1024
    return f"{n:.1f} PB"


def safe_filename(name: str) -> str:
    return re.sub(r'[^\w\-.]', '_', name).strip('_')


def hash_content(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:16]


def normalize_sas_name(name: str) -> str:
    return name.strip().lower()
