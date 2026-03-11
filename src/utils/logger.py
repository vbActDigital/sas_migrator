import logging
import os
from logging.handlers import RotatingFileHandler


_configured = False


def _configure_root(log_dir="logs", level=logging.INFO):
    global _configured
    if _configured:
        return
    _configured = True

    fmt = logging.Formatter(
        "[%(asctime)s] [%(levelname)s] [%(name)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    console = logging.StreamHandler()
    console.setFormatter(fmt)
    console.setLevel(level)

    root = logging.getLogger("sas_migrator")
    root.setLevel(level)
    root.addHandler(console)

    try:
        os.makedirs(log_dir, exist_ok=True)
        file_handler = RotatingFileHandler(
            os.path.join(log_dir, "sas_migrator.log"),
            maxBytes=10 * 1024 * 1024,
            backupCount=5,
        )
        file_handler.setFormatter(fmt)
        file_handler.setLevel(logging.DEBUG)
        root.addHandler(file_handler)
    except OSError:
        pass


def get_logger(name: str) -> logging.Logger:
    _configure_root()
    return logging.getLogger(f"sas_migrator.{name}")
