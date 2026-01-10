import os
import logging
from datetime import datetime
from typing import Optional


def _cleanup_old_logs(logs_dir: str, prefix: str, keep_last: int) -> None:
    """Keep only the newest `keep_last` log files matching prefix; delete older ones."""
    if keep_last <= 0:
        return

    files = [
        os.path.join(logs_dir, f)
        for f in os.listdir(logs_dir)
        if f.startswith(prefix) and f.endswith(".log")
    ]

    # Sort by last modified time (newest first)
    files.sort(key=lambda p: os.path.getmtime(p), reverse=True)

    # Delete older files beyond keep_last
    for old_path in files[keep_last:]:
        try:
            os.remove(old_path)
            logging.info(f"Deleted old log file: {old_path}")
        except Exception as e:
            # If deletion fails, don't break the pipeline
            logging.warning(f"Could not delete old log file {old_path}: {e}")


def setup_logging(project_root: str, keep_last: int = 10) -> str:
    """
    Configure logging to both console and a per-execution log file.
    Keeps only the last N pipeline logs in /logs to avoid unlimited growth.
    """
    logs_dir = os.path.join(project_root, "logs")
    os.makedirs(logs_dir, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = os.path.join(logs_dir, f"pipeline_{timestamp}.log")

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    # Avoid duplicated handlers if the script is re-run in the same session
    if logger.handlers:
        logger.handlers.clear()

    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")

    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # File handler
    file_handler = logging.FileHandler(log_file, mode="w", encoding="utf-8")
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    logging.info(f"Logging initialized. Log file: {log_file}")

    # Retention policy: keep only the last N
    _cleanup_old_logs(logs_dir, prefix="pipeline_", keep_last=keep_last)

    return log_file
