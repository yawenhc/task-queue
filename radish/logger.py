import logging
import os
from pathlib import Path
from typing import Optional


DEFAULT_LOG_DIR = "logs"
LOG_TIME_FORMAT = "%Y%m%d%H%M%S"


def ensure_log_dir(log_dir: str = DEFAULT_LOG_DIR) -> Path:
    path = Path(log_dir)
    path.mkdir(parents=True, exist_ok=True)
    return path


def build_log_filename(
    run_id: str,
    project_name: str,
    role: str,
    log_dir: str = DEFAULT_LOG_DIR,
) -> Path:
    """
    Example:
        20260323153045_radish_supervisor.log
        20260323153045_radish_worker-1.log
    """
    base_dir = ensure_log_dir(log_dir)
    filename = f"{run_id}_{project_name}_{role}.log"
    return base_dir / filename


def _reset_logger_handlers(logger: logging.Logger) -> None:
    """
    Prevent duplicate handlers when the same logger name is reused.
    """
    logger.propagate = False
    for handler in list(logger.handlers):
        logger.removeHandler(handler)
        handler.close()


def _build_formatter() -> logging.Formatter:
    return logging.Formatter(
        fmt="%(asctime)s %(message)s",
        datefmt=LOG_TIME_FORMAT,
    )


def _create_logger(
    logger_name: str,
    file_path: Path,
    *,
    console: bool,
    level: int = logging.INFO,
) -> logging.Logger:
    logger = logging.getLogger(logger_name)
    logger.setLevel(level)
    _reset_logger_handlers(logger)

    formatter = _build_formatter()

    file_handler = logging.FileHandler(file_path, encoding="utf-8")
    file_handler.setLevel(level)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    if console:
        stream_handler = logging.StreamHandler()
        stream_handler.setLevel(level)
        stream_handler.setFormatter(formatter)
        logger.addHandler(stream_handler)

    return logger


def create_supervisor_logger(
    *,
    run_id: str,
    project_name: str,
    log_dir: str = DEFAULT_LOG_DIR,
    console: bool = True,
) -> logging.Logger:
    file_path = build_log_filename(
        run_id=run_id,
        project_name=project_name,
        role="supervisor",
        log_dir=log_dir,
    )
    logger_name = f"radish.supervisor.{project_name}.{run_id}"
    return _create_logger(
        logger_name=logger_name,
        file_path=file_path,
        console=console,
        level=logging.INFO,
    )


def create_worker_logger(
    *,
    run_id: str,
    project_name: str,
    worker_id: str,
    log_dir: str = DEFAULT_LOG_DIR,
    console: bool = False,
) -> logging.Logger:
    file_path = build_log_filename(
        run_id=run_id,
        project_name=project_name,
        role=worker_id,
        log_dir=log_dir,
    )
    logger_name = f"radish.worker.{project_name}.{run_id}.{worker_id}"
    return _create_logger(
        logger_name=logger_name,
        file_path=file_path,
        console=console,
        level=logging.INFO,
    )