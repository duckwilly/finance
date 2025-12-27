
"""Application-wide logging utilities with rich console output and progress helpers."""
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, date
from logging.handlers import QueueHandler, QueueListener
from pathlib import Path
from queue import SimpleQueue
from threading import RLock
from typing import Optional

from rich.console import Console
from rich.logging import RichHandler
from rich.traceback import install as install_rich_traceback

from .context import ContextFilter, log_context
from .progress import progress_manager
from .timing import timeit

__all__ = [
    "init_logging",
    "get_logger",
    "set_level",
    "shutdown_logging",
    "log_context",
    "progress_manager",
    "timeit",
]


@dataclass
class LoggingConfig:
    """Runtime configuration for the logging subsystem."""

    app_name: str = "finance"
    level: str | int = "INFO"
    log_dir: Optional[Path] = Path("logs")
    console: bool = True
    rich_tracebacks: bool = True
    queue: bool = True


_config_lock = RLock()
_config: LoggingConfig | None = None
_listener: QueueListener | None = None
_queue: SimpleQueue | None = None
_context_filter = ContextFilter()


def _parse_level(level: str | int) -> int:
    if isinstance(level, int):
        return level
    return getattr(logging, str(level).upper(), logging.INFO)


class DailyFileHandler(logging.FileHandler):
    """Logging handler that writes to a single log file per day."""

    def __init__(
        self,
        directory: Path,
        *,
        encoding: str = "utf-8",
        date_format: str = "%Y_%m_%d",
    ) -> None:
        self.directory = directory
        self.date_format = date_format
        self.directory.mkdir(parents=True, exist_ok=True)
        self._current_date: date = datetime.now().date()
        super().__init__(
            self._path_for_date(self._current_date),
            mode="a",
            encoding=encoding,
        )

    def _path_for_date(self, target_date: date) -> Path:
        return self.directory / f"{target_date.strftime(self.date_format)}.log"

    def _switch_file(self) -> None:
        if self.stream:
            try:
                self.stream.flush()
            finally:
                self.stream.close()
        self.baseFilename = os.fspath(self._path_for_date(self._current_date))
        self.stream = self._open()

    def emit(self, record: logging.LogRecord) -> None:
        record_date = datetime.fromtimestamp(record.created).date()
        if record_date != self._current_date:
            self._current_date = record_date
            self._switch_file()
        super().emit(record)


def _build_handlers(cfg: LoggingConfig, level: int) -> list[logging.Handler]:
    handlers: list[logging.Handler] = []

    if cfg.rich_tracebacks:
        install_rich_traceback(show_locals=False)

    console = Console()
    progress_manager.use_console(console)

    if cfg.console:
        rich_handler = RichHandler(
            console=console,
            rich_tracebacks=cfg.rich_tracebacks,
            show_level=True,
            show_path=False,
            markup=True,
            log_time_format="%Y-%m-%d %H:%M:%S",
        )
        rich_handler.setLevel(level)
        rich_handler.setFormatter(logging.Formatter("%(context)s%(message)s"))
        rich_handler.addFilter(_context_filter)
        handlers.append(rich_handler)

    if cfg.log_dir:
        log_dir = Path(cfg.log_dir)
        file_handler = DailyFileHandler(log_dir)
        file_handler.setLevel(level)
        file_handler.setFormatter(
            logging.Formatter(
                "%(asctime)s | %(levelname)-8s | %(name)s | %(context)s%(message)s",
                datefmt="%Y-%m-%d %H:%M:%S",
            )
        )
        file_handler.addFilter(_context_filter)
        handlers.append(file_handler)

    return handlers


def init_logging(**kwargs: object) -> None:
    """Initialise the shared logging configuration.

    The function is idempotent; repeated calls reuse the existing configuration
    unless explicit keyword arguments request a different log level or other
    options.
    """

    with _config_lock:
        global _config, _listener, _queue

        cfg = LoggingConfig()
        for key, value in kwargs.items():
            if hasattr(cfg, key):
                setattr(cfg, key, value)  # type: ignore[arg-type]

        if _config is not None:
            if _config == cfg:
                return
            _teardown_locked()
        level = _parse_level(cfg.level)

        root = logging.getLogger()
        root.setLevel(logging.NOTSET)
        for handler in list(root.handlers):
            root.removeHandler(handler)

        handlers = _build_handlers(cfg, level)

        if cfg.queue and handlers:
            log_queue: SimpleQueue = SimpleQueue()
            queue_handler = QueueHandler(log_queue)
            queue_handler.setLevel(level)
            queue_handler.addFilter(_context_filter)
            root.addHandler(queue_handler)
            listener = QueueListener(log_queue, *handlers, respect_handler_level=True)
            listener.start()
            _queue = log_queue
            _listener = listener
        else:
            for handler in handlers:
                root.addHandler(handler)

        _config = cfg


def _teardown_locked() -> None:
    global _listener, _queue, _config
    if _listener:
        _listener.stop()
    _listener = None
    _queue = None
    _config = None
    progress_manager.reset_console()
    root = logging.getLogger()
    for handler in list(root.handlers):
        root.removeHandler(handler)
    root.handlers.clear()


def shutdown_logging() -> None:
    """Tear down queue listeners, intended for tests."""

    with _config_lock:
        _teardown_locked()


def get_logger(name: str | None = None) -> logging.Logger:
    with _config_lock:
        if _config is None:
            init_logging()
    cfg = _config or LoggingConfig()
    return logging.getLogger(name or cfg.app_name)


def set_level(level: str | int) -> None:
    new_level = _parse_level(level)
    root = logging.getLogger()
    for handler in root.handlers:
        handler.setLevel(new_level)
    logging.getLogger().setLevel(logging.NOTSET)
