# logger.py
from __future__ import annotations
from datetime import datetime
from pathlib import Path
from typing import Literal

Level = Literal["DEBUG", "INFO", "WARNING", "ERROR"]
_LEVEL_ORDER = {"DEBUG": 10, "INFO": 20, "WARNING": 30, "ERROR": 40}

_COLORS = {
    "DEBUG": "\033[94m",   # Blue
    "INFO": "\033[92m",    # Green
    "WARNING": "\033[93m", # Yellow
    "ERROR": "\033[91m",   # Red
    "RESET": "\033[0m"
}


class Logger:
    """
    Minimal colorized logger with daily log file rotation.
    Example filename: logs/2025-10-10_app.log
    """

    def __init__(self, name: str = "app", level: Level = "INFO", console: bool = True):
        self.name = name
        self.level = level
        self.console = console
        self.log_dir = Path("logs")
        self.log_dir.mkdir(parents=True, exist_ok=True)
        self._update_path()  # sets self.path for the current day

    def _update_path(self) -> None:
        """Ensure the log file path is set for today's date."""
        today = datetime.now().strftime("%Y-%m-%d")
        self.path = self.log_dir / f"{today}_{self.name}.log"

    def _should_log(self, level: Level) -> bool:
        return _LEVEL_ORDER[level] >= _LEVEL_ORDER[self.level]

    def _format(self, level: Level, msg: str) -> str:
        """Prefix every line with timestamp and level for consistent formatting."""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        prefix = f"[{timestamp}] [{level}] "
        return "\n".join(f"{prefix}{line}" for line in msg.splitlines())

    def _write_file(self, text: str) -> None:
        self._update_path()  # ensure log file rotates daily
        with self.path.open("a", encoding="utf-8") as f:
            f.write(text + "\n")

    def log(self, level: Level, msg: str) -> None:
        if not self._should_log(level):
            return
        formatted = self._format(level, msg)
        if self.console:
            color = _COLORS.get(level, "")
            print(f"{color}{formatted}{_COLORS['RESET']}")
        self._write_file(formatted)

    def debug(self, msg: str) -> None:
        self.log("DEBUG", msg)

    def info(self, msg: str) -> None:
        self.log("INFO", msg)

    def warning(self, msg: str) -> None:
        self.log("WARNING", msg)

    def error(self, msg: str) -> None:
        self.log("ERROR", msg)


# Example singleton logger
log = Logger(name="app", level="INFO", console=True)