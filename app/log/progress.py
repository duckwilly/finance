
"""Progress utilities backed by rich progress bars."""
from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from typing import Iterable, Iterator, Optional

from rich.console import Console
from rich.progress import (
    BarColumn,
    MofNCompleteColumn,
    Progress,
    SpinnerColumn,
    TaskID,
    TaskProgressColumn,
    TextColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
    TransferSpeedColumn,
)


@dataclass
class _Task:
    progress: Progress
    task_id: TaskID

    def advance(self, amount: float = 1.0) -> None:
        self.progress.advance(self.task_id, amount)

    def update(self, **kwargs: object) -> None:
        self.progress.update(self.task_id, **kwargs)


class ProgressManager:
    """Create progress bars and spinners that play nicely with logging."""

    def __init__(self) -> None:
        self._console: Console = Console()

    def use_console(self, console: Console) -> None:
        self._console = console

    def reset_console(self) -> None:
        self._console = Console()

    def _columns(self, unit: str) -> list[object]:
        return [
            TextColumn("[bold blue]{task.description}[/]"),
            BarColumn(bar_width=None),
            TaskProgressColumn(),
            MofNCompleteColumn(),
            TimeElapsedColumn(),
            TimeRemainingColumn(),
            TransferSpeedColumn(unit=f"{unit}/s"),
        ]

    @contextmanager
    def task(
        self,
        description: str,
        *,
        total: Optional[float] = None,
        unit: str = "items",
    ) -> Iterator[_Task]:
        progress = Progress(*self._columns(unit), console=self._console, transient=True)
        with progress:
            task_id = progress.add_task(description, total=total)
            yield _Task(progress, task_id)

    def track(
        self,
        iterable: Iterable[object],
        *,
        description: str,
        total: Optional[int] = None,
        unit: str = "items",
    ) -> Iterator[object]:
        with self.task(description, total=total, unit=unit) as task:
            for item in iterable:
                yield item
                task.advance(1)

    @contextmanager
    def spinner(self, description: str) -> Iterator[None]:
        progress = Progress(
            SpinnerColumn(),
            TextColumn("[bold blue]{task.description}[/]"),
            TimeElapsedColumn(),
            console=self._console,
            transient=True,
        )
        with progress:
            task_id = progress.add_task(description, total=None)
            yield
            progress.update(task_id, description=f"{description} ✓")


progress_manager = ProgressManager()
