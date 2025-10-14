
"""Timing helpers to log duration and throughput of operations."""
from __future__ import annotations

import logging
from contextlib import contextmanager
from dataclasses import dataclass, field
from time import perf_counter
from typing import Optional


@dataclass
class _Timer:
    label: str
    logger: logging.Logger
    level: int
    unit: str
    expected_total: Optional[int]
    count: int = 0
    start: float = field(default_factory=perf_counter)

    def add(self, amount: int = 1) -> None:
        self.count += amount

    def set_total(self, total: int) -> None:
        self.expected_total = total

    def _resolved_total(self) -> Optional[int]:
        return self.expected_total if self.expected_total is not None else self.count

    def finish(self, success: bool = True) -> None:
        elapsed = perf_counter() - self.start
        total = self._resolved_total()
        if success:
            message = f"{self.label} completed in {elapsed:.2f}s"
            if total is not None:
                message += f" ({total:,} {self.unit}"
                if elapsed > 0 and total:
                    rate = total / elapsed
                    message += f" @ {rate:,.0f} {self.unit}/s"
                message += ")"
            self.logger.log(self.level, message)
        else:
            fail_message = f"{self.label} failed after {elapsed:.2f}s"
            if total:
                fail_message += f" ({total:,} {self.unit})"
            self.logger.error(fail_message)


@contextmanager
def timeit(
    label: str,
    *,
    logger: Optional[logging.Logger] = None,
    level: int = logging.INFO,
    unit: str = "items",
    total: Optional[int] = None,
) -> _Timer:
    log = logger or logging.getLogger("finance.timer")
    timer = _Timer(label=label, logger=log, level=level, unit=unit, expected_total=total)
    try:
        yield timer
    except Exception:
        timer.finish(success=False)
        raise
    else:
        timer.finish(success=True)
