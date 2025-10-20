
"""Timing helpers to log duration and throughput of operations."""
from __future__ import annotations

import logging
from contextlib import contextmanager
from dataclasses import dataclass, field
from time import perf_counter
from typing import Any, Optional

from sqlalchemy.orm import Session


class DatabaseCallTracker:
    """Tracks database calls made during an operation."""

    def __init__(self) -> None:
        self.call_count = 0
        self.original_execute = None
        self.original_scalar = None
        self.original_scalars = None

    def track_calls(self, session: Session) -> Session:
        """Wrap session methods to track database calls."""
        if not hasattr(session, '_call_tracker'):
            # Store original methods
            self.original_execute = session.execute
            self.original_scalar = session.scalar
            self.original_scalars = session.scalars

            # Wrap execute method
            def tracked_execute(*args, **kwargs):
                self.call_count += 1
                return self.original_execute(*args, **kwargs)

            # Wrap scalar method
            def tracked_scalar(*args, **kwargs):
                self.call_count += 1
                return self.original_scalar(*args, **kwargs)

            # Wrap scalars method
            def tracked_scalars(*args, **kwargs):
                self.call_count += 1
                return self.original_scalars(*args, **kwargs)

            # Replace methods
            session.execute = tracked_execute
            session.scalar = tracked_scalar
            session.scalars = tracked_scalars

            # Mark as tracked
            session._call_tracker = self

        return session


@dataclass
class _Timer:
    label: str
    logger: logging.Logger
    level: int
    unit: str
    expected_total: Optional[int]
    count: int = 0
    start: float = field(default_factory=perf_counter)
    db_call_tracker: Optional[DatabaseCallTracker] = None
    track_db_calls: bool = False

    def add(self, amount: int = 1) -> None:
        self.count += amount

    def set_total(self, total: int) -> None:
        self.expected_total = total

    def _resolved_total(self) -> Optional[int]:
        return self.expected_total if self.expected_total is not None else self.count

    def get_db_call_count(self) -> int:
        """Get the number of database calls tracked."""
        return self.db_call_tracker.call_count if self.db_call_tracker else 0

    def finish(self, success: bool = True) -> None:
        elapsed = perf_counter() - self.start
        total = self._resolved_total()
        db_calls = self.get_db_call_count()

        if success:
            message = f"{self.label} completed in {elapsed:.2f}s"
            if total is not None:
                message += f" ({total:,} {self.unit}"
                if elapsed > 0 and total:
                    rate = total / elapsed
                    message += f" @ {rate:,.0f} {self.unit}/s"
                message += ")"

            if self.track_db_calls and db_calls > 0:
                message += f" ({db_calls:,} DB calls)"

            self.logger.log(self.level, message)
        else:
            fail_message = f"{self.label} failed after {elapsed:.2f}s"
            if total is not None:
                fail_message += f" ({total:,} {self.unit})"
            if self.track_db_calls and db_calls > 0:
                fail_message += f" ({db_calls:,} DB calls)"
            self.logger.error(fail_message)


@contextmanager
def timeit(
    label: str,
    *,
    logger: Optional[logging.Logger] = None,
    level: int = logging.INFO,
    unit: str = "items",
    total: Optional[int] = None,
    track_db_calls: bool = False,
    session: Optional[Session] = None,
) -> _Timer:
    """Context manager for timing operations with optional database call tracking.

    Args:
        label: Description of the operation being timed
        logger: Logger instance to use (defaults to "finance.timer")
        level: Logging level for the timing message
        unit: Unit for throughput calculation (e.g., "items", "rows")
        total: Expected total count for throughput calculation
        track_db_calls: Whether to track database calls made during the operation
        session: SQLAlchemy session to track database calls on (required if track_db_calls=True)
    """
    log = logger or logging.getLogger("finance.timer")
    db_tracker = None

    if track_db_calls:
        if session is None:
            raise ValueError("session parameter is required when track_db_calls=True")
        db_tracker = DatabaseCallTracker()

    timer = _Timer(
        label=label,
        logger=log,
        level=level,
        unit=unit,
        expected_total=total,
        db_call_tracker=db_tracker,
        track_db_calls=track_db_calls
    )

    try:
        # Track database calls if requested
        if track_db_calls and session is not None:
            db_tracker.track_calls(session)

        yield timer
    except Exception:
        timer.finish(success=False)
        raise
    else:
        timer.finish(success=True)
