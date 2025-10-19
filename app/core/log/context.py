
"""Context helpers that enrich log records with structured metadata."""
from __future__ import annotations

import contextvars
import logging
from typing import Dict


_context_var: contextvars.ContextVar[dict[str, object]] = contextvars.ContextVar(
    "log_context", default={}
)


class LogContext:
    """Utility to bind contextual information to subsequent log records."""

    def bind(self, **values: object) -> None:
        current = dict(_context_var.get())
        current.update({k: v for k, v in values.items() if v is not None})
        _context_var.set(current)

    def unbind(self, *keys: str) -> None:
        current = dict(_context_var.get())
        for key in keys:
            current.pop(key, None)
        _context_var.set(current)

    def clear(self) -> None:
        _context_var.set({})

    def as_dict(self) -> Dict[str, object]:
        return dict(_context_var.get())


class ContextFilter(logging.Filter):
    """Attach contextual key-value pairs to log records."""

    def filter(self, record: logging.LogRecord) -> bool:
        context = _context_var.get()
        if context:
            record.context = " ".join(f"{k}={v}" for k, v in context.items()) + " "
        else:
            record.context = ""
        return True


log_context = LogContext()
