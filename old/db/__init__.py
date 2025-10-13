# db/__init__.py
from .engine import get_engine  # re-export for easy import
__all__ = ["get_engine"]