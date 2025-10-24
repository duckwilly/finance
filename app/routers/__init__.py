"""FastAPI routers for the finance application."""

from .corporate import router as corporate_router
from .dashboard import router as dashboard_router
from .individuals import router as individuals_router

__all__ = ["dashboard_router", "individuals_router", "corporate_router"]
