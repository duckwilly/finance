"""FastAPI routers for the finance application."""

from .auth import router as auth_router
from .corporate import router as corporate_router
from .dashboard import router as dashboard_router
from .individuals import router as individuals_router
from .presentation import router as presentation_router

__all__ = [
    "auth_router",
    "dashboard_router",
    "individuals_router",
    "corporate_router",
    "presentation_router",
]
