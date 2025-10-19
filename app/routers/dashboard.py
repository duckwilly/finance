"""Placeholder dashboard routes."""
from __future__ import annotations

from fastapi import APIRouter

from app.core.logger import get_logger

router = APIRouter(prefix="/dashboard", tags=["dashboard"])
LOGGER = get_logger(__name__)


@router.get("/", summary="Finance dashboard placeholder")
async def read_dashboard() -> dict[str, str]:
    """Return a placeholder payload until the UI is implemented."""

    LOGGER.info("Dashboard endpoint requested")
    return {"status": "pending", "message": "Dashboard implementation coming soon."}
