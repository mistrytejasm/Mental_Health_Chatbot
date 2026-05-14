"""
Crisis & Resources API routes.
"""

from fastapi import APIRouter
from app.core.constants import CRISIS_RESOURCES
from app.api.schemas.response import CrisisResource
from app.core.logger import get_logger

logger = get_logger(__name__)

router = APIRouter(prefix="/api", tags=["Crisis & Resources"])

@router.get("/resources")
async def get_resources():
    """Returns a list of mental health crisis resources."""
    logger.info("Resources — Serving crisis resource list")
    return {"resources": CRISIS_RESOURCES}


@router.post("/crisis")
async def crisis_response():
    """Returns immediate crisis response and resources."""
    logger.info("Crisis — Immediate crisis endpoint triggered")
    return {
        "message": (
            "You are not alone, and your life matters deeply. "
            "Please reach out to a trained counselor right now."
        ),
        "crisis_resources": CRISIS_RESOURCES,
    }
