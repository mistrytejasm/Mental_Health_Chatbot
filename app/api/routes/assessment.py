"""
Assessment Route
────────────────
POST /api/assessment — One-time onboarding from Android.
Saves personality, creates session, returns opening message.
If the user already has a session, returns the existing session_id.
"""

import uuid
from datetime import datetime, timezone
from bson import ObjectId                              # Fix #3: top-level import
from bson.errors import InvalidId
from fastapi import APIRouter, HTTPException, Depends

from app.api.schemas.request import AssessmentRequest
from app.api.schemas.response import AssessmentResponse
from app.services import llm as llm_svc
from app.core.logger import get_logger
from app.core.auth.oauth2 import get_current_user
from app.core.database import get_database
from app.services.db_service import build_personality_summary
from app.services.db_service import save_message

logger = get_logger(__name__)
router = APIRouter(prefix="/api/assessment", tags=["assessment"])


@router.post("", response_model=AssessmentResponse)
async def submit_assessment(req: AssessmentRequest, current_user = Depends(get_current_user)):
    """
    Called when a user completes onboarding in Android.
    1. Saves/updates personality to DB
    2. Checks if a session already exists for this user
       - If YES: returns the existing session_id (no new session created)
       - If NO:  creates a new session + generates opening message
    """

    # Fix #4: Harden user_id extraction — never silently continue with an empty ID
    user_id = current_user.get("user_id") or current_user.get("_id")
    if not user_id:
        raise HTTPException(status_code=401, detail="Invalid token payload")
    user_id = str(user_id)
    logger.info(f"Assessment received for user: {user_id}")

    db = get_database()
    if db is None:
        raise HTTPException(status_code=500, detail="Database connection failed")

    # Fetch user doc first to check if this is their first assessment
    or_query = [{"user_id": user_id}]
    try:
        or_query.append({"_id": ObjectId(user_id)})
    except InvalidId:
        pass

    user_doc = await db.users.find_one({"$or": or_query})
    is_first_assessment = not bool(user_doc.get("personality_summary")) if user_doc else True

    # 1. Save/update personality only
    personality_dict = req.personality_answers.model_dump()
    personality_summary = build_personality_summary(personality_dict)

    update_doc = {
        "personality_answers": personality_dict,
        "personality_summary": personality_summary,
        "last_active": datetime.now(timezone.utc),
    }

    await db.users.update_one(
        {"$or": or_query},
        {"$set": update_doc}
    )

    # 2. Get existing session or create a new one
    existing = await db.sessions.find_one({"user_id": user_id}, sort=[("created_at", -1)])
    if existing:
        session_id = existing.get("session_id")
        logger.info(f"Reusing existing session {session_id} for user {user_id}")
    else:
        session_id = str(uuid.uuid4())
        try:
            await db.sessions.insert_one({
                "session_id": session_id,
                "user_id": user_id,
                "is_active": True,
                "lethality_alert": False,
                "is_escalated": False,
                "created_at": datetime.now(timezone.utc),
                "updated_at": datetime.now(timezone.utc),
            })
            logger.info(f"Assessment complete. New session: {session_id}")
        except Exception as e:
            logger.error(f"Failed to create session: {e}")
            raise HTTPException(status_code=500, detail="Failed to create session")

    # Fetch real user name from DB; fall back to "Friend" only if missing
    user_name = "Friend"
    if user_doc:
        user_name = (
            user_doc.get("first_name")
            or user_doc.get("name")
            or user_doc.get("full_name")
            or current_user.get("name")
            or "Friend"
        )
        # Use first name only for a warmer greeting
        user_name = user_name.strip().split()[0] if user_name.strip() else "Friend"

    if is_first_assessment:
        # 4. Generate opening message with full profile context
        llm_profile = {
            "name": user_name,
            "personality_summary": personality_summary,
            "age": user_doc.get("age") if user_doc else None,
            "gender": user_doc.get("gender") if user_doc else None,
            "country": "IN",
        }

        opening = await llm_svc.get_opening_message(llm_profile)
    else:
        # Returning user — personalized welcome back using their name
        opening = f"Welcome back, {user_name}! How are you feeling today?"

    # Save opening message to DB so it appears in history for both new and returning users
    await save_message({
        "session_id": session_id,
        "user_id": user_id,
        "turn_number": 0,
        "role": "assistant",
        "content": opening,
    })

    return AssessmentResponse(
        status="success",
        session_id=session_id,
        opening_message=opening,
        timestamp=datetime.now(timezone.utc),
        user_id=user_id,
    )