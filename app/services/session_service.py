import asyncio
from datetime import datetime, timezone
from app.core.database import get_database
from app.services.routing_service import get_available_counselor_count, route_crisis_session
from app.services.db_service import get_existing_session, escalate_session

class SessionService:
    @staticmethod
    async def process_manual_escalation(user_id: str) -> dict:
        """
        Handles the business logic for manual escalation.
        Returns a dict with 'status' and optionally 'message'.
        """
        # 1. Fetch the user's active session
        session_data = await get_existing_session(user_id)
        if not session_data:
            return {"status": "failed", "message": "No active chat session found to escalate."}
            
        actual_session_id = session_data["session_id"]

        # 2. Check if any counselors are actually online before escalating
        count = await get_available_counselor_count()
        if count == 0:
            hotline_text = "No counselor is available at the moment. Please call the helpline at 911."
            db = get_database()
            if db is not None:
                await db.messages.insert_one({
                    "session_id": actual_session_id,
                    "user_id": user_id,
                    "role": "system",
                    "sender_type": "system",
                    "content": hotline_text,
                    "timestamp": datetime.now(timezone.utc),
                })
            return {"status": "failed", "message": hotline_text}

        # 3. Mark the session as escalated (is_escalated = True)
        success = await escalate_session(actual_session_id)
        if not success:
            return {"status": "failed", "message": "Failed to escalate session"}

        # 4. Create a pseudo-consensus to feed into the routing engine
        consensus = {
            "is_crisis": True,
            "category": "manual_escalation",
            "intensity": "high",
            "reasoning": "User requested manual escalation via app button",
        }

        # 5. Trigger the smart routing engine in the background.
        asyncio.create_task(
            route_crisis_session(
                user_id=user_id,
                session_id=actual_session_id,
                consensus=consensus,
            )
        )

        return {"status": "success"}

    @staticmethod
    def build_recent_history_string(history: list[dict], n_turns: int = 4) -> str:
        if not history:
            return ""
        recent = history[-(n_turns * 2):]
        lines = []
        for msg in recent:
            role = "User" if msg.get("role") == "user" else "MindBuddy"
            content = msg.get("content", "").strip()
            if content:
                lines.append(f"{role}: {content}")
        return "\n".join(lines)

    @staticmethod
    def safe_fallback_consensus() -> dict:
        return {
            "llm_sentiment":    "neutral",
            "category":         "general",
            "intensity":        "moderate",
            "is_crisis":        False,
            "wants_counselor":  False,
            "crisis_type":      None,
            "reasoning":        "fallback",
            "recommended_tone": "validating",
            "message_class":    "emotional_ongoing",
            "token_budget":     320,
        }

session_service = SessionService()
