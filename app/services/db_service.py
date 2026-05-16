import asyncio
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional

from openai import AsyncOpenAI

from app.core.config import get_settings
from app.core.database import get_database
from app.core.logger import get_logger

logger = get_logger(__name__)
settings = get_settings()


# ── Embeddings ────────────────────────────────────────────────────────────────

async def generate_embedding(text: str) -> List[float]:
    """
    Converts text into a 1536-dimensional vector using OpenAI's
    text-embedding-3-small model. This is the cheapest, fastest model
    and is more than sufficient for RAG chat retrieval.
    Returns empty list on failure (embedding is non-critical).
    """
    try:
        client = AsyncOpenAI(api_key=settings.OPENAI_API_KEY)
        response = await client.embeddings.create(
            input=text,
            model="text-embedding-3-small",
        )
        return response.data[0].embedding
    except Exception as e:
        logger.error(f"Embedding generation failed: {e}")
        return []


async def retrieve_long_term_memory(
    user_id: str,
    query_vector: List[float],
    exclude_session_id: str = "",
    limit: int = 4,
) -> List[str]:
    """
    Searches ALL past messages belonging to this user_id using a
    cosine-similarity $vectorSearch on MongoDB Atlas.
    Returns plain-text snippets of the most relevant past turns.
    Returns empty list on failure or if no index exists yet.
    """
    if not query_vector:
        return []

    db = get_database()
    if db is None:
        return []

    try:
        pipeline = [
            {
                "$vectorSearch": {
                    "index": "messages_vector_index",
                    "path": "embedding",
                    "queryVector": query_vector,
                    "numCandidates": 50,
                    "limit": limit + 2,  # fetch extra to allow exclusion filter
                    "filter": {"user_id": user_id},
                }
            },
            # Exclude current session to avoid echo chamber
            {"$match": {"session_id": {"$ne": exclude_session_id}}},
            {"$limit": limit},
            {"$project": {"role": 1, "content": 1, "_id": 0}},
        ]
        cursor = db.messages.aggregate(pipeline)
        docs = await cursor.to_list(length=limit)

        snippets = []
        for doc in docs:
            role = "User" if doc.get("role") == "user" else "MindBridge"
            content = doc.get("content", "")[:200].strip()
            if content:
                snippets.append(f"{role}: {content}")

        if snippets:
            logger.info(f"Long-term memory: {len(snippets)} relevant past turns retrieved for {user_id}")
        return snippets

    except Exception as e:
        # Graceful fallback — if no vector index exists yet, just skip
        logger.warning(f"Vector search unavailable (index may not exist yet): {e}")
        return []


# ── Personality conversion ────────────────────────────────────────────────────

_PERSONALITY_MAP = {
    "prefers_solitude":    {"Yes": "Introverted",         "No": "Extroverted",        "Sometimes": "Ambivert"},
    "logic_over_emotion":  {"Yes": "Logic-driven",        "No": "Emotion-driven",     "Sometimes": "Balanced thinker"},
    "plans_ahead":         {"Yes": "Structured planner",  "No": "Spontaneous",        "Sometimes": "Flexible planner"},
    "energized_by_social": {"Yes": "Socially energized",  "No": "Socially drained",   "Sometimes": "Selectively social"},
    "trusts_instincts":    {"Yes": "Trusts gut feelings", "No": "Analytical decider", "Sometimes": "Situational decider"},
}


def build_personality_summary(answers: dict) -> str:
    """Converts raw personality answers into a human-readable summary for the LLM."""
    traits = []
    for key, options in _PERSONALITY_MAP.items():
        val = answers.get(key, "Sometimes")
        traits.append(options.get(val, options["Sometimes"]))
    return ", ".join(traits)


# ── User profile ──────────────────────────────────────────────────────────────

async def upsert_user_profile(user_id: str, profile: dict, personality_answers: dict) -> bool:
    """Saves or updates a user profile with personality data."""
    db = get_database()
    if db is None:
        logger.error("DB not connected")
        return False

    personality_summary = build_personality_summary(personality_answers)

    update_doc = {
        "personality_answers": personality_answers,
        "personality_summary": personality_summary,
        "last_active": datetime.now(timezone.utc),
        # Include only the filtered profile fields
        **profile
    }

    update_doc.pop("user_id", None)
    update_doc.pop("_id", None)

    try:
        await db.users.update_one(
            {"user_id": user_id},
            {"$set": update_doc, "$setOnInsert": {"created_at": datetime.now(timezone.utc)}},
            upsert=True,
        )
        logger.info(f"Profile saved for user {user_id}: {personality_summary}")
        return True
    except Exception as e:
        logger.error(f"Failed to upsert user profile: {e}")
        return False


async def get_user_profile(user_id: str) -> Optional[dict]:
    """Loads a user profile from DB. Returns None if not found."""
    db = get_database()
    if db is None:
        logger.warning("DB not connected")
        return None

    try:
        from bson import ObjectId

        query = [{"user_id": user_id}]
        if ObjectId.is_valid(user_id):
            query.append({"_id": ObjectId(user_id)})

        doc = await db.users.find_one({"$or": query})

        # FC4: prefer first_name, fall back to full_name (legacy docs), then "Friend"
        name = "Friend"
        if doc:
            name = (
                doc.get("first_name")
                or doc.get("full_name")
                or doc.get("name")
                or "Friend"
            )
        return {
            "user_id": user_id,
            "name": name,
            "personality_summary": doc.get("personality_summary", "Not provided") if doc else "Not provided",
            "age": doc.get("age") if doc else None,
            "gender": doc.get("gender") if doc else None,
            "country": "IN",
        }
    except Exception as e:
        logger.error(f"Failed to get profile for {user_id}: {e}")
        return None


# ── Session ───────────────────────────────────────────────────────────────────

async def get_existing_session(user_id: str) -> Optional[dict]:
    """
    Returns the existing session for a user_id, if one exists.
    Enforces the one-device-one-session rule.
    """
    db = get_database()
    if db is None:
        return None
    try:
        doc = await db.sessions.find_one(
            {"user_id": user_id},
            sort=[("created_at", -1)],  # get the most recent one
        )
        if doc:
            return {
                "session_id": doc.get("session_id"),
                "user_id": doc.get("user_id"),
                "is_active": doc.get("is_active", True),
                "is_escalated": doc.get("is_escalated", False),
            }
        return None
    except Exception as e:
        logger.error(f"Failed to lookup session for user {user_id}: {e}")
        return None


async def get_session_history(session_id: str, limit: int = 20) -> list:
    """Returns the last N messages for a session in chronological order."""
    db = get_database()
    if db is None:
        return []
    try:
        cursor = db.messages.find(
            {"session_id": session_id},
            sort=[("timestamp", -1)],
            limit=limit,
        )
        messages = await cursor.to_list(length=limit)
        return list(reversed(messages))
    except Exception as e:
        logger.error(f"get_session_history failed for session {session_id}: {e}")
        return []


async def upsert_session(user_id: str, session_id: str) -> Optional[dict]:
    """Creates a session document if it does not exist, then returns it."""
    db = get_database()
    if db is None:
        return None
    try:
        await db.sessions.update_one(
            {"session_id": session_id},
            {"$setOnInsert": {
                "session_id": session_id,
                "user_id": user_id,
                "is_active": True,
                "is_escalated": False,
                "lethality_alert": False,
                "created_at": datetime.now(timezone.utc),
                "updated_at": datetime.now(timezone.utc),
            }},
            upsert=True,
        )
        return await db.sessions.find_one({"session_id": session_id})
    except Exception as e:
        logger.error(f"upsert_session failed for session {session_id}: {e}")
        return None


async def create_session(session_data: dict) -> bool:
    """Creates a new session document."""
    db = get_database()
    if db is None:
        return False

    try:
        doc = {
            "session_id": session_data["session_id"],
            "user_id": session_data["user_id"],
            "is_active": True,
            "lethality_alert": False,
            "is_escalated": False,          # True once handed off to a human
            "created_at": datetime.now(timezone.utc),
            "updated_at": datetime.now(timezone.utc),
        }
        await db.sessions.insert_one(doc)
        return True
    except Exception as e:
        logger.error(f"Failed to create session: {e}")
        return False


async def escalate_session(session_id: str) -> bool:
    """
    Marks a session as escalated to a human operator.
    Called immediately when safety.py detects is_crisis == True.
    """
    db = get_database()
    if db is None:
        return False
    try:
        await db.sessions.update_one(
            {"session_id": session_id},
            {"$set": {
                "is_escalated": True,
                "escalated_at": datetime.now(timezone.utc),
                "updated_at": datetime.now(timezone.utc),
            }},
        )
        logger.warning(f"[ESCALATION] Session {session_id} handed off to human operator.")
        return True
    except Exception as e:
        logger.error(f"Failed to escalate session {session_id}: {e}")
        return False


async def escalate_user(user_id: str) -> bool:
    """
    Marks all sessions for a user as escalated to a human operator.
    Called immediately when safety.py detects is_crisis == True.
    """
    db = get_database()
    if db is None:
        return False
    try:
        await db.sessions.update_many(
            {"user_id": user_id},
            {"$set": {
                "is_escalated": True,
                "escalated_at": datetime.now(timezone.utc),
                "updated_at": datetime.now(timezone.utc),
            }},
        )
        logger.warning(f"[ESCALATION] User {user_id} handed off to human operator.")
        return True
    except Exception as e:
        logger.error(f"Failed to escalate user {user_id}: {e}")
        return False


async def close_escalation(session_id: str) -> bool:
    """
    Marks a session as no longer escalated.
    Called by the human counselor when they click 'End Session'.
    After this, the user's next message will go back to the AI.
    """
    db = get_database()
    if db is None:
        return False
    try:
        await db.sessions.update_one(
            {"session_id": session_id},
            {"$set": {
                "is_escalated": False,
                "assigned_counselor_id": None,
                "escalation_closed_at": datetime.now(timezone.utc),
                "updated_at": datetime.now(timezone.utc),
            }},
        )
        logger.info(f"[ESCALATION CLOSED] Session {session_id} returned to AI mode.")
        return True
    except Exception as e:
        logger.error(f"Failed to close escalation for {session_id}: {e}")
        return False


async def close_escalation_by_user(user_id: str) -> bool:
    """
    Marks all sessions for a user as no longer escalated.
    """
    db = get_database()
    if db is None:
        return False
    try:
        await db.sessions.update_many(
            {"user_id": user_id, "is_escalated": True},
            {"$set": {
                "is_escalated": False,
                "assigned_counselor_id": None,
                "escalation_closed_at": datetime.now(timezone.utc),
                "updated_at": datetime.now(timezone.utc),
            }},
        )
        logger.info(f"[ESCALATION CLOSED] All sessions for user {user_id} returned to AI mode.")
        return True
    except Exception as e:
        logger.error(f"Failed to close escalation for user {user_id}: {e}")
        return False


async def is_session_escalated(session_id: str) -> bool:
    """
    Checks whether a session is currently under human control.
    Used by chat.py to block AI responses during active human intervention.
    """
    db = get_database()
    if db is None:
        return False
    try:
        doc = await db.sessions.find_one({"session_id": session_id})
        if doc and doc.get("is_escalated") is True:
            return True
        return False
    except Exception as e:
        logger.error(f"Failed to check escalation status for session {session_id}: {e}")
        return False


async def is_user_escalated(user_id: str) -> bool:
    """
    Checks whether ANY session for this user is currently under human control.
    Used by chat.py to block AI responses during active human intervention.
    """
    db = get_database()
    if db is None:
        return False
    try:
        doc = await db.sessions.find_one({"user_id": user_id, "is_escalated": True})
        if doc:
            return True
        return False
    except Exception as e:
        logger.error(f"Failed to check escalation status for user {user_id}: {e}")
        return False


# ── Messages ──────────────────────────────────────────────────────────────────

async def save_message(message_data: dict) -> bool:
    """
    Saves a single message (user or assistant) to MongoDB.

    The message is inserted immediately so it is visible in history without
    delay.  Embedding generation (OpenAI network call) then runs as a
    fire-and-forget background task so a slow or failing embedding call
    cannot block the insert or leave messages invisible to the history API.
    """
    db = get_database()
    if db is None:
        return False

    doc = {
        "session_id": message_data.get("session_id"),
        "user_id": message_data.get("user_id"),
        "turn_number": message_data.get("turn_number", 0),
        "role": message_data.get("role"),
        "content": message_data.get("content"),
        "timestamp": datetime.now(timezone.utc),
    }

    if "roberta_analysis" in message_data:
        doc["roberta_analysis"] = message_data["roberta_analysis"]
    if "llm_consensus" in message_data:
        doc["llm_consensus"] = message_data["llm_consensus"]
    if "is_human_message" in message_data:
        doc["is_human_message"] = message_data["is_human_message"]

    try:
        result = await db.messages.insert_one(doc)
        await db.sessions.update_one(
            {"session_id": message_data.get("session_id")},
            {"$set": {"updated_at": datetime.now(timezone.utc)}},
        )
    except Exception as e:
        logger.error(f"Failed to save message: {e}")
        return False

    # Generate embedding in the background — does not block message visibility
    async def _attach_embedding(doc_id, content: str):
        try:
            embedding = await generate_embedding(content)
            if embedding:
                await db.messages.update_one(
                    {"_id": doc_id},
                    {"$set": {"embedding": embedding}},
                )
        except Exception as emb_err:
            logger.warning(f"Embedding generation failed for message {doc_id}: {emb_err}")

    if message_data.get("content"):
        asyncio.create_task(_attach_embedding(result.inserted_id, message_data["content"]))

    return True


async def get_formatted_history(session_id: str, limit: int = 100) -> List[Dict[str, str]]:
    """
    Loads conversation history from MongoDB.
    Returns list of {role, content} dicts sorted chronologically.
    Limit set to 100 to give GPT-4o full conversation context.
    """
    db = get_database()
    if db is None:
        logger.warning(f"DB not connected, returning empty history for session {session_id}")
        return []

    try:
        cursor = db.messages.find({"session_id": session_id}).sort("timestamp", -1).limit(limit)
        docs = await cursor.to_list(length=limit)
        docs.reverse()  # chronological order for LLM

        return [
            {"role": doc.get("role", "user"), "content": doc.get("content", "")}
            for doc in docs
            if doc.get("content")
        ]
    except Exception as e:
        logger.error(f"Failed to fetch history for session {session_id}: {e}")
        return []


async def get_session_messages(session_id: str) -> List[Dict]:
    """
    Retrieves ALL messages for a given session_id.
    Returns them sorted chronologically (oldest to newest) to rebuild the UI.
    """
    db = get_database()
    if db is None:
        logger.warning(f"DB not connected, returning empty history for session {session_id}")
        return []

    try:
        cursor = db.messages.find({"session_id": session_id}).sort("timestamp", 1)
        docs = await cursor.to_list(length=None)

        formatted = []
        for doc in docs:
            if doc.get("content"):
                formatted.append({
                    "session_id": doc.get("session_id", "unknown"),
                    "role": doc.get("role", "unknown"),
                    "content": doc.get("content", ""),
                    "timestamp": _ensure_utc(doc.get("timestamp")),
                })

        logger.info(f"Fetched {len(formatted)} messages for session {session_id}")
        return formatted
    except Exception as e:
        logger.error(f"Failed to fetch messages for session {session_id}: {e}")
        return []


def _ensure_utc(ts) -> Optional[datetime]:
    """Return a timezone-aware UTC datetime regardless of whether ts is naive or aware."""
    if ts is None:
        return None
    if hasattr(ts, "tzinfo") and ts.tzinfo is not None:
        return ts.astimezone(timezone.utc)
    return ts.replace(tzinfo=timezone.utc)


async def get_user_messages(user_id: str, limit: int = 500) -> List[Dict]:
    """
    Retrieves the most recent `limit` messages for a given user_id.
    Capped to prevent unbounded queries that cause gateway timeouts (502).
    Sorted chronologically oldest→newest for the UI to render correctly.
    """
    db = get_database()
    if db is None:
        logger.warning(f"DB not connected, returning empty history for user {user_id}")
        return []

    try:
        from bson import ObjectId
        # Support both String and ObjectId user_id formats in the database
        query = {"user_id": user_id}
        if ObjectId.is_valid(user_id):
            query = {"$or": [{"user_id": user_id}, {"user_id": ObjectId(user_id)}]}

        # Fetch newest `limit` messages first (descending), then reverse for chronological order.
        cursor = db.messages.find(query).sort("timestamp", -1).limit(limit)
        docs = await cursor.to_list(length=limit)
        docs.reverse()

        formatted = []
        for doc in docs:
            if doc.get("role") == "system" or doc.get("sender_type") == "system":
                continue
            if doc.get("content"):
                formatted.append({
                    "user_id": doc.get("user_id", "unknown"),
                    "role": doc.get("role", "unknown"),
                    "content": doc.get("content", ""),
                    "timestamp": _ensure_utc(doc.get("timestamp")),
                })

        logger.info(f"Fetched {len(formatted)} messages for user {user_id}")
        return formatted
    except Exception as e:
        logger.error(f"Failed to fetch messages for user {user_id}: {e}")
        return []


async def get_all_sessions(user_id: str) -> List[Dict]:
    """
    Retrieves ALL sessions for a given user_id.
    Returns them sorted by creation time (newest first).
    """
    db = get_database()
    if db is None:
        logger.warning(f"DB not connected, returning empty sessions for user {user_id}")
        return []

    try:
        cursor = db.sessions.find({"user_id": user_id}).sort("created_at", -1)
        docs = await cursor.to_list(length=None)

        sessions = []
        for doc in docs:
            sessions.append({
                "session_id": doc.get("session_id"),
                "user_id": doc.get("user_id"),
                "is_active": doc.get("is_active", False),
                "is_escalated": doc.get("is_escalated", False),
                "created_at": doc.get("created_at").replace(tzinfo=timezone.utc) if doc.get("created_at") else None,
                "updated_at": doc.get("updated_at").replace(tzinfo=timezone.utc) if doc.get("updated_at") else None,
            })

        logger.info(f"Fetched {len(sessions)} sessions for user {user_id}")
        return sessions
    except Exception as e:
        logger.error(f"Failed to fetch sessions for user {user_id}: {e}")
        return []


async def get_escalated_sessions() -> List[Dict]:
    """
    Retrieves ALL sessions that have been escalated (is_escalated == True).
    Used by the Human Admin Dashboard to see which users need help.
    Joins the users collection to fetch the user's name.
    """
    db = get_database()
    if db is None:
        return []

    try:
        pipeline = [
            {"$match": {"is_escalated": True}},
            {"$sort": {"escalated_at": -1}},
            {
                "$group": {
                    "_id": "$user_id",
                    "doc": {"$first": "$$ROOT"}
                }
            },
            {"$replaceRoot": {"newRoot": "$doc"}},
            {"$sort": {"escalated_at": -1}},
            {
                "$lookup": {
                    "from": "users",
                    "localField": "user_id",
                    "foreignField": "user_id",
                    "as": "user_info"
                }
            },
            {
                "$addFields": {
                    "first_name": {
                        "$cond": {
                            "if": {"$gt": [{"$size": "$user_info"}, 0]},
                            "then": {"$arrayElemAt": ["$user_info.first_name", 0]},
                            "else": "Unknown"
                        }
                    },
                    "last_name": {
                        "$cond": {
                            "if": {"$gt": [{"$size": "$user_info"}, 0]},
                            "then": {"$arrayElemAt": ["$user_info.last_name", 0]},
                            "else": None
                        }
                    },
                }
            },
            {
                "$project": {
                    "user_info": 0
                }
            }
        ]
        
        cursor = db.sessions.aggregate(pipeline)
        docs = await cursor.to_list(length=None)

        sessions = []
        for doc in docs:
            sessions.append({
                "session_id": doc.get("session_id"),
                "user_id": doc.get("user_id"),
                "first_name": doc.get("first_name", "Unknown"),
                "last_name": doc.get("last_name"),
                "is_escalated": doc.get("is_escalated", False),
                "escalated_at": doc.get("escalated_at").replace(tzinfo=timezone.utc).isoformat() if doc.get("escalated_at") else None,
                "created_at": doc.get("created_at").replace(tzinfo=timezone.utc).isoformat() if doc.get("created_at") else None,
            })

        logger.info(f"Fetched {len(sessions)} escalated sessions.")
        return sessions
    except Exception as e:
        logger.error(f"Failed to fetch escalated sessions: {e}")
        return []


async def get_expired_escalated_sessions(timeout_minutes: int) -> List[Dict[str, str]]:
    """
    Finds escalated sessions that haven't been updated in `timeout_minutes` minutes.
    Returns a list of dicts containing `session_id` and `user_id`.
    """
    db = get_database()
    if db is None:
        return []

    expiration_time = datetime.now(timezone.utc) - timedelta(minutes=timeout_minutes)

    try:
        cursor = db.sessions.find({
            "is_escalated": True,
            "updated_at": {"$lte": expiration_time}
        })
        docs = await cursor.to_list(length=None)
        return [{"session_id": doc["session_id"], "user_id": doc.get("user_id", doc["session_id"])} for doc in docs]
    except Exception as e:
        logger.error(f"Failed to fetch expired escalated sessions: {e}")
        return []
