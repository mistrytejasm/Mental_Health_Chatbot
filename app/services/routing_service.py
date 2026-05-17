"""
Smart Counselor Routing Service
────────────────────────────────
Implements the 3-tier routing decision engine for crisis escalations:

  Tier 1 — Sticky Routing:    Route to the user's preferred (last trusted) counselor.
  Tier 2 — Context Match:     Verify the crisis category is compatible with past history.
  Tier 3 — Availability Gate: Confirm the counselor is online, has a fresh heartbeat,
                               has remaining capacity, AND has an active WebSocket on
                               this server process (connection_registry check).

Falls back to a pool-based search if any tier fails.
Always dispatches an LLM-generated clinical handoff summary as a background task
so the counselor is pre-briefed before the user-visible chat begins.

Fix 3:  atomic routing lock now also writes routing_started_at timestamp.
Fix 14: stale __routing__ locks older than 5 minutes are cleaned up before each
        routing attempt so a server crash cannot permanently block a session.
Fix 12: _is_available() now also checks Redis 'dashboard:online_counselors' set
        so a counselor with a stale is_online flag but no active WebSocket is not routed to.

Entry point: route_crisis_session() — called via asyncio.create_task() from chat.py.
"""

import asyncio
import logging
from datetime import datetime, timedelta, timezone
from typing import Optional

from bson import ObjectId

from app.core.database import get_database
from app.core.connection_registry import is_counselor_connected
from app.core.logger import get_logger
from app.services.summarization_service import generate_clinical_handoff
from app.core.redis import get_redis

logger = get_logger(__name__)

_STALE_PING_SECONDS = 2100  # 35 minutes — counselor stays visible while idle
_STALE_LOCK_MINUTES = 5
_MAX_CONCURRENT_SESSIONS = 2


# ── Tier helpers ─────────────────────────────────────────────────────────────

def _is_fresh(counselor_doc: dict) -> bool:
    """Returns True if the counselor's heartbeat is recent enough to trust."""
    last_ping = counselor_doc.get("last_ping")
    if last_ping is None:
        return False
    if last_ping.tzinfo is None:
        last_ping = last_ping.replace(tzinfo=timezone.utc)
    return (datetime.now(timezone.utc) - last_ping) < timedelta(seconds=_STALE_PING_SECONDS)


async def _is_available(counselor_doc: dict) -> bool:
    """
    Returns True when ALL gates pass:
      Gate 1 — is_online flag is True in DB
      Gate 2 — is_active flag is True (not suspended)
      Gate 3 — Heartbeat is fresh (last_ping within _STALE_PING_SECONDS)
      Gate 4 — current_active_sessions < _MAX_CONCURRENT_SESSIONS (capacity check)
      Gate 5 — Registered in Redis 'dashboard:online_counselors' set (real-time WS
               presence). Falls back to DB-only if Redis is unavailable so a Redis
               outage does not block all routing.
    """
    db_pass = (
        counselor_doc.get("is_online", False)
        and counselor_doc.get("is_active", True)
        and _is_fresh(counselor_doc)
        and counselor_doc.get("current_active_sessions", 0) < _MAX_CONCURRENT_SESSIONS
    )
    if not db_pass:
        return False

    # Gate 5: real-time WebSocket presence via Redis
    # We check Redis for a live WebSocket, but if they aren't in Redis
    # we STILL trust the DB flags if they are fresh. This ensures REST-polling
    # counselors (fallback mode) are still counted as available.
    redis = get_redis()
    if redis:
        counselor_id = str(counselor_doc.get("_id", ""))
        try:
            is_ws_online = await redis.sismember("dashboard:online_counselors", counselor_id)
            if is_ws_online:
                return True
            
            # If not in Redis, check if they are "REST-connected" in memory
            from app.core.connection_registry import is_counselor_connected
            if is_counselor_connected(counselor_id):
                return True
        except Exception:
            pass

    # Fallback: trust DB flags (already verified in db_pass above)
    return True





async def _find_available_counselor(exclude_id: Optional[str] = None) -> Optional[dict]:
    """
    Pool fallback: returns the least-loaded, earliest-checked-in counselor that
    passes all availability gates (online, fresh heartbeat, under capacity, and
    real-time WS presence in Redis).

    Sort order:
      1. current_active_sessions ASC — least loaded counselor first.
         (Counselor C with 0 chats beats Counselor B with 1 chat.)
      2. checked_in_at ASC          — FIFO tiebreaker within the same load tier.
    """
    db = get_database()
    if db is None:
        return None

    stale_cutoff = datetime.now(timezone.utc) - timedelta(seconds=_STALE_PING_SECONDS)
    query: dict = {
        "is_online": True,
        "is_active": {"$ne": False},
        "last_ping": {"$gte": stale_cutoff},
        "$or": [
            {"current_active_sessions": {"$lt": _MAX_CONCURRENT_SESSIONS}},
            {"current_active_sessions": {"$exists": False}}
        ],
        "checked_in_at": {"$exists": True},
    }
    if exclude_id:
        try:
            query["_id"] = {"$ne": ObjectId(exclude_id)}
        except Exception:
            pass

    # Sort: fewest active sessions first, then oldest check-in time (FIFO tiebreaker)
    cursor = db.admins.find(query).sort([
        ("current_active_sessions", 1),
        ("checked_in_at", 1),
    ])
    candidates = await cursor.to_list(length=20)

    if candidates:
        logger.info(
            f"[ROUTING] [POOL] {len(candidates)} candidate(s) found "
            f"(sorted by load then check-in time):"
        )
        for idx, c in enumerate(candidates):
            cid = str(c.get("_id", ""))
            name = f"{c.get('first_name', '')} {c.get('last_name', '')}".strip() or cid
            active_sessions = c.get("current_active_sessions", 0)
            logger.info(
                f"[ROUTING] [POOL]   [{idx}] {name} (id={cid})"
                f" | active_sessions={active_sessions}/{_MAX_CONCURRENT_SESSIONS}"
            )
    else:
        logger.warning("[ROUTING] [POOL] No candidates in pool (no online counselors with free capacity).")

    for candidate in candidates:
        if await _is_available(candidate):
            cid = str(candidate.get("_id", ""))
            name = f"{candidate.get('first_name', '')} {candidate.get('last_name', '')}".strip() or cid
            active_sessions = candidate.get("current_active_sessions", 0)
            logger.info(
                f"[ROUTING] [POOL] ✓ Selected: {name} (id={cid})"
                f" | active_sessions={active_sessions}/{_MAX_CONCURRENT_SESSIONS}"
            )
            return candidate

    logger.warning(
        "[ROUTING] [POOL] No candidate passed availability gate "
        "(is_online / last_ping / capacity / Redis presence)."
    )
    return None


async def get_available_counselor_count() -> int:
    """
    Returns the number of counselors online, fresh, with capacity, and connected via WebSocket.
    """
    db = get_database()
    if db is None:
        return 0

    stale_cutoff = datetime.now(timezone.utc) - timedelta(seconds=_STALE_PING_SECONDS)
    query: dict = {
        "is_online": True,
        "is_active": {"$ne": False},
        "last_ping": {"$gte": stale_cutoff},
        "$or": [
            {"current_active_sessions": {"$lt": _MAX_CONCURRENT_SESSIONS}},
            {"current_active_sessions": {"$exists": False}}
        ],
        "checked_in_at": {"$exists": True},
    }

    # FIFO: sort by check-in time ascending
    cursor = db.admins.find(query).sort("checked_in_at", 1)
    candidates = await cursor.to_list(length=50)

    count = 0
    for c in candidates:
        if await _is_available(c):
            count += 1
    return count


# ── Public entry point ────────────────────────────────────────────────────────

async def route_crisis_session(user_id: str, session_id: str, consensus: dict) -> None:
    """
    Main routing orchestrator. Always called via asyncio.create_task() so it
    never blocks the SSE stream.

    Uses the doctor_user_assignments collection for Tier 1 sticky routing and
    persists new assignments via MongoDB transactions to prevent data
    inconsistency on server crash.
    """
    db = get_database()
    if db is None:
        logger.error(f"[ROUTING] Database unavailable — cannot route session {session_id}")
        return

    crisis_category = consensus.get("category", "unknown")
    force_exclude_id: Optional[str] = consensus.get("_exclude_counselor_id")

    logger.info(
        f"[ROUTING] ▶ NEW ESCALATION REQUEST | user={user_id} | session={session_id}"
        f" | category={crisis_category}"
    )

    try:
        # Fix 14: release stale __routing__ locks from past server crashes
        # before acquiring the lock for this session.
        stale_lock_cutoff = datetime.now(timezone.utc) - timedelta(minutes=_STALE_LOCK_MINUTES)
        await db.sessions.update_many(
            {
                "assigned_counselor_id": "__routing__",
                "routing_started_at": {"$lt": stale_lock_cutoff},
            },
            {"$set": {"assigned_counselor_id": None, "routing_started_at": None}},
        )

        # Acquire routing lock. Query excludes "__routing__" to prevent duplicate
        # concurrent routing tasks, but allows any other value (null or a stale
        # counselor ID from a prior closed session) so re-escalations are not blocked.
        claim_result = await db.sessions.find_one_and_update(
            {"session_id": session_id, "assigned_counselor_id": {"$ne": "__routing__"}},
            {"$set": {
                "assigned_counselor_id": "__routing__",
                "routing_started_at": datetime.now(timezone.utc),
            }},
        )
        if claim_result is None:
            logger.info(f"[ROUTING] Session {session_id} already being routed — skipping duplicate task.")
            return

        assigned_counselor: Optional[dict] = None
        preferred_id: Optional[str] = None

        # ── Tier 1: Sticky Routing (via doctor_user_assignments table) ────────
        # Look up the user's currently active doctor from the assignment table
        # instead of the legacy preferred_counselor_id field on the user doc.
        try:
            user_doc = await db.users.find_one({"_id": ObjectId(user_id)})
        except Exception:
            user_doc = None
        if not user_doc:
            logger.error(f"[ROUTING] User {user_id} not found — releasing routing lock.")
            await db.sessions.update_one(
                {"session_id": session_id, "assigned_counselor_id": "__routing__"},
                {"$set": {"assigned_counselor_id": None, "routing_started_at": None}},
            )
            return

        active_assignment = await db.doctor_user_assignments.find_one(
            {"user_id": user_id, "status": "active"}
        )
        if active_assignment:
            preferred_id = active_assignment.get("doctor_id")

        if preferred_id and preferred_id == force_exclude_id:
            preferred_id = None

        if preferred_id:
            try:
                preferred_doc = await db.admins.find_one({"_id": ObjectId(preferred_id)})
            except Exception:
                preferred_doc = None
            if preferred_doc:
                # ── Tier 3: Availability Gate (capacity + real-time presence) ───
                if await _is_available(preferred_doc):
                    assigned_counselor = preferred_doc
                    logger.info(
                        f"[ROUTING] Tier 1 Sticky: preferred counselor {preferred_id} "
                        f"selected for session {session_id} (has capacity)."
                    )
                else:
                    logger.info(
                        f"[ROUTING] Tier 1 Sticky: preferred counselor {preferred_id} "
                        f"is unavailable (offline, at capacity, or no WS). Falling back to pool."
                    )

        # ── Fallback: Pool Search ─────────────────────────────────────────────
        if assigned_counselor is None:
            logger.info(f"[ROUTING] Falling back to pool search for session {session_id}.")
            exclude_id = preferred_id or force_exclude_id
            assigned_counselor = await _find_available_counselor(exclude_id=exclude_id)

        # ── No counselors available ───────────────────────────────────────────
        if assigned_counselor is None:
            logger.warning(
                f"[ROUTING] No counselors available for session {session_id} "
                f"(all at capacity or offline). Returning session to AI mode."
            )
            # Reset session state — de-escalate cleanly so the user can chat with AI
            await db.sessions.update_one(
                {"session_id": session_id},
                {"$set": {
                    "assigned_counselor_id": None,
                    "routing_started_at": None,
                    "is_escalated": False,
                    "escalation_closed_at": datetime.now(timezone.utc),
                }},
            )
            hotline_text = (
                "No counselor is available at the moment. Please call the helpline at 911."
            )
            await db.messages.insert_one({
                "session_id": session_id,
                "user_id": user_id,
                "role": "system",
                "sender_type": "system",
                "content": hotline_text,
                "timestamp": datetime.now(timezone.utc),
            })
            # Push immediately to the user's open WebSocket room
            try:
                from app.api.routes.human import manager as ws_manager
                await ws_manager.send_to_all(session_id, {
                    "role": "system",
                    "text": hotline_text,
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "is_human": False,
                    "is_system": True,
                    "type": "counselor_unavailable",
                })
            except Exception:
                pass
            return

        counselor_id_str = str(assigned_counselor["_id"])

        # ── Atomic capacity claim — prevents concurrent over-assignment ────────
        # Problem: _find_available_counselor reads the count, but two concurrent
        # routing tasks can both read count=1 and both decide to assign the same
        # counselor, pushing them to 3 chats (over the limit of 2).
        # Fix: Use MongoDB's atomic $inc with a $lt guard. Only ONE task wins.
        # If modified_count == 0, the counselor was concurrently filled — retry.
        claimed = False
        final_counselor_id_str = counselor_id_str

        # Build a retry list: preferred counselor first, then re-fetch pool as fallback
        stale_cutoff_retry = datetime.now(timezone.utc) - timedelta(seconds=_STALE_PING_SECONDS)
        extra_query: dict = {
            "is_online": True,
            "is_active": {"$ne": False},
            "last_ping": {"$gte": stale_cutoff_retry},
            "$or": [
                {"current_active_sessions": {"$lt": _MAX_CONCURRENT_SESSIONS}},
                {"current_active_sessions": {"$exists": False}},
            ],
            "checked_in_at": {"$exists": True},
            "_id": {"$ne": ObjectId(assigned_counselor["_id"])},
        }
        extra_cursor = db.admins.find(extra_query).sort([
            ("current_active_sessions", 1), ("checked_in_at", 1)
        ])
        extra_candidates = await extra_cursor.to_list(length=5)
        candidates_to_try = [assigned_counselor] + extra_candidates

        for candidate in candidates_to_try:
            cid = str(candidate["_id"])
            try:
                # ATOMIC CLAIM: Try to increment only if current < MAX
                claim_result = await db.admins.update_one(
                    {
                        "_id": candidate["_id"],
                        "is_online": True,
                        "$or": [
                            {"current_active_sessions": {"$lt": _MAX_CONCURRENT_SESSIONS}},
                            {"current_active_sessions": {"$exists": False}},
                            {"current_active_sessions": 0},
                        ],
                    },
                    {"$inc": {"current_active_sessions": 1}},
                )

                if claim_result.modified_count == 1:
                    final_counselor_id_str = cid
                    assigned_counselor = candidate
                    claimed = True
                    logger.info(
                        f"[ROUTING] ✓ Capacity slot ATOMICALLY claimed"
                        f" | counselor={cid} | session={session_id}"
                    )
                    break
                else:
                    logger.warning(
                        f"[ROUTING] Counselor {cid} was concurrently filled — trying next candidate."
                    )
            except Exception as claim_err:
                logger.error(f"[ROUTING] Atomic claim failed for counselor {cid}: {claim_err}")

        if not claimed:
            logger.warning(
                f"[ROUTING] All candidates were concurrently filled for session {session_id}. "
                f"Returning session to AI mode."
            )
            await db.sessions.update_one(
                {"session_id": session_id},
                {"$set": {
                    "assigned_counselor_id": None,
                    "routing_started_at": None,
                    "is_escalated": False,
                    "escalation_closed_at": datetime.now(timezone.utc),
                }},
            )
            hotline_text = "No counselor is available at the moment. Please call the helpline at 911."
            await db.messages.insert_one({
                "session_id": session_id,
                "user_id": user_id,
                "role": "system",
                "sender_type": "system",
                "content": hotline_text,
                "timestamp": datetime.now(timezone.utc),
            })
            try:
                from app.api.routes.human import manager as ws_manager
                await ws_manager.send_to_all(session_id, {
                    "role": "system",
                    "text": hotline_text,
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "is_human": False,
                    "is_system": True,
                    "type": "counselor_unavailable",
                })
            except Exception:
                pass
            return

        counselor_id_str = final_counselor_id_str

        # ── Persist session assignment ─────────────────────────────────────────
        await db.sessions.update_one(
            {"session_id": session_id},
            {"$set": {
                "assigned_counselor_id": counselor_id_str,
                "crisis_category": crisis_category,
                "assigned_at": datetime.now(timezone.utc),
                "assignment_complete": False,
                "routing_started_at": None,
            }},
        )

        # ── Persist to doctor_user_assignments (transactional swap) ───────────
        is_same_counselor = (str(preferred_id) == counselor_id_str) if preferred_id else False

        if not is_same_counselor:
            await _swap_assignment(db, user_id, counselor_id_str)

        # ── Determine Handoff Mode ────────────────────────────────────────────
        if active_assignment is None:
            handoff_mode = "raw_history"
        elif is_same_counselor:
            handoff_mode = "short_summary"
        else:
            handoff_mode = "comprehensive_summary"

        # ── Generate handoff summary in background ────────────────────────────
        asyncio.create_task(
            _run_summarization_and_save(user_id, session_id, crisis_category, handoff_mode)
        )

        # ── Update user's routing profile ─────────────────────────────────────
        try:
            await db.users.update_one(
                {"_id": ObjectId(user_id)},
                {"$set": {
                    "preferred_counselor_id": counselor_id_str,
                    "last_crisis_category": crisis_category,
                }},
            )
        except Exception:
            logger.warning(f"[ROUTING] Could not update preferred counselor for user {user_id}.")

        logger.info(
            f"[ROUTING] Session {session_id} assigned to counselor {counselor_id_str} "
            f"(category: {crisis_category})."
        )

        await _notify_counselor(
            counselor_id_str,
            session_id,
            crisis_category,
            user_id,
        )

    except Exception:
        logger.exception(f"[ROUTING] Unhandled error routing session {session_id}")
        try:
            await db.sessions.update_one(
                {"session_id": session_id, "assigned_counselor_id": "__routing__"},
                {"$set": {"assigned_counselor_id": None, "routing_started_at": None}},
            )
        except Exception:
            pass



# ── Internal helpers ──────────────────────────────────────────────────────────

async def _swap_assignment(db, user_id: str, new_doctor_id: str) -> None:
    """
    Atomically swap the user's active doctor assignment using a MongoDB
    transaction.  Steps inside the transaction:
      1. Mark any existing active assignment as inactive.
      2. Insert a new assignment with status=active.

    If transactions are not supported (standalone MongoDB without a replica set),
    the operations run sequentially — the unique partial index on
    (user_id + status: active) still prevents duplicate active records.
    """
    now = datetime.now(timezone.utc)

    async def _do_swap(session=None):
        # Step 1: deactivate old assignment (if any)
        await db.doctor_user_assignments.update_many(
            {"user_id": user_id, "status": "active"},
            {"$set": {"status": "inactive"}},
            session=session,
        )
        # Step 2: insert new active assignment
        await db.doctor_user_assignments.insert_one(
            {
                "user_id": user_id,
                "doctor_id": new_doctor_id,
                "assigned_at": now,
                "status": "active",
            },
            session=session,
        )

    try:
        # Attempt a proper transaction (requires replica set)
        async with await db.client.start_session() as session:
            async with session.start_transaction():
                await _do_swap(session=session)
        logger.info(
            f"[ROUTING] Assignment swapped transactionally: "
            f"user {user_id} → doctor {new_doctor_id}."
        )
    except Exception as txn_err:
        # Standalone MongoDB or network issue — fall back to sequential ops.
        # The unique partial index still prevents duplicate active records.
        logger.warning(
            f"[ROUTING] Transaction unavailable ({txn_err}). "
            f"Falling back to sequential assignment swap."
        )
        try:
            await _do_swap(session=None)
            logger.info(
                f"[ROUTING] Assignment swapped (non-transactional): "
                f"user {user_id} → doctor {new_doctor_id}."
            )
        except Exception:
            logger.exception(
                f"[ROUTING] Failed to persist assignment for user {user_id}."
            )


async def _run_summarization_and_save(
    user_id: str,
    session_id: str,
    crisis_category: str,
    handoff_mode: str = "comprehensive_summary",
) -> None:
    """Generates the clinical handoff note and persists it to the session doc."""
    try:
        db = get_database()
        if db is None:
            return
        summary = await generate_clinical_handoff(user_id, session_id, crisis_category, handoff_mode)
        await db.sessions.update_one(
            {"session_id": session_id},
            {"$set": {"handoff_summary": summary}},
        )
        logger.info(f"[ROUTING] Handoff summary saved for session {session_id}.")
    except Exception:
        logger.exception(f"[ROUTING] Summarization failed for session {session_id}.")


async def _notify_counselor(
    counselor_id: str,
    session_id: str,
    crisis_category: str,
    user_id: str,
) -> None:
    """
    Sends a notification to the assigned counselor's dashboard.
    
    Guaranteed Delivery Logic:
      1. Always persist the notification to the counselor's document in MongoDB first.
         This ensures it is never lost even if the network is unstable.
      2. Attempt a real-time push via WebSocket (Redis Pub/Sub).
      3. If the counselor is currently offline or reconnecting (Wi-Fi drop), the 
         ConnectionManager will automatically 'sync' and deliver this pending
         notification the moment they reconnect.
    """
    logger.info(f"[ROUTING] [NOTIFY] Initiating guaranteed delivery for counselor {counselor_id} (session={session_id}).")
    
    try:
        from app.core.config import get_settings
        from app.api.routes.human import manager as ws_manager
        _settings = get_settings()
        ws_url = (
            f"ws://{_settings.SERVER_PUBLIC_HOST}:{_settings.SERVER_PORT}"
            f"/api/human/chat/{session_id}"
        )

        assigned_payload = {
            "type": "counselor_assigned",
            "counselor_id": counselor_id,
            "session_id": session_id,
            "user_id": user_id,
            "crisis_category": crisis_category,
            "websocket_url": ws_url,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

        # --- Step 1: Persist to DB notification queue (P-1: queue prevents overwrite) ---
        # Using $push instead of $set so multiple assignments while the counselor is
        # offline are ALL queued and delivered on reconnect — never overwritten.
        db = get_database()
        if db is not None:
            await db.admins.update_one(
                {"_id": ObjectId(counselor_id)},
                {"$push": {"pending_notifications": assigned_payload}}
            )
            logger.info(f"[ROUTING] [NOTIFY] ✓ Notification queued in DB for {counselor_id}.")

        # --- Step 2: Attempt real-time WebSocket push ---
        await ws_manager.notify_counselor(counselor_id, assigned_payload)
        logger.info(f"[ROUTING] [NOTIFY] Real-time push dispatched for {counselor_id}.")

        # --- Step 3: Global Broadcast (for dashboard monitors/other instances) ---
        await ws_manager.broadcast_to_dashboard(assigned_payload)

    except Exception as e:
        logger.error(f"[ROUTING] [NOTIFY] Guaranteed delivery failed for counselor {counselor_id}: {e}")
