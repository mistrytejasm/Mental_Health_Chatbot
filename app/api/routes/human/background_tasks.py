"""
Human Handoff — Background Tasks & Watchdogs
──────────────────────────────────────────────
Contains all long-running async background functions used by the human handoff system:

  - _counselor_timeout_watchdog     : Closes a session if no counselor joins within 20 min.
  - inactivity_watchdog             : Global 35-min loop to close stale escalated sessions.
  - _notify_assigned_counselor_user_waiting : Pushes "patient waiting" alert to counselor dashboard.
  - _deliver_handoff_when_ready     : Polls for and delivers GPT-4o handoff summary.
  - _generate_and_save_post_session_summaries : Generates and saves Summary-2 and Summary-3.
  - _user_inactivity_watchdog       : Closes session after 10 min of patient silence.
  - _counselor_heartbeat            : Keeps last_ping fresh every 20 seconds.
  - _counsel_reconnect_grace        : 2-min window for counselor to reconnect after a drop.
"""

import asyncio
from datetime import datetime, timezone
from typing import Optional
from fastapi import WebSocket


from bson import ObjectId

from app.core.database import get_database
from app.core.logger import get_logger
from app.services.db_service import get_expired_escalated_sessions, save_message

from .connection_manager import manager

logger = get_logger(__name__)

# ── Timing constants ──────────────────────────────────────────────────────────

COUNSELOR_JOIN_TIMEOUT_SECONDS = 1200  # 20 min: max wait for counselor to join after user connects
USER_INACTIVITY_TIMEOUT_SECONDS = 600   # 10 min: max user silence before auto-close
GLOBAL_INACTIVITY_TIMEOUT_MINUTES = 35  # 35 min: max session inactivity before watchdog closes it
HEARTBEAT_INTERVAL_SECONDS = 20         # How often to refresh counselor last_ping in DB
RECONNECT_GRACE_PERIOD_SECONDS = 120    # 2 min window for counselor to reconnect after a drop


# ── Counselor Join Timeout ────────────────────────────────────────────────────

async def _counselor_timeout_watchdog(session_id: str, user_id: str) -> None:
    """
    Fires if no counselor joins the chat room within COUNSELOR_JOIN_TIMEOUT_SECONDS.

    On timeout:
      - Attempts to re-route to another counselor.
      - If re-routing fails, sends a fallback message and returns the session to AI mode.
    """
    try:
        await asyncio.sleep(COUNSELOR_JOIN_TIMEOUT_SECONDS)
    except asyncio.CancelledError:
        return
    finally:
        manager.remove_timeout_task(session_id)

    if await manager.human_has_joined(session_id):
        return  # Counselor arrived before timeout — nothing to do

    logger.warning(
        f"[TIMEOUT] No counselor joined room within {COUNSELOR_JOIN_TIMEOUT_SECONDS}s"
        f" | session={session_id} | user_id={user_id}"
    )

    db = get_database()
    session_doc = None

    if db is not None:
        session_doc = await db.sessions.find_one({"session_id": session_id, "is_escalated": True})

    if db is not None and session_doc is not None:
        failed_counselor_id = session_doc.get("assigned_counselor_id")
        await db.sessions.update_one(
            {"session_id": session_id},
            {"$set": {"assigned_counselor_id": None}},
        )

        from app.services.routing_service import route_crisis_session

        crisis_category = session_doc.get("crisis_category", "unknown")
        reroute_consensus: dict = {"category": crisis_category, "is_crisis": True}
        if failed_counselor_id:
            reroute_consensus["_exclude_counselor_id"] = failed_counselor_id

        logger.info(
            f"[TIMEOUT] Re-routing session | session={session_id}"
            f" | excluded_counselor={failed_counselor_id}"
        )
        asyncio.create_task(
            route_crisis_session(user_id=user_id, session_id=session_id, consensus=reroute_consensus)
        )
        return

    # No valid session to re-route — notify user and return to AI
    fallback_message = {
        "role": "system",
        "text": (
            "Our crisis counselors are currently unavailable. "
            "If you are in immediate danger, please call the crisis helpline: "
            "911 (Emergency). "
            "I'll stay with you and continue our conversation."
        ),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "is_human": False,
        "is_system": True,
        "type": "counselor_unavailable",
    }
    await manager.send_to_all(session_id, fallback_message)
    await save_message({
        "session_id": session_id,
        "role": "system",
        "content": fallback_message["text"],
        "user_id": user_id,
    })

    if db is not None:
        await db.sessions.update_one(
            {"session_id": session_id, "is_escalated": True},
            {"$set": {"is_escalated": False, "escalation_closed_at": datetime.now(timezone.utc)}},
        )
    logger.info(f"[TIMEOUT] Session returned to AI mode | session={session_id}")


# ── Global 35-Minute Inactivity Watchdog ──────────────────────────────────────

async def inactivity_watchdog() -> None:
    """
    Runs continuously in the background, checking every 60 seconds for escalated
    sessions that have been inactive for more than GLOBAL_INACTIVITY_TIMEOUT_MINUTES.

    On expiry:
      - Closes the session in the database.
      - Frees the counselor's capacity slot.
      - Sends a system close notice to all participants via WebSocket.
    """
    logger.info(
        f"[WATCHDOG] Global inactivity watchdog started"
        f" | timeout={GLOBAL_INACTIVITY_TIMEOUT_MINUTES}min | check_interval=60s"
    )

    while True:
        try:
            await asyncio.sleep(60)
            expired_sessions = await get_expired_escalated_sessions(
                timeout_minutes=GLOBAL_INACTIVITY_TIMEOUT_MINUTES
            )

            for session_info in expired_sessions:
                session_id = session_info["session_id"]
                user_id = session_info.get("user_id", "unknown")

                logger.warning(
                    f"[WATCHDOG] Session expired due to inactivity"
                    f" | session={session_id} | user_id={user_id}"
                    f" | timeout={GLOBAL_INACTIVITY_TIMEOUT_MINUTES}min"
                )

                db = get_database()
                if db is not None:
                    session_doc = await db.sessions.find_one({"session_id": session_id})
                    assigned_counselor_id = (session_doc or {}).get("assigned_counselor_id")

                    await db.sessions.update_one(
                        {"session_id": session_id},
                        {"$set": {
                            "is_escalated": False,
                            "assigned_counselor_id": None,
                            "escalation_closed_at": datetime.now(timezone.utc),
                        }},
                    )

                    if assigned_counselor_id and assigned_counselor_id != "__routing__":
                        try:
                            await db.admins.update_one(
                                {
                                    "_id": ObjectId(assigned_counselor_id),
                                    "current_active_sessions": {"$gt": 0},
                                },
                                {"$inc": {"current_active_sessions": -1}},
                            )
                            logger.info(
                                f"[WATCHDOG] Freed counselor capacity slot"
                                f" | counselor_id={assigned_counselor_id}"
                            )
                        except Exception as exc:
                            logger.error(
                                f"[WATCHDOG] Failed to free counselor capacity slot"
                                f" | counselor_id={assigned_counselor_id} | error={exc}"
                            )

                close_notice = {
                    "role": "system",
                    "text": "This live session has been closed due to inactivity. You will now be returned to AI support.",
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "is_human": False,
                    "is_system": True,
                    "type": "session_inactive",
                }
                await save_message({
                    "session_id": session_id,
                    "role": "system",
                    "content": close_notice["text"],
                    "user_id": user_id,
                })
                await manager.send_to_all(session_id, close_notice)

        except Exception as exc:
            logger.error(f"[WATCHDOG] Unexpected error in inactivity loop | error={exc}")


# ── User-Waiting Notification ─────────────────────────────────────────────────

async def _notify_assigned_counselor_user_waiting(
    session_id: str,
    user_id: str,
    session_doc: Optional[dict],
) -> None:
    """
    Pushes a real-time "patient is waiting" notification to the assigned counselor's
    dashboard WebSocket when the patient connects to the chat room.

    If the counselor's dashboard socket is unavailable (they may have navigated to
    the chat URL), waits 30 seconds for them to appear in the chat room before
    triggering a re-route.
    """
    from app.core.config import get_settings
    settings = get_settings()

    assigned_counselor_id: Optional[str] = None

    if session_doc:
        assigned_counselor_id = session_doc.get("assigned_counselor_id")
        # Routing may still be in progress — wait briefly and re-query
        if assigned_counselor_id in (None, "__routing__"):
            db = get_database()
            if db is not None:
                for _ in range(6):  # Up to 12 seconds
                    await asyncio.sleep(2)
                    fresh_doc = await db.sessions.find_one({"session_id": session_id})
                    cid = (fresh_doc or {}).get("assigned_counselor_id")
                    if cid and cid != "__routing__":
                        assigned_counselor_id = cid
                        break

    ws_url = (
        f"ws://{settings.SERVER_PUBLIC_HOST}:{settings.SERVER_PORT}"
        f"/api/human/chat/{session_id}"
    )
    notification_payload = {
        "type": "user_waiting_in_room",
        "user_id": user_id,
        "session_id": session_id,
        "websocket_url": ws_url,
        "message": "Your patient has connected and is waiting in the chat room.",
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }

    if assigned_counselor_id and assigned_counselor_id != "__routing__":
        notification_payload["counselor_id"] = assigned_counselor_id
        delivered = await manager.notify_counselor(assigned_counselor_id, notification_payload)

        if delivered:
            logger.info(
                f"[NOTIFY] Patient-waiting push delivered"
                f" | session={session_id} | counselor_id={assigned_counselor_id}"
            )
        else:
            logger.warning(
                f"[NOTIFY] Patient-waiting push failed — counselor dashboard WS not found"
                f" | session={session_id} | counselor_id={assigned_counselor_id}"
                f" | waiting 30s for counselor to connect via chat URL"
            )
            await asyncio.sleep(30)

            if await manager.is_role_in_room(session_id, "human_counselor"):
                logger.info(
                    f"[NOTIFY] Counselor joined chat room within 30s grace window | session={session_id}"
                )
                return

            db = get_database()
            if db is not None:
                fresh_doc = await db.sessions.find_one({"session_id": session_id})
                if (fresh_doc or {}).get("assignment_complete"):
                    logger.info(
                        f"[NOTIFY] assignment_complete=True in DB — counselor connected | session={session_id}"
                    )
                    return
                if not (fresh_doc or {}).get("is_escalated", True):
                    logger.info(
                        f"[NOTIFY] Session no longer escalated — skipping re-route | session={session_id}"
                    )
                    return

            logger.warning(
                f"[NOTIFY] Counselor did not connect within 30s — triggering re-route"
                f" | session={session_id} | excluded_counselor={assigned_counselor_id}"
            )
            from app.services.routing_service import route_crisis_session
            db = get_database()
            if db is not None:
                await db.sessions.update_one(
                    {"session_id": session_id},
                    {"$set": {"assigned_counselor_id": None}},
                )
            reroute_consensus = {
                "category": session_doc.get("crisis_category", "unknown") if session_doc else "unknown",
                "is_crisis": True,
                "_exclude_counselor_id": assigned_counselor_id,
            }
            asyncio.create_task(
                route_crisis_session(
                    user_id=user_id, session_id=session_id, consensus=reroute_consensus
                )
            )
    else:
        logger.info(
            f"[NOTIFY] Skipping patient-waiting push — no counselor assigned yet | session={session_id}"
        )


# ── Handoff Summary Background Delivery ───────────────────────────────────────

async def _deliver_handoff_when_ready(websocket: WebSocket, session_id: str, db) -> None:
    """
    Polls the database every 10 seconds (up to 5 minutes) for the GPT-4o
    handoff summary and delivers it to the counselor's WebSocket once ready.

    Runs as a fire-and-forget background task so it never blocks the WebSocket flow.
    """
    for _ in range(30):  # 30 × 10s = 5 minutes maximum
        await asyncio.sleep(10)
        try:
            session_doc = await db.sessions.find_one({"session_id": session_id})
            handoff_summary = (session_doc or {}).get("handoff_summary")
            if handoff_summary:
                await websocket.send_json({
                    "type": "system_handoff_brief_ready",
                    "content": handoff_summary,
                    "crisis_category": (session_doc or {}).get("crisis_category", "unknown"),
                    "summary_ready": True,
                })
                logger.info(
                    f"[HANDOFF] Delayed handoff summary delivered to counselor | session={session_id}"
                )
                return
        except Exception:
            return  # WebSocket closed or DB error — stop silently


# ── Post-Session Summary Generation ──────────────────────────────────────────

async def _generate_and_save_post_session_summaries(
    session_id: str,
    crisis_category: str,
    handoff_summary: str,
    db,
) -> None:
    """
    Generates and persists the two post-session summaries:
      - Summary-2: Clinical note of the counselor ↔ patient conversation.
      - Summary-3: Merged longitudinal record combining Summary-1 and Summary-2.

    Always called via asyncio.create_task() so it never blocks the close flow.
    """
    from app.services.summarization_service import (
        generate_counselor_session_summary,
        generate_merged_summary,
    )
    try:
        summary_2 = await generate_counselor_session_summary(session_id, crisis_category)
        summary_3 = await generate_merged_summary(
            handoff_summary, summary_2, crisis_category, session_id
        )
        await db.sessions.update_one(
            {"session_id": session_id},
            {"$set": {
                "counselor_session_summary": summary_2,
                "final_merged_summary": summary_3,
            }},
        )
        logger.info(f"[SUMMARY] Post-session summaries saved | session={session_id}")
    except Exception as exc:
        logger.error(
            f"[SUMMARY] Failed to generate post-session summaries"
            f" | session={session_id} | error={exc}"
        )


# ── User Inactivity Watchdog ──────────────────────────────────────────────────

async def _user_inactivity_watchdog(
    session_id: str,
    user_id: str,
    activity_event: asyncio.Event,
) -> None:
    """
    Auto-closes an active counselor session if the patient sends no messages or
    pings for USER_INACTIVITY_TIMEOUT_SECONDS.

    The timer resets each time the calling WebSocket handler sets activity_event.
    """
    while True:
        activity_event.clear()
        try:
            await asyncio.wait_for(
                activity_event.wait(),
                timeout=USER_INACTIVITY_TIMEOUT_SECONDS,
            )
        except asyncio.TimeoutError:
            if not await manager.is_role_in_room(session_id, "human_counselor"):
                return  # No counselor present — let counselor-timeout watchdog handle recovery

            logger.warning(
                f"[TIMEOUT] User inactive for {USER_INACTIVITY_TIMEOUT_SECONDS}s — closing session"
                f" | session={session_id} | user_id={user_id}"
            )

            db = get_database()
            if db is not None:
                session_doc = await db.sessions.find_one({"session_id": session_id})
                if not (session_doc or {}).get("is_escalated", False):
                    return
                assigned_counselor_id = (session_doc or {}).get("assigned_counselor_id")
                await db.sessions.update_one(
                    {"session_id": session_id},
                    {"$set": {
                        "is_escalated": False,
                        "assigned_counselor_id": None,
                        "assignment_complete": False,
                        "escalation_closed_at": datetime.now(timezone.utc),
                    }},
                )
                if assigned_counselor_id and assigned_counselor_id not in (None, "__routing__"):
                    try:
                        await db.admins.update_one(
                            {
                                "_id": ObjectId(assigned_counselor_id),
                                "current_active_sessions": {"$gt": 0},
                            },
                            {"$inc": {"current_active_sessions": -1}},
                        )
                    except Exception:
                        pass

            await manager.send_to_all(session_id, {
                "role": "system",
                "text": "Session closed due to user inactivity.",
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "is_human": False,
                "is_system": True,
                "type": "session_closed",
            })
            return

        except asyncio.CancelledError:
            return


# ── Counselor Heartbeat ────────────────────────────────────────────────────────

async def _counselor_heartbeat(counselor_id: str) -> None:
    """
    Keeps the counselor's last_ping timestamp fresh in the database while they
    are connected. Updates every HEARTBEAT_INTERVAL_SECONDS.

    Cancelled automatically in the WebSocket finally block on disconnect.
    A single DB failure does not kill the loop — it logs a warning and retries.
    """
    db = get_database()
    if db is None:
        return
    try:
        while True:
            await asyncio.sleep(HEARTBEAT_INTERVAL_SECONDS)
            try:
                await db.admins.update_one(
                    {"_id": ObjectId(counselor_id)},
                    {"$set": {"last_ping": datetime.now(timezone.utc)}},
                )
            except Exception as exc:
                logger.warning(
                    f"[HEARTBEAT] DB write failed — retrying next tick"
                    f" | counselor_id={counselor_id} | error={exc}"
                )
    except asyncio.CancelledError:
        pass
    except Exception as exc:
        logger.warning(
            f"[HEARTBEAT] Heartbeat stopped unexpectedly"
            f" | counselor_id={counselor_id} | error={exc}"
        )


# ── Counselor Reconnect Grace Period ─────────────────────────────────────────

async def _counsel_reconnect_grace(
    session_id: str,
    user_id: str,
    counselor_id: str,
) -> None:
    """
    Waits RECONNECT_GRACE_PERIOD_SECONDS for the counselor to reconnect after an
    unexpected disconnect (network blip, tab refresh).

    If the counselor reconnects within the window:
      - Logs the reconnection and exits silently — escalation is preserved.

    If the counselor does not reconnect:
      - Notifies the patient via WebSocket.
      - Marks the session as no longer escalated in the database.
      - Triggers post-session summary generation as a background task.

    Uses is_role_in_room() (live socket check) rather than human_has_joined()
    (which is a one-time flag that stays True even after disconnect).
    """
    await asyncio.sleep(RECONNECT_GRACE_PERIOD_SECONDS)

    if await manager.is_role_in_room(session_id, "human_counselor"):
        logger.info(
            f"[GRACE] RECONNECTED within grace period — escalation preserved"
            f" | session={session_id} | counselor_id={counselor_id}"
        )
        return

    logger.warning(
        f"[GRACE] EXPIRED — counselor did not reconnect; closing escalation"
        f" | session={session_id} | counselor_id={counselor_id}"
        f" | grace_window={RECONNECT_GRACE_PERIOD_SECONDS}s"
    )

    db = get_database()
    if db is None:
        return

    try:
        session_doc = await db.sessions.find_one({"session_id": session_id})

        if not (session_doc or {}).get("is_escalated", False):
            logger.info(
                f"[GRACE] Session already closed by another process — exiting grace"
                f" | session={session_id}"
            )
            return

        crisis_category = (session_doc or {}).get("crisis_category", "unknown")
        handoff_summary = (session_doc or {}).get("handoff_summary", "")

        await db.sessions.update_one(
            {"session_id": session_id},
            {"$set": {
                "is_escalated": False,
                "assigned_counselor_id": None,
                "assignment_complete": False,
                "escalation_closed_at": datetime.now(timezone.utc),
                "updated_at": datetime.now(timezone.utc),
            }},
        )

        await manager.send_to_all(session_id, {
            "role": "system",
            "text": "The counselor has ended this session. You will be connected back to AI support.",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "is_human": False,
            "is_system": True,
            "type": "session_closed",
        })

        asyncio.create_task(
            _generate_and_save_post_session_summaries(
                session_id, crisis_category, handoff_summary, db
            )
        )

    except Exception as exc:
        logger.error(
            f"[GRACE] Failed to close escalation after grace period"
            f" | session={session_id} | error={exc}"
        )
