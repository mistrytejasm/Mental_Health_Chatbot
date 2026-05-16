"""
Human Handoff — REST API Route Handlers
─────────────────────────────────────────
Provides REST endpoints for the human handoff system:

  GET  /api/human/escalated                   — List escalated sessions for the counselor.
  GET  /api/human/escalated/{user_id}/messages — Fetch all messages for an escalated session.
  POST /api/human/escalated/{user_id}/close   — End a counselor session, return user to AI.
  POST /api/human/checkin-checkout            — Toggle counselor availability.
  GET  /api/human/checkin-status             — Return current counselor check-in state.
  GET  /api/human/ws-status/{session_id}     — Return live WebSocket connection status.
"""

import asyncio
from datetime import datetime, timezone
from typing import Optional

from bson import ObjectId
from fastapi import APIRouter, Depends, HTTPException, status

from app.api.schemas.request import CheckinCheckoutRequest
from app.api.schemas.response import (
    ChatHistoryResponse,
    ChatMessageResponse,
    CheckinCheckoutResponse,
    CounselorStatusResponse,
    EscalatedSessionListResponse,
    EscalatedSessionResponse,
    WebSocketStatusResponse,
)
from app.core.auth.oauth2 import get_current_user
from app.core.connection_registry import is_counselor_connected, mark_counselor_connected, mark_counselor_disconnected
from app.core.database import get_database
from app.core.logger import get_logger
from app.services.db_service import _ensure_utc, get_user_messages

from .background_tasks import _generate_and_save_post_session_summaries
from .connection_manager import manager

logger = get_logger(__name__)

router = APIRouter(prefix="/api/human", tags=["human"])


# ── GET /escalated ─────────────────────────────────────────────────────────────

@router.get("/escalated", response_model=EscalatedSessionListResponse)
async def list_escalated_sessions(
    user_id: Optional[str] = None,
    current_provider=Depends(get_current_user),
):
    """
    Returns the list of active escalated sessions visible to the requesting counselor.

    Visibility rules (counselor sees a session if ANY one of the following is true):
      Case A — Returning patient: this counselor has a prior session history with the user,
               the user's previous counselor is the requesting counselor, AND the counselor
               is currently online and has an active dashboard WebSocket.
      Case B — Directly assigned: the routing engine assigned this session to this counselor.
      Case C — Routing pending: the __routing__ lock is held, counselor is free
               (session surfaces to all free counselors so it is not invisible during routing).

    Side effects on every call:
      - Marks the counselor as online with a fresh last_ping (auto-checkin).
      - Delivers any pending_notification that was queued while offline.
    """
    if current_provider.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Only counselors can access this resource.")

    db = get_database()
    if db is None:
        raise HTTPException(status_code=500, detail="Database connection failed.")

    doctor_id = str(current_provider.get("user_id") or current_provider.get("_id"))
    counselor_doc = await db.admins.find_one({"_id": ObjectId(doctor_id)})

    # Auto-checkin: stamp online + checked_in_at if they don't have one
    now_utc = datetime.now(timezone.utc)
    if not counselor_doc or not counselor_doc.get("is_online", False) or "checked_in_at" not in counselor_doc:
        await db.admins.update_one(
            {"_id": ObjectId(doctor_id)},
            {"$set": {"is_online": True, "checked_in_at": now_utc, "last_ping": now_utc}},
        )
        counselor_doc = await db.admins.find_one({"_id": ObjectId(doctor_id)})

    # Mark counselor as connected in memory so REST-polling counselors bypass WS checks
    from app.core.connection_registry import force_counselor_connected
    force_counselor_connected(doctor_id)
    update_fields: dict = {"last_ping": now_utc}

    # Deliver any pending_notification queued while counselor was offline
    pending_notif = (counselor_doc or {}).get("pending_notification")
    if pending_notif:
        notif_session_id = pending_notif.get("session_id")
        should_deliver = True
        if notif_session_id:
            try:
                notif_session_doc = await db.sessions.find_one({"session_id": notif_session_id})
                if notif_session_doc and not notif_session_doc.get("is_escalated", False):
                    should_deliver = False  # Session already closed — discard stale notification
            except Exception:
                pass
        if should_deliver:
            delivered = await manager.notify_counselor(doctor_id, pending_notif)
            if delivered:
                logger.info(
                    f"[NOTIFY] Deferred pending_notification delivered via WS"
                    f" | counselor_id={doctor_id} | session={notif_session_id}"
                )
            else:
                logger.info(
                    f"[NOTIFY] pending_notification will surface via session list (Case B)"
                    f" | counselor_id={doctor_id} | session={notif_session_id}"
                )
        update_fields["pending_notification"] = None

    await db.admins.update_one(
        {"_id": ObjectId(doctor_id)},
        {
            "$set": {k: v for k, v in update_fields.items() if k != "pending_notification"},
            **({"$unset": {"pending_notification": ""}} if pending_notif else {}),
        },
    )

    counselor_is_free = (counselor_doc or {}).get("current_active_sessions", 0) == 0
    counselor_ws_active = is_counselor_connected(doctor_id)

    query: dict = {"is_escalated": True}
    if user_id:
        query["user_id"] = user_id

    docs = await db.sessions.find(query).sort("escalated_at", -1).to_list(length=None)

    # Batch-fetch user names
    user_ids = list({doc.get("user_id") for doc in docs if doc.get("user_id")})
    user_names: dict[str, tuple[str, Optional[str]]] = {}
    if user_ids:
        valid_oids = []
        for uid in user_ids:
            try:
                valid_oids.append(ObjectId(uid))
            except Exception:
                pass
        if valid_oids:
            try:
                user_docs = await db.users.find(
                    {"_id": {"$in": valid_oids}},
                    {"_id": 1, "first_name": 1, "last_name": 1, "full_name": 1},
                ).to_list(length=None)
                for u in user_docs:
                    uid_str = str(u["_id"])
                    fn = u.get("first_name") or (u.get("full_name") or "Unknown").split()[0]
                    ln = u.get("last_name")
                    user_names[uid_str] = (fn, ln)
            except Exception:
                pass

    # Batch-fetch counselor names
    counselor_ids = list({doc.get("assigned_counselor_id") for doc in docs if doc.get("assigned_counselor_id")})
    counselor_names: dict[str, tuple[str, Optional[str]]] = {}
    if counselor_ids:
        valid_coids = []
        for cid in counselor_ids:
            try:
                valid_coids.append(ObjectId(cid))
            except Exception:
                pass
        if valid_coids:
            try:
                c_docs = await db.admins.find(
                    {"_id": {"$in": valid_coids}},
                    {"_id": 1, "first_name": 1, "last_name": 1},
                ).to_list(length=None)
                for c in c_docs:
                    cid_str = str(c["_id"])
                    counselor_names[cid_str] = (c.get("first_name", "Counselor"), c.get("last_name"))
            except Exception:
                pass

    # Resolve doctor_id per session for returning-patient routing (Case A)
    doctor_assignment_map: dict[str, str] = {}
    if user_ids:
        try:
            assign_docs = await db.doctor_user_assignments.find(
                {"user_id": {"$in": user_ids}, "status": "active"},
                {"user_id": 1, "doctor_id": 1},
            ).to_list(length=None)

            raw_map: dict[str, str] = {}
            doctor_ids_to_check: list = []
            for a in assign_docs:
                did = str(a["doctor_id"])
                raw_map[a["user_id"]] = did
                doctor_ids_to_check.append(did)

            users_with_history: set[str] = set()
            if raw_map:
                history_docs = await db.sessions.find(
                    {
                        "user_id": {"$in": list(raw_map.keys())},
                        "escalation_closed_at": {"$exists": True, "$ne": None},
                    },
                    {"user_id": 1},
                ).to_list(length=None)
                users_with_history = {d["user_id"] for d in history_docs}

            online_doctor_ids: set[str] = set()
            if doctor_ids_to_check:
                valid_doids = []
                for did in doctor_ids_to_check:
                    try:
                        valid_doids.append(ObjectId(did))
                    except Exception:
                        pass
                if valid_doids:
                    online_docs = await db.admins.find(
                        {"_id": {"$in": valid_doids}, "is_online": True},
                        {"_id": 1},
                    ).to_list(length=None)
                    online_doctor_ids = {str(d["_id"]) for d in online_docs}

            for uid, did in raw_map.items():
                if uid in users_with_history and did in online_doctor_ids:
                    doctor_assignment_map[uid] = did
        except Exception:
            pass

    sessions = []
    for doc in docs:
        uid = doc.get("user_id", "")
        assigned_cid = doc.get("assigned_counselor_id")
        previous_doctor_id = doctor_assignment_map.get(uid, False)

        is_returning_patient_mine = (
            previous_doctor_id == doctor_id
            and counselor_is_free
            and counselor_ws_active
        )
        is_routing_assigned = assigned_cid == doctor_id
        is_routing_pending = assigned_cid == "__routing__" and counselor_is_free

        if not is_returning_patient_mine and not is_routing_assigned and not is_routing_pending:
            continue

        fn, ln = user_names.get(uid, ("Unknown", None))
        cfn, cln = counselor_names.get(assigned_cid, (None, None)) if assigned_cid else (None, None)
        sessions.append({
            "session_id": doc.get("session_id"),
            "user_id": uid,
            "first_name": fn,
            "last_name": ln,
            "is_active": doc.get("is_active", True),
            "is_escalated": doc.get("is_escalated", True),
            "lethality_alert": doc.get("lethality_alert", False),
            "created_at": doc.get("created_at"),
            "updated_at": doc.get("updated_at"),
            "escalated_at": doc.get("escalated_at"),
            "assigned_counselor_id": assigned_cid,
            "counselor_first_name": cfn,
            "counselor_last_name": cln,
            "doctor_id": doctor_assignment_map.get(uid, False),
        })

    formatted = [EscalatedSessionResponse(**s) for s in sessions]
    return EscalatedSessionListResponse(status="success", total=len(formatted), sessions=formatted)


# ── GET /escalated/{user_id}/messages ─────────────────────────────────────────

@router.get("/escalated/{user_id}/messages", response_model=ChatHistoryResponse)
async def get_escalated_session_messages(
    user_id: str,
    current_provider=Depends(get_current_user),
):
    """Returns all chat messages for the most recent escalated session of a given user."""
    if current_provider.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Only counselors can access this resource.")
    if not user_id.strip():
        raise HTTPException(status_code=400, detail="User ID is required.")

    db = get_database()
    if db is None:
        raise HTTPException(status_code=500, detail="Database connection failed.")

    # Fetch cross-session history for the user (AI + Counselor messages)
    messages = await get_user_messages(user_id, limit=500)
    
    formatted = [ChatMessageResponse(**msg) for msg in messages]
    return ChatHistoryResponse(
        status="success",
        user_id=user_id,
        total_messages=len(formatted),
        messages=formatted,
    )


# ── POST /escalated/{user_id}/close ───────────────────────────────────────────

@router.post("/escalated/{user_id}/close")
async def close_escalated_session(
    user_id: str,
    current_provider=Depends(get_current_user),
):
    """
    Ends an active counselor session for a user.

    Side effects:
      - Marks the session as no longer escalated.
      - Frees the counselor's capacity slot.
      - Sends a close notice to all WebSocket participants in the room.
      - Triggers post-session summary generation as a background task.

    Authorization:
      Only the assigned counselor may close their own session.
    """
    if current_provider.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Only counselors can access this resource.")
    if not user_id.strip():
        raise HTTPException(status_code=400, detail="User ID is required.")

    db = get_database()
    if db is None:
        raise HTTPException(status_code=500, detail="Database connection failed.")

    session_doc = await db.sessions.find_one(
        {"user_id": user_id, "is_escalated": True},
        sort=[("escalated_at", -1)],
    )
    if not session_doc:
        raise HTTPException(status_code=404, detail="No active escalated session found.")

    # Ownership check — only the assigned counselor may close their own session
    assigned_id = session_doc.get("assigned_counselor_id")
    provider_id = str(current_provider.get("user_id") or current_provider.get("_id") or "")
    if assigned_id and assigned_id != "__routing__" and provider_id != str(assigned_id):
        raise HTTPException(
            status_code=403,
            detail="Forbidden: You are not authorized to close a session assigned to another counselor.",
        )

    closing_session_id = session_doc.get("session_id")
    closing_crisis_category = session_doc.get("crisis_category", "unknown")
    closing_handoff_summary = session_doc.get("handoff_summary", "")
    closing_counselor_id = session_doc.get("assigned_counselor_id")

    try:
        await db.sessions.update_many(
            {"user_id": user_id, "is_escalated": True},
            {"$set": {
                "is_escalated": False,
                "assigned_counselor_id": None,
                "assignment_complete": False,
                "escalation_closed_at": datetime.now(timezone.utc),
                "updated_at": datetime.now(timezone.utc),
            }},
        )
    except Exception as exc:
        logger.error(f"Failed to close escalation | user_id={user_id} | error={exc}")
        raise HTTPException(status_code=500, detail="Failed to close escalation.")

    # Free the counselor's capacity slot so they become available for the next escalation
    if closing_counselor_id and closing_counselor_id not in (None, "__routing__"):
        try:
            await db.admins.update_one(
                {"_id": ObjectId(closing_counselor_id), "current_active_sessions": {"$gt": 0}},
                {"$inc": {"current_active_sessions": -1}},
            )
            logger.info(
                f"[CLOSE] Freed counselor capacity slot | counselor_id={closing_counselor_id}"
            )
        except Exception as exc:
            logger.warning(
                f"[CLOSE] Could not free counselor capacity | counselor_id={closing_counselor_id} | error={exc}"
            )

    room_key = closing_session_id or user_id
    manager.cancel_timeout_task(room_key)

    if closing_session_id:
        asyncio.create_task(
            _generate_and_save_post_session_summaries(
                closing_session_id, closing_crisis_category, closing_handoff_summary, db
            )
        )

    close_notice = {
        "role": "system",
        "text": "The counselor has ended this session. You will be connected back to AI support.",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "is_human": False,
        "is_system": True,
        "type": "session_closed",
    }
    await manager.send_to_all(room_key, close_notice)

    return {
        "status": "success",
        "user_id": user_id,
        "message": "Escalation closed. User will return to AI on next message.",
    }


# ── POST /checkin-checkout ────────────────────────────────────────────────────

@router.post("/checkin-checkout", response_model=CheckinCheckoutResponse)
async def checkin_checkout(
    request: CheckinCheckoutRequest,
    current_user=Depends(get_current_user),
):
    """
    Explicit REST check-in / check-out for counselors.

    Allows mobile/REST-only dashboard clients to toggle availability without
    relying on the WebSocket lifecycle.
      is_online: true  → check-in (mark available for routing)
      is_online: false → check-out (mark unavailable; blocked if sessions are active)
    """
    if current_user.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Only counselors can check in/out.")

    doctor_id = str(current_user.get("user_id") or current_user.get("_id"))
    db = get_database()
    if db is None:
        raise HTTPException(status_code=500, detail="Database connection failed.")

    if request.is_online:
        await db.admins.update_one(
            {"_id": ObjectId(doctor_id)},
            {"$set": {
                "is_online": True,
                "last_ping": datetime.now(timezone.utc),
                "checked_in_at": datetime.now(timezone.utc),
            }},
        )
        mark_counselor_connected(doctor_id)
        logger.info(f"[CHECKIN] Counselor checked IN via REST | counselor_id={doctor_id}")
        return {"status": "success", "message": "Check-in successful"}
    else:
        active_count = await db.sessions.count_documents({
            "assigned_counselor_id": doctor_id,
            "is_escalated": True,
        })
        if active_count > 0:
            raise HTTPException(
                status_code=400,
                detail=f"You have {active_count} active session(s). Please close them before checking out.",
            )
        await db.admins.update_one(
            {"_id": ObjectId(doctor_id)},
            {"$set": {"is_online": False}, "$unset": {"checked_in_at": ""}},
        )
        mark_counselor_disconnected(doctor_id)
        logger.info(f"[CHECKOUT] Counselor checked OUT via REST | counselor_id={doctor_id}")
        return {"status": "success", "message": "Check-out successful"}


# ── GET /checkin-status ───────────────────────────────────────────────────────

@router.get("/checkin-status", response_model=CounselorStatusResponse)
async def get_checkin_status(current_user=Depends(get_current_user)):
    """Returns the current check-in status of the authenticated counselor."""
    if current_user.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Only counselors can view check-in status.")

    doctor_id = str(current_user.get("user_id") or current_user.get("_id"))
    db = get_database()
    if db is None:
        raise HTTPException(status_code=500, detail="Database connection failed.")

    counselor_doc = await db.admins.find_one({"_id": ObjectId(doctor_id)})
    if not counselor_doc:
        raise HTTPException(status_code=404, detail="Counselor not found.")

    return {"status": "success", "is_checked": counselor_doc.get("is_online", False)}


# ── GET /ws-status/{session_id} ───────────────────────────────────────────────

@router.get("/ws-status/{session_id}", response_model=WebSocketStatusResponse)
async def get_ws_status(session_id: str, current_user=Depends(get_current_user)):
    """
    Returns the live WebSocket connection state for a given session room.

    Response fields:
      is_user_connected      — True if the patient has an active WebSocket in this room.
      is_counselor_connected — True if a counselor has an active WebSocket in this room.
      is_socket_connected    — True only when both are simultaneously connected.
    """
    is_user = manager.is_role_in_room(session_id, "user")
    is_counselor = manager.is_role_in_room(session_id, "human_counselor")
    return WebSocketStatusResponse(
        status="success",
        session_id=session_id,
        is_user_connected=is_user,
        is_counselor_connected=is_counselor,
        is_socket_connected=is_user and is_counselor,
    )
