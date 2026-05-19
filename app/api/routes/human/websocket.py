"""
Human Handoff — WebSocket Route Handlers
──────────────────────────────────────────
Contains the two WebSocket endpoints for the live human handoff system:

  /api/human/escalated/ws         — Counselor dashboard notification channel.
  /api/human/chat/{session_id}    — Bidirectional chat room for user ↔ counselor.

Room model:
  Both parties connect to the same session_id room. Messages are broadcast to
  all members of the room except the sender.

Authentication:
  Both endpoints accept the JWT via ?token=<jwt> query param (standard for
  WebSocket clients that cannot set Authorization headers).
  The chat endpoint also accepts Bearer via the Authorization header.

Structured Log Format:
  [WS EVENT] <event> | session=<id> | user=<name> (id=<id>) | counselor=<name> (id=<id>) | status=... | ...
"""

import asyncio
import json
from datetime import datetime, timezone
from typing import Optional

from bson import ObjectId
from fastapi import APIRouter, HTTPException, WebSocket, WebSocketDisconnect, status

from app.core.auth.jwt_handler import verify_token
from app.core.connection_registry import mark_counselor_connected, mark_counselor_disconnected
from app.core.database import get_database
from app.core.logger import get_logger
from app.models.events import (
    OutgoingMessageEvent,
    SystemNoticeEvent,
    SessionEndedEvent,
    SystemHandoffBriefEvent,
    UserDisconnectedEvent,
    SessionClaimedEvent,
)
from app.services.db_service import save_message

from .background_tasks import (
    COUNSELOR_JOIN_TIMEOUT_SECONDS,
    RECONNECT_GRACE_PERIOD_SECONDS,
    _counsel_reconnect_grace,
    _counselor_heartbeat,
    _counselor_timeout_watchdog,
    _deliver_handoff_when_ready,
    _generate_and_save_post_session_summaries,
    _notify_assigned_counselor_user_waiting,
    _user_inactivity_watchdog,
)
from .connection_manager import manager

logger = get_logger(__name__)

router = APIRouter(prefix="/api/human", tags=["human"])


# ── Dashboard WebSocket ────────────────────────────────────────────────────────

@router.websocket("/escalated/ws")
async def dashboard_notifications_ws(websocket: WebSocket) -> None:
    """
    Counselor dashboard notification WebSocket.

    Authenticates the counselor via ?token=<jwt>. On successful authentication:
      - Marks the counselor as available in the connection_registry.
      - Sets is_online=True and refreshes last_ping in the database.
      - Starts a heartbeat task to keep last_ping fresh.
      - Delivers any pending_notification queued while the counselor was offline.

    Unauthenticated connections (e.g. monitoring scripts) are accepted but do not
    affect counselor availability or routing.

    Structured Log Events:
      [WS DASHBOARD] CONNECTED    — Counselor or anonymous client connected.
      [WS DASHBOARD] DISCONNECTED — Counselor or anonymous client disconnected.
      [WS DASHBOARD] REJECTED     — Token validation failed.
      [WS DASHBOARD] NOTIF DELIVERED — Pending notification delivered on reconnect.
    """
    token = websocket.query_params.get("token", "").strip()
    counselor_id: Optional[str] = None
    counselor_display_name: str = "anonymous"
    client_ip = websocket.client.host if websocket.client else "unknown"

    # ── Token validation (optional — dashboard degrades gracefully) ────────────
    if token:
        try:
            credentials_exc = HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Could not validate credentials",
            )
            token_data = await verify_token(token, credentials_exc)
            counselor_id = token_data.user_id
        except Exception:
            logger.warning(
                f"[WS DASHBOARD] REJECTED | reason=invalid_token | ip={client_ip}"
            )
            # Treat as unauthenticated monitor — do not reject the connection

    await manager.connect_dashboard(websocket, counselor_id=counselor_id)

    db = get_database()
    heartbeat_task: Optional[asyncio.Task] = None

    # ── Authenticated counselor setup ──────────────────────────────────────────
    if counselor_id and db is not None:
        mark_counselor_connected(counselor_id)
        pending_notification = None

        try:
            now_utc = datetime.now(timezone.utc)
            await db.admins.update_one(
                {"_id": ObjectId(counselor_id)},
                {"$set": {"is_online": True, "last_ping": now_utc}},
            )
            # Stamp checked_in_at only on first login — preserves FIFO queue position
            await db.admins.update_one(
                {"_id": ObjectId(counselor_id), "checked_in_at": {"$exists": False}},
                {"$set": {"checked_in_at": now_utc}},
            )

            admin_doc = await db.admins.find_one(
                {"_id": ObjectId(counselor_id)},
                {"first_name": 1, "last_name": 1, "pending_notifications": 1},
            )
            if admin_doc:
                fn = admin_doc.get("first_name", "")
                ln = admin_doc.get("last_name", "")
                counselor_display_name = f"{fn} {ln}".strip() or counselor_id
                # Fix: Use plural array for guaranteed delivery
                pending_notifications = admin_doc.get("pending_notifications", [])

            logger.info(
                f"[WS DASHBOARD] CONNECTED"
                f" | counselor={counselor_display_name} (id={counselor_id})"
                f" | ip={client_ip} | status=online | available_for_routing=True"
            )
        except Exception as exc:
            logger.warning(
                f"[WS DASHBOARD] Could not set online status"
                f" | counselor_id={counselor_id} | error={exc}"
            )

        heartbeat_task = asyncio.create_task(_counselor_heartbeat(counselor_id))

        # Deliver any notifications queued while counselor was offline
        if pending_notifications:
            delivered_any = False
            for notif in pending_notifications:
                try:
                    notif_session_id = notif.get("session_id")
                    should_deliver = True
                    if notif_session_id and db is not None:
                        notif_session = await db.sessions.find_one({"session_id": notif_session_id})
                        if notif_session and not notif_session.get("is_escalated", False):
                            should_deliver = False
                    
                    if should_deliver:
                        await websocket.send_json(notif)
                        delivered_any = True
                except Exception:
                    pass
            
            # Note: We NO LONGER clear the queue here. Notifications persist in the DB 
            # until the counselor actually JOINS the chat room. This prevents 
            # notifications from 'disappearing' if the counselor refreshes.
            if delivered_any:
                logger.info(f"[WS DASHBOARD] Delivered {len(pending_notifications)} pending notification(s) to {counselor_id}")
    else:
        logger.info(
            f"[WS DASHBOARD] CONNECTED | role=anonymous_monitor | ip={client_ip}"
        )

    # ── Message loop ───────────────────────────────────────────────────────────
    try:
        while True:
            data = await websocket.receive_text()
            if data == "ping":
                await websocket.send_text("pong")

    except WebSocketDisconnect:
        manager.disconnect_dashboard(websocket, counselor_id=counselor_id)
        logger.info(
            f"[WS DASHBOARD] DISCONNECTED"
            f" | counselor={counselor_display_name} (id={counselor_id or 'anonymous'})"
            f" | status=last_ping will expire if not reconnected"
        )

    except Exception as exc:
        logger.error(
            f"[WS DASHBOARD] UNHANDLED ERROR"
            f" | counselor={counselor_display_name} (id={counselor_id or 'anonymous'})"
            f" | error={exc}",
            exc_info=True,
        )
        manager.disconnect_dashboard(websocket, counselor_id=counselor_id)
        try:
            await websocket.close(code=1011)
        except Exception as e:
            logger.warning(f"[WS DASHBOARD] Failed to cleanly close socket | error={e}")

    finally:
        if heartbeat_task is not None:
            heartbeat_task.cancel()
        if counselor_id and db is not None:
            mark_counselor_disconnected(counselor_id)
            # Do NOT set is_online=False — a WS drop could be a brief browser refresh or
            # network blip. The counselor is considered online until:
            #   (a) they explicitly check out via REST /checkin-checkout, or
            #   (b) last_ping goes stale (heartbeat was 20s; staleness threshold is 35 min).
            # The connection_registry gates FIFO routing so a disconnected counselor
            # cannot receive new assignments even while their is_online flag is still True.


# ── Human Chat WebSocket ───────────────────────────────────────────────────────

@router.websocket("/chat/{session_id}")
async def human_chat_ws(websocket: WebSocket, session_id: str) -> None:
    """
    Bidirectional real-time chat endpoint for patient ↔ counselor communication.
    Rooms are keyed by session_id.

    Query parameters:
      ?token=<jwt>             JWT token (fallback when Authorization header unavailable)
      ?role=user               Identifies the connecting party as the patient (Android app)
      ?role=human_counselor    Identifies the connecting party as the counselor (web app)
      ?counselor_name=...      Display name shown to the patient in the Android UI

    Structured Log Events:
      [WS CHAT] CONNECTED     — User or counselor joined the room.
      [WS CHAT] SESSION LIVE  — Both user and counselor are connected.
      [WS CHAT] DISCONNECTED  — User or counselor left the room.
      [WS CHAT] MESSAGE       — A message was sent in the session.
      [WS CHAT] REJECTED      — Connection was refused (auth/validation failure).
      [WS CHAT] ERROR         — Unhandled exception during session.
      [GRACE]   RECONNECTED   — Counselor reconnected within the grace period.
      [GRACE]   EXPIRED       — Grace period expired; session returned to AI.
    """
    # ── 1. Token extraction ────────────────────────────────────────────────────
    auth_header = websocket.headers.get("authorization", "")
    token = (
        auth_header.split(" ", 1)[1].strip()
        if auth_header.startswith("Bearer ")
        else websocket.query_params.get("token", "").strip()
    )

    role = websocket.query_params.get("role", "user")
    counselor_name = websocket.query_params.get("counselor_name", "Crisis Support Team")
    client_ip = websocket.client.host if websocket.client else "unknown"

    if not token:
        logger.warning(
            f"[WS CHAT] REJECTED | session={session_id} | role={role}"
            f" | ip={client_ip} | reason=no_token"
        )
        await websocket.close(code=status.WS_1008_POLICY_VIOLATION)
        return

    # ── 2. Token verification (before WebSocket handshake) ─────────────────────
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        token_data = await verify_token(token, credentials_exception)
    except HTTPException:
        logger.warning(
            f"[WS CHAT] REJECTED | session={session_id} | role={role}"
            f" | ip={client_ip} | reason=invalid_token"
        )
        await websocket.close(code=status.WS_1008_POLICY_VIOLATION)
        return

    authenticated_user_id = token_data.user_id

    # ── 3. Session document lookup ─────────────────────────────────────────────
    db = get_database()
    session_doc: Optional[dict] = None
    user_id: str = authenticated_user_id  # overwritten from DB below

    if db is not None:
        try:
            session_doc = await db.sessions.find_one({"session_id": session_id})
        except Exception as exc:
            logger.error(
                f"[WS CHAT] DB error loading session"
                f" | session={session_id} | error={exc}"
            )
            await websocket.close(code=1011, reason="Internal server error")
            return

    if session_doc is None:
        logger.warning(
            f"[WS CHAT] REJECTED | session={session_id} | role={role}"
            f" | reason=session_not_found"
        )
        await websocket.close(code=4004)
        return

    user_id = session_doc.get("user_id", authenticated_user_id)

    # ── 4. Identity validation ─────────────────────────────────────────────────
    if role == "user" and authenticated_user_id != user_id:
        logger.warning(
            f"[WS CHAT] REJECTED | session={session_id} | role=user"
            f" | auth_id={authenticated_user_id} | session_owner={user_id}"
            f" | reason=identity_mismatch"
        )
        await websocket.close(code=status.WS_1008_POLICY_VIOLATION)
        return

    # ── 5. Escalation guard (user path) ───────────────────────────────────────
    if role == "user":
        if not session_doc.get("is_escalated"):
            if manager.is_session_ended(session_id):
                await websocket.close(code=4001)
                return
            manager.mark_session_ended(session_id)
            await websocket.accept()
            event = SessionEndedEvent(text="This session has already ended. You will be returned to AI support.")
            await websocket.send_json(event.model_dump())
            await websocket.close(code=4001)
            logger.warning(
                f"[WS CHAT] REJECTED | session={session_id} | role=user"
                f" | reason=no_active_escalation"
            )
            return

    # ── 6. Counselor assignment validation ────────────────────────────────────
    if role == "human_counselor":
        assigned_counselor_id = session_doc.get("assigned_counselor_id")
        if (
            assigned_counselor_id
            and assigned_counselor_id not in (None, "__routing__")
            and assigned_counselor_id != authenticated_user_id
        ):
            logger.warning(
                f"[WS CHAT] REJECTED | session={session_id} | role=human_counselor"
                f" | connecting_counselor={authenticated_user_id}"
                f" | assigned_to={assigned_counselor_id}"
                f" | reason=not_assigned"
            )
            await websocket.close(code=4003)
            return

    # ── 7. Accept and register the connection ─────────────────────────────────
    await manager.connect(session_id, websocket)
    await manager.register_ws_role(websocket, session_id, role)

    # Resolve display names for structured logging
    user_display_name = user_id
    counselor_display_name = counselor_name

    if db is not None:
        try:
            if role == "user":
                user_doc = await db.users.find_one({"_id": ObjectId(user_id)}, {"first_name": 1})
                if user_doc:
                    user_display_name = user_doc.get("first_name", user_id)
            else:
                admin_doc = await db.admins.find_one(
                    {"_id": ObjectId(authenticated_user_id)},
                    {"first_name": 1, "last_name": 1},
                )
                if admin_doc:
                    fn = admin_doc.get("first_name", "")
                    ln = admin_doc.get("last_name", "")
                    counselor_display_name = f"{fn} {ln}".strip() or counselor_name
        except Exception:
            pass  # Display name resolution is non-critical

    if role == "human_counselor":
        logger.info(
            f"[WS CHAT] CONNECTED | role=counselor"
            f" | session={session_id}"
            f" | counselor={counselor_display_name} (id={authenticated_user_id})"
            f" | active_connections={len(manager.rooms.get(session_id, []))}"
        )
    else:
        await manager.mark_user_joined(session_id)
        logger.info(
            f"[WS CHAT] CONNECTED | role=user"
            f" | session={session_id}"
            f" | user={user_display_name} (id={authenticated_user_id})"
            f" | active_connections={len(manager.rooms.get(session_id, []))}"
        )

    # ── 8. User: start watchdogs and notify counselor ─────────────────────────
    # Global activity event shared between all connections in this room
    user_activity_event = manager.get_activity_event(session_id)
    user_inactivity_task: Optional[asyncio.Task] = None

    if role == "user":
        if session_id not in manager.timeout_tasks:
            timeout_task = asyncio.create_task(
                _counselor_timeout_watchdog(session_id, user_id)
            )
            manager.start_timeout_task(session_id, timeout_task)
            logger.info(
                f"[WS CHAT] Counselor join timeout watchdog started"
                f" | session={session_id} | timeout={COUNSELOR_JOIN_TIMEOUT_SECONDS}s"
            )

        notify_task = asyncio.create_task(
            _notify_assigned_counselor_user_waiting(session_id, user_id, session_doc)
        )
        manager.start_notify_task(session_id, notify_task)

        user_inactivity_task = asyncio.create_task(
            _user_inactivity_watchdog(session_id, user_id, user_activity_event)
        )

    heartbeat_task: Optional[asyncio.Task] = None

    # ── 9. Counselor: update presence and send handoff brief ──────────────────
    if role == "human_counselor":
        mark_counselor_connected(authenticated_user_id)
        active_counselor_tabs = await manager.get_role_count(session_id, "human_counselor")
        has_joined_before = await manager.human_has_joined(session_id)
        should_send_join_notice = not has_joined_before

        if db is not None:
            # Update counselor presence in DB
            try:
                presence_update: dict = {
                    "$set": {"is_online": True, "last_ping": datetime.now(timezone.utc)}
                }
                # NOTE: current_active_sessions is now incremented atomically in
                # routing_service.py at assignment time (single $inc with $lt guard).
                # Do NOT increment here — that would double-count the capacity slot.

                await db.admins.update_one(
                    {"_id": ObjectId(authenticated_user_id)},
                    presence_update,
                )
            except Exception as exc:
                logger.warning(
                    f"[WS CHAT] Could not update counselor presence"
                    f" | counselor_id={authenticated_user_id} | error={exc}"
                )

            # Write confirmed counselor assignment to session document
            try:
                await db.sessions.update_one(
                    {"session_id": session_id},
                    {"$set": {
                        "assigned_counselor_id": authenticated_user_id,
                        "assignment_complete": True,
                    }},
                )
                logger.info(
                    f"[WS CHAT] Counselor assignment confirmed in DB"
                    f" | session={session_id}"
                    f" | counselor={counselor_display_name} (id={authenticated_user_id})"
                )
            except Exception as exc:
                logger.warning(
                    f"[WS CHAT] Could not write counselor assignment"
                    f" | session={session_id} | error={exc}"
                )

            # Stamp accepted_at on the doctor_user_assignments record
            try:
                result = await db.doctor_user_assignments.update_one(
                    {"user_id": user_id, "status": "active"},
                    {"$set": {"accepted_at": datetime.now(timezone.utc)}},
                )
                if result.matched_count:
                    logger.info(
                        f"[WS CHAT] accepted_at stamped on assignment"
                        f" | user_id={user_id}"
                    )
                
                # Clear this specific notification from the counselor's queue now that they have joined
                await db.admins.update_one(
                    {"_id": ObjectId(authenticated_user_id)},
                    {"$pull": {"pending_notifications": {"session_id": session_id}}}
                )
                logger.info(f"[WS CHAT] Cleared notification for session {session_id} from counselor queue.")
            except Exception as exc:
                logger.warning(
                    f"[WS CHAT] Could not update assignment/notification status | user_id={user_id} | error={exc}"
                )

        heartbeat_task = asyncio.create_task(_counselor_heartbeat(authenticated_user_id))
        await manager.mark_human_joined(session_id)
        manager.cancel_timeout_task(session_id)
        manager.cancel_notify_task(session_id)

        # Log the clear user↔counselor mapping now that both sides are identified
        logger.info(
            f"[WS CHAT] SESSION LIVE"
            f" | session={session_id}"
            f" | user={user_display_name} (id={user_id})"
            f" | counselor={counselor_display_name} (id={authenticated_user_id})"
            f" | mapping=User {user_display_name} ↔ Counselor {counselor_display_name}"
            f" | active_connections={len(manager.rooms.get(session_id, []))}"
        )

        # Notify all dashboards that this session has been claimed
        claim_event = SessionClaimedEvent(
            session_id=session_id,
            counselor_id=authenticated_user_id
        )
        asyncio.create_task(manager.broadcast_to_dashboard(claim_event.model_dump()))

        # Send handoff brief immediately; start background task for the real GPT-4o summary
        if db is not None and session_doc is not None:
            handoff_summary = session_doc.get("handoff_summary")
            handoff_event = SystemHandoffBriefEvent(
                content=handoff_summary or "Clinical summary is being generated — you will receive it shortly.",
                crisis_category=session_doc.get("crisis_category", "unknown"),
                summary_ready=bool(handoff_summary),
            )
            # --- Delivered Handoff Brief (Multi-tab Safe) ---
            try:
                await websocket.send_json(handoff_event.model_dump())
                logger.info(
                    f"[WS CHAT] Handoff brief delivered immediately"
                    f" | session={session_id} | counselor={counselor_display_name} (id={authenticated_user_id})"
                )
            except Exception:
                # Socket might have closed during serialization or sending
                logger.warning(f"[WS CHAT] Failed to deliver initial handoff brief (counselor disconnected) | session={session_id}")

            if not handoff_summary:
                logger.info(
                    f"[WS CHAT] Placeholder handoff sent; background delivery task started"
                    f" | session={session_id}"
                )
                asyncio.create_task(_deliver_handoff_when_ready(websocket, session_id, db))

        # Broadcast join notice to the patient
        # Fix: Ensure this only happens once per session to avoid duplicate notices
        # if the counselor opens multiple tabs.
        if should_send_join_notice:
            join_event = SystemNoticeEvent(
                role="human_counselor",
                counselor_name=counselor_display_name,
                text=f"{counselor_display_name} has joined the chat. You're not alone.",
                is_human=True,
                is_system=True,
            )
            await manager.broadcast(session_id, join_event.model_dump(), websocket)
            logger.info(f"[WS CHAT] First counselor join notice broadcast | session={session_id}")

    # ── 10. Message loop ───────────────────────────────────────────────────────
    # MEDIUM-7: Server-initiated ping every 30s to keep counselor WebSocket alive.
    # Only for human_counselor (web browser). Android user app sends its own pings.
    async def _keepalive_ping():
        if role != "human_counselor":
            return
        try:
            while True:
                await asyncio.sleep(30)
                try:
                    await asyncio.wait_for(
                        websocket.send_text(json.dumps({"type": "ping"})),
                        timeout=5.0,
                    )
                except Exception:
                    break
        except asyncio.CancelledError:
            pass

    keepalive_task: Optional[asyncio.Task] = asyncio.create_task(_keepalive_ping())

    try:
        while True:
            raw = await websocket.receive_text()

            # Handle plain-text pings from legacy clients
            try:
                data = json.loads(raw)
            except json.JSONDecodeError:
                if raw.strip().lower() == "ping":
                    await websocket.send_text("pong")
                    if user_activity_event is not None:
                        user_activity_event.set()
                    continue
                await websocket.send_text(json.dumps({"error": "Invalid JSON"}))
                continue

            if data.get("type") == "ping":
                await websocket.send_text(json.dumps({"type": "pong"}))
                manager.mark_activity(session_id)
                continue

            message_text = data.get("text", "").strip()
            if not message_text:
                continue

            manager.mark_activity(session_id)

            is_counselor_message = role == "human_counselor"
            sender_label = "counselor" if is_counselor_message else "user"
            preview = message_text[:80] + ("..." if len(message_text) > 80 else "")

            logger.info(
                f"[WS CHAT] MESSAGE"
                f" | session={session_id}"
                f" | from={sender_label}"
                f" | user={user_display_name} (id={user_id})"
                f" | counselor={counselor_display_name} (id={authenticated_user_id if is_counselor_message else 'N/A'})"
                f" | len={len(message_text)}"
                f' | preview="{preview}"'
            )

            outbound_event = OutgoingMessageEvent(
                role=role,
                counselor_name=counselor_display_name if is_counselor_message else None,
                text=message_text,
                is_human=is_counselor_message,
                done=True,
            )

            await save_message({
                "session_id": session_id,
                "role": role,
                "content": message_text,
                "user_id": user_id,
                "is_human_message": is_counselor_message,
            })

            await manager.broadcast(session_id, outbound_event.model_dump(), sender_ws=websocket)

    except WebSocketDisconnect:
        await manager.disconnect(session_id, websocket)

        if role == "human_counselor":
            logger.warning(
                f"[WS CHAT] DISCONNECTED | role=counselor"
                f" | session={session_id}"
                f" | counselor={counselor_display_name} (id={authenticated_user_id})"
                f" | grace_period={RECONNECT_GRACE_PERIOD_SECONDS}s started"
            )
            # Grace period: counselor has RECONNECT_GRACE_PERIOD_SECONDS to reconnect
            # before the session is closed and the patient is notified.
        else:
            logger.warning(
                f"[WS CHAT] DISCONNECTED | role=user"
                f" | session={session_id}"
                f" | user={user_display_name} (id={authenticated_user_id})"
            )
            if await manager.is_role_in_room(session_id, "human_counselor"):
                disconnect_event = UserDisconnectedEvent()
                await manager.broadcast(session_id, disconnect_event.model_dump(), websocket)

    except Exception as exc:
        logger.error(
            f"[WS CHAT] ERROR | session={session_id} | role={role}"
            f" | user={user_display_name} (id={authenticated_user_id}) | error={exc}",
            exc_info=True,
        )
        manager.disconnect(session_id, websocket)
        try:
            await websocket.close(code=1011)
        except Exception as e:
            logger.warning(f"[WS CHAT] Failed to cleanly close socket | session={session_id} | error={e}")

    finally:
        if heartbeat_task is not None:
            heartbeat_task.cancel()
        if user_inactivity_task is not None:
            user_inactivity_task.cancel()
        if keepalive_task is not None:
            keepalive_task.cancel()  # MEDIUM-7: always cancel the keepalive ping

        if role == "human_counselor":
            mark_counselor_disconnected(authenticated_user_id)
            # Reference counted role removal (multi-tab safe)
            await manager.unregister_ws_role(websocket, session_id)
            still_in_room = await manager.is_role_in_room(session_id, "human_counselor")

            if db is not None:
                # Start grace period — gives counselor time to reconnect before closing.
                # Capacity slot will be released IF the grace period expires.
                asyncio.create_task(
                    _counsel_reconnect_grace(session_id, user_id, authenticated_user_id)
                )
