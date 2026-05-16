"""
WebSocket Connection Manager
─────────────────────────────
In-memory manager for all active WebSocket connections in the human handoff system.

Room model:
  - Each session_id maps to a list of active WebSocket objects (self.rooms).
  - User and counselor sockets coexist in the same room.
  - Messages are broadcast to all room members except the sender.

Dashboard model:
  - All connected counselor dashboard WebSockets are tracked in self.dashboard_clients.
  - Targeted pushes to a specific counselor use self.counselor_ws[counselor_id].
"""

import json
from typing import Optional

from fastapi import WebSocket

from app.core.logger import get_logger

logger = get_logger(__name__)


class ConnectionManager:
    """
    Manages all active WebSocket connections for the human handoff system.

    Thread-safety: All methods are called from the single asyncio event loop.
    No locks are needed.
    """

    def __init__(self) -> None:
        # session_id → list of active WebSocket objects in that room
        self.rooms: dict[str, list[WebSocket]] = {}

        # session_id → True when a human counselor has joined
        self.has_human: dict[str, bool] = {}

        # session_id → True when the patient (user) has joined
        self.has_user: dict[str, bool] = {}

        # id(websocket) → "user" | "human_counselor"
        self.ws_roles: dict[int, str] = {}

        # All connected counselor dashboard WebSockets
        self.dashboard_clients: set[WebSocket] = set()

        # counselor_id → list of their active dashboard WebSocket(s)
        self.counselor_ws: dict[str, list[WebSocket]] = {}

        # session_id → active asyncio.Task for the counselor timeout watchdog
        self.timeout_tasks: dict = {}

        # session_id → set of counselor_ids already counted in current_active_sessions
        # Prevents double-counting when a counselor opens multiple tabs for the same patient.
        self.room_counselors: dict[str, set[str]] = {}

        # Sessions permanently closed — reconnect attempts are silently rejected
        # to prevent the "session ended" reconnect loop on the client.
        self._ended_sessions: set[str] = set()

    # ── Session-ended tracking ────────────────────────────────────────────────

    def mark_session_ended(self, session_id: str) -> None:
        """Marks a session as permanently closed so reconnect attempts are rejected."""
        self._ended_sessions.add(session_id)

    def is_session_ended(self, session_id: str) -> bool:
        """Returns True if the session has been permanently closed."""
        return session_id in self._ended_sessions

    # ── Timeout task management ───────────────────────────────────────────────

    def start_timeout_task(self, session_id: str, task) -> None:
        """Registers a counselor timeout watchdog task for a session."""
        self.timeout_tasks[session_id] = task

    def cancel_timeout_task(self, session_id: str) -> None:
        """Cancels and removes the timeout task for a session, if one exists."""
        task = self.timeout_tasks.pop(session_id, None)
        if task:
            task.cancel()

    def remove_timeout_task(self, session_id: str) -> None:
        """Removes the timeout task reference without cancelling it."""
        self.timeout_tasks.pop(session_id, None)

    # ── Room counselor tracking ───────────────────────────────────────────────

    def is_counselor_in_room(self, session_id: str, counselor_id: str) -> bool:
        """Returns True if this counselor has already been counted in this room."""
        return counselor_id in self.room_counselors.get(session_id, set())

    def add_counselor_to_room(self, session_id: str, counselor_id: str) -> bool:
        """
        Adds a counselor to the room's tracked set.
        Returns True if this is the first connection for this counselor in this room.
        """
        if counselor_id in self.room_counselors.get(session_id, set()):
            return False
        self.room_counselors.setdefault(session_id, set()).add(counselor_id)
        return True

    def remove_counselor_from_room(self, session_id: str, counselor_id: str) -> None:
        """Removes a counselor from the room's tracked set."""
        if session_id in self.room_counselors:
            self.room_counselors[session_id].discard(counselor_id)
            if not self.room_counselors[session_id]:
                del self.room_counselors[session_id]

    # ── WebSocket role tracking ───────────────────────────────────────────────

    def register_ws_role(self, websocket: WebSocket, role: str) -> None:
        """Associates a live WebSocket object with its participant role."""
        self.ws_roles[id(websocket)] = role

    def unregister_ws_role(self, websocket: WebSocket) -> None:
        """Removes the role association for a WebSocket (called on disconnect)."""
        self.ws_roles.pop(id(websocket), None)

    def is_role_in_room(self, session_id: str, role: str) -> bool:
        """Returns True if any currently-open socket in this room has the given role."""
        return any(
            self.ws_roles.get(id(ws)) == role
            for ws in self.rooms.get(session_id, [])
        )

    # ── Presence flags ────────────────────────────────────────────────────────

    def mark_human_joined(self, session_id: str) -> None:
        self.has_human[session_id] = True

    def mark_human_left(self, session_id: str) -> None:
        self.has_human[session_id] = False

    def human_has_joined(self, session_id: str) -> bool:
        return self.has_human.get(session_id, False)

    def mark_user_joined(self, session_id: str) -> None:
        self.has_user[session_id] = True

    def mark_user_left(self, session_id: str) -> None:
        self.has_user[session_id] = False

    def user_has_joined(self, session_id: str) -> bool:
        return self.has_user.get(session_id, False)

    # ── Connection lifecycle ──────────────────────────────────────────────────

    async def connect(self, session_id: str, websocket: WebSocket) -> None:
        """Accepts a WebSocket and registers it in the session room."""
        await websocket.accept()
        self.rooms.setdefault(session_id, []).append(websocket)
        active_count = len(self.rooms[session_id])
        logger.info(
            f"[WS] Socket registered in room | session={session_id}"
            f" | active_connections_in_room={active_count}"
        )

    def disconnect(self, session_id: str, websocket: WebSocket) -> None:
        """
        Removes a WebSocket from its room.
        If the room becomes empty, cleans up all associated state.
        """
        self.unregister_ws_role(websocket)
        if session_id in self.rooms:
            self.rooms[session_id] = [ws for ws in self.rooms[session_id] if ws is not websocket]
            if not self.rooms[session_id]:
                del self.rooms[session_id]
                counselor_had_joined = self.has_human.pop(session_id, False)
                self.has_user.pop(session_id, None)
                self.room_counselors.pop(session_id, None)
                # Only cancel the watchdog if a counselor already joined.
                # If no counselor ever joined, let the timeout fire so re-routing
                # can trigger — cancelling here would orphan the session.
                if counselor_had_joined:
                    self.cancel_timeout_task(session_id)
        logger.info(f"[WS] Socket removed from room | session={session_id}")

    # ── Message broadcasting ──────────────────────────────────────────────────

    async def broadcast(
        self,
        session_id: str,
        payload: dict,
        sender_ws: Optional[WebSocket] = None,
    ) -> None:
        """Sends a message to all sockets in a room except the sender."""
        message = json.dumps(payload)
        dead_sockets: list[WebSocket] = []

        for ws in self.rooms.get(session_id, []):
            if ws is sender_ws:
                continue
            try:
                await ws.send_text(message)
            except Exception:
                dead_sockets.append(ws)

        for ws in dead_sockets:
            self.disconnect(session_id, ws)

    async def send_to_all(self, session_id: str, payload: dict) -> None:
        """
        Sends a terminal message to every socket in the room and closes them all.
        Uses close code 4001 so clients can distinguish a permanent session-end
        from a transient network drop and stop auto-reconnecting.
        """
        self.mark_session_ended(session_id)
        message = json.dumps(payload)
        socket_list = self.rooms.get(session_id, []).copy()

        for ws in socket_list:
            self.unregister_ws_role(ws)
            try:
                await ws.send_text(message)
                await ws.close(code=4001)
            except Exception as exc:
                logger.warning(f"[WS] Failed to send terminal message or close socket | session={session_id} | error={exc}")

        # Clean up room state directly without triggering timeout cancellation
        self.rooms.pop(session_id, None)
        self.has_human.pop(session_id, None)
        self.has_user.pop(session_id, None)
        self.room_counselors.pop(session_id, None)

    # ── Dashboard connections ─────────────────────────────────────────────────

    async def connect_dashboard(
        self,
        websocket: WebSocket,
        counselor_id: Optional[str] = None,
    ) -> None:
        """Accepts and registers a counselor dashboard WebSocket connection."""
        await websocket.accept()
        self.dashboard_clients.add(websocket)
        if counselor_id:
            self.counselor_ws.setdefault(counselor_id, []).append(websocket)
        logger.info(
            f"[WS DASHBOARD] Connected | counselor_id={counselor_id or 'anonymous'}"
            f" | total_dashboard_clients={len(self.dashboard_clients)}"
        )

    def disconnect_dashboard(
        self,
        websocket: WebSocket,
        counselor_id: Optional[str] = None,
    ) -> None:
        """Removes a counselor dashboard WebSocket connection."""
        self.dashboard_clients.discard(websocket)
        if counselor_id and counselor_id in self.counselor_ws:
            self.counselor_ws[counselor_id] = [
                ws for ws in self.counselor_ws[counselor_id] if ws is not websocket
            ]
            if not self.counselor_ws[counselor_id]:
                del self.counselor_ws[counselor_id]
        logger.info(
            f"[WS DASHBOARD] Disconnected | counselor_id={counselor_id or 'anonymous'}"
            f" | total_dashboard_clients={len(self.dashboard_clients)}"
        )

    async def broadcast_to_dashboard(self, payload: dict) -> None:
        """Broadcasts a message to all connected counselor dashboard clients."""
        if not self.dashboard_clients:
            return
        message = json.dumps(payload)
        dead_sockets: set[WebSocket] = set()

        for ws in self.dashboard_clients:
            try:
                await ws.send_text(message)
            except Exception:
                dead_sockets.add(ws)

        for ws in dead_sockets:
            self.disconnect_dashboard(ws)

    async def notify_counselor(self, counselor_id: str, payload: dict) -> bool:
        """
        Sends a targeted push to a specific counselor's dashboard WebSocket(s).

        Returns:
            True if at least one message was delivered successfully.
            False if the counselor has no active dashboard connection.
        """
        target_sockets = self.counselor_ws.get(counselor_id, []).copy()
        if not target_sockets:
            return False

        message = json.dumps(payload)
        delivered = False
        dead_sockets: list[WebSocket] = []

        for ws in target_sockets:
            try:
                await ws.send_text(message)
                delivered = True
            except Exception:
                dead_sockets.append(ws)

        for ws in dead_sockets:
            self.disconnect_dashboard(ws, counselor_id)

        return delivered


# Module-level singleton — shared across all route handlers in this package
manager = ConnectionManager()
