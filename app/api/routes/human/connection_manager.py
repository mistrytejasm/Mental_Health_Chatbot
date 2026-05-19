import json
import time
import uuid
import asyncio
from typing import Optional

from fastapi import WebSocket

from app.core.logger import get_logger
from app.core.redis import get_redis
from app.core.database import get_database
from bson import ObjectId

logger = get_logger(__name__)

class ConnectionManager:
    def __init__(self) -> None:
        self.rooms: dict[str, list[WebSocket]] = {}
        self.ws_ids: dict[WebSocket, str] = {}
        self.ws_roles_local: dict[WebSocket, str] = {}
        
        self.dashboard_clients: set[WebSocket] = set()
        self.counselor_ws: dict[str, list[WebSocket]] = {}
        
        self.session_activity_events: dict[str, asyncio.Event] = {}
        self.timeout_tasks: dict = {}
        # H-10: timestamp dict instead of a plain set.
        # Entries are auto-expired after 1 hour by is_session_ended().
        # This bounds memory to O(sessions per hour) instead of O(all-time sessions).
        self._ended_sessions: dict[str, float] = {}  # session_id -> unix timestamp
        self.pubsub_tasks: dict[str, asyncio.Task] = {}
        self.notify_tasks: dict[str, asyncio.Task] = {}
        self._dashboard_listener: Optional[asyncio.Task] = None

    # ── Session-ended tracking (H-10: TTL dict + Redis backup) ──────────────
    _SESSION_ENDED_TTL = 3600  # 1 hour

    def mark_session_ended(self, session_id: str) -> None:
        """Record a session as ended. Timestamp enables inline TTL expiry."""
        self._ended_sessions[session_id] = time.monotonic()
        # Also persist to Redis with matching TTL so the flag survives a server restart.
        redis = get_redis()
        if redis:
            asyncio.create_task(
                redis.setex(f"session:{session_id}:ended", self._SESSION_ENDED_TTL, "1")
            )

    def is_session_ended(self, session_id: str) -> bool:
        """Returns True if the session was marked ended within the last hour.
        Expired entries are removed inline — no background cleanup needed.
        """
        ts = self._ended_sessions.get(session_id)
        if ts is None:
            return False
        if time.monotonic() - ts > self._SESSION_ENDED_TTL:
            # Entry has expired — remove it to free memory
            del self._ended_sessions[session_id]
            return False
        return True

    # ── Timeout task management ───────────────────────────────────────────────
    def start_timeout_task(self, session_id: str, task) -> None:
        self.timeout_tasks[session_id] = task

    def cancel_timeout_task(self, session_id: str) -> None:
        task = self.timeout_tasks.pop(session_id, None)
        if task:
            task.cancel()

    def remove_timeout_task(self, session_id: str) -> None:
        self.timeout_tasks.pop(session_id, None)

    def start_notify_task(self, session_id: str, task: asyncio.Task) -> None:
        existing = self.notify_tasks.pop(session_id, None)
        if existing: existing.cancel()
        self.notify_tasks[session_id] = task

    def cancel_notify_task(self, session_id: str) -> None:
        task = self.notify_tasks.pop(session_id, None)
        if task: task.cancel()

    # ── Room counselor tracking (Redis) ───────────────────────────────────────
    async def is_counselor_in_room(self, session_id: str, counselor_id: str) -> bool:
        redis = get_redis()
        if not redis: return False
        return await redis.sismember(f"session:{session_id}:counselors", counselor_id)

    async def add_counselor_to_room(self, session_id: str, counselor_id: str) -> bool:
        redis = get_redis()
        if not redis: return False
        # returns 1 if added (was not there), 0 if already there
        added = await redis.sadd(f"session:{session_id}:counselors", counselor_id)
        # expire the set after 24h just to be safe
        await redis.expire(f"session:{session_id}:counselors", 86400)
        return bool(added)

    async def remove_counselor_from_room(self, session_id: str, counselor_id: str) -> None:
        redis = get_redis()
        if redis:
            await redis.srem(f"session:{session_id}:counselors", counselor_id)

    # ── WebSocket role tracking (Redis) ───────────────────────────────────────
    async def register_ws_role(self, websocket: WebSocket, session_id: str, role: str) -> None:
        self.ws_roles_local[websocket] = role
        redis = get_redis()
        if redis:
            await redis.hincrby(f"session:{session_id}:roles", role, 1)
            await redis.expire(f"session:{session_id}:roles", 86400)

    async def unregister_ws_role(self, websocket: WebSocket, session_id: str) -> None:
        role = self.ws_roles_local.pop(websocket, None)
        if not role: return
        redis = get_redis()
        if redis:
            count = await redis.hincrby(f"session:{session_id}:roles", role, -1)
            if count <= 0:
                await redis.hdel(f"session:{session_id}:roles", role)

    async def is_role_in_room(self, session_id: str, role: str) -> bool:
        count = await self.get_role_count(session_id, role)
        return count > 0

    async def get_role_count(self, session_id: str, role: str) -> int:
        redis = get_redis()
        if not redis:
            # Local fallback
            return sum(1 for r in self.ws_roles_local.values() if r == role)
        count = await redis.hget(f"session:{session_id}:roles", role)
        return int(count) if count else 0

    # ── Presence flags (Redis) ────────────────────────────────────────────────
    async def mark_human_joined(self, session_id: str) -> None:
        redis = get_redis()
        if redis: await redis.set(f"session:{session_id}:has_human", "1", ex=86400)

    async def mark_human_left(self, session_id: str) -> None:
        redis = get_redis()
        if redis: await redis.set(f"session:{session_id}:has_human", "0", ex=86400)

    async def human_has_joined(self, session_id: str) -> bool:
        redis = get_redis()
        if not redis: return False
        val = await redis.get(f"session:{session_id}:has_human")
        return val == "1"

    async def mark_user_joined(self, session_id: str) -> None:
        redis = get_redis()
        if redis: await redis.set(f"session:{session_id}:has_user", "1", ex=86400)

    async def mark_user_left(self, session_id: str) -> None:
        redis = get_redis()
        if redis: await redis.set(f"session:{session_id}:has_user", "0", ex=86400)

    async def user_has_joined(self, session_id: str) -> bool:
        redis = get_redis()
        if not redis: return False
        val = await redis.get(f"session:{session_id}:has_user")
        return val == "1"

    # ── Activity Tracking ─────────────────────────────────────────────────────
    def get_activity_event(self, session_id: str) -> asyncio.Event:
        if session_id not in self.session_activity_events:
            self.session_activity_events[session_id] = asyncio.Event()
        return self.session_activity_events[session_id]

    def mark_activity(self, session_id: str) -> None:
        if session_id in self.session_activity_events:
            self.session_activity_events[session_id].set()

    def remove_activity_event(self, session_id: str) -> None:
        self.session_activity_events.pop(session_id, None)

    # ── Connection lifecycle ──────────────────────────────────────────────────
    async def connect(self, session_id: str, websocket: WebSocket) -> None:
        await websocket.accept()
        ws_id = uuid.uuid4().hex
        self.ws_ids[websocket] = ws_id
        
        self.rooms.setdefault(session_id, []).append(websocket)
        
        if session_id not in self.pubsub_tasks:
            self.pubsub_tasks[session_id] = asyncio.create_task(self._listen_to_room(session_id))
            
        logger.info(f"[WS] Socket registered locally | session={session_id}")

    async def disconnect(self, session_id: str, websocket: WebSocket) -> None:
        await self.unregister_ws_role(websocket, session_id)
        self.ws_ids.pop(websocket, None)
        if session_id in self.rooms:
            self.rooms[session_id] = [ws for ws in self.rooms[session_id] if ws is not websocket]
            if not self.rooms[session_id]:
                del self.rooms[session_id]
                self.remove_activity_event(session_id)
                # Cancel local pubsub listener
                task = self.pubsub_tasks.pop(session_id, None)
                if task:
                    task.cancel()
                    
                counselor_had_joined = await self.human_has_joined(session_id)
                if counselor_had_joined:
                    self.cancel_timeout_task(session_id)
        logger.info(f"[WS] Socket removed locally | session={session_id}")

    # ── Redis Pub/Sub Listener ───────────────────────────────────────────────
    async def _listen_to_room(self, session_id: str):
        redis = get_redis()
        if not redis: return
        pubsub = redis.pubsub()
        channel = f"channel:room:{session_id}"
        await pubsub.subscribe(channel)
        
        try:
            async for message in pubsub.listen():
                if message["type"] == "message":
                    self.mark_activity(session_id)
                    data = json.loads(message["data"])
                    action = data.get("action", "broadcast")
                    
                    if action == "broadcast":
                        payload = data.get("payload", {})
                        exclude_ws_id = data.get("exclude_ws_id")
                        dead_sockets = []
                        for ws in self.rooms.get(session_id, []):
                            if self.ws_ids.get(ws) == exclude_ws_id:
                                continue
                            try:
                                await asyncio.wait_for(ws.send_text(json.dumps(payload)), timeout=5.0)  # H-6
                            except (asyncio.TimeoutError, Exception):
                                dead_sockets.append(ws)
                        for ws in dead_sockets:
                            await self.disconnect(session_id, ws)
                            
                    elif action == "send_to_all":
                        payload = data.get("payload", {})
                        message_str = json.dumps(payload)
                        socket_list = self.rooms.get(session_id, []).copy()
                        for ws in socket_list:
                            try:
                                await asyncio.wait_for(ws.send_text(message_str), timeout=5.0)  # H-6
                                await ws.close(code=4001)
                            except (asyncio.TimeoutError, Exception):
                                pass
                        self.rooms.pop(session_id, None)
                        
        except asyncio.CancelledError:
            try:
                await pubsub.unsubscribe(channel)
                await pubsub.close()
            except Exception:
                pass
        except Exception as exc:
            logger.error(f"[WS] PubSub listener crashed | session={session_id} | error={exc}", exc_info=True)
            try:
                await pubsub.unsubscribe(channel)
                await pubsub.close()
            except Exception:
                pass
            # Task died, remove from tracking so it can be recreated if needed
            self.pubsub_tasks.pop(session_id, None)

    # ── Message broadcasting ──────────────────────────────────────────────────
    async def broadcast(self, session_id: str, payload: dict, sender_ws: Optional[WebSocket] = None) -> None:
        redis = get_redis()
        if redis:
            exclude_ws_id = self.ws_ids.get(sender_ws) if sender_ws else None
            data = {
                "action": "broadcast",
                "payload": payload,
                "exclude_ws_id": exclude_ws_id
            }
            await redis.publish(f"channel:room:{session_id}", json.dumps(data))

    async def send_to_all(self, session_id: str, payload: dict) -> None:
        self.mark_session_ended(session_id)
        redis = get_redis()
        if redis:
            data = {
                "action": "send_to_all",
                "payload": payload
            }
            await redis.publish(f"channel:room:{session_id}", json.dumps(data))
            # Clean up all Redis state for this session so stale data
            # cannot affect future escalations on the same session_id.
            await redis.delete(
                f"session:{session_id}:roles",
                f"session:{session_id}:has_human",
                f"session:{session_id}:has_user",
                f"session:{session_id}:counselors",
            )

    # ── Dashboard connections ─────────────────────────────────────────────────
    # We will keep dashboards simple for now, but a global notification needs PubSub
    async def connect_dashboard(self, websocket: WebSocket, counselor_id: Optional[str] = None) -> None:
        await websocket.accept()
        ws_id = uuid.uuid4().hex
        self.ws_ids[websocket] = ws_id
        self.dashboard_clients.add(websocket)
        if counselor_id:
            self.counselor_ws.setdefault(counselor_id, []).append(websocket)
            # Mark the counselor as online in the global Redis set so
            # notify_counselor() can report accurate delivery status.
            redis = get_redis()
            if redis:
                await redis.sadd("dashboard:online_counselors", counselor_id)

            # H-5: Pending notification sync is handled authoritatively by the
            # dashboard_notifications_ws handler in websocket.py which has full
            # validation (session still escalated check). Do NOT duplicate here.

        if not self._dashboard_listener:
            self._dashboard_listener = asyncio.create_task(self._listen_to_dashboard())

    def disconnect_dashboard(self, websocket: WebSocket, counselor_id: Optional[str] = None) -> None:
        self.ws_ids.pop(websocket, None)
        self.dashboard_clients.discard(websocket)
        if counselor_id and counselor_id in self.counselor_ws:
            self.counselor_ws[counselor_id] = [ws for ws in self.counselor_ws[counselor_id] if ws is not websocket]
            if not self.counselor_ws[counselor_id]:
                del self.counselor_ws[counselor_id]
                # No more local sockets for this counselor — remove from the global
                # Redis online set so the routing engine sees them as offline.
                import asyncio as _asyncio
                redis = get_redis()
                if redis:
                    _asyncio.create_task(redis.srem("dashboard:online_counselors", counselor_id))

        if not self.dashboard_clients and self._dashboard_listener:
            self._dashboard_listener.cancel()
            self._dashboard_listener = None

    async def _listen_to_dashboard(self):
        """
        MEDIUM-6: Wrapped in a reconnect loop so a Redis restart or connection
        drop does not silently kill all dashboard notifications. Auto-reconnects
        after a 3-second backoff.
        """
        while True:
            try:
                redis = get_redis()
                if not redis:
                    await asyncio.sleep(5)
                    continue
                pubsub = redis.pubsub()
                await pubsub.subscribe("channel:dashboard:broadcast")
                await pubsub.psubscribe("channel:dashboard:notify:*")

                async for message in pubsub.listen():
                    if message["type"] not in ("message", "pmessage"):
                        continue
                    raw = message["data"]
                    if isinstance(raw, bytes):
                        raw = raw.decode()
                    try:
                        data = json.loads(raw)
                    except (json.JSONDecodeError, TypeError):
                        continue
                    action = data.get("action")
                    payload = data.get("payload", {})

                    if action == "broadcast_all":
                        dead_sockets: set = set()
                        msg_str = json.dumps(payload)
                        for ws in list(self.dashboard_clients):
                            try:
                                await asyncio.wait_for(ws.send_text(msg_str), timeout=5.0)  # H-6
                            except (asyncio.TimeoutError, Exception):
                                dead_sockets.add(ws)
                        for ws in dead_sockets:
                            self.disconnect_dashboard(ws)

                    elif action == "notify_counselor":
                        counselor_id = data.get("counselor_id")
                        target_sockets = self.counselor_ws.get(counselor_id, []).copy()
                        msg_str = json.dumps(payload)
                        dead: list = []
                        for ws in target_sockets:
                            try:
                                await asyncio.wait_for(ws.send_text(msg_str), timeout=5.0)  # H-6
                            except (asyncio.TimeoutError, Exception):
                                dead.append(ws)
                        for ws in dead:
                            self.disconnect_dashboard(ws, counselor_id)

            except asyncio.CancelledError:
                break  # Intentional shutdown — do not reconnect
            except Exception as e:
                logger.error(
                    f"[PUBSUB] Dashboard listener crashed: {e}. "
                    f"Reconnecting in 3 seconds..."
                )
                await asyncio.sleep(3)  # Brief backoff before reconnect

    async def broadcast_to_dashboard(self, payload: dict) -> None:
        redis = get_redis()
        if redis:
            await redis.publish("channel:dashboard:broadcast", json.dumps({
                "action": "broadcast_all",
                "payload": payload
            }))

    async def notify_counselor(self, counselor_id: str, payload: dict) -> bool:
        """
        Sends a targeted notification to a specific counselor.

        Delivery strategy (two-tier):
          1. Local socket delivery (instantaneous).
          2. Redis Pub/Sub for cross-instance delivery.

        H-6: All sends are wrapped with a 5-second timeout so a half-open TCP
        socket on an unstable network never blocks the notification coroutine.
        """
        # --- Tier 1: direct delivery on this process ---
        local_sockets = self.counselor_ws.get(counselor_id, [])
        if local_sockets:
            msg_str = json.dumps(payload)
            dead: list = []
            delivered = False
            for ws in list(local_sockets):
                try:
                    await asyncio.wait_for(ws.send_text(msg_str), timeout=5.0)  # H-6
                    delivered = True
                except (asyncio.TimeoutError, Exception):
                    dead.append(ws)
            for ws in dead:
                self.disconnect_dashboard(ws, counselor_id)
            if delivered:
                return True

        # --- Tier 2: Redis Pub/Sub for multi-instance ---
        redis = get_redis()
        if redis:
            await redis.publish(
                f"channel:dashboard:notify:{counselor_id}",
                json.dumps({
                    "action": "notify_counselor",
                    "counselor_id": counselor_id,
                    "payload": payload,
                }),
            )
            # Report True if the counselor is online on any node
            is_online = await redis.sismember("dashboard:online_counselors", counselor_id)
            return bool(is_online)
        return False

manager = ConnectionManager()
