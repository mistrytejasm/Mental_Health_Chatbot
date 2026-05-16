import json
import uuid
import asyncio
from typing import Optional

from fastapi import WebSocket

from app.core.logger import get_logger
from app.core.redis import get_redis

logger = get_logger(__name__)

class ConnectionManager:
    def __init__(self) -> None:
        self.rooms: dict[str, list[WebSocket]] = {}
        self.ws_ids: dict[WebSocket, str] = {}
        self.ws_roles_local: dict[WebSocket, str] = {}
        
        self.dashboard_clients: set[WebSocket] = set()
        self.counselor_ws: dict[str, list[WebSocket]] = {}
        
        self.timeout_tasks: dict = {}
        self._ended_sessions: set[str] = set()
        self.pubsub_tasks: dict[str, asyncio.Task] = {}
        self._dashboard_listener: Optional[asyncio.Task] = None

    # ── Session-ended tracking ────────────────────────────────────────────────
    def mark_session_ended(self, session_id: str) -> None:
        self._ended_sessions.add(session_id)

    def is_session_ended(self, session_id: str) -> bool:
        return session_id in self._ended_sessions

    # ── Timeout task management ───────────────────────────────────────────────
    def start_timeout_task(self, session_id: str, task) -> None:
        self.timeout_tasks[session_id] = task

    def cancel_timeout_task(self, session_id: str) -> None:
        task = self.timeout_tasks.pop(session_id, None)
        if task:
            task.cancel()

    def remove_timeout_task(self, session_id: str) -> None:
        self.timeout_tasks.pop(session_id, None)

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
        redis = get_redis()
        if not redis: return False
        count = await redis.hget(f"session:{session_id}:roles", role)
        return int(count) > 0 if count else False

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
                                await ws.send_text(json.dumps(payload))
                            except Exception:
                                dead_sockets.append(ws)
                        for ws in dead_sockets:
                            await self.disconnect(session_id, ws)
                            
                    elif action == "send_to_all":
                        payload = data.get("payload", {})
                        message_str = json.dumps(payload)
                        socket_list = self.rooms.get(session_id, []).copy()
                        for ws in socket_list:
                            try:
                                await ws.send_text(message_str)
                                await ws.close(code=4001)
                            except Exception:
                                pass
                        self.rooms.pop(session_id, None)
                        
        except asyncio.CancelledError:
            await pubsub.unsubscribe(channel)
            await pubsub.close()

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
        redis = get_redis()
        if not redis: return
        pubsub = redis.pubsub()
        await pubsub.subscribe("channel:dashboard:broadcast")
        await pubsub.psubscribe("channel:dashboard:notify:*")
        
        try:
            async for message in pubsub.listen():
                if message["type"] not in ("message", "pmessage"):
                    continue
                raw = message["data"]
                # decode_responses=True means data is already str, but guard against bytes
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
                            await ws.send_text(msg_str)
                        except Exception:
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
                            await ws.send_text(msg_str)
                        except Exception:
                            dead.append(ws)
                    for ws in dead:
                        self.disconnect_dashboard(ws, counselor_id)
        except asyncio.CancelledError:
            await pubsub.unsubscribe()
            await pubsub.punsubscribe()
            await pubsub.close()

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
          1. If the counselor has a dashboard WebSocket on *this* process, deliver
             directly via the local socket list — instantaneous and reliable.
          2. If they are on a *different* process (multi-instance deploy), publish to
             the Redis Pub/Sub channel so the other instance delivers it.

        Returns True when the counselor is reachable (local delivery confirmed OR
        they are registered in the global 'dashboard:online_counselors' Redis set).
        """
        # --- Tier 1: direct delivery on this process ---
        local_sockets = self.counselor_ws.get(counselor_id, [])
        if local_sockets:
            msg_str = json.dumps(payload)
            dead: list = []
            delivered = False
            for ws in list(local_sockets):
                try:
                    await ws.send_text(msg_str)
                    delivered = True
                except Exception:
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
