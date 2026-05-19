# MindBuddy Backend — Full Performance & Optimization Audit Report

> **Branch:** `backend_optimization_delay_notification`
> **Date:** 2026-05-16
> **Scope:** Complete backend codebase review — Database, Real-Time, Routing, Scalability, Architecture

---

## Executive Summary

The codebase is **well-structured and production-aware**, with solid foundations: async Motor/MongoDB, Redis Pub/Sub, WebSocket management, LLM integration, and background watchdogs. The critical notification reliability gap has already been fixed (Guaranteed Delivery). This report identifies the remaining issues ordered by priority.

---

## Priority Legend

| Priority | Meaning |
|---|---|
| 🔴 HIGH | Production-breaking. Can cause data loss, race conditions, or silent failures. Fix immediately. |
| 🟡 MEDIUM | Degrades performance or reliability under load. Fix before scaling. |
| 🟢 LOW | Best practice improvements. Fix during next maintenance window. |

---

## 1. Database Optimization

### 🔴 HIGH-1 — `upsert_session` does a double DB round-trip on every chat message

**File:** `db_service.py:231-250`

```python
# After upsert, it immediately does a second find_one to return the doc:
await db.sessions.update_one(...)     # write
return await db.sessions.find_one({"session_id": session_id})  # unnecessary read
```

**Problem:** Every single user message calls `upsert_session` in `chat.py:112`, which triggers two DB round-trips. Under load, this doubles the session-related read pressure on every message.

**Fix:** Use `find_one_and_update` with `return_document=True` to get the document in a single atomic operation:

```python
from pymongo import ReturnDocument
doc = await db.sessions.find_one_and_update(
    {"session_id": session_id},
    {"$setOnInsert": {...}},
    upsert=True,
    return_document=ReturnDocument.AFTER,
)
return doc
```

---

### 🔴 HIGH-2 — `get_session_messages` uses unbounded `to_list(length=None)`

**File:** `db_service.py:510`

```python
docs = await cursor.to_list(length=None)  # ← loads ALL messages into memory
```

**Problem:** If a session has 10,000 messages, this loads all of them into RAM. This is a memory bomb. Also seen in `get_all_sessions` (line 592) and `get_escalated_sessions` (line 668).

**Fix:** Always add a limit. For `get_session_messages`, the UI will never render 10,000 messages. Cap at 500 or paginate.

```python
docs = await cursor.to_list(length=500)
```

---

### 🔴 HIGH-3 — Missing Compound Index for Routing Query (Counselor Availability)

**File:** `database.py:46-47`

The routing query in `routing_service.py` filters on `is_online`, `last_ping`, `current_active_sessions`, and `checked_in_at`. But the current indexes are:

```
index: (is_online, last_ping)   ← exists
index: current_active_sessions  ← exists, single field
```

**Problem:** MongoDB cannot use both indexes simultaneously for a single query. The routing query will use `(is_online, last_ping)` and then do a **collection scan** on the remaining 3 fields. Under load with many counselors, this becomes slow.

**Fix:** Add a compound index covering the full routing query:

```python
await db.admins.create_index([
    ("is_online", 1),
    ("is_active", 1),
    ("last_ping", -1),
    ("current_active_sessions", 1),
    ("checked_in_at", 1),
], name="counselor_routing_compound")
```

---

### 🔴 HIGH-4 — `get_expired_escalated_sessions` is missing an Index on `updated_at`

**File:** `db_service.py:701-705`, called every 60 seconds by the global watchdog

```python
cursor = db.sessions.find({
    "is_escalated": True,
    "updated_at": {"$lte": expiration_time}
})
```

**Problem:** `updated_at` has **no index**. This query runs every 60 seconds and does a full collection scan of `sessions`. Under 10,000+ sessions, this is a guaranteed slow query.

**Fix:** Add to `database.py`:

```python
await db.sessions.create_index([("is_escalated", 1), ("updated_at", 1)])
```

---

### 🟡 MEDIUM-1 — `save_message` does 2 DB writes per message (insert + update)

**File:** `db_service.py:444-448`

```python
result = await db.messages.insert_one(doc)   # write 1
await db.sessions.update_one(                 # write 2 — just to set updated_at
    {"session_id": ...},
    {"$set": {"updated_at": datetime.now(timezone.utc)}},
)
```

**Problem:** Every message costs 2 DB round-trips. The `sessions.updated_at` update is useful for the inactivity watchdog, but it doesn't need to block the critical path.

**Fix:** Fire the session `updated_at` stamp as a background task so the message insert is not blocked:

```python
result = await db.messages.insert_one(doc)
asyncio.create_task(
    db.sessions.update_one({"session_id": ...}, {"$set": {"updated_at": ...}})
)
```

---

### 🟡 MEDIUM-2 — `is_session_escalated` fetches full document just to check one field

**File:** `db_service.py:386`

```python
doc = await db.sessions.find_one({"session_id": session_id})  # loads full document
if doc and doc.get("is_escalated") is True:
```

**Fix:** Project only the needed field:

```python
doc = await db.sessions.find_one(
    {"session_id": session_id},
    {"is_escalated": 1}  # ← project only what you need
)
```

---

### 🟡 MEDIUM-3 — `get_user_profile` builds an `$or` query on every single chat message

**File:** `db_service.py:157-161`

```python
query = [{"user_id": user_id}]
if ObjectId.is_valid(user_id):
    query.append({"_id": ObjectId(user_id)})
doc = await db.users.find_one({"$or": query})
```

**Problem:** `$or` queries cannot use a single index efficiently — MongoDB must evaluate both branches. This is called on every `/api/chat/stream` request.

**Fix:** Since users are always identified by their `_id` (ObjectId), standardize on `_id` lookups. The dual-format fallback is only needed for legacy migration. Consider caching the profile in Redis for 5 minutes (TTL) since personality data rarely changes.

---

### 🟡 MEDIUM-4 — `get_escalated_sessions` uses a `$group` stage without a `session_id` index

**File:** `db_service.py:623-686`

The pipeline is:
1. `$match` on `is_escalated: True`
2. `$sort` on `escalated_at: -1`
3. `$group` by `user_id` — full in-memory group
4. `$replaceRoot`
5. `$sort` again
6. `$lookup` into `users`

**Problem:** Steps 2-4 cannot be optimized by indexes because `$sort` before `$group` forces MongoDB to sort the entire matched result set in memory. Also the double `$sort` is inefficient.

**Fix:** Move the `$group` before the first `$sort`, or better, add a compound index:

```python
await db.sessions.create_index([("is_escalated", 1), ("escalated_at", -1)])
```

---

### 🟡 MEDIUM-5 — `generate_embedding` creates a new `AsyncOpenAI` client on every call

**File:** `db_service.py:25`

```python
client = AsyncOpenAI(api_key=settings.OPENAI_API_KEY)  # new client every time!
```

**Problem:** `AsyncOpenAI` client construction sets up an HTTP connection pool. Creating a new one for every embedding call wastes connections and adds latency (~5-10ms per call).

**Fix:** Initialize once as a module-level singleton:

```python
_openai_client: Optional[AsyncOpenAI] = None

def _get_openai_client() -> AsyncOpenAI:
    global _openai_client
    if _openai_client is None:
        _openai_client = AsyncOpenAI(api_key=settings.OPENAI_API_KEY)
    return _openai_client
```

---

### 🟢 LOW-1 — `get_all_sessions` and `get_escalated_sessions` have no pagination

**Fix:** Add `skip` and `limit` parameters for API-level pagination. Exposing unlimited lists to the API is a security and performance risk.

---

## 2. Real-Time Notification System

### 🔴 HIGH-5 — Double Pending-Notification Delivery (Both managers sync on connect)

**File:** `connection_manager.py:connect_dashboard` + `websocket.py:150-175`

After the recent Guaranteed Delivery fix, BOTH the `connection_manager.connect_dashboard` method AND the `dashboard_notifications_ws` WebSocket handler independently check for and deliver `pending_notification`:

- `websocket.py:126-170` — Fetches `admin_doc`, delivers `pending_notification`, clears it
- `connection_manager.py:~233` — Also fetches `admin_doc`, delivers, clears it

**Problem:** The WebSocket handler runs **after** `connect_dashboard`. The connection manager may deliver and clear the notification first, then the WebSocket handler finds nothing. Or they both run before either clears — delivering the notification twice and creating a race on the `$unset`.

**Fix:** Remove the duplicate sync from `connection_manager.py`. The authoritative sync already lives in `websocket.py` (the WebSocket handler), which has the correct validation logic (e.g., checking if the session is still escalated before delivering). The connection manager should not duplicate this.

---

### 🔴 HIGH-6 — WebSocket Dead Socket Detection has No Timeout

**File:** `connection_manager.py:notify_counselor:336-338`

```python
for ws in list(local_sockets):
    try:
        await ws.send_text(msg_str)  # ← No timeout
        delivered = True
    except Exception:
        dead.append(ws)
```

**Problem:** On a half-open TCP connection (exactly your office Wi-Fi scenario), `send_text` can hang for **30-90 seconds** before the OS-level TCP timeout fires. This blocks the notification coroutine for the full TCP timeout duration, delaying counselor assignment.

**Fix:** Wrap every WebSocket send with `asyncio.wait_for`:

```python
try:
    await asyncio.wait_for(ws.send_text(msg_str), timeout=5.0)
    delivered = True
except (asyncio.TimeoutError, Exception):
    dead.append(ws)
```

---

### 🟡 MEDIUM-6 — Redis Pub/Sub listener has no error recovery / reconnect loop

**File:** `connection_manager.py:_listen_to_dashboard:258`

```python
async for message in pubsub.listen():
    ...
except asyncio.CancelledError:
    ...
```

**Problem:** If Redis drops the connection (restart, network blip), `pubsub.listen()` raises a connection error. The `except asyncio.CancelledError` does NOT catch it, so the entire listener task dies silently. All future dashboard notifications are lost until the server restarts.

**Fix:** Wrap the listener in a reconnect loop:

```python
async def _listen_to_dashboard(self):
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
                # ... handle message
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(f"[PUBSUB] Dashboard listener crashed: {e}. Reconnecting in 3s...")
            await asyncio.sleep(3)  # reconnect after brief delay
```

---

### 🟡 MEDIUM-7 — No heartbeat/ping-pong on user-facing chat WebSocket

**File:** `websocket.py` (chat endpoint)

The dashboard WebSocket has a `ping`/`pong` handler (line 185), but the **user chat WebSocket** has no ping-pong mechanism. On mobile clients or unstable networks, the WebSocket can appear "connected" to the server but be silently dead.

**Fix:** Add a server-initiated ping every 30 seconds on the chat WebSocket, and close the connection if no pong is received within 10 seconds.

---

## 3. Counselor Assignment & Race Conditions

### 🔴 HIGH-7 — `current_active_sessions` is never atomically incremented on assignment

**File:** `routing_service.py`

The routing system reads `current_active_sessions` from the DB to check capacity, then assigns the counselor. But **there is no `$inc` on assignment** — the counter is never explicitly incremented in the routing code.

**Problem:** The counter can only be accurate if something else is updating it. If the heartbeat or WebSocket join handler is responsible for this, there's a window between "assignment" and "counter update" where 3 concurrent escalations could all read `current_active_sessions = 0` for the same counselor and all assign to them — violating the 2-chat limit.

**Fix:** Atomically increment the counter as part of the assignment `update_one`:

```python
# In route_crisis_session, replace the assignment persist with:
await db.sessions.update_one({"session_id": session_id}, {"$set": {...}})
await db.admins.update_one(
    {"_id": ObjectId(counselor_id_str)},
    {"$inc": {"current_active_sessions": 1}}  # ← atomic, race-safe
)
```

And decrement it when a session ends.

---

### 🔴 HIGH-8 — `__routing__` lock only prevents duplicate routing for the same session, NOT concurrent capacity reads

**File:** `routing_service.py:212-221`

The routing lock (`assigned_counselor_id = "__routing__"`) prevents two routing tasks from racing on the **same session**. But it does NOT prevent two different sessions from simultaneously reading the same counselor's `current_active_sessions` count and both seeing capacity.

**This is a separate race condition from HIGH-7.** Fix HIGH-7 (atomic `$inc`) to resolve both.

---

### 🟡 MEDIUM-8 — `_swap_assignment` falls back silently on transaction failure

**File:** `routing_service.py:415-428`

The transaction fallback runs `_do_swap` sequentially without a transaction. The unique partial index is documented as the fallback guard, but the index is on `user_id` with `status: active` — it only prevents duplicate **active** records. If the deactivation write succeeds but the insert fails, the user is left with no active assignment.

**Recommendation:** Log transaction failures with more severity and add a reconciliation check.

---

## 4. Backend Performance

### 🔴 HIGH-9 — `get_formatted_history(limit=100)` loads up to 100 messages on EVERY chat message

**File:** `chat.py:146`

```python
history = await get_formatted_history(actual_session_id, limit=100)
```

**Problem:** Every single `/api/chat/stream` request loads 100 messages from DB, transfers them to the server, and passes the full list to the LLM. For long conversations, this is:
- 100 DB rows transferred on every message
- Full history passed to GPT-4o (burns token budget)
- No caching — same 99 messages loaded again and again

**Fix:** Cache the last N turns in Redis with a short TTL (e.g., 5 minutes), append the new message, and only fetch from DB on cache miss.

---

### 🟡 MEDIUM-9 — Emotion model is loaded synchronously and blocks on import

**File:** `app/services/emotion.py` (loaded at startup)

The RoBERTa model is loaded into memory on startup, which is correct. However, the `analyse()` call runs in the **main async thread** using `asyncio.get_event_loop().run_in_executor`. This is the right pattern, but the thread pool used by the executor is the default Python ThreadPoolExecutor, which is limited to `min(32, cpu_count + 4)` threads. Under concurrent load, inference calls will queue.

**Fix:** Create a dedicated `ThreadPoolExecutor` for ML inference with a fixed size and submit all inference calls to it.

---

### 🟡 MEDIUM-10 — Settings are re-read from environment inside hot paths

**File:** `routing_service.py:474`, `chat.py:213`

```python
from app.core.config import get_settings
_settings = get_settings()  # called inside _notify_counselor on every routing
```

`get_settings()` uses `@lru_cache`, so it won't reload from disk repeatedly. This is acceptable. However, the `from ... import` inside a hot function creates repeated module attribute lookups. This is a minor style issue — move imports to module level.

---

### 🟢 LOW-2 — Logging is too verbose in the production hot path

**File:** `chat.py:150-153`

```python
logger.info("\n" + "═" * 70)
logger.info(f"[STREAM] User: {user_id} | Session: ...")
logger.info(f"[USER]:  {req.message}")  # ← logs user message content
logger.info("═" * 70)
```

**Problem:** User message content is logged verbatim. This is a **privacy/PII concern** for a mental health application. Also, the `═ * 70` string is built on every message even if log level is INFO.

**Fix:** Redact or summarize message content in logs. Use `logger.isEnabledFor(logging.DEBUG)` guard for expensive log formatting.

---

### 🟢 LOW-3 — `background_tasks.py` uses bare `asyncio.sleep` in watchdog without jitter

**File:** `background_tasks.py:144`

```python
while True:
    await asyncio.sleep(60)
    expired_sessions = await get_expired_escalated_sessions(...)
```

**Problem:** Under multiple server instances, all instances wake up at exactly the same second and issue the same query simultaneously. This creates a "thundering herd" on MongoDB every 60 seconds.

**Fix:** Add random jitter:

```python
import random
await asyncio.sleep(60 + random.uniform(0, 10))
```

---

## 5. Scalability Review

### 🔴 HIGH-10 — In-memory `_ended_sessions` set grows unbounded

**File:** `connection_manager.py:23, 28-32`

```python
self._ended_sessions: set[str] = set()

def mark_session_ended(self, session_id: str) -> None:
    self._ended_sessions.add(session_id)  # never pruned
```

**Problem:** Session IDs are added to this set but **never removed**. Over days/weeks of uptime, this set grows to millions of entries and causes a steady memory leak. A server with 1M ended sessions would hold ~50MB of UUIDs in RAM for no purpose.

**Fix:** Move ended-session tracking to Redis with a TTL:

```python
async def mark_session_ended(self, session_id: str) -> None:
    redis = get_redis()
    if redis:
        await redis.setex(f"session:{session_id}:ended", 3600, "1")  # TTL 1 hour
    self._ended_sessions.add(session_id)  # keep local as fast cache
```

---

### 🔴 HIGH-11 — In-memory `timeout_tasks` and `pubsub_tasks` dicts are never pruned on crash

**File:** `connection_manager.py:22, 24`

If the server process restarts mid-session, the timeout tasks (asyncio.Task objects) and pubsub tasks are completely lost. The `_counselor_timeout_watchdog` never fires, so escalated sessions with no counselor are stuck in escalated state forever until the global 35-minute watchdog catches them.

**Recommendation:** This is partially mitigated by the stale lock cleanup in `routing_service.py`. But document this behavior clearly and consider persisting task metadata to Redis so a restarted instance can re-register watchdogs for in-flight sessions.

---

### 🟡 MEDIUM-11 — `get_available_counselor_count()` and `_find_available_counselor()` duplicate the same MongoDB query

**File:** `routing_service.py` and `chat.py:249`, `session_service.py:22`

Both functions build the same availability query. In the crisis detection flow, both are called sequentially:

1. `chat.py:249` calls `get_available_counselor_count()` — 1 DB round-trip
2. `route_crisis_session()` calls `_find_available_counselor()` — another DB round-trip with the same logic

**Fix:** Have `route_crisis_session` return a "no counselor available" result through its normal flow, eliminating the pre-check in `chat.py`. Or cache the count in Redis with a 2-second TTL.

---

### 🟡 MEDIUM-12 — `counselor_ws` dict in ConnectionManager is process-local only

**File:** `connection_manager.py:20`

```python
self.counselor_ws: dict[str, list[WebSocket]] = {}
```

**Problem:** This is in-process memory. In a horizontally scaled deployment (2+ Uvicorn instances), Instance A doesn't know about counselors connected to Instance B. The Redis Pub/Sub in `notify_counselor` addresses the delivery, but `notify_counselor` Tier 1 (local delivery) will never fire for cross-process connections.

**Status:** The current Redis Pub/Sub Tier 2 handles this correctly. This is an expected architectural trade-off. Document it explicitly.

---

## 6. Security & Reliability Risks

### 🔴 HIGH-12 — User message content is logged at INFO level

**File:** `chat.py:152`

```python
logger.info(f"[USER]:  {req.message}")
```

For a mental health application, this is a **serious privacy violation**. User messages may contain sensitive disclosures about suicide, abuse, trauma, and medical history. These are written to server logs which may be stored in plaintext.

**Fix:** Remove or hash user message content from INFO-level logs immediately. Log only metadata (turn count, message length, session ID).

---

### 🟡 MEDIUM-13 — JWT token validation errors in dashboard WS are silently downgraded

**File:** `websocket.py:98-102`

```python
except Exception:
    logger.warning("REJECTED | reason=invalid_token")
    # Treat as unauthenticated monitor — do not reject the connection
```

**Problem:** A counselor with an expired token is silently treated as an "anonymous monitor." They remain connected to the dashboard but are excluded from routing. This could lead to confusion ("Why am I not getting notifications?") or security issues.

**Fix:** Reject the connection with `WebSocket.close(code=4001)` if a token is present but invalid.

---

## 7. Recommended MongoDB Indexes (Complete Set)

Add all of these to `database.py:connect_to_mongo()`:

```python
# Routing compound index (replaces two partial indexes)
await db.admins.create_index([
    ("is_online", 1), ("is_active", 1),
    ("last_ping", -1), ("current_active_sessions", 1),
    ("checked_in_at", 1)
], name="counselor_routing_compound")

# Inactivity watchdog query
await db.sessions.create_index([("is_escalated", 1), ("updated_at", 1)])

# Escalated sessions dashboard query
await db.sessions.create_index([("is_escalated", 1), ("escalated_at", -1)])

# Session lookup by user (already exists but verify)
await db.sessions.create_index([("user_id", 1), ("created_at", -1)])

# Pending notification lookup on admin
await db.admins.create_index("pending_notification", sparse=True)
```

---

## 8. Priority-Wise Fix Summary

### 🔴 HIGH — Fix Immediately

| # | Issue | File | Impact |
|---|---|---|---|
| HIGH-1 | Double DB round-trip in `upsert_session` | `db_service.py` | 2× writes per message |
| HIGH-2 | Unbounded `to_list(length=None)` | `db_service.py` | Memory bomb |
| HIGH-3 | Missing routing compound index | `database.py` | Slow routing under load |
| HIGH-4 | Missing `updated_at` index for watchdog | `database.py` | Full scan every 60s |
| HIGH-5 | Double pending-notification delivery | `connection_manager.py` | Duplicate notifications |
| HIGH-6 | No timeout on WebSocket send | `connection_manager.py` | 30-90s hangs on bad network |
| HIGH-7 | No atomic `$inc` on counselor assignment | `routing_service.py` | Race condition, over-capacity |
| HIGH-9 | 100 messages loaded per chat message | `chat.py` | High DB + LLM cost |
| HIGH-10 | `_ended_sessions` set never pruned | `connection_manager.py` | Memory leak |
| HIGH-12 | User messages logged in plaintext | `chat.py` | Privacy / PII violation |

### 🟡 MEDIUM — Fix Before Scaling

| # | Issue | File | Impact |
|---|---|---|---|
| MEDIUM-1 | 2 DB writes per message | `db_service.py` | 2× write pressure |
| MEDIUM-2 | Full doc fetch for boolean check | `db_service.py` | Unnecessary data transfer |
| MEDIUM-5 | New OpenAI client per embedding call | `db_service.py` | Connection waste |
| MEDIUM-6 | No Pub/Sub reconnect loop | `connection_manager.py` | Silent notification death after Redis restart |
| MEDIUM-7 | No ping-pong on user chat WS | `websocket.py` | Ghost connections on mobile |
| MEDIUM-11 | Duplicate availability queries | `routing_service.py` + `chat.py` | 2× DB cost on crisis path |

### 🟢 LOW — Next Maintenance Window

| # | Issue | File | Impact |
|---|---|---|---|
| LOW-1 | No pagination on session/message lists | `db_service.py` | Risk at scale |
| LOW-2 | Verbose PII logging | `chat.py` | Privacy |
| LOW-3 | No jitter in watchdog sleep | `background_tasks.py` | Thundering herd |

---

## 9. Architecture Strengths (What's Already Good)

- ✅ Full async/await with Motor — no blocking I/O
- ✅ Redis Pub/Sub for multi-instance WebSocket delivery
- ✅ Routing lock (`__routing__`) prevents duplicate routing tasks per session
- ✅ Unique partial index on `doctor_user_assignments` prevents duplicate active assignments
- ✅ Stale lock cleanup prevents dead routing locks after crashes
- ✅ Guaranteed Delivery (persist-first notification) implemented
- ✅ Background embedding generation (non-blocking)
- ✅ Clinical handoff summaries as background tasks
- ✅ Counselor heartbeat and reconnect grace period
- ✅ Session-level routing lock acquired atomically via `find_one_and_update`
- ✅ FIFO + load-balanced counselor pool selection
