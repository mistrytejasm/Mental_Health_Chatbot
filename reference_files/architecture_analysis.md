# Real-Time WebSocket Chat Architecture Analysis

Based on a thorough review of the codebase (specifically `ARCHITECTURE.md`, `human.py`, and `routing_service.py`), here is the detailed breakdown of how your real-time chat system operates and how it handles production-scale scenarios.

---

## 1. Production-Scale Test Scenarios

Your current architecture elegantly supports your mentioned test scenarios:

*   **Single User ↔ Multiple Counselors:** 
    A single session (`session_id`) is strictly bound to *one* assigned counselor via the `assigned_counselor_id` in the database. However, if a user disconnects, times out, or triggers a new escalation later, the routing engine can seamlessly assign them to a different counselor.
*   **Multiple Users ↔ Single Counselor:** 
    Fully supported. A counselor can be assigned multiple active users up to their `max_concurrent_sessions` limit (default is 3). For each user, the counselor's dashboard opens a *separate* WebSocket connection dedicated to that specific `session_id`.
*   **Multiple Users ↔ Multiple Counselors:** 
    Fully supported. The system isolates traffic using an in-memory dictionary. Dozens of concurrent pairs can chat simultaneously without messages bleeding into the wrong chat.
*   **Continuous Real-Time Chat (Dynamic Pairs):** 
    Fully supported. The system seamlessly transitions a user from an AI HTTP stream to a live WebSocket connection the moment a crisis is detected. The `routing_service.py` dynamically pairs them with an available counselor in real-time.

---

## 2. Answers to Specific Clarifications

### How are users and counselors connected? What unique identifiers are used?
They are connected via isolated WebSocket URLs built using the **`session_id`** as the unique identifier. 
Both the user and the counselor connect to the exact same endpoint:
`ws://<host>/api/human/chat/{session_id}`
The server distinguishes who is who via a query parameter (`?role=user` vs `?role=human_counselor`).

### Do they communicate through specific rooms/channels?
**Yes.** The system uses a "room" concept. Once connected, both WebSockets are appended to a list under the `session_id` key. When one party sends a message, the server broadcasts it to the other socket in that specific list.

### How is the next available counselor assigned using FIFO logic?
When a new user triggers an escalation, `routing_service.py` handles assignment. If the user doesn't have a preferred counselor, it falls back to a **Pool Search**.
The system queries the database for counselors who are:
1. Online (`is_online = True`)
2. Have a fresh heartbeat (pinged within the last 35 minutes)
3. Have remaining capacity (`current_active_sessions < max_concurrent_sessions`)

**FIFO Logic:** It sorts these available counselors by `checked_in_at` ascending. This guarantees that the counselor who has been waiting the longest for a chat gets assigned first.

### Once assigned, how do they get connected together?
1. The user's app immediately receives the WebSocket URL and connects.
2. The server sends a real-time push notification (`type: counselor_assigned`) containing the `session_id` and URL to the assigned counselor's "Global Dashboard" WebSocket.
3. The counselor clicks "Accept" on their UI.
4. The counselor's UI opens a new, session-specific WebSocket to the provided URL. They are now in the same logical room as the user.

### Is there a specific in-memory data structure used to manage active chat pairs?
**Yes.** In `app/api/routes/human.py`, there is a singleton class called `ConnectionManager`. It manages state using Python dictionaries:
```python
class ConnectionManager:
    def __init__(self):
        self.rooms: dict[str, list[WebSocket]] = {} # session_id -> [user_ws, counselor_ws]
        self.ws_roles: dict[int, str] = {}          # Tracks who is 'user' vs 'human_counselor'
        self.has_human: dict[str, bool] = {}        # Quick lookup for counselor presence
```

### Does each pair use a separate WebSocket connection, or do all users share a single server connection?
**Separate connections.** Every active chat session involves *two distinct WebSocket connections* to the server (one from the user's device, one from the counselor's browser). They do not share a single multiplexed connection for chat messages. 
*(Note: The counselor does have one persistent "dashboard" connection to receive incoming alerts, but actual chat traffic happens on separate, dedicated sockets).*

---

## 3. Is this the correct and scalable approach?

### The Good:
*   **Correctness:** Yes, this is the standard, highly-performant way to build WebSockets in FastAPI. Using `asyncio` and an in-memory dictionary allows for thousands of concurrent connections with extremely low latency.
*   **Clean Isolation:** Managing rooms via `session_id` prevents data leakage between sensitive therapy sessions.

### The Scalability Caveat (Important for Production):
Because your `ConnectionManager` stores rooms in **server memory (RAM)**, this architecture is currently limited to **Vertical Scaling** (running on a single, powerful server). 

If your user base grows to the point where you need to run multiple servers behind a load balancer (Horizontal Scaling), this architecture will break. 
*Why?* If the user connects to Server A, and the counselor connects to Server B, they will be in different memory spaces and won't be able to chat.

**How to make it horizontally scalable for massive production:**
When you reach the limits of a single server, you must replace the in-memory `self.rooms = {}` dictionary with a **Pub/Sub system like Redis** (using a library like `broadcaster`). 
In that architecture, Server A and Server B both publish messages to a central Redis channel (`channel:session_id`), allowing the user and counselor to chat regardless of which physical server they connected to.

For early to mid-stage production, your current single-server in-memory approach is completely fine and very fast.
