# Mental Health Chatbot Architecture

This document provides a comprehensive technical overview of the real-time chat system, specifically detailing the architecture for WebSocket communication, session handling, smart counselor assignment, escalation flows, and notifications.

## 1. Overall Project Architecture
The system employs a hybrid architecture combining stateless REST APIs for AI interactions and stateful WebSockets for real-time human counseling.

*   **Backend:** FastAPI using asynchronous handlers (`asyncio`).
*   **Database:** MongoDB (accessed via Motor for async I/O). Uses transactions for atomic assignments.
*   **AI Integration:** OpenAI (GPT-4o) for chat and summarization, HuggingFace (RoBERTa) for emotion analysis, and Groq for Whisper STT.
*   **Communication Flow:**
    *   **AI Mode:** User sends a message via HTTP POST (`/api/chat/stream`). The backend responds with Server-Sent Events (SSE) to stream the AI response token-by-token.
    *   **Human Mode (Escalation):** When a crisis is detected or a user manually escalates, the system transitions to WebSocket communication. Both the user and the assigned counselor connect to a shared WebSocket room to chat in real-time.

## 2. WebSocket Architecture
*   **When is the WebSocket connection opened?**
    *   **Counselor Dashboard:** A global notification WebSocket (`ws://host/api/human/escalated/ws`) opens when the counselor logs into their dashboard. This socket handles incoming escalation alerts and keeps the counselor marked as "online."
    *   **User Chat:** The user's app opens a WebSocket connection to `ws://.../api/human/chat/{session_id}?role=user` *immediately* after an escalation is triggered (either auto or manual).
    *   **Counselor Chat:** The counselor opens a separate WebSocket to the same URL (`?role=human_counselor`) only when they click "Accept" or enter a specific escalated chat session.
*   **Persistent vs. Per-Session:**
    *   Connections are **per-session**. A new WebSocket connection is established for every user-counselor session.
*   **Connection Basis:**
    *   The user and counselor are brought together based on the **`session_id`**. The WebSocket URL requires the session ID to route both parties to the correct virtual room.

## 3. Room & Session Management
*   **Chat Rooms:** Implemented via an in-memory `ConnectionManager`. It maintains a dictionary where the key is the `session_id` and the value is a list of active `WebSocket` objects.
*   **Identification:** The backend identifies messages by the `session_id` inherent to the WebSocket URL. It also maps each WebSocket object memory address to its specific role (`user` or `human_counselor`) to track who is currently active in the room.
*   **Isolation:** Since rooms are keyed by `session_id`, multiple concurrent chats run completely isolated from each other in memory.

## 4. Escalation Flow
*   **Auto-escalation:** On every user message, the system uses an NLP pipeline (RoBERTa) + an LLM Synthesizer (GPT-4o-mini) to detect crisis intent. If an active emergency or suicidal ideation is detected, `is_crisis` is set to `True`, automatically triggering escalation.
*   **Manual Escalation:** The user taps a "Connect to Counselor" button, which calls `POST /api/chat/manual-escalate`.
*   **Technical Process:**
    1.  The session document in MongoDB is updated (`is_escalated: True`).
    2.  The backend acquires an atomic routing lock (`__routing__`) to prevent duplicate routing tasks.
    3.  The `route_crisis_session` background task begins determining the best counselor.
    4.  An LLM background task generates a 600-word clinical handoff summary.
    5.  A real-time notification (`type: counselor_assigned`) is pushed via the global dashboard WebSocket to the selected counselor.
*   **Counselor Acceptance:** The counselor sees the notification on their dashboard, clicks "Accept," which opens a new frontend view that connects to the session-specific WebSocket room.

## 5. Smart Counselor Assignment Logic
The system uses a robust **3-Tier Routing Engine** to assign escalated sessions:
*   **Tier 1 — Sticky Routing:** The system checks the `doctor_user_assignments` collection to see if this user has a historically trusted counselor.
*   **Tier 2 — Context Match:** If a past counselor exists, the system verifies that the current crisis category (e.g., `suicidal_ideation`) is clinically compatible with what the counselor previously handled for this user.
*   **Tier 3 — Availability Gate:** The system confirms the preferred counselor meets ALL criteria:
    *   `is_online` is True.
    *   Heartbeat is fresh (pinged within the last 35 minutes).
    *   Has remaining capacity (`current_active_sessions < max_concurrent_sessions`).
*   **Fallback (Pool Search):** If the preferred counselor is unavailable or the user is new, the system falls back to a FIFO (First-In, First-Out) pool search, picking the least-loaded, longest-waiting available counselor based on `checked_in_at`.
*   **No Counselors Available:** If the entire pool is busy, the user receives an automated fallback message providing emergency hotline numbers (e.g., 911), and the session is returned to AI mode.

## 6. Notification System
*   **Real-time Push:** Notifications are sent directly through the counselor's global dashboard WebSocket (`/escalated/ws`).
*   **Targeted vs. Broadcast:** When the routing engine assigns a session, it sends a *targeted* push only to that specific counselor.
*   **Deferred Delivery:** If the assigned counselor is using a REST-polling dashboard (no active WS), the notification is saved to their database record (`pending_notification`). It is delivered instantly the next time they poll the API or open a socket.
*   **User Waiting Alert:** Once the Android user connects to the chat socket, a `user_waiting_in_room` alert is pushed to the counselor so they know the patient has arrived.

## 7. Multiple Concurrent Chats
Suppose User 1 is with Counselor 1, User 2 arrives for Counselor 2, and User 3 for Counselor 3.
*   **Are new sessions created?** Yes. Every distinct conversation has a unique `session_id` in MongoDB.
*   **Are separate rooms created?** Yes. The `ConnectionManager` creates a distinct list in memory for `session_id_1`, `session_id_2`, and `session_id_3`.
*   **Are new WebSocket connections opened?** Yes. User 2 and Counselor 2 will open two brand new WebSockets pointing specifically to `ws://.../chat/{session_id_2}`.
*   **How are they managed simultaneously?** FastAPI uses `asyncio`. When a message arrives on User 2's WebSocket, FastAPI handles that network event independently of User 1's socket. The `ConnectionManager` iterates only over the sockets within `session_id_2`'s room, ensuring no message bleed.

## 8. Detailed Technical Flow (Step-by-Step)
1.  **User Enters System:** User authenticates. JWT is issued.
2.  **Chat Initiation:** User sends a POST request to `/api/chat/stream`. The AI streams back a response via SSE. The conversation is logged in the `messages` collection under a unique `session_id`.
3.  **Escalation:** User types "I want to die." The `safety.py` synthesizer flags `is_crisis = True`. The SSE stream immediately terminates with a payload indicating an escalation is active, providing the WebSocket URL.
4.  **Counselor Assignment:** The `routing_service.py` runs transactionally. It finds the user's previous counselor (Tier 1), verifies they are online (Tier 3), and assigns them to the session document in MongoDB.
5.  **Notifications & Handoff:** A `counselor_assigned` WS message hits the counselor's dashboard. Concurrently, `summarization_service.py` uses GPT-4o to read the AI chat history and generates a clinical brief.
6.  **WebSocket Connection:**
    *   User connects to `ws://.../chat/{session_id}?role=user`.
    *   Counselor clicks accept and connects to `ws://.../chat/{session_id}?role=human_counselor`.
7.  **Real-time Messaging:** Messages are sent as JSON over the WebSockets. The `ConnectionManager` broadcasts incoming text to the other party in the room and saves the turn to MongoDB (`save_message`).
8.  **Session Ending:** Counselor clicks "End Session" (triggers `POST /api/human/escalated/{user_id}/close`). The system closes the WebSocket room with code `4001`, marks the escalation closed in DB, frees the counselor's capacity slot, and triggers a background task to generate a final post-session clinical summary. The user is seamlessly returned to the AI SSE endpoint.
