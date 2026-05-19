from typing import Optional, Any
from pydantic import BaseModel, Field
from datetime import datetime, timezone

class BaseWSEvent(BaseModel):
    type: str
    timestamp: str = Field(default_factory=lambda: datetime.now(timezone.utc).isoformat())

# ─── Incoming Events ────────────────────────────────────────────────────────
class IncomingMessageEvent(BaseModel):
    type: str = "message"
    text: str

class IncomingPingEvent(BaseModel):
    type: str = "ping"

# ─── Outgoing Events (Chat) ─────────────────────────────────────────────────
class OutgoingMessageEvent(BaseWSEvent):
    type: str = "message"
    role: str
    counselor_name: Optional[str] = None
    text: str
    is_human: bool = False
    done: bool = True

class SystemNoticeEvent(BaseWSEvent):
    type: str = "system_notice"
    role: str = "system"
    counselor_name: Optional[str] = None
    text: str
    is_human: bool = False
    is_system: bool = True

class SessionEndedEvent(BaseWSEvent):
    type: str = "session_ended"
    role: str = "system"
    text: str
    is_system: bool = True

class SystemHandoffBriefEvent(BaseModel):
    type: str = "system_handoff_brief"
    content: str
    crisis_category: str = "unknown"
    summary_ready: bool

class UserDisconnectedEvent(BaseWSEvent):
    type: str = "user_disconnected"
    role: str = "system"
    text: str = "The user has disconnected from the session."
    is_human: bool = False
    is_system: bool = True

# ─── Outgoing Events (Dashboard) ────────────────────────────────────────────
class SessionClaimedEvent(BaseWSEvent):
    type: str = "session_claimed"
    session_id: str
    counselor_id: str

class DashboardNotificationEvent(BaseWSEvent):
    type: str = "pending_notification"
    session_id: str
    # other fields are dynamic based on the routing service
    payload: dict[str, Any]
