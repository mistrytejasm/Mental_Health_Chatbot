from pydantic import BaseModel, Field
from typing import Optional, Dict, Any
from datetime import datetime


class EmergencyContact(BaseModel):
    name: Optional[str] = None
    relation: Optional[str] = None
    phone: Optional[str] = None


class UserModelDB(BaseModel):
    user_id: Optional[str] = None

    # FC4: first_name + last_name replace full_name for new registrations.
    # full_name kept as Optional for backward compatibility with existing documents.
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    full_name: Optional[str] = None  # legacy field — do not write on new registrations

    # FC5: demographic fields
    gender: Optional[str] = None
    age: Optional[int] = None

    name: Optional[str] = None
    emergency_contact: Optional[EmergencyContact] = None
    personality_answers: Optional[Dict[str, str]] = None
    personality_summary: Optional[str] = None

    # ── Authentication fields ─────────────────────────────────────────────────
    email: Optional[str] = None
    password_hash: Optional[str] = None
    phone_number: Optional[str] = None
    # FC3: is_admin removed — role is determined by which collection the doc lives in.
    is_user: bool = True
    is_active: bool = True
    last_login: Optional[datetime] = None
    password_reset_token: Optional[str] = None
    password_reset_expires: Optional[datetime] = None

    # ── Smart Routing ─────────────────────────────────────────────────────────
    preferred_counselor_id: Optional[str] = None
    last_crisis_category: Optional[str] = None

    created_at: datetime = Field(default_factory=datetime.utcnow)
    last_active: datetime = Field(default_factory=datetime.utcnow)


class AdminModelDB(BaseModel):
    user_id: str = ""
    # FC4: first_name + last_name; full_name kept for backward compat
    first_name: str = ""
    last_name: str = ""
    full_name: Optional[str] = None  # legacy
    email: str
    password_hash: str
    phone_number: str
    role: str = "admin"
    # FC5: demographic fields
    gender: Optional[str] = None
    age: Optional[int] = None
    # Professional credentials
    professional_role: str = ""
    license_number: str = ""
    state_of_licensure: str = ""
    npi_number: str = ""
    practice_type: str = ""
    city: str = ""
    state: str = ""
    consultation_mode: str = ""
    # ── Smart Routing — Presence & Capacity ──────────────────────────────────
    is_online: bool = False
    current_active_sessions: int = 0
    max_concurrent_sessions: int = 3
    last_ping: Optional[datetime] = None

    created_at: datetime = Field(default_factory=datetime.utcnow)
    last_login: Optional[datetime] = None


class SessionModelDB(BaseModel):
    session_id: str
    user_id: str = ""
    doctor_id: Optional[str] = None
    started_at: datetime = Field(default_factory=datetime.utcnow)
    ended_at: Optional[datetime] = None
    status: str = "active"
    summary: Optional[str] = None

    # ── Smart Routing ─────────────────────────────────────────────────────────
    assigned_counselor_id: Optional[str] = None
    crisis_category: Optional[str] = None
    handoff_summary: Optional[str] = None
    # Fix 3: written atomically with the __routing__ lock for stale-lock detection
    routing_started_at: Optional[datetime] = None
    assigned_at: Optional[datetime] = None
    assignment_complete: bool = False


class DoctorUserAssignmentDB(BaseModel):
    """
    Doctor-User Assignment System
    ──────────────────────────────
    Bridge table between the users and admins (doctors/counselors) collections.
    Maintains a complete history of all crisis assignments.

    Rules:
      - One user can have MULTIPLE records but only ONE active record at any time.
      - Old records are never deleted, only marked inactive (preserved as history).
      - A unique partial index on (user_id + status="active") prevents duplicates.
    """
    user_id: str
    doctor_id: str
    assigned_at: datetime = Field(default_factory=datetime.utcnow)
    status: str = Field(default="active", description="'active' or 'inactive'")


class RobertaAnalysis(BaseModel):
    dominant_emotion: str
    scores: Dict[str, float] = Field(default_factory=dict)


class LLMConsensus(BaseModel):
    category: str
    intensity: str
    message_class: str
    is_crisis: bool
    recommended_tone: str
    reasoning: str


class MessageModelDB(BaseModel):
    session_id: str
    sender_type: str = "user"
    sender_id: str = ""
    turn_number: int = 1
    role: str
    content: str
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    roberta_analysis: Optional[RobertaAnalysis] = None
    llm_consensus: Optional[LLMConsensus] = None
    tokens_used: Optional[int] = None
