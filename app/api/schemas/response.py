from pydantic import BaseModel
from typing import Optional, Union
from datetime import datetime


# ── Assessment response ───────────────────────────────────────────────────────

class AssessmentResponse(BaseModel):
    status: str
    session_id: str
    opening_message: str
    timestamp: datetime
    user_id: str


# ── Chat stream metadata ──────────────────────────────────────────────────────

class EmotionData(BaseModel):
    dominant_emotion: str
    response_mode: str
    message_class: str
    intensity: str
    recommended_tone: str
    is_crisis_signal: bool
    sadness_scores: list[float] = []


# ── Chat history API ──────────────────────────────────────────────────────────

class ChatMessageResponse(BaseModel):
    user_id: str
    role: str
    content: str
    timestamp: datetime


class ChatHistoryResponse(BaseModel):
    status: str
    user_id: str
    total_messages: int
    messages: list[ChatMessageResponse]


# ── Session list API ──────────────────────────────────────────────────────────

class SessionResponse(BaseModel):
    session_id: str
    user_id: str
    is_active: bool
    is_escalated: bool
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


class SessionListResponse(BaseModel):
    status: str
    user_id: str
    total_sessions: int
    sessions: list[SessionResponse]


# ── Human intervention API ────────────────────────────────────────────────────

class EscalatedSessionResponse(BaseModel):
    session_id: str
    user_id: str
    first_name: str = "Unknown"
    last_name: Optional[str] = None
    is_active: bool = True
    is_escalated: bool = True
    lethality_alert: bool = False
    escalated_at: Optional[datetime] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    assigned_counselor_id: Optional[str] = None
    counselor_first_name: Optional[str] = None
    counselor_last_name: Optional[str] = None
    doctor_id: Union[str, bool] = False


class EscalatedSessionListResponse(BaseModel):
    status: str
    total: int
    sessions: list[EscalatedSessionResponse]


# ── Health / utility ──────────────────────────────────────────────────────────

class TranscriptionResponse(BaseModel):
    text: str
    language: Optional[str] = None
    duration: Optional[float] = None


# ── Token / Auth ──────────────────────────────────────────────────────────────

class TokenData(BaseModel):
    user_id: str
    email: str
    role: Optional[str] = None


# ── User Profile ──────────────────────────────────────────────────────────────

class UserProfileData(BaseModel):
    user_id: str
    # FC4: first_name + last_name are primary; full_name kept for backward compat
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    full_name: Optional[str] = None
    email: str
    phone_number: str
    # FC3: is_admin derived at response-build time, not stored in DB
    is_user: bool = True
    is_admin: bool = False
    # FC5: demographic fields
    gender: Optional[str] = None
    age: Optional[int] = None
    professional_role: Optional[str] = None
    license_number: Optional[str] = None
    state_of_licensure: Optional[str] = None
    npi_number: Optional[str] = None
    practice_type: Optional[str] = None
    city: Optional[str] = None
    state: Optional[str] = None
    consultation_mode: Optional[str] = None
    created_at: Optional[datetime] = None
    last_login: Optional[datetime] = None

    model_config = {
        "json_schema_extra": {
            "example": {
                "user_id": "12345",
                "first_name": "Jane",
                "last_name": "Doe",
                "full_name": "Jane Doe",
                "email": "jane@example.com",
                "phone_number": "+911234567890",
                "is_user": True,
                "is_admin": False,
                "gender": "female",
                "age": 28,
            }
        }
    }


# ── Auth Responses ────────────────────────────────────────────────────────────

class UserSignupResponse(BaseModel):
    """FC1: registration returns tokens so the client is immediately authenticated."""
    status: str
    message: str
    user: UserProfileData
    access_token: str
    refresh_token: str
    token_type: str = "bearer"


class UserLoginResponse(BaseModel):
    status: str
    message: str
    user: UserProfileData
    access_token: str
    refresh_token: str
    token_type: str = "bearer"
    assessment_completed: bool = False
    session_id: Optional[str] = None
    welcome_message: Optional[str] = None


class ForgotPasswordResponse(BaseModel):
    status: str
    message: str


class VerifyOtpResponse(BaseModel):
    status: str
    message: str


class ResetPasswordResponse(BaseModel):
    status: str
    message: str


class RefreshTokenResponse(BaseModel):
    status: str
    access_token: str
    refresh_token: str
    token_type: str = "bearer"


class LogoutResponse(BaseModel):
    status: str
    message: str


class UserProfileResponse(BaseModel):
    status: str
    user: UserProfileData


class CheckinCheckoutResponse(BaseModel):
    status: str
    message: str


class ManualEscalateResponse(BaseModel):
    status: str
    message: Optional[str] = None


class CounselorStatusResponse(BaseModel):
    status: str
    is_checked: bool


class WebSocketStatusResponse(BaseModel):
    status: str
    session_id: str
    is_socket_connected: bool
    is_user_connected: bool
    is_counselor_connected: bool
