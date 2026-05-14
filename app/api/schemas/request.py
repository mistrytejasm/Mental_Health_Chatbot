from pydantic import BaseModel, Field, EmailStr, field_validator, model_validator
from typing import Optional
from enum import Enum


# ── Enums ─────────────────────────────────────────────────────────────────────

class GenderEnum(str, Enum):
    MALE = "male"
    FEMALE = "female"
    NON_BINARY = "non-binary"
    PREFER_NOT_TO_SAY = "prefer-not-to-say"

    @classmethod
    def _missing_(cls, value):
        # Accept underscore variants sent by older clients (e.g. "non_binary")
        if isinstance(value, str):
            normalised = value.replace("_", "-").lower()
            for member in cls:
                if member.value == normalised:
                    return member
        return None


class ProfessionalRole(str, Enum):
    PSYCHOLOGIST = "Licensed Psychologist (PhD / PsyD)"
    PSYCHIATRIST = "Psychiatrist (MD / DO)"
    LCSW = "Licensed Clinical Social Worker (LCSW)"
    LPC = "Licensed Professional Counselor (LPC)"
    LMFT = "Marriage & Family Therapist (LMFT)"
    BCBA = "Board Certified Behavior Analyst (BCBA)"
    OTHER = "Other (with specification)"
    NONE = "none"


class PracticeType(str, Enum):
    PRIVATE = "Private"
    CLINIC = "Clinic"
    TELEHEALTH = "Telehealth"
    NONE = "none"


class ConsultationMode(str, Enum):
    IN_PERSON = "In-person"
    TELEHEALTH = "Telehealth"
    NONE = "none"


# ── Assessment ────────────────────────────────────────────────────────────────

class ProfileInput(BaseModel):
    first_name: str = Field(..., min_length=1, max_length=40)
    last_name: Optional[str] = Field(default=None, max_length=40)
    gender: Optional[str] = None
    age: Optional[int] = Field(default=None, ge=1, le=120)


class PersonalityAnswers(BaseModel):
    prefers_solitude: str = "Sometimes"
    logic_over_emotion: str = "Sometimes"
    plans_ahead: str = "Sometimes"
    energized_by_social: str = "Sometimes"
    trusts_instincts: str = "Sometimes"


# ── Nested Registration (Mobile Compat) ───────────────────────────────────────

class RoleDefinition(BaseModel):
    is_user: bool

class RoleSection(BaseModel):
    definition: RoleDefinition

class CommonFields(BaseModel):
    first_name: str = Field(..., min_length=1, max_length=50, description="First name (1-50 characters)")
    last_name: str = Field(..., min_length=1, max_length=50, description="Last name (1-50 characters)")
    email: EmailStr = Field(..., description="Valid email address")
    password: str = Field(..., min_length=8, max_length=128, description="Password (8-128 characters)")
    gender: Optional[str] = None
    age: Optional[int] = Field(default=None, ge=13, le=150, description="Age must be between 13 and 150")
    phone_number: str = Field(..., pattern=r"^\+[1-9]\d{3,14}$", description="Phone number in E.164 format (e.g. +911234567890)")

    @field_validator("first_name", mode="before")
    @classmethod
    def validate_first_name(cls, v):
        if not v or not str(v).strip():
            raise ValueError("First name must not be empty")
        return str(v).strip()

    @field_validator("last_name", mode="before")
    @classmethod
    def validate_last_name(cls, v):
        if not v or not str(v).strip():
            raise ValueError("Last name must not be empty")
        return str(v).strip()

    @field_validator("password", mode="before")
    @classmethod
    def validate_password_strength(cls, v):
        import re
        if not v or len(str(v)) < 8:
            raise ValueError("Password must be at least 8 characters")
        if len(str(v)) > 128:
            raise ValueError("Password must not exceed 128 characters")
        pattern = re.compile(r"^(?=.*[A-Z])(?=.*\d)(?=.*[^A-Za-z0-9]).{8,128}$")
        if not pattern.match(str(v)):
            raise ValueError(
                "Password must include at least one uppercase letter, "
                "one number, and one special character"
            )
        return v

    @field_validator("gender", mode="before")
    @classmethod
    def validate_gender(cls, v):
        if v is None:
            return v
        # Normalise underscore variants (e.g. 'non_binary' → 'non-binary')
        normalised = str(v).replace("_", "-").lower().strip()
        _ALLOWED = {"male", "female", "non-binary", "prefer-not-to-say"}
        if normalised not in _ALLOWED:
            raise ValueError(
                f"Invalid gender '{v}'. "
                "Allowed values are: male, female, non-binary, prefer-not-to-say"
            )
        return normalised

class AdminRegistration(BaseModel):
    city: Optional[str] = None
    state: Optional[str] = None
    npi_number: Optional[str] = None
    professional_role: Optional[str] = None
    license_number: Optional[str] = None
    state_of_licensure: Optional[str] = None
    practice_type: Optional[str] = None
    consultation_mode: Optional[str] = None

class NestedRegisterPayload(BaseModel):
    role: RoleSection
    common_fields: CommonFields
    admin_registration: AdminRegistration


class AssessmentRequest(BaseModel):
    """POST /api/assessment — one-time onboarding from Android."""
    personality_answers: PersonalityAnswers


# ── Registration ──────────────────────────────────────────────────────────────

class UserCreateRequest(BaseModel):
    """
    FC3: is_admin removed — role determined solely by is_user.
    FC4: full_name replaced by first_name + last_name.
    FC5: gender and age added as required fields.
    FC6: model_validator enforces role-specific required fields.
    """
    first_name: str = Field(..., min_length=1, max_length=50)
    last_name: str = Field(..., min_length=1, max_length=50)
    email: EmailStr
    password: str = Field(..., min_length=8, max_length=128)
    phone_number: str = Field(..., pattern=r"^\+[1-9]\d{3,14}$")
    # True = patient (users collection), False = counselor/admin (admins collection)
    is_user: bool = Field(..., description="True for patient, False for counselor/admin")
    gender: GenderEnum
    age: int = Field(..., ge=13, le=150)

    @field_validator("gender", mode="before")
    @classmethod
    def normalise_gender(cls, v):
        """
        Pydantic v2 does not reliably call the _missing_ classmethod on enums.
        This validator normalises underscore variants before enum coercion so that
        both 'non_binary' and 'non-binary' are accepted, and provides a clear
        human-readable error listing all valid choices.
        """
        if v is None:
            raise ValueError("Gender is required")
        normalised = str(v).replace("_", "-").lower().strip()
        _VALID = {m.value for m in GenderEnum}
        if normalised not in _VALID:
            raise ValueError(
                f"Invalid gender '{v}'. "
                "Accepted values: male, female, non-binary, prefer-not-to-say"
            )
        return normalised

    # Counselor-specific fields
    professional_role: Optional[str] = None
    license_number: Optional[str] = None
    state_of_licensure: Optional[str] = None
    npi_number: Optional[str] = None
    practice_type: Optional[str] = None
    city: Optional[str] = None
    state: Optional[str] = None
    consultation_mode: Optional[str] = None

    @model_validator(mode="after")
    def validate_role_fields(self) -> "UserCreateRequest":
        """FC6: enforce role-specific required fields at schema boundary."""
        if not self.is_user:
            # Counselor: professional credentials are required for compliance
            required_counselor = [
                "professional_role", "license_number", "state_of_licensure",
                "npi_number", "practice_type", "city", "state", "consultation_mode",
            ]
            missing = [f for f in required_counselor if not getattr(self, f)]
            if missing:
                raise ValueError(
                    f"Counselor registration requires: {', '.join(missing)}"
                )
        return self

    model_config = {
        "json_schema_extra": {
            "examples": [
                
                    {
                        "first_name": "Dr. Sarah",
                        "last_name": "Smith",
                        "email": "sarah@clinic.com",
                        "password": "Abcd@1234",
                        "phone_number": "+911234567891",
                        "is_user": False,
                        "gender": "female",
                        "age": 42,
                        "professional_role": "Licensed Psychologist (PhD / PsyD)",
                        "license_number": "PSY12345",
                        "state_of_licensure": "California",
                        "npi_number": "1234567890",
                        "practice_type": "Private",
                        "city": "Los Angeles",
                        "state": "CA",
                        "consultation_mode": "In-person"
                    
                }
            ]
        }
    }


# ── Auth ──────────────────────────────────────────────────────────────────────

class UserLoginRequest(BaseModel):
    email: str = Field(..., min_length=1)
    password: str = Field(..., min_length=1)

    model_config = {
        "json_schema_extra": {
            "example": {
                "email": "jane@example.com",
                "password": "Abcd@1234"
            }
        }
    }


class ForgotPasswordRequest(BaseModel):
    email: str = Field(..., pattern=r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$")

    model_config = {
        "json_schema_extra": {
            "example": {"email": "jane@example.com"}
        }
    }


class VerifyOtpRequest(BaseModel):
    email: str = Field(..., description="User email address")
    otp: str = Field(..., description="6-digit OTP")

    model_config = {
        "json_schema_extra": {
            "example": {"email": "jane@example.com", "otp": "123456"}
        }
    }


class ResetPasswordRequest(BaseModel):
    email: str = Field(..., description="User email address")
    new_password: str = Field(
        ...,
        min_length=8,
        max_length=128,
        json_schema_extra={
            "pattern": r"^(?=.*[A-Z])(?=.*\d)(?=.*[^A-Za-z0-9]).{8,128}$"
        }
    )

    model_config = {
        "json_schema_extra": {
            "example": {"email": "jane@example.com", "new_password": "NewPass@1234"}
        }
    }


class RefreshTokenRequest(BaseModel):
    """POST /api/users/refresh — exchange a refresh token for a new access token."""
    refresh_token: str = Field(..., min_length=1)


# ── Chat ──────────────────────────────────────────────────────────────────────

class StreamChatRequest(BaseModel):
    """
    POST /api/chat/stream — every chat message from Android.
    FC7: user_id removed — identity is extracted exclusively from the JWT token.
    """
    session_id: str = Field(..., min_length=1)
    message: str = Field(..., min_length=1, max_length=2000)


# ── Human Intervention ────────────────────────────────────────────────────────

class CheckinCheckoutRequest(BaseModel):
    is_online: bool
