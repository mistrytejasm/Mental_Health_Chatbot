"""
User Routes
-----------
POST /api/users/register
POST /api/users/login
POST /api/users/forgot-password
POST /api/users/verify-otp
POST /api/users/reset-password
POST /api/users/refresh
POST /api/users/logout
"""

import re
import uuid
from typing import Literal
from fastapi import APIRouter, HTTPException, Depends, Request
from fastapi.concurrency import run_in_threadpool
from fastapi.responses import JSONResponse
from datetime import datetime, timedelta, timezone
from pydantic import ValidationError
from app.core.auth.oauth2 import get_current_user
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials


token_auth_scheme = HTTPBearer()

from app.api.schemas.request import (
    VerifyOtpRequest,
    ResetPasswordRequest,
    RefreshTokenRequest,
    ForgotPasswordRequest,
    UserCreateRequest,
    NestedRegisterPayload,
    UserLoginRequest,
)
from app.api.schemas.response import (
    UserSignupResponse,
    UserLoginResponse,
    ForgotPasswordResponse,
    VerifyOtpResponse,
    RefreshTokenResponse,
    LogoutResponse,
    ResetPasswordResponse,
    UserProfileData,
    UserProfileResponse,
)
from app.models.db import UserModelDB, AdminModelDB
from bson import ObjectId
from app.core.database import get_database
from app.core.logger import get_logger
from app.core.config import get_settings
from app.core.auth.hashing import Hash
from app.core.auth.password_policy import validate_password
from app.core.auth.jwt_handler import (
    create_access_token,
    create_refresh_token,
    verify_refresh_token,
)
from app.core.auth.token_blacklist import add_to_blacklist
from app.services.email_service import generate_otp, validate_email, send_otp_email
from app.services.db_service import get_existing_session, upsert_session
from app.services import llm as llm_svc


logger = get_logger(__name__)
router = APIRouter(prefix="/api/users", tags=["users"])


# ── Helpers ───────────────────────────────────────────────────────────────────

def detect_identifier_type(identifier: str) -> Literal["email", "phone"]:
    if re.match(r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$", identifier):
        return "email"
    if re.match(r"^\+?[1-9]\d{1,14}$", identifier):
        return "phone"
    raise ValueError(f"Invalid identifier format: {identifier}")


async def find_user_by_identifier(db, identifier: str):
    """
    Searches users then admins by email or phone.
    Tags the returned doc with _source_collection so callers can determine
    which collection to update without relying on the is_admin field (FC3).
    """
    identifier = identifier.strip().lower()
    identifier_type = detect_identifier_type(identifier)
    query = {"email": identifier} if identifier_type == "email" else {"phone_number": identifier}

    user_doc = await db.users.find_one(query)
    if user_doc:
        user_doc["_source_collection"] = "users"
        return user_doc

    user_doc = await db.admins.find_one(query)
    if user_doc:
        user_doc["_source_collection"] = "admins"
        return user_doc

    return None


def _resolve_collection(db, user_doc: dict):
    """Returns the Motor collection the user_doc came from."""
    src = user_doc.get("_source_collection")
    # Fallback to legacy is_admin field for docs loaded outside find_user_by_identifier
    if src == "admins" or user_doc.get("is_admin", False):
        return db.admins
    return db.users


def _build_profile_data(user_doc: dict, user_id: str) -> UserProfileData:
    """Builds UserProfileData from a DB document, handling both old and new schema."""
    is_admin = (
        user_doc.get("_source_collection") == "admins"
        or user_doc.get("is_admin", False)
    )
    first_name = user_doc.get("first_name")
    last_name = user_doc.get("last_name")
    # Derive full_name for backward compat
    if first_name and last_name:
        full_name = f"{first_name} {last_name}".strip()
    elif first_name:
        full_name = first_name
    else:
        full_name = user_doc.get("full_name")

    return UserProfileData(
        user_id=user_id,
        first_name=first_name,
        last_name=last_name,
        full_name=full_name,
        email=user_doc.get("email", ""),
        phone_number=user_doc.get("phone_number", ""),
        is_user=not is_admin,
        is_admin=is_admin,
        gender=user_doc.get("gender"),
        age=user_doc.get("age"),
        professional_role=user_doc.get("professional_role"),
        license_number=user_doc.get("license_number"),
        state_of_licensure=user_doc.get("state_of_licensure"),
        npi_number=user_doc.get("npi_number"),
        practice_type=user_doc.get("practice_type"),
        city=user_doc.get("city"),
        state=user_doc.get("state"),
        consultation_mode=user_doc.get("consultation_mode"),
        created_at=user_doc.get("created_at"),
        last_login=user_doc.get("last_login"),
    )


async def _generate_login_greeting(first_name: str, personality_summary: str) -> str:
    """
    Generates a short, warm, personalised welcome-back greeting for a returning
    user at login time.  Uses the LLM so the tone matches the user's personality.
    Falls back to a plain string if the LLM call fails.
    """
    profile = {
        "name": first_name,
        "personality_summary": personality_summary,
    }
    prompt = (
        f"Write a single short sentence (max 15 words) welcoming {first_name} back "
        "to MindBridge. Use their name. Sound warm and genuine, not generic. "
        "No sign-off, no questions, no emojis. Plain text only."
    )
    try:
        from openai import AsyncOpenAI
        from app.core.config import get_settings as _gs
        _settings = _gs()
        client = AsyncOpenAI(api_key=_settings.OPENAI_API_KEY)
        resp = await client.chat.completions.create(
            model=_settings.MAIN_MODEL,
            messages=[
                {"role": "system", "content": f"You are MindBridge, a warm mental health companion. Personality context for {first_name}: {personality_summary}"},
                {"role": "user", "content": prompt},
            ],
            max_tokens=40,
            temperature=0.7,
        )
        text = resp.choices[0].message.content.strip().strip('"')
        if text:
            return text
    except Exception as e:
        logger.warning(f"Login greeting LLM call failed: {e}")
    return f"Welcome back, {first_name}! Good to have you here again."


# ── Routes ────────────────────────────────────────────────────────────────────

@router.post("/register/mobile", response_model=UserSignupResponse)
async def mobile_register(payload: NestedRegisterPayload):
    """
    Mobile-specific registration endpoint.
    Accepts the nested JSON structure required by the mobile team,
    flattens it, and passes it to the standard validation schema.
    """
    flat_data = {
        "is_user": payload.role.definition.is_user,
        "first_name": payload.common_fields.first_name,
        "last_name": payload.common_fields.last_name,
        "email": payload.common_fields.email,
        "password": payload.common_fields.password,
        "gender": payload.common_fields.gender,
        "age": payload.common_fields.age,
        "phone_number": payload.common_fields.phone_number,
    }

    if not flat_data["is_user"]:
        flat_data.update({
            "city": payload.admin_registration.city,
            "state": payload.admin_registration.state,
            "npi_number": payload.admin_registration.npi_number,
            "professional_role": payload.admin_registration.professional_role,
            "license_number": payload.admin_registration.license_number,
            "state_of_licensure": payload.admin_registration.state_of_licensure,
            "practice_type": payload.admin_registration.practice_type,
            "consultation_mode": payload.admin_registration.consultation_mode,
        })

    try:
        # Validate through the standard Pydantic schema.
        # Pydantic v2 raises ValidationError (not ValueError) on model construction.
        validated_request = UserCreateRequest(**flat_data)
    except ValidationError as e:
        # Translate Pydantic errors into the same structured format as the
        # global RequestValidationError handler so mobile clients always
        # receive consistent, field-specific error messages.
        # Return only the first error so the Android app can show one
        # validation message at a time and guide the user field by field.
        all_errs = e.errors()
        first = all_errs[0] if all_errs else {}
        loc_parts = [str(p) for p in first.get("loc", [])]
        field = loc_parts[-1] if loc_parts else "unknown"
        error_message = first.get("msg", "Invalid value")
        return JSONResponse(
            status_code=400,
            content={
                "error_code": "VALIDATION_ERROR",
                "message": error_message,
                "details": [
                    {
                        "field": field,
                        "message": error_message,
                        "type": first.get("type", "value_error"),
                    }
                ],
            },
        )

    # Delegate to the main standard registration logic
    return await user_register(validated_request)


@router.post("/register", response_model=UserSignupResponse)
async def user_register(payload: UserCreateRequest):
    """
    Register a new user or counselor.
    FC1: returns access_token + refresh_token so the client is immediately authenticated.
    FC3: role determined by is_user only (is_admin removed).
    FC4: accepts first_name + last_name instead of full_name.
    FC5: accepts gender and age.
    FC6: role-specific required fields enforced by model_validator on UserCreateRequest.
    """
    try:
        masked_email = payload.email[:2] + "***" + payload.email.split("@")[1] if "@" in payload.email else "***"
        logger.info(f"Registration attempt | Email: {masked_email}")

        db = get_database()
        if db is None:
            raise HTTPException(status_code=503, detail="Database connection failed. Please try again later.")

        email = payload.email.strip().lower()
        phone_number = payload.phone_number.strip()
        
        first_name = payload.first_name.strip()
        last_name = payload.last_name.strip()
        full_name = f"{first_name} {last_name}".strip()

        # Duplicate check across both collections
        existing = await db.users.find_one({"$or": [{"email": email}, {"phone_number": phone_number}]})
        if not existing:
            existing = await db.admins.find_one({"$or": [{"email": email}, {"phone_number": phone_number}]})
        if existing:
            if existing.get("email") == email:
                raise HTTPException(status_code=400, detail="Email already registered")
            raise HTTPException(status_code=400, detail="Phone number already registered")

        validate_password(payload.password)
        password_hash = await run_in_threadpool(Hash.bcrypt, payload.password)

        doc = {
            "first_name": first_name,
            "last_name": last_name,
            "full_name": full_name,
            "email": email,
            "phone_number": phone_number,
            "password_hash": password_hash,
            "is_user": payload.is_user,
            "gender": payload.gender.value if payload.gender else None,
            "age": payload.age,
            "is_active": True,
            "created_at": datetime.now(timezone.utc),
            "last_active": datetime.now(timezone.utc),
        }

        if payload.is_user:
            result = await db.users.insert_one(doc)
            role_log = "user"
        else:
            # Counselor/admin — store professional credentials
            doc.update({
                "professional_role": payload.professional_role,
                "license_number": payload.license_number,
                "state_of_licensure": payload.state_of_licensure,
                "npi_number": payload.npi_number,
                "practice_type": payload.practice_type,
                "city": payload.city,
                "state": payload.state,
                "consultation_mode": payload.consultation_mode,
                # Routing presence fields — populated on first WebSocket connect
                "is_online": False,
                "current_active_sessions": 0,
                "max_concurrent_sessions": 3,
            })
            result = await db.admins.insert_one(doc)
            role_log = "admin"

        user_id = str(result.inserted_id)
        logger.info(f"User Registered: {masked_email} | Role: {role_log}")

        # FC1: issue tokens immediately after registration
        token_subject = email
        user_role = role_log
        access_token = create_access_token(data={"sub": token_subject, "role": user_role, "user_id": user_id})
        refresh_token = create_refresh_token(data={"sub": token_subject, "role": user_role, "user_id": user_id})

        user_data = UserProfileData(
            user_id=user_id,
            first_name=first_name,
            last_name=last_name,
            full_name=full_name,
            email=email,
            phone_number=phone_number,
            is_user=payload.is_user,
            is_admin=not payload.is_user,
            gender=payload.gender.value if payload.gender else None,
            age=payload.age,
            professional_role=payload.professional_role,
            license_number=payload.license_number,
            state_of_licensure=payload.state_of_licensure,
            npi_number=payload.npi_number,
            practice_type=payload.practice_type,
            city=payload.city,
            state=payload.state,
            consultation_mode=payload.consultation_mode,
        )

        return UserSignupResponse(
            status="success",
            message="Registration successful",
            user=user_data,
            access_token=access_token,
            refresh_token=refresh_token,
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"event=register_failed error={str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


@router.post("/login", response_model=UserLoginResponse)
async def user_login(payload: UserLoginRequest):
    """Login using email or phone number."""
    try:
        masked_email = payload.email[:2] + "***" + payload.email.split("@")[1] if "@" in payload.email else "***"
        logger.info(f"Login attempt | Email: {masked_email}")

        db = get_database()
        if db is None:
            raise HTTPException(status_code=503, detail="Database connection failed.")

        login_identifier = payload.email.strip().lower()
        user_doc = await find_user_by_identifier(db, login_identifier)
        if not user_doc:
            raise HTTPException(status_code=401, detail="Invalid credentials")

        stored_hash = user_doc.get("password_hash")
        if not stored_hash or not await run_in_threadpool(Hash.checkpw, payload.password, stored_hash):
            raise HTTPException(status_code=401, detail="Invalid credentials")

        if not user_doc.get("is_active", True):
            raise HTTPException(status_code=403, detail="Account is disabled. Please contact support.")

        if "_id" not in user_doc:
            raise HTTPException(status_code=500, detail="User data corrupted")

        collection = _resolve_collection(db, user_doc)
        await collection.update_one(
            {"_id": user_doc["_id"]},
            {"$set": {"last_login": datetime.now(timezone.utc)}}
        )

        user_id_str = str(user_doc["_id"])
        is_admin = user_doc.get("_source_collection") == "admins" or user_doc.get("is_admin", False)
        user_role = "admin" if is_admin else "user"
        token_subject = user_doc.get("email") or user_id_str

        access_token = create_access_token(data={"sub": token_subject, "role": user_role, "user_id": user_id_str})
        refresh_token = create_refresh_token(data={"sub": token_subject, "role": user_role, "user_id": user_id_str})

        user_data = _build_profile_data(user_doc, user_id_str)

        # Resolve session_id — patients always get one, even on first login.
        # assessment_completed is true only when personality_summary is saved
        # (set exclusively by the assessment endpoint after onboarding).
        welcome_message = None
        if not is_admin:
            existing_session = await get_existing_session(user_id_str)
            if existing_session:
                session_id_val = existing_session.get("session_id")
            else:
                # First login — no session yet. Create one so the client
                # always has a valid session_id to work with.
                session_id_val = str(uuid.uuid4())
                await upsert_session(user_id_str, session_id_val)
            assessment_completed = bool(user_doc.get("personality_summary"))

            # Generate a personalised welcome-back greeting for returning users
            # who have already completed the assessment.
            if assessment_completed:
                first_name = (
                    user_doc.get("first_name")
                    or user_doc.get("full_name", "").split()[0]
                    or "there"
                ).strip()
                welcome_message = await _generate_login_greeting(
                    first_name,
                    user_doc.get("personality_summary", ""),
                )
        else:
            assessment_completed = True
            session_id_val = None

        logger.info(f"Login Successful | Role: {user_role} | Assessment: {assessment_completed}")

        return UserLoginResponse(
            status="success",
            message="Login successful",
            user=user_data,
            access_token=access_token,
            refresh_token=refresh_token,
            assessment_completed=assessment_completed,
            session_id=session_id_val,
            welcome_message=welcome_message,
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"event=login_failed error={str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


@router.post("/forgot-password", response_model=ForgotPasswordResponse)
async def forgot_password(payload: ForgotPasswordRequest):
    """Send OTP to user's email for password reset."""
    try:
        masked_email = payload.email[:2] + "***" + payload.email.split("@")[-1] if "@" in payload.email else "***"
        logger.info(f"Forgot password request | Email: {masked_email}")

        db = get_database()
        if db is None:
            raise HTTPException(status_code=503, detail="Database connection failed.")

        user_doc = await find_user_by_identifier(db, payload.email)
        if not user_doc:
            return ForgotPasswordResponse(
                status="failure",
                message="If your email is registered, you will receive an OTP shortly.",
            )

        otp = generate_otp()
        settings = get_settings()
        expires_at = datetime.now(timezone.utc) + timedelta(minutes=settings.OTP_EXPIRY_MINUTES)

        if "_id" not in user_doc:
            raise HTTPException(status_code=500, detail="User data corrupted")

        collection = _resolve_collection(db, user_doc)
        await collection.update_one(
            {"_id": user_doc["_id"]},
            {"$set": {"password_reset_token": otp, "password_reset_expires": expires_at}}
        )

        try:
            email_sent = await send_otp_email(payload.email, otp)
        except Exception as e:
            logger.error(f"event=send_otp_email_failed error={str(e)}")
            email_sent = False

        if not email_sent:
            raise HTTPException(status_code=500, detail="Failed to send OTP email. Please try again later.")

        logger.info(f"Password reset OTP sent to {masked_email}")
        return ForgotPasswordResponse(
            status="success",
            message="OTP sent to your email. Please check your inbox.",
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"event=forgot_password_failed error={str(e)}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.post("/verify-otp", response_model=VerifyOtpResponse)
async def verify_otp(payload: VerifyOtpRequest):
    """Verify OTP for password reset."""
    try:
        masked_email = payload.email[:2] + "***" + payload.email.split("@")[-1] if "@" in payload.email else "***"

        db = get_database()
        if db is None:
            raise HTTPException(status_code=503, detail="Database connection failed.")

        user_doc = await find_user_by_identifier(db, payload.email)
        if not user_doc:
            raise HTTPException(status_code=404, detail="User not found")

        stored_otp = user_doc.get("password_reset_token")
        expires_at = user_doc.get("password_reset_expires")

        if not stored_otp or not expires_at:
            raise HTTPException(status_code=400, detail="No reset request found. Please request a new OTP.")
        expires_at_aware = expires_at.replace(tzinfo=timezone.utc) if expires_at.tzinfo is None else expires_at
        if datetime.now(timezone.utc) > expires_at_aware:
            raise HTTPException(status_code=400, detail="OTP has expired. Please request a new OTP.")
        if payload.otp != stored_otp:
            raise HTTPException(status_code=400, detail="Invalid OTP. Please check and try again.")

        if "_id" not in user_doc:
            raise HTTPException(status_code=500, detail="User data corrupted")

        collection = _resolve_collection(db, user_doc)
        await collection.update_one(
            {"_id": user_doc["_id"]},
            {"$unset": {"password_reset_token": "", "password_reset_expires": ""}}
        )

        logger.info(f"OTP verified for {masked_email}")
        return VerifyOtpResponse(status="success", message="OTP verified successfully. You can now reset your password.")

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"event=verify_otp_failed error={str(e)}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.post("/reset-password", response_model=ResetPasswordResponse)
async def reset_password(payload: ResetPasswordRequest):
    """Reset password after OTP verification."""
    try:
        masked_email = payload.email[:2] + "***" + payload.email.split("@")[-1] if "@" in payload.email else "***"

        db = get_database()
        if db is None:
            raise HTTPException(status_code=503, detail="Database connection failed.")

        user_doc = await find_user_by_identifier(db, payload.email)
        if not user_doc:
            raise HTTPException(status_code=404, detail="User not found")

        if user_doc.get("password_reset_token") or user_doc.get("password_reset_expires"):
            raise HTTPException(status_code=400, detail="Please verify your OTP first before resetting password.")

        if "password_hash" in user_doc:
            if await run_in_threadpool(Hash.verify, user_doc["password_hash"], payload.new_password):
                raise HTTPException(status_code=400, detail="New password must not match the current password.")

        validate_password(payload.new_password)
        password_hash = await run_in_threadpool(Hash.bcrypt, payload.new_password)

        if "_id" not in user_doc:
            raise HTTPException(status_code=500, detail="User data corrupted")

        collection = _resolve_collection(db, user_doc)
        await collection.update_one(
            {"_id": user_doc["_id"]},
            {"$set": {"password_hash": password_hash, "last_active": datetime.now(timezone.utc)}}
        )

        logger.info(f"Password reset for {masked_email}")
        return ResetPasswordResponse(status="success", message="Password reset successfully.")

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"event=reset_password_failed error={str(e)}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.post("/refresh", response_model=RefreshTokenResponse)
async def refresh_token(payload: RefreshTokenRequest):
    """Refresh access token using a valid refresh token."""
    try:
        logger.info("Refresh token attempt")

        credentials_exception = HTTPException(
            status_code=401,
            detail="Invalid refresh token",
            headers={"WWW-Authenticate": "Bearer"},
        )

        token_data = await verify_refresh_token(payload.refresh_token, credentials_exception)

        db = get_database()
        if db is None:
            raise HTTPException(status_code=503, detail="Database connection failed")

        user_doc = await find_user_by_identifier(db, token_data.email)
        if not user_doc:
            raise HTTPException(status_code=401, detail="User not found")

        if "_id" not in user_doc:
            raise HTTPException(status_code=500, detail="User data corrupted")

        is_admin = user_doc.get("_source_collection") == "admins" or user_doc.get("is_admin", False)
        user_role = "admin" if is_admin else "user"
        token_subject = user_doc.get("email") or str(user_doc["_id"])
        user_id_str = str(user_doc["_id"])

        access_token = create_access_token(data={"sub": token_subject, "role": user_role, "user_id": user_id_str})
        new_refresh_token = create_refresh_token(data={"sub": token_subject, "role": user_role, "user_id": user_id_str})

        logger.info(f"Access token refreshed for: {token_data.email}")
        return RefreshTokenResponse(
            status="success", 
            access_token=access_token, 
            refresh_token=new_refresh_token,
            token_type="bearer"
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"event=refresh_token_failed error={str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


@router.post("/logout")
async def user_logout(
    credentials: HTTPAuthorizationCredentials = Depends(token_auth_scheme),
    current_user: dict = Depends(get_current_user),
):
    try:
        await add_to_blacklist(credentials.credentials)
        return {"status": "success", "message": "Successfully logged out"}
    except Exception as e:
        logger.error(f"event=logout_failed error={str(e)}")
        raise HTTPException(status_code=500, detail="Internal server error")
