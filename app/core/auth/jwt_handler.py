"""
JWT Token Handler
──────────────────
Handles creation and verification of access and refresh tokens using python-jose.
Token blacklist checks are performed on every verification to prevent use of
logged-out tokens even within their expiry window.
"""

from datetime import datetime, timedelta, timezone

from fastapi import HTTPException
from jose import ExpiredSignatureError, JWTError, jwt

from app.api.schemas.response import TokenData
from app.core.auth.token_blacklist import is_token_blacklisted
from app.core.config import get_settings
from app.core.logger import get_logger

logger = get_logger(__name__)


# ── Token Creation ────────────────────────────────────────────────────────────

def create_access_token(data: dict) -> str:
    """Creates a signed JWT access token with a configurable expiry."""
    settings = get_settings()
    payload = data.copy()
    payload.update({
        "exp": datetime.now(timezone.utc) + timedelta(minutes=settings.ACCESS_TOKEN_EXPIRE_MINUTES),
        "type": "access",
    })
    return jwt.encode(payload, settings.SECRET_KEY, algorithm=settings.ALGORITHM)


def create_refresh_token(data: dict) -> str:
    """Creates a signed JWT refresh token with a longer configurable expiry."""
    settings = get_settings()
    payload = data.copy()
    payload.update({
        "exp": datetime.now(timezone.utc) + timedelta(days=settings.REFRESH_TOKEN_EXPIRE_DAYS),
        "type": "refresh",
    })
    return jwt.encode(payload, settings.SECRET_KEY, algorithm=settings.ALGORITHM)


# ── Token Verification ────────────────────────────────────────────────────────

async def verify_refresh_token(token: str, credentials_exception: HTTPException) -> TokenData:
    """
    Decodes and validates a refresh token.
    Raises HTTPException(401) if blacklisted, expired, or malformed.
    """
    if await is_token_blacklisted(token):
        logger.warning("event=refresh_token_rejected reason=blacklisted")
        raise HTTPException(status_code=401, detail="Token is blacklisted")

    settings = get_settings()
    try:
        payload = jwt.decode(token, settings.SECRET_KEY, algorithms=[settings.ALGORITHM])

        user_id = payload.get("user_id")
        if user_id is None or payload.get("type") != "refresh":
            logger.warning("event=refresh_token_rejected reason=invalid_claims")
            raise credentials_exception

        return TokenData(
            user_id=user_id,
            email=payload.get("sub"),
            role=payload.get("role"),
        )

    except ExpiredSignatureError:
        logger.warning("event=refresh_token_rejected reason=expired")
        raise HTTPException(
            status_code=403,
            detail="Refresh token expired. Please log in again.",
        )
    except JWTError:
        logger.warning("event=refresh_token_rejected reason=decode_error")
        raise credentials_exception


async def verify_token(token: str, credentials_exception: HTTPException) -> TokenData:
    """
    Decodes and validates an access token.
    Raises HTTPException(401) if blacklisted, expired, or malformed.
    """
    settings = get_settings()
    try:
        payload = jwt.decode(token, settings.SECRET_KEY, algorithms=[settings.ALGORITHM])

        if await is_token_blacklisted(token):
            logger.warning("event=access_token_rejected reason=blacklisted")
            raise HTTPException(
                status_code=401,
                detail="Token has been revoked. Please log in again.",
                headers={"WWW-Authenticate": "Bearer"},
            )

        user_id = payload.get("user_id")
        if user_id is None or payload.get("type") != "access":
            logger.warning("event=access_token_rejected reason=invalid_claims")
            raise credentials_exception

        return TokenData(
            user_id=user_id,
            email=payload.get("sub"),
            role=payload.get("role"),
        )

    except ExpiredSignatureError:
        logger.warning("event=access_token_rejected reason=expired")
        raise HTTPException(
            status_code=401,
            detail="Token expired. Please log in again.",
            headers={"WWW-Authenticate": "Bearer"},
        )
    except JWTError:
        logger.warning("event=access_token_rejected reason=decode_error")
        raise credentials_exception
