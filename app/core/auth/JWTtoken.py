from datetime import datetime, timedelta, timezone
from jose import JWTError, jwt, ExpiredSignatureError
from fastapi import HTTPException

from app.api.schemas.response import TokenData
from app.core.config import get_settings
from app.core.auth.token_blacklist import is_token_blacklisted
from app.core.logger import get_logger

logger = get_logger(__name__)


# ─────────────────────────────────────────────────────────────
# Create Tokens
# ─────────────────────────────────────────────────────────────

def create_access_token(data: dict):
    settings = get_settings()

    to_encode = data.copy()
    expire = datetime.now(timezone.utc) + timedelta(
        minutes=settings.ACCESS_TOKEN_EXPIRE_MINUTES
    )

    to_encode.update({
        "exp": expire,
        "type": "access"
    })

    encoded_jwt = jwt.encode(
        to_encode,
        settings.SECRET_KEY,
        algorithm=settings.ALGORITHM,
    )

    return encoded_jwt


def create_refresh_token(data: dict):
    settings = get_settings()

    to_encode = data.copy()
    expire = datetime.now(timezone.utc) + timedelta(
        days=settings.REFRESH_TOKEN_EXPIRE_DAYS
    )

    to_encode.update({
        "exp": expire,
        "type": "refresh"
    })

    encoded_jwt = jwt.encode(
        to_encode,
        settings.SECRET_KEY,
        algorithm=settings.ALGORITHM,
    )

    return encoded_jwt


# ─────────────────────────────────────────────────────────────
# Verify Refresh Token
# ─────────────────────────────────────────────────────────────

async def verify_refresh_token(token: str, credentials_exception):
    settings = get_settings()

    if await is_token_blacklisted(token):
        logger.warning("event=refresh_token_rejected reason=blacklisted")
        raise HTTPException(status_code=401, detail="Token is blacklisted")

    try:
        payload = jwt.decode(
            token,
            settings.SECRET_KEY,
            algorithms=[settings.ALGORITHM]
        )

        email = payload.get("sub")
        user_id = payload.get("user_id")
        token_type = payload.get("type")
        role = payload.get("role")

        if user_id is None or token_type != "refresh":
            logger.warning("event=refresh_token_rejected reason=invalid_claims")
            raise credentials_exception

        token_data = TokenData(
            user_id=user_id,
            email=email,
            role=role
        )

        return token_data

    except ExpiredSignatureError:
        logger.warning("event=refresh_token_rejected reason=expired")
        raise HTTPException(
            status_code=403,
            detail="Refresh token expired! Please login again.",
        )

    except JWTError:
        logger.warning("event=refresh_token_rejected reason=decode_error")
        raise credentials_exception


# ─────────────────────────────────────────────────────────────
# Verify Access Token
# ─────────────────────────────────────────────────────────────

async def verify_token(token: str, credentials_exception):
    settings = get_settings()

    try:
        payload = jwt.decode(
            token,
            settings.SECRET_KEY,
            algorithms=[settings.ALGORITHM]
        )

        if await is_token_blacklisted(token):
            logger.warning("event=access_token_rejected reason=blacklisted")
            raise HTTPException(
                status_code=401,
                detail="Token has been revoked. Please login again.",
                headers={"WWW-Authenticate": "Bearer"},
            )

        user_id = payload.get("user_id")
        email = payload.get("sub")
        token_type = payload.get("type")
        role = payload.get("role")

        if user_id is None or token_type != "access":
            logger.warning("event=access_token_rejected reason=invalid_claims")
            raise credentials_exception

        token_data = TokenData(
            user_id=user_id,
            email=email,
            role=role
        )

        return token_data

    except ExpiredSignatureError:
        logger.warning("event=access_token_rejected reason=expired")
        raise HTTPException(
            status_code=401,
            detail="Token expired! Please login again.",
            headers={"WWW-Authenticate": "Bearer"},
        )

    except JWTError:
        logger.warning("event=access_token_rejected reason=decode_error")
        raise credentials_exception
