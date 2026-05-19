"""
OAuth2 / HTTP Bearer Authentication Dependency
────────────────────────────────────────────────
Provides the `get_current_user` FastAPI dependency used by all protected routes.
Extracts the Bearer token from the Authorization header, verifies it via
jwt_handler, and returns a user context dict for downstream use.
"""

from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from app.core.auth.jwt_handler import verify_token
from app.core.logger import get_logger

logger = get_logger(__name__)

_bearer_scheme = HTTPBearer()


async def get_current_user(
    credentials: HTTPAuthorizationCredentials = Depends(_bearer_scheme),
) -> dict:
    """
    FastAPI dependency that validates the Bearer token and returns a user context dict.

    Returns:
        dict with keys: _id, user_id, email, role, token
    """
    token = credentials.credentials

    # Guard against clients that accidentally double-prefix with "Bearer "
    if token.startswith("Bearer "):
        token = token.split(" ", 1)[1]

    logger.debug(f"Token received for validation: {token[:8]}...")

    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Invalid authentication credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )

    token_data = await verify_token(token, credentials_exception)

    return {
        "_id": token_data.user_id,
        "user_id": token_data.user_id,
        "email": token_data.email,
        "role": token_data.role,
        "token": token,
    }