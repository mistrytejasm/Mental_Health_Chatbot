from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from app.core.logger import get_logger

from app.core.auth.JWTtoken import verify_token

logger = get_logger(__name__)

token_auth_scheme = HTTPBearer()


async def get_current_user(credentials: HTTPAuthorizationCredentials = Depends(token_auth_scheme)):
    token = credentials.credentials

    # Fix Swagger double "Bearer Bearer"
    if token.startswith("Bearer "):
        token = token.split(" ")[1]

    logger.debug(f"Token received for validation: {token[:8]}...")

    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Invalid authentication credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )

    # Decode + validate token
    token_data = await verify_token(token, credentials_exception)

    # ✅ FIXED: use user_id as primary key
    user_doc = {
        "_id": token_data.user_id,
        "user_id": token_data.user_id,
        "email": token_data.email,
        # "useremail": token_data.email,  # optional (keep if your code expects it)
        "role": token_data.role,
        "token": token  # IMPORTANT for logout
    }

    return user_doc