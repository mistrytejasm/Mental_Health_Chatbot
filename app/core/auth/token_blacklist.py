import hashlib
from datetime import datetime, timezone
from jose import jwt, JWTError

from app.core.config import get_settings
from app.core.logger import get_logger

logger = get_logger(__name__)


def _token_hash(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def _extract_exp(token: str) -> datetime:
    settings = get_settings()
    payload = jwt.decode(token, settings.SECRET_KEY, algorithms=[settings.ALGORITHM])
    exp = payload.get("exp")
    if exp is None:
        raise ValueError("Token missing exp claim")
    return datetime.fromtimestamp(exp, tz=timezone.utc)


def _get_collection():
    from app.core.database import get_database
    db = get_database()
    if db is None:
        raise RuntimeError("Database not initialised — ensure connect_to_mongo() has run")
    return db.token_blacklist


async def add_to_blacklist(token: str) -> None:
    try:
        collection = _get_collection()
        token_expiry = _extract_exp(token)
        now_utc = datetime.now(timezone.utc)
        if token_expiry <= now_utc:
            logger.info("event=token_blacklist_skip reason=already_expired")
            return

        token_hash = _token_hash(token)
        await collection.update_one(
            {"token_hash": token_hash},
            {"$set": {"token_hash": token_hash, "expires_at": token_expiry}},
            upsert=True,
        )
        logger.info("event=token_blacklist_added")
    except JWTError:
        logger.warning("event=token_blacklist_add_failed reason=invalid_token")
        raise
    except Exception as e:
        logger.error(f"event=token_blacklist_add_failed reason=unexpected error={str(e)}")
        raise


async def is_token_blacklisted(token: str) -> bool:
    try:
        collection = _get_collection()
        now_utc = datetime.now(timezone.utc)
        doc = await collection.find_one(
            {"token_hash": _token_hash(token), "expires_at": {"$gt": now_utc}}
        )
        return doc is not None
    except Exception as e:
        logger.error(f"event=token_blacklist_check_failed reason=unexpected error={str(e)}")
        return False


async def cleanup_expired_blacklist() -> None:
    try:
        collection = _get_collection()
        now_utc = datetime.now(timezone.utc)
        await collection.delete_many({"expires_at": {"$lte": now_utc}})
    except Exception as e:
        logger.error(f"event=token_blacklist_cleanup_failed error={str(e)}")


async def get_blacklist_size() -> int:
    try:
        collection = _get_collection()
        now_utc = datetime.now(timezone.utc)
        return await collection.count_documents({"expires_at": {"$gt": now_utc}})
    except Exception as e:
        logger.error(f"event=token_blacklist_size_failed error={str(e)}")
        return 0
