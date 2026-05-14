from app.core.logger import get_logger
from app.models.db import AdminModelDB

logger = get_logger(__name__)


async def create_admin(db, user_id: str, admin_payload: dict) -> bool:
    try:
        admin_data = AdminModelDB(user_id=user_id, **admin_payload)
        await db.admins.insert_one(admin_data.model_dump())
        logger.info(f"event=admin_created user_id={user_id}")
        return True
    except Exception as e:
        logger.error(f"event=admin_create_failed user_id={user_id} error={str(e)}")
        return False

async def update_admin(db, user_id: str, admin_updates: dict) -> bool:
    try:
        admin_updates.pop("user_id", None)
        admin_updates.pop("_id", None)
        result = await db.admins.update_one({"user_id": user_id}, {"$set": admin_updates})
        if result.matched_count == 0:
            logger.warning(f"event=admin_update_missing user_id={user_id}")
            return False
        logger.info(f"event=admin_updated user_id={user_id}")
        return True
    except Exception as e:
        logger.error(f"event=admin_update_failed user_id={user_id} error={str(e)}")
        return False
