import asyncio
from typing import Optional
from redis.asyncio import Redis
from app.core.config import get_settings
from app.core.logger import get_logger

logger = get_logger(__name__)

class RedisManager:
    def __init__(self):
        self.redis: Optional[Redis] = None

    async def connect(self):
        """Initializes the Redis connection pool."""
        settings = get_settings()
        try:
            self.redis = Redis.from_url(
                settings.REDIS_URL,
                decode_responses=True,
                health_check_interval=30
            )
            await self.redis.ping()
            logger.info(f"Connected to Redis at {settings.REDIS_URL}")
        except Exception as e:
            logger.error(f"Failed to connect to Redis: {e}")
            self.redis = None

    async def disconnect(self):
        """Closes the Redis connection pool."""
        if self.redis:
            await self.redis.close()
            logger.info("Disconnected from Redis")
            self.redis = None

    def get_client(self) -> Optional[Redis]:
        return self.redis

redis_manager = RedisManager()

def get_redis() -> Optional[Redis]:
    return redis_manager.get_client()
