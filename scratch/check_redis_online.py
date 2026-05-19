
import asyncio
import os
from redis.asyncio import Redis
from dotenv import load_dotenv

async def check_redis_set():
    load_dotenv()
    redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    
    try:
        redis = Redis.from_url(redis_url, decode_responses=True)
        online = await redis.smembers("dashboard:online_counselors")
        print(f"Counselors in Redis 'dashboard:online_counselors': {online}")
        await redis.close()
    except Exception as e:
        print(f"Redis Error: {e}")

if __name__ == "__main__":
    asyncio.run(check_redis_set())
