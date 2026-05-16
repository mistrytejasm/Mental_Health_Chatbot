
import asyncio
import os
from motor.motor_asyncio import AsyncIOMotorClient
from dotenv import load_dotenv
from datetime import datetime, timedelta, timezone

async def check_counselor_health():
    load_dotenv()
    mongo_url = os.getenv("MONGODB_URL")
    db_name = os.getenv("DATABASE_NAME", "mindbuddy_db")
    
    client = AsyncIOMotorClient(mongo_url)
    db = client[db_name]
    
    print(f"--- Counselor Health Check [{db_name}] ---")
    stale_cutoff = datetime.now(timezone.utc) - timedelta(seconds=2100)
    
    cursor = db.admins.find()
    async for doc in cursor:
        email = doc.get("email")
        is_online = doc.get("is_online")
        last_ping = doc.get("last_ping")
        checked_in = doc.get("checked_in_at")
        active_sessions = doc.get("current_active_sessions", 0)
        
        fresh = False
        if last_ping:
            if last_ping.tzinfo is None:
                last_ping = last_ping.replace(tzinfo=timezone.utc)
            fresh = (datetime.now(timezone.utc) - last_ping) < timedelta(seconds=2100)
            
        print(f"\nCounselor: {email}")
        print(f"  - DB is_online: {is_online}")
        print(f"  - last_ping: {last_ping} ({'FRESH' if fresh else 'STALE'})")
        print(f"  - checked_in_at: {checked_in} ({'EXISTS' if checked_in else 'MISSING'})")
        print(f"  - current_active_sessions: {active_sessions}")
        
        available = is_online and fresh and checked_in and (active_sessions < 2)
        print(f"  - RESULT: {'AVAILABLE' if available else 'UNAVAILABLE'}")
        
        if not available:
            reasons = []
            if not is_online: reasons.append("is_online is False")
            if not fresh: reasons.append("last_ping is stale")
            if not checked_in: reasons.append("checked_in_at is missing")
            if active_sessions >= 2: reasons.append(f"At capacity ({active_sessions}/2)")
            print(f"  - WHY: {', '.join(reasons)}")

    client.close()

if __name__ == "__main__":
    asyncio.run(check_counselor_health())
