
import asyncio
import os
from motor.motor_asyncio import AsyncIOMotorClient
from dotenv import load_dotenv

async def list_recent_users():
    load_dotenv()
    mongo_url = os.getenv("MONGODB_URL")
    db_name = os.getenv("DATABASE_NAME", "mindbuddy_db")
    
    if not mongo_url:
        print("Error: MONGODB_URL not found")
        return

    client = AsyncIOMotorClient(mongo_url)
    db = client[db_name]
    
    print(f"Listing most recent users in {db_name}...")
    
    for collection_name in ["users", "admins"]:
        print(f"\n--- Recent in [{collection_name}] ---")
        collection = db[collection_name]
        cursor = collection.find().sort("created_at", -1).limit(10)
        async for doc in cursor:
            email = doc.get("email")
            created_at = doc.get("created_at")
            print(f"Email: {email} | Created: {created_at}")
    
    client.close()

if __name__ == "__main__":
    asyncio.run(list_recent_users())
