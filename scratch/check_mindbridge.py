
import asyncio
import os
import re
from motor.motor_asyncio import AsyncIOMotorClient
from dotenv import load_dotenv

async def check_user_mindbridge():
    load_dotenv()
    mongo_url = os.getenv("MONGODB_URL")
    
    # Check MindBridge instead of MindBuddy
    db_name = "mindbridge_db"
    
    client = AsyncIOMotorClient(mongo_url)
    db = client[db_name]
    
    print(f"Searching in {db_name}...")
    found = False
    for collection_name in ["users", "admins"]:
        collection = db[collection_name]
        cursor = collection.find().sort("created_at", -1).limit(5)
        async for doc in cursor:
            found = True
            print(f"[{collection_name}] Email: {doc.get('email')} | Created: {doc.get('created_at')}")
    
    if not found:
        print("No users found in MindBridge DB either.")
    client.close()

if __name__ == "__main__":
    asyncio.run(check_user_mindbridge())
