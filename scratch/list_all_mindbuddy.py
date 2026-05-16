
import asyncio
import os
from motor.motor_asyncio import AsyncIOMotorClient
from dotenv import load_dotenv

async def list_all_mindbuddy():
    load_dotenv()
    mongo_url = os.getenv("MONGODB_URL")
    db_name = "mindbuddy_db"
    
    client = AsyncIOMotorClient(mongo_url)
    db = client[db_name]
    
    print(f"Listing all users in {db_name}...")
    for collection_name in ["users", "admins"]:
        collection = db[collection_name]
        async for doc in collection.find():
            print(f"[{collection_name}] Email: {doc.get('email')}")
    client.close()

if __name__ == "__main__":
    asyncio.run(list_all_mindbuddy())
