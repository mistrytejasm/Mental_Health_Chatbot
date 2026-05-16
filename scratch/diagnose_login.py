
import asyncio
import os
import re
from motor.motor_asyncio import AsyncIOMotorClient
from dotenv import load_dotenv

async def check_user():
    load_dotenv()
    mongo_url = os.getenv("MONGODB_URL")
    db_name = os.getenv("DATABASE_NAME", "mindbuddy_db")
    
    if not mongo_url:
        print("Error: MONGODB_URL not found in .env")
        return

    client = AsyncIOMotorClient(mongo_url)
    db = client[db_name]
    
    # Search pattern for te***two.com
    pattern = re.compile(r"^te.*two\.com$", re.IGNORECASE)
    
    print(f"Searching for users matching: ^te.*two.com$ in {db_name}...")
    
    found = False
    for collection_name in ["users", "admins"]:
        collection = db[collection_name]
        cursor = collection.find({"email": pattern})
        async for doc in cursor:
            found = True
            email = doc.get("email")
            first_name = doc.get("first_name", "N/A")
            is_active = doc.get("is_active", True)
            has_hash = "password_hash" in doc
            hash_start = doc.get("password_hash")[:10] if has_hash else "NONE"
            
            print(f"\n--- MATCH FOUND in [{collection_name}] ---")
            print(f"Email: {email}")
            print(f"Name: {first_name}")
            print(f"Active: {is_active}")
            print(f"Password Hash Present: {has_hash}")
            print(f"Hash Format Start: {hash_start}")
    
    if not found:
        print("\nNo matching users found in 'users' or 'admins' collections.")
    
    client.close()

if __name__ == "__main__":
    asyncio.run(check_user())
