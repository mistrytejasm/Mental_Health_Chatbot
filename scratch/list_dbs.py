
import asyncio
import os
from motor.motor_asyncio import AsyncIOMotorClient
from dotenv import load_dotenv

async def list_dbs():
    load_dotenv()
    mongo_url = os.getenv("MONGODB_URL")
    if not mongo_url:
        print("No URL")
        return
    client = AsyncIOMotorClient(mongo_url)
    dbs = await client.list_database_names()
    print("Databases on cluster:")
    for d in dbs:
        print(f" - {d}")
    client.close()

if __name__ == "__main__":
    asyncio.run(list_dbs())
