from motor.motor_asyncio import AsyncIOMotorClient
from fastapi import Depends, HTTPException
from typing import AsyncGenerator
from config import settings

# Global MongoDB client
client = AsyncIOMotorClient(settings.MONGODB_URI)

async def get_db() -> AsyncGenerator:
    """Get database connection."""
    try:
        # Verify connection is alive
        await client.admin.command('ping')
        yield client[settings.MONGODB_DB]
    except Exception as e:
        print(f"❌ Database connection error: {e}")
        raise HTTPException(
            status_code=500,
            detail="Database connection error"
        )

async def get_current_task(task_id: str, db=Depends(get_db)):
    """Get current task by ID."""
    task = await db.tasks.find_one({"_id": task_id})
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")
    return task