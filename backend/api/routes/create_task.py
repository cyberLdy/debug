from bson import ObjectId
from fastapi import APIRouter, Depends, HTTPException
from motor.motor_asyncio import AsyncIOMotorDatabase
from datetime import datetime
from ..dependencies import get_db
from ..models import TaskCreate

router = APIRouter()

async def create_task(
    task: TaskCreate,
    db: AsyncIOMotorDatabase = Depends(get_db)
):
    """Create a new screening task"""
    try:
        # Create task document with initial pending state
        task_doc = {
            "userId": task.userId,
            "searchQuery": task.searchQuery,
            "criteria": task.criteria,
            "model": task.model,
            "status": "running",  # Initial state is now running
            "progress": {
                "total": task.totalArticles,
                "current": 0
            },
            "startedAt": datetime.utcnow(),
            "name": f"Screening: {task.searchQuery[:50]}",
            "remainingArticles": []  # Initialize empty remaining articles list
        }

        # Insert into database
        result = await db.tasks.insert_one(task_doc)
        
        # Get the created task
        created_task = await db.tasks.find_one({"_id": result.inserted_id})
        
        if not created_task:
            raise HTTPException(status_code=500, detail="Failed to create task")

        return {
            "success": True,
            "task": {
                **created_task,
                "_id": str(created_task["_id"])
            }
        }

    except Exception as e:
        print(f"Error creating task: {e}")
        raise HTTPException(
            status_code=500,
            detail=str(e)
        )