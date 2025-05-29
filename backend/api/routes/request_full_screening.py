from bson import ObjectId
from fastapi import APIRouter, Depends, HTTPException
from motor.motor_asyncio import AsyncIOMotorDatabase
from datetime import datetime
from ..dependencies import get_db

router = APIRouter()

async def request_full_screening(
    task_id: str,
    data: dict,
    db: AsyncIOMotorDatabase = Depends(get_db)
):
    """Request full screening for remaining articles"""
    print(f"\n🚀 Starting full screening request for task: {task_id}")
    
    try:
        # Verify task exists and is in paused state
        task = await db.tasks.find_one({
            "_id": ObjectId(task_id),
            "status": "paused"
        })

        if not task:
            raise HTTPException(
                status_code=404,
                detail="Task not found or not in paused state"
            )

        # Get remaining articles
        remaining_articles = data.get("remainingArticles", [])
        if not remaining_articles:
            raise HTTPException(
                status_code=400,
                detail="No remaining articles provided"
            )

        # Get current progress
        current_progress = task.get("progress", {}).get("current", 0)
        total_articles = task.get("progress", {}).get("total", 0)

        print(f"Current state: progress={task.get('progress')}, remaining={len(remaining_articles)}")

        # Update task status to full_screening while preserving progress
        await db.tasks.update_one(
            {"_id": ObjectId(task_id)},
            {
                "$set": {
                    "status": "full_screening",
                    "error": None,
                    "remainingArticles": remaining_articles,
                    "progress.current": current_progress,
                    "progress.total": total_articles
                }
            }
        )

        print(f"✅ Task {task_id} updated to full_screening state")
        print(f"Progress preserved: current={current_progress}, total={total_articles}")

        return {
            "success": True,
            "message": "Full screening started successfully"
        }

    except Exception as e:
        print(f"Error requesting full screening: {e}")
        raise HTTPException(
            status_code=500,
            detail=str(e)
        )