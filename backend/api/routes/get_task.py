from bson import ObjectId
from fastapi import APIRouter, Depends, HTTPException
from motor.motor_asyncio import AsyncIOMotorDatabase
from ..dependencies import get_db

router = APIRouter()

async def get_task(
    task_id: str,
    db: AsyncIOMotorDatabase = Depends(get_db)
):
    """Get task details with screening stats"""
    try:
        # Verify task exists
        task = await db.tasks.find_one({"_id": ObjectId(task_id)})
        if not task:
            raise HTTPException(status_code=404, detail="Task not found")

        # Get screening stats
        stats = await db.screening_results.aggregate([
            {"$match": {"taskId": task_id}},
            {
                "$group": {
                    "_id": None,
                    "included": {"$sum": {"$cond": ["$included", 1, 0]}},
                    "excluded": {"$sum": {"$cond": ["$included", 0, 1]}}
                }
            }
        ]).to_list(length=1)

        # Get article count for verification
        article_count = await db.articles.count_documents({"taskId": task_id})

        # Add article count to response
        task_response = {
            **task,
            "_id": str(task["_id"]),
            "stats": stats[0] if stats else {"included": 0, "excluded": 0},
            "articleCount": article_count
        }

        return {
            "success": True,
            "task": task_response
        }

    except Exception as e:
        print(f"Error fetching task: {e}")
        raise HTTPException(
            status_code=500,
            detail=str(e)
        )