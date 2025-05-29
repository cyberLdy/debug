from fastapi import APIRouter, Depends, HTTPException, Query
from motor.motor_asyncio import AsyncIOMotorDatabase
from typing import Optional
from ..dependencies import get_db

router = APIRouter()

async def list_tasks(
    db: AsyncIOMotorDatabase = Depends(get_db),
    status: Optional[str] = None,
    page: int = Query(1, ge=1),
    limit: int = Query(20, ge=1, le=100)
):
    """List tasks with pagination and optional status filter"""
    try:
        # Build query
        query = {}
        if status and status != "all":
            query["status"] = status

        # Get total count for pagination
        total = await db.tasks.count_documents(query)
        
        # Get paginated tasks
        tasks = await db.tasks.find(query)\
            .sort("startedAt", -1)\
            .skip((page - 1) * limit)\
            .limit(limit)\
            .to_list(length=limit)

        # Get stats for each task
        for task in tasks:
            stats = await db.screening_results.aggregate([
                {"$match": {"taskId": str(task["_id"])}},
                {
                    "$group": {
                        "_id": None,
                        "included": {"$sum": {"$cond": ["$included", 1, 0]}},
                        "excluded": {"$sum": {"$cond": ["$included", 0, 1]}}
                    }
                }
            ]).to_list(length=1)
            
            task["_id"] = str(task["_id"])
            task["stats"] = stats[0] if stats else {"included": 0, "excluded": 0}

        return {
            "success": True,
            "tasks": tasks,
            "pagination": {
                "page": page,
                "limit": limit,
                "total": total,
                "pages": (total + limit - 1) // limit
            }
        }

    except Exception as e:
        print(f"Error listing tasks: {e}")
        raise HTTPException(
            status_code=500,
            detail=str(e)
        )