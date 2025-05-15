from fastapi import APIRouter, Depends, Query, HTTPException
from motor.motor_asyncio import AsyncIOMotorDatabase
from typing import Optional
from bson import ObjectId
from ..dependencies import get_db

router = APIRouter(tags=["results"])

@router.get("")  # Changed from "/tasks/{task_id}/results" since prefix is already set
async def get_results(
    task_id: str,
    page: int = Query(1, ge=1),
    limit: int = Query(50, ge=1, le=100),
    included: Optional[bool] = None,
    db: AsyncIOMotorDatabase = Depends(get_db)
):
    """Get screening results with pagination"""
    try:
        # Verify task exists
        task = await db.tasks.find_one({"_id": ObjectId(task_id)})
        if not task:
            raise HTTPException(status_code=404, detail="Task not found")

        # Build query
        query = {"taskId": task_id}
        if included is not None:
            query["included"] = included
        
        # Get results with pagination
        skip = (page - 1) * limit
        
        results = await db.screening_results.find(query)\
            .sort("relevanceScore", -1)\
            .skip(skip)\
            .limit(limit)\
            .to_list(length=limit)
        
        # Convert ObjectId to string for JSON serialization
        for result in results:
            result["_id"] = str(result["_id"])
        
        # Get total count
        total = await db.screening_results.count_documents(query)
        
        return {
            "success": True,
            "results": results,
            "pagination": {
                "page": page,
                "limit": limit,
                "total": total,
                "pages": (total + limit - 1) // limit
            }
        }

    except Exception as e:
        print(f"Error fetching results: {e}")
        raise HTTPException(
            status_code=500,
            detail=str(e)
        )