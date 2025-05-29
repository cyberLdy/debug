from bson import ObjectId
from fastapi import APIRouter, Depends, HTTPException
from motor.motor_asyncio import AsyncIOMotorDatabase
from datetime import datetime
from ..dependencies import get_db

router = APIRouter()

async def cancel_task(
    task_id: str,
    db: AsyncIOMotorDatabase = Depends(get_db)
):
    """Cancel a running task"""
    try:
        # Verify task exists
        task = await db.tasks.find_one({"_id": ObjectId(task_id)})
        if not task:
            raise HTTPException(status_code=404, detail="Task not found")

        # Check if task can be cancelled
        if task["status"] in ["done", "error"]:
            raise HTTPException(
                status_code=400, 
                detail=f"Task cannot be cancelled in {task['status']} state"
            )

        print(f"🛑 Cancelling task {task_id} in {task['status']} state")

        # Update task status immediately
        await db.tasks.update_one(
            {"_id": ObjectId(task_id)},
            {
                "$set": {
                    "status": "error",
                    "error": "Task cancelled by user",
                    "completedAt": datetime.utcnow()
                }
            }
        )

        print(f"✅ Task {task_id} cancelled")
        
        return {
            "success": True,
            "message": "Task cancelled successfully"
        }

    except Exception as e:
        print(f"Error cancelling task: {e}")
        raise HTTPException(
            status_code=500,
            detail=str(e)
        )