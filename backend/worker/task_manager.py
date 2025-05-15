from datetime import datetime
from motor.motor_asyncio import AsyncIOMotorDatabase
from bson import ObjectId
from typing import Dict, Optional
from config import settings

class TaskManager:
    def __init__(self, db: AsyncIOMotorDatabase):
        self.db = db

    async def load_task(self, task_id: str) -> Optional[Dict]:
        """Load task from database"""
        print(f"\n📝 Loading task {task_id}")
        return await self.db.tasks.find_one({"_id": ObjectId(task_id)})

    async def update_task_status(self, task_id: str, status: str):
        """Update task status"""
        print(f"🔄 Updating task {task_id} status to {status}")
        await self.db.tasks.update_one(
            {"_id": ObjectId(task_id)},
            {"$set": {"status": status}}
        )

    async def clear_task_errors(self, task_id: str, preserve_progress: bool = False):
        """Clear any previous errors from task"""
        print(f"🧹 Clearing previous errors for task {task_id}")
        update = {
            "$set": {
                "error": None,
                "currentArticle": None
            }
        }
        
        # Only reset progress if not preserving
        if not preserve_progress:
            update["$set"]["progress.current"] = 0
            
        await self.db.tasks.update_one(
            {"_id": ObjectId(task_id)},
            update
        )

    async def finalize_task(self, task_id: str, total_processed: int):
        """Finalize task status"""
        print(f"\n🏁 Finalizing task {task_id}")
        
        # Get task to verify totals
        task = await self.load_task(task_id)
        if not task:
            print("❌ Task not found during finalization")
            return

        # Get current task status
        current_status = task.get("status", "running")
        
        # Determine final status
        if current_status == "full_screening":
            final_status = "done"
            completion_time = datetime.utcnow()
        elif current_status == "running":
            if total_processed >= settings.ARTICLE_LIMIT:
                final_status = "paused"
                completion_time = None
            else:
                final_status = "done"
                completion_time = datetime.utcnow()
        else:
            # Preserve current status if not running/full_screening
            final_status = current_status
            completion_time = datetime.utcnow() if final_status == "done" else None

        # Update task
        update = {
            "status": final_status,
            "currentArticle": None,
            "progress.current": total_processed
        }

        if completion_time:
            update["completedAt"] = completion_time

        await self.db.tasks.update_one(
            {"_id": ObjectId(task_id)},
            {"$set": update}
        )

        print(f"✅ Task {task_id} marked as {final_status}")
        print(f"Final progress: {total_processed}/{task['progress']['total']} articles")

    async def mark_task_error(self, task_id: str, error_message: str):
        """Mark task as error with message"""
        print(f"\n❌ Marking task {task_id} as error: {error_message}")
        await self.db.tasks.update_one(
            {"_id": ObjectId(task_id)},
            {
                "$set": {
                    "status": "error",
                    "error": error_message,
                    "completedAt": datetime.utcnow(),
                    "currentArticle": None
                }
            }
        )
