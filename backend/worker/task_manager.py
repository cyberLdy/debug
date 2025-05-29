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
        
        # Get task to verify totals and get actual progress from DB
        task = await self.load_task(task_id)
        if not task:
            print("❌ Task not found during finalization")
            return

        # Get actual processed count from database (source of truth)
        actual_processed = await self.db.screening_results.count_documents({"taskId": task_id})
        print(f"📊 Actual processed count from DB: {actual_processed}")
        print(f"📊 Local processed count: {total_processed}")
        
        # Use the higher value (in case of discrepancy)
        final_processed = max(actual_processed, total_processed)
        
        # Get current task status
        current_status = task.get("status", "running")
        
        # If task is already paused, don't change it
        if current_status == "paused":
            print("✅ Task already paused, no changes needed")
            return
        
        # Determine final status based on current status and progress
        if current_status == "full_screening":
            final_status = "done"
            completion_time = datetime.utcnow()
        elif current_status == "running":
            # Check against the limit, not total articles
            if final_processed >= settings.ARTICLE_LIMIT:
                final_status = "paused"
                completion_time = None
                print(f"⏸️ Setting status to paused (processed: {final_processed}, limit: {settings.ARTICLE_LIMIT})")
            else:
                # Only mark as done if we processed all available articles
                total_articles = await self.db.articles.count_documents({"taskId": task_id})
                if final_processed >= total_articles:
                    final_status = "done"
                    completion_time = datetime.utcnow()
                else:
                    # Still have articles but stopped for some reason
                    final_status = "paused"
                    completion_time = None
        else:
            # Preserve current status if not running/full_screening
            final_status = current_status
            completion_time = datetime.utcnow() if final_status == "done" else None

        # Update task
        update = {
            "status": final_status,
            "currentArticle": None,
            "progress.current": final_processed
        }

        # For initial screening that reaches limit, ensure total is set correctly
        if final_status == "paused" and current_status == "running":
            update["progress.total"] = settings.ARTICLE_LIMIT

        if completion_time:
            update["completedAt"] = completion_time

        # Use atomic update to ensure we only update if status hasn't changed
        result = await self.db.tasks.update_one(
            {
                "_id": ObjectId(task_id),
                "status": {"$ne": "paused"}  # Don't update if already paused
            },
            {"$set": update}
        )

        if result.modified_count == 1:
            print(f"✅ Task {task_id} marked as {final_status}")
            print(f"Final progress: {final_processed}/{task.get('progress', {}).get('total', 0)} articles")
        else:
            print(f"⚠️ Task {task_id} status was already updated, no changes made")
            # Re-load task to log current status
            current_task = await self.load_task(task_id)
            if current_task:
                print(f"Current task status is: {current_task.get('status', 'unknown')}")
                print(f"Current progress: {current_task.get('progress', {})}")

    async def mark_task_error(self, task_id: str, error_message: str):
        """Mark task as error with message"""
        print(f"\n❌ Marking task {task_id} as error: {error_message}")
        result = await self.db.tasks.update_one(
            {
                "_id": ObjectId(task_id),
                "status": {"$nin": ["done"]}  # Don't override done status
            },
            {
                "$set": {
                    "status": "error",
                    "error": error_message,
                    "completedAt": datetime.utcnow(),
                    "currentArticle": None
                }
            }
        )
        
        if result.modified_count == 0:
            print(f"⚠️ Could not update task {task_id} to error state - status may have been changed externally")
            current_task = await self.load_task(task_id)
            if current_task:
                print(f"Current task status is: {current_task.get('status', 'unknown')}")