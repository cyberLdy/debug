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
        
        # Determine final status
        if current_status == "full_screening":
            # Full screening completed
            final_status = "done"
            completion_time = datetime.utcnow()
            print(f"✅ Full screening completed - {final_processed} articles processed")
        elif current_status == "running":
            # Initial screening - should have been paused in the processor
            # This is a fallback
            if final_processed >= settings.ARTICLE_LIMIT:
                final_status = "paused"
                completion_time = None
                print(f"⏸️ Initial screening completed - pausing at {final_processed} articles")
            else:
                # Check if we processed all available articles
                total_articles = await self.db.articles.count_documents({"taskId": task_id})
                if final_processed >= total_articles:
                    final_status = "done"
                    completion_time = datetime.utcnow()
                    print(f"✅ All articles processed - {final_processed} articles")
                else:
                    # Unexpected state
                    final_status = "paused"
                    completion_time = None
                    print(f"⚠️ Unexpected state - pausing at {final_processed} articles")
        else:
            # Unknown status
            print(f"⚠️ Unknown status: {current_status}")
            return

        # Update task
        update = {
            "status": final_status,
            "currentArticle": None,
            "progress.current": final_processed
        }

        if completion_time:
            update["completedAt"] = completion_time

        await self.db.tasks.update_one(
            {"_id": ObjectId(task_id)},
            {"$set": update}
        )

        print(f"✅ Task {task_id} finalized as {final_status}")
        print(f"   Final progress: {final_processed} articles")

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