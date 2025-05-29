import asyncio
from datetime import datetime, timedelta
from typing import Optional
from motor.motor_asyncio import AsyncIOMotorDatabase
from bson import ObjectId
from .tasks import TaskProcessor
from config import settings
import time

class Worker:
    def __init__(self, db: AsyncIOMotorDatabase):
        self.db = db
        self.running = False
        self.current_task: Optional[str] = None
        self.task_processor = TaskProcessor(db)
        self._shutdown = asyncio.Event()
        self._processing_tasks = set()  # Track tasks being processed
        self._error_counts = {}  # Track error counts per task
        self._config_check_interval = 5  # Check config every 5 seconds

    async def start(self):
        """Start the worker process"""
        print("🚀 Starting screening worker...")
        self.running = True
        self._shutdown.clear()
        
        last_config_check = 0
        
        while not self._shutdown.is_set():
            try:
                # Check for configuration changes
                current_time = time.time()
                if current_time - last_config_check >= self._config_check_interval:
                    if settings.reload_if_changed():
                        print("⚡ Worker detected configuration changes")
                    last_config_check = current_time

                # Find new tasks that are not being processed
                tasks = await self.db.tasks.find({
                    "status": "running",
                    "_id": {
                        "$nin": [ObjectId(tid) for tid in self._processing_tasks]
                    },
                    "startedAt": {
                        "$gte": datetime.utcnow() - timedelta(days=1)
                    }
                }).sort("startedAt", 1).limit(10).to_list(length=10)

                if not tasks:
                    # No tasks to process, wait before checking again
                    await asyncio.sleep(2)
                    continue

                for task in tasks:
                    if self._shutdown.is_set():
                        break

                    task_id = str(task["_id"])
                    
                    # Skip if task is already being processed
                    if task_id in self._processing_tasks:
                        continue

                    # Skip if task has too many errors
                    if self._error_counts.get(task_id, 0) >= 3:
                        print(f"⚠️ Task {task_id} has too many errors, marking as failed")
                        await self._mark_task_failed(task_id, "Too many processing attempts")
                        continue

                    try:
                        # Verify task still exists and is in running state
                        current_task = await self.db.tasks.find_one({
                            "_id": ObjectId(task_id),
                            "status": "running"
                        })
                        
                        if not current_task:
                            print(f"Task {task_id} no longer available for processing")
                            continue

                        # Mark task as being processed
                        self._processing_tasks.add(task_id)
                        self.current_task = task_id

                        print(f"📝 Processing task: {task_id}")
                        await self.task_processor.process(task_id)

                    except Exception as e:
                        print(f"❌ Error processing task {task_id}: {e}")
                        # Increment error count
                        self._error_counts[task_id] = self._error_counts.get(task_id, 0) + 1
                        
                        if self._error_counts[task_id] >= 3:
                            await self._mark_task_failed(task_id, str(e))
                        else:
                            # Mark as error but allow retry
                            await self.db.tasks.update_one(
                                {"_id": ObjectId(task_id)},
                                {
                                    "$set": {
                                        "status": "error",
                                        "error": f"Attempt {self._error_counts[task_id]}: {str(e)}",
                                        "completedAt": datetime.utcnow()
                                    }
                                }
                            )
                    finally:
                        # Clean up
                        self._processing_tasks.discard(task_id)
                        if self.current_task == task_id:
                            self.current_task = None

            except Exception as e:
                print(f"❌ Worker error: {e}")
                await asyncio.sleep(5)

        print("👋 Worker stopped")

    async def stop(self):
        """Stop the worker process gracefully"""
        print("🛑 Stopping worker...")
        self._shutdown.set()
        self.running = False
        
        # Cancel all processing tasks
        for task_id in list(self._processing_tasks):
            print(f"🔄 Cancelling task: {task_id}")
            
            # Stop task processor
            self.task_processor.cancel()
            
            # Mark task as error
            await self.db.tasks.update_one(
                {"_id": ObjectId(task_id)},
                {
                    "$set": {
                        "status": "error",
                        "error": "Worker stopped",
                        "completedAt": datetime.utcnow()
                    }
                }
            )
            
            print(f"🧹 Stopped task {task_id}")
            self._processing_tasks.discard(task_id)

    async def _mark_task_failed(self, task_id: str, error_message: str):
        """Mark a task as permanently failed"""
        try:
            await self.db.tasks.update_one(
                {"_id": ObjectId(task_id)},
                {
                    "$set": {
                        "status": "error",
                        "error": f"Task failed permanently: {error_message}",
                        "completedAt": datetime.utcnow()
                    }
                }
            )
            # Clean up error tracking
            self._error_counts.pop(task_id, None)
            print(f"❌ Task {task_id} marked as permanently failed")
        except Exception as e:
            print(f"Error marking task as failed: {e}")