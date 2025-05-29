import asyncio
from datetime import datetime, timedelta
from typing import Optional, Set
from motor.motor_asyncio import AsyncIOMotorDatabase
from bson import ObjectId
from .tasks import TaskProcessor
from config import settings
import time
import uuid

class Worker:
    def __init__(self, db: AsyncIOMotorDatabase):
        self.db = db
        self.running = False
        self.current_task: Optional[str] = None
        self.task_processor = TaskProcessor(db)
        self._shutdown = asyncio.Event()
        self._processing_tasks: Set[str] = set()  # Track tasks being processed
        self._error_counts = {}  # Track error counts per task
        self._config_check_interval = 5  # Check config every 5 seconds
        self._worker_id = str(uuid.uuid4())[:8]  # Unique worker ID
        self._active_processors = {}  # Track active processors

    async def start(self):
        """Start the worker process"""
        print(f"🚀 Starting screening worker {self._worker_id}...")
        self.running = True
        self._shutdown.clear()
        
        last_config_check = 0
        
        while not self._shutdown.is_set():
            try:
                # Check for configuration changes
                current_time = time.time()
                if current_time - last_config_check >= self._config_check_interval:
                    if settings.reload_if_changed():
                        print(f"⚡ Worker {self._worker_id} detected configuration changes")
                    last_config_check = current_time

                # Find tasks that need processing
                # Use findOneAndUpdate to atomically claim a task
                task = await self.db.tasks.find_one_and_update(
                    {
                        "status": "running",
                        "$or": [
                            {"workerClaim": None},
                            {"workerClaim": {"$exists": False}},
                            {
                                "workerClaim.claimedAt": {
                                    "$lt": datetime.utcnow() - timedelta(minutes=5)
                                }
                            }
                        ],
                        "_id": {
                            "$nin": [ObjectId(tid) for tid in self._processing_tasks]
                        }
                    },
                    {
                        "$set": {
                            "workerClaim": {
                                "workerId": self._worker_id,
                                "claimedAt": datetime.utcnow()
                            }
                        }
                    },
                    return_document=True
                )

                if not task:
                    # No tasks to process, wait before checking again
                    await asyncio.sleep(2)
                    continue

                task_id = str(task["_id"])
                
                # Skip if task has too many errors
                if self._error_counts.get(task_id, 0) >= 3:
                    print(f"⚠️ Worker {self._worker_id}: Task {task_id} has too many errors, marking as failed")
                    await self._mark_task_failed(task_id, "Too many processing attempts")
                    # Release the claim
                    await self._release_task_claim(task_id)
                    continue

                try:
                    # Double-check task is still valid
                    current_task = await self.db.tasks.find_one({
                        "_id": ObjectId(task_id),
                        "status": "running",
                        "workerClaim.workerId": self._worker_id
                    })
                    
                    if not current_task:
                        print(f"Worker {self._worker_id}: Task {task_id} no longer available")
                        continue

                    # Mark task as being processed
                    self._processing_tasks.add(task_id)
                    self.current_task = task_id

                    print(f"📝 Worker {self._worker_id} processing task: {task_id}")
                    
                    # Create a task processor for this specific task
                    processor = TaskProcessor(self.db)
                    self._active_processors[task_id] = processor
                    
                    await processor.process(task_id)

                except asyncio.CancelledError:
                    print(f"🛑 Worker {self._worker_id}: Processing cancelled for task {task_id}")
                    raise
                except Exception as e:
                    print(f"❌ Worker {self._worker_id} error processing task {task_id}: {e}")
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
                                },
                                "$unset": {
                                    "workerClaim": ""
                                }
                            }
                        )
                finally:
                    # Clean up
                    self._processing_tasks.discard(task_id)
                    self._active_processors.pop(task_id, None)
                    if self.current_task == task_id:
                        self.current_task = None
                    # Release the claim
                    await self._release_task_claim(task_id)

            except asyncio.CancelledError:
                print(f"🛑 Worker {self._worker_id} cancelled")
                break
            except Exception as e:
                print(f"❌ Worker {self._worker_id} error: {e}")
                await asyncio.sleep(5)

        print(f"👋 Worker {self._worker_id} stopped")

    async def stop(self):
        """Stop the worker process gracefully"""
        print(f"🛑 Stopping worker {self._worker_id}...")
        self._shutdown.set()
        self.running = False
        
        # Cancel all active processors
        for task_id, processor in list(self._active_processors.items()):
            print(f"🔄 Worker {self._worker_id} cancelling task: {task_id}")
            
            # Stop task processor
            processor.cancel()
            
            # Mark task as error and release claim
            await self.db.tasks.update_one(
                {"_id": ObjectId(task_id)},
                {
                    "$set": {
                        "status": "error",
                        "error": f"Worker {self._worker_id} stopped",
                        "completedAt": datetime.utcnow()
                    },
                    "$unset": {
                        "workerClaim": "",
                        "processingLock": ""
                    }
                }
            )
            
            print(f"🧹 Worker {self._worker_id} stopped task {task_id}")
            self._processing_tasks.discard(task_id)

    async def _release_task_claim(self, task_id: str):
        """Release the worker's claim on a task"""
        try:
            await self.db.tasks.update_one(
                {
                    "_id": ObjectId(task_id),
                    "workerClaim.workerId": self._worker_id
                },
                {
                    "$unset": {"workerClaim": ""}
                }
            )
        except Exception as e:
            print(f"Error releasing task claim: {e}")

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
                    },
                    "$unset": {
                        "workerClaim": "",
                        "processingLock": ""
                    }
                }
            )
            # Clean up error tracking
            self._error_counts.pop(task_id, None)
            print(f"❌ Worker {self._worker_id}: Task {task_id} marked as permanently failed")
        except Exception as e:
            print(f"Error marking task as failed: {e}")