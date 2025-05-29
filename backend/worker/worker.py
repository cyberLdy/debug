# worker.py
from datetime import datetime, timedelta
from typing import Optional, Set
import asyncio
import time
from datetime import datetime, timedelta
from typing import Optional

from bson import ObjectId
from motor.motor_asyncio import AsyncIOMotorDatabase

from .tasks import TaskProcessor
from config import settings


class Worker:
    def __init__(self, db: AsyncIOMotorDatabase):
        self.db = db
        self.running = False
        self.current_task: Optional[str] = None
        self.task_processor = TaskProcessor(db)
        self._shutdown = asyncio.Event()
        self._processing_tasks: set[str] = set()        # Track tasks in flight
        self._error_counts: dict[str, int] = {}         # Track per‑task errors
        self._config_check_interval = 5                 # Seconds
        self._initialized = False                       # Track initialization state
        self._last_task_check = 0.0                    # Track last time we checked for tasks

    async def start(self):
        """Main worker loop."""
        print("🚀 Starting screening worker...")
        self.running = True
        self._shutdown.clear()

        # Pre-initialize components
        if not self._initialized:
            try:
                print("⚙️ Pre-initializing worker components...")
                # Initialize LLM service
                await self.task_processor.screening_service.llm_service.initialize()
                print("✅ LLM service initialized")
                
                # Wait a moment to ensure everything is ready
                await asyncio.sleep(1)
                self._initialized = True
                print("✅ Worker fully initialized and ready")
            except Exception as e:
                print(f"⚠️ Warning: Error during pre-initialization: {e}")
                # Continue anyway - non-fatal

        last_config_check = 0.0
        last_paused_log = 0.0

        while not self._shutdown.is_set():
            try:
                now_ts = time.time()
                
                # ── 1. Reload config if changed ────────────────────────────────
                if now_ts - last_config_check >= self._config_check_interval:
                    if settings.reload_if_changed():
                        print("⚡ Worker detected configuration changes")
                    last_config_check = now_ts

                # ── 2. Find runnable tasks (running / full_screening) ─────────
                # Add a small delay to prevent rapid polling
                if now_ts - self._last_task_check < 2.0:
                    await asyncio.sleep(1)
                    continue
                    
                self._last_task_check = now_ts

                # Check for tasks that need processing
                tasks = await self.db.tasks.find(
                    {
                        "status": {"$in": ["running", "full_screening"]},
                        "_id": {
                            "$nin": [ObjectId(tid) for tid in self._processing_tasks]
                        }
                    }
                ).sort("startedAt", 1).to_list(length=10)

                # ── 3. If none found, check for paused tasks and idle ──────────
                if not tasks:
                    # Check for paused tasks periodically
                    if now_ts - last_paused_log > 60:  # Log every 60 seconds
                        paused_tasks = await self.db.tasks.count_documents({"status": "paused"})
                        if paused_tasks > 0:
                            print(f"⏸️  {paused_tasks} task(s) in paused state, waiting for full-screening request...")
                            last_paused_log = now_ts
                    
                    await asyncio.sleep(self._config_check_interval)
                    continue

                # ── 4. Process each eligible task ────────────────────────────
                for task in tasks:
                    if self._shutdown.is_set():
                        break

                    task_id = str(task["_id"])
                    
                    # Skip if already processing
                    if task_id in self._processing_tasks:
                        continue

                    # Skip if too many errors
                    if self._error_counts.get(task_id, 0) >= 3:
                        print(f"⚠️ Task {task_id} has too many errors, marking as failed")
                        await self._mark_task_failed(task_id, "Too many processing attempts")
                        continue

                    try:
                        # Double‑check status still valid
                        current = await self.db.tasks.find_one(
                            {"_id": ObjectId(task_id), "status": {"$in": ["running", "full_screening"]}}
                        )
                        if not current:
                            continue

                        # Mark as processing
                        self._processing_tasks.add(task_id)
                        self.current_task = task_id
                        
                        print(f"\n📝 Worker {id(self)} processing task: {task_id}")
                        print(f"   Status: {current['status']}")
                        print(f"   Progress: {current.get('progress', {})}")
                        
                        # Ensure initialization before each task (safety check)
                        if not self._initialized:
                            print("⚙️ Initializing components before processing task...")
                            await self.task_processor.screening_service.llm_service.initialize()
                            self._initialized = True
                        
                        # Process the task
                        await self.task_processor.process(task_id)
                        
                        # Reset error count on successful processing
                        self._error_counts.pop(task_id, None)

                        # Check final status
                        final_task = await self.db.tasks.find_one({"_id": ObjectId(task_id)})
                        if final_task:
                            final_status = final_task.get("status")
                            if final_status == "paused":
                                print(f"✅ Task {task_id} successfully paused")
                                print(f"   Progress: {final_task.get('progress', {})}")
                            elif final_status == "done":
                                print(f"✅ Task {task_id} completed successfully")
                            elif final_status == "error":
                                print(f"❌ Task {task_id} ended with error: {final_task.get('error')}")

                    except Exception as e:
                        print(f"❌ Error processing task {task_id}: {e}")
                        self._error_counts[task_id] = self._error_counts.get(task_id, 0) + 1
                        
                        if self._error_counts[task_id] >= 3:
                            await self._mark_task_failed(task_id, str(e))
                        else:
                            await self.db.tasks.update_one(
                                {"_id": ObjectId(task_id)},
                                {
                                    "$set": {
                                        "status": "error",
                                        "error": f"Attempt {self._error_counts[task_id]}: {e}",
                                        "completedAt": datetime.utcnow(),
                                    }
                                },
                            )
                    finally:
                        # Clean up
                        self._processing_tasks.discard(task_id)
                        if self.current_task == task_id:
                            self.current_task = None

                # Small delay before next check
                await asyncio.sleep(1)

            except Exception as loop_err:
                print(f"❌ Worker loop error: {loop_err}")
                await asyncio.sleep(5)

        print("👋 Worker stopped")

    async def stop(self):
        """Gracefully stop the worker."""
        print("🛑 Stopping worker...")
        self._shutdown.set()
        self.running = False

        # Clean up resources
        try:
            await self.task_processor.screening_service.llm_service.cleanup()
            print("✅ LLM service cleaned up")
        except Exception as e:
            print(f"⚠️ Error cleaning up LLM service: {e}")

        # Cancel any tasks in progress
        for task_id in list(self._processing_tasks):
            print(f"🔄 Cancelling task: {task_id}")
            self.task_processor.cancel()
            
            await self.db.tasks.update_one(
                {"_id": ObjectId(task_id)},
                {
                    "$set": {
                        "status": "error",
                        "error": "Worker stopped",
                        "completedAt": datetime.utcnow(),
                    }
                },
            )
            print(f"🧹 Stopped task {task_id}")
            self._processing_tasks.discard(task_id)

    async def _mark_task_failed(self, task_id: str, error_message: str):
        """Mark a task as permanently failed."""
        try:
            await self.db.tasks.update_one(
                {"_id": ObjectId(task_id)},
                {
                    "$set": {
                        "status": "error",
                        "error": f"Task failed permanently: {error_message}",
                        "completedAt": datetime.utcnow(),
                    }
                },
            )
            self._error_counts.pop(task_id, None)
            print(f"❌ Task {task_id} marked as permanently failed")
        except Exception as e:
            print(f"Error marking task as failed: {e}")