# worker.py
# Add at the top of worker.py
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

    async def start(self):
        """Main worker loop."""
        print("🚀 Starting screening worker...")
        self.running = True
        self._shutdown.clear()

        last_config_check = 0.0

        while not self._shutdown.is_set():
            try:
                # ── 1. Reload config if changed ────────────────────────────────
                now_ts = time.time()
                if now_ts - last_config_check >= self._config_check_interval:
                    if settings.reload_if_changed():
                        print("⚡ Worker detected configuration changes")
                    last_config_check = now_ts

                # ── 2. Find runnable tasks (running / full_screening) ─────────
                tasks = await self.db.tasks.find(
                    {
                        "status": {"$in": ["running", "full_screening"]},
                        "_id": {
                            "$nin": [ObjectId(tid) for tid in self._processing_tasks]
                        },
                        "startedAt": {
                            "$gte": datetime.utcnow() - timedelta(days=1)
                        },
                    }
                ).sort("startedAt", 1).to_list(length=10)

                # ── 3. If none found, idle‑wait; if a task is paused, log it ──
                if not tasks:
                    paused_tasks = await self.db.tasks.find(
                        {"status": "paused"},
                        projection={"_id": 1}
                    ).to_list(length=None)

                    if paused_tasks:
                        if not hasattr(self, "_last_paused_log_time"):
                            self._last_paused_log_time = 0.0
                        if now_ts - self._last_paused_log_time > 60:
                            ids = ", ".join(str(t["_id"]) for t in paused_tasks)
                            print(
                                f"⏸️  Detected paused task(s): [{ids}] — "
                                "waiting for full‑screening request…"
                            )
                            self._last_paused_log_time = now_ts

                    await asyncio.sleep(self._config_check_interval)
                    continue

                # ── 4. Process each eligible task ────────────────────────────
                for task in tasks:
                    if self._shutdown.is_set():
                        break

                    task_id = str(task["_id"])
                    if task_id in self._processing_tasks:
                        continue

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

                        self._processing_tasks.add(task_id)
                        self.current_task = task_id
                        print(f"📝 Processing task: {task_id} (Status: {current['status']})")
                        await self.task_processor.process(task_id)

                        # Check if task was paused - if so, log it
                        updated_task = await self.db.tasks.find_one({"_id": ObjectId(task_id)})
                        if updated_task and updated_task.get("status") == "paused":
                            print(f"⏸️ Task {task_id} was paused at {updated_task.get('progress', {}).get('current', 0)} articles")

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
                        self._processing_tasks.discard(task_id)
                        if self.current_task == task_id:
                            self.current_task = None

            except Exception as loop_err:
                print(f"❌ Worker error: {loop_err}")
                await asyncio.sleep(5)

        print("👋 Worker stopped")

    async def stop(self):
        """Gracefully stop the worker."""
        print("🛑 Stopping worker...")
        self._shutdown.set()
        self.running = False

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