# worker/tasks.py
import asyncio
from datetime import datetime
from motor.motor_asyncio import AsyncIOMotorDatabase
from bson import ObjectId
from config import settings
from .article_processor import ArticleProcessor
from .screening_service import ScreeningService
from .task_manager import TaskManager

class TaskProcessor:
    def __init__(self, db: AsyncIOMotorDatabase):
        self.db = db
        self._cancelled = False
        self._current_task: asyncio.Task | None = None
        
        # Initialize components
        self.article_processor = ArticleProcessor(db)
        self.screening_service = ScreeningService()
        self.task_manager = TaskManager(db)

    def cancel(self):
        """Cancel the current task processing"""
        print("🛑 TaskProcessor: Cancelling task")
        self._cancelled = True
        if self._current_task and not self._current_task.done():
            print("🛑 TaskProcessor: Cancelling current batch")
            self._current_task.cancel()

    async def process(self, task_id: str):
        """Process a screening task"""
        try:
            # Ensure LLM service is initialized before processing
            await self.screening_service.llm_service.initialize()
            
            # Load task
            task = await self.task_manager.load_task(task_id)
            if not task:
                print(f"Task {task_id} not found")
                return

            # Verify task is in a processable state
            if task["status"] not in ["running", "full_screening"]:
                print(f"Task {task_id} is in {task['status']} state, skipping processing")
                return

            # Lock the task for processing
            locked_task = await self.db.tasks.find_one_and_update(
                {
                    "_id": ObjectId(task_id),
                    "status": {"$in": ["running", "full_screening"]},
                    "processingLock": {"$exists": False}  # Ensure not already locked
                },
                {
                    "$set": {
                        "processingLock": True,
                        "lastActivityAt": datetime.utcnow()
                    }
                },
                return_document=True
            )

            if not locked_task:
                current_task = await self.db.tasks.find_one({"_id": ObjectId(task_id)})
                if current_task and current_task.get("processingLock"):
                    print(f"Task {task_id} is already being processed")
                else:
                    print(f"Could not lock task {task_id} for processing - status may have changed")
                return

            try:
                # Clear any previous errors
                await self.task_manager.clear_task_errors(task_id)

                # Prepare articles for processing
                articles_to_process, total_expected = await self.article_processor.prepare_articles(locked_task)

                if not articles_to_process:
                    print("No articles to process")
                    await self.task_manager.finalize_task(task_id, 0)
                    return

                # Process articles in batches
                batch_size = settings.BATCH_SIZE
                total_processed = 0
                retry_count = 0

                # Calculate total batches
                total_batches = (len(articles_to_process) + batch_size - 1) // batch_size

                for batch_num in range(total_batches):
                    if self._cancelled:
                        print(f"🛑 Task {task_id} cancelled during processing")
                        return

                    # Check task status before processing batch - use atomic operation
                    current_task = await self.db.tasks.find_one_and_update(
                        {
                            "_id": ObjectId(task_id),
                            "status": {"$in": ["running", "full_screening"]},
                            "processingLock": True
                        },
                        {
                            "$set": {"lastActivityAt": datetime.utcnow()}
                        },
                        return_document=True
                    )

                    if not current_task:
                        print(f"Task {task_id} lock lost or status changed, halting processing")
                        break
                        
                    if current_task["status"] not in ["running", "full_screening"]:
                        print(f"Task status changed to {current_task['status']}, halting processing")
                        break

                    # Get batch
                    start_idx = batch_num * batch_size
                    end_idx = min(start_idx + batch_size, len(articles_to_process))
                    batch = articles_to_process[start_idx:end_idx]

                    print(f"\n📦 Processing batch {batch_num + 1}/{total_batches}")
                    print(f"Batch size: {len(batch)} articles")
                    print(f"Current progress: {total_processed}/{total_expected}")

                    try:
                        # Format articles for screening
                        formatted = [
                            {
                                "id": art["articleId"],
                                "title": art["title"],
                                "abstract": art["abstract"]
                            } for art in batch
                        ]

                        # Screen batch
                        results = await self.screening_service.screen_batch(
                            formatted,
                            current_task["criteria"],
                            current_task["model"]
                        )

                        # Save results and update progress atomically
                        total_processed = await self.article_processor.save_batch_results(
                            task_id,
                            batch,
                            results,
                            total_processed,
                            total_expected
                        )

                        # Check if we should continue processing
                        # Re-check task status after batch processing
                        updated_task = await self.db.tasks.find_one({"_id": ObjectId(task_id)})
                        if not updated_task:
                            print(f"Task {task_id} no longer exists, halting processing")
                            break
                            
                        if updated_task["status"] == "paused":
                            print(f"⏸️ Task {task_id} was paused during batch processing")
                            break
                            
                        if updated_task["status"] not in ["running", "full_screening"]:
                            print(f"Task status changed to {updated_task['status']}, halting processing")
                            break

                        if total_processed >= settings.ARTICLE_LIMIT and updated_task["status"] == "running":
                            print(f"⏸️ Reached article limit ({settings.ARTICLE_LIMIT}), pausing processing")
                            break

                    except Exception as batch_err:
                        print(f"Error processing batch: {batch_err}")
                        retry_count += 1
                        if retry_count > settings.MAX_RETRIES:
                            raise
                        await asyncio.sleep(settings.RETRY_DELAY * retry_count)
                        continue

                # Finalize task with accurate progress
                await self.task_manager.finalize_task(task_id, total_processed)

            finally:
                # Always ensure the lock is released
                await self.db.tasks.update_one(
                    {"_id": ObjectId(task_id)},
                    {"$unset": {"processingLock": "", "lastActivityAt": ""}}
                )

        except Exception as e:
            print(f"Error processing task {task_id}: {e}")
            # Attempt to release lock and mark as error
            try:
                await self.db.tasks.update_one(
                    {"_id": ObjectId(task_id)},
                    {"$unset": {"processingLock": "", "lastActivityAt": ""}}
                )
                await self.task_manager.mark_task_error(task_id, str(e))
            except Exception as cleanup_err:
                print(f"Error during cleanup: {cleanup_err}")
        finally:
            # Always cleanup LLM service
            try:
                await self.screening_service.llm_service.cleanup()
            except:
                pass