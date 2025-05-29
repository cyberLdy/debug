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
                # Clear any previous errors but preserve progress
                await self.task_manager.clear_task_errors(task_id, preserve_progress=True)

                # Get current progress before preparing articles
                current_progress = locked_task.get("progress", {}).get("current", 0)
                
                # Get batch size
                batch_size = settings.BATCH_SIZE
                print(f"📝 Batch size: {batch_size} articles")
                
                # If we already have progress, adjust articles to process
                if current_progress > 0 and locked_task["status"] == "running":
                    print(f"📊 Resuming from progress: {current_progress}")
                    
                    # First, ensure the task has the correct total set
                    await self.db.tasks.update_one(
                        {"_id": ObjectId(task_id)},
                        {"$set": {"progress.total": settings.ARTICLE_LIMIT}}
                    )
                    
                    # Skip already processed articles
                    articles = await self.db.articles.find({"taskId": task_id}).to_list(length=None)
                    actual_total = len(articles)
                    
                    # Get processed article IDs
                    processed_results = await self.db.screening_results.find(
                        {"taskId": task_id},
                        {"articleId": 1}
                    ).to_list(length=None)
                    processed_ids = {r["articleId"] for r in processed_results}
                    
                    # Filter out processed articles
                    articles_to_process = [a for a in articles if a["articleId"] not in processed_ids]
                    
                    # Limit to remaining articles within ARTICLE_LIMIT
                    remaining_limit = settings.ARTICLE_LIMIT - current_progress
                    print(f"📊 Remaining limit: {remaining_limit} (limit: {settings.ARTICLE_LIMIT}, current: {current_progress})")
                    
                    if remaining_limit > 0:
                        articles_to_process = articles_to_process[:remaining_limit]
                        print(f"📊 Will process {len(articles_to_process)} more articles")
                    else:
                        # Already at limit, should pause
                        print(f"⏸️ Already at article limit, pausing task")
                        await self.db.tasks.update_one(
                            {"_id": ObjectId(task_id)},
                            {
                                "$set": {
                                    "status": "paused",
                                    "progress.total": settings.ARTICLE_LIMIT,
                                    "progress.current": current_progress
                                }
                            }
                        )
                        return
                    
                    total_expected = settings.ARTICLE_LIMIT
                    total_processed = current_progress
                else:
                    # Fresh start or full screening
                    articles_to_process, total_expected, actual_total = await self.article_processor.prepare_articles(locked_task)
                    total_processed = 0

                if not articles_to_process:
                    print("No articles to process")
                    await self.task_manager.finalize_task(task_id, total_processed)
                    return

                # Process articles in batches
                retry_count = 0

                # Calculate total batches
                total_batches = (len(articles_to_process) + batch_size - 1) // batch_size
                
                print(f"\nProcessing starting:")

                for batch_num in range(total_batches):
                    if self._cancelled:
                        print(f"🛑 Task {task_id} cancelled during processing")
                        return

                    # Check if we should stop processing (for initial screening)
                    if locked_task["status"] == "running" and total_processed >= settings.ARTICLE_LIMIT:
                        print(f"⏸️ Reached article limit, stopping processing")
                        break

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

                    # For initial screening, ensure we don't exceed limit
                    if current_task["status"] == "running":
                        remaining_limit = settings.ARTICLE_LIMIT - total_processed
                        if len(batch) > remaining_limit:
                            batch = batch[:remaining_limit]
                            print(f"📦 Limiting batch to {len(batch)} articles to stay within limit")

                    if not batch:
                        print("No more articles to process within limit")
                        break

                    # Get actual total for display
                    if not hasattr(self, 'actual_total'):
                        self.actual_total = await self.db.articles.count_documents({"taskId": task_id})
                    
                    print(f"Current progress: {total_processed}/{self.actual_total}")

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
                            total_expected,
                            self.actual_total
                        )

                        # Reset retry count on success
                        retry_count = 0

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