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

                # Get all articles for this task
                all_articles = await self.db.articles.find({"taskId": task_id}).to_list(length=None)
                total_articles_count = len(all_articles)
                print(f"📋 Total articles in database: {total_articles_count}")

                # Determine processing limit and starting point
                if locked_task["status"] == "running":
                    # Initial screening - limit to ARTICLE_LIMIT
                    processing_limit = settings.ARTICLE_LIMIT
                    print(f"📝 Processing limit: {processing_limit}")
                else:
                    # Full screening - process all articles
                    processing_limit = total_articles_count
                    print(f"📝 Full screening mode - processing all {processing_limit} articles")

                # Get already processed articles
                processed_results = await self.db.screening_results.find(
                    {"taskId": task_id},
                    {"articleId": 1}
                ).to_list(length=None)
                processed_ids = {r["articleId"] for r in processed_results}
                already_processed = len(processed_ids)
                print(f"📊 Already processed: {already_processed} articles")

                # Filter out already processed articles
                articles_to_process = [a for a in all_articles if a["articleId"] not in processed_ids]
                
                # Calculate how many more we can process within the limit
                remaining_within_limit = processing_limit - already_processed
                
                if remaining_within_limit <= 0:
                    print(f"✅ Already reached processing limit ({processing_limit} articles)")
                    if locked_task["status"] == "running":
                        # Update to paused status
                        await self.db.tasks.update_one(
                            {"_id": ObjectId(task_id)},
                            {
                                "$set": {
                                    "status": "paused",
                                    "progress.current": already_processed,
                                    "progress.total": processing_limit
                                }
                            }
                        )
                        print(f"⏸️ Task paused at limit")
                    return

                # Limit articles to process based on remaining quota
                articles_to_process = articles_to_process[:remaining_within_limit]
                print(f"📝 Articles to process: {len(articles_to_process)}")

                if not articles_to_process:
                    print("No articles to process")
                    await self.task_manager.finalize_task(task_id, already_processed)
                    return

                # Set initial progress
                await self.db.tasks.update_one(
                    {"_id": ObjectId(task_id)},
                    {
                        "$set": {
                            "progress.total": processing_limit,
                            "progress.current": already_processed,
                            "remainingArticles": [a["articleId"] for a in all_articles[processing_limit:]] if locked_task["status"] == "running" else []
                        }
                    }
                )

                # Process articles in batches
                batch_size = settings.BATCH_SIZE
                print(f"✅ Loaded {len(articles_to_process)} articles for processing")
                
                total_processed = already_processed
                
                for batch_num in range(0, len(articles_to_process), batch_size):
                    if self._cancelled:
                        print(f"🛑 Task {task_id} cancelled during processing")
                        return

                    # Check if we've reached the limit
                    if total_processed >= processing_limit:
                        print(f"✅ Reached processing limit of {processing_limit} articles")
                        break

                    # Get batch
                    batch_start = batch_num
                    batch_end = min(batch_num + batch_size, len(articles_to_process))
                    
                    # Ensure we don't exceed the limit
                    max_batch_size = processing_limit - total_processed
                    if batch_end - batch_start > max_batch_size:
                        batch_end = batch_start + max_batch_size
                    
                    batch = articles_to_process[batch_start:batch_end]
                    
                    if not batch:
                        break
                    
                    batch_number = (total_processed - already_processed) // batch_size + 1
                    print(f"🔄 Processing batch {batch_number}, articles {total_processed + 1}-{total_processed + len(batch)}")

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
                            locked_task["criteria"],
                            locked_task["model"]
                        )

                        if not results:
                            print(f"⚠️ No results returned for batch {batch_number}")
                            continue

                        # Save results
                        batch_saved = 0
                        for article in batch:
                            article_id = article["articleId"]
                            if article_id in results:
                                result = results[article_id]
                                
                                doc = {
                                    "taskId": task_id,
                                    "articleId": article_id,
                                    "included": bool(result["included"]),
                                    "reason": str(result["reason"]),
                                    "relevanceScore": float(result["relevanceScore"]),
                                    "metadata": {
                                        "title": article.get("title", ""),
                                        "abstract": article.get("abstract", "")
                                    },
                                    "updatedAt": datetime.utcnow()
                                }

                                await self.db.screening_results.update_one(
                                    {
                                        "taskId": task_id,
                                        "articleId": article_id
                                    },
                                    {"$set": doc},
                                    upsert=True
                                )
                                batch_saved += 1

                        total_processed += batch_saved
                        
                        # Update progress
                        await self.db.tasks.update_one(
                            {"_id": ObjectId(task_id)},
                            {
                                "$set": {
                                    "progress.current": total_processed
                                }
                            }
                        )
                        
                        print(f"✅ Batch {batch_number} complete. Progress: {total_processed}/{processing_limit}")

                        # Check if we've reached the limit after this batch
                        if locked_task["status"] == "running" and total_processed >= processing_limit:
                            print(f"⏸️ Reached article limit ({processing_limit}), stopping processing")
                            break

                    except Exception as batch_err:
                        print(f"❌ Error processing batch {batch_number}: {batch_err}")
                        raise

                # Final status update
                if locked_task["status"] == "running" and total_processed >= processing_limit:
                    # Pause the task
                    await self.db.tasks.update_one(
                        {"_id": ObjectId(task_id)},
                        {
                            "$set": {
                                "status": "paused",
                                "progress.current": total_processed,
                                "progress.total": processing_limit,
                                "currentArticle": None
                            }
                        }
                    )
                    print(f"⏸️ Task paused. Processed {total_processed}/{total_articles_count} articles (limit: {processing_limit})")
                else:
                    # Complete the task
                    await self.task_manager.finalize_task(task_id, total_processed)

            finally:
                # Always ensure the lock is released
                await self.db.tasks.update_one(
                    {"_id": ObjectId(task_id)},
                    {"$unset": {"processingLock": "", "lastActivityAt": ""}}
                )
                print(f"🔓 Released lock for task {task_id}")

        except Exception as e:
            print(f"❌ Error processing task {task_id}: {e}")
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