import asyncio
import json
from datetime import datetime
from motor.motor_asyncio import AsyncIOMotorDatabase
from bson import ObjectId
from typing import List, Dict, Any, Optional
from config import settings
from api.services.screening import ScreeningService
from api.services.prompts import build_screening_prompt
from api.models import TaskStatus

class TaskProcessor:
    def __init__(self, db: AsyncIOMotorDatabase):
        self.db = db
        self.screening_service = ScreeningService()
        self._cancelled = False
        self._current_task: asyncio.Task | None = None
        self._task_lock: Optional[str] = None

    def cancel(self):
        """Cancel the current task processing"""
        print("🛑 TaskProcessor: Cancelling task")
        self._cancelled = True
        self.screening_service.llm_service.cancel()
        if self._current_task and not self._current_task.done():
            print("🛑 TaskProcessor: Cancelling current batch")
            self._current_task.cancel()

    async def acquire_task_lock(self, task_id: str) -> bool:
        """Acquire a lock for processing a task"""
        try:
            # Use findOneAndUpdate to atomically acquire lock
            result = await self.db.tasks.find_one_and_update(
                {
                    "_id": ObjectId(task_id),
                    "status": TaskStatus.running,
                    "$or": [
                        {"processingLock": None},
                        {"processingLock": {"$exists": False}}
                    ]
                },
                {
                    "$set": {
                        "processingLock": self._task_lock,
                        "processingStartedAt": datetime.utcnow()
                    }
                },
                return_document=True
            )
            
            return result is not None
        except Exception as e:
            print(f"Error acquiring task lock: {e}")
            return False

    async def release_task_lock(self, task_id: str):
        """Release the task processing lock"""
        try:
            await self.db.tasks.update_one(
                {"_id": ObjectId(task_id)},
                {
                    "$unset": {
                        "processingLock": "",
                        "processingStartedAt": ""
                    }
                }
            )
        except Exception as e:
            print(f"Error releasing task lock: {e}")

    async def process(self, task_id: str):
        """Process a screening task with article limit"""
        # Generate unique lock ID for this processor instance
        import uuid
        self._task_lock = str(uuid.uuid4())
        
        try:
            # Try to acquire lock
            if not await self.acquire_task_lock(task_id):
                print(f"Could not acquire lock for task {task_id}")
                return

            print(f"🔒 Acquired lock for task {task_id}")

            # Get task details
            task = await self.db.tasks.find_one({"_id": ObjectId(task_id)})
            if not task:
                print(f"Task {task_id} not found")
                return

            # Verify task is in correct state
            if task["status"] != TaskStatus.running:
                print(f"Task {task_id} is in wrong state: {task['status']}")
                return

            # Get article count and limit
            total_article_count = await self.db.articles.count_documents({"taskId": task_id})
            if total_article_count == 0:
                await self._mark_task_error(task_id, "No articles found for task")
                return

            print(f"📋 Total articles in database: {total_article_count}")
            
            # Apply article limit
            article_limit = settings.ARTICLE_LIMIT
            articles_to_process = min(total_article_count, article_limit)
            
            print(f"📝 Processing limit: {article_limit}")
            print(f"📝 Articles to process: {articles_to_process}")

            # Get articles with limit
            articles = await self.db.articles.find({"taskId": task_id})\
                .limit(articles_to_process)\
                .to_list(length=articles_to_process)
            
            print(f"✅ Loaded {len(articles)} articles for processing")

            # Update task with correct totals
            await self.db.tasks.update_one(
                {"_id": ObjectId(task_id)},
                {
                    "$set": {
                        "progress.total": articles_to_process,
                        "progress.current": 0,
                        "remainingArticles": total_article_count - articles_to_process,
                        "error": None
                    }
                }
            )

            total_processed = 0
            retry_count = 0

            while total_processed < len(articles):
                # Check if task was cancelled or taken over
                if self._cancelled:
                    print(f"🛑 Task {task_id} cancelled during processing")
                    return

                # Verify we still hold the lock
                current_task = await self.db.tasks.find_one({
                    "_id": ObjectId(task_id),
                    "processingLock": self._task_lock
                })
                
                if not current_task:
                    print(f"Task {task_id} lock lost, stopping processing")
                    return

                if current_task["status"] == TaskStatus.error:
                    print(f"Task {task_id} was cancelled or errored, stopping processing")
                    return

                try:
                    # Get next batch
                    batch = articles[total_processed:total_processed + settings.BATCH_SIZE]
                    
                    print(f"🔄 Processing batch {total_processed // settings.BATCH_SIZE + 1}, articles {total_processed + 1}-{total_processed + len(batch)}")
                    
                    # Format batch for screening
                    formatted_batch = [{
                        "id": article["articleId"],
                        "title": article["title"],
                        "abstract": article["abstract"]
                    } for article in batch]
                    
                    # Create a task for batch processing
                    self._current_task = asyncio.create_task(
                        self._screen_batch(formatted_batch, task["criteria"], task["model"]),
                        name=f"batch_{total_processed}"
                    )

                    try:
                        results = await self._current_task
                    except asyncio.CancelledError:
                        print(f"🛑 Batch processing cancelled for task {task_id}")
                        return
                    finally:
                        self._current_task = None

                    # Check cancellation after batch processing
                    if self._cancelled:
                        print(f"🛑 Task {task_id} cancelled after batch processing")
                        return

                    # Verify we still hold the lock before saving results
                    current_task = await self.db.tasks.find_one({
                        "_id": ObjectId(task_id),
                        "processingLock": self._task_lock
                    })
                    
                    if not current_task or current_task["status"] == TaskStatus.error:
                        print(f"Task {task_id} lock lost or status changed, halting processing")
                        return

                    if results:
                        # Process each article in the batch
                        batch_processed = 0
                        for article in batch:
                            if self._cancelled:
                                print(f"🛑 Task {task_id} cancelled during result save")
                                return

                            article_id = article["articleId"]
                            if article_id in results:
                                result = results[article_id]
                                
                                # Create document for insert/update
                                doc = {
                                    "taskId": task_id,
                                    "articleId": article_id,
                                    "included": bool(result["included"]),
                                    "reason": str(result["reason"]),
                                    "relevanceScore": float(result["relevanceScore"]),
                                    "metadata": {
                                        "title": article["title"],
                                        "abstract": article["abstract"]
                                    },
                                    "updatedAt": datetime.utcnow()
                                }

                                try:
                                    await self.db.screening_results.update_one(
                                        {
                                            "taskId": task_id,
                                            "articleId": article_id
                                        },
                                        {"$set": doc},
                                        upsert=True
                                    )
                                    batch_processed += 1
                                except Exception as update_error:
                                    print(f"Error updating result: {update_error}")
                                    raise

                        total_processed += batch_processed
                        print(f"✅ Batch complete: {batch_processed} articles processed")

                        # Update task progress with lock check
                        if not self._cancelled:
                            update_result = await self.db.tasks.update_one(
                                {
                                    "_id": ObjectId(task_id),
                                    "processingLock": self._task_lock
                                },
                                {
                                    "$set": {
                                        "progress.current": total_processed
                                    }
                                }
                            )
                            
                            if update_result.modified_count == 0:
                                print(f"Failed to update progress for task {task_id}, lock may be lost")
                                return

                except Exception as batch_error:
                    print(f"Error processing batch: {batch_error}")
                    retry_count += 1
                    if retry_count > settings.MAX_RETRIES:
                        raise
                    await asyncio.sleep(settings.RETRY_DELAY * retry_count)
                    continue

            # Mark task as complete if not cancelled and we still hold the lock
            if not self._cancelled:
                print(f"🏁 Finalizing task {task_id}")
                
                # Determine final status
                final_status = TaskStatus.done
                remaining_count = total_article_count - articles_to_process
                
                if remaining_count > 0:
                    print(f"📊 Task completed with {remaining_count} articles remaining")
                    final_status = TaskStatus.paused
                
                update_result = await self.db.tasks.update_one(
                    {
                        "_id": ObjectId(task_id),
                        "processingLock": self._task_lock
                    },
                    {
                        "$set": {
                            "status": final_status,
                            "completedAt": datetime.utcnow(),
                            "progress.current": total_processed,
                            "remainingArticles": remaining_count
                        },
                        "$unset": {
                            "processingLock": "",
                            "processingStartedAt": ""
                        }
                    }
                )
                
                if update_result.modified_count > 0:
                    print(f"✅ Task {task_id} completed successfully")
                    print(f"📊 Final status: {final_status}")
                    print(f"📊 Articles processed: {total_processed}/{articles_to_process}")
                    print(f"📊 Articles remaining: {remaining_count}")
                else:
                    print(f"⚠️ Failed to mark task {task_id} as complete, lock may be lost")

        except Exception as e:
            print(f"❌ Error processing task {task_id}: {e}")
            await self._mark_task_error(task_id, str(e))
        finally:
            # Always release lock
            await self.release_task_lock(task_id)

    async def _mark_task_error(self, task_id: str, error_message: str):
        """Mark task as error"""
        try:
            await self.db.tasks.update_one(
                {"_id": ObjectId(task_id)},
                {
                    "$set": {
                        "status": TaskStatus.error,
                        "error": error_message,
                        "completedAt": datetime.utcnow()
                    },
                    "$unset": {
                        "processingLock": "",
                        "processingStartedAt": ""
                    }
                }
            )
            print(f"✅ Task {task_id} marked as error: {error_message}")
        except Exception as e:
            print(f"Error marking task as error: {e}")

    async def _screen_batch(
        self,
        articles: List[Dict],
        criteria: str,
        model: str
    ) -> Dict[str, Any]:
        """Screen a batch of articles using LLM"""
        if self._cancelled:
            return {}

        prompt = build_screening_prompt(articles, criteria)
        
        if self._cancelled:
            return {}

        try:
            response = await self.screening_service.llm_service.generate_response(prompt, model)
            
            if self._cancelled:
                return {}

            results = json.loads(response)
            if not isinstance(results, dict):
                raise ValueError("Results must be a dictionary")
            
            validated_results = {}
            for article_id, result in results.items():
                if not isinstance(result, dict):
                    print(f"Invalid result format for article {article_id}: {result}")
                    continue

                included = result.get("included")
                if included is None:
                    excluded = result.get("excluded")
                    if excluded is not None:
                        included = not excluded
                    else:
                        included = False

                validated_result = {
                    "included": bool(included),
                    "reason": str(result.get("reason", "")),
                    "relevanceScore": float(result.get("relevanceScore", 0))
                }

                if not 0 <= validated_result["relevanceScore"] <= 100:
                    validated_result["relevanceScore"] = max(0, min(100, validated_result["relevanceScore"]))

                validated_results[article_id] = validated_result

            return validated_results

        except asyncio.CancelledError:
            print("🛑 Batch processing cancelled")
            raise
        except json.JSONDecodeError as e:
            print(f"Error parsing screening response: {str(e)}")
            print(f"Raw response: {response}")
            raise ValueError(f"Failed to parse screening response: {str(e)}")
        except Exception as e:
            print(f"Error in batch processing: {str(e)}")
            raise