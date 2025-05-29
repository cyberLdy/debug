import asyncio
import json
from datetime import datetime
from motor.motor_asyncio import AsyncIOMotorDatabase
from bson import ObjectId
from typing import List, Dict, Any
from config import settings
from api.services.llm import LLMService
from api.services.prompts import build_screening_prompt
from api.models import TaskStatus

class TaskProcessor:
    def __init__(self, db: AsyncIOMotorDatabase):
        self.db = db
        self.llm_service = LLMService()
        self._cancelled = False
        self._current_task: asyncio.Task | None = None

    def cancel(self):
        """Cancel the current task processing"""
        print("🛑 TaskProcessor: Cancelling task")
        self._cancelled = True
        self.llm_service.cancel()
        if self._current_task and not self._current_task.done():
            print("🛑 TaskProcessor: Cancelling current batch")
            self._current_task.cancel()

    async def process(self, task_id: str):
        """Process a screening task"""
        try:
            # Get task details
            task = await self.db.tasks.find_one({"_id": ObjectId(task_id)})
            if not task:
                print(f"Task {task_id} not found")
                return

            # Verify task is in correct state
            if task["status"] != TaskStatus.running:
                print(f"Task {task_id} is in wrong state: {task['status']}")
                return

            # Verify articles exist
            article_count = await self.db.articles.count_documents({"taskId": task_id})
            if article_count == 0:
                await self._mark_task_error(task_id, "No articles found for task")
                return

            # Update task status to running
            await self.db.tasks.update_one(
                {"_id": ObjectId(task_id)},
                {
                    "$set": {
                        "status": TaskStatus.running,
                        "error": None
                    }
                }
            )

            # Get all articles for this task
            articles = await self.db.articles.find({"taskId": task_id}).to_list(length=None)
            total_processed = 0
            retry_count = 0

            while total_processed < len(articles):
                # Check if task was cancelled
                if self._cancelled:
                    print(f"🛑 Task {task_id} cancelled during processing")
                    return

                # Check task status in database
                current_task = await self.db.tasks.find_one(
                    {"_id": ObjectId(task_id)},
                    {"status": 1}
                )
                
                if current_task["status"] == TaskStatus.error:
                    print(f"Task {task_id} was cancelled or errored, stopping processing")
                    return

                try:
                    # Get next batch
                    batch = articles[total_processed:total_processed + settings.BATCH_SIZE]
                    
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

                    # Check task status again before saving results
                    current_task = await self.db.tasks.find_one(
                        {"_id": ObjectId(task_id)},
                        {"status": 1}
                    )
                    
                    if current_task["status"] == TaskStatus.error:
                        print(f"Task {task_id} status changed to error, stopping")
                        return

                    if results:
                        # Process each article in the batch
                        for article in batch:
                            if self._cancelled:
                                print(f"🛑 Task {task_id} cancelled ")
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
                                    total_processed += 1
                                except Exception as update_error:
                                    print(f"Error updating result: {update_error}")
                                    raise

                        # Update task progress
                        if not self._cancelled:
                            await self.db.tasks.update_one(
                                {"_id": ObjectId(task_id)},
                                {
                                    "$set": {
                                        "progress.current": total_processed
                                    }
                                }
                            )

                except Exception as batch_error:
                    print(f"Error processing batch: {batch_error}")
                    retry_count += 1
                    if retry_count > settings.MAX_RETRIES:
                        raise
                    await asyncio.sleep(settings.RETRY_DELAY * retry_count)
                    continue

            # Mark task as complete if not cancelled
            if not self._cancelled:
                await self.db.tasks.update_one(
                    {"_id": ObjectId(task_id)},
                    {
                        "$set": {
                            "status": TaskStatus.done,
                            "completedAt": datetime.utcnow(),
                            "progress.current": total_processed
                        }
                    }
                )

        except Exception as e:
            print(f"Error processing task {task_id}: {e}")
            await self._mark_task_error(task_id, str(e))

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
            response = await self.llm_service.generate_response(prompt, model)
            
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