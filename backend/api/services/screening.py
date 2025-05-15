import asyncio
import json
from datetime import datetime
from motor.motor_asyncio import AsyncIOMotorDatabase
from bson import ObjectId
from typing import List, Dict, Any
from config import settings
from .llm import LLMService
from .prompts import build_screening_prompt

class ScreeningService:
    def __init__(self):
        self.llm_service = LLMService()

    async def process_task(self, task_id: str, db: AsyncIOMotorDatabase):
        """Process a screening task"""
        try:
            # Get task details
            task = await db.tasks.find_one({"_id": ObjectId(task_id)})
            if not task:
                print(f"Task {task_id} not found")
                return

            # Update task status to running
            await db.tasks.update_one(
                {"_id": ObjectId(task_id)},
                {
                    "$set": {
                        "status": "running",
                        "error": None
                    }
                }
            )

            # Get all articles for this task
            articles = await db.articles.find({"taskId": task_id}).to_list(length=None)
            
            # Process articles in batches
            batch_size = settings.BATCH_SIZE
            total_processed = 0
            retry_count = 0

            for i in range(0, len(articles), batch_size):
                batch = articles[i:i + batch_size]
                
                try:
                    # Format articles for screening
                    formatted_articles = [{
                        "id": article["articleId"],  # Use articleId from database
                        "title": article["title"],
                        "abstract": article["abstract"]
                    } for article in batch]

                    # Screen batch
                    results = await self._screen_batch(
                        formatted_articles,
                        task["criteria"],
                        task["model"]
                    )

                    if results:
                        # Process each article in the batch
                        for article in batch:
                            article_id = article["articleId"]
                            if article_id in results:
                                result = results[article_id]
                                
                                # Validate result format
                                included = result.get("included")
                                if included is None:
                                    excluded = result.get("excluded")
                                    if excluded is not None:
                                        included = not excluded
                                    else:
                                        included = False

                                # Create document for insert/update
                                doc = {
                                    "taskId": task_id,
                                    "articleId": article_id,
                                    "included": bool(included),
                                    "reason": str(result.get("reason", "")),
                                    "relevanceScore": float(result.get("relevanceScore", 0)),
                                    "metadata": {
                                        "title": article.get("title", ""),
                                        "abstract": article.get("abstract", "")
                                    },
                                    "updatedAt": datetime.utcnow()
                                }

                                try:
                                    # Use updateOne instead of bulk write
                                    await db.screening_results.update_one(
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
                        await db.tasks.update_one(
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

            # Mark task as complete
            await db.tasks.update_one(
                {"_id": ObjectId(task_id)},
                {
                    "$set": {
                        "status": "done",
                        "completedAt": datetime.utcnow(),
                        "progress.current": total_processed
                    }
                }
            )

        except Exception as e:
            print(f"Error processing task {task_id}: {e}")
            await db.tasks.update_one(
                {"_id": ObjectId(task_id)},
                {
                    "$set": {
                        "status": "error",
                        "error": str(e),
                        "completedAt": datetime.utcnow()
                    }
                }
            )

    async def _screen_batch(
        self,
        articles: List[Dict],
        criteria: str,
        model: str
    ) -> Dict[str, Any]:
        """Screen a batch of articles using LLM"""
        try:
            # Build prompt and get LLM response
            prompt = build_screening_prompt(articles, criteria)
            response = await self.llm_service.generate_response(prompt, model)

            # Parse response
            try:
                results = json.loads(response)
                
                # Validate results format
                if not isinstance(results, dict):
                    raise ValueError("Results must be a dictionary")
                
                # Validate each result
                validated_results = {}
                for article_id, result in results.items():
                    if not isinstance(result, dict):
                        print(f"Invalid result format for article {article_id}: {result}")
                        continue

                    # Handle both "included" and "excluded" formats
                    included = result.get("included")
                    if included is None:
                        excluded = result.get("excluded")
                        if excluded is not None:
                            included = not excluded
                        else:
                            included = False

                    # Ensure required fields with correct types
                    validated_result = {
                        "included": bool(included),
                        "reason": str(result.get("reason", "")),
                        "relevanceScore": float(result.get("relevanceScore", 0))
                    }

                    # Validate score range
                    if not 0 <= validated_result["relevanceScore"] <= 100:
                        validated_result["relevanceScore"] = max(0, min(100, validated_result["relevanceScore"]))

                    validated_results[article_id] = validated_result

                return validated_results

            except json.JSONDecodeError as e:
                print(f"Error parsing screening response: {str(e)}")
                print(f"Raw response: {response}")
                raise Exception(f"Failed to parse screening response: {str(e)}")

        except Exception as e:
            print(f"LLM error: {str(e)}")
            raise