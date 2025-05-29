import asyncio
from datetime import datetime
from motor.motor_asyncio import AsyncIOMotorDatabase
from bson import ObjectId
from typing import List, Dict, Tuple
from config import settings

class ArticleProcessor:
    def __init__(self, db: AsyncIOMotorDatabase):
        self.db = db

    async def prepare_articles(self, task: Dict) -> Tuple[List[Dict], int, int]:
        """Prepare articles for initial screening - returns (articles, progress_total, actual_total)"""
        task_id = str(task["_id"])
        print(f"\n📋 Preparing articles for task {task_id}")

        # Get all articles for this task
        articles = await self.db.articles.find({"taskId": task_id}).to_list(length=None)
        total_articles = len(articles)
        print(f"Total articles found: {total_articles}")

        # For initial screening, limit articles but keep total count accurate
        if task.get("status") == "running":
            articles_to_process = articles[:settings.ARTICLE_LIMIT]
            remaining_articles = [a["articleId"] for a in articles[settings.ARTICLE_LIMIT:]]
            # IMPORTANT: Set process_total to ARTICLE_LIMIT for initial screening
            process_total = settings.ARTICLE_LIMIT  # This is what we'll show as progress total
            print(f"📝 Reading task limit -> {settings.ARTICLE_LIMIT} articles.")

        else:
            # For full screening, process all remaining articles
            articles_to_process = articles
            remaining_articles = []
            process_total = total_articles
        
        # Update task with remaining articles and total - use atomic update
        result = await self.db.tasks.update_one(
            {
                "_id": ObjectId(task_id),
                "status": task.get("status")  # Only update if status hasn't changed
            },
            {
                "$set": {
                    "remainingArticles": remaining_articles,
                    "progress.total": process_total,  # This will be ARTICLE_LIMIT for initial screening
                    "progress.current": 0,
                    "currentArticle": None,
                    "actualTotal": total_articles  # Store the actual total separately
                }
            }
        )
        
        if result.modified_count != 1:
            print(f"⚠️ Warning: Task {task_id} status changed during preparation")

        print(f"Processing {len(articles_to_process)} articles")
        print(f"Progress total set to: {process_total}")  # Debug log
        if remaining_articles:
            print(f"Remaining articles: {len(remaining_articles)}")

        return articles_to_process, process_total, total_articles

    async def save_batch_results(
        self,
        task_id: str,
        batch: List[Dict],
        results: Dict[str, Dict],
        total_processed: int,
        total_expected: int,
        actual_total: int
    ) -> int:
        """Save batch results to database with atomic updates"""
        print(f"\n💾 Saving batch results for task {task_id}")
        print(f"📊 Batch contains {len(batch)} articles, results for {len(results)} articles")
        
        # Get task to verify status and check if we should pause
        task = await self.db.tasks.find_one({"_id": ObjectId(task_id)})
        if not task:
            raise ValueError(f"Task {task_id} not found")

        # Track batch progress
        batch_processed = 0

        # Process each article in batch
        for idx, art in enumerate(batch):
            aid = art["articleId"]
            print(f"🔍 Processing article {aid} ({idx + 1}/{len(batch)} in batch)")
            
            if aid not in results:
                print(f"⚠️ No result found for article {aid}, skipping")
                continue
                
            res = results[aid]
            
            # Check if we're about to exceed the limit for initial screening
            new_total = total_processed + batch_processed + 1  # +1 for current article
            
            # For initial screening, check limit BEFORE processing
            if task["status"] == "running" and new_total > settings.ARTICLE_LIMIT:
                print(f"⏸️ Reached article limit ({settings.ARTICLE_LIMIT}), stopping batch processing")
                print(f"📊 Processed {batch_processed} articles from this batch")
                # Don't process this article, return current progress
                return total_processed + batch_processed
                
            doc = {
                "taskId": task_id,
                "articleId": aid,
                "included": bool(res["included"]),
                "reason": str(res["reason"]),
                "relevanceScore": float(res["relevanceScore"]),
                "metadata": {
                    "title": art["title"],
                    "abstract": art["abstract"]
                },
                "updatedAt": datetime.utcnow()
            }

            # Save result
            await self.db.screening_results.update_one(
                {"taskId": task_id, "articleId": aid},
                {"$set": doc},
                upsert=True
            )

            batch_processed += 1
            current_total = total_processed + batch_processed

            # Print detailed progress for each article
            print(f"📊 Processed article {aid} ({current_total}/{actual_total})")
            print(f"   ├─ Included: {doc['included']}")
            print(f"   ├─ Score: {doc['relevanceScore']}")
            print(f"   └─ Reason: {doc['reason'][:50]}...")

            # For initial screening, check if we've reached the limit
            if task["status"] == "running" and current_total >= settings.ARTICLE_LIMIT:
                # Use atomic update to ensure we only change status if it's still "running"
                print(f"\n** reaching task limit, stop worker now**")
                result = await self.db.tasks.update_one(
                    {
                        "_id": ObjectId(task_id),
                        "status": "running"  # Only update if status is still running
                    },
                    {
                        "$set": {
                            "status": "paused",
                            "progress.current": current_total,
                            "progress.total": settings.ARTICLE_LIMIT,  # Ensure total is set to limit
                            "currentArticle": None
                        }
                    }
                )
                
                if result.modified_count == 1:
                    print(f"turn task status into *paused*, save the current stage {current_total}/{actual_total} on database.")
                    print(f"⏸️ Initial screening limit reached - {current_total} articles processed")
                else:
                    # Task status might have been changed externally
                    current_task = await self.db.tasks.find_one({"_id": ObjectId(task_id)})
                    if current_task:
                        print(f"⚠️ Could not pause task - current status is {current_task.get('status', 'unknown')}")
                    else:
                        print("⚠️ Could not pause task - task not found")
                
                return current_total
            else:
                # Standard progress update - use atomic update to prevent race conditions
                result = await self.db.tasks.update_one(
                    {
                        "_id": ObjectId(task_id),
                        "status": task["status"]  # Only update if status hasn't changed
                    },
                    {
                        "$set": {
                            "progress.current": current_total,
                            "currentArticle": aid
                        }
                    }
                )
                
                if result.modified_count != 1:
                    print(f"⚠️ Warning: Could not update progress - task status may have changed")

            # Small delay between updates
            if idx < len(batch) - 1:
                await asyncio.sleep(0.1)

        total_processed += batch_processed
        print(f"✅ Batch complete - Progress: {total_processed}/{actual_total}")
        return total_processed