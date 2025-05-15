import asyncio
from datetime import datetime
from motor.motor_asyncio import AsyncIOMotorDatabase
from bson import ObjectId
from typing import List, Dict, Tuple
from config import settings

class ArticleProcessor:
    def __init__(self, db: AsyncIOMotorDatabase):
        self.db = db

    async def prepare_articles(self, task: Dict) -> Tuple[List[Dict], int]:
        """Prepare articles for initial screening"""
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
            process_total = total_articles  # Use total_articles to show real total

        else:
            # For full screening, process all remaining articles
            articles_to_process = articles
            remaining_articles = []
            process_total = total_articles
        
        # Update task with remaining articles and total
        await self.db.tasks.update_one(
            {"_id": ObjectId(task_id)},
            {
                "$set": {
                    "remainingArticles": remaining_articles,
                    "progress.total": process_total,
                    "progress.current": 0,
                    "currentArticle": None
                }
            }
        )

        print(f"Processing {len(articles_to_process)} articles")
        if remaining_articles:
            print(f"Remaining articles: {len(remaining_articles)}")

        return articles_to_process, process_total

    async def save_batch_results(
        self,
        task_id: str,
        batch: List[Dict],
        results: Dict[str, Dict],
        total_processed: int,
        total_expected: int
    ) -> int:
        """Save batch results to database with atomic updates"""
        print(f"\n💾 Saving batch results for task {task_id}")
        
        # Get task to verify status
        task = await self.db.tasks.find_one({"_id": ObjectId(task_id)})
        if not task:
            raise ValueError(f"Task {task_id} not found")

        # Track batch progress
        batch_processed = 0

        # Process each article in batch
        for idx, art in enumerate(batch):
            aid = art["articleId"]
            if aid in results:
                res = results[aid]
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
                new_total = total_processed + batch_processed

                # Print progress for each article
                print(f"📊 Processed article {aid} ({new_total}/{total_expected})")

                # For initial screening, check if we've reached the limit
                if task["status"] == "running" and new_total >= settings.ARTICLE_LIMIT:
                    await self.db.tasks.update_one(
                        {"_id": ObjectId(task_id)},
                        {
                            "$set": {
                                "status": "paused",
                                "progress.current": new_total,
                                "currentArticle": None
                            }
                        }
                    )
                    print(f"⏸️ Initial screening limit reached - {new_total} articles processed")
                    return new_total
                else:
                    # Standard progress update
                    await self.db.tasks.update_one(
                        {"_id": ObjectId(task_id)},
                        {
                            "$set": {
                                "progress.current": new_total,
                                "currentArticle": aid
                            }
                        }
                    )

                # Small delay between updates
                if idx < len(batch) - 1:
                    await asyncio.sleep(0.1)

        total_processed += batch_processed
        print(f"✅ Batch complete - Progress: {total_processed}/{total_expected}")
        return total_processed
