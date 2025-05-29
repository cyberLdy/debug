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
            print(f"📝 Initial screening mode - limiting to {settings.ARTICLE_LIMIT} articles")

        else:
            # For full screening, process all remaining articles
            articles_to_process = articles
            remaining_articles = []
            process_total = total_articles
            print(f"📝 Full screening mode - processing all {total_articles} articles")
        
        # Update task with remaining articles and total
        await self.db.tasks.update_one(
            {"_id": ObjectId(task_id)},
            {
                "$set": {
                    "remainingArticles": remaining_articles,
                    "progress.total": process_total,
                    "progress.current": 0,
                    "currentArticle": None,
                    "actualTotal": total_articles  # Store the actual total separately
                }
            }
        )

        print(f"Processing {len(articles_to_process)} articles")
        print(f"Progress total set to: {process_total}")
        if remaining_articles:
            print(f"Remaining articles for later: {len(remaining_articles)}")

        return articles_to_process, process_total, total_articles