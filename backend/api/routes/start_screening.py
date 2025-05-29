from bson import ObjectId
from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks
from motor.motor_asyncio import AsyncIOMotorDatabase
from datetime import datetime
from ..dependencies import get_db

router = APIRouter()

async def start_screening(
    task_id: str,
    data: dict,
    background_tasks: BackgroundTasks,
    db: AsyncIOMotorDatabase = Depends(get_db)
):
    """Start screening for a task"""
    try:
        # Verify task exists and is in running state
        task = await db.tasks.find_one({"_id": ObjectId(task_id)})
        if not task:
            raise HTTPException(status_code=404, detail="Task not found")
            
        if task["status"] not in ["running"]:
            raise HTTPException(
                status_code=400, 
                detail=f"Task cannot be started in {task['status']} state"
            )

        # Save articles
        articles = data.get("articles", [])
        if not articles:
            raise HTTPException(status_code=400, detail="No articles provided")

        print(f"📝 Starting screening for task {task_id} with {len(articles)} articles")

        try:
            # Insert new articles
            articles_to_insert = []
            for article in articles:
                articles_to_insert.append({
                    "taskId": task_id,
                    "articleId": article["id"],
                    "title": article["title"],
                    "abstract": article["abstract"],
                    "createdAt": datetime.utcnow()
                })

            if articles_to_insert:
                await db.articles.insert_many(articles_to_insert)
                print(f"✅ Saved {len(articles_to_insert)} articles")

            # Verify articles were saved
            saved_count = await db.articles.count_documents({"taskId": task_id})
            if saved_count != len(articles):
                raise HTTPException(
                    status_code=500,
                    detail=f"Article save mismatch: expected {len(articles)}, got {saved_count}"
                )

            print(f"✅ Task {task_id} ready for processing")
            return {
                "success": True,
                "message": "Articles saved successfully"
            }

        except Exception as e:
            # Rollback on error
            print(f"❌ Error saving articles: {e}")
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
            raise

    except Exception as e:
        print(f"Error starting screening: {e}")
        # Ensure task is marked as error
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
        raise HTTPException(
            status_code=500,
            detail=str(e)
        )