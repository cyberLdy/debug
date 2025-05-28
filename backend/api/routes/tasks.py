from bson import ObjectId
from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks
from motor.motor_asyncio import AsyncIOMotorDatabase
from datetime import datetime
from ..dependencies import get_db
from ..models import TaskCreate

router = APIRouter(tags=["tasks"])



@router.post("")  # Changed from "/tasks" since prefix is already set
async def create_task(
    task: TaskCreate,
    db: AsyncIOMotorDatabase = Depends(get_db)
):
    """Create a new screening task"""
    try:
        # Create task document with initial state
        task_doc = {
            "userId": task.userId,
            "searchQuery": task.searchQuery,
            "criteria": task.criteria,
            "model": task.model,
            "status": "running",  # Initial state is now running
            "progress": {
                "total": task.totalArticles,
                "current": 0
            },
            "startedAt": datetime.utcnow(),
            "name": f"Screening: {task.searchQuery[:50]}",
            "remainingArticles": []  # Initialize empty remaining articles list
        }

        # Insert into database
        result = await db.tasks.insert_one(task_doc)
        
        # Get the created task
        created_task = await db.tasks.find_one({"_id": result.inserted_id})
        
        if not created_task:
            raise HTTPException(status_code=500, detail="Failed to create task")

        return {
            "success": True,
            "task": {
                **created_task,
                "_id": str(created_task["_id"])
            }
        }

    except Exception as e:
        print(f"Error creating task: {e}")
        raise HTTPException(
            status_code=500,
            detail=str(e)
        )

@router.post("/{task_id}/screen")
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

@router.post("/{task_id}/request-full-screening")
async def request_full_screening(
    task_id: str,
    data: dict,
    db: AsyncIOMotorDatabase = Depends(get_db)
):
    """Request full screening for remaining articles"""
    print(f"\n🚀 Starting full screening request for task: {task_id}")
    
    try:
        # Verify task exists and is in paused state
        task = await db.tasks.find_one({
            "_id": ObjectId(task_id),
            "status": "paused"
        })

        if not task:
            raise HTTPException(
                status_code=404,
                detail="Task not found or not in paused state"
            )

        # Get remaining articles
        remaining_articles = data.get("remainingArticles", [])
        if not remaining_articles:
            raise HTTPException(
                status_code=400,
                detail="No remaining articles provided"
            )

        # Get current progress
        current_progress = task.get("progress", {}).get("current", 0)
        total_articles = task.get("progress", {}).get("total", 0)

        print(f"Current state: progress={task.get('progress')}, remaining={len(remaining_articles)}")

        # Update task status to full_screening while preserving progress
        await db.tasks.update_one(
            {"_id": ObjectId(task_id)},
            {
                "$set": {
                    "status": "full_screening",
                    "error": None,
                    "remainingArticles": remaining_articles,
                    "progress.current": current_progress,
                    "progress.total": total_articles
                }
            }
        )

        print(f"✅ Task {task_id} updated to full_screening state")
        print(f"Progress preserved: current={current_progress}, total={total_articles}")

        return {
            "success": True,
            "message": "Full screening started successfully"
        }

    except Exception as e:
        print(f"Error requesting full screening: {e}")
        raise HTTPException(
            status_code=500,
            detail=str(e)
        )

@router.post("/{task_id}/cancel")
async def cancel_task(
    task_id: str,
    db: AsyncIOMotorDatabase = Depends(get_db)
):
    """Cancel a running task"""
    try:
        # Verify task exists
        task = await db.tasks.find_one({"_id": ObjectId(task_id)})
        if not task:
            raise HTTPException(status_code=404, detail="Task not found")

        # Check if task can be cancelled
        if task["status"] in ["done", "error"]:
            raise HTTPException(
                status_code=400, 
                detail=f"Task cannot be cancelled in {task['status']} state"
            )

        print(f"🛑 Cancelling task {task_id} in {task['status']} state")

        # Update task status using atomic operation to prevent race conditions
        result = await db.tasks.find_one_and_update(
            {
                "_id": ObjectId(task_id),
                "status": {"$nin": ["done", "error"]}
            },
            {
                "$set": {
                    "status": "error",
                    "error": "Task cancelled by user",
                    "completedAt": datetime.utcnow()
                }
            },
            return_document=True
        )

        if not result:
            raise HTTPException(
                status_code=409,
                detail="Task status changed during cancellation attempt"
            )

        print(f"✅ Task {task_id} cancelled")
        
        return {
            "success": True,
            "message": "Task cancelled successfully"
        }

    except Exception as e:
        print(f"Error cancelling task: {e}")
        raise HTTPException(
            status_code=500,
            detail=str(e)
        )

@router.get("/{task_id}")
async def get_task(
    task_id: str,
    db: AsyncIOMotorDatabase = Depends(get_db)
):
    """Get task details with screening stats"""
    try:
        # Verify task exists
        task = await db.tasks.find_one({"_id": ObjectId(task_id)})
        if not task:
            raise HTTPException(status_code=404, detail="Task not found")

        # Get screening stats
        stats = await db.screening_results.aggregate([
            {"$match": {"taskId": task_id}},
            {
                "$group": {
                    "_id": None,
                    "included": {"$sum": {"$cond": ["$included", 1, 0]}},
                    "excluded": {"$sum": {"$cond": ["$included", 0, 1]}}
                }
            }
        ]).to_list(length=1)

        # Count processed articles (actual count from results collection)
        processed_count = await db.screening_results.count_documents({"taskId": task_id})
        
        # Get total article count
        total_article_count = await db.articles.count_documents({"taskId": task_id})

        # Make sure progress is consistent with processed articles
        if task.get("progress", {}).get("current", 0) != processed_count:
            # Update task with correct progress
            await db.tasks.update_one(
                {"_id": ObjectId(task_id)},
                {"$set": {"progress.current": processed_count}}
            )
            # Update the in-memory task object as well
            if "progress" in task:
                task["progress"]["current"] = processed_count
            else:
                task["progress"] = {"current": processed_count, "total": total_article_count}

        # Add article count to response
        task_response = {
            **task,
            "_id": str(task["_id"]),
            "stats": stats[0] if stats else {"included": 0, "excluded": 0},
            "articleCount": total_article_count,
            "processedCount": processed_count
        }

        return {
            "success": True,
            "task": task_response
        }

    except Exception as e:
        print(f"Error fetching task: {e}")
        raise HTTPException(
            status_code=500,
            detail=str(e)
        )

@router.get("")
async def list_tasks(
    db: AsyncIOMotorDatabase = Depends(get_db),
    status: str = None,
    page: int = 1,
    limit: int = 20
):
    """List tasks with pagination and optional status filter"""
    try:
        # Build query
        query = {}
        if status and status != "all":
            query["status"] = status

        # Get total count for pagination
        total = await db.tasks.count_documents(query)
        
        # Get paginated tasks
        tasks = await db.tasks.find(query)\
            .sort("startedAt", -1)\
            .skip((page - 1) * limit)\
            .limit(limit)\
            .to_list(length=limit)

        # Get stats for each task
        for task in tasks:
            stats = await db.screening_results.aggregate([
                {"$match": {"taskId": str(task["_id"])}},
                {
                    "$group": {
                        "_id": None,
                        "included": {"$sum": {"$cond": ["$included", 1, 0]}},
                        "excluded": {"$sum": {"$cond": ["$included", 0, 1]}}
                    }
                }
            ]).to_list(length=1)
            
            # Count processed articles for each task
            processed_count = await db.screening_results.count_documents({"taskId": str(task["_id"])})
            
            # Update the task object
            task["_id"] = str(task["_id"])
            task["stats"] = stats[0] if stats else {"included": 0, "excluded": 0}
            task["processedCount"] = processed_count

        return {
            "success": True,
            "tasks": tasks,
            "pagination": {
                "page": page,
                "limit": limit,
                "total": total,
                "pages": (total + limit - 1) // limit
            }
        }

    except Exception as e:
        print(f"Error listing tasks: {e}")
        raise HTTPException(
            status_code=500,
            detail=str(e)
        )