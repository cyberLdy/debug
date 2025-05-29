import asyncio
import json
from datetime import datetime
from pathlib import Path
from motor.motor_asyncio import AsyncIOMotorClient
from bson import ObjectId
from dotenv import load_dotenv
import os

# Import backend modules
from api.models import TaskCreate, TaskStatus, TaskProgress
from api.services.screening import ScreeningService
from api.services.llm import LLMService
from config import settings

# Get the absolute path to .env.local and load it
env_path = Path(__file__).parent.parent / '.env.local'
load_dotenv(env_path)

async def test_backend():
    print('\n🔍 Testing Backend API Endpoints\n')

    try:
        # Print environment configuration
        print('📋 Environment Configuration:')
        print('- MongoDB URI:', '✅ Configured' if settings.MONGODB_URI else '❌ Missing')
        print('- JWT Secret:', '✅ Configured' if os.getenv('JWT_SECRET') else '❌ Missing')
        print('- OpenAI Model:', settings.OPENAI_MODEL)
        print('- Ollama Model:', settings.OLLAMA_MODEL)

        # Connect to MongoDB - using main database
        client = AsyncIOMotorClient(settings.MONGODB_URI)
        db = client[settings.MONGODB_DB]
        
        try:
            await client.admin.command('ping')
            print('✅ MongoDB connection successful')
        except Exception as e:
            raise Exception(f'MongoDB connection failed: {str(e)}')

        # Clean up previous test data
        print('\n🧹 Cleaning up previous test data...')
        await db.tasks.delete_many({"userId": "test@example.com"})
        print('✅ Cleaned up previous test tasks')

        # Test data
        test_task = {
            "userId": "test@example.com",
            "searchQuery": "cancer immunotherapy clinical trials",
            "criteria": """Include articles that meet ALL of these criteria:
1. Clinical trials of immunotherapy treatments
2. Cancer patients as study population
3. Published in the last 5 years
4. Reports efficacy outcomes""",
            "model": settings.OLLAMA_MODEL or settings.OPENAI_MODEL,
            #"model": settings.OPENAI_MODEL or settings.OLLAMA_MODEL ,
            "status": "new",
            "progress": {
                "total": 2,
                "current": 0
            },
            "startedAt": datetime.utcnow(),
            "name": "Test Screening Task"  # Added name for better identification
        }

        test_articles = [
            {
                "id": "12345",
                "title": "Phase II Trial of PD-1 Inhibitor in Advanced Melanoma",
                "abstract": "In this multicenter clinical trial, we evaluated the efficacy of anti-PD-1 immunotherapy in 100 patients with advanced melanoma. The study demonstrated a 45% objective response rate with median progression-free survival of 11.5 months. Grade 3-4 adverse events occurred in 15% of patients."
            },
            {
                "id": "67890",
                "title": "Genetic Analysis of Lung Cancer Mutations",
                "abstract": "This study analyzed genetic mutations in 500 lung cancer samples using next-generation sequencing. We identified novel mutations in the EGFR pathway and their correlation with patient outcomes. No clinical trial or immunotherapy data was included in this analysis."
            }
        ]

        # Step 1: Create Task
        print('\n📝 Step 1: Creating test task...')
        task_result = await db.tasks.insert_one(test_task)
        task_id = str(task_result.inserted_id)
        print('✅ Task created successfully')
        print('Task ID:', task_id)

        # Step 2: Save Articles
        print('\n📝 Step 2: Saving articles...')
        # First clean up any existing articles for this task
        await db.articles.delete_many({"taskId": task_id})
        
        # Insert new articles
        articles_to_insert = []
        for article in test_articles:
            articles_to_insert.append({
                "taskId": task_id,
                "articleId": article["id"],
                "title": article["title"],
                "abstract": article["abstract"],
                "createdAt": datetime.utcnow()
            })

        if articles_to_insert:
            await db.articles.insert_many(articles_to_insert)
            print(f'✅ Saved {len(articles_to_insert)} articles to database')

        # Verify articles were saved
        saved_articles = await db.articles.find({"taskId": task_id}).to_list(length=100)
        print(f'📚 Found {len(saved_articles)} articles in database for task {task_id}')

        if len(saved_articles) == 0:
            raise Exception('No articles were saved to database')

        # Step 3: Start Screening
        print('\n🔍 Step 3: Starting screening...')
        
        # Initialize screening service
        screening_service = ScreeningService()
        
        # Process task in background
        print('⏳ Processing task...')
        await screening_service.process_task(task_id, db)

        # Step 4: Check Results
        print('\n📊 Step 4: Checking results...')
        
        # Get task status
        task = await db.tasks.find_one({"_id": ObjectId(task_id)})
        if not task:
            raise Exception('Task not found')

        print(f'Task Status: {task["status"]}')
        print(f'Progress: {task["progress"]["current"]}/{task["progress"]["total"]}')

        if task['status'] == 'error':
            raise Exception(f'Task failed: {task.get("error")}')

        # Get screening results
        results = await db.screening_results.find({"taskId": task_id}).to_list(length=100)
        
        print('\n📊 Screening Results:')
        for result in results:
            print(f'\nArticle: {result["articleId"]}')
            print(f'Decision: {"Included" if result["included"] else "Excluded"}')
            print(f'Relevance Score: {result["relevanceScore"]}%')
            print(f'Reason: {result["reason"]}')

        print('\n✨ All tests passed! Backend is working correctly.\n')

    except Exception as error:
        print('\n❌ Error:', str(error))
        
        print('\n📋 Troubleshooting steps:')
        print('1. Make sure MongoDB is running and accessible')
        print('2. Verify environment variables in .env.local')
        print('3. Check MongoDB connection string is correct')
        print('4. Verify LLM API configuration')
        print('5. Check server logs for detailed error messages')
        
        # Print database contents for debugging
        if 'db' in locals():
            print('\n🔍 Database Debug Info:')
            tasks = await db.tasks.find().to_list(length=100)
            print(f'Tasks in database: {len(tasks)}')
            for task in tasks:
                print(f'- Task {task["_id"]}: {task.get("name", "Unnamed")} ({task["status"]})')
            
            articles = await db.articles.find().to_list(length=100)
            print(f'\nArticles in database: {len(articles)}')
            for article in articles:
                print(f'- Article {article["articleId"]} for task {article["taskId"]}')
        raise
    finally:
        # Close MongoDB connection
        client.close()

if __name__ == '__main__':
    asyncio.run(test_backend())