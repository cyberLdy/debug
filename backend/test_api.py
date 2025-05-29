import asyncio
import httpx
import json
from datetime import datetime
from dotenv import load_dotenv
import os
from pathlib import Path

# Get the absolute path to .env.local
env_path = Path(__file__).parent.parent / '.env.local'

# Load environment variables from .env.local
load_dotenv(env_path)

async def test_backend():
    print('\n🔍 Testing Backend API Endpoints\n')

    try:
        # Print environment configuration
        print('📋 Environment Configuration:')
        print('- MongoDB URI:', '✅ Configured' if os.getenv('MONGODB_URI') else '❌ Missing')
        model = os.getenv('NEXT_PUBLIC_OLLAMA_MODEL') or os.getenv('NEXT_PUBLIC_OPENAI_MODEL')
        print('- Model:', model)

        # Test data
        test_task = {
            "searchQuery": "cancer immunotherapy clinical trials",
            "criteria": """Include articles that meet ALL of these criteria:
1. Clinical trials of immunotherapy treatments
2. Cancer patients as study population
3. Published in the last 5 years
4. Reports efficacy outcomes""",
            "model": model,
            "totalArticles": 2,
            "userId": "test@example.com"  # Add userId for authentication
        }

        test_articles = [
            {
                "id": "12345",  # Keep as "id" for the API request
                "title": "Phase II Trial of PD-1 Inhibitor in Advanced Melanoma",
                "abstract": "In this multicenter clinical trial, we evaluated the efficacy of anti-PD-1 immunotherapy in 100 patients with advanced melanoma. The study demonstrated a 45% objective response rate with median progression-free survival of 11.5 months. Grade 3-4 adverse events occurred in 15% of patients."
            },
            {
                "id": "67890",  # Keep as "id" for the API request
                "title": "Genetic Analysis of Lung Cancer Mutations",
                "abstract": "This study analyzed genetic mutations in 500 lung cancer samples using next-generation sequencing. We identified novel mutations in the EGFR pathway and their correlation with patient outcomes. No clinical trial or immunotherapy data was included in this analysis."
            }
        ]

        # Use httpx for better async HTTP handling
        async with httpx.AsyncClient(timeout=60.0) as client:
            # Step 1: Create Task
            print('\n📝 Step 1: Creating test task...')
            print('Task Configuration:', {
                'model': test_task['model'],
                'totalArticles': test_task['totalArticles']
            })

            create_response = await client.post(
                'http://localhost:8000/api/tasks',
                json=test_task
            )

            # Print raw response for debugging
            print(f'Create Task Response ({create_response.status_code}):')
            print(create_response.text)

            if not create_response.is_success:
                raise Exception(f'Failed to create task: {create_response.status_code}\n{create_response.text}')

            task_data = create_response.json()
            if not task_data.get('success'):
                raise Exception(f'Task creation failed: {task_data.get("message")}')

            task_id = task_data['task']['_id']
            print('✅ Task created successfully')
            print('Task ID:', task_id)

            # Step 2: Start Screening
            print('\n🔍 Step 2: Starting screening...')
            print('Articles to screen:', len(test_articles))

            screen_response = await client.post(
                f'http://localhost:8000/api/tasks/{task_id}/screen',
                json={'articles': test_articles}
            )

            # Print raw response for debugging
            print(f'Screen Response ({screen_response.status_code}):')
            print(screen_response.text)

            if not screen_response.is_success:
                raise Exception(f'Failed to start screening: {screen_response.status_code}\n{screen_response.text}')

            screen_data = screen_response.json()
            if not screen_data.get('success'):
                raise Exception(f'Screening failed: {screen_data.get("message")}')

            print('✅ Screening started successfully')

            # Step 3: Poll for Results
            print('\n⏳ Step 3: Polling for results...')
            completed = False
            attempts = 0
            max_attempts = 30  # 30 attempts * 2 seconds = 60 seconds max wait

            while not completed and attempts < max_attempts:
                status_response = await client.get(
                    f'http://localhost:8000/api/tasks/{task_id}'
                )

                if not status_response.is_success:
                    print(f'Status Response ({status_response.status_code}):')
                    print(status_response.text)
                    raise Exception(f'Failed to get task status: {status_response.status_code}')

                status_data = status_response.json()
                if not status_data.get('success'):
                    raise Exception(f'Failed to get task status: {status_data.get("message")}')

                task = status_data['task']
                print(f'📊 Progress: {task["progress"]["current"]}/{task["progress"]["total"]} (Status: {task["status"]})')

                if task['status'] == 'done':
                    completed = True
                    print('\n✅ Screening completed successfully!')
                    
                    # Fetch final results
                    results_response = await client.get(
                        f'http://localhost:8000/api/tasks/{task_id}/results'
                    )

                    if not results_response.is_success:
                        print(f'Results Response ({results_response.status_code}):')
                        print(results_response.text)
                        raise Exception(f'Failed to get results: {results_response.status_code}')

                    results_data = results_response.json()
                    if not results_data.get('success'):
                        raise Exception(f'Failed to get results: {results_data.get("message")}')
                    
                    print('\n📊 Screening Results:')
                    print(json.dumps(results_data['results'], indent=2))
                    break

                elif task['status'] == 'error':
                    raise Exception(f'Task failed: {task.get("error")}')

                await asyncio.sleep(2)
                attempts += 1

            if not completed:
                raise Exception('Screening timed out')

            print('\n✨ All tests passed! Backend is working correctly.\n')

    except Exception as error:
        print('\n❌ Error:', str(error))
        
        print('\n📋 Troubleshooting steps:')
        print('1. Make sure MongoDB is running and accessible')
        print('2. Check that the FastAPI server is running on port 8000')
        print('3. Verify environment variables in .env.local')
        print('4. Check server logs for detailed error messages')
        print('5. Verify MongoDB connection string is correct')
        print('6. Check if task IDs are in correct ObjectId format')
        raise

if __name__ == '__main__':
    asyncio.run(test_backend())