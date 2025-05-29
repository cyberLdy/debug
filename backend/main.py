import asyncio
import signal
import sys
import uvicorn
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from api.routes import tasks, results
from worker import Worker
from config import settings
from api.dependencies import client as mongodb_client
import os

app = FastAPI(
    title="PubMed Screening API",
    description="API for PubMed article screening with AI",
    version="1.0.0"
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Global worker instance and shutdown flag
worker: Worker = None
shutdown_event = asyncio.Event()

async def shutdown(signal, loop):
    """Cleanup tasks tied to the service's shutdown."""
    print(f"\n🛑 Received exit signal {signal.name}...")
    
    tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
    
    for task in tasks:
        # Log task cancellation
        print(f"🔄 Cancelling task: {task.get_name()}")
        task.cancel()
    
    print(f"💤 Cancelling {len(tasks)} outstanding tasks")
    await asyncio.gather(*tasks, return_exceptions=True)
    
    if worker:
        print("🔄 Stopping worker...")
        await worker.stop()
    
    print("🔌 Closing MongoDB connection...")
    mongodb_client.close()
    
    loop.stop()
    print("👋 Shutdown complete!")

def handle_exception(loop, context):
    """Handle exceptions that escape the event loop."""
    msg = context.get("exception", context["message"])
    print(f"🚨 Caught exception: {msg}")
    print("Shutting down...")
    asyncio.create_task(shutdown(signal.SIGTERM, loop))

@app.on_event("startup")
async def startup_event():
    print("🚀 Starting API server...")
    
    try:
        await mongodb_client.admin.command('ping')
        print("✅ MongoDB connection verified")
    except Exception as e:
        print(f"❌ MongoDB connection failed: {e}")
        raise
    
    # Set up signal handlers
    loop = asyncio.get_running_loop()
    loop.set_exception_handler(handle_exception)
    
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(
            sig,
            lambda s=sig: asyncio.create_task(shutdown(s, loop))
        )
    
    # Create and start worker
    global worker
    worker = Worker(mongodb_client[settings.MONGODB_DB])
    asyncio.create_task(worker.start())
    print("🤖 Worker initialized and started")

@app.on_event("shutdown")
async def shutdown_event():
    print("🔄 Application shutdown initiated...")
    if worker:
        print("🛑 Stopping worker...")
        await worker.stop()
    print("🔌 Closing MongoDB connection...")
    mongodb_client.close()
    print("✅ Shutdown complete")

# Include routers with /api prefix
app.include_router(tasks.router, prefix="/api/tasks")
app.include_router(results.router, prefix="/api/tasks/{task_id}/results")

if __name__ == "__main__":
    port = int(os.getenv("PORT", "8000"))
    
    try:
        uvicorn.run(
            "main:app",
            host="0.0.0.0",
            port=port,
            reload=True,
            log_level="info"
        )
        
    except KeyboardInterrupt:
        print("\n⚡ Caught keyboard interrupt. Shutting down...")
        sys.exit(0)