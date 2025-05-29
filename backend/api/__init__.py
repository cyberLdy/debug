from fastapi import FastAPI
from .routes import tasks, results

def create_app() -> FastAPI:
    app = FastAPI(
        title="PubMed Screening API",
        description="API for PubMed article screening with AI",
        version="1.0.0"
    )
    
    # Include routers
    app.include_router(tasks.router)
    app.include_router(results.router)
    
    return app