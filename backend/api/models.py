from pydantic import BaseModel, Field
from typing import Optional, Dict, List
from datetime import datetime
from enum import Enum

class TaskStatus(str, Enum):
    running = "running"
    done = "done"
    error = "error"
    paused = "paused"
    full_screening = "full_screening"

class TaskProgress(BaseModel):
    total: int = Field(..., ge=0)
    current: int = Field(..., ge=0)

class TaskCreate(BaseModel):
    userId: str = Field(..., description="User ID (email) creating the task")
    searchQuery: str = Field(..., min_length=1)
    criteria: str = Field(..., min_length=1)
    model: str
    totalArticles: int = Field(..., gt=0)
    timestamp: int = Field(..., description="Timestamp for task creation")

class TaskUpdate(BaseModel):
    status: Optional[TaskStatus]
    progress: Optional[TaskProgress]
    error: Optional[str]

class ScreeningResult(BaseModel):
    included: bool
    reason: str
    relevanceScore: float = Field(..., ge=0, le=100)
    metadata: Optional[Dict]

class Task(BaseModel):
    id: str
    userId: str
    searchQuery: str
    criteria: str
    model: str
    status: TaskStatus
    progress: TaskProgress
    startedAt: datetime
    completedAt: Optional[datetime]
    error: Optional[str]
    name: Optional[str]
    remainingArticles: Optional[List[str]]
