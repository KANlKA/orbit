from typing import List, Optional
from pydantic import BaseModel


class TaskCreate(BaseModel):
    key: str
    command: str
    depends_on: List[str] = []


class WorkflowCreate(BaseModel):
    name: str
    tasks: List[TaskCreate]


class TaskRunOut(BaseModel):
    task_key: str
    status: str
    attempt: int
    output: Optional[str] = None
    error: Optional[str] = None

    class Config:
        from_attributes = True


class RunOut(BaseModel):
    id: str
    status: str
    task_runs: List[TaskRunOut]

    class Config:
        from_attributes = True
