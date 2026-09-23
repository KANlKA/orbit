import enum
import uuid
from datetime import datetime

from sqlalchemy import (
    Column, String, ForeignKey, DateTime, Integer, Enum, JSON, UniqueConstraint
)
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import relationship

from app.db import Base


def gen_uuid():
    return str(uuid.uuid4())


class TaskRunStatus(str, enum.Enum):
    PENDING = "PENDING"          # waiting on upstream dependencies
    QUEUED = "QUEUED"            # dependencies met, sitting in Redis queue
    RUNNING = "RUNNING"          # a worker has claimed it
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"            # failed, about to be retried
    RETRYING = "RETRYING"        # scheduled for retry after backoff
    DEAD_LETTER = "DEAD_LETTER"  # exhausted retries
    SKIPPED = "SKIPPED"          # an upstream dependency failed permanently


class RunStatus(str, enum.Enum):
    RUNNING = "RUNNING"
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"


class Workflow(Base):
    __tablename__ = "workflows"

    id = Column(UUID(as_uuid=False), primary_key=True, default=gen_uuid)
    name = Column(String, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)

    tasks = relationship("Task", back_populates="workflow", cascade="all, delete-orphan")
    runs = relationship("Run", back_populates="workflow", cascade="all, delete-orphan")


class Task(Base):
    """A node in the DAG. `depends_on` stores upstream task keys (JSON list)."""
    __tablename__ = "tasks"
    __table_args__ = (UniqueConstraint("workflow_id", "key", name="uq_workflow_task_key"),)

    id = Column(UUID(as_uuid=False), primary_key=True, default=gen_uuid)
    workflow_id = Column(UUID(as_uuid=False), ForeignKey("workflows.id"), nullable=False)
    key = Column(String, nullable=False)  # human-readable id, unique within workflow
    command = Column(String, nullable=False)  # shell command this task executes
    depends_on = Column(JSON, default=list)  # list of task `key`s

    workflow = relationship("Workflow", back_populates="tasks")


class Run(Base):
    """One execution instance of a Workflow (i.e. one DAG run)."""
    __tablename__ = "runs"

    id = Column(UUID(as_uuid=False), primary_key=True, default=gen_uuid)
    workflow_id = Column(UUID(as_uuid=False), ForeignKey("workflows.id"), nullable=False)
    status = Column(Enum(RunStatus), default=RunStatus.RUNNING)
    created_at = Column(DateTime, default=datetime.utcnow)
    finished_at = Column(DateTime, nullable=True)

    workflow = relationship("Workflow", back_populates="runs")
    task_runs = relationship("TaskRun", back_populates="run", cascade="all, delete-orphan")


class TaskRun(Base):
    """One task's execution within a specific Run. This is what workers claim."""
    __tablename__ = "task_runs"

    id = Column(UUID(as_uuid=False), primary_key=True, default=gen_uuid)
    run_id = Column(UUID(as_uuid=False), ForeignKey("runs.id"), nullable=False)
    task_key = Column(String, nullable=False)
    command = Column(String, nullable=False)
    depends_on = Column(JSON, default=list)

    status = Column(Enum(TaskRunStatus), default=TaskRunStatus.PENDING)
    attempt = Column(Integer, default=0)
    max_retries = Column(Integer, default=3)
    next_retry_at = Column(DateTime, nullable=True)

    output = Column(String, nullable=True)
    error = Column(String, nullable=True)

    started_at = Column(DateTime, nullable=True)
    finished_at = Column(DateTime, nullable=True)

    run = relationship("Run", back_populates="task_runs")
