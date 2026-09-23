"""
Workers are the only processes that actually execute task commands. Multiple
worker replicas poll the same Redis queue; Postgres row-level locking
(`SELECT ... FOR UPDATE SKIP LOCKED`) guarantees two workers can never claim
the same TaskRun, even if the same id briefly appears twice in the queue.
"""
import os
import random
import subprocess
import time
from datetime import datetime, timedelta

from app.db import SessionLocal
from app.models import TaskRun, TaskRunStatus
from app import queue

BASE_BACKOFF = float(os.getenv("BASE_BACKOFF_SECONDS", "2"))
WORKER_ID = os.getenv("HOSTNAME", f"worker-{os.getpid()}")


def claim_task_run(db, task_run_id: str):
    """Row-lock the TaskRun so only this worker proceeds. Returns None if
    another worker already claimed it or it's no longer QUEUED."""
    task_run = (
        db.query(TaskRun)
        .filter(TaskRun.id == task_run_id)
        .with_for_update(skip_locked=True)
        .first()
    )
    if task_run is None or task_run.status != TaskRunStatus.QUEUED:
        return None

    task_run.status = TaskRunStatus.RUNNING
    task_run.attempt += 1
    task_run.started_at = datetime.utcnow()
    db.commit()
    return task_run


def execute(task_run: TaskRun) -> tuple[bool, str]:
    """Run the task's shell command. Returns (success, output_or_error)."""
    try:
        result = subprocess.run(
            task_run.command,
            shell=True,
            capture_output=True,
            text=True,
            timeout=60,
        )
        if result.returncode == 0:
            return True, result.stdout.strip()
        return False, (result.stderr or result.stdout).strip()
    except subprocess.TimeoutExpired:
        return False, "task timed out after 60s"
    except Exception as e:  # noqa: BLE001
        return False, str(e)


def backoff_seconds(attempt: int) -> float:
    """Exponential backoff with full jitter: prevents thundering-herd retries."""
    ceiling = BASE_BACKOFF * (2 ** (attempt - 1))
    return random.uniform(0, ceiling)


def handle_result(db, task_run: TaskRun, success: bool, output: str) -> None:
    task_run.finished_at = datetime.utcnow()
    if success:
        task_run.status = TaskRunStatus.SUCCESS
        task_run.output = output
    else:
        task_run.error = output
        if task_run.attempt >= task_run.max_retries:
            task_run.status = TaskRunStatus.DEAD_LETTER
            queue.enqueue_dead_letter(task_run.id, output)
        else:
            task_run.status = TaskRunStatus.RETRYING
            delay = backoff_seconds(task_run.attempt)
            task_run.next_retry_at = datetime.utcnow() + timedelta(seconds=delay)
    db.commit()


def worker_loop():
    print(f"[worker {WORKER_ID}] starting")
    while True:
        task_run_id = queue.dequeue_task(timeout=5)
        if task_run_id is None:
            continue

        db = SessionLocal()
        try:
            task_run = claim_task_run(db, task_run_id)
            if task_run is None:
                continue  # already claimed/handled by another worker

            print(f"[worker {WORKER_ID}] running {task_run.task_key} "
                  f"(attempt {task_run.attempt}/{task_run.max_retries})")
            success, output = execute(task_run)
            handle_result(db, task_run, success, output)
        finally:
            db.close()


if __name__ == "__main__":
    worker_loop()
