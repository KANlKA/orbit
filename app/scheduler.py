"""
The scheduler is intentionally separate from the API process: in a real
deployment you'd run one scheduler (or a leader-elected group of them) and
scale workers independently. It never executes task commands itself — it
only decides *which* tasks are ready and hands them to Redis.
"""
import os
import time
from datetime import datetime

from app.db import SessionLocal
from app.models import Run, TaskRun, TaskRunStatus, RunStatus
from app import dag, queue

POLL_INTERVAL = float(os.getenv("SCHEDULER_POLL_INTERVAL", "1"))


def create_run(db, workflow) -> Run:
    """Instantiate a Run + one TaskRun per Task, all starting PENDING."""
    run = Run(workflow_id=workflow.id, status=RunStatus.RUNNING)
    db.add(run)
    db.flush()  # get run.id

    for task in workflow.tasks:
        task_run = TaskRun(
            run_id=run.id,
            task_key=task.key,
            command=task.command,
            depends_on=task.depends_on,
            status=TaskRunStatus.PENDING,
            max_retries=int(os.getenv("MAX_RETRIES", "3")),
        )
        db.add(task_run)

    db.commit()
    db.refresh(run)
    return run


def dispatch_ready_tasks(db, run_id: str) -> None:
    """One scheduling pass for a single run: skip-blocked, dispatch-ready, finalize."""
    task_runs = db.query(TaskRun).filter(TaskRun.run_id == run_id).all()
    if not task_runs:
        return

    all_keys = {tr.task_key for tr in task_runs}
    depends_on = {tr.task_key: tr.depends_on or [] for tr in task_runs}
    completed = {tr.task_key for tr in task_runs if tr.status == TaskRunStatus.SUCCESS}
    failed = {
        tr.task_key
        for tr in task_runs
        if tr.status in (TaskRunStatus.DEAD_LETTER, TaskRunStatus.SKIPPED)
    }
    pending = {tr.task_key for tr in task_runs if tr.status == TaskRunStatus.PENDING}

    blocked = dag.blocked_by_failure(all_keys, failed, pending, depends_on)
    for tr in task_runs:
        if tr.task_key in blocked and tr.status == TaskRunStatus.PENDING:
            tr.status = TaskRunStatus.SKIPPED
            tr.finished_at = datetime.utcnow()

    still_pending = pending - blocked
    ready = dag.ready_tasks(all_keys, completed, failed, still_pending, depends_on)
    for tr in task_runs:
        if tr.task_key in ready and tr.status == TaskRunStatus.PENDING:
            tr.status = TaskRunStatus.QUEUED
            queue.enqueue_task(tr.id)

    # also requeue anything whose retry backoff has elapsed
    for tr in task_runs:
        if (
            tr.status == TaskRunStatus.RETRYING
            and tr.next_retry_at
            and tr.next_retry_at <= datetime.utcnow()
        ):
            tr.status = TaskRunStatus.QUEUED
            queue.enqueue_task(tr.id)

    db.commit()

    # finalize the run once every task_run is in a terminal state
    terminal = {
        TaskRunStatus.SUCCESS,
        TaskRunStatus.DEAD_LETTER,
        TaskRunStatus.SKIPPED,
    }
    refreshed = db.query(TaskRun).filter(TaskRun.run_id == run_id).all()
    if all(tr.status in terminal for tr in refreshed):
        run = db.query(Run).get(run_id)
        if run and run.status == RunStatus.RUNNING:
            any_failed = any(
                tr.status in (TaskRunStatus.DEAD_LETTER, TaskRunStatus.SKIPPED)
                for tr in refreshed
            )
            run.status = RunStatus.FAILED if any_failed else RunStatus.SUCCESS
            run.finished_at = datetime.utcnow()
            db.commit()


def scheduler_loop():
    """Continuously scan all in-progress runs and dispatch ready tasks."""
    print("[scheduler] starting")
    while True:
        db = SessionLocal()
        try:
            active_runs = db.query(Run).filter(Run.status == RunStatus.RUNNING).all()
            for run in active_runs:
                dispatch_ready_tasks(db, run.id)
        finally:
            db.close()
        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    scheduler_loop()
