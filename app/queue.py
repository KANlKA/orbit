import os
import json
import redis

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
TASK_QUEUE_KEY = os.getenv("TASK_QUEUE_KEY", "orbit:queue")
DEAD_LETTER_KEY = os.getenv("DEAD_LETTER_KEY", "orbit:dead_letter")

r = redis.Redis.from_url(REDIS_URL, decode_responses=True)


def enqueue_task(task_run_id: str) -> None:
    r.rpush(TASK_QUEUE_KEY, task_run_id)


def dequeue_task(timeout: int = 5):
    """Blocking pop so workers don't busy-poll. Returns task_run_id or None."""
    item = r.blpop(TASK_QUEUE_KEY, timeout=timeout)
    if item is None:
        return None
    _, task_run_id = item
    return task_run_id


def enqueue_dead_letter(task_run_id: str, error: str) -> None:
    r.rpush(DEAD_LETTER_KEY, json.dumps({"task_run_id": task_run_id, "error": error}))
