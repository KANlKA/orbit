# Orbit - A Distributed Workflow Orchestration Engine

Orbit is a small, self-built alternative to Airflow/Temporal/Celery: you define
workflows as a **DAG of tasks**, submit a run, and a pool of workers picks up
ready tasks, executes them, retries failures with exponential backoff, and
routes permanently-failed tasks to a dead-letter queue — all while the API
tracks live state in Postgres.

This is a *learning-grade but architecturally real* implementation. It is not
a clone of Airflow's code — it's built from the ground up so you can explain
every line in an interview.

## Why this project 

This project forces you to reason about problems senior backend engineers actually deal
with:

- **Graph algorithms in production code** — topological sort + cycle
  detection to validate a DAG before it ever runs.
- **State machines** — a task run moves through
  `PENDING → QUEUED → RUNNING → SUCCESS/FAILED/RETRYING → DEAD_LETTER`,
  and the scheduler must never emit an illegal transition.
- **Concurrency & idempotency** — multiple workers can poll the queue at
  once; task claims use `SELECT ... FOR UPDATE SKIP LOCKED` so two workers
  never execute the same task twice.
- **Failure handling** — exponential backoff with jitter, max retries, and a
  dead-letter queue instead of silently dropping failed work.
- **Fan-out/fan-in** — a task can depend on multiple upstream tasks and only
  becomes "ready" once all its dependencies succeed.

## Architecture

```
                 ┌──────────────┐
   POST /runs -> │   FastAPI    │  <-- define workflows, trigger runs, poll status
                 │   (main.py)  │
                 └──────┬───────┘
                        │ writes Run + TaskRun rows
                        ▼
                 ┌──────────────┐
                 │  Postgres    │  workflows, tasks, runs, task_runs
                 └──────┬───────┘
                        │ scheduler polls for ready tasks
                        ▼
                 ┌──────────────┐        ┌────────────┐
                 │  Scheduler   │ -----> │ Redis Queue │
                 │(scheduler.py)│        └─────┬──────┘
                 └──────────────┘              │
                                        ┌───────┴────────┐
                                        ▼                ▼
                                   ┌─────────┐      ┌─────────┐
                                   │ Worker 1│ ...  │ Worker N│
                                   └────┬────┘      └────┬────┘
                                        └───────┬────────┘
                                                 ▼
                                     updates TaskRun status in Postgres
                                     (retry -> requeue, else DAG unlocks
                                      downstream tasks, or dead-letter)
```

## Tech stack

- **FastAPI** — REST API for defining workflows and querying run status
- **PostgreSQL + SQLAlchemy** — durable state (source of truth)
- **Redis** — task queue (List) + dead-letter queue
- **Docker Compose** — one-command local cluster (api, N workers, postgres, redis)

## Quickstart

```bash
docker compose up --build --scale worker=3
```

Register a workflow (see `examples/example_workflow.json` for a fan-out/fan-in DAG):

```bash
curl -X POST localhost:8000/workflows -H "Content-Type: application/json" \
  -d @examples/example_workflow.json
```

Trigger a run:

```bash
curl -X POST localhost:8000/workflows/{workflow_id}/runs
```

Poll status:

```bash
curl localhost:8000/runs/{run_id}
```
