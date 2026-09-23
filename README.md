# Orbit — A Distributed Workflow Orchestration Engine

Orbit is a small, self-built alternative to Airflow/Temporal/Celery: you define
workflows as a **DAG of tasks**, submit a run, and a pool of workers picks up
ready tasks, executes them, retries failures with exponential backoff, and
routes permanently-failed tasks to a dead-letter queue — all while the API
tracks live state in Postgres.

This is a *learning-grade but architecturally real* implementation. It is not
a clone of Airflow's code — it's built from the ground up so you can explain
every line in an interview.

## Why this project (and not another CRUD app)

Most fresher backend projects are "CRUD + auth + Postgres." This project
forces you to reason about problems senior backend engineers actually deal
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
cp .env.example .env
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

## What to build next (good "v2" resume bullets)

- Swap the Redis list for a priority queue so high-priority runs jump ahead
- Add a `/workflows` YAML DSL instead of raw JSON (mirrors how Dagu/Airflow do it)
- Add task timeouts with `SIGKILL` on a subprocess executor
- Add a leader-election mechanism (etcd or Postgres advisory locks) so only
  one scheduler instance dispatches tasks when you run multiple replicas
- Add OpenTelemetry tracing across API → scheduler → worker
- Add a simple web UI showing the DAG and live task states

## Suggested resume bullet points

> Built Orbit, a distributed workflow orchestration engine (Airflow-style)
> in Python/FastAPI supporting DAG-based task dependencies, exponential
> backoff retries, and dead-letter queues; used `SELECT FOR UPDATE SKIP
> LOCKED` for safe concurrent task claiming across N worker replicas.

> Designed a task state machine (7 states) backed by Postgres and a Redis
> queue, achieving exactly-once task dispatch under concurrent worker polling.

## Honesty note

Don't submit this repo as-is and call it entirely your own without
understanding it — interviewers who've seen Airflow/Temporal will ask you
*why* `SKIP LOCKED` matters, *why* backoff needs jitter, and *how* you'd
scale the scheduler. Read `app/scheduler.py` and `app/worker.py` closely,
run it locally, break it on purpose (kill a worker mid-task, submit a
cyclic DAG) and watch what happens. That's what will make the interview
conversation land.

## Reference implementations worth studying (do not copy verbatim)

- [dagu-org/dagu](https://github.com/dagu-org/dagu) — single-binary,
  YAML-DAG workflow engine (Go); great for seeing how a "no infra" version
  of this idea looks.
- [Apache Airflow](https://github.com/apache/airflow) — the industry
  standard; read the scheduler and executor source for the real-world
  version of what `scheduler.py`/`worker.py` here do at toy scale.
