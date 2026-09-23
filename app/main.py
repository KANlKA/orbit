from fastapi import FastAPI, HTTPException, Depends
from sqlalchemy.orm import Session

from app.db import Base, engine, get_db
from app.models import Workflow, Task, Run
from app.schemas import WorkflowCreate, RunOut
from app import dag, scheduler

Base.metadata.create_all(bind=engine)

app = FastAPI(title="Orbit — Workflow Orchestration Engine")


@app.post("/workflows", status_code=201)
def create_workflow(payload: WorkflowCreate, db: Session = Depends(get_db)):
    task_dicts = [t.model_dump() for t in payload.tasks]
    try:
        dag.validate_dag(task_dicts)
    except (dag.CyclicGraphError, dag.UnknownDependencyError) as e:
        raise HTTPException(status_code=400, detail=str(e))

    workflow = Workflow(name=payload.name)
    db.add(workflow)
    db.flush()

    for t in payload.tasks:
        db.add(Task(
            workflow_id=workflow.id,
            key=t.key,
            command=t.command,
            depends_on=t.depends_on,
        ))
    db.commit()
    db.refresh(workflow)
    return {"workflow_id": workflow.id, "name": workflow.name}


@app.post("/workflows/{workflow_id}/runs", response_model=RunOut, status_code=201)
def trigger_run(workflow_id: str, db: Session = Depends(get_db)):
    workflow = db.query(Workflow).filter(Workflow.id == workflow_id).first()
    if not workflow:
        raise HTTPException(status_code=404, detail="workflow not found")

    run = scheduler.create_run(db, workflow)
    # kick off an immediate scheduling pass so root tasks are queued right away
    scheduler.dispatch_ready_tasks(db, run.id)
    db.refresh(run)
    return run


@app.get("/runs/{run_id}", response_model=RunOut)
def get_run(run_id: str, db: Session = Depends(get_db)):
    run = db.query(Run).filter(Run.id == run_id).first()
    if not run:
        raise HTTPException(status_code=404, detail="run not found")
    return run


@app.get("/health")
def health():
    return {"status": "ok"}
