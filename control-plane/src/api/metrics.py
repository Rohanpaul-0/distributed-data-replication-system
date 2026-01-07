from fastapi import APIRouter, Response, Depends
from sqlalchemy.orm import Session

from src.db.session import get_db
from src.db.models import Job, Node

router = APIRouter(tags=["metrics"])


@router.get("/metrics")
def metrics(db: Session = Depends(get_db)):
    jobs_total = db.query(Job).count()
    nodes_total = db.query(Node).count()

    jobs_failed = db.query(Job).filter(Job.status == "failed").count()
    jobs_succeeded = db.query(Job).filter(Job.status == "succeeded").count()
    jobs_running = db.query(Job).filter(Job.status == "running").count()
    jobs_queued = db.query(Job).filter(Job.status == "queued").count()

    lines = [
        "# HELP replicator_jobs_total Total jobs",
        "# TYPE replicator_jobs_total counter",
        f"replicator_jobs_total {jobs_total}",
        "# HELP replicator_nodes_total Total registered nodes",
        "# TYPE replicator_nodes_total gauge",
        f"replicator_nodes_total {nodes_total}",
        "# HELP replicator_jobs_by_status Jobs by status",
        "# TYPE replicator_jobs_by_status gauge",
        f'replicator_jobs_by_status{{status="queued"}} {jobs_queued}',
        f'replicator_jobs_by_status{{status="running"}} {jobs_running}',
        f'replicator_jobs_by_status{{status="succeeded"}} {jobs_succeeded}',
        f'replicator_jobs_by_status{{status="failed"}} {jobs_failed}',
    ]
    return Response("\n".join(lines) + "\n", media_type="text/plain; version=0.0.4")
