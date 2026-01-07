from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy.orm import Session

from src.db.session import get_db
from src.db.models import Job

router = APIRouter(prefix="/jobs", tags=["jobs"])


class MigrateReq(BaseModel):
    src_node: str
    dst_node: str
    object_id: str


@router.post("/migrate")
def migrate(req: MigrateReq, db: Session = Depends(get_db)):
    job = Job.create_migrate(db, req.src_node, req.dst_node, req.object_id)
    return {"job_id": job.id, "status": job.status}


@router.get("")
def list_jobs(limit: int = 50, db: Session = Depends(get_db)):
    jobs = db.query(Job).order_by(Job.id.desc()).limit(limit).all()
    return [
        {
            "id": j.id,
            "kind": j.kind,
            "src_node": j.src_node,
            "dst_node": j.dst_node,
            "object_id": j.object_id,
            "status": j.status,
            "retries": j.retries,
            "last_error": j.last_error,
            "created_at": j.created_at,
            "updated_at": j.updated_at,
        }
        for j in jobs
    ]


@router.get("/{job_id}")
def get_job(job_id: int, db: Session = Depends(get_db)):
    j = db.query(Job).filter(Job.id == job_id).first()
    if not j:
        raise HTTPException(status_code=404, detail="job not found")

    return {
        "id": j.id,
        "kind": j.kind,
        "src_node": j.src_node,
        "dst_node": j.dst_node,
        "object_id": j.object_id,
        "status": j.status,
        "retries": j.retries,
        "last_error": j.last_error,
        "created_at": j.created_at,
        "updated_at": j.updated_at,
    }
