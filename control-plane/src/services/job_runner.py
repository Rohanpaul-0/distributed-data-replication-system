from __future__ import annotations

import asyncio
from sqlalchemy.orm import Session

from src.db.session import SessionLocal
from src.db.models import Job
from src.services.migration_service import MigrationService


class JobRunner:
    def __init__(self, poll_interval_s: float = 1.0):
        self.poll_interval_s = poll_interval_s
        self._stop = asyncio.Event()
        self.migrator = MigrationService()

    async def run_forever(self):
        while not self._stop.is_set():
            try:
                await self._run_once()
            except Exception as e:
                print("JobRunner error:", repr(e))
            await asyncio.sleep(self.poll_interval_s)

    async def _run_once(self):
        db: Session = SessionLocal()
        try:
            job = (
                db.query(Job)
                .filter(Job.status == "queued", Job.kind == "migrate")
                .order_by(Job.id.asc())
                .first()
            )
            if not job:
                return

            job.mark_running()
            db.commit()
            job_id = job.id

        finally:
            db.close()

        # do the actual migration outside the DB session scope
        await self._execute(job_id)

    async def _execute(self, job_id: int):
        # read everything you need from DB
        db: Session = SessionLocal()
        try:
            job = db.query(Job).filter(Job.id == job_id).first()
            if not job:
                return

            # migrate does HTTP async, but DB reads are sync here
            try:
                await self.migrator.migrate_object(job)
                job.mark_succeeded()
            except Exception as e:
                job.mark_failed(str(e))

            db.commit()
        finally:
            db.close()

    def stop(self):
        self._stop.set()
