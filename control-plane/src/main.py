import asyncio
import logging

from fastapi import FastAPI

from src.api.health import router as health_router
from src.api.nodes import router as nodes_router
from src.api.jobs import router as jobs_router
from src.api.metrics import router as metrics_router

from src.db.session import init_db
from src.services.job_runner import JobRunner

logger = logging.getLogger("replicator")

app = FastAPI(title="Replicator Control Plane", version="0.1.0")
runner = JobRunner()


@app.on_event("startup")
async def on_startup():
    init_db()
    logger.info("Starting JobRunner background task...")
    asyncio.create_task(runner.run_forever())


@app.on_event("shutdown")
async def on_shutdown():
    runner.stop()


app.include_router(health_router)
app.include_router(nodes_router)
app.include_router(jobs_router)
app.include_router(metrics_router)
