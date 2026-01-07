from fastapi import FastAPI

from src.api.health import router as health_router
from src.api.chunks import router as chunks_router
from src.api.objects import router as objects_router
from src.api.metrics import router as metrics_router
from src.db.session import init_db

app = FastAPI(title="Replicator Data Plane", version="0.1.0")

@app.on_event("startup")
def _startup():
    init_db()

app.include_router(health_router)
app.include_router(chunks_router)
app.include_router(objects_router)
app.include_router(metrics_router)
