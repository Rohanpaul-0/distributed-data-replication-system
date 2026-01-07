import os
from pydantic import BaseModel


class Settings(BaseModel):
    control_plane_host: str = os.getenv("CONTROL_PLANE_HOST", "0.0.0.0")
    control_plane_port: int = int(os.getenv("CONTROL_PLANE_PORT", "8000"))
    database_url: str = os.getenv("DATABASE_URL", "sqlite:////app/data/control_plane.db")
    log_level: str = os.getenv("LOG_LEVEL", "info")


settings = Settings()
