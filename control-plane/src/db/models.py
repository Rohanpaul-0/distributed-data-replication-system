from __future__ import annotations

from datetime import datetime
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy import String, Integer, Text
from sqlalchemy.orm import Session


class Base(DeclarativeBase):
    pass


class Node(Base):
    __tablename__ = "nodes"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(128), unique=True, nullable=False)
    base_url: Mapped[str] = mapped_column(String(512), nullable=False)
    status: Mapped[str] = mapped_column(String(32), default="healthy")
    last_heartbeat: Mapped[str] = mapped_column(
        String(64),
        default=lambda: datetime.utcnow().isoformat(),
    )


class Job(Base):
    __tablename__ = "jobs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    kind: Mapped[str] = mapped_column(String(32), nullable=False)  # migrate/sync
    src_node: Mapped[str] = mapped_column(String(128), nullable=False)
    dst_node: Mapped[str] = mapped_column(String(128), nullable=False)
    object_id: Mapped[str] = mapped_column(String(256), nullable=False)

    status: Mapped[str] = mapped_column(String(32), default="queued")  # queued/running/succeeded/failed
    retries: Mapped[int] = mapped_column(Integer, default=0)
    last_error: Mapped[str] = mapped_column(Text, default="")

    created_at: Mapped[str] = mapped_column(
        String(64),
        default=lambda: datetime.utcnow().isoformat(),
    )
    updated_at: Mapped[str] = mapped_column(
        String(64),
        default=lambda: datetime.utcnow().isoformat(),
    )

    @staticmethod
    def _now_iso() -> str:
        return datetime.utcnow().isoformat()

    @classmethod
    def create_migrate(cls, db: Session, src_node: str, dst_node: str, object_id: str) -> "Job":
        """
        Create a migration job in queued state.
        """
        now = cls._now_iso()
        job = cls(
            kind="migrate",
            src_node=src_node,
            dst_node=dst_node,
            object_id=object_id,
            status="queued",
            retries=0,
            last_error="",
            created_at=now,
            updated_at=now,
        )
        db.add(job)
        db.commit()
        db.refresh(job)
        return job

    def mark_running(self) -> None:
        self.status = "running"
        self.updated_at = self._now_iso()

    def mark_succeeded(self) -> None:
        self.status = "succeeded"
        self.last_error = ""
        self.updated_at = self._now_iso()

    def mark_failed(self, err: str) -> None:
        self.status = "failed"
        self.last_error = err
        self.updated_at = self._now_iso()

    def bump_retry(self, err: str) -> None:
        self.retries = (self.retries or 0) + 1
        self.last_error = err
        self.updated_at = self._now_iso()
