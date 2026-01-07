from __future__ import annotations
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy import String, Integer, Text

class Base(DeclarativeBase):
    pass

class ObjectManifest(Base):
    __tablename__ = "object_manifests"

    object_id: Mapped[str] = mapped_column(String(256), primary_key=True)
    size_bytes: Mapped[int] = mapped_column(Integer, nullable=False)
    chunk_size: Mapped[int] = mapped_column(Integer, nullable=False)

    # JSON string: ["hash1","hash2",...]
    chunks_json: Mapped[str] = mapped_column(Text, nullable=False)
