from __future__ import annotations
from dataclasses import dataclass
from pathlib import Path

@dataclass(frozen=True)
class ChunkStore:
    root: Path  # e.g. /app/data/blobs

    def _path_for(self, chunk_hash: str) -> Path:
        prefix = chunk_hash[:2]
        return self.root / prefix / chunk_hash

    def exists(self, chunk_hash: str) -> bool:
        return self._path_for(chunk_hash).is_file()

    def read(self, chunk_hash: str) -> bytes:
        p = self._path_for(chunk_hash)
        return p.read_bytes()

    def write(self, chunk_hash: str, data: bytes) -> None:
        p = self._path_for(chunk_hash)
        p.parent.mkdir(parents=True, exist_ok=True)
        tmp = p.with_suffix(".tmp")
        tmp.write_bytes(data)
        tmp.replace(p)
