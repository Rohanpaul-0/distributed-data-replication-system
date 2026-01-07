from __future__ import annotations

import json
from pathlib import Path
from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import Response

from sqlalchemy.orm import Session
from fastapi import Depends

from src.core.hashing import sha256_hex
from src.core.chunking import iter_chunks
from src.db.session import get_db
from src.db.models import ObjectManifest
from src.storage.chunk_store import ChunkStore
from pydantic import BaseModel
from typing import List

from src.api.metrics import bytes_in_total, bytes_out_total

router = APIRouter(prefix="/objects", tags=["objects"])

store = ChunkStore(root=Path("/app/data/blobs"))

DEFAULT_CHUNK_SIZE = 1024 * 1024  

class ManifestIn(BaseModel):
    size_bytes: int
    chunk_size: int
    chunks: List[str]


def _validate_object_id(object_id: str) -> None:
    if not object_id or len(object_id) > 256:
        raise HTTPException(status_code=400, detail="invalid object_id")


@router.post("/{object_id}/ingest")
async def ingest_object(object_id: str, request: Request, db: Session = Depends(get_db)):
    _validate_object_id(object_id)

    raw = await request.body()
    bytes_in_total.inc(len(raw))

    # allow override via header (handy for tests)
    chunk_size = int(request.headers.get("x-chunk-size", str(DEFAULT_CHUNK_SIZE)))

    chunk_hashes: list[str] = []
    for chunk in iter_chunks(raw, chunk_size):
        h = sha256_hex(chunk)
        chunk_hashes.append(h)
        if not store.exists(h):
            store.write(h, chunk)

    manifest = ObjectManifest(
        object_id=object_id,
        size_bytes=len(raw),
        chunk_size=chunk_size,
        chunks_json=json.dumps(chunk_hashes),
    )

    existing = db.get(ObjectManifest, object_id)
    if existing:
        # overwrite to keep it simple for now
        existing.size_bytes = manifest.size_bytes
        existing.chunk_size = manifest.chunk_size
        existing.chunks_json = manifest.chunks_json
    else:
        db.add(manifest)

    db.commit()

    return {
        "object_id": object_id,
        "size_bytes": len(raw),
        "chunk_size": chunk_size,
        "chunks": len(chunk_hashes),
    }


@router.get("/{object_id}/manifest")
def get_manifest(object_id: str, db: Session = Depends(get_db)):
    _validate_object_id(object_id)
    m = db.get(ObjectManifest, object_id)
    if not m:
        raise HTTPException(status_code=404, detail="object not found")

    return {
        "object_id": m.object_id,
        "size_bytes": m.size_bytes,
        "chunk_size": m.chunk_size,
        "chunks": json.loads(m.chunks_json),
    }


@router.get("/{object_id}")
def download_object(object_id: str, db: Session = Depends(get_db)):
    _validate_object_id(object_id)
    m = db.get(ObjectManifest, object_id)
    if not m:
        raise HTTPException(status_code=404, detail="object not found")

    chunks = json.loads(m.chunks_json)
    out = bytearray()

    for h in chunks:
        if not store.exists(h):
            raise HTTPException(status_code=500, detail=f"missing chunk {h}")
        out.extend(store.read(h))

    bytes_out_total.inc(len(out))
    return Response(content=bytes(out), media_type="application/octet-stream")

@router.put("/{object_id}/manifest")
def put_manifest(object_id: str, body: ManifestIn, db: Session = Depends(get_db)):
    _validate_object_id(object_id)

    manifest = ObjectManifest(
        object_id=object_id,
        size_bytes=body.size_bytes,
        chunk_size=body.chunk_size,
        chunks_json=json.dumps(body.chunks),
    )

    existing = db.get(ObjectManifest, object_id)
    if existing:
        existing.size_bytes = manifest.size_bytes
        existing.chunk_size = manifest.chunk_size
        existing.chunks_json = manifest.chunks_json
    else:
        db.add(manifest)

    db.commit()
    return {"status": "manifest_saved", "object_id": object_id, "chunks": len(body.chunks)}
