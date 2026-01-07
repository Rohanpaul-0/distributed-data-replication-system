from __future__ import annotations

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import Response

from src.storage.chunk_store import ChunkStore
from src.api.metrics import (
    chunks_put_total,
    chunks_get_total,
    chunks_head_total,
    bytes_in_total,
    bytes_out_total,
    dedupe_hits_total,
    dedupe_misses_total,
)

from pathlib import Path

router = APIRouter(prefix="/chunks", tags=["chunks"])

# Store chunks on the container volume
store = ChunkStore(root=Path("/app/data/blobs"))


def _validate_hash(h: str) -> None:
    # SHA-256 hex = 64 chars
    if len(h) != 64:
        raise HTTPException(status_code=400, detail="invalid hash length")
    # ensure hex
    try:
        int(h, 16)
    except ValueError:
        raise HTTPException(status_code=400, detail="invalid hash format")


@router.head("/{chunk_hash}")
def head_chunk(chunk_hash: str):
    _validate_hash(chunk_hash)
    chunks_head_total.inc()

    if store.exists(chunk_hash):
        dedupe_hits_total.inc()
        return Response(status_code=200)
    else:
        dedupe_misses_total.inc()
        return Response(status_code=404)


@router.get("/{chunk_hash}")
def get_chunk(chunk_hash: str):
    _validate_hash(chunk_hash)
    chunks_get_total.inc()

    if not store.exists(chunk_hash):
        raise HTTPException(status_code=404, detail="chunk not found")

    data = store.read(chunk_hash)
    bytes_out_total.inc(len(data))
    return Response(content=data, media_type="application/octet-stream")


@router.put("/{chunk_hash}")
async def put_chunk(chunk_hash: str, request: Request):
    _validate_hash(chunk_hash)
    chunks_put_total.inc()

    data = await request.body()
    bytes_in_total.inc(len(data))

    # idempotent PUT: if exists, treat as dedupe hit
    if store.exists(chunk_hash):
        dedupe_hits_total.inc()
        return {"status": "exists", "hash": chunk_hash, "bytes": len(data)}

    store.write(chunk_hash, data)
    dedupe_misses_total.inc()
    return {"status": "stored", "hash": chunk_hash, "bytes": len(data)}
