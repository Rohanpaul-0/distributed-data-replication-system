from fastapi import APIRouter
from fastapi.responses import Response
from prometheus_client import Counter, generate_latest, CONTENT_TYPE_LATEST

router = APIRouter(tags=["metrics"])

chunks_put_total = Counter("replicator_chunks_put_total", "Total chunk PUTs")
chunks_get_total = Counter("replicator_chunks_get_total", "Total chunk GETs")
chunks_head_total = Counter("replicator_chunks_head_total", "Total chunk HEAD checks")

bytes_in_total = Counter("replicator_bytes_in_total", "Total bytes received by node")
bytes_out_total = Counter("replicator_bytes_out_total", "Total bytes sent by node")

dedupe_hits_total = Counter("replicator_dedupe_hits_total", "Total dedupe hits (chunk already existed)")
dedupe_misses_total = Counter("replicator_dedupe_misses_total", "Total dedupe misses (chunk stored)")

@router.get("/metrics")
def metrics():
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)
