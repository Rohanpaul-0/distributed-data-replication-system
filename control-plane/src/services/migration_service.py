from __future__ import annotations

import aiohttp

from src.db.session import SessionLocal
from src.db.models import Node, Job


class MigrationService:
    def __init__(self, timeout_s: float = 30.0):
        self.timeout = aiohttp.ClientTimeout(total=timeout_s)

    async def migrate_object(self, job: Job) -> None:
        # 1) Lookup node base URLs from DB (sync)
        db = SessionLocal()
        try:
            src: Node | None = db.query(Node).filter(Node.name == job.src_node).first()
            dst: Node | None = db.query(Node).filter(Node.name == job.dst_node).first()
            if not src or not dst:
                raise RuntimeError(f"Unknown node(s): src={job.src_node} dst={job.dst_node}")

            src_base = src.base_url.rstrip("/")
            dst_base = dst.base_url.rstrip("/")
        finally:
            db.close()

        object_id = job.object_id

        # 2) Pull manifest from source (async HTTP)
        async with aiohttp.ClientSession(timeout=self.timeout) as session:
            manifest_url = f"{src_base}/objects/{object_id}/manifest"
            async with session.get(manifest_url) as r:
                if r.status != 200:
                    text = await r.text()
                    raise RuntimeError(f"manifest fetch failed {r.status}: {text}")
                manifest = await r.json()

            chunks: list[str] = manifest.get("chunks", [])
            if not chunks:
                raise RuntimeError("manifest has no chunks")

            # 3) Ensure chunks exist on destination (copy missing chunks)
            for ch in chunks:
                head_url = f"{dst_base}/chunks/{ch}"
                async with session.head(head_url) as head:
                    if head.status == 200:
                        continue
                    if head.status not in (404,):
                        raise RuntimeError(f"dst HEAD chunk {ch} unexpected {head.status}")

                # fetch from source
                get_url = f"{src_base}/chunks/{ch}"
                async with session.get(get_url) as gr:
                    if gr.status != 200:
                        text = await gr.text()
                        raise RuntimeError(f"src GET chunk {ch} failed {gr.status}: {text}")
                    data = await gr.read()

                # put into destination
                put_url = f"{dst_base}/chunks/{ch}"
                async with session.put(put_url, data=data) as pr:
                    if pr.status != 200:
                        text = await pr.text()
                        raise RuntimeError(f"dst PUT chunk {ch} failed {pr.status}: {text}")

            # 4) Rebuild object on destination by ingesting the full object bytes
            # simplest: stream whole object from source and ingest into dst
            obj_get = f"{src_base}/objects/{object_id}"
            async with session.get(obj_get) as ogr:
                if ogr.status != 200:
                    text = await ogr.text()
                    raise RuntimeError(f"src GET object failed {ogr.status}: {text}")
                obj_bytes = await ogr.read()

            ingest_url = f"{dst_base}/objects/{object_id}/ingest"
            async with session.post(ingest_url, data=obj_bytes) as ir:
                if ir.status != 200:
                    text = await ir.text()
                    raise RuntimeError(f"dst ingest failed {ir.status}: {text}")
