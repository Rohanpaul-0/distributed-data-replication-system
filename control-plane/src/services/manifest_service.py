import asyncio
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

from src.core.http_client import HttpClient
from src.core.retry import retry_async
from src.core.rate_limit import RateLimiter


@dataclass
class Manifest:
    object_id: str
    size_bytes: int
    chunk_size: int
    chunks: List[str]


class MigrationService:
    """
    Orchestrates object migration between data-plane nodes:
    - fetch manifest from src node
    - delta-check chunks on dst node (HEAD)
    - copy only missing chunks (GET src -> PUT dst)
    - write manifest to dst node
    """

    def __init__(
        self,
        http: Optional[HttpClient] = None,
        limiter: Optional[RateLimiter] = None,
        max_concurrency: int = 4,
    ):
        self.http = http or HttpClient()
        self.limiter = limiter or RateLimiter(rate_per_sec=20, burst=20)
        self.max_concurrency = max_concurrency

    async def _fetch_manifest(self, base_url: str, object_id: str) -> Manifest:
        url = f"{base_url.rstrip('/')}/objects/{object_id}/manifest"

        async def _do():
            return await self.http.get_json(url)

        data = await retry_async(_do)
        return Manifest(
            object_id=data["object_id"],
            size_bytes=int(data["size_bytes"]),
            chunk_size=int(data["chunk_size"]),
            chunks=list(data["chunks"]),
        )

    async def _head_chunk(self, base_url: str, chunk_hash: str) -> bool:
        url = f"{base_url.rstrip('/')}/chunks/{chunk_hash}"

        async def _do():
            status = await self.http.head_status(url)
            return status == 200

        return await retry_async(_do)

    async def _copy_chunk(self, src_base: str, dst_base: str, chunk_hash: str) -> Tuple[str, str]:
        """
        Returns (chunk_hash, result) where result is "copied" or "exists"
        """
        src_url = f"{src_base.rstrip('/')}/chunks/{chunk_hash}"
        dst_url = f"{dst_base.rstrip('/')}/chunks/{chunk_hash}"

        async def _do_copy():
            await self.limiter.acquire()  
            blob = await self.http.get_bytes(src_url)
            await self.http.put_bytes(dst_url, blob)
            return True

        await retry_async(_do_copy)
        return (chunk_hash, "copied")

    async def _put_manifest(self, dst_base: str, manifest: Manifest) -> None:
        url = f"{dst_base.rstrip('/')}/objects/{manifest.object_id}/manifest"
        body = {
            "size_bytes": manifest.size_bytes,
            "chunk_size": manifest.chunk_size,
            "chunks": manifest.chunks,
        }

        async def _do():
            return await self.http.put_json(url, body)

        await retry_async(_do)

    async def migrate_object(self, src_base: str, dst_base: str, object_id: str) -> Dict[str, Any]:
        manifest = await self._fetch_manifest(src_base, object_id)

        missing: List[str] = []
        for h in manifest.chunks:
            exists = await self._head_chunk(dst_base, h)
            if not exists:
                missing.append(h)

        sem = asyncio.Semaphore(self.max_concurrency)

        async def worker(h: str):
            async with sem:
                return await self._copy_chunk(src_base, dst_base, h)

        copied_results = []
        if missing:
            copied_results = await asyncio.gather(*(worker(h) for h in missing))

        await self._put_manifest(dst_base, manifest)

        return {
            "object_id": object_id,
            "total_chunks": len(manifest.chunks),
            "missing_chunks": len(missing),
            "copied_chunks": len(copied_results),
        }
