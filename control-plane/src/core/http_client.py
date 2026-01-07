import aiohttp
from typing import Any, Dict, Optional


class HttpClient:
    def __init__(self, timeout_s: float = 30.0):
        self.timeout = aiohttp.ClientTimeout(total=timeout_s)

    async def get_json(self, url: str) -> Dict[str, Any]:
        async with aiohttp.ClientSession(timeout=self.timeout) as s:
            async with s.get(url) as r:
                r.raise_for_status()
                return await r.json()

    async def put_json(self, url: str, body: Dict[str, Any]) -> Dict[str, Any]:
        async with aiohttp.ClientSession(timeout=self.timeout) as s:
            async with s.put(url, json=body) as r:
                r.raise_for_status()
                return await r.json()

    async def get_bytes(self, url: str) -> bytes:
        async with aiohttp.ClientSession(timeout=self.timeout) as s:
            async with s.get(url) as r:
                r.raise_for_status()
                return await r.read()

    async def put_bytes(self, url: str, data: bytes) -> None:
        async with aiohttp.ClientSession(timeout=self.timeout) as s:
            async with s.put(url, data=data) as r:
                r.raise_for_status()
                return None

    async def head_status(self, url: str) -> int:
        async with aiohttp.ClientSession(timeout=self.timeout) as s:
            async with s.head(url) as r:
                return r.status
