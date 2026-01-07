from __future__ import annotations
from typing import Iterator

def iter_chunks(data: bytes, chunk_size: int) -> Iterator[bytes]:
    if chunk_size <= 0:
        raise ValueError("chunk_size must be > 0")
    for i in range(0, len(data), chunk_size):
        yield data[i : i + chunk_size]
