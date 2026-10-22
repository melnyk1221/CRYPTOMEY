"""Общие HTTP-помощники для публичных REST-источников."""
from __future__ import annotations

import asyncio
import gzip
import time
import zlib
from typing import Any

import aiohttp
import orjson

DEFAULT_TIMEOUT = aiohttp.ClientTimeout(total=15, connect=8)
USER_AGENT = "scalpbot/1.0 (+signal scanner; public market data only)"


class SourceError(Exception):
    def __init__(self, msg: str, status: int | None = None):
        super().__init__(msg)
        self.status = status

    @property
    def blocked(self) -> bool:
        return self.status in (403, 451)


def make_session() -> aiohttp.ClientSession:
    conn = aiohttp.TCPConnector(limit=20, ttl_dns_cache=300, enable_cleanup_closed=True)
    return aiohttp.ClientSession(connector=conn, timeout=DEFAULT_TIMEOUT,
                                 headers={"User-Agent": USER_AGENT}, json_serialize=lambda o: orjson.dumps(o).decode())


async def request_json(session: aiohttp.ClientSession, method: str, url: str, *,
                       params: dict | None = None, json: Any = None,
                       timeout: aiohttp.ClientTimeout | None = None) -> tuple[Any, float]:
    """Возвращает (json, задержка_мс). Бросает SourceError с HTTP-статусом."""
    t0 = time.monotonic()
    try:
        async with session.request(method, url, params=params, json=json, timeout=timeout) as resp:
            body = await resp.read()
            lat = (time.monotonic() - t0) * 1000
            if resp.status != 200:
                raise SourceError(f"HTTP {resp.status}: {body[:200]!r}", resp.status)
            try:
                return orjson.loads(body), lat
            except orjson.JSONDecodeError:
                raise SourceError(f"не JSON: {body[:200]!r}", resp.status)
    except SourceError:
        raise
    except (aiohttp.ClientError, asyncio.TimeoutError, TimeoutError, OSError) as e:
        raise SourceError(f"{type(e).__name__}: {e}" if str(e) else type(e).__name__) from e


def decode_frame(data: bytes | str) -> Any:
    """WS-кадр → JSON. Бинарные кадры пробуем распаковать gzip/zlib."""
    if isinstance(data, (bytes, bytearray)):
        raw = bytes(data)
        for fn in (gzip.decompress, zlib.decompress, lambda b: zlib.decompress(b, -zlib.MAX_WBITS)):
            try:
                raw = fn(raw)
                break
            except Exception:
                continue
        return orjson.loads(raw)
    return orjson.loads(data)
