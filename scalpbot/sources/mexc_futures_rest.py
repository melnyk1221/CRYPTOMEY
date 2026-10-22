"""Публичный REST MEXC Futures (без ключей) с ограничением частоты."""
from __future__ import annotations

import logging
from typing import Any

import aiohttp

from ..health import SourceStatus
from ..ratelimit import TokenBucket
from .common import SourceError, request_json

log = logging.getLogger("mexc.rest")

FALLBACK_BASE = "https://api.mexc.com"


class MexcFuturesRest:
    def __init__(self, session: aiohttp.ClientSession, base: str, status: SourceStatus | None = None):
        self.session = session
        self.bases = [base.rstrip("/")]
        if FALLBACK_BASE not in self.bases:
            self.bases.append(FALLBACK_BASE)
        self.base_idx = 0
        self.status = status
        # Лимит биржи 20 запросов / 2 с — используем не больше половины.
        self.bucket = TokenBucket.per(10, 2.0)
        # contract/detail: 1 запрос / 5 с.
        self.detail_bucket = TokenBucket.per(1, 5.0)
        self.requests = 0
        self.errors = 0

    @property
    def base(self) -> str:
        return self.bases[self.base_idx]

    async def _get(self, path: str, params: dict | None = None, bucket: TokenBucket | None = None) -> Any:
        await (bucket or self.bucket).acquire()
        last_err: SourceError | None = None
        for attempt in range(len(self.bases)):
            url = self.base + path
            try:
                self.requests += 1
                js, lat = await request_json(self.session, "GET", url, params=params)
                if self.status:
                    self.status.latency_ms = lat
                if isinstance(js, dict) and js.get("success") is False:
                    raise SourceError(f"MEXC code={js.get('code')} msg={js.get('message')}", 200)
                return js.get("data") if isinstance(js, dict) else js
            except SourceError as e:
                self.errors += 1
                last_err = e
                # На сетевой ошибке или 5xx переключаемся на запасной адрес.
                if e.status is None or (e.status >= 500):
                    old = self.base
                    self.base_idx = (self.base_idx + 1) % len(self.bases)
                    log.warning("MEXC REST: переключение базы", extra={"from": old, "to": self.base, "err": str(e)})
                    continue
                raise
        assert last_err
        raise last_err

    async def ping(self) -> Any:
        return await self._get("/api/v1/contract/ping")

    async def contract_detail(self, symbol: str | None = None) -> list[dict]:
        data = await self._get("/api/v1/contract/detail", {"symbol": symbol} if symbol else None, self.detail_bucket)
        if isinstance(data, dict):
            return [data]
        return list(data or [])

    async def tickers(self) -> list[dict]:
        data = await self._get("/api/v1/contract/ticker")
        if isinstance(data, dict):
            return [data]
        return list(data or [])

    async def depth(self, symbol: str, limit: int = 1000) -> dict:
        return await self._get(f"/api/v1/contract/depth/{symbol}", {"limit": limit})

    async def depth_commits(self, symbol: str, n: int = 1000) -> list[dict]:
        data = await self._get(f"/api/v1/contract/depth_commits/{symbol}/{n}")
        # VERIFY: биржа отдаёт коммиты по убыванию версии — сортируем по возрастанию.
        return sorted(data or [], key=lambda c: int(c.get("version", 0)))

    async def deals(self, symbol: str, limit: int = 100) -> list[dict]:
        return list(await self._get(f"/api/v1/contract/deals/{symbol}", {"limit": min(100, limit)}) or [])

    async def klines(self, symbol: str, interval: str = "Min1", start: int | None = None,
                     end: int | None = None) -> dict:
        params: dict[str, Any] = {"interval": interval}
        if start:
            params["start"] = int(start)
        if end:
            params["end"] = int(end)
        return await self._get(f"/api/v1/contract/kline/{symbol}", params)

    async def funding_rate(self, symbol: str) -> dict:
        return await self._get(f"/api/v1/contract/funding_rate/{symbol}")
