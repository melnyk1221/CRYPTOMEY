"""Публичный REST спота MEXC (без WebSocket в v1).

Лимит биржи: у каждого эндпоинта свой бюджет 500 запросов / 10 с, при превышении 429, при повторах —
бан IP. Держимся ниже 20%: 100 запросов / 10 с.
"""
from __future__ import annotations

import logging
from array import array

import aiohttp

from ..health import SourceStatus
from ..ratelimit import TokenBucket
from .common import SourceError, request_json

log = logging.getLogger("mexc.spot")

BASE = "https://api.mexc.com"


class MexcSpotRest:
    def __init__(self, session: aiohttp.ClientSession, status: SourceStatus | None = None):
        self.session = session
        self.status = status
        self.bucket = TokenBucket.per(100, 10.0, burst=20)
        self.symbols: set[str] = set()
        self.quote_volume: dict[str, float] = {}
        self.requests = 0
        self.errors = 0

    async def _get(self, path: str, params: dict | None = None, weight: float = 1.0):
        await self.bucket.acquire(min(weight, self.bucket.capacity))
        self.requests += 1
        try:
            js, lat = await request_json(self.session, "GET", BASE + path, params=params)
        except SourceError:
            self.errors += 1
            raise
        if self.status:
            self.status.latency_ms = lat
            self.status.beat()
        return js

    async def load_exchange_info(self) -> int:
        js = await self._get("/api/v3/exchangeInfo", weight=10)
        syms = set()
        for s in js.get("symbols", []):
            if s.get("quoteAsset") == "USDT" and str(s.get("status")) in ("1", "ENABLED", "TRADING") \
                    and s.get("isSpotTradingAllowed", True):
                syms.add(s["symbol"])
        if syms:
            self.symbols = syms
        return len(syms)

    async def load_tickers_24h(self) -> int:
        js = await self._get("/api/v3/ticker/24hr", weight=20)
        qv = {}
        for t in js if isinstance(js, list) else [js]:
            try:
                qv[t["symbol"]] = float(t.get("quoteVolume") or 0)
            except (KeyError, TypeError, ValueError):
                continue
        if qv:
            self.quote_volume = qv
        return len(qv)

    async def depth(self, symbol: str, limit: int = 50) -> dict:
        return await self._get("/api/v3/depth", {"symbol": symbol, "limit": limit})


class SpotSnapshot:
    """Компактный снимок стакана спота: топ-N уровней в array('d') [p0, q0, p1, q1, …]."""

    __slots__ = ("ts", "best_bid", "best_ask", "bids", "asks")

    def __init__(self, ts: int, bids: list, asks: list, n: int = 50):
        self.ts = ts
        b = array("d")
        for p, q in bids[:n]:
            b.append(float(p))
            b.append(float(q))
        a = array("d")
        for p, q in asks[:n]:
            a.append(float(p))
            a.append(float(q))
        self.bids = b
        self.asks = a
        self.best_bid = b[0] if b else 0.0
        self.best_ask = a[0] if a else 0.0

    @property
    def mid(self) -> float:
        if self.best_bid > 0 and self.best_ask > 0:
            return 0.5 * (self.best_bid + self.best_ask)
        return 0.0

    def levels(self, is_bid: bool) -> list[tuple[float, float]]:
        arr = self.bids if is_bid else self.asks
        return [(arr[i], arr[i + 1]) for i in range(0, len(arr), 2)]

    def usd_at(self, is_bid: bool, price: float) -> float:
        arr = self.bids if is_bid else self.asks
        for i in range(0, len(arr), 2):
            if arr[i] == price:
                return arr[i] * arr[i + 1]
        return 0.0
