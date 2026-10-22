"""Ликвидации Bybit (публично): у MEXC нет публичного канала ликвидаций.

WS wss://stream.bybit.com/v5/public/linear, топик allLiquidation.{SYMBOL}, пуш каждые 500 мс.
Поля: T (мс), s, S, v (кол-во монет), p (цена банкротства).
Сторона: S = "Buy" → ликвидирован ЛОНГ; S = "Sell" → ликвидирован ШОРТ.
Пинг {"op":"ping"} каждые 20 с; подписка пачками ≤ 10 топиков.
Если Bybit блокирует IP (403/451) — источник отключается, бот работает дальше.
"""
from __future__ import annotations

import asyncio
import logging
import time
from typing import Callable

import aiohttp
import orjson

from ..health import SourceStatus
from .common import SourceError, request_json

log = logging.getLogger("bybit")

WS_URL = "wss://stream.bybit.com/v5/public/linear"
REST = "https://api.bybit.com"
BATCH = 10


class Liquidation:
    __slots__ = ("ts", "symbol", "long_liq", "usd", "price")

    def __init__(self, ts: int, symbol: str, long_liq: bool, usd: float, price: float):
        self.ts = ts
        self.symbol = symbol
        self.long_liq = long_liq
        self.usd = usd
        self.price = price


def parse_liquidations(msg: dict) -> list[Liquidation]:
    topic = str(msg.get("topic") or "")
    if not topic.startswith("allLiquidation."):
        return []
    data = msg.get("data")
    items = data if isinstance(data, list) else [data] if isinstance(data, dict) else []
    out = []
    for it in items:
        try:
            v = float(it["v"])
            p = float(it["p"])
            side = str(it["S"])
            out.append(Liquidation(int(it["T"]), str(it["s"]), side == "Buy", v * p, p))
        except (KeyError, TypeError, ValueError):
            continue
    return out


def to_bybit(symbol: str) -> str:
    return symbol.replace("_", "")


class BybitLiquidations:
    def __init__(self, session: aiohttp.ClientSession, status: SourceStatus,
                 on_liq: Callable[[Liquidation], None]):
        self.session = session
        self.status = status
        self.on_liq = on_liq
        self.instruments: set[str] = set()
        self.topics: set[str] = set()
        self._ws: aiohttp.ClientWebSocketResponse | None = None
        self.received = 0

    async def load_instruments(self) -> int:
        cursor = ""
        syms: set[str] = set()
        for _ in range(20):
            params = {"category": "linear", "limit": 1000}
            if cursor:
                params["cursor"] = cursor
            js, _ = await request_json(self.session, "GET", REST + "/v5/market/instruments-info", params=params)
            res = js.get("result") or {}
            for it in res.get("list") or []:
                if it.get("status") == "Trading" and it.get("quoteCoin") == "USDT":
                    syms.add(it["symbol"])
            cursor = res.get("nextPageCursor") or ""
            if not cursor:
                break
        if syms:
            self.instruments = syms
        return len(syms)

    def has(self, symbol: str) -> bool:
        return to_bybit(symbol) in self.instruments

    async def _send(self, op: str, topics: list[str]) -> None:
        ws = self._ws
        if ws is None or ws.closed or not topics:
            return
        for i in range(0, len(topics), BATCH):
            await ws.send_str(orjson.dumps({"op": op, "args": topics[i:i + BATCH]}).decode())

    async def subscribe(self, symbol: str) -> None:
        if not self.has(symbol):
            return
        t = f"allLiquidation.{to_bybit(symbol)}"
        if t not in self.topics:
            self.topics.add(t)
            await self._send("subscribe", [t])

    async def unsubscribe(self, symbol: str) -> None:
        t = f"allLiquidation.{to_bybit(symbol)}"
        if t in self.topics:
            self.topics.discard(t)
            await self._send("unsubscribe", [t])

    async def run(self) -> None:
        if not self.status.enabled:
            await asyncio.Event().wait()   # источник отключён — просто ждём остановки
        try:
            async with self.session.ws_connect(WS_URL, heartbeat=None) as ws:
                self._ws = ws
                self.status.state = "ok"
                await self._send("subscribe", sorted(self.topics))
                pinger = asyncio.create_task(self._ping(ws))
                try:
                    async for m in ws:
                        if m.type != aiohttp.WSMsgType.TEXT:
                            if m.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                                break
                            continue
                        self.status.beat()
                        try:
                            d = orjson.loads(m.data)
                        except orjson.JSONDecodeError:
                            continue
                        for liq in parse_liquidations(d):
                            self.received += 1
                            self.on_liq(liq)
                finally:
                    pinger.cancel()
                    self._ws = None
        except aiohttp.WSServerHandshakeError as e:
            if e.status in (403, 451):
                self.status.enabled = False
                self.status.state = "blocked"
                self.status.http_status = e.status
                log.warning("Bybit блокирует подключение — источник отключён", extra={"status": e.status})
                await asyncio.Event().wait()
            raise
        raise SourceError("Bybit WS закрыт")

    async def _ping(self, ws: aiohttp.ClientWebSocketResponse) -> None:
        msg = orjson.dumps({"op": "ping"}).decode()
        while not ws.closed:
            await asyncio.sleep(20)
            try:
                await ws.send_str(msg)
            except Exception:
                return


def liq_window(ring, now_ms: int, window_ms: int = 15 * 60_000) -> dict:
    """Суммы ликвидаций за окно: {'long_usd', 'short_usd', 'n'}."""
    long_usd = short_usd = 0.0
    n = 0
    for liq in reversed(ring):
        if now_ms - liq.ts > window_ms:
            break
        n += 1
        if liq.long_liq:
            long_usd += liq.usd
        else:
            short_usd += liq.usd
    return {"long_usd": round(long_usd, 2), "short_usd": round(short_usd, 2), "n": n, "at": time.time()}
