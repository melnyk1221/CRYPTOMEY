"""WebSocket MEXC Futures (wss://contract.mexc.com/edge): пинг, подписки, переподключение.

Один экземпляр = одно соединение. Переподключение делает супервизор (run() завершается при обрыве),
после переподключения все подписки отправляются заново, затем вызывается on_connect (пересним стаканы).
"""
from __future__ import annotations

import asyncio
import logging
import time
from typing import Any, Awaitable, Callable

import aiohttp
import orjson

from ..bus import DropQueue, MarketMsg
from ..health import SourceStatus
from .common import decode_frame

log = logging.getLogger("mexc.ws")

WS_URL = "wss://contract.mexc.com/edge"


class WsClosed(Exception):
    expected = True  # обычный обрыв: супервизор переподключит без трейсбека


def sub_key(method: str, param: dict | None) -> str:
    sym = (param or {}).get("symbol", "")
    return f"{method}:{sym}"


class MexcWs:
    def __init__(self, session: aiohttp.ClientSession, name: str, out: DropQueue[MarketMsg],
                 status: SourceStatus | None = None, url: str = WS_URL,
                 ping_interval: float = 15.0, silence_timeout: float = 30.0,
                 max_lifetime: float = 12 * 3600,
                 on_connect: Callable[[], Awaitable[None]] | None = None):
        self.session = session
        self.name = name
        self.url = url
        self.out = out
        self.status = status
        self.ping_interval = ping_interval
        self.silence_timeout = silence_timeout
        self.max_lifetime = max_lifetime
        self.on_connect = on_connect
        self._subs: dict[str, dict] = {}
        self._ws: aiohttp.ClientWebSocketResponse | None = None
        self.connected = False
        self.connects = 0
        self.messages = 0
        self.last_message_at = 0.0
        self.errors: list[str] = []

    # ---------- подписки ----------
    @property
    def symbols(self) -> set[str]:
        return {k.split(":", 1)[1] for k in self._subs if ":" in k and k.split(":", 1)[1]}

    async def subscribe(self, method: str, param: dict | None = None, gzip: bool = False) -> None:
        msg: dict[str, Any] = {"method": method}
        if param is not None:
            msg["param"] = param
        if not gzip:
            msg["gzip"] = False
        self._subs[sub_key(method, param)] = msg
        await self._send(msg)

    async def unsubscribe(self, method: str, param: dict | None = None) -> None:
        key = sub_key(method, param)
        if self._subs.pop(key, None) is None:
            return
        umethod = "unsub." + method.split(".", 1)[1]
        msg: dict[str, Any] = {"method": umethod}
        if param is not None:
            msg["param"] = {k: v for k, v in param.items() if k == "symbol"}
        await self._send(msg)

    async def _send(self, msg: dict) -> None:
        ws = self._ws
        if ws is None or ws.closed:
            return  # отправится после подключения
        try:
            await ws.send_str(orjson.dumps(msg).decode())
        except Exception as e:  # noqa: BLE001
            log.warning("WS: ошибка отправки", extra={"conn": self.name, "err": str(e)})

    # ---------- основной цикл ----------
    async def run(self) -> None:
        self.connected = False
        async with self.session.ws_connect(self.url, heartbeat=None, autoping=True,
                                           max_msg_size=16 * 1024 * 1024, compress=0) as ws:
            self._ws = ws
            self.connected = True
            self.connects += 1
            opened = time.monotonic()
            self.last_message_at = opened
            log.info("WS подключён", extra={"conn": self.name, "subs": len(self._subs)})
            for msg in list(self._subs.values()):
                await ws.send_str(orjson.dumps(msg).decode())
            if self.on_connect:
                asyncio.create_task(self._safe_on_connect())
            pinger = asyncio.create_task(self._ping_loop(ws, opened))
            try:
                await self._read_loop(ws)
            finally:
                pinger.cancel()
                self.connected = False
                self._ws = None
        raise WsClosed(f"{self.name}: соединение закрыто")

    async def _safe_on_connect(self) -> None:
        try:
            assert self.on_connect
            await self.on_connect()
        except Exception:
            log.exception("WS on_connect упал", extra={"conn": self.name})

    async def _ping_loop(self, ws: aiohttp.ClientWebSocketResponse, opened: float) -> None:
        ping = orjson.dumps({"method": "ping"}).decode()
        while not ws.closed:
            await asyncio.sleep(self.ping_interval)
            now = time.monotonic()
            if now - self.last_message_at > self.silence_timeout:
                log.warning("WS молчит — переподключение", extra={"conn": self.name,
                                                                  "silent_s": round(now - self.last_message_at)})
                await ws.close()
                return
            if now - opened > self.max_lifetime:
                log.info("WS: плановое переподключение (12 ч)", extra={"conn": self.name})
                await ws.close()
                return
            try:
                await ws.send_str(ping)
            except Exception:
                await ws.close()
                return

    async def _read_loop(self, ws: aiohttp.ClientWebSocketResponse) -> None:
        async for m in ws:
            if m.type in (aiohttp.WSMsgType.TEXT, aiohttp.WSMsgType.BINARY):
                try:
                    d = decode_frame(m.data)
                except Exception:
                    log.warning("WS: не удалось разобрать кадр", extra={"conn": self.name})
                    continue
                self.last_message_at = time.monotonic()
                self.messages += 1
                if self.status:
                    self.status.beat()
                self._dispatch(d)
            elif m.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR, aiohttp.WSMsgType.CLOSING):
                break

    def _dispatch(self, d: Any) -> None:
        if not isinstance(d, dict):
            return
        ch = d.get("channel") or ""
        if ch == "pong":
            return
        if ch.startswith("rs."):
            if ch == "rs.error" or (d.get("data") not in ("success", None) and "error" in str(d.get("data")).lower()):
                err = str(d.get("data"))[:200]
                self.errors.append(err)
                del self.errors[:-20]
                log.warning("WS: ошибка подписки", extra={"conn": self.name, "resp": err})
            return
        if ch.startswith("push."):
            self.out.put_nowait(MarketMsg(ch, d.get("symbol"), d.get("data"), int(d.get("ts") or 0)))
