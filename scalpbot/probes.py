"""Проверка доступности источников при старте: ok / blocked (403/451) / error."""
from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass

import aiohttp
import orjson

from .config import Settings
from .sources.common import SourceError, request_json
from .sources.mexc_futures_ws import WS_URL

PROBE_TIMEOUT = aiohttp.ClientTimeout(total=10, connect=6)

# name -> (подпись, режим из настроек или "on")
SOURCES = {
    "mexc_futures": "MEXC фьючерсы",
    "mexc_futures_ws": "MEXC WS",
    "mexc_spot": "MEXC спот",
    "dexscreener": "DexScreener",
    "bybit": "Bybit",
    "hyperliquid": "Hyperliquid",
    "binance": "Binance",
}


@dataclass
class ProbeResult:
    name: str
    state: str                 # ok | blocked | error | disabled
    latency_ms: float | None = None
    http_status: int | None = None
    error: str = ""


def mode_of(settings: Settings, name: str) -> str:
    return {"bybit": settings.ENABLE_BYBIT, "hyperliquid": settings.ENABLE_HYPERLIQUID,
            "binance": settings.ENABLE_BINANCE}.get(name, "on")


async def _http(session: aiohttp.ClientSession, name: str, method: str, url: str, json=None) -> ProbeResult:
    try:
        _, lat = await request_json(session, method, url, json=json, timeout=PROBE_TIMEOUT)
        return ProbeResult(name, "ok", round(lat))
    except SourceError as e:
        return ProbeResult(name, "blocked" if e.blocked else "error", None, e.status, str(e)[:120])


async def _ws(session: aiohttp.ClientSession) -> ProbeResult:
    t0 = time.monotonic()
    try:
        async with session.ws_connect(WS_URL, timeout=aiohttp.ClientWSTimeout(ws_close=5),
                                      heartbeat=None) as ws:
            await ws.send_str(orjson.dumps({"method": "ping"}).decode())
            msg = await ws.receive(timeout=8)
            lat = (time.monotonic() - t0) * 1000
            if msg.type == aiohttp.WSMsgType.TEXT and "pong" in msg.data:
                return ProbeResult("mexc_futures_ws", "ok", round(lat))
            return ProbeResult("mexc_futures_ws", "error", round(lat), None, f"неожиданный ответ: {str(msg.data)[:80]}")
    except aiohttp.WSServerHandshakeError as e:
        return ProbeResult("mexc_futures_ws", "blocked" if e.status in (403, 451) else "error", None, e.status, str(e)[:120])
    except Exception as e:  # noqa: BLE001
        return ProbeResult("mexc_futures_ws", "error", None, None, f"{type(e).__name__}: {e}"[:120])


async def run_probes(session: aiohttp.ClientSession, settings: Settings) -> dict[str, ProbeResult]:
    base = settings.MEXC_FUTURES_REST_BASE
    jobs = {
        "mexc_futures": _http(session, "mexc_futures", "GET", f"{base}/api/v1/contract/ping"),
        "mexc_futures_ws": _ws(session),
        "mexc_spot": _http(session, "mexc_spot", "GET", "https://api.mexc.com/api/v3/ping"),
        "dexscreener": _http(session, "dexscreener", "GET", "https://api.dexscreener.com/latest/dex/search?q=BTC"),
        "bybit": _http(session, "bybit", "GET", "https://api.bybit.com/v5/market/time"),
        "hyperliquid": _http(session, "hyperliquid", "POST", "https://api.hyperliquid.xyz/info", {"type": "meta"}),
        "binance": _http(session, "binance", "GET", "https://fapi.binance.com/fapi/v1/ping"),
    }
    out: dict[str, ProbeResult] = {}
    names = []
    coros = []
    for name, coro in jobs.items():
        if mode_of(settings, name) == "off":
            coro.close()
            out[name] = ProbeResult(name, "disabled")
            continue
        names.append(name)
        coros.append(coro)
    for name, res in zip(names, await asyncio.gather(*coros)):
        out[name] = res
    # Если основной REST упал, пробуем запасную базу.
    if out["mexc_futures"].state != "ok" and base != "https://api.mexc.com":
        alt = await _http(session, "mexc_futures", "GET", "https://api.mexc.com/api/v1/contract/ping")
        if alt.state == "ok":
            alt.error = "основная база недоступна, работает api.mexc.com"
            out["mexc_futures"] = alt
    return out


def format_table(results: dict[str, ProbeResult]) -> str:
    lines = [f"{'источник':<16} {'статус':<9} {'мс':>6} {'http':>5}  ошибка"]
    for name, r in results.items():
        lines.append(f"{name:<16} {r.state:<9} {r.latency_ms if r.latency_ms is not None else '—':>6} "
                     f"{r.http_status or '—':>5}  {r.error}")
    return "\n".join(lines)
