"""Binance — в v1 только проверка доступности (из регионов Railway Binance может отвечать 451).

Потоки Binance не реализованы. Обходить блокировку нельзя.
"""
from __future__ import annotations

import aiohttp

from .common import SourceError, request_json

PING_URL = "https://fapi.binance.com/fapi/v1/ping"


async def probe(session: aiohttp.ClientSession) -> tuple[str, int | None, str]:
    """('ok' | 'blocked' | 'error', http_status, ошибка)."""
    try:
        await request_json(session, "GET", PING_URL)
        return "ok", 200, ""
    except SourceError as e:
        return ("blocked" if e.blocked else "error"), e.status, str(e)[:120]
