"""Hyperliquid (публично): OI и funding той же монеты как контекст с другой площадки.

POST https://api.hyperliquid.xyz/info {"type":"metaAndAssetCtxs"} → [meta, assetCtxs];
meta["universe"][i]["name"] соответствует assetCtxs[i]. openInterest — в монетах (× markPx = USD).
Funding на Hyperliquid начисляется каждый час. Опрос раз в 60 с, один запрос на все активы.
"""
from __future__ import annotations

import asyncio
import logging
import re

import aiohttp

from ..health import SourceStatus
from .common import SourceError, request_json

log = logging.getLogger("hyperliquid")

URL = "https://api.hyperliquid.xyz/info"
_K_RE = re.compile(r"^1000([A-Z].*)$")


def hl_name(base: str) -> str:
    """MEXC base → имя на Hyperliquid: '1000PEPE' → 'kPEPE'."""
    m = _K_RE.match(base.upper())
    return "k" + m.group(1) if m else base.upper()


def parse_ctxs(js) -> dict[str, dict]:
    out: dict[str, dict] = {}
    if not isinstance(js, list) or len(js) != 2:
        return out
    meta, ctxs = js
    for u, c in zip(meta.get("universe") or [], ctxs or []):
        if u.get("isDelisted"):
            continue
        try:
            mark = float(c.get("markPx") or 0)
            oi = float(c.get("openInterest") or 0)
            out[u["name"]] = {
                "funding_pct_1h": float(c.get("funding") or 0) * 100.0,
                "oi_usd": oi * mark,
                "mark_px": mark,
                "day_ntl_vlm": float(c.get("dayNtlVlm") or 0),
                "premium": float(c.get("premium") or 0),
                "prev_day_px": float(c.get("prevDayPx") or 0),
            }
        except (TypeError, ValueError, KeyError):
            continue
    return out


class Hyperliquid:
    def __init__(self, session: aiohttp.ClientSession, status: SourceStatus):
        self.session = session
        self.status = status
        self.assets: dict[str, dict] = {}

    async def poll(self) -> int:
        js, lat = await request_json(self.session, "POST", URL, json={"type": "metaAndAssetCtxs"})
        self.status.latency_ms = lat
        self.status.beat()
        assets = parse_ctxs(js)
        if assets:
            self.assets = assets
        return len(assets)

    def get(self, base: str) -> dict | None:
        return self.assets.get(hl_name(base))

    async def run(self) -> None:
        while True:
            try:
                await self.poll()
                self.status.state = "ok"
            except SourceError as e:
                self.status.state = "blocked" if e.blocked else "error"
                self.status.error = str(e)[:80]
                log.warning("Hyperliquid: ошибка опроса", extra={"err": str(e)})
            await asyncio.sleep(60)
