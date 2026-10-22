"""DexScreener (публично, без ключа): поиск токена, сопоставление с монетой MEXC, капитализация, DEX-объём.

Тикеры неоднозначны, поэтому:
1. берём пары с baseToken.symbol == BASE;
2. сортируем по liquidity.usd;
3. принимаем первую, у которой |priceUsd / цена_MEXC − 1| ≤ 3%. Если у самой ликвидной пары цена
   не совпадает, но совпадает у другой — берём её и помечаем (на живых данных у AGT самая ликвидная
   пара — другой токен с тем же тикером). Если не совпала ни одна — «нет надёжного соответствия».
Сопоставление кэшируется на 24 ч, метрики — на 5 мин. Ручная привязка: /map SYMBOL CHAIN ADDRESS.
"""
from __future__ import annotations

import logging
import re
import time
from typing import Any

import aiohttp

from ..health import SourceStatus
from ..ratelimit import TokenBucket
from ..storage.db import Database
from .common import SourceError, request_json

log = logging.getLogger("dexscreener")

BASE = "https://api.dexscreener.com"
MATCH_TTL_MS = 24 * 3600_000
METRICS_TTL_MS = 5 * 60_000
PRICE_TOL = 0.03

_MULT_RE = re.compile(r"^(1000000|100000|10000|1000)([A-Z].*)$")

# Для мейджоров DexScreener находит только обёрнутые токены (wBTC и т.п.) — капа и DEX-объём бессмысленны.
SKIP_BASES = frozenset({
    "BTC", "ETH", "SOL", "BNB", "XRP", "DOGE", "ADA", "TRX", "LTC", "BCH", "DOT", "AVAX", "LINK", "TON",
    "MATIC", "POL", "ATOM", "XLM", "ETC", "FIL", "NEAR", "APT", "SUI", "ARB", "OP", "XMR", "HBAR", "ICP",
    "USDC", "USDT", "DAI", "WBTC", "WETH", "STETH",
})


def skip_dex(base: str) -> bool:
    b = base.upper()
    return b in SKIP_BASES or b.endswith("STOCK")


def split_multiplier(base: str) -> tuple[str, float]:
    """'1000BONK' → ('BONK', 1000). Обычные тикеры вроде '1INCH' не трогаем."""
    m = _MULT_RE.match(base.upper())
    if m:
        return m.group(2), float(m.group(1))
    return base.upper(), 1.0


def _f(x: Any) -> float | None:
    try:
        v = float(x)
        return v if v == v else None
    except (TypeError, ValueError):
        return None


def match_pair(pairs: list[dict], base: str, ref_price: float) -> tuple[dict | None, str]:
    """Возвращает (пара, примечание). Пара None — надёжного соответствия нет."""
    exact = [p for p in pairs if str((p.get("baseToken") or {}).get("symbol", "")).upper() == base.upper()]
    if not exact:
        return None, "нет пар с таким тикером"
    exact.sort(key=lambda p: _f((p.get("liquidity") or {}).get("usd")) or 0.0, reverse=True)
    if ref_price <= 0:
        return None, "нет цены MEXC для проверки"
    for i, p in enumerate(exact):
        px = _f(p.get("priceUsd"))
        if px and abs(px / ref_price - 1) <= PRICE_TOL:
            return p, ("" if i == 0 else "самая ликвидная пара — другой токен, выбрана совпадающая по цене")
    top_px = _f(exact[0].get("priceUsd"))
    return None, f"цена не совпадает (DEX {top_px} vs MEXC {ref_price:g})"


def build_metrics(pair: dict, all_pairs: list[dict], note: str = "") -> dict:
    tok = pair.get("baseToken") or {}
    addr = tok.get("address")
    chain = pair.get("chainId")
    same = [p for p in all_pairs if (p.get("baseToken") or {}).get("address") == addr and p.get("chainId") == chain]
    vol24 = sum(_f((p.get("volume") or {}).get("h24")) or 0.0 for p in same) if same else \
        (_f((pair.get("volume") or {}).get("h24")) or 0.0)
    liq = sum(_f((p.get("liquidity") or {}).get("usd")) or 0.0 for p in same) if same else \
        (_f((pair.get("liquidity") or {}).get("usd")) or 0.0)
    mcap = _f(pair.get("marketCap"))
    fdv = _f(pair.get("fdv"))
    src = "mcap"
    if not mcap:
        mcap, src = fdv, "fdv"
    vol = pair.get("volume") or {}
    return {
        "chain_id": chain, "address": addr, "pair_address": pair.get("pairAddress"), "dex_id": pair.get("dexId"),
        "price_usd": _f(pair.get("priceUsd")), "liquidity_usd": liq, "volume_h24": vol24,
        "volume_h1": _f(vol.get("h1")), "volume_m5": _f(vol.get("m5")),
        "mcap": mcap, "fdv": fdv, "mcap_source": src if mcap else None,
        "price_change": pair.get("priceChange") or {}, "pairs": len(same) or 1, "note": note,
    }


class DexScreener:
    def __init__(self, session: aiohttp.ClientSession, db: Database, status: SourceStatus | None = None):
        self.session = session
        self.db = db
        self.status = status
        # Свой бюджет ≤ 30 запросов/мин (у API ~300/мин).
        self.bucket = TokenBucket.per(30, 60.0, burst=5)
        self.requests = 0
        self.errors = 0

    async def _get(self, path: str, params: dict | None = None) -> Any:
        await self.bucket.acquire()
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

    async def search(self, q: str) -> list[dict]:
        js = await self._get("/latest/dex/search", {"q": q})
        return list((js or {}).get("pairs") or [])

    async def token_pairs(self, chain_id: str, address: str) -> list[dict]:
        js = await self._get(f"/tokens/v1/{chain_id}/{address}")
        return list(js or []) if isinstance(js, list) else list((js or {}).get("pairs") or [])

    async def metrics(self, symbol: str, base: str, mexc_price: float, force: bool = False) -> dict | None:
        """Метрики токена (кэш 5 мин). None — нет надёжного соответствия."""
        now = int(time.time() * 1000)
        if skip_dex(split_multiplier(base)[0]):
            return None
        if not force:
            cached = await self.db.get_dex_cache(symbol)
            if cached and now - cached[1] < METRICS_TTL_MS:
                return cached[0] or None
        base_clean, mult = split_multiplier(base)
        ref = mexc_price / mult if mexc_price > 0 else 0.0
        mapping = await self.db.get_dex_map(symbol)
        result: dict | None = None
        try:
            if mapping and mapping["address"] and (mapping["manual"] or now - int(mapping["ts"]) < MATCH_TTL_MS):
                pairs = await self.token_pairs(mapping["chain_id"], mapping["address"])
                pairs.sort(key=lambda p: _f((p.get("liquidity") or {}).get("usd")) or 0.0, reverse=True)
                if pairs:
                    result = build_metrics(pairs[0], pairs, "ручная привязка" if mapping["manual"] else "")
            if result is None and not (mapping and mapping["manual"]):
                pairs: list[dict] = []
                try:
                    pairs = await self.search(base_clean)
                except SourceError as e:
                    if e.status != 400:   # короткие тикеры (например «Q») API отклоняет
                        raise
                pair, note = match_pair(pairs, base_clean, ref)
                if pair is None:
                    pairs = await self.search(f"{base_clean} USDT")
                    pair, note = match_pair(pairs, base_clean, ref)
                if pair is not None:
                    result = build_metrics(pair, pairs, note)
                    await self.db.set_dex_map(symbol, result["chain_id"], result["address"], manual=False)
                else:
                    log.info("DexScreener: нет надёжного соответствия", extra={"symbol": symbol, "why": note})
        except SourceError as e:
            log.warning("DexScreener: ошибка", extra={"symbol": symbol, "err": str(e)})
            cached = await self.db.get_dex_cache(symbol)
            return cached[0] if cached and cached[0] else None
        if result is not None:
            result["multiplier"] = mult
        await self.db.set_dex_cache(symbol, result or {})
        return result

    async def set_manual(self, symbol: str, chain_id: str, address: str) -> dict | None:
        pairs = await self.token_pairs(chain_id, address)
        if not pairs:
            return None
        await self.db.set_dex_map(symbol, chain_id, address, manual=True)
        pairs.sort(key=lambda p: _f((p.get("liquidity") or {}).get("usd")) or 0.0, reverse=True)
        m = build_metrics(pairs[0], pairs, "ручная привязка")
        await self.db.set_dex_cache(symbol, m)
        return m
