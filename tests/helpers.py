"""Синтетические данные для тестов (без сети)."""
from __future__ import annotations

import math
import random

from scalpbot.config import Settings
from scalpbot.market.tier1 import Tier1
from scalpbot.market.universe import Universe


def make_settings(**over) -> Settings:
    env = {"TELEGRAM_CHAT_ID": "1", "DATA_DIR": "./data-test"}
    env.update({k: str(v) for k, v in over.items()})
    return Settings(env=env)


def make_universe(symbols: dict[str, float] | None = None) -> Universe:
    symbols = symbols or {"BTC_USDT": 0.0001, "AGT_USDT": 10.0}
    u = Universe()
    u.load([{"symbol": s, "state": 0, "contractSize": cs, "priceUnit": 0.00001, "baseCoin": s[:-5],
             "quoteCoin": "USDT", "settleCoin": "USDT"} for s, cs in symbols.items()])
    return u


class Feed:
    """Подаёт в Tier1 кадры push.tickers каждые 2 с."""

    def __init__(self, tier1: Tier1, start_ts: int = 1_700_000_000_000, amount24: float = 5e6):
        self.tier1 = tier1
        self.ts = start_ts
        self.amount24 = amount24
        self.prices: dict[str, float] = {}

    def step(self, prices: dict[str, float], dt_ms: int = 2000) -> int:
        self.ts += dt_ms
        self.prices.update(prices)
        self.tier1.on_tickers_frame(self.ts, [{"symbol": s, "lastPrice": p, "amount24": self.amount24}
                                              for s, p in self.prices.items()])
        return self.ts

    def noise(self, minutes: float, base: dict[str, float], vol: float = 0.0005, seed: int = 1) -> None:
        rnd = random.Random(seed)
        cur = dict(base)
        for _ in range(int(minutes * 30)):
            for s in cur:
                cur[s] *= math.exp(rnd.gauss(0, vol))
            self.step(cur)
