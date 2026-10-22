"""Восстановление тейкер-ордеров из сырых заполнений и скользящая дельта.

С compress:false один агрессивный ордер, прошедший несколько уровней, даёт несколько заполнений
с одинаковыми t и T. Подряд идущие заполнения с одинаковым (t, T) склеиваются в один TakerOrder.
Self-trade (M == 1) исключаются из объёма, дельты и детектора роботов.
"""
from __future__ import annotations

from collections import deque

from .stats import SideBuckets


class TakerOrder:
    __slots__ = ("ts", "side_buy", "notional_usd", "n_fills", "vwap", "qty")

    def __init__(self, ts: int, side_buy: bool, notional_usd: float, n_fills: int, vwap: float, qty: float):
        self.ts = ts
        self.side_buy = side_buy
        self.notional_usd = notional_usd
        self.n_fills = n_fills
        self.vwap = vwap
        self.qty = qty

    def __repr__(self) -> str:
        return f"TakerOrder({'B' if self.side_buy else 'S'} ${self.notional_usd:.0f} x{self.n_fills} @{self.vwap})"


class Fill:
    __slots__ = ("ts", "price", "side_buy", "usd")

    def __init__(self, ts: int, price: float, side_buy: bool, usd: float):
        self.ts = ts
        self.price = price
        self.side_buy = side_buy
        self.usd = usd


class TradeAggregator:
    def __init__(self, symbol: str, contract_size: float, maxlen: int = 2000):
        self.symbol = symbol
        self.contract_size = contract_size
        self.orders: deque[TakerOrder] = deque(maxlen=maxlen)
        self.fills: deque[Fill] = deque(maxlen=3000)
        self.buckets = SideBuckets(300)
        self.new_orders: list[TakerOrder] = []
        self.self_trades = 0
        self.fill_count = 0
        self.last_trade_ts = 0
        self.last_price = 0.0
        # текущая группа заполнений
        self._key: tuple[int, int] | None = None
        self._usd = 0.0
        self._qty = 0.0
        self._pq = 0.0
        self._n = 0

    def add_fill(self, it: dict) -> None:
        if it.get("M") == 1:
            self.self_trades += 1
            return
        try:
            t = int(it["t"])
            side = int(it["T"])
            p = float(it["p"])
            v = float(it["v"])
        except (KeyError, TypeError, ValueError):
            return
        if p <= 0 or v <= 0:
            return
        self.fill_count += 1
        key = (t, side)
        if self._key is not None and key != self._key:
            self._finalize()
        qty = v * self.contract_size
        usd = qty * p
        self._key = key
        self._usd += usd
        self._qty += qty
        self._pq += p * qty
        self._n += 1
        buy = side == 1
        self.fills.append(Fill(t, p, buy, usd))
        self.buckets.add(t, buy, usd)
        self.last_trade_ts = max(self.last_trade_ts, t)
        self.last_price = p

    def _finalize(self) -> None:
        if self._key is None or self._n == 0:
            return
        t, side = self._key
        o = TakerOrder(t, side == 1, self._usd, self._n, self._pq / self._qty if self._qty else 0.0, self._qty)
        self.orders.append(o)
        self.new_orders.append(o)
        if len(self.new_orders) > 5000:
            del self.new_orders[:-2000]
        self._key = None
        self._usd = self._qty = self._pq = 0.0
        self._n = 0

    def flush(self, now_ts: int, max_age_ms: int = 300) -> None:
        """Закрывает текущую группу, если с её t прошло больше max_age_ms."""
        if self._key is not None and now_ts - self._key[0] >= max_age_ms:
            self._finalize()

    def drain_new(self) -> list[TakerOrder]:
        out, self.new_orders = self.new_orders, []
        return out

    def delta(self, now_ms: int, seconds: int) -> tuple[float, float]:
        """(покупки $, продажи $) тейкеров за окно."""
        return self.buckets.window(now_ms, seconds)

    def traded_at(self, price: float, side_buy: bool, since_ts: int, until_ts: int, tol: float = 1e-9) -> float:
        """$ объём заполнений по цене price указанной стороны тейкера в (since, until]."""
        tot = 0.0
        for f in reversed(self.fills):
            if f.ts <= since_ts:
                break
            if f.ts <= until_ts and f.side_buy == side_buy and abs(f.price - price) <= tol * max(1.0, price):
                tot += f.usd
        return tot

    def traded_through(self, price: float, side_buy: bool, since_ts: int) -> float:
        """$ объём, напечатанный на уровне или за ним (продажи ≤ price для бида, покупки ≥ price для аска)."""
        tot = 0.0
        for f in reversed(self.fills):
            if f.ts <= since_ts:
                break
            if f.side_buy == side_buy and ((not side_buy and f.price <= price) or (side_buy and f.price >= price)):
                tot += f.usd
        return tot
