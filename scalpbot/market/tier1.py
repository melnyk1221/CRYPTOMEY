"""Tier 1: лёгкое состояние для каждого включённого USDT-перпетуала."""
from __future__ import annotations

import math
from typing import Iterable

from .stats import Ewma, Ring, median
from .universe import Universe

PRICE_RING = 450      # 15 мин при шаге 2 с
OI_RING = 400         # ~100 мин при опросе раз в 15 с
SPREAD_RING = 120     # 30 мин при опросе раз в 15 с
EWMA_HALFLIFE = 1800  # 60 мин в 2-секундных отсчётах


class SymbolState:
    __slots__ = ("symbol", "last_price", "bid1", "ask1", "amount24", "hold_vol", "funding_rate",
                 "funding_cycle_h", "contract_size", "price_unit", "price_ring", "oi_ring", "spread_ring",
                 "ret_ewma", "first_ts", "last_ts", "cooldowns", "rest_ts", "fair_price")

    def __init__(self, symbol: str, contract_size: float, price_unit: float):
        self.symbol = symbol
        self.last_price = 0.0
        self.fair_price = 0.0
        self.bid1 = 0.0
        self.ask1 = 0.0
        self.amount24 = 0.0
        self.hold_vol = 0.0
        self.funding_rate = 0.0
        self.funding_cycle_h = 8.0
        self.contract_size = contract_size
        self.price_unit = price_unit
        self.price_ring = Ring(PRICE_RING)
        self.oi_ring = Ring(OI_RING)
        self.spread_ring = Ring(SPREAD_RING)
        self.ret_ewma = Ewma(EWMA_HALFLIFE)
        self.first_ts = 0
        self.last_ts = 0
        self.rest_ts = 0
        self.cooldowns: dict[str, int] = {}

    # ---------- производные ----------
    @property
    def mid(self) -> float:
        if self.bid1 > 0 and self.ask1 > 0:
            return 0.5 * (self.bid1 + self.ask1)
        return self.last_price

    @property
    def spread_pct(self) -> float | None:
        if self.bid1 > 0 and self.ask1 > 0 and self.ask1 >= self.bid1:
            return (self.ask1 - self.bid1) / self.mid * 100.0
        return None

    @property
    def oi_usd(self) -> float:
        return self.hold_vol * self.contract_size * self.last_price

    def log_return(self, window_s: float, now_ts: int | None = None) -> float | None:
        """ln(P_now / P_{now-window}); None если истории не хватает."""
        last = self.price_ring.last()
        if last is None:
            return None
        t_now = now_ts if now_ts is not None else last[0]
        p_then = self.price_ring.value_at(t_now - int(window_s * 1000))
        p_now = self.last_price or last[1]
        if not p_then or p_then <= 0 or p_now <= 0:
            return None
        return math.log(p_now / p_then)

    def pct_change(self, window_s: float) -> float | None:
        r = self.log_return(window_s)
        return None if r is None else (math.exp(r) - 1.0) * 100.0

    def oi_change_pct(self, window_s: float) -> float | None:
        last = self.oi_ring.last()
        if last is None or last[1] <= 0:
            return None
        then = self.oi_ring.value_at(last[0] - int(window_s * 1000))
        if not then or then <= 0:
            return None
        return (last[1] / then - 1.0) * 100.0

    def warm_minutes(self) -> float:
        if not self.first_ts or not self.last_ts:
            return 0.0
        return (self.last_ts - self.first_ts) / 60000.0

    def spread_median_before(self, ts: int, minutes: float = 30) -> float | None:
        vals = [v for t, v in self.spread_ring.iter_since(ts - int(minutes * 60000)) if t <= ts]
        return median(vals) if vals else None


class Tier1:
    def __init__(self, universe: Universe):
        self.universe = universe
        self.states: dict[str, SymbolState] = {}
        self.frames = 0
        self.last_frame_ts = 0
        self.sync_with_universe()

    def sync_with_universe(self) -> None:
        for sym, spec in self.universe.specs.items():
            st = self.states.get(sym)
            if st is None:
                self.states[sym] = SymbolState(sym, spec.contract_size, spec.price_unit)
            else:
                st.contract_size = spec.contract_size
                st.price_unit = spec.price_unit
        for sym in [s for s in self.states if s not in self.universe.specs]:
            del self.states[sym]

    def get(self, symbol: str) -> SymbolState | None:
        return self.states.get(symbol)

    # ---------- WS push.tickers (каждые 2 с; первый кадр полный, далее — только изменившиеся) ----------
    def on_tickers_frame(self, ts: int, items: Iterable[dict]) -> None:
        states = self.states
        for it in items:
            st = states.get(it.get("symbol"))
            if st is None:
                continue
            p = it.get("lastPrice")
            if p:
                st.last_price = float(p)
            fp = it.get("fairPrice")
            if fp:
                st.fair_price = float(fp)
            a = it.get("amount24")
            if a is not None:
                st.amount24 = float(a)
            # На случай, если биржа начнёт присылать эти поля в push.tickers:
            if it.get("holdVol") is not None:
                st.hold_vol = float(it["holdVol"])
            if it.get("bid1"):
                st.bid1 = float(it["bid1"])
            if it.get("ask1"):
                st.ask1 = float(it["ask1"])
        self.sample(ts)

    def sample(self, ts: int) -> None:
        """Добавляет точку цены каждому символу (равномерная сетка по 2 с) и обновляет EWMA."""
        if ts <= self.last_frame_ts:
            return
        self.frames += 1
        self.last_frame_ts = ts
        for st in self.states.values():
            p = st.last_price
            if p <= 0:
                continue
            last = st.price_ring.last()
            if last is not None and last[1] > 0:
                st.ret_ewma.update(math.log(p / last[1]))
            st.price_ring.append(ts, p)
            if not st.first_ts:
                st.first_ts = ts
            st.last_ts = ts

    # ---------- REST /contract/ticker (каждые 15 с) ----------
    def on_rest_tickers(self, ts: int, items: Iterable[dict]) -> None:
        for it in items:
            st = self.states.get(it.get("symbol"))
            if st is None:
                continue
            for key, attr in (("bid1", "bid1"), ("ask1", "ask1"), ("amount24", "amount24"),
                              ("holdVol", "hold_vol"), ("fundingRate", "funding_rate")):
                v = it.get(key)
                if v is not None:
                    setattr(st, attr, float(v))
            if not st.last_price and it.get("lastPrice"):
                st.last_price = float(it["lastPrice"])
            t = int(it.get("timestamp") or ts)
            st.rest_ts = t
            if st.hold_vol > 0 and st.last_price > 0:
                st.oi_ring.append(t, st.oi_usd)
            sp = st.spread_pct
            if sp is not None:
                st.spread_ring.append(t, sp)

    # ---------- выборки ----------
    def eligible(self, min_turnover: float) -> list[SymbolState]:
        return [s for s in self.states.values() if s.amount24 >= min_turnover and s.last_price > 0]

    def top_movers(self, min_turnover: float, window_s: float = 900, n: int = 10) -> list[tuple[SymbolState, float]]:
        out = []
        for st in self.eligible(min_turnover):
            ch = st.pct_change(window_s)
            if ch is not None:
                out.append((st, ch))
        out.sort(key=lambda x: abs(x[1]), reverse=True)
        return out[:n]

    def memory_bytes(self) -> int:
        return sum(s.price_ring.nbytes() + s.oi_ring.nbytes() + s.spread_ring.nbytes() for s in self.states.values())
