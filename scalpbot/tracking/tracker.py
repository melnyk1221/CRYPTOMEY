"""Форвард-доходности и MFE/MAE для каждого события и алерта.

r_h   — сырая доходность к горизонту, % (без знака направления);
mfe_h — максимальное благоприятное отклонение до горизонта, % (со знаком направления, ≥ 0 обычно);
mae_h — максимальное неблагоприятное отклонение, % (обычно ≤ 0).
Для нейтральных событий: mfe = макс. рост, mae = макс. падение (сырые).
"""
from __future__ import annotations

import logging
from typing import Iterable

from ..bus import LONG, SHORT
from ..storage.db import HORIZONS_MIN, Database

log = logging.getLogger("tracker")

MAX_OPEN = 5000


class OpenTracker:
    __slots__ = ("event_id", "symbol", "direction", "ts0", "ref", "run_max", "run_min", "next_idx",
                 "missed")

    def __init__(self, event_id: int, symbol: str, direction: str, ts0: int, ref: float):
        self.event_id = event_id
        self.symbol = symbol
        self.direction = direction
        self.ts0 = ts0
        self.ref = ref
        self.run_max = ref
        self.run_min = ref
        self.next_idx = 0
        self.missed = 0


def excursions(direction: str, ref: float, run_max: float, run_min: float) -> tuple[float, float]:
    up = (run_max / ref - 1.0) * 100.0
    down = (run_min / ref - 1.0) * 100.0
    if direction == SHORT:
        return -down, -up
    return up, down  # long и neutral


class Tracker:
    def __init__(self, db: Database):
        self.db = db
        self.by_symbol: dict[str, list[OpenTracker]] = {}
        self.count = 0
        self.completed = 0
        self.dropped = 0

    def add(self, event_id: int, symbol: str, direction: str, ts0: int, ref_price: float,
            move_sign: int = 0, persist: bool = True) -> OpenTracker | None:
        if ref_price <= 0:
            return None
        if self.count >= MAX_OPEN:
            self._evict()
        t = OpenTracker(event_id, symbol, direction, ts0, ref_price)
        self.by_symbol.setdefault(symbol, []).append(t)
        self.count += 1
        if persist:
            self.db.add_outcome(event_id=event_id, symbol=symbol, direction=direction, ts0=ts0,
                                ref_price=ref_price, move_sign=move_sign)
        return t

    def _evict(self) -> None:
        """Сначала выбрасываем самые старые нейтральные."""
        everything = [t for lst in self.by_symbol.values() for t in lst]
        neutral = sorted((t for t in everything if t.direction not in (LONG, SHORT)), key=lambda t: t.ts0)
        victim = neutral[0] if neutral else min(everything, key=lambda t: t.ts0)
        self.by_symbol[victim.symbol].remove(victim)
        self.count -= 1
        self.dropped += 1
        self.db.update_outcome(victim.event_id, done=1, missed=victim.missed | _mask_from(victim.next_idx))

    def on_price(self, symbol: str, ts: int, price: float) -> None:
        lst = self.by_symbol.get(symbol)
        if not lst or price <= 0:
            return
        finished = False
        for t in lst:
            if price > t.run_max:
                t.run_max = price
            if price < t.run_min:
                t.run_min = price
            while t.next_idx < len(HORIZONS_MIN) and ts >= t.ts0 + HORIZONS_MIN[t.next_idx] * 60000:
                h = HORIZONS_MIN[t.next_idx]
                r = (price / t.ref - 1.0) * 100.0
                mfe, mae = excursions(t.direction, t.ref, t.run_max, t.run_min)
                cols = {f"r{h}": round(r, 4), f"mfe{h}": round(mfe, 4), f"mae{h}": round(mae, 4),
                        "run_max": t.run_max, "run_min": t.run_min}
                t.next_idx += 1
                if t.next_idx >= len(HORIZONS_MIN):
                    cols["done"] = 1
                    finished = True
                self.db.update_outcome(t.event_id, **cols)
        if finished:
            keep = [t for t in lst if t.next_idx < len(HORIZONS_MIN)]
            n_done = len(lst) - len(keep)
            self.count -= n_done
            self.completed += n_done
            if keep:
                self.by_symbol[symbol] = keep
            else:
                del self.by_symbol[symbol]

    def on_prices(self, ts: int, prices: Iterable[tuple[str, float]]) -> None:
        for sym, p in prices:
            self.on_price(sym, ts, p)

    def restore(self, rows: Iterable, now: int) -> int:
        """Восстанавливает открытые трекеры после рестарта. Пропущенные горизонты помечаются missed."""
        n = 0
        for row in rows:
            t = OpenTracker(int(row["event_id"]), row["symbol"], row["direction"], int(row["ts0"]),
                            float(row["ref_price"]))
            t.run_max = float(row["run_max"] or t.ref)
            t.run_min = float(row["run_min"] or t.ref)
            missed = int(row["missed"] or 0)
            idx = 0
            for i, h in enumerate(HORIZONS_MIN):
                if row[f"r{h}"] is not None:
                    idx = i + 1
                    continue
                if now >= t.ts0 + h * 60000:
                    missed |= 1 << i
                    idx = i + 1
                else:
                    break
            t.next_idx = idx
            t.missed = missed
            if idx >= len(HORIZONS_MIN):
                self.db.update_outcome(t.event_id, done=1, missed=missed)
                continue
            if missed != int(row["missed"] or 0):
                self.db.update_outcome(t.event_id, missed=missed)
            self.by_symbol.setdefault(t.symbol, []).append(t)
            self.count += 1
            n += 1
        return n


def _mask_from(idx: int) -> int:
    m = 0
    for i in range(idx, len(HORIZONS_MIN)):
        m |= 1 << i
    return m
