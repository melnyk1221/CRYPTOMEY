"""C: робот — периодические тейкер-ордера одинакового размера (паттерн «$5k каждую 1.0 с»).

Алгоритм (O(n) на корзину, раз в секунду на символ):
1. Последние ROBOT_WINDOW ордеров каждой стороны, без ордеров < $200.
2. Кластеризация по log-размеру с шагом 4% (+ соседние корзины).
3. Внутри кластера — только ордера в пределах ±ROBOT_SIZE_TOL от медианы кластера.
4. Самая длинная «свежая» серия (заканчивается последним ордером, он не старше 30 с), у которой
   CV интервалов ≤ ROBOT_CV_MAX и медиана интервала 0.3…30 с.
"""
from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Iterable, Sequence

from ..bus import LONG, ROBOT, SHORT, DetectorEvent
from ..config import Settings
from ..market.stats import median
from ..market.trades import TakerOrder

MIN_ORDER_USD = 200.0
BUCKET_STEP = math.log(1.04)
RECENT_MS = 30_000


@dataclass(slots=True)
class RobotRun:
    side_buy: bool
    count: int
    size_usd: float
    interval_s: float
    cv: float
    total_usd: float
    start_ts: int
    end_ts: int

    @property
    def duration_s(self) -> float:
        return (self.end_ts - self.start_ts) / 1000.0


def _best_suffix(orders: Sequence[TakerOrder], cv_max: float) -> tuple[int, float, float] | None:
    """Длина самой длинной серии-суффикса с CV интервалов ≤ cv_max: (длина, медиана, cv)."""
    n = len(orders)
    if n < 3:
        return None
    best = None
    s = ss = 0.0
    intervals: list[float] = []
    for k in range(n - 1, 0, -1):  # добавляем интервалы с конца
        dt = (orders[k].ts - orders[k - 1].ts) / 1000.0
        intervals.append(dt)
        s += dt
        ss += dt * dt
        m = len(intervals)
        if m < 2:
            continue
        mu = s / m
        if mu <= 0:
            continue
        var = max(0.0, ss / m - mu * mu)
        cv = math.sqrt(var) / mu
        if cv <= cv_max:
            best = (m + 1, median(intervals), cv)
    return best


def find_run(orders: Iterable[TakerOrder], side_buy: bool, now_ts: int, s: Settings) -> RobotRun | None:
    side = [o for o in orders if o.side_buy == side_buy and o.notional_usd >= MIN_ORDER_USD]
    side = side[-s.ROBOT_WINDOW:]
    if len(side) < s.ROBOT_MIN_COUNT:
        return None
    buckets: dict[int, list[int]] = {}
    for i, o in enumerate(side):
        buckets.setdefault(int(math.floor(math.log(o.notional_usd) / BUCKET_STEP)), []).append(i)
    best: RobotRun | None = None
    checked: set[int] = set()
    for b in sorted(buckets, key=lambda k: -len(buckets[k])):
        if b in checked:
            continue
        idx = sorted(set(buckets.get(b - 1, [])) | set(buckets[b]) | set(buckets.get(b + 1, [])))
        checked.add(b)
        if len(idx) < s.ROBOT_MIN_COUNT:
            continue
        med = median(side[i].notional_usd for i in idx)
        cl = [side[i] for i in idx if abs(side[i].notional_usd / med - 1.0) <= s.ROBOT_SIZE_TOL]
        if len(cl) < s.ROBOT_MIN_COUNT or now_ts - cl[-1].ts > RECENT_MS:
            continue
        res = _best_suffix(cl, s.ROBOT_CV_MAX)
        if res is None:
            continue
        length, med_int, cv = res
        if length < s.ROBOT_MIN_COUNT or not (0.3 <= med_int <= 30.0):
            continue
        run = cl[-length:]
        total = sum(o.notional_usd for o in run)
        cand = RobotRun(side_buy, length, median(o.notional_usd for o in run), med_int, cv, total,
                        run[0].ts, run[-1].ts)
        if best is None or cand.total_usd > best.total_usd:
            best = cand
    return best


class RobotDetector:
    """Состояние на символ: что уже отправлено (повтор — только если серия продолжилась и удвоилась)."""

    def __init__(self, settings: Settings):
        self.s = settings
        self.emitted: dict[bool, RobotRun] = {}

    def evaluate(self, symbol: str, orders: Sequence[TakerOrder], now_ts: int, amount24: float) -> list[DetectorEvent]:
        out = []
        min_total = max(self.s.ROBOT_MIN_TOTAL_USD, 0.002 * amount24)
        for side_buy in (True, False):
            run = find_run(orders, side_buy, now_ts, self.s)
            prev = self.emitted.get(side_buy)
            if prev and now_ts - prev.end_ts > max(60_000, 5 * prev.interval_s * 1000):
                prev = None
                self.emitted.pop(side_buy, None)
            if run is None or run.total_usd < min_total:
                continue
            same_run = prev is not None and abs(run.size_usd / prev.size_usd - 1) <= 2 * self.s.ROBOT_SIZE_TOL \
                and run.start_ts <= prev.end_ts
            if same_run:
                prev.end_ts = max(prev.end_ts, run.end_ts)
                if run.count < 2 * prev.count:
                    continue
            self.emitted[side_buy] = run
            reg = max(0.0, 1.0 - run.cv / (2 * self.s.ROBOT_CV_MAX))
            strength = min(1.0, run.total_usd / (3 * min_total)) * (0.5 + 0.5 * reg)
            out.append(DetectorEvent(symbol, ROBOT, LONG if side_buy else SHORT, round(strength, 3), now_ts, {
                "side": "buy" if side_buy else "sell",
                "size_usd": round(run.size_usd, 2),
                "interval_s": round(run.interval_s, 3),
                "count": run.count,
                "total_usd": round(run.total_usd, 2),
                "duration_s": round(run.duration_s, 1),
                "cv": round(run.cv, 3),
                "repeat": same_run,
            }))
        return out
