"""E: «толкание» на споте — крупная бид-стенка переставляется всё выше (REST-снимки каждые 2 с).

Стенка — самый крупный уровень бида в пределах 3% от середины, объём ≥ max($5k, 3 × медиана топ-20).
Шаг вверх: стенка исчезла на P1 и в течение ≤ 2 снимков похожая (±30%) появилась на P2 > P1, а лучший бид
спота вырос. PUSH (long) — ≥ PUSH_MIN_STEPS шагов за 10 мин и рост середины спота ≥ 1%.
Зеркально для аска: стенка переставляется ниже → short.
"""
from __future__ import annotations

from collections import deque

from ..bus import LONG, PUSH, SHORT, DetectorEvent
from ..config import Settings
from ..market.stats import Ring, median
from ..sources.mexc_spot_rest import SpotSnapshot

WALL_DIST_PCT = 3.0
WALL_MIN_USD = 5_000.0
WINDOW_MS = 10 * 60_000
MIN_MOVE_PCT = 1.0


class _SideState:
    __slots__ = ("wall", "pending", "steps")

    def __init__(self) -> None:
        self.wall: tuple[float, float, float] | None = None       # (цена, $, лучшая цена своей стороны)
        self.pending: tuple[float, float, float, int] | None = None  # исчезнувшая стенка + номер снимка
        self.steps: deque[tuple[int, float]] = deque(maxlen=50)    # (ts, mid спота на момент шага)


def find_wall(snap: SpotSnapshot, is_bid: bool) -> tuple[float, float] | None:
    mid = snap.mid
    if mid <= 0:
        return None
    levels = snap.levels(is_bid)
    if not levels:
        return None
    usd = [p * q for p, q in levels[:20]]
    thr = max(WALL_MIN_USD, 3.0 * median(usd))
    best = None
    for p, q in levels:
        if abs(p / mid - 1) * 100 > WALL_DIST_PCT:
            continue
        u = p * q
        if u >= thr and (best is None or u > best[1]):
            best = (p, u)
    return best


class PushDetector:
    def __init__(self, settings: Settings):
        self.s = settings
        self.sides = {True: _SideState(), False: _SideState()}
        self.mids = Ring(300)
        self.n = 0
        self.emitted_at = {True: 0, False: 0}

    def on_snapshot(self, symbol: str, snap: SpotSnapshot, fut_ring: Ring | None = None) -> list[DetectorEvent]:
        self.n += 1
        mid = snap.mid
        if mid <= 0:
            return []
        self.mids.append(snap.ts, mid)
        out = []
        for is_bid in (True, False):
            st = self.sides[is_bid]
            best_own = snap.best_bid if is_bid else snap.best_ask
            wall = find_wall(snap, is_bid)
            # Предыдущая стенка исчезла?
            if st.wall is not None and st.pending is None:
                p1, u1, b1 = st.wall
                if snap.usd_at(is_bid, p1) < 0.3 * u1:
                    st.pending = (p1, u1, b1, self.n)
            if st.pending is not None:
                p1, u1, b1, k = st.pending
                if wall and abs(wall[1] / u1 - 1) <= 0.3 and \
                        ((is_bid and wall[0] > p1 and best_own > b1) or (not is_bid and wall[0] < p1 and best_own < b1)):
                    st.steps.append((snap.ts, mid))
                    st.pending = None
                elif self.n - k >= 2:
                    st.pending = None
            st.wall = (wall[0], wall[1], best_own) if wall else None
            # Окно 10 минут
            while st.steps and snap.ts - st.steps[0][0] > WINDOW_MS:
                st.steps.popleft()
            if len(st.steps) >= self.s.PUSH_MIN_STEPS and snap.ts - self.emitted_at[is_bid] > WINDOW_MS:
                first_mid = self.mids.value_at(st.steps[0][0] - 2000) or st.steps[0][1]
                move = (mid / first_mid - 1) * 100.0
                if (is_bid and move >= MIN_MOVE_PCT) or (not is_bid and move <= -MIN_MOVE_PCT):
                    self.emitted_at[is_bid] = snap.ts
                    steps = len(st.steps)
                    minutes = (st.steps[-1][0] - st.steps[0][0]) / 60000.0
                    st.steps.clear()
                    strength = min(1.0, 0.4 + 0.1 * steps + 0.1 * abs(move))
                    out.append(DetectorEvent(symbol, PUSH, LONG if is_bid else SHORT, round(strength, 3), snap.ts, {
                        "side": "bid" if is_bid else "ask", "steps": steps, "minutes": round(max(minutes, 0.1), 1),
                        "spot_move_pct": round(move, 2), "spot_mid": mid,
                        "spot_leads": self.spot_leads(fut_ring) if fut_ring is not None else None,
                    }))
        return out

    def spot_leads(self, fut: Ring, minutes: float = 5, lag_ms: int = 2000, thr_pct: float = 0.2) -> dict:
        """Как часто спот двигается первым: шаг спота ≥ 0.2%, фьючерс догоняет в ту же сторону через ~2 с."""
        last = self.mids.last()
        if last is None:
            return {"spot_first": 0, "spot_moves": 0}
        pts = list(self.mids.iter_since(last[0] - int(minutes * 60000)))
        moves = first = 0
        for (t0, m0), (t1, m1) in zip(pts, pts[1:]):
            ch = (m1 / m0 - 1) * 100.0
            if abs(ch) < thr_pct:
                continue
            moves += 1
            f0 = fut.value_at(t0)
            f1 = fut.value_at(t1)
            f2 = fut.value_at(t1 + lag_ms)
            if not (f0 and f1 and f2):
                continue
            same_now = (f1 / f0 - 1) * 100.0
            later = (f2 / f1 - 1) * 100.0
            if abs(same_now) < abs(ch) / 2 and later * ch > 0:
                first += 1
        return {"spot_first": first, "spot_moves": moves}
