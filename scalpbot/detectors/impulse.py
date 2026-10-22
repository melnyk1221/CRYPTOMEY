"""A: импульс / резкое падение (tier 1).

σ₂ₛ — EWMA-стандартное отклонение 2-секундных лог-доходностей. Ожидаемое σ за n секунд ≈ σ₂ₛ·√(n/2).
Импульс, если |r60| ≥ max(IMPULSE_MIN_PCT_60S, Z·σ₂ₛ·√30) или |r300| ≥ max(IMPULSE_MIN_PCT_5M, Z·σ₂ₛ·√150).
"""
from __future__ import annotations

import math
from dataclasses import dataclass

from ..bus import IMPULSE, NEUTRAL, DetectorEvent
from ..config import Settings
from ..market.tier1 import SymbolState, Tier1

BTC = "BTC_USDT"
WINDOWS = ((60, "60с", "IMPULSE_MIN_PCT_60S"), (300, "5м", "IMPULSE_MIN_PCT_5M"))


@dataclass(slots=True)
class _Fired:
    ts: int
    abs_r: float


class ImpulseDetector:
    def __init__(self, settings: Settings, tier1: Tier1):
        self.s = settings
        self.tier1 = tier1
        self._fired: dict[tuple[str, int], _Fired] = {}
        self.pretriggered: set[str] = set()

    def thresholds(self, st: SymbolState) -> dict[int, float]:
        """Порог в единицах лог-доходности для каждого окна."""
        sigma = st.ret_ewma.std
        out = {}
        for win, _, key in WINDOWS:
            floor = getattr(self.s, key) / 100.0
            out[win] = max(floor, self.s.IMPULSE_Z * sigma * math.sqrt(win / 2.0))
        return out

    def evaluate(self, now_ts: int, symbols: list[SymbolState] | None = None) -> list[DetectorEvent]:
        s = self.s
        events: list[DetectorEvent] = []
        self.pretriggered = set()
        btc = self.tier1.get(BTC)
        btc_r = {w: (btc.log_return(w, now_ts) if btc else None) for w, _, _ in WINDOWS}
        warm_min = s.IMPULSE_WARMUP_MIN
        rearm_ms = int(s.IMPULSE_REARM_MIN * 60000)
        pool = symbols if symbols is not None else self.tier1.eligible(s.MIN_TURNOVER_24H_USD)
        for st in pool:
            if st.warm_minutes() < warm_min:
                continue
            thr = self.thresholds(st)
            best = None  # (ratio, win, label, r, thr)
            for win, label, _ in WINDOWS:
                r = st.log_return(win, now_ts)
                if r is None:
                    continue
                ratio = abs(r) / thr[win]
                if ratio >= 0.6:
                    self.pretriggered.add(st.symbol)
                if ratio >= 1.0 and (best is None or ratio > best[0]):
                    best = (ratio, win, label, r, thr[win])
            if best is None:
                continue
            ratio, win, label, r, t = best
            sign = 1 if r > 0 else -1
            key = (st.symbol, sign)
            prev = self._fired.get(key)
            if prev and now_ts - prev.ts < rearm_ms and abs(r) < 2 * prev.abs_r:
                continue
            self._fired[key] = _Fired(now_ts, abs(r))
            events.append(self._make_event(st, now_ts, win, label, r, t, btc_r.get(win)))
        if len(self._fired) > 5000:
            cutoff = now_ts - rearm_ms
            self._fired = {k: v for k, v in self._fired.items() if v.ts >= cutoff}
        return events

    def _make_event(self, st: SymbolState, now_ts: int, win: int, label: str, r: float, thr: float,
                    btc_r: float | None) -> DetectorEvent:
        sigma = st.ret_ewma.std
        z = r / (sigma * math.sqrt(win / 2.0)) if sigma > 0 else None
        strength = min(1.0, abs(r) / (2.0 * thr))
        beta = False
        if st.symbol != BTC and btc_r is not None and btc_r * r > 0 and abs(btc_r) >= 0.5 * abs(r):
            beta = True
            strength *= 0.5
        move_pct = (math.exp(r) - 1.0) * 100.0
        details = {
            "tag": "pump" if r > 0 else "dump",
            "move_sign": 1 if r > 0 else -1,
            "window": label,
            "window_s": win,
            "move_pct": round(move_pct, 3),
            "threshold_pct": round(thr * 100.0, 3),
            "z": round(z, 2) if z is not None else None,
            "btc_move_pct": round((math.exp(btc_r) - 1) * 100.0, 3) if btc_r is not None else None,
            "beta_btc": beta,
            "oi_change_15m_pct": _r(st.oi_change_pct(900)),
            "funding_pct": round(st.funding_rate * 100.0, 4),
            "funding_cycle_h": st.funding_cycle_h,
            "amount24": st.amount24,
            "price": st.last_price,
            "change_15m_pct": _r(st.pct_change(900)),
        }
        return DetectorEvent(st.symbol, IMPULSE, NEUTRAL, round(strength, 3), now_ts, details)


def _r(x: float | None, nd: int = 2) -> float | None:
    return None if x is None else round(x, nd)
