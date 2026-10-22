"""A: импульс / резкое падение (tier 1).

σ₂ₛ — EWMA-стандартное отклонение 2-секундных лог-доходностей. Ожидаемое σ за n секунд ≈ σ₂ₛ·√(n/2).
Импульс, если |r60| ≥ max(IMPULSE_MIN_PCT_60S, Z·σ₂ₛ·√30) или |r300| ≥ max(IMPULSE_MIN_PCT_5M, Z·σ₂ₛ·√150).
"""
from __future__ import annotations

import math
from dataclasses import dataclass

from ..bus import EXHAUSTION, IMPULSE, LONG, NEUTRAL, SHORT, DetectorEvent
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


# ======================================================================
# A2. Затухание после импульса (tier 2)
# ======================================================================
ARM_MS = 30 * 60_000
MIN_DELAY_MS = 2 * 60_000
MIN_DELTA_TOTAL_USD = 1_000.0


class _Arm:
    __slots__ = ("ts", "sign", "move_pct", "start_price", "extreme", "peak_r60", "oi_start", "spread_med",
                 "fired", "tag")

    def __init__(self, ts: int, sign: int, move_pct: float, start_price: float, peak_r60: float,
                 oi_start: float | None, spread_med: float | None):
        self.ts = ts
        self.sign = sign
        self.move_pct = move_pct
        self.start_price = start_price
        self.extreme = start_price * math.exp(math.log1p(move_pct / 100.0))
        self.peak_r60 = peak_r60
        self.oi_start = oi_start
        self.spread_med = spread_med
        self.fired = False
        self.tag = "pump" if sign > 0 else "dump"


class ExhaustionDetector:
    """Взводится на 30 мин после каждого события IMPULSE; срабатывает один раз на импульс."""

    def __init__(self, settings: Settings):
        self.s = settings
        self.arms: dict[str, _Arm] = {}

    def arm(self, ev: DetectorEvent, st: SymbolState) -> None:
        d = ev.details
        sign = int(d.get("move_sign", 1))
        win = int(d.get("window_s", 60))
        start_price = st.price_ring.value_at(ev.ts - win * 1000) or st.last_price
        r60 = st.log_return(60, ev.ts)
        prev = self.arms.get(ev.symbol)
        if prev and not prev.fired and prev.sign == sign and ev.ts - prev.ts < ARM_MS:
            # Продолжение того же импульса: обновляем размер, но не перевзводим.
            prev.move_pct = (st.last_price / prev.start_price - 1) * 100.0
            return
        self.arms[ev.symbol] = _Arm(ev.ts, sign, float(d.get("move_pct", 0.0)), start_price,
                                    abs(r60) if r60 is not None else 0.0,
                                    st.oi_ring.value_at(ev.ts - win * 1000),
                                    st.spread_median_before(ev.ts, 30))

    def evaluate(self, st: SymbolState, now: int, delta60: tuple[float, float],
                 spread_now: float | None, liq_usd: float | None = None) -> list[DetectorEvent]:
        a = self.arms.get(st.symbol)
        if a is None:
            return []
        if a.fired or now - a.ts > ARM_MS:
            if now - a.ts > ARM_MS:
                del self.arms[st.symbol]
            return []
        p = st.last_price
        if p <= 0:
            return []
        # Экстремум импульса и пик скорости
        if (a.sign < 0 and p < a.extreme) or (a.sign > 0 and p > a.extreme):
            a.extreme = p
        r60 = st.log_return(60, now)
        if r60 is None:
            return []
        if r60 * a.sign > 0:
            a.peak_r60 = max(a.peak_r60, abs(r60))
        if now - a.ts < MIN_DELAY_MS:
            return []
        if abs(r60) >= 0.25 * a.peak_r60:
            return []
        buy, sell = delta60
        total = buy + sell
        if total < MIN_DELTA_TOTAL_USD:
            return []
        opposite = buy if a.sign < 0 else sell
        share = opposite / total
        if share < 0.55:
            return []
        if spread_now is None:
            return []
        if a.spread_med is not None and a.spread_med > 0:
            if spread_now > 2.0 * a.spread_med:
                return []
        elif self.s.MAX_SPREAD_PCT > 0 and spread_now > self.s.MAX_SPREAD_PCT:
            return []
        a.fired = True
        move_total = a.extreme - a.start_price
        retrace = (p - a.extreme) / -move_total * 100.0 if move_total else 0.0
        oi_now = st.oi_ring.last()
        oi_ch = ((oi_now[1] / a.oi_start - 1) * 100.0) if (oi_now and a.oi_start) else None
        direction = LONG if a.sign < 0 else SHORT
        strength = min(1.0, 0.4 + 0.6 * (share - 0.5) / 0.3)
        details = {
            "impulse_tag": a.tag,
            "impulse_move_pct": round((a.extreme / a.start_price - 1) * 100.0, 2),
            "minutes_since": round((now - a.ts) / 60000.0, 1),
            "retrace_pct": round(max(0.0, retrace), 1),
            "r60_pct": round((math.exp(r60) - 1) * 100.0, 3),
            "peak_r60_pct": round((math.exp(a.peak_r60) - 1) * 100.0 * a.sign, 3),
            "delta_share_pct": round(share * 100.0, 1),
            "delta_total_usd": round(total, 2),
            "spread_pct": round(spread_now, 4),
            "spread_median_before_pct": round(a.spread_med, 4) if a.spread_med is not None else None,
            "oi_change_during_pct": round(oi_ch, 2) if oi_ch is not None else None,
            "liq_usd": round(liq_usd, 2) if liq_usd else None,
            "price": p,
        }
        return [DetectorEvent(st.symbol, EXHAUSTION, direction, round(strength, 3), now, details)]

    def forget(self, symbol: str) -> None:
        self.arms.pop(symbol, None)
