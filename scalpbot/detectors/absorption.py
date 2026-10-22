"""D: поглощение — подтверждённая плотность «съедена», а не снята.

Если сделки по уровню ≥ ABSORB_FRAC × пик за 10 мин — следим 30 с:
- цена не ушла за уровень дальше 0.3% → «удержание»: бид → long (продавцы выдохлись о покупателя),
  аск → short (покупатели выдохлись о продавца);
- цена пробила уровень дальше 0.5% → broken=true, направление пробоя (бид пробит → short, аск → long).
"""
from __future__ import annotations

from ..bus import ABSORPTION, LONG, SHORT, DetectorEvent
from ..config import Settings
from .density import DensityDetector, DensityTrack

WATCH_MS = 30_000
CONSUME_WINDOW_MS = 10 * 60_000
HOLD_PCT = 0.3
BREAK_PCT = 0.5


class AbsorptionDetector:
    def __init__(self, settings: Settings):
        self.s = settings

    @staticmethod
    def _beyond_pct(tr: DensityTrack, px: float) -> float:
        """Насколько цена ушла ЗА уровень, % (>0 — за уровнем)."""
        if tr.is_bid:
            return (tr.price - px) / tr.price * 100.0
        return (px - tr.price) / tr.price * 100.0

    def evaluate(self, symbol: str, density: DensityDetector, px: float, now: int) -> list[DetectorEvent]:
        if px <= 0:
            return []
        out = []
        for tr in list(density.tracks.values()):
            if not tr.confirmed or tr.absorb_state == "done" or tr.spoof_flagged:
                continue
            if tr.absorb_state == "none":
                if now - tr.first_seen <= CONSUME_WINDOW_MS and tr.traded_usd >= self.s.ABSORB_FRAC * tr.peak_usd:
                    tr.absorb_state = "watch"
                    tr.consumed_ts = now
                    tr.absorb_extreme = self._beyond_pct(tr, px)
                continue
            # watch
            beyond = self._beyond_pct(tr, px)
            tr.absorb_extreme = max(tr.absorb_extreme, beyond)
            assert tr.consumed_ts is not None
            if tr.absorb_extreme > BREAK_PCT:
                tr.absorb_state = "done"
                direction = SHORT if tr.is_bid else LONG
                note = "бид-плотность съедена и пробита вниз" if tr.is_bid else "аск-плотность съедена, пробой вверх"
                out.append(self._event(symbol, tr, now, direction, True, note))
            elif now - tr.consumed_ts >= WATCH_MS:
                tr.absorb_state = "done"
                if tr.absorb_extreme <= HOLD_PCT:
                    direction = LONG if tr.is_bid else SHORT
                    note = ("продажи съели бид, цена удержалась — продавцы выдохлись" if tr.is_bid
                            else "покупки съели аск, цена не прошла выше — покупатели выдохлись")
                    out.append(self._event(symbol, tr, now, direction, False, note))
        return out

    def _event(self, symbol: str, tr: DensityTrack, now: int, direction: str, broken: bool, note: str) -> DetectorEvent:
        strength = min(1.0, 0.5 + 0.5 * min(1.0, tr.traded_share)) * (0.8 if broken else 1.0)
        return DetectorEvent(symbol, ABSORPTION, direction, round(strength, 3), now, {
            "side": tr.side, "price": tr.price, "peak_usd": round(tr.peak_usd, 2),
            "traded_usd": round(tr.traded_usd, 2), "traded_share_pct": round(tr.traded_share * 100, 1),
            "broken": broken, "beyond_max_pct": round(tr.absorb_extreme, 3), "note": note,
            "lifetime_s": round(tr.lifetime_s(now), 1),
        })
