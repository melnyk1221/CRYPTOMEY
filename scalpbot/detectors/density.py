"""B: плотность (крупная лимитная заявка) с защитой от спуфинга, по стакану фьючерсов.

Раз в секунду: медиана $-объёма топ-20 уровней каждой стороны; кандидат — уровень, где
объём ≥ max(DENSITY_K × медиана, DENSITY_MIN_USD, DENSITY_TURNOVER_FRAC × оборот24ч) и расстояние
от середины ≤ DENSITY_MAX_DIST_PCT. Уменьшение уровня делится на «съедено» (сделки тейкеров ровно по
этой цене: продажи бьют в бид, покупки — в аск) и «снято» (остаток).
"""
from __future__ import annotations

from ..bus import DENSITY, LONG, NEUTRAL, SHORT, DetectorEvent
from ..config import Settings
from ..market.orderbook import OrderBook
from ..market.stats import median
from ..market.trades import TradeAggregator

MAX_TRACKS = 40
RELOC_WINDOW_MS = 5_000
SPOOF_APPROACH_PCT = 0.3
SPOOF_MIN_LIFETIME_S = 15
# Ограничение частоты событий (иначе лестницы маркет-мейкеров раздувают БД):
CONFIRM_GAP_MS = 120_000   # подтверждённая плотность: ≤ 1 событие на сторону за 2 мин (если не крупнее в 1.5×)
SPOOF_GAP_MS = 300_000     # спуф: ≤ 1 событие на символ за 5 мин


class DensityTrack:
    __slots__ = ("side", "price", "first_seen", "peak_usd", "cur_usd", "traded_usd", "pulled_usd",
                 "relocations", "reloc_notes", "confirmed", "spoof_flagged", "dist_pct", "last_check_ts",
                 "vanished_ts", "gone", "consumed_ts", "absorb_state", "absorb_extreme", "threshold_usd")

    def __init__(self, side: str, price: float, ts: int, usd: float, dist_pct: float, threshold: float):
        self.side = side
        self.price = price
        self.first_seen = ts
        self.peak_usd = usd
        self.cur_usd = usd
        self.traded_usd = 0.0
        self.pulled_usd = 0.0
        self.relocations = 0
        self.reloc_notes: list[str] = []
        self.confirmed = False
        self.spoof_flagged = False
        self.dist_pct = dist_pct
        self.last_check_ts = ts
        self.vanished_ts: int | None = None
        self.gone = False                 # уровень исчез (съеден/снят)
        self.consumed_ts: int | None = None
        self.absorb_state = "none"        # none | watch | done
        self.absorb_extreme = 0.0
        self.threshold_usd = threshold

    @property
    def is_bid(self) -> bool:
        return self.side == "bid"

    @property
    def pulled_share(self) -> float:
        return self.pulled_usd / self.peak_usd if self.peak_usd > 0 else 0.0

    @property
    def traded_share(self) -> float:
        return self.traded_usd / self.peak_usd if self.peak_usd > 0 else 0.0

    def lifetime_s(self, now: int) -> float:
        return (now - self.first_seen) / 1000.0


class DensityDetector:
    def __init__(self, settings: Settings):
        self.s = settings
        self.tracks: dict[tuple[str, float], DensityTrack] = {}
        self._last_confirm: dict[str, tuple[int, float]] = {}
        self._last_spoof = 0
        self.suppressed = 0

    def thresholds(self, book: OrderBook, amount24: float) -> dict[str, float]:
        out = {}
        for side, is_bid in (("bid", True), ("ask", False)):
            top = book.top(is_bid, 20)
            med = median([book.usd(p, v) for p, v in top]) if top else 0.0
            if med != med:
                med = 0.0
            out[side] = max(self.s.DENSITY_K * med, self.s.DENSITY_MIN_USD, self.s.DENSITY_TURNOVER_FRAC * amount24)
        return out

    def evaluate(self, symbol: str, book: OrderBook, trades: TradeAggregator, now: int,
                 amount24: float) -> list[DetectorEvent]:
        mid = book.mid
        if mid <= 0 or not book.synced:
            return []
        s = self.s
        events: list[DetectorEvent] = []
        thr = self.thresholds(book, amount24)
        # Кандидаты
        cands: dict[tuple[str, float], tuple[float, float]] = {}
        for side, is_bid in (("bid", True), ("ask", False)):
            for p, v in book.levels_within(is_bid, s.DENSITY_MAX_DIST_PCT):
                usd = book.usd(p, v)
                if usd >= thr[side]:
                    cands[(side, p)] = (usd, abs(p / mid - 1) * 100.0)

        # Обновление существующих
        for key, tr in list(self.tracks.items()):
            if tr.gone:
                if tr.vanished_ts is not None and now - tr.vanished_ts > RELOC_WINDOW_MS:
                    if tr.absorb_state != "watch":
                        del self.tracks[key]
                continue
            cur = book.usd(tr.price, book.level(tr.is_bid, tr.price))
            if cur < tr.cur_usd:
                dec = tr.cur_usd - cur
                traded = trades.traded_at(tr.price, side_buy=not tr.is_bid, since_ts=tr.last_check_ts, until_ts=now)
                att = min(dec, traded)
                tr.traded_usd += att
                tr.pulled_usd += dec - att
            elif cur > tr.peak_usd:
                tr.peak_usd = cur
            tr.cur_usd = cur
            tr.last_check_ts = now
            tr.dist_pct = abs(tr.price / mid - 1) * 100.0
            if cur < 0.25 * tr.peak_usd:
                tr.gone = True
                tr.vanished_ts = now
                if tr.traded_share < s.ABSORB_FRAC and tr.pulled_share >= 0.7 and tr.dist_pct <= SPOOF_APPROACH_PCT \
                        and tr.lifetime_s(now) >= SPOOF_MIN_LIFETIME_S and not tr.spoof_flagged:
                    tr.spoof_flagged = True
                    if now - self._last_spoof >= SPOOF_GAP_MS:
                        self._last_spoof = now
                        events.append(self._event(symbol, tr, now, NEUTRAL, 0.0, spoof=True))
                    else:
                        self.suppressed += 1

        # Перестановки (relocation): исчезла «снятием» и через ≤5 с похожая появилась рядом.
        tracked_prices = {(t.side, t.price) for t in self.tracks.values() if not t.gone}
        for key, tr in list(self.tracks.items()):
            if not tr.gone or tr.vanished_ts is None or tr.pulled_share < 0.5 or tr.absorb_state == "watch":
                continue
            for (side, p), (usd, dist) in cands.items():
                if side != tr.side or (side, p) in tracked_prices or p == tr.price:
                    continue
                if abs(p / tr.price - 1) <= 0.01 and abs(usd / tr.peak_usd - 1) <= 0.25:
                    toward = (p > tr.price) if tr.is_bid else (p < tr.price)
                    del self.tracks[key]
                    tr.reloc_notes.append("ближе к цене" if toward else "дальше от цены")
                    tr.price = p
                    tr.relocations += 1
                    tr.gone = False
                    tr.vanished_ts = None
                    tr.cur_usd = usd
                    tr.dist_pct = dist
                    tr.last_check_ts = now
                    self.tracks[(side, p)] = tr
                    tracked_prices.add((side, p))
                    break

        # Новые
        for (side, p), (usd, dist) in cands.items():
            if (side, p) in self.tracks:
                continue
            if len(self.tracks) >= MAX_TRACKS:
                weakest = min((t for t in self.tracks.values() if not t.confirmed), key=lambda t: t.peak_usd, default=None)
                if weakest is None or weakest.peak_usd >= usd:
                    continue
                del self.tracks[(weakest.side, weakest.price)]
            self.tracks[(side, p)] = DensityTrack(side, p, now, usd, dist, thr[side])

        # Подтверждение
        for tr in self.tracks.values():
            if tr.confirmed or tr.gone:
                continue
            if tr.lifetime_s(now) >= s.DENSITY_MIN_LIFETIME_S and tr.relocations == 0 and tr.pulled_share < 0.3:
                tr.confirmed = True
                last = self._last_confirm.get(tr.side)
                if last and now - last[0] < CONFIRM_GAP_MS and tr.cur_usd < 1.5 * last[1]:
                    self.suppressed += 1
                    continue
                self._last_confirm[tr.side] = (now, tr.cur_usd)
                s1 = min(1.0, tr.cur_usd / (3.0 * tr.threshold_usd))
                s2 = min(1.0, tr.lifetime_s(now) / (5.0 * s.DENSITY_MIN_LIFETIME_S))
                strength = max(0.3, 0.6 * s1 + 0.4 * s2)
                events.append(self._event(symbol, tr, now, LONG if tr.is_bid else SHORT, strength))
        return events

    def _event(self, symbol: str, tr: DensityTrack, now: int, direction: str, strength: float,
               spoof: bool = False) -> DetectorEvent:
        sign = -1 if tr.is_bid else 1
        d = {
            "side": tr.side,
            "price": tr.price,
            "notional_usd": round(tr.cur_usd if not spoof else tr.peak_usd, 2),
            "peak_usd": round(tr.peak_usd, 2),
            "dist_pct": round(sign * tr.dist_pct, 3),
            "lifetime_s": round(tr.lifetime_s(now), 1),
            "traded_share_pct": round(tr.traded_share * 100, 1),
            "pulled_share_pct": round(tr.pulled_share * 100, 1),
            "relocations": tr.relocations,
            "threshold_usd": round(tr.threshold_usd, 2),
        }
        if spoof:
            d["spoof_pulled"] = True
        if tr.reloc_notes:
            d["relocation_notes"] = tr.reloc_notes[-3:]
        return DetectorEvent(symbol, DENSITY, direction, round(strength, 3), now, d)
