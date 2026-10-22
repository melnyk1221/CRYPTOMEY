"""F: топливо для сквиза / контекст плеча (tier 2).

Единицы: oi_usd = holdVol × contractSize × lastPrice (только MEXC); oi_to_mcap = oi_usd / mcap
(mcap из DexScreener, при отсутствии — FDV с пометкой); fut_spot_ratio = оборот фьючерсов MEXC за 24ч /
(спот MEXC quoteVolume + DEX volume h24) — приблизительно (только MEXC + DEX).

SQUEEZE, если выполнено ≥ 2 условий: OI/капа ≥ 0.3; фьюч/спот ≥ 10; funding ≤ −0.05% (шорты платят → long)
или ≥ +0.10% (перегретые лонги → short); цена↑ при OI↓ за 15м (закрытие шортов → long); цена↓ при OI↓
(ликвидации лонгов, информативно); ликвидации шортов за 15м ≥ $100k (Bybit).

Эталоны (10.2026): Q — OI ≈ 64% капы, фьюч ≈ 25.7× спот; AGT 16.10 — OI ≈ $61M при капе ≈ $97M (0.63),
фьюч/спот ≈ 37.5×, funding ≈ 0.12% / 8ч. Оба потом резко откатились.
"""
from __future__ import annotations

from ..bus import LONG, NEUTRAL, SHORT, SQUEEZE, DetectorEvent
from ..market.tier1 import SymbolState

OI_MCAP_MIN = 0.3
FUT_SPOT_MIN = 10.0
FUNDING_SHORTS_PAY_PCT = -0.05
FUNDING_LONGS_HOT_PCT = 0.10
PRICE_MOVE_PCT = 1.0
OI_DROP_PCT = -1.0
SHORT_LIQ_MIN_USD = 100_000.0
REEMIT_MS = 30 * 60_000


def squeeze_metrics(st: SymbolState, dex: dict | None, spot_quote_volume: float | None,
                    liq: dict | None = None) -> dict:
    oi_usd = st.oi_usd
    mcap = (dex or {}).get("mcap")
    dex_vol = (dex or {}).get("volume_h24") or 0.0
    spot_vol = spot_quote_volume or 0.0
    denom = spot_vol + dex_vol
    return {
        "oi_usd": round(oi_usd, 2),
        "mcap": mcap,
        "mcap_source": (dex or {}).get("mcap_source"),
        "oi_to_mcap": round(oi_usd / mcap, 4) if mcap else None,
        "fut_turnover_24h": st.amount24,
        "spot_volume_24h": spot_vol if spot_quote_volume is not None else None,
        "dex_volume_24h": dex_vol if dex else None,
        "fut_spot_ratio": round(st.amount24 / denom, 2) if denom > 0 else None,
        "fut_spot_note": "приблизительно (только MEXC + DEX)",
        "funding_pct": round(st.funding_rate * 100.0, 4),
        "funding_cycle_h": st.funding_cycle_h,
        "oi_change_15m_pct": _r(st.oi_change_pct(900)),
        "oi_change_60m_pct": _r(st.oi_change_pct(3600)),
        "price_change_15m_pct": _r(st.pct_change(900)),
        "price_change_60m_pct": _r(st.pct_change_slow(3600)),
        "short_liq_usd_15m": (liq or {}).get("short_usd"),
        "long_liq_usd_15m": (liq or {}).get("long_usd"),
    }


def evaluate_conditions(m: dict) -> tuple[list[str], dict[str, int]]:
    conds: list[str] = []
    votes = {LONG: 0, SHORT: 0}
    if m.get("oi_to_mcap") is not None and m["oi_to_mcap"] >= OI_MCAP_MIN:
        conds.append("oi_to_mcap")
    if m.get("fut_spot_ratio") is not None and m["fut_spot_ratio"] >= FUT_SPOT_MIN:
        conds.append("fut_spot_ratio")
    f = m.get("funding_pct")
    if f is not None and f <= FUNDING_SHORTS_PAY_PCT:
        conds.append("funding_negative")
        votes[LONG] += 1
    elif f is not None and f >= FUNDING_LONGS_HOT_PCT:
        conds.append("funding_positive")
        votes[SHORT] += 1
    p15, oi15 = m.get("price_change_15m_pct"), m.get("oi_change_15m_pct")
    if p15 is not None and oi15 is not None and oi15 <= OI_DROP_PCT:
        if p15 >= PRICE_MOVE_PCT:
            conds.append("short_covering")
            votes[LONG] += 1
        elif p15 <= -PRICE_MOVE_PCT:
            conds.append("long_liquidation")
    sl = m.get("short_liq_usd_15m")
    if sl is not None and sl >= SHORT_LIQ_MIN_USD:
        conds.append("short_liquidations")
        votes[LONG] += 1
    return conds, votes


class SqueezeDetector:
    def __init__(self) -> None:
        self.last: dict[str, tuple[int, frozenset]] = {}

    def evaluate(self, st: SymbolState, now: int, dex: dict | None, spot_qv: float | None,
                 liq: dict | None = None) -> list[DetectorEvent]:
        m = squeeze_metrics(st, dex, spot_qv, liq)
        conds, votes = evaluate_conditions(m)
        if len(conds) < 2:
            return []
        key = frozenset(conds)
        prev = self.last.get(st.symbol)
        if prev and now - prev[0] < REEMIT_MS and key <= prev[1]:
            return []
        self.last[st.symbol] = (now, key)
        if votes[LONG] > votes[SHORT]:
            direction = LONG
        elif votes[SHORT] > votes[LONG]:
            direction = SHORT
        else:
            direction = NEUTRAL
        strength = min(1.0, 0.25 + 0.15 * len(conds))
        m["conditions"] = conds
        return [DetectorEvent(st.symbol, SQUEEZE, direction, round(strength, 3), now, m)]


def _r(x: float | None) -> float | None:
    return None if x is None else round(x, 2)
