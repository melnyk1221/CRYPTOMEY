import random

import pytest

from helpers import Feed, make_settings, make_universe
from scalpbot.alerts import formatter as fmt
from scalpbot.bus import DENSITY, LONG, PUSH, ROBOT, SHORT, SQUEEZE, DetectorEvent
from scalpbot.detectors.push import PushDetector
from scalpbot.detectors.squeeze import SqueezeDetector, evaluate_conditions, squeeze_metrics
from scalpbot.market.tier1 import Tier1
from scalpbot.scoring import L_SETUP, AlertDecision
from scalpbot.sources.dexscreener import DexScreener, build_metrics, match_pair, skip_dex, split_multiplier
from scalpbot.sources.mexc_spot_rest import SpotSnapshot
from scalpbot.storage.db import Database


# ---------- 7. толкание ----------
def _book(ts, best_bid, wall_price, wall_qty=100_000.0, rnd=None):
    """Бид-лестница по 0.001 вниз от best_bid, стенка на wall_price; аск над best_bid."""
    bids = []
    for i in range(50):
        p = round(best_bid - 0.001 * i, 6)
        q = wall_qty if abs(p - wall_price) < 1e-9 else (rnd.uniform(500, 1500) if rnd else 1000.0)
        bids.append((p, q))
    asks = [(round(best_bid + 0.001 * (i + 1), 6), 1000.0) for i in range(50)]
    return SpotSnapshot(ts, bids, asks)


def test_push_three_step_ups_emit_long():
    s = make_settings()
    det = PushDetector(s)
    ts = 1_000_000
    bb = 1.000
    evs = []
    # стенка на 0.990, затем 3 перестановки выше, лучший бид растёт; середина +~1.5%
    for k in range(4):
        wall = round(0.990 + 0.005 * k, 6)
        for _ in range(5):
            ts += 2000
            evs += det.on_snapshot("X_USDT", _book(ts, round(bb, 6), wall))
        bb += 0.005
    assert len(evs) == 1
    e = evs[0]
    assert e.family == PUSH and e.direction == LONG and e.details["steps"] >= 3 and e.details["spot_move_pct"] >= 1.0


def test_push_random_wall_noise_no_event():
    s = make_settings()
    det = PushDetector(s)
    rnd = random.Random(5)
    ts = 1_000_000
    evs = []
    for _ in range(300):
        ts += 2000
        bb = 1.0 + rnd.uniform(-0.003, 0.003)
        wall = round(bb - 0.001 * rnd.randint(3, 30), 6)
        evs += det.on_snapshot("X_USDT", _book(ts, round(bb, 6), wall, rnd=rnd))
    assert evs == []


def test_push_steps_without_price_rise_no_event():
    s = make_settings()
    det = PushDetector(s)
    ts = 1_000_000
    evs = []
    for k in range(5):
        wall = round(0.990 + 0.001 * k, 6)
        for _ in range(3):
            ts += 2000
            evs += det.on_snapshot("X_USDT", _book(ts, round(1.0 + 0.0005 * k, 6), wall))
    assert evs == []   # шаги есть, но спот вырос < 1%


# ---------- 10. сквиз ----------
def _state(**kw):
    u = make_universe({"AGT_USDT": 10.0})
    t1 = Tier1(u)
    st = t1.get("AGT_USDT")
    st.last_price = 0.04
    st.hold_vol = kw.get("hold_vol", 152_500_000)          # контракты × 10 × 0.04 = $61M
    st.amount24 = kw.get("amount24", 375_000_000.0)
    st.funding_rate = kw.get("funding", 0.0012)
    return st


def test_squeeze_ratios_and_mcap_fallback_flag():
    st = _state()
    dex = {"mcap": 97e6, "mcap_source": "fdv", "volume_h24": 5e6}
    m = squeeze_metrics(st, dex, 5e6)
    assert m["oi_usd"] == pytest.approx(61e6)
    assert m["oi_to_mcap"] == pytest.approx(0.6289, abs=1e-3)
    assert m["fut_spot_ratio"] == pytest.approx(37.5)
    assert m["mcap_source"] == "fdv"
    conds, votes = evaluate_conditions(m)
    assert set(conds) == {"oi_to_mcap", "fut_spot_ratio", "funding_positive"} and votes[SHORT] == 1
    evs = SqueezeDetector().evaluate(st, 1000, dex, 5e6)
    assert len(evs) == 1 and evs[0].family == SQUEEZE and evs[0].direction == SHORT
    assert evs[0].details["mcap_source"] == "fdv"


def test_squeeze_requires_two_conditions():
    st = _state(funding=0.0001)
    dex = {"mcap": 1e9, "mcap_source": "mcap", "volume_h24": 5e6}
    det = SqueezeDetector()
    assert det.evaluate(st, 1000, dex, 30e6) == []         # только фьюч/спот ≥ 10
    st2 = _state(funding=-0.0006)                          # + funding ≤ −0.05% → long
    evs = det.evaluate(st2, 2000, dex, 30e6)
    assert len(evs) == 1 and evs[0].direction == LONG
    assert det.evaluate(st2, 3000, dex, 30e6) == []        # повтор подавлен


def test_squeeze_without_dex_has_no_ratio():
    st = _state()
    m = squeeze_metrics(st, None, None)
    assert m["oi_to_mcap"] is None and m["fut_spot_ratio"] is None


# ---------- 14. DexScreener ----------
def pair(sym, price, liq, chain="solana", addr=None, mcap=None, fdv=None, vol=1000.0):
    return {"chainId": chain, "pairAddress": f"p{addr or sym}{liq}", "baseToken": {"symbol": sym, "address": addr or f"a{sym}{liq}"},
            "priceUsd": str(price), "liquidity": {"usd": liq}, "marketCap": mcap, "fdv": fdv,
            "volume": {"h24": vol, "h1": 1, "m5": 0}}


def test_dex_match_highest_liquidity_exact_symbol():
    pairs = [pair("AGT", 0.040, 1e6, addr="good"), pair("AGT", 0.0401, 5e6, addr="best"),
             pair("AGTX", 0.04, 9e9), pair("agt", 0.0399, 2e5, addr="small")]
    p, note = match_pair(pairs, "AGT", 0.040)
    assert p["baseToken"]["address"] == "best" and note == ""


def test_dex_rejects_illiquid_pairs_even_if_price_matches():
    pairs = [pair("UNITREE", 92.59, 16_151, chain="robinhood", mcap=15_421)]
    p, note = match_pair(pairs, "UNITREE", 93.0)
    assert p is None and "ликвидность" in note


def test_squeeze_ignores_absurd_oi_to_mcap():
    st = _state(funding=-0.0107)
    m = squeeze_metrics(st, {"mcap": 15_421, "mcap_source": "mcap", "volume_h24": 3_492}, None)
    assert m["oi_to_mcap"] is None and m["mcap_suspicious"] is True and m["oi_to_mcap_raw"] > 5
    conds, _ = evaluate_conditions(m)
    assert "oi_to_mcap" not in conds


def test_dex_rejects_price_deviation():
    pairs = [pair("AGT", 0.050, 5e6), pair("AGT", 0.030, 1e6)]
    p, note = match_pair(pairs, "AGT", 0.040)
    assert p is None and "цена не совпадает" in note


def test_dex_picks_price_consistent_when_top_liquidity_is_other_token():
    pairs = [pair("AGT", 0.0566, 2.8e8, addr="other"), pair("AGT", 0.0311, 3.1e7, addr="alaya")]
    p, note = match_pair(pairs, "AGT", 0.0312)
    assert p["baseToken"]["address"] == "alaya" and note


def test_dex_metrics_mcap_fallback_and_volume_sum():
    pairs = [pair("AGT", 0.04, 5e6, addr="t", fdv=9e7, vol=1000), pair("AGT", 0.04, 1e6, addr="t", vol=500)]
    m = build_metrics(pairs[0], pairs)
    assert m["mcap"] == 9e7 and m["mcap_source"] == "fdv" and m["volume_h24"] == 1500 and m["liquidity_usd"] == 6e6


def test_multiplier_and_skip():
    assert split_multiplier("1000BONK") == ("BONK", 1000.0)
    assert split_multiplier("1INCH") == ("1INCH", 1.0)
    assert skip_dex("BTC") and skip_dex("EBAYSTOCK") and not skip_dex("AGT")


async def test_dex_cache_and_mapping(tmp_path):
    db = Database(str(tmp_path))
    await db.open()
    dx = DexScreener(None, db)
    calls = []

    async def fake_search(q):
        calls.append(q)
        if q == "Q":
            from scalpbot.sources.common import SourceError
            raise SourceError("bad", 400)
        return [pair("Q", 0.1205, 1.4e6, chain="bsc", addr="qaddr", mcap=1.2e8)]

    async def fake_token_pairs(chain, addr):
        calls.append(("tok", chain, addr))
        return [pair("Q", 0.1205, 1.4e6, chain="bsc", addr="qaddr", mcap=1.2e8)]

    dx.search = fake_search
    dx.token_pairs = fake_token_pairs
    m = await dx.metrics("Q_USDT", "Q", 0.1204)
    assert m and m["chain_id"] == "bsc" and m["mcap"] == 1.2e8
    assert calls == ["Q", "Q USDT"]
    await dx.metrics("Q_USDT", "Q", 0.1204)       # из кэша 5 мин
    assert len(calls) == 2
    m2 = await dx.metrics("Q_USDT", "Q", 0.1204, force=True)   # кэш сопоставления 24 ч → по адресу
    assert m2 and calls[-1] == ("tok", "bsc", "qaddr")
    assert (await db.get_dex_map("Q_USDT"))["address"] == "qaddr"
    await db.close()


# ---------- дайджест ----------
def test_digest_format():
    evs = [DetectorEvent("AGT_USDT", DENSITY, LONG, 0.5, 1, {"side": "bid", "price": 0.0384, "notional_usd": 310000,
                                                             "dist_pct": -4.0, "lifetime_s": 360}),
           DetectorEvent("AGT_USDT", ROBOT, LONG, 0.5, 2, {"side": "buy", "size_usd": 5000, "interval_s": 1.0,
                                                           "count": 23, "duration_s": 25, "total_usd": 115000})]
    over = [AlertDecision("Q_USDT", L_SETUP, SHORT, 3, [])]
    text = fmt.format_digest(evs, over, lambda s: (lambda p: f"{p:.5f}"))
    assert "Дайджест" in text and "AGT_USDT" in text and "Сверх лимита" in text and "Q_USDT" in text
    assert fmt.format_digest([], [], lambda s: str) == ""


def test_stage_alert_format_example():
    evs = [DetectorEvent("AGT_USDT", DENSITY, LONG, 0.6, 1, {"side": "bid", "price": 0.0384, "notional_usd": 310000,
                                                             "dist_pct": -4.0, "lifetime_s": 360, "traded_share_pct": 72}),
           DetectorEvent("AGT_USDT", ROBOT, LONG, 0.7, 2, {"side": "buy", "size_usd": 5000, "interval_s": 1.0,
                                                           "count": 23, "duration_s": 25, "total_usd": 115000}),
           DetectorEvent("AGT_USDT", PUSH, LONG, 0.7, 3, {"steps": 4, "minutes": 7, "spot_move_pct": 1.4}),
           DetectorEvent("AGT_USDT", SQUEEZE, LONG, 0.5, 4, {"oi_to_mcap": 0.64, "fut_spot_ratio": 25.7,
                                                             "funding_pct": -0.03, "funding_cycle_h": 8})]
    dec = AlertDecision("AGT_USDT", "STRONG", LONG, 4, evs, families=["DENSITY", "PUSH", "ROBOT", "SQUEEZE"])
    text = fmt.format_stage_alert(dec, price_str="0.04000", change_15m=5.7, amount24=41.2e6, spread_pct=0.05,
                                  history="📊 История STRONG (30д): статистики пока нет",
                                  url="https://futures.mexc.com/exchange/AGT_USDT", fp=lambda p: f"{p:.5f}",
                                  stop_hint="Логичный стоп — за плотностью 0.03840.")
    assert text.startswith("🟢 <b>STRONG</b> · AGT_USDT · лонг-контекст (стадии 4/5)")
    assert "Робот: покупки ≈$5.0k каждые 1.0 с ×23" in text and "OI/капа 0.64" in text
    assert text.rstrip().endswith("Логичный стоп — за плотностью 0.03840.") and len(text) <= 1200
