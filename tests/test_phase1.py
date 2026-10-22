import math

import pytest

from helpers import Feed, make_settings, make_universe
from scalpbot.alerts import formatter as fmt
from scalpbot.bus import IMPULSE, LONG, NEUTRAL, SHORT, DetectorEvent
from scalpbot.detectors.impulse import ImpulseDetector
from scalpbot.market.stats import Ewma, Ring, SideBuckets, median
from scalpbot.market.tier1 import Tier1
from scalpbot.market.universe import contracts_to_usd, futures_to_spot, normalize_symbol
from scalpbot.scoring import L_IMPULSE, AlertDecision, AlertGate, Scorer
from scalpbot.storage.db import Database
from scalpbot.tracking import report
from scalpbot.tracking.tracker import Tracker


# ---------- 1. единицы ----------
def test_to_usd_with_different_contract_sizes():
    u = make_universe({"BTC_USDT": 0.0001, "AGT_USDT": 10.0, "PEPE_USDT": 100000.0})
    assert u.to_usd("BTC_USDT", 1000, 80000) == pytest.approx(8000.0)      # 0.1 BTC
    assert u.to_usd("AGT_USDT", 50, 0.04) == pytest.approx(20.0)            # 500 AGT
    assert u.to_usd("PEPE_USDT", 3, 0.00001) == pytest.approx(3.0)
    assert contracts_to_usd(2, 0.5, 10) == 10
    with pytest.raises(KeyError):
        u.to_usd("NOPE_USDT", 1, 1)


def test_symbol_normalization():
    assert normalize_symbol("agt") == "AGT_USDT"
    assert normalize_symbol("AGTUSDT") == "AGT_USDT"
    assert normalize_symbol("AGT_USDT") == "AGT_USDT"
    assert futures_to_spot("AGT_USDT") == "AGTUSDT"


def test_universe_filters_disabled_and_non_usdt():
    from scalpbot.market.universe import Universe
    u = Universe()
    u.load([{"symbol": "A_USDT", "state": 0, "contractSize": 1},
            {"symbol": "B_USDT", "state": 4, "contractSize": 1},
            {"symbol": "C_USD", "state": 0, "contractSize": 1}])
    assert list(u.specs) == ["A_USDT"]


# ---------- статистические примитивы ----------
def test_ring_value_at_and_wrap():
    r = Ring(5)
    for i in range(8):
        r.append(i * 10, float(i))
    assert len(r) == 5 and r.first() == (30, 3.0) and r.last() == (70, 7.0)
    assert r.value_at(55) == 5.0
    assert r.value_at(29) is None
    assert r.values_since(50) == [5.0, 6.0, 7.0]


def test_ewma_and_buckets():
    e = Ewma(10)
    for x in [1.0] * 100:
        e.update(x)
    assert e.mean == pytest.approx(1.0) and e.std == pytest.approx(0.0, abs=1e-9)
    b = SideBuckets(300)
    b.add(1000, True, 100)
    b.add(1500, False, 50)
    b.add(61_000, True, 10)
    assert b.window(61_000, 60) == (10, 0)
    assert b.window(61_000, 300) == (110, 50)
    assert median([3, 1, 2]) == 2


# ---------- 8. импульс ----------
def _warm(minutes=31, **over):
    s = make_settings(**over)
    u = make_universe()
    t1 = Tier1(u)
    f = Feed(t1)
    f.noise(minutes, {"BTC_USDT": 80000.0, "AGT_USDT": 0.04})
    return s, t1, f, ImpulseDetector(s, t1)


def test_impulse_fires_on_drop_above_floor():
    s, t1, f, det = _warm()
    base = f.prices["AGT_USDT"]
    evs = []
    for k in range(1, 16):  # −4.5% за 30 с
        ts = f.step({"AGT_USDT": base * (1 - 0.003 * k)})
        evs += det.evaluate(ts)
    assert len(evs) == 1
    ev = evs[0]
    assert ev.family == IMPULSE and ev.direction == NEUTRAL and ev.details["tag"] == "dump"
    assert ev.details["move_pct"] <= -3.0 and not ev.details["beta_btc"]
    assert 0 < ev.strength <= 1


def test_impulse_respects_min_pct_floor():
    s, t1, f, det = _warm()
    base = f.prices["AGT_USDT"]
    evs = []
    for k in range(1, 16):  # −2% < 3% (z огромный, но действует нижняя граница)
        evs += det.evaluate(f.step({"AGT_USDT": base * (1 - 0.0013 * k)}))
    assert evs == []


def test_impulse_z_threshold_dominates_for_volatile_coin():
    s, t1, f, det = _warm(IMPULSE_MIN_PCT_60S=0.1, IMPULSE_MIN_PCT_5M=0.1)
    thr = det.thresholds(t1.get("AGT_USDT"))
    sigma = t1.get("AGT_USDT").ret_ewma.std
    assert thr[60] == pytest.approx(4.0 * sigma * math.sqrt(30))
    assert thr[300] == pytest.approx(4.0 * sigma * math.sqrt(150))


def test_impulse_btc_beta_tagging_halves_strength():
    s, t1, f, det = _warm()
    b_agt, b_btc = f.prices["AGT_USDT"], f.prices["BTC_USDT"]
    evs = []
    for k in range(1, 16):
        evs += det.evaluate(f.step({"AGT_USDT": b_agt * (1 - 0.003 * k), "BTC_USDT": b_btc * (1 - 0.0025 * k)}))
    agt = [e for e in evs if e.symbol == "AGT_USDT"]
    assert agt and agt[0].details["beta_btc"] is True
    plain = min(1.0, abs(math.log(1 + agt[0].details["move_pct"] / 100)) / (2 * agt[0].details["threshold_pct"] / 100))
    assert agt[0].strength == pytest.approx(plain / 2, rel=0.05)


def test_impulse_warmup_suppression():
    s, t1, f, det = _warm(minutes=10)
    base = f.prices["AGT_USDT"]
    evs = []
    for k in range(1, 16):
        evs += det.evaluate(f.step({"AGT_USDT": base * (1 - 0.004 * k)}))
    assert evs == []


def test_impulse_rearm_no_spam():
    s, t1, f, det = _warm()
    base = f.prices["AGT_USDT"]
    evs = []
    for k in range(1, 60):
        evs += det.evaluate(f.step({"AGT_USDT": base * (1 - 0.002 * min(k, 25))}))
    assert len(evs) == 1


# ---------- 11. скоринг (уровень IMPULSE, кулдаун, лимит) ----------
def _imp(ts, strength=0.8, beta=False, sym="AGT_USDT"):
    return DetectorEvent(sym, IMPULSE, NEUTRAL, strength, ts, {"tag": "dump", "move_sign": -1, "beta_btc": beta})


def test_impulse_alert_level_rules():
    sc = Scorer(make_settings())
    assert [d.level for d in sc.add(_imp(1000))] == [L_IMPULSE]
    assert sc.add(_imp(2000, strength=0.4)) == []
    assert sc.add(_imp(3000, beta=True)) == []


def test_gate_cooldown_and_hourly_cap():
    s = make_settings(MAX_ALERTS_PER_HOUR=2)
    g = AlertGate(s)
    d = AlertDecision("AGT_USDT", L_IMPULSE, NEUTRAL, 0, [])
    assert g.check(d, 0, 1e6, 0.05) is None
    g.set_cooldown(d, 0)
    assert g.check(d, 60_000, 1e6, 0.05) == "cooldown"
    assert g.check(d, 15 * 60_000 + 1, 1e6, 0.05) is None
    assert g.check(d, 0, 1000, 0.05) == "turnover"
    g.record_sent(0)
    g.record_sent(1)
    assert g.over_cap(2)
    assert not g.over_cap(3_600_002)


# ---------- 12. трекер ----------
async def test_tracker_forward_returns_mfe_mae(tmp_path):
    db = Database(str(tmp_path))
    await db.open()
    tr = Tracker(db)
    t0 = 1_000_000
    tr.add(1, "AGT_USDT", LONG, t0, 100.0)
    tr.add(2, "AGT_USDT", SHORT, t0, 100.0)
    path = [(30_000, 103.0), (50_000, 97.0), (60_000, 101.0)]
    for dt, p in path:
        tr.on_price("AGT_USDT", t0 + dt, p)
    await db.flush()
    o1 = await db.fetchone("SELECT * FROM outcomes WHERE event_id=1")
    o2 = await db.fetchone("SELECT * FROM outcomes WHERE event_id=2")
    assert o1["r1"] == pytest.approx(1.0) and o1["mfe1"] == pytest.approx(3.0) and o1["mae1"] == pytest.approx(-3.0)
    assert o2["r1"] == pytest.approx(1.0) and o2["mfe1"] == pytest.approx(3.0) and o2["mae1"] == pytest.approx(-3.0)
    # все горизонты → done, трекер удалён
    tr.on_price("AGT_USDT", t0 + 241 * 60_000, 110.0)
    await db.flush()
    o1 = await db.fetchone("SELECT * FROM outcomes WHERE event_id=1")
    assert o1["done"] == 1 and o1["r240"] == pytest.approx(10.0) and tr.count == 0
    await db.close()


async def test_tracker_restore_marks_missed(tmp_path):
    db = Database(str(tmp_path))
    await db.open()
    tr = Tracker(db)
    t0 = 1_000_000
    tr.add(1, "AGT_USDT", LONG, t0, 100.0)
    tr.on_price("AGT_USDT", t0 + 61_000, 101.0)  # записан только 1м
    await db.flush()
    # «рестарт» через 20 минут: 5м и 15м пропущены, 60м и 240м ещё впереди
    tr2 = Tracker(db)
    n = tr2.restore(await db.load_open_outcomes(), t0 + 20 * 60_000)
    await db.flush()
    o = await db.fetchone("SELECT * FROM outcomes WHERE event_id=1")
    assert n == 1 and o["missed"] == 0b110 and o["r5"] is None and o["r1"] == pytest.approx(1.0)
    t = tr2.by_symbol["AGT_USDT"][0]
    assert t.next_idx == 3
    tr2.on_price("AGT_USDT", t0 + 61 * 60_000, 99.0)
    await db.flush()
    o = await db.fetchone("SELECT * FROM outcomes WHERE event_id=1")
    assert o["r60"] == pytest.approx(-1.0)
    await db.close()


async def test_stats_net_after_costs(tmp_path):
    db = Database(str(tmp_path))
    await db.open()
    now = 10_000_000_000
    for i, (direction, r15) in enumerate([(LONG, 1.0), (LONG, -0.5), (SHORT, -2.0)], start=1):
        db.add_event(id=i, ts=now - 1000, symbol="X_USDT", kind="alert", level="SETUP", direction=direction,
                     strength=1, price=1, ref_price=1, spread_pct=0, details={}, sent=True)
        db.add_outcome(event_id=i, symbol="X_USDT", direction=direction, ts0=now - 1000, ref_price=1)
        db.update_outcome(i, r5=r15, r15=r15, r60=r15, mfe15=abs(r15), mae15=-0.1)
    await db.flush()
    groups = await report.compute_stats(db, 7, 0.15, now_ms=now)
    g = groups[0]
    assert g.level == "SETUP" and g.n == 3
    assert sorted(g.net[15]) == pytest.approx([-0.65, 0.85, 1.85])
    assert g.share_pos(15) == pytest.approx(200 / 3)
    text = report.format_stats(groups, 7, 0.15)
    assert "мало данных" in text and "SETUP" in text
    csv_bytes, n = await report.export_csv(db, 7, now_ms=now)
    assert n == 3 and b"r15" in csv_bytes
    await db.close()


# ---------- 13. форматирование ----------
def test_formatter_escapes_and_limits():
    ev = DetectorEvent("<b>X</b>_USDT", IMPULSE, NEUTRAL, 0.9, 0,
                       {"tag": "pump", "move_sign": 1, "window": "60с", "move_pct": 5.2, "threshold_pct": 3.0,
                        "z": 6.2, "btc_move_pct": 0.1, "beta_btc": False, "oi_change_15m_pct": 12.0,
                        "funding_pct": 0.01, "funding_cycle_h": 8, "amount24": 4.1e7, "change_15m_pct": 6.0})
    text = fmt.format_impulse(ev, price_str="0.04000", spread_pct=0.05, history="📊 История <x> & co",
                              url="https://futures.mexc.com/exchange/X_USDT?a=1&b=2")
    assert "<b>X</b>_USDT" not in text and "&lt;b&gt;X&lt;/b&gt;_USDT" in text
    assert "&lt;x&gt; &amp; co" in text
    assert "ИМПУЛЬС вверх" in text and "Не торговый совет" in text
    assert 'href="https://futures.mexc.com/exchange/X_USDT?a=1&amp;b=2"' in text
    assert len(text) <= 1200


def test_money_and_pct_format():
    assert fmt.money(41_200_000) == "$41.2M"
    assert fmt.money(310_000) == "$310k"
    assert fmt.money(5_000) == "$5.0k"
    assert fmt.pct(-4.0) == "−4.0%"
    assert fmt.pct(5.7) == "+5.7%"
