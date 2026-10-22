import gc
import math
import random

import pytest

from helpers import Feed, make_settings, make_universe
from scalpbot.bus import (ABSORPTION, DENSITY, EXHAUSTION, IMPULSE, LONG, NEUTRAL, PUSH, ROBOT, SHORT,
                          SQUEEZE, DetectorEvent)
from scalpbot.detectors.absorption import AbsorptionDetector
from scalpbot.detectors.density import DensityDetector
from scalpbot.detectors.impulse import ExhaustionDetector
from scalpbot.detectors.robot import RobotDetector, find_run
from scalpbot.market.orderbook import OrderBook
from scalpbot.market.tier1 import Tier1
from scalpbot.market.tier2 import Tier2Manager
from scalpbot.market.trades import TakerOrder, TradeAggregator
from scalpbot.scoring import L_SETUP, L_STRONG, AlertDecision, AlertGate, Scorer


# ---------- 2. стакан ----------
def snap(version, bids, asks):
    return {"version": version, "bids": [[p, v, 1] for p, v in bids], "asks": [[p, v, 1] for p, v in asks]}


def upd(version, bids=(), asks=(), begin=None):
    d = {"version": version, "bids": [[p, v, 1] for p, v in bids], "asks": [[p, v, 1] for p, v in asks]}
    if begin is not None:
        d["begin"], d["end"] = begin, version
    return d


def test_book_snapshot_and_sequential_updates():
    b = OrderBook("X_USDT", 1.0)
    b.apply_snapshot(snap(100, [(99, 5), (98, 3)], [(101, 4), (102, 2)]))
    assert b.best_bid == 99 and b.best_ask == 101 and b.mid == 100
    assert b.on_update(upd(101, bids=[(99.5, 7)])) == "applied"
    assert b.best_bid == 99.5
    assert b.on_update(upd(102, asks=[(101, 0)])) == "applied"   # 0 → удалить уровень
    assert 101 not in b.asks and b.best_ask == 102
    assert b.on_update(upd(100)) == "stale"
    assert b.version == 102


def test_book_buffers_before_snapshot_and_drops_old():
    b = OrderBook("X_USDT", 1.0)
    assert b.on_update(upd(99, bids=[(90, 1)])) == "buffered"
    assert b.on_update(upd(101, bids=[(99, 9)])) == "buffered"
    b.apply_snapshot(snap(100, [(99, 5)], [(101, 4)]))
    assert b.version == 101 and b.bids[99] == 9 and 90 not in b.bids


def test_book_out_of_order_and_gap_resync():
    b = OrderBook("X_USDT", 1.0)
    b.apply_snapshot(snap(100, [(99, 5)], [(101, 4)]))
    assert b.on_update(upd(102, bids=[(98, 1)]), now=0.0) == "gap"
    assert b.on_update(upd(101, bids=[(97, 1)]), now=0.5) == "applied"   # пробел закрылся → буфер применён
    assert b.version == 102 and 98 in b.bids and not b.pending
    assert b.on_update(upd(105, bids=[(96, 1)]), now=1.0) == "gap"
    assert b.on_update(upd(106), now=3.5) == "resync" and b.needs_resync
    # resync через depth_commits
    ok = b.apply_commits([{"version": 103, "bids": [[95, 1, 1]], "asks": []},
                          {"version": 104, "bids": [], "asks": [[101, 0, 0]]}])
    assert ok and b.version == 106 and not b.needs_resync and 101 not in b.asks
    # коммиты не покрывают разрыв → нужен снапшот
    b.on_update(upd(110), now=10)
    assert b.apply_commits([{"version": 109, "bids": [], "asks": []}]) is False


def test_book_begin_end_ranges():
    b = OrderBook("X_USDT", 1.0)
    b.apply_snapshot(snap(100, [(99, 5)], [(101, 4)]))
    assert b.on_update(upd(103, bids=[(99, 1)], begin=101)) == "applied"
    assert b.version == 103
    assert b.on_update(upd(104, bids=[(99, 2)], begin=103)) == "applied"   # перекрытие допустимо
    assert b.on_update(upd(108, begin=106)) == "gap"


def test_book_prune_keeps_band():
    b = OrderBook("X_USDT", 1.0)
    b.apply_snapshot(snap(1, [(100 - i * 0.5, 1) for i in range(1, 40)], [(100 + i * 0.5, 1) for i in range(1, 40)]))
    b.prune(5.0)
    assert min(b.bids) >= 95 * 0.999 and max(b.asks) <= 105 * 1.001
    assert b.usd(100, 3) == 300


# ---------- 3. тейкер-ордера ----------
def test_taker_order_merge_and_self_trade_exclusion():
    a = TradeAggregator("X_USDT", 10.0)
    for p in (1.00, 1.01, 1.02):  # один агрессивный ордер на 3 уровня
        a.add_fill({"p": p, "v": 100, "T": 1, "O": 1, "M": 2, "t": 1000})
    a.add_fill({"p": 1.0, "v": 999, "T": 2, "O": 3, "M": 1, "t": 1000})   # self-trade
    a.add_fill({"p": 0.99, "v": 50, "T": 2, "O": 1, "M": 2, "t": 1000})   # другая сторона → новый ордер
    a.add_fill({"p": 0.99, "v": 50, "T": 2, "O": 1, "M": 2, "t": 1500})
    a.flush(10_000)
    orders = list(a.orders)
    assert len(orders) == 3
    o = orders[0]
    assert o.side_buy and o.n_fills == 3
    assert o.notional_usd == pytest.approx(1000 * 1.0 + 1000 * 1.01 + 1000 * 1.02)
    assert o.vwap == pytest.approx(1.01)
    assert a.self_trades == 1
    buy, sell = a.delta(1500, 60)
    assert buy == pytest.approx(3030) and sell == pytest.approx(990)
    assert a.traded_at(0.99, side_buy=False, since_ts=0, until_ts=2000) == pytest.approx(990)


# ---------- 4. робот ----------
def _orders(robot_n=20, size=5000.0, interval=1.0, noise=40, seed=7, t0=1_000_000):
    rnd = random.Random(seed)
    out = []
    for i in range(robot_n):
        ts = t0 + int((i * interval + rnd.uniform(-0.1, 0.1)) * 1000)
        out.append(TakerOrder(ts, True, size * rnd.uniform(0.97, 1.03), 1, 1.0, 1.0))
    span = robot_n * interval
    for _ in range(noise):
        ts = t0 + int(rnd.uniform(0, span) * 1000)
        out.append(TakerOrder(ts, rnd.random() < 0.5, math.exp(rnd.uniform(math.log(200), math.log(50000))), 1, 1.0, 1.0))
    out.sort(key=lambda o: o.ts)
    return out


def test_robot_detects_periodic_equal_orders():
    s = make_settings()
    orders = _orders()
    now = orders[-1].ts + 500
    run = find_run(orders, True, now, s)
    assert run is not None and run.count >= 18
    assert run.size_usd == pytest.approx(5000, rel=0.05) and run.interval_s == pytest.approx(1.0, abs=0.1)
    evs = RobotDetector(s).evaluate("X_USDT", orders, now, 1e6)
    assert [e.family for e in evs] == [ROBOT] and evs[0].direction == LONG
    assert evs[0].details["count"] >= 18 and evs[0].details["total_usd"] >= 15000


def test_robot_not_on_random_flow():
    s = make_settings()
    rnd = random.Random(3)
    t = 1_000_000
    orders = []
    for _ in range(200):
        t += int(rnd.expovariate(1.0) * 1000)
        orders.append(TakerOrder(t, rnd.random() < 0.5, math.exp(rnd.uniform(math.log(200), math.log(50000))), 1, 1, 1))
    assert RobotDetector(s).evaluate("X_USDT", orders, t + 100, 1e6) == []


def test_robot_requires_min_count_and_total():
    s = make_settings()
    few = _orders(robot_n=8, noise=0)
    assert RobotDetector(s).evaluate("X_USDT", few, few[-1].ts + 100, 1e6) == []
    small = _orders(robot_n=20, size=500.0, noise=0)  # 20 × $500 = $10k < $15k
    assert RobotDetector(s).evaluate("X_USDT", small, small[-1].ts + 100, 1e6) == []
    # min total масштабируется с оборотом: 0.002 × $100M = $200k > $100k
    big = _orders(robot_n=20, noise=0)
    assert RobotDetector(s).evaluate("X_USDT", big, big[-1].ts + 100, 1e8) == []


def test_robot_reemit_only_when_doubled():
    s = make_settings()
    det = RobotDetector(s)
    orders = _orders(robot_n=14, noise=0)
    assert len(det.evaluate("X", orders, orders[-1].ts + 100, 1e6)) == 1
    more = _orders(robot_n=20, noise=0)
    assert det.evaluate("X", more, more[-1].ts + 100, 1e6) == []
    most = _orders(robot_n=30, noise=0)
    assert len(det.evaluate("X", most, most[-1].ts + 100, 1e6)) == 1


# ---------- 5–6. плотность и поглощение ----------
def _book_with_wall(wall_price=99.0, wall_vol=1000.0):
    b = OrderBook("X_USDT", 1.0)
    bids = [(100 - 0.1 * i, 10.0) for i in range(1, 30)]
    asks = [(100 + 0.1 * i, 10.0) for i in range(1, 30)]
    bids = [(p, wall_vol if abs(p - wall_price) < 1e-9 else v) for p, v in bids]
    b.apply_snapshot(snap(1, bids, asks))
    return b


def _set(book, is_bid, price, vol, version):
    book.on_update(upd(version, bids=[(price, vol)] if is_bid else [], asks=[] if is_bid else [(price, vol)]))


def _wall_price(book):
    return [p for p, v in book.bids.items() if v == 1000.0][0]


def test_density_confirmed_after_lifetime():
    s = make_settings(DENSITY_MIN_USD=1000, DENSITY_MIN_LIFETIME_S=45)
    b = _book_with_wall()
    wp = _wall_price(b)
    tr = TradeAggregator("X_USDT", 1.0)
    det = DensityDetector(s)
    evs = []
    for sec in range(0, 50):
        evs += det.evaluate("X_USDT", b, tr, 1_000_000 + sec * 1000, 1e6)
    conf = [e for e in evs if e.family == DENSITY and e.direction == LONG]
    assert len(conf) == 1 and conf[0].details["price"] == wp and conf[0].details["lifetime_s"] >= 45


def test_density_traded_vs_pulled_attribution():
    s = make_settings(DENSITY_MIN_USD=1000)
    b = _book_with_wall()
    wp = _wall_price(b)
    tr = TradeAggregator("X_USDT", 1.0)
    det = DensityDetector(s)
    t = 1_000_000
    det.evaluate("X_USDT", b, tr, t, 1e6)
    # продажи на $300 по цене стены, уровень уменьшился на 500 контрактов ($ ~495) → 300 съедено, ~195 снято
    tr.add_fill({"p": wp, "v": 300 / wp, "T": 2, "M": 2, "t": t + 500})
    _set(b, True, wp, 500.0, 2)
    det.evaluate("X_USDT", b, tr, t + 1000, 1e6)
    track = det.tracks[("bid", wp)]
    assert track.traded_usd == pytest.approx(300, rel=1e-6)
    assert track.pulled_usd == pytest.approx(500 * wp - 300, rel=1e-6)


def test_density_spoof_pulled_on_approach():
    s = make_settings(DENSITY_MIN_USD=1000)
    b = OrderBook("X_USDT", 1.0)
    b.apply_snapshot(snap(1, [(99.9, 10), (99.8, 1000), (99.7, 10)] + [(99.6 - 0.1 * i, 10) for i in range(20)],
                          [(100.0, 10)] + [(100.1 + 0.1 * i, 10) for i in range(20)]))
    tr = TradeAggregator("X_USDT", 1.0)
    det = DensityDetector(s)
    t = 1_000_000
    for sec in range(20):
        det.evaluate("X_USDT", b, tr, t + sec * 1000, 1e6)
    _set(b, True, 99.8, 0, 2)   # сняли, цена в 0.15% от стены
    evs = det.evaluate("X_USDT", b, tr, t + 21_000, 1e6)
    spoof = [e for e in evs if e.details.get("spoof_pulled")]
    assert len(spoof) == 1 and spoof[0].direction == NEUTRAL and spoof[0].strength == 0


def test_density_relocation_detected():
    s = make_settings(DENSITY_MIN_USD=1000, DENSITY_MIN_LIFETIME_S=10)
    b = _book_with_wall()
    wp = _wall_price(b)
    tr = TradeAggregator("X_USDT", 1.0)
    det = DensityDetector(s)
    t = 1_000_000
    det.evaluate("X_USDT", b, tr, t, 1e6)
    _set(b, True, wp, 0, 2)
    new_p = [p for p in b.bids if abs(p - (wp + 0.3)) < 1e-6][0]
    _set(b, True, new_p, 1050.0, 3)
    det.evaluate("X_USDT", b, tr, t + 2000, 1e6)
    track = det.tracks.get(("bid", new_p))
    assert track is not None and track.relocations == 1 and track.reloc_notes == ["ближе к цене"]
    evs = []
    for sec in range(3, 30):
        evs += det.evaluate("X_USDT", b, tr, t + sec * 1000, 1e6)
    assert not [e for e in evs if e.direction == LONG]   # переставленная плотность не подтверждается


def _confirmed_wall(s):
    b = _book_with_wall()
    wp = _wall_price(b)
    tr = TradeAggregator("X_USDT", 1.0)
    det = DensityDetector(s)
    t = 1_000_000
    for sec in range(0, 46):
        det.evaluate("X_USDT", b, tr, t + sec * 1000, 1e6)
    return b, wp, tr, det, t + 46_000


def test_absorption_eaten_and_hold():
    s = make_settings(DENSITY_MIN_USD=1000)
    b, wp, tr, det, t = _confirmed_wall(s)
    ab = AbsorptionDetector(s)
    tr.add_fill({"p": wp, "v": 800, "T": 2, "M": 2, "t": t + 100})
    _set(b, True, wp, 200.0, 2)
    det.evaluate("X_USDT", b, tr, t + 1000, 1e6)
    assert ab.evaluate("X_USDT", det, wp, t + 1000) == []          # начали наблюдение
    evs = []
    for sec in range(2, 33):
        evs += ab.evaluate("X_USDT", det, wp * 0.999, t + sec * 1000)  # ушли на 0.1% — удержание
    assert len(evs) == 1 and evs[0].family == ABSORPTION and evs[0].direction == LONG
    assert evs[0].details["broken"] is False


def test_absorption_eaten_and_broken():
    s = make_settings(DENSITY_MIN_USD=1000)
    b, wp, tr, det, t = _confirmed_wall(s)
    ab = AbsorptionDetector(s)
    tr.add_fill({"p": wp, "v": 1000, "T": 2, "M": 2, "t": t + 100})
    _set(b, True, wp, 0, 2)
    det.evaluate("X_USDT", b, tr, t + 1000, 1e6)
    ab.evaluate("X_USDT", det, wp, t + 1000)
    evs = ab.evaluate("X_USDT", det, wp * 0.993, t + 5000)
    assert len(evs) == 1 and evs[0].direction == SHORT and evs[0].details["broken"] is True


# ---------- 9. затухание ----------
def _dump_setup():
    s = make_settings()
    u = make_universe()
    t1 = Tier1(u)
    f = Feed(t1)
    f.noise(31, {"BTC_USDT": 80000.0, "AGT_USDT": 0.04}, vol=0.0003)
    st = t1.get("AGT_USDT")
    st.bid1, st.ask1 = 0.03999, 0.04001
    t1.on_rest_tickers(f.ts, [{"symbol": "AGT_USDT", "bid1": 0.03999, "ask1": 0.04001, "holdVol": 1e6}])
    base = f.prices["AGT_USDT"]
    for k in range(1, 16):
        f.step({"AGT_USDT": base * (1 - 0.004 * k)})   # −6% за 30 с
    ev = DetectorEvent("AGT_USDT", IMPULSE, NEUTRAL, 0.8, f.ts, {"move_sign": -1, "window_s": 60, "move_pct": -6.0})
    det = ExhaustionDetector(s)
    det.arm(ev, st)
    return s, f, st, det, base * (1 - 0.06)


def test_exhaustion_fires_once_when_all_conditions_hold():
    s, f, st, det, low = _dump_setup()
    for _ in range(30):   # стоим на месте 1 мин: время ещё не вышло (< 2 мин)
        f.step({"AGT_USDT": low})
        assert det.evaluate(st, f.ts, (8000, 2000), 0.05) == []
    fired = []
    for _ in range(60):
        f.step({"AGT_USDT": low * 1.001})
        fired += det.evaluate(st, f.ts, (8000, 2000), 0.05)
    assert len(fired) == 1
    ev = fired[0]
    assert ev.family == EXHAUSTION and ev.direction == LONG and ev.details["impulse_tag"] == "dump"
    assert ev.details["delta_share_pct"] == pytest.approx(80.0)


def test_exhaustion_blocked_by_each_condition():
    for case in ("delta", "spread", "speed"):
        s, f, st, det, low = _dump_setup()
        out = []
        for i in range(90):
            p = low * (1 - 0.0015 * i) if case == "speed" else low
            f.step({"AGT_USDT": p})
            delta = (3000, 7000) if case == "delta" else (8000, 2000)
            spread = 1.0 if case == "spread" else 0.05
            out += det.evaluate(st, f.ts, delta, spread)
        assert out == [], case


# ---------- 11. скоринг ----------
def ev(fam, direction, ts, strength=0.7, **d):
    return DetectorEvent("AGT_USDT", fam, direction, strength, ts, d)


def test_scoring_setup_and_strong_and_consensus():
    sc = Scorer(make_settings())
    assert sc.add(ev(DENSITY, LONG, 1000)) == []
    lv = [d.level for d in sc.add(ev(PUSH, LONG, 2000))]
    assert lv == [L_SETUP]
    lv = [d.level for d in sc.add(ev(ROBOT, LONG, 3000))]
    assert lv == [L_STRONG]
    # противоположная сторона не в 1.5 раза слабее → консенсуса нет
    sc2 = Scorer(make_settings())
    sc2.add(ev(DENSITY, LONG, 1000, 0.8))
    sc2.add(ev(PUSH, LONG, 1000, 0.4))
    assert sc2.add(ev(ROBOT, SHORT, 2000, 1.0)) == []


def test_scoring_strong_requires_robot_or_absorption():
    sc = Scorer(make_settings())
    sc.add(ev(DENSITY, SHORT, 1000))
    sc.add(ev(PUSH, SHORT, 1000))
    assert [d.level for d in sc.add(ev(SQUEEZE, SHORT, 1000))] == [L_SETUP]


def test_scoring_spoof_penalty_and_window():
    s = make_settings()
    sc = Scorer(s)
    sc.add(ev(DENSITY, LONG, 1000, 0.5))
    sc.add(ev(DENSITY, NEUTRAL, 1000, 0.0, spoof_pulled=True))
    decs = sc.add(ev(PUSH, LONG, 1000, 0.3))
    assert decs and decs[0].score["long"] == pytest.approx(0.3)     # 0.8 − 0.5 штраф за спуф
    sc.add(ev(DENSITY, NEUTRAL, 1000, 0.0, spoof_pulled=True))
    assert sc.add(ev(PUSH, LONG, 1000, 0.1)) == []                 # 0.9 − 1.0 < 0 → нет консенсуса
    sc3 = Scorer(s)
    sc3.add(ev(DENSITY, LONG, 0))
    assert sc3.add(ev(PUSH, LONG, 16 * 60_000)) == []              # первое событие вне окна 15 мин


def test_upgrade_allowed_inside_cooldown():
    g = AlertGate(make_settings())
    setup = AlertDecision("AGT_USDT", L_SETUP, LONG, 0, [])
    strong = AlertDecision("AGT_USDT", L_STRONG, LONG, 0, [])
    g.set_cooldown(setup, 0)
    assert g.check(setup, 60_000, 1e6, 0.05) == "cooldown"
    assert g.check(strong, 60_000, 1e6, 0.05) is None          # апгрейд разрешён
    g.set_cooldown(strong, 60_000)
    assert g.check(setup, 120_000, 1e6, 0.05) == "cooldown"     # даунгрейд — нет
    assert g.check(setup, 120_000, 1e6, 0.5) == "spread"


# ---------- tier 2 менеджер ----------
def test_tier2_eviction_and_idle_demotion():
    s = make_settings(TIER2_MAX=2, TIER2_IDLE_MIN=45)
    subs = []
    u = make_universe({"A_USDT": 1, "B_USDT": 1, "C_USDT": 1, "D_USDT": 1})
    m = Tier2Manager(s, lambda i, sym: subs.append(("sub", i, sym)), lambda i, sym: subs.append(("unsub", i, sym)))
    m.promote(u.get("A_USDT"), "x", 0)
    m.promote(u.get("B_USDT"), "x", 0)
    assert m.promote(u.get("C_USDT"), "x", 60_000) is None            # все < 20 мин — вытеснять нельзя
    m.touch("B_USDT", 21 * 60_000)
    assert m.promote(u.get("C_USDT"), "x", 21 * 60_000) is not None   # вытеснен A (старейшее событие)
    assert "A_USDT" not in m and ("unsub", 0, "A_USDT") in subs
    m.promote(u.get("D_USDT"), "manual", 21 * 60_000, manual=True)    # ручные сверх лимита
    assert len(m) == 3
    out = m.demote_idle(21 * 60_000 + 46 * 60_000)
    assert set(out) == {"B_USDT", "C_USDT"} and "D_USDT" in m


def test_tier2_impulse_can_evict_young():
    s = make_settings(TIER2_MAX=1)
    u = make_universe({"A_USDT": 1, "B_USDT": 1})
    m = Tier2Manager(s, lambda *a: None, lambda *a: None)
    m.promote(u.get("A_USDT"), "funding", 0)
    assert m.promote(u.get("B_USDT"), "impulse", 1000, force=True) is not None
    assert "A_USDT" not in m


# ---------- память ----------
def test_memory_900_tier1_15_tier2_ten_minutes():
    psutil = pytest.importorskip("psutil")
    proc = psutil.Process()
    gc.collect()
    rss0 = proc.memory_info().rss / 1e6
    syms = {f"C{i}_USDT": 1.0 for i in range(900)}
    u = make_universe(syms)
    t1 = Tier1(u)
    s = make_settings()
    rnd = random.Random(1)
    prices = {sym: 1.0 + rnd.random() for sym in syms}
    t2 = []
    for i in range(15):
        sym = f"C{i}_USDT"
        b = OrderBook(sym, 1.0)
        p = prices[sym]
        b.apply_snapshot(snap(1, [(round(p * (1 - 0.0005 * k), 6), 100) for k in range(1, 1000)],
                              [(round(p * (1 + 0.0005 * k), 6), 100) for k in range(1, 1000)]))
        t2.append((sym, b, TradeAggregator(sym, 1.0), DensityDetector(s), RobotDetector(s)))
    ts = 1_700_000_000_000
    version = 1
    for step in range(300):   # 10 минут по 2 с
        ts += 2000
        for sym in prices:
            prices[sym] *= math.exp(rnd.gauss(0, 0.001))
        t1.on_tickers_frame(ts, [{"symbol": k, "lastPrice": v, "amount24": 1e6} for k, v in prices.items()])
        for sym, b, tr, dens, rob in t2:
            for _ in range(10):
                version += 1
                p = prices[sym] * (1 + rnd.uniform(-0.02, 0.02))
                b.on_update({"version": version, "begin": b.version + 1, "end": b.version + 1,
                             "bids" if p < b.mid else "asks": [[round(p, 6), rnd.choice([0, 50, 500]), 1]]})
            for k in range(40):
                tr.add_fill({"p": prices[sym], "v": rnd.uniform(1, 1000), "T": rnd.choice([1, 2]), "M": 2,
                             "t": ts + k * 50})
            tr.flush(ts + 3000)
            b.prune(5.0)
            dens.evaluate(sym, b, tr, ts, 1e6)
            rob.evaluate(sym, tr.orders, ts, 1e6)
    gc.collect()
    rss1 = proc.memory_info().rss / 1e6
    assert rss1 < 350, rss1
    assert rss1 - rss0 < 150, (rss0, rss1)
