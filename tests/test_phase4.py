import pytest

from scalpbot.probes import decide_enabled
from scalpbot.sources.bybit_liq import BybitLiquidations, Liquidation, liq_window, parse_liquidations, to_bybit
from scalpbot.sources.hyperliquid import hl_name, parse_ctxs
from scalpbot.health import SourceStatus


def test_bybit_liquidation_side_semantics():
    msg = {"topic": "allLiquidation.AGTUSDT", "type": "snapshot", "ts": 1,
           "data": [{"T": 1000, "s": "AGTUSDT", "S": "Buy", "v": "1000", "p": "0.04"},
                    {"T": 1001, "s": "AGTUSDT", "S": "Sell", "v": "5000", "p": "0.041"}]}
    liqs = parse_liquidations(msg)
    assert len(liqs) == 2
    assert liqs[0].long_liq is True and liqs[0].usd == pytest.approx(40.0)     # Buy → ликвидирован лонг
    assert liqs[1].long_liq is False and liqs[1].usd == pytest.approx(205.0)   # Sell → ликвидирован шорт
    assert parse_liquidations({"op": "pong"}) == []
    assert to_bybit("1000BONK_USDT") == "1000BONKUSDT"


def test_liq_window_sums():
    ring = [Liquidation(0, "X", True, 100, 1), Liquidation(10 * 60_000, "X", False, 50_000, 1),
            Liquidation(14 * 60_000, "X", False, 60_000, 1)]
    w = liq_window(ring, 16 * 60_000)
    assert w["short_usd"] == 110_000 and w["long_usd"] == 0 and w["n"] == 2


async def test_bybit_instruments_pagination():
    class FakeResp:
        pass

    pages = [{"result": {"list": [{"symbol": "AGTUSDT", "status": "Trading", "quoteCoin": "USDT"},
                                  {"symbol": "OLDUSDT", "status": "Closed", "quoteCoin": "USDT"}],
                         "nextPageCursor": "c2"}},
             {"result": {"list": [{"symbol": "BTCUSDT", "status": "Trading", "quoteCoin": "USDT"},
                                  {"symbol": "BTCUSDC", "status": "Trading", "quoteCoin": "USDC"}],
                         "nextPageCursor": ""}}]
    import scalpbot.sources.bybit_liq as mod

    calls = []

    async def fake_request(session, method, url, params=None, **kw):
        calls.append(params.get("cursor"))
        return pages[len(calls) - 1], 1.0

    orig = mod.request_json
    mod.request_json = fake_request
    try:
        b = BybitLiquidations(None, SourceStatus("bybit", "Bybit"), lambda liq: None)
        assert await b.load_instruments() == 2
        assert b.instruments == {"AGTUSDT", "BTCUSDT"} and calls == [None, "c2"]
        assert b.has("AGT_USDT") and not b.has("Q_USDT")
    finally:
        mod.request_json = orig


def test_source_auto_disable_rules():
    assert decide_enabled("auto", "ok") is True
    assert decide_enabled("auto", "blocked") is False
    assert decide_enabled("auto", "error") is False
    assert decide_enabled("on", "error") is True
    assert decide_enabled("off", "ok") is False


def test_hyperliquid_parse_and_names():
    js = [{"universe": [{"name": "BTC"}, {"name": "kPEPE"}, {"name": "OLD", "isDelisted": True}]},
          [{"funding": "0.0000125", "openInterest": "100", "markPx": "80000", "dayNtlVlm": "1e9", "premium": "0"},
           {"funding": "-0.0001", "openInterest": "1000000", "markPx": "0.01", "dayNtlVlm": "5e6"},
           {"funding": "0", "openInterest": "1", "markPx": "1"}]]
    a = parse_ctxs(js)
    assert set(a) == {"BTC", "kPEPE"}
    assert a["BTC"]["oi_usd"] == pytest.approx(8_000_000) and a["BTC"]["funding_pct_1h"] == pytest.approx(0.00125)
    assert hl_name("1000PEPE") == "kPEPE" and hl_name("BTC") == "BTC" and hl_name("1INCH") == "1INCH"


async def test_binance_probe_classifies_451():
    import scalpbot.sources.binance_probe as bp
    from scalpbot.sources.common import SourceError

    async def blocked(*a, **k):
        raise SourceError("Service unavailable from a restricted location", 451)

    orig = bp.request_json
    bp.request_json = blocked
    try:
        state, status, err = await bp.probe(None)
        assert state == "blocked" and status == 451
    finally:
        bp.request_json = orig
