"""python -m scalpbot.smoke --seconds 120

Живая проверка MEXC (пункты VERIFY): сообщения/с по каналам, поля push.tickers, сырые сделки при
compress:false, пересчёт объёма стакана в USDT, доля self-trade, версии depth, RSS.
"""
from __future__ import annotations

import argparse
import asyncio
import time
from collections import Counter

from .bus import DropQueue, MarketMsg
from .config import Settings
from .health import rss_mb
from .logging_setup import setup_logging
from .market.trades import TradeAggregator
from .market.universe import Universe
from .sources.common import make_session
from .sources.mexc_futures_rest import MexcFuturesRest
from .sources.mexc_futures_ws import MexcWs


async def run(seconds: int) -> None:
    s = Settings()
    async with make_session() as session:
        rest = MexcFuturesRest(session, s.MEXC_FUTURES_REST_BASE)
        uni = Universe()
        uni.load(await rest.contract_detail())
        tickers = await rest.tickers()
        alts = [t for t in tickers if t.get("symbol") in uni and t["symbol"] != "BTC_USDT"
                and float(t.get("amount24") or 0) > 2e6]
        alts.sort(key=lambda t: abs(float(t.get("riseFallRate") or 0)), reverse=True)
        alt = alts[0]["symbol"] if alts else "ETH_USDT"
        symbols = ["BTC_USDT", alt]
        print(f"Контрактов: {len(uni)}; символы: {symbols}")

        for sym in symbols:
            d = await rest.depth(sym, 1000)
            spec = uni.get(sym)
            bids, asks = d["bids"], d["asks"]
            mid = 0.5 * (bids[0][0] + asks[0][0])
            near = sum(lv[1] * spec.contract_size * lv[0] for lv in bids + asks if abs(lv[0] / mid - 1) <= 0.01)
            print(f"[стакан] {sym}: contractSize={spec.contract_size}, уровней {len(bids)}/{len(asks)}, "
                  f"объём ±1% ≈ ${near:,.0f}, версия {d.get('version')}")
        fr = await rest.funding_rate(alt)
        print(f"[funding] {alt}: rate={fr.get('fundingRate')} collectCycle={fr.get('collectCycle')}ч")

        q: DropQueue[MarketMsg] = DropQueue(20000)
        ws = MexcWs(session, "smoke", q)
        await ws.subscribe("sub.tickers", {})
        for sym in symbols:
            await ws.subscribe("sub.deal", {"symbol": sym, "compress": False})
            await ws.subscribe("sub.depth", {"symbol": sym})
        task = asyncio.create_task(ws.run())

        counts: Counter = Counter()
        tick_keys: set[str] = set()
        aggs = {sym: TradeAggregator(sym, uni.get(sym).contract_size) for sym in symbols}
        fills = Counter()
        self_trades = Counter()
        depth_multi = 0
        depth_gaps = 0
        last_ver: dict[str, int] = {}
        t0 = time.monotonic()
        while time.monotonic() - t0 < seconds:
            try:
                m = await asyncio.wait_for(q.get(), 1.0)
            except asyncio.TimeoutError:
                continue
            counts[m.channel] += 1
            if m.channel == "push.tickers":
                for it in (m.data if isinstance(m.data, list) else [m.data])[:5]:
                    tick_keys |= set(it)
            elif m.channel == "push.deal":
                items = m.data if isinstance(m.data, list) else [m.data]
                for it in items:
                    fills[m.symbol] += 1
                    if it.get("M") == 1:
                        self_trades[m.symbol] += 1
                    aggs[m.symbol].add_fill(it)
            elif m.channel == "push.depth":
                d = m.data
                b, e = int(d.get("begin", d["version"])), int(d.get("end", d["version"]))
                if e > b:
                    depth_multi += 1
                lv = last_ver.get(m.symbol)
                if lv is not None and b != lv + 1:
                    depth_gaps += 1
                last_ver[m.symbol] = e
        task.cancel()
        el = time.monotonic() - t0
        print("\n[каналы] сообщений/с:", {k: round(v / el, 2) for k, v in counts.items()})
        print("[push.tickers] поля:", sorted(tick_keys))
        for f in ("holdVol", "fundingRate", "bid1", "ask1"):
            print(f"   {f}: {'есть' if f in tick_keys else 'НЕТ → берём из REST /contract/ticker'}")
        print(f"[push.depth] пушей с несколькими версиями (begin<end): {depth_multi}, разрывов версий: {depth_gaps}")
        for sym in symbols:
            agg = aggs[sym]
            agg.flush(10**15)
            orders = list(agg.orders)
            multi = sum(1 for o in orders if o.n_fills > 1)
            print(f"[сделки] {sym}: заполнений {fills[sym]}, self-trade (M=1) {self_trades[sym]}, "
                  f"тейкер-ордеров {len(orders)}, из них из нескольких заполнений {multi}")
            for o in orders[-5:]:
                print(f"    {'BUY ' if o.side_buy else 'SELL'} ${o.notional_usd:,.0f} fills={o.n_fills} "
                      f"vwap={o.vwap:g} t={o.ts}")
        print("[VERIFY compress:false]", "сырые заполнения приходят ✅" if any(
            any(o.n_fills > 1 for o in aggs[s_].orders) for s_ in symbols) or sum(fills.values()) > 0
            else "сделок не было — повторите позже")
        print(f"[RSS] {rss_mb():.0f} МБ")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--seconds", type=int, default=120)
    args = ap.parse_args()
    setup_logging("WARNING")
    asyncio.run(run(args.seconds))


if __name__ == "__main__":
    main()
