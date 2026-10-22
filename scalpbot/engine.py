"""Рыночный движок: источники → состояние → детекторы → скоринг → алерты, трекинг исходов."""
from __future__ import annotations

import asyncio
import logging
import time
from collections import deque
from datetime import datetime, timedelta
from typing import TYPE_CHECKING
from zoneinfo import ZoneInfo

from .alerts import formatter as fmt
from .bus import (DENSITY, EXHAUSTION, IMPULSE, LONG, PUSH, ROBOT, SHORT, SQUEEZE, DetectorEvent, DropQueue,
                  MarketMsg)
from .detectors.absorption import AbsorptionDetector
from .detectors.density import DensityDetector
from .detectors.impulse import ExhaustionDetector, ImpulseDetector
from .detectors.push import PushDetector
from .detectors.robot import RobotDetector
from .detectors.squeeze import SqueezeDetector
from .market.clock import clock
from .market.tier1 import Tier1
from .market.tier2 import Tier2Manager, Tier2State
from .market.universe import Universe, futures_to_spot, normalize_symbol
from .probes import SOURCES, decide_enabled, mode_of, run_probes
from .scoring import L_EXHAUSTION, L_IMPULSE, L_SETUP, L_STRONG, AlertDecision, AlertGate, Scorer
from .sources.bybit_liq import BybitLiquidations, Liquidation, liq_window
from .sources.common import SourceError, make_session
from .sources.dexscreener import DexScreener
from .sources.hyperliquid import Hyperliquid
from .sources.mexc_spot_rest import MexcSpotRest, SpotSnapshot
from .sources.mexc_futures_rest import MexcFuturesRest
from .sources.mexc_futures_ws import MexcWs
from .supervisor import backoff_delay
from .tracking import report
from .tracking.tracker import Tracker

if TYPE_CHECKING:
    from .app import App

log = logging.getLogger("engine")

DIGEST_FAMILIES = (DENSITY, ROBOT, PUSH, SQUEEZE)


class Engine:
    def __init__(self, app: "App"):
        self.app = app
        self.s = app.settings
        self.db = app.db
        self.health = app.health
        self.session = None
        self.rest: MexcFuturesRest | None = None
        self.universe = Universe()
        self.tier1 = Tier1(self.universe)
        self.impulse = ImpulseDetector(self.s, self.tier1)
        self.exhaustion = ExhaustionDetector(self.s)
        self.absorption = AbsorptionDetector(self.s)
        self.tier2 = Tier2Manager(self.s, self._t2_subscribe, self._t2_unsubscribe,
                                  on_demoted=self._t2_demoted)
        self.t2_queue: DropQueue[MarketMsg] = DropQueue(20000, "tier2")
        self.t2_conns: list[MexcWs] = []
        self.detector_errors = 0
        self.spot: MexcSpotRest | None = None
        self.dex: DexScreener | None = None
        self.squeeze = SqueezeDetector()
        self.bybit: BybitLiquidations | None = None
        self.hl: Hyperliquid | None = None
        self.scorer = Scorer(self.s)
        self.gate = AlertGate(self.s)
        self.tracker = Tracker(self.db)
        self.history = report.LevelHistory(self.db)
        self.queue: DropQueue[MarketMsg] = DropQueue(5000, "universe")
        self.ws_universe: MexcWs | None = None
        self.digest_events: deque[DetectorEvent] = deque(maxlen=500)
        self.funding_cycle: dict[str, float] = {}
        self.depth_vol_index = 1
        self.universe_dirty = False
        self.events_total = 0
        self.alerts_total = 0
        self.outages: list[str] = []
        self.started = False

    # ======================================================================
    # Запуск
    # ======================================================================
    async def start(self) -> None:
        self.session = make_session()
        st = self.health.source("mexc_futures", SOURCES["mexc_futures"])
        self.rest = MexcFuturesRest(self.session, self.s.MEXC_FUTURES_REST_BASE, st)
        await self.run_startup_probes()
        await self.load_universe_with_retry()
        await self.verify_depth_index()
        self.tier2.vol_idx = self.depth_vol_index
        # Трекеры и кулдауны из SQLite
        now = clock.now()
        restored = self.tracker.restore(await self.db.load_open_outcomes(), now)
        self.gate.load(await self.db.load_cooldowns(now), await self.db.recent_sent_alert_ts(now - 3_600_000))
        log.info("Восстановлено из БД", extra={"trackers": restored, "cooldowns": len(self.gate.cooldowns)})

        await self.start_context_sources()
        ws_status = self.health.source("mexc_futures_ws", SOURCES["mexc_futures_ws"])
        self.ws_universe = MexcWs(self.session, "universe", self.queue, ws_status)
        await self.ws_universe.subscribe("sub.tickers", {})
        await self.ws_universe.subscribe("sub.contract", None, gzip=True)
        sp = self.app.spawn
        sp("ws_universe", self.ws_universe.run)
        sp("process_universe", self.process_loop)
        sp("rest_tickers", self.rest_ticker_loop)
        sp("universe_refresh", self.universe_refresh_loop)
        sp("ws_silence", self.silence_watch_loop)
        sp("daily_report", self.daily_report_loop)
        sp("maintenance", self.maintenance_loop)
        sp("digest", self.digest_loop)
        sp("process_tier2", self.t2_process_loop)
        sp("book_sync", self.book_sync_loop)
        sp("tier2_eval", self.tier2_eval_loop)
        sp("memory_watchdog", self.memory_watchdog_loop)
        self.s.on_change(self._on_setting_changed)
        self.started = True

    def _on_setting_changed(self, name: str, value) -> None:
        if name == "COST_ROUND_TRIP_PCT":
            self.history.invalidate()
        if name == "TIER2_MAX":
            self.tier2.max_override = None

    async def stop(self) -> None:
        if self.session:
            await self.session.close()

    async def run_startup_probes(self) -> None:
        attempt = 0
        while True:
            res = await run_probes(self.session, self.s)
            for name, r in res.items():
                mode = mode_of(self.s, name)
                st = self.health.source(name, SOURCES[name], mode)
                st.state = r.state
                st.http_status = r.http_status
                st.latency_ms = r.latency_ms
                st.error = r.error
                # auto + недоступен → отключаем. Binance в v1 только проверяется.
                st.enabled = decide_enabled(mode, r.state)
            log.info("Проверка источников", extra={"probes": {k: (v.state, v.latency_ms, v.http_status, v.error)
                                                               for k, v in res.items()}})
            core_ok = res["mexc_futures"].state == "ok"
            text = "Бот запущен. Источники: " + ", ".join(
                self.health.sources[n].short() for n in SOURCES if n in self.health.sources)
            if core_ok:
                if attempt:
                    text = "✅ MEXC фьючерсы снова доступны. " + text
                self.app.notify(text)
                return
            if attempt == 0 or attempt % 10 == 0:
                self.app.notify("❗ MEXC фьючерсы недоступны — без них бот работать не может. Повторяю попытки.\n"
                                + text)
            await asyncio.sleep(backoff_delay(attempt, cap=120))
            attempt += 1

    async def load_universe_with_retry(self) -> None:
        attempt = 0
        while True:
            try:
                await self.refresh_universe()
                return
            except (SourceError, ValueError) as e:
                log.error("Не удалось загрузить список контрактов", extra={"err": str(e)})
                await asyncio.sleep(backoff_delay(attempt))
                attempt += 1

    async def refresh_universe(self) -> None:
        assert self.rest
        details = await self.rest.contract_detail()
        added, removed = self.universe.load(details, clock.now())
        self.tier1.sync_with_universe()
        for sym in removed:
            self.on_symbol_removed(sym)
        log.info("Список контрактов обновлён", extra={"total": len(self.universe), "added": len(added),
                                                      "removed": len(removed)})

    def on_symbol_removed(self, symbol: str) -> None:
        self.scorer.forget(symbol)
        self.exhaustion.forget(symbol)
        if symbol in self.tier2:
            self.tier2.demote(symbol, clock.now(), why="контракт выключен")

    async def verify_depth_index(self) -> None:
        """VERIFY: индекс 1 в уровнях стакана = объём в контрактах. Проверяем по BTC_USDT."""
        assert self.rest
        spec = self.universe.get("BTC_USDT")
        if not spec:
            return
        try:
            d = await self.rest.depth("BTC_USDT", 1000)
        except SourceError as e:
            log.warning("Проверка индекса стакана не удалась", extra={"err": str(e)})
            return
        bids, asks = d.get("bids") or [], d.get("asks") or []
        if not bids or not asks:
            return
        mid = 0.5 * (float(bids[0][0]) + float(asks[0][0]))

        def notional(idx: int) -> float:
            tot = 0.0
            for lv in bids + asks:
                p = float(lv[0])
                if abs(p / mid - 1) <= 0.01 and len(lv) > idx:
                    tot += float(lv[idx]) * spec.contract_size * p
            return tot

        n1, n2 = notional(1), notional(2)
        plausible = 1e5 <= n1 <= 5e10
        log.info("VERIFY стакан: объём в ±1% от середины BTC_USDT",
                 extra={"notional_idx1_usd": round(n1), "notional_idx2_usd": round(n2), "plausible": plausible})
        if not plausible and 1e5 <= n2 <= 5e10:
            self.depth_vol_index = 2
            log.critical("ВНИМАНИЕ: индекс объёма в стакане похож на 2, а не 1 — переключаюсь на индекс 2")

    # ======================================================================
    # Обработка сообщений
    # ======================================================================
    async def process_loop(self) -> None:
        q = self.queue
        while True:
            m = await q.get()
            self.health.dropped_events = q.dropped
            try:
                self.handle_msg(m)
            except Exception:
                log.exception("Ошибка обработки сообщения", extra={"channel": m.channel, "symbol": m.symbol})
            # Отдаём управление циклу при длинной очереди.
            if len(q) > 100:
                await asyncio.sleep(0)

    def handle_msg(self, m: MarketMsg) -> None:
        clock.update(m.ts)
        if m.channel == "push.tickers":
            data = m.data if isinstance(m.data, list) else [m.data]
            self.tier1.on_tickers_frame(m.ts, data)
            self.on_tier1_frame(m.ts)
        elif m.channel == "push.contract":
            self.on_contract_push(m.data)

    def on_tier1_frame(self, ts: int) -> None:
        # Трекеры исходов: одна проверка на символ с открытыми трекерами.
        states = self.tier1.states
        for sym in list(self.tracker.by_symbol):
            st = states.get(sym)
            if st and st.last_price > 0:
                self.tracker.on_price(sym, ts, st.last_price)
        for ev in self.impulse.evaluate(ts):
            self.handle_event(ev)
        self.after_tier1_frame(ts)

    def after_tier1_frame(self, ts: int) -> None:
        # Промоушен по предтриггеру импульса (≥ 0.6 порога).
        for sym in self.impulse.pretriggered:
            if sym not in self.tier2:
                self.promote(sym, "предимпульс", ts)

    def on_contract_push(self, data) -> None:
        items = data if isinstance(data, list) else [data]
        for it in items:
            if not isinstance(it, dict):
                continue
            sym = it.get("symbol")
            state = it.get("state")
            if sym and state is not None and state != 0 and sym in self.universe:
                log.info("Контракт выключен биржей", extra={"symbol": sym, "state": state})
                self.universe.remove(sym)
                self.tier1.sync_with_universe()
                self.on_symbol_removed(sym)
            elif sym and sym not in self.universe:
                self.universe_dirty = True

    # ======================================================================
    # События и алерты
    # ======================================================================
    def mid_and_spread(self, symbol: str) -> tuple[float | None, float | None]:
        t2 = self.tier2.get(symbol)
        if t2 and t2.book.synced and t2.book.mid > 0:
            return t2.book.mid, t2.book.spread_pct
        st = self.tier1.get(symbol)
        if not st:
            return None, None
        return st.mid, st.spread_pct

    def handle_event(self, ev: DetectorEvent) -> None:
        self.events_total += 1
        st = self.tier1.get(ev.symbol)
        ref = st.last_price if st else 0.0
        mid, spread = self.mid_and_spread(ev.symbol)
        eid = self.db.new_id()
        self.db.add_event(id=eid, ts=ev.ts, symbol=ev.symbol, kind="event", level=ev.family,
                          direction=ev.direction, strength=ev.strength, price=mid, ref_price=ref,
                          spread_pct=spread, details=ev.details, sent=False)
        self.tracker.add(eid, ev.symbol, ev.direction, ev.ts, ref, int(ev.details.get("move_sign", 0)))
        if ev.family in DIGEST_FAMILIES:
            self.digest_events.append(ev)
        self.on_event_hook(ev)
        for dec in self.scorer.add(ev):
            self.handle_decision(dec)

    def on_event_hook(self, ev: DetectorEvent) -> None:
        self.tier2.touch(ev.symbol, ev.ts)
        if ev.family == IMPULSE:
            st = self.tier1.get(ev.symbol)
            if st:
                self.exhaustion.arm(ev, st)
            self.promote(ev.symbol, "impulse", ev.ts, force=True)

    def handle_decision(self, dec: AlertDecision) -> None:
        now = dec.ts
        st = self.tier1.get(dec.symbol)
        amount24 = st.amount24 if st else 0.0
        mid, spread = self.mid_and_spread(dec.symbol)
        reason = self.gate.check(dec, now, amount24, spread)
        if reason in ("turnover", "spread"):
            return  # не проходит фильтры — событие уже записано детектором
        if reason == "cooldown":
            return
        until = self.gate.set_cooldown(dec, now)
        self.app.bg(self._save_cooldown(dec, until))
        send = True
        why = None
        if self.gate.over_cap(now):
            send, why = False, "hourly_cap"
            self.gate.overflow.append(dec)
            del self.gate.overflow[:-50]
        elif self.app.paused:
            send, why = False, "paused"
        elif self.s.setup_mode and not self.s.DRY_RUN:
            send, why = False, "setup_mode"
        if send:
            self.gate.record_sent(now)
        self.alerts_total += 1
        ref = st.last_price if st else 0.0
        eid = self.db.new_id()
        details = {"families": dec.families, "score": dec.score, "stages": dec.stages,
                   "events": [{"family": e.family, "direction": e.direction, "strength": e.strength,
                               "ts": e.ts, **e.details} for e in dec.events[-12:]]}
        if why:
            details["not_sent_reason"] = why
        self.db.add_event(id=eid, ts=now, symbol=dec.symbol, kind="alert", level=dec.level,
                          direction=dec.direction, strength=dec.strength, price=mid, ref_price=ref,
                          spread_pct=spread, details=details, sent=False)
        move_sign = int(dec.events[0].details.get("move_sign", 0)) if dec.level == L_IMPULSE else 0
        self.tracker.add(eid, dec.symbol, dec.direction, now, ref, move_sign)
        log.info("Алерт", extra={"symbol": dec.symbol, "level": dec.level, "direction": dec.direction,
                                 "send": send, "reason": why})
        if send:
            self.app.bg(self._send_alert(eid, dec))

    async def _save_cooldown(self, dec: AlertDecision, until: int) -> None:
        try:
            await self.db.save_cooldown(dec.symbol, dec.level, until)
        except Exception:
            log.exception("Не удалось сохранить кулдаун")

    async def _send_alert(self, eid: int, dec: AlertDecision) -> None:
        try:
            text = await self.render_alert(dec)
        except Exception:
            log.exception("Ошибка форматирования алерта", extra={"symbol": dec.symbol})
            return

        def done(ok: bool) -> None:
            if ok:
                self.db.mark_sent(eid)

        assert self.app.sender
        self.app.sender.send_text(text, on_result=done)

    async def ensure_funding_cycle(self, symbol: str) -> float:
        if symbol in self.funding_cycle:
            return self.funding_cycle[symbol]
        cycle = 8.0
        try:
            assert self.rest
            d = await asyncio.wait_for(self.rest.funding_rate(symbol), 4)
            if d and d.get("collectCycle"):
                cycle = float(d["collectCycle"])
        except Exception:
            log.warning("Интервал фандинга неизвестен, считаю 8 ч", extra={"symbol": symbol})
        self.funding_cycle[symbol] = cycle
        st = self.tier1.get(symbol)
        if st:
            st.funding_cycle_h = cycle
        return cycle

    def fp(self, symbol: str):
        spec = self.universe.get(symbol)
        dec = spec.price_decimals if spec else None
        return lambda p: fmt.fmt_price(p, dec)

    async def render_alert(self, dec: AlertDecision) -> str:
        sym = dec.symbol
        cycle = await self.ensure_funding_cycle(sym)
        for ev in dec.events:
            if "funding_cycle_h" in ev.details:
                ev.details["funding_cycle_h"] = cycle
        st = self.tier1.get(sym)
        mid, spread = self.mid_and_spread(sym)
        price_str = self.fp(sym)(st.last_price if st else mid)
        hist = await self.history.line(dec.level, self.s.COST_ROUND_TRIP_PCT)
        url = self.s.mexc_link(sym)
        ev = dec.events[0]
        if dec.level == L_IMPULSE:
            return fmt.format_impulse(ev, price_str=price_str, spread_pct=spread, history=hist, url=url)
        if dec.level == L_EXHAUSTION:
            return fmt.format_exhaustion(ev, price_str=price_str, spread_pct=spread, history=hist, url=url)
        return fmt.format_stage_alert(dec, price_str=price_str, change_15m=st.pct_change(900) if st else None,
                                      amount24=st.amount24 if st else 0.0, spread_pct=spread, history=hist,
                                      url=url, fp=self.fp(sym), stop_hint=self.stop_hint(dec))

    def stop_hint(self, dec: AlertDecision) -> str | None:
        for ev in sorted(dec.events, key=lambda x: -x.ts):
            if ev.family in (DENSITY,) and ev.direction == dec.direction and ev.details.get("price"):
                return f"Логичный стоп — за плотностью {self.fp(dec.symbol)(ev.details['price'])}."
        return None

    # ======================================================================
    # Периодические задачи
    # ======================================================================
    async def rest_ticker_loop(self) -> None:
        assert self.rest
        while True:
            try:
                items = await self.rest.tickers()
                self.tier1.on_rest_tickers(clock.now(), items)
                self.after_rest_tickers()
            except SourceError as e:
                log.warning("REST ticker: ошибка", extra={"err": str(e)})
            await asyncio.sleep(15)

    def after_rest_tickers(self) -> None:
        """Промоушен по скачку OI и экстремальному фандингу — не больше половины мест tier 2."""
        now = clock.now()
        cands: list[tuple[float, str, str]] = []
        for st in self.tier1.eligible(self.s.MIN_TURNOVER_24H_USD):
            if st.symbol in self.tier2:
                continue
            oi = st.oi_change_pct(900)
            if oi is not None and oi >= self.s.OI_JUMP_PCT:
                cands.append((oi / self.s.OI_JUMP_PCT, st.symbol, f"OI +{oi:.0f}%/15м"))
            elif abs(st.funding_rate * 100) >= self.s.FUNDING_EXTREME:
                cands.append((abs(st.funding_rate * 100) / self.s.FUNDING_EXTREME, st.symbol,
                              f"funding {st.funding_rate * 100:+.3f}%"))
        budget = self.tier2.capacity // 2 - self.tier2.count_reason("OI") - self.tier2.count_reason("funding")
        for _, sym, reason in sorted(cands, reverse=True)[:max(0, budget)]:
            self.promote(sym, reason, now)

    async def universe_refresh_loop(self) -> None:
        last = time.monotonic()
        while True:
            await asyncio.sleep(10)
            if self.universe_dirty or time.monotonic() - last > 6 * 3600:
                self.universe_dirty = False
                try:
                    await self.refresh_universe()
                    last = time.monotonic()
                except (SourceError, ValueError) as e:
                    log.warning("Обновление контрактов не удалось", extra={"err": str(e)})

    async def silence_watch_loop(self) -> None:
        """Поток тикеров молчит 5 мин → предупреждение (один раз) и уведомление о восстановлении."""
        st = self.health.source("mexc_futures_ws", SOURCES["mexc_futures_ws"])
        await asyncio.sleep(60)
        while True:
            await asyncio.sleep(10)
            silent = time.monotonic() - (st.last_message_at or self.health.started_at)
            if silent > 300 and not st.warned:
                st.warned = True
                st.outages += 1
                st.down_since = time.monotonic()
                self.outages.append(f"MEXC WS {datetime.now().strftime('%H:%M')}")
                self.app.notify("⚠️ Поток тикеров MEXC молчит больше 5 минут. Переподключаюсь…")
            elif silent < 30 and st.warned:
                st.warned = False
                mins = (time.monotonic() - (st.down_since or time.monotonic())) / 60
                self.app.notify(f"✅ Поток тикеров MEXC восстановлен (простой ≈{mins:.0f} мин).")

    async def daily_report_loop(self) -> None:
        tz = ZoneInfo(self.s.TZ)
        while True:
            now = datetime.now(tz)
            nxt = now.replace(hour=self.s.DAILY_REPORT_HOUR, minute=0, second=0, microsecond=0)
            if nxt <= now:
                nxt += timedelta(days=1)
            await asyncio.sleep(max(1.0, (nxt - now).total_seconds()))
            try:
                await self.send_daily_report()
            except Exception:
                log.exception("Дневной отчёт не отправлен")

    async def send_daily_report(self) -> None:
        cost = self.s.COST_ROUND_TRIP_PCT
        summary = await report.daily_summary(self.db, cost)
        stats_text = report.format_stats(await report.compute_stats(self.db, 1, cost), 1, cost)
        text = report.format_daily(summary, stats_text, self.health.rss_peak_day_mb, self.outages)
        await self.db.execute("INSERT OR REPLACE INTO daily_summary(day, data) VALUES (?, ?)",
                              (datetime.now(ZoneInfo(self.s.TZ)).strftime("%Y-%m-%d"),
                               report.details_json({"summary": summary, "rss_peak": self.health.rss_peak_day_mb,
                                                    "outages": self.outages})))
        self.health.rss_peak_day_mb = self.health.sample_rss()
        self.outages = []
        from .alerts.sender import split_text
        for chunk in split_text(text):
            self.app.notify(chunk)
        data, n = await report.export_csv(self.db, 7)
        assert self.app.sender
        self.app.sender.send_document(data, f"scalpbot_{datetime.now().strftime('%Y%m%d')}_7d.csv",
                                      caption=f"Резервная копия: {n} строк за 7 дней")

    async def maintenance_loop(self) -> None:
        """Раз в сутки: ретеншн 60 дней; раз в неделю — VACUUM ночью; тревога по размеру БД."""
        last_vacuum = time.time()
        warned_size = False
        while True:
            await asyncio.sleep(3600)
            try:
                hour = datetime.now(ZoneInfo(self.s.TZ)).hour
                if hour == 4:
                    n = await self.db.retention(60)
                    log.info("Ретеншн выполнен", extra={"deleted": n})
                    if time.time() - last_vacuum > 7 * 86400:
                        await self.db.vacuum()
                        last_vacuum = time.time()
                size = self.db.size_mb()
                if size > 300 and not warned_size:
                    warned_size = True
                    self.app.notify(f"⚠️ База данных занимает {size:.0f} МБ (том 0.5 ГБ). Сделайте /export и "
                                    f"уменьшите срок хранения.")
                elif size < 250:
                    warned_size = False
            except Exception:
                log.exception("Ошибка обслуживания БД")

    async def digest_loop(self) -> None:
        while True:
            now = datetime.now()
            nxt = (now + timedelta(hours=1)).replace(minute=0, second=5, microsecond=0)
            await asyncio.sleep((nxt - now).total_seconds())
            events = list(self.digest_events)
            self.digest_events.clear()
            overflow = list(self.gate.overflow)
            self.gate.overflow.clear()
            if not self.app.digest_enabled or self.app.paused:
                continue
            text = fmt.format_digest(events, overflow, self.fp)
            if text:
                self.app.notify(text)

    # ======================================================================
    # Tier 2
    # ======================================================================
    def promote(self, symbol: str, reason: str, now: int, manual: bool = False, force: bool = False) -> Tier2State | None:
        spec = self.universe.get(symbol)
        if spec is None:
            return None
        st = self.tier2.promote(spec, reason, now, manual=manual, force=force)
        if st is not None and not st.det:
            st.det["density"] = DensityDetector(self.s)
            st.det["robot"] = RobotDetector(self.s)
            self.on_promoted_hook(st)
        return st

    def on_promoted_hook(self, st: Tier2State) -> None:
        if self.spot and self.spot.symbols:
            spot_sym = futures_to_spot(st.symbol)
            st.spot_symbol = spot_sym if spot_sym in self.spot.symbols else None
        if st.spot_symbol:
            st.det["push"] = PushDetector(self.s)
        st.det["squeeze_next"] = 0
        if self.dex:
            self.app.bg(self.refresh_dex(st))
        self.on_promoted_extra(st)

    def on_promoted_extra(self, st: Tier2State) -> None:
        if self.bybit:
            self.app.bg(self.bybit.subscribe(st.symbol))

    def _conn(self, idx: int) -> MexcWs:
        while len(self.t2_conns) <= idx:
            i = len(self.t2_conns)
            ws = MexcWs(self.session, f"tier2-{i}", self.t2_queue,
                        self.health.source("mexc_futures_ws", SOURCES["mexc_futures_ws"]))

            async def on_connect(i: int = i) -> None:
                for st in self.tier2.states.values():
                    if st.conn_idx == i:
                        st.book.invalidate()

            ws.on_connect = on_connect
            self.t2_conns.append(ws)
            self.app.spawn(f"ws_tier2_{i}", ws.run)
        return self.t2_conns[idx]

    def _t2_subscribe(self, idx: int, symbol: str) -> None:
        ws = self._conn(idx)

        async def go() -> None:
            await ws.subscribe("sub.deal", {"symbol": symbol, "compress": False})
            await ws.subscribe("sub.depth", {"symbol": symbol})
        self.app.bg(go())

    def _t2_unsubscribe(self, idx: int, symbol: str) -> None:
        if not (0 <= idx < len(self.t2_conns)):
            return
        ws = self.t2_conns[idx]

        async def go() -> None:
            await ws.unsubscribe("sub.deal", {"symbol": symbol})
            await ws.unsubscribe("sub.depth", {"symbol": symbol})
        self.app.bg(go())

    def _t2_demoted(self, symbol: str) -> None:
        self.exhaustion.forget(symbol)
        self.on_demoted_hook(symbol)

    def on_demoted_hook(self, symbol: str) -> None:
        if self.bybit:
            self.app.bg(self.bybit.unsubscribe(symbol))

    async def t2_process_loop(self) -> None:
        q = self.t2_queue
        states = self.tier2.states
        while True:
            m = await q.get()
            try:
                clock.update(m.ts)
                st = states.get(m.symbol or "")
                if st is None:
                    continue
                if m.channel == "push.deal":
                    items = m.data if isinstance(m.data, list) else [m.data]
                    for it in items:
                        st.trades.add_fill(it)
                elif m.channel == "push.depth":
                    if st.book.on_update(m.data) == "resync":
                        self.health.inc("book_resync_requests")
            except Exception:
                log.exception("Ошибка обработки tier 2", extra={"channel": m.channel, "symbol": m.symbol})
            if len(q) > 200:
                await asyncio.sleep(0)

    async def book_sync_loop(self) -> None:
        """Снапшоты стаканов: после промоушена/переподключения, при разрыве версий и раз в 10 минут."""
        assert self.rest
        while True:
            await asyncio.sleep(0.25)
            for st in list(self.tier2.states.values()):
                book = st.book
                try:
                    if book.needs_resync and book.synced:
                        commits = await self.rest.depth_commits(st.symbol, 1000)
                        if st.symbol not in self.tier2:
                            continue
                        if book.apply_commits(commits):
                            self.health.inc("book_resync_ok")
                        else:
                            book.needs_snapshot = True
                            self.health.inc("book_resync_failed")
                    if book.due_resnapshot():
                        snap = await self.rest.depth(st.symbol, 1000)
                        if st.symbol in self.tier2:
                            book.apply_snapshot(snap)
                except SourceError as e:
                    log.warning("Стакан: ошибка REST", extra={"symbol": st.symbol, "err": str(e)})
                    await asyncio.sleep(1)

    async def tier2_eval_loop(self) -> None:
        last_top = 0.0
        last_idle = 0.0
        while True:
            await asyncio.sleep(1.0)
            now = clock.now()
            for st in list(self.tier2.states.values()):
                try:
                    for ev in self.eval_tier2_symbol(st, now):
                        self.handle_event(ev)
                except Exception:
                    self.detector_errors += 1
                    log.exception("Ошибка детектора", extra={"symbol": st.symbol})
            mono = time.monotonic()
            if mono - last_top >= 60:
                last_top = mono
                for t1, _ in self.tier1.top_movers(self.s.MIN_TURNOVER_24H_USD, 900, 5):
                    if t1.symbol not in self.tier2:
                        self.promote(t1.symbol, "топ-5 за 15м", now)
            if mono - last_idle >= 30:
                last_idle = mono
                self.tier2.demote_idle(now)

    def eval_tier2_symbol(self, st: Tier2State, now: int) -> list[DetectorEvent]:
        t1 = self.tier1.get(st.symbol)
        amount24 = t1.amount24 if t1 else 0.0
        st.trades.flush(now)
        book = st.book
        events: list[DetectorEvent] = []
        if book.synced:
            book.prune(self.s.BOOK_KEEP_PCT)
            dens: DensityDetector = st.det["density"]
            events += dens.evaluate(st.symbol, book, st.trades, now, amount24)
            px = st.trades.last_price or (t1.last_price if t1 else 0.0)
            events += self.absorption.evaluate(st.symbol, dens, px, now)
        robot: RobotDetector = st.det["robot"]
        events += robot.evaluate(st.symbol, st.trades.orders, now, amount24)
        if t1:
            spread = book.spread_pct if book.synced else t1.spread_pct
            events += self.exhaustion.evaluate(t1, now, st.trades.delta(now, 60), spread, self.liq_usd(st, now))
        events += self.eval_tier2_extra(st, now)
        return events

    def eval_tier2_extra(self, st: Tier2State, now: int) -> list[DetectorEvent]:
        """Сквиз — раз в минуту на символ (толкание считается по приходу снимка спота)."""
        if now < st.det.get("squeeze_next", 0):
            return []
        st.det["squeeze_next"] = now + 60_000
        t1 = self.tier1.get(st.symbol)
        if t1 is None or t1.hold_vol <= 0:
            return []
        spot_qv = self.spot.quote_volume.get(st.spot_symbol) if (self.spot and st.spot_symbol) else None
        evs = self.squeeze.evaluate(t1, now, st.dex_metrics, spot_qv, self.liq_summary(st, now))
        hl = self.hl.get(st.spec.base) if self.hl else None
        for ev in evs:
            if hl:   # Hyperliquid — отдельно, как контекст (не участвует в условиях)
                ev.details["hl_oi_usd"] = round(hl["oi_usd"], 2)
                ev.details["hl_funding_pct_1h"] = round(hl["funding_pct_1h"], 5)
        return evs

    def liq_summary(self, st: Tier2State, now: int) -> dict | None:
        if not self.bybit or not self.bybit.has(st.symbol):
            return None
        return liq_window(st.liq_ring, now)

    # ---------- контекстные источники: спот MEXC и DexScreener ----------
    async def start_context_sources(self) -> None:
        spot_st = self.health.source("mexc_spot", SOURCES["mexc_spot"])
        if spot_st.enabled:
            self.spot = MexcSpotRest(self.session, spot_st)
            try:
                n = await self.spot.load_exchange_info()
                await self.spot.load_tickers_24h()
                log.info("Спот MEXC загружен", extra={"usdt_pairs": n})
            except SourceError as e:
                log.warning("Спот MEXC недоступен", extra={"err": str(e)})
            self.app.spawn("spot_meta", self.spot_meta_loop)
            self.app.spawn("spot_depth", self.spot_depth_loop)
        dex_st = self.health.source("dexscreener", SOURCES["dexscreener"])
        if dex_st.enabled:
            self.dex = DexScreener(self.session, self.db, dex_st)
            self.app.spawn("dex_refresh", self.dex_refresh_loop)
        await self.start_optional_venues()

    async def start_optional_venues(self) -> None:
        """Bybit (ликвидации) и Hyperliquid (OI/funding) — по флагам auto|on|off и результатам проверки."""
        by_st = self.health.source("bybit", SOURCES["bybit"])
        if by_st.enabled:
            self.bybit = BybitLiquidations(self.session, by_st, self.on_liquidation)
            try:
                n = await self.bybit.load_instruments()
                log.info("Bybit: инструменты загружены", extra={"linear_usdt": n})
                self.app.spawn("bybit_liq", self.bybit.run)
            except SourceError as e:
                log.warning("Bybit недоступен — ликвидации отключены", extra={"err": str(e)})
                by_st.enabled = False
                by_st.state = "blocked" if e.blocked else "error"
                by_st.http_status = e.status
                self.bybit = None
        hl_st = self.health.source("hyperliquid", SOURCES["hyperliquid"])
        if hl_st.enabled:
            self.hl = Hyperliquid(self.session, hl_st)
            self.app.spawn("hyperliquid", self.hl.run)

    def on_liquidation(self, liq: Liquidation) -> None:
        for st in self.tier2.states.values():
            if st.symbol.replace("_", "") == liq.symbol:
                st.liq_ring.append(liq)
                return

    async def spot_meta_loop(self) -> None:
        assert self.spot
        last_info = time.monotonic()
        while True:
            await asyncio.sleep(60)
            try:
                await self.spot.load_tickers_24h()
                if time.monotonic() - last_info > 6 * 3600:
                    await self.spot.load_exchange_info()
                    last_info = time.monotonic()
            except SourceError as e:
                log.warning("Спот: ошибка обновления", extra={"err": str(e)})

    async def spot_depth_loop(self) -> None:
        """Стакан спота раз в 2 с для символов tier 2, у которых есть спот-рынок."""
        assert self.spot
        while True:
            t0 = time.monotonic()
            todo = [st for st in self.tier2.states.values() if st.spot_symbol]
            if todo:
                res = await asyncio.gather(*(self.spot.depth(st.spot_symbol, 50) for st in todo),
                                           return_exceptions=True)
                now = clock.now()
                for st, r in zip(todo, res):
                    if isinstance(r, Exception) or st.symbol not in self.tier2:
                        continue
                    try:
                        snap = SpotSnapshot(int(r.get("timestamp") or now), r.get("bids") or [], r.get("asks") or [])
                        st.spot_book_snapshots.append(snap)
                        push: PushDetector | None = st.det.get("push")
                        t1 = self.tier1.get(st.symbol)
                        if push:
                            for ev in push.on_snapshot(st.symbol, snap, t1.price_ring if t1 else None):
                                self.handle_event(ev)
                    except Exception:
                        self.detector_errors += 1
                        log.exception("Ошибка детектора толкания", extra={"symbol": st.symbol})
            await asyncio.sleep(max(0.2, 2.0 - (time.monotonic() - t0)))

    async def refresh_dex(self, st: Tier2State, force: bool = False) -> dict | None:
        if not self.dex:
            return None
        t1 = self.tier1.get(st.symbol)
        try:
            m = await self.dex.metrics(st.symbol, st.spec.base, t1.last_price if t1 else 0.0, force=force)
        except Exception:
            log.exception("DexScreener: сбой", extra={"symbol": st.symbol})
            return None
        st.dex_metrics = m
        st.det["dex_at"] = time.monotonic()
        return m

    async def dex_refresh_loop(self) -> None:
        while True:
            await asyncio.sleep(30)
            for st in list(self.tier2.states.values()):
                if time.monotonic() - st.det.get("dex_at", 0) > 300:
                    await self.refresh_dex(st)

    async def map_token(self, raw_symbol: str, chain_id: str, address: str) -> str:
        if not self.dex:
            return "DexScreener отключён или недоступен."
        sym = normalize_symbol(raw_symbol)
        if sym not in self.universe:
            return f"Контракт {fmt.e(sym)} не найден."
        try:
            m = await self.dex.set_manual(sym, chain_id.strip().lower(), address.strip())
        except SourceError as e:
            return f"DexScreener ошибка: {fmt.e(str(e))}"
        if not m:
            return "DexScreener не нашёл пар для этого адреса. Проверьте chainId (solana, ethereum, bsc, base…) и адрес."
        st = self.tier2.get(sym)
        if st:
            st.dex_metrics = m
        return (f"✅ {fmt.e(sym)} → {fmt.e(m['chain_id'])}:{fmt.e(m['address'])}\n"
                f"Цена DEX ${m['price_usd']} · ликвидность {fmt.money(m['liquidity_usd'])} · "
                f"{'капа' if m.get('mcap_source') == 'mcap' else 'FDV'} {fmt.money(m.get('mcap'))}")

    def liq_usd(self, st: Tier2State, now: int) -> float | None:
        """Сумма ликвидаций Bybit за 15 мин (для затухания), None — данных нет."""
        if not self.bybit or not st.liq_ring:
            return None
        w = liq_window(st.liq_ring, now)
        return w["long_usd"] + w["short_usd"]

    def watch(self, raw: str) -> str:
        sym = normalize_symbol(raw)
        if sym not in self.universe:
            return f"Контракт {fmt.e(sym)} не найден среди активных USDT-перпетуалов MEXC."
        st = self.promote(sym, "вручную /watch", clock.now(), manual=True)
        if st is None:
            return "Не удалось добавить в tier 2."
        return (f"👁 {fmt.e(sym)} в tier 2 (стакан + сделки). Автоматически не будет удалён. "
                f"/unwatch {fmt.e(sym)} — снять.")

    def unwatch(self, raw: str) -> str:
        sym = normalize_symbol(raw)
        if sym not in self.tier2:
            return f"{fmt.e(sym)} не в tier 2."
        self.tier2.demote(sym, clock.now(), why="/unwatch")
        return f"{fmt.e(sym)} снят с детального наблюдения."

    async def memory_watchdog_loop(self) -> None:
        while True:
            await asyncio.sleep(30)
            self.memory_step(self.health.sample_rss(), time.monotonic())

    _mem_last_warn = -1e9
    _mem_calm_since: float | None = None

    def memory_step(self, rss: float, mono: float) -> str:
        """RSS > 80% лимита → TIER2_MAX −3 (мин. 5), понизить простаивающих, gc;
        RSS > 95% → понизить всех неручных. Предупреждение в Telegram не чаще раза в час."""
        import gc
        limit = float(self.s.MEMORY_LIMIT_MB)
        now = clock.now()
        if rss > 0.95 * limit:
            demoted = self.tier2.demote_all_auto(now)
            self.tier2.max_override = 5
            gc.collect()
            self._mem_calm_since = None
            if mono - self._mem_last_warn > 3600:
                self._mem_last_warn = mono
                self.app.notify(f"🧯 Память {rss:.0f} МБ (> 95% от {limit:.0f}). Из tier 2 убраны все "
                                f"автоматические символы ({len(demoted)}), лимит tier 2 = 5.")
            return "critical"
        if rss > 0.8 * limit:
            self.tier2.max_override = max(5, self.tier2.capacity - 3)
            self.tier2.demote_idle(now)
            gc.collect()
            self._mem_calm_since = None
            if mono - self._mem_last_warn > 3600:
                self._mem_last_warn = mono
                self.app.notify(f"⚠️ Память {rss:.0f} МБ (> 80% от {limit:.0f}). Лимит tier 2 снижен до "
                                f"{self.tier2.capacity}.")
            return "high"
        if self.tier2.max_override is not None and rss < 0.6 * limit:
            self._mem_calm_since = self._mem_calm_since or mono
            if mono - self._mem_calm_since > 600:   # 10 минут спокойно — понемногу возвращаем ёмкость
                nxt = self.tier2.max_override + 3
                self.tier2.max_override = None if nxt >= self.s.TIER2_MAX else nxt
                self._mem_calm_since = mono
                return "relaxed"
            return "calm"
        self._mem_calm_since = None
        return "ok"

    # ======================================================================
    # Для команд
    # ======================================================================
    def top_text(self) -> str:
        rows = self.tier1.top_movers(self.s.MIN_TURNOVER_24H_USD, 900, 10)
        if not rows:
            return "Пока нет данных (нужно ≥ 15 минут истории цен)."
        lines = ["📈 <b>Топ-10 движений за 15 минут</b> (оборот ≥ " + fmt.money(self.s.MIN_TURNOVER_24H_USD) + ")"]
        for i, (st, ch) in enumerate(rows, 1):
            oi = st.oi_change_pct(900)
            lines.append(f"{i}. {fmt.e(st.symbol)} {fmt.pct(ch)} · OI {fmt.pct(oi)} · funding "
                         f"{fmt.pct(st.funding_rate * 100, 3)} · оборот {fmt.money(st.amount24)}")
        return "\n".join(lines)

    def status_lines(self) -> list[str]:
        ws = self.ws_universe
        up = max(1.0, self.health.uptime_s())
        total_msgs = (ws.messages if ws else 0) + sum(c.messages for c in self.t2_conns)
        warm = sum(1 for s in self.tier1.states.values() if s.warm_minutes() >= self.s.IMPULSE_WARMUP_MIN)
        eligible = len(self.tier1.eligible(self.s.MIN_TURNOVER_24H_USD))
        lines = [
            f"Tier 1: {len(self.tier1.states)} контрактов (под алерты: {eligible}, прогреты: {warm})",
            f"WS тикеры: {'подключён' if ws and ws.connected else 'НЕТ'}, кадров {self.tier1.frames}, "
            f"переподключений {max(0, (ws.connects if ws else 0) - 1)}",
            f"WS сообщений/с (среднее): {total_msgs / up:.1f} · потеряно событий {self.queue.dropped}",
            f"События: {self.events_total}, алерты: {self.alerts_total}, отправлено за час: "
            f"{self.gate.sent_last_hour(clock.now())}/{self.s.MAX_ALERTS_PER_HOUR}",
            f"Открытых трекеров исходов: {self.tracker.count}",
        ]
        if self.rest:
            lines.append(f"REST MEXC: {self.rest.base}, запросов {self.rest.requests}, ошибок {self.rest.errors}")
        t2_msgs = sum(c.messages for c in self.t2_conns)
        lines.append(f"Tier 2: {len(self.tier2)}/{self.tier2.capacity} · WS-соединений {len(self.t2_conns)} · "
                     f"сообщений {t2_msgs} · потеряно {self.t2_queue.dropped} · ошибок детекторов {self.detector_errors}")
        lines.extend(self.tier2.describe(clock.now()))
        if self.spot:
            lines.append(f"Спот MEXC: пар USDT {len(self.spot.symbols)}, запросов {self.spot.requests}, "
                         f"ошибок {self.spot.errors}")
        if self.dex:
            lines.append(f"DexScreener: запросов {self.dex.requests}, ошибок {self.dex.errors}")
        if self.bybit:
            lines.append(f"Bybit ликвидации: топиков {len(self.bybit.topics)}, получено {self.bybit.received}")
        if self.hl:
            lines.append(f"Hyperliquid: активов {len(self.hl.assets)}")
        b = self.health.sources.get("binance")
        if b:
            lines.append(f"Binance (только проверка в v1): {b.short()}")
        return lines
