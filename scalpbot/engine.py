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
from .detectors.impulse import ImpulseDetector
from .market.clock import clock
from .market.tier1 import Tier1
from .market.universe import Universe
from .probes import SOURCES, mode_of, run_probes
from .scoring import L_EXHAUSTION, L_IMPULSE, L_SETUP, L_STRONG, AlertDecision, AlertGate, Scorer
from .sources.common import SourceError, make_session
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
        # Трекеры и кулдауны из SQLite
        now = clock.now()
        restored = self.tracker.restore(await self.db.load_open_outcomes(), now)
        self.gate.load(await self.db.load_cooldowns(now), await self.db.recent_sent_alert_ts(now - 3_600_000))
        log.info("Восстановлено из БД", extra={"trackers": restored, "cooldowns": len(self.gate.cooldowns)})

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
        self.started = True

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
                st.enabled = r.state == "ok" or (mode == "on" and r.state != "disabled")
                if r.state == "disabled":
                    st.enabled = False
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
        """Точка расширения (tier 2)."""

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
        """Точка расширения: промоушен в tier 2, взвод детектора затухания и т.п."""

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
        asyncio.get_running_loop().create_task(self._save_cooldown(dec, until))
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
            asyncio.get_running_loop().create_task(self._send_alert(eid, dec))

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
        """Точка расширения (промоушен по OI/фандингу)."""

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
        warm = sum(1 for s in self.tier1.states.values() if s.warm_minutes() >= self.s.IMPULSE_WARMUP_MIN)
        eligible = len(self.tier1.eligible(self.s.MIN_TURNOVER_24H_USD))
        lines = [
            f"Tier 1: {len(self.tier1.states)} контрактов (под алерты: {eligible}, прогреты: {warm})",
            f"WS тикеры: {'подключён' if ws and ws.connected else 'НЕТ'}, кадров {self.tier1.frames}, "
            f"переподключений {max(0, (ws.connects if ws else 0) - 1)}",
            f"Очередь: потеряно событий {self.queue.dropped}",
            f"События: {self.events_total}, алерты: {self.alerts_total}, отправлено за час: "
            f"{self.gate.sent_last_hour(clock.now())}/{self.s.MAX_ALERTS_PER_HOUR}",
            f"Открытых трекеров исходов: {self.tracker.count}",
        ]
        if self.rest:
            lines.append(f"REST MEXC: {self.rest.base}, запросов {self.rest.requests}, ошибок {self.rest.errors}")
        return lines
