"""Tier 2: детальное наблюдение (стакан, сделки) для ≤ TIER2_MAX символов.

Промоушен — по импульсу/предимпульсу, скачку OI, экстремальному фандингу, топ-5 по 15м и /watch.
Вытеснение — символ с самым старым last_event_at, пробывший в tier 2 ≥ 20 мин (ручные не вытесняются).
Понижение — TIER2_IDLE_MIN минут без событий детекторов. При понижении вся память освобождается.
"""
from __future__ import annotations

import logging
from collections import deque
from typing import Any, Callable

from ..config import Settings
from .orderbook import OrderBook
from .trades import TradeAggregator
from .universe import ContractSpec

log = logging.getLogger("tier2")

SYMBOLS_PER_CONN = 8
MIN_STAY_MS = 20 * 60_000
REPROMOTE_BLOCK_MS = 30 * 60_000


class Tier2State:
    __slots__ = ("symbol", "spec", "book", "trades", "promoted_at", "last_event_at", "reason", "manual",
                 "conn_idx", "det", "spot_book_snapshots", "spot_symbol", "dex_metrics", "liq_ring",
                 "spot_mid_ring")

    def __init__(self, spec: ContractSpec, now_ms: int, reason: str, manual: bool, vol_idx: int):
        self.symbol = spec.symbol
        self.spec = spec
        self.book = OrderBook(spec.symbol, spec.contract_size, vol_idx)
        self.trades = TradeAggregator(spec.symbol, spec.contract_size)
        self.promoted_at = now_ms
        self.last_event_at = now_ms
        self.reason = reason
        self.manual = manual
        self.conn_idx = -1
        self.det: dict[str, Any] = {}           # состояние детекторов по имени
        self.spot_book_snapshots: deque = deque(maxlen=300)
        self.spot_symbol: str | None = None
        self.dex_metrics: dict | None = None
        self.liq_ring: deque = deque(maxlen=500)
        self.spot_mid_ring: deque = deque(maxlen=300)

    def mid(self) -> float:
        return self.book.mid


class Tier2Manager:
    def __init__(self, settings: Settings,
                 subscribe: Callable[[int, str], Any],
                 unsubscribe: Callable[[int, str], Any],
                 on_promoted: Callable[[Tier2State], Any] | None = None,
                 on_demoted: Callable[[str], Any] | None = None):
        self.s = settings
        self.states: dict[str, Tier2State] = {}
        self._subscribe = subscribe
        self._unsubscribe = unsubscribe
        self.on_promoted = on_promoted
        self.on_demoted = on_demoted
        self.conn_load: list[int] = []
        self.demoted_at: dict[str, int] = {}
        self.max_override: int | None = None    # снижается сторожем памяти
        self.vol_idx = 1
        self.promotions = 0
        self.refusals = 0

    @property
    def capacity(self) -> int:
        cap = self.s.TIER2_MAX
        if self.max_override is not None:
            cap = min(cap, self.max_override)
        return max(1, cap)

    def __contains__(self, symbol: str) -> bool:
        return symbol in self.states

    def __len__(self) -> int:
        return len(self.states)

    def get(self, symbol: str) -> Tier2State | None:
        return self.states.get(symbol)

    def touch(self, symbol: str, ts: int) -> None:
        st = self.states.get(symbol)
        if st and ts > st.last_event_at:
            st.last_event_at = ts

    def _pick_conn(self) -> int:
        for i, n in enumerate(self.conn_load):
            if n < SYMBOLS_PER_CONN:
                return i
        self.conn_load.append(0)
        return len(self.conn_load) - 1

    def promote(self, spec: ContractSpec, reason: str, now_ms: int, manual: bool = False,
                force: bool = False) -> Tier2State | None:
        sym = spec.symbol
        st = self.states.get(sym)
        if st is not None:
            if manual:
                st.manual = True
            if reason not in st.reason:
                st.reason = (st.reason + ", " + reason)[:120]
            return st
        if not manual and not force:
            last_dem = self.demoted_at.get(sym)
            if last_dem and now_ms - last_dem < REPROMOTE_BLOCK_MS and reason not in ("impulse",):
                return None
        if len(self.states) >= self.capacity and not manual:
            victim = self._eviction_candidate(now_ms, ignore_min_stay=force)
            if victim is None:
                self.refusals += 1
                return None
            self.demote(victim, now_ms, why="вытеснен")
        st = Tier2State(spec, now_ms, reason, manual, self.vol_idx)
        st.conn_idx = self._pick_conn()
        self.conn_load[st.conn_idx] += 1
        self.states[sym] = st
        self.promotions += 1
        self._subscribe(st.conn_idx, sym)
        log.info("Tier 2: добавлен", extra={"symbol": sym, "reason": reason, "manual": manual,
                                            "size": len(self.states)})
        if self.on_promoted:
            self.on_promoted(st)
        return st

    def _eviction_candidate(self, now_ms: int, ignore_min_stay: bool = False) -> str | None:
        cands = [s for s in self.states.values() if not s.manual and now_ms - s.promoted_at >= MIN_STAY_MS]
        if not cands and ignore_min_stay:
            # Импульс важнее: вытесняем самый «тихий» символ, попавший не по импульсу.
            cands = [s for s in self.states.values() if not s.manual and "impulse" not in s.reason]
        if not cands:
            return None
        return min(cands, key=lambda s: s.last_event_at).symbol

    def demote(self, symbol: str, now_ms: int, why: str = "простой") -> None:
        st = self.states.pop(symbol, None)
        if st is None:
            return
        if 0 <= st.conn_idx < len(self.conn_load):
            self.conn_load[st.conn_idx] = max(0, self.conn_load[st.conn_idx] - 1)
        self._unsubscribe(st.conn_idx, symbol)
        self.demoted_at[symbol] = now_ms
        if len(self.demoted_at) > 2000:
            cutoff = now_ms - REPROMOTE_BLOCK_MS
            self.demoted_at = {k: v for k, v in self.demoted_at.items() if v >= cutoff}
        log.info("Tier 2: удалён", extra={"symbol": symbol, "why": why, "size": len(self.states)})
        if self.on_demoted:
            self.on_demoted(symbol)

    def demote_idle(self, now_ms: int) -> list[str]:
        idle_ms = int(self.s.TIER2_IDLE_MIN * 60_000)
        out = [s.symbol for s in self.states.values() if not s.manual and now_ms - s.last_event_at > idle_ms]
        for sym in out:
            self.demote(sym, now_ms)
        # Если ёмкость уменьшили (сторож памяти) — убираем лишние неручные.
        while len(self.states) > self.capacity:
            cands = [s for s in self.states.values() if not s.manual]
            if not cands:
                break
            victim = min(cands, key=lambda s: s.last_event_at).symbol
            self.demote(victim, now_ms, why="лимит памяти")
            out.append(victim)
        return out

    def demote_all_auto(self, now_ms: int) -> list[str]:
        out = [s.symbol for s in self.states.values() if not s.manual]
        for sym in out:
            self.demote(sym, now_ms, why="память")
        return out

    def count_reason(self, prefix: str) -> int:
        return sum(1 for s in self.states.values() if s.reason.startswith(prefix))

    def describe(self, now: int) -> list[str]:
        rows = []
        for st in sorted(self.states.values(), key=lambda s: s.promoted_at):
            mins = (now - st.promoted_at) / 60000
            book = "стакан ✅" if st.book.synced else "стакан ⏳"
            rows.append(f"• {st.symbol}{' 📌' if st.manual else ''} — {st.reason} ({mins:.0f} мин, {book})")
        return rows
