"""Локальный стакан фьючерсов с контролем версий (официальная процедура MEXC).

VERIFY (живой API, 10.2026): push.depth содержит begin/end — одно сообщение может покрывать несколько
версий. Непрерывность проверяем так: end <= version → старое; begin <= version + 1 → применяем,
version = end; иначе — разрыв (буферизуем; если разрыв держится > 2 с → resync через depth_commits).
Объёмы уровней — абсолютные (в контрактах), 0 = удалить уровень.
"""
from __future__ import annotations

import heapq
import logging
import time

log = logging.getLogger("orderbook")

GAP_RESYNC_S = 2.0
RESNAPSHOT_S = 600.0
MAX_PENDING = 600


class OrderBook:
    __slots__ = ("symbol", "contract_size", "vol_idx", "bids", "asks", "version", "synced", "pending",
                 "gap_since", "best_bid", "best_ask", "last_update_ts", "snapshot_at", "needs_snapshot",
                 "needs_resync", "applied", "stale", "gaps", "resyncs", "snapshots")

    def __init__(self, symbol: str, contract_size: float, vol_idx: int = 1):
        self.symbol = symbol
        self.contract_size = contract_size
        self.vol_idx = vol_idx
        self.bids: dict[float, float] = {}
        self.asks: dict[float, float] = {}
        self.version = 0
        self.synced = False
        self.pending: dict[int, dict] = {}
        self.gap_since: float | None = None
        self.best_bid = 0.0
        self.best_ask = 0.0
        self.last_update_ts = 0
        self.snapshot_at = 0.0
        self.needs_snapshot = True
        self.needs_resync = False
        self.applied = self.stale = self.gaps = self.resyncs = self.snapshots = 0

    # ---------- утилиты ----------
    @staticmethod
    def _range(d: dict) -> tuple[int, int]:
        v = int(d.get("version") or 0)
        b = int(d.get("begin") or v)
        e = int(d.get("end") or v)
        return b, max(e, v)

    @property
    def mid(self) -> float:
        if self.best_bid > 0 and self.best_ask > 0:
            return 0.5 * (self.best_bid + self.best_ask)
        return 0.0

    @property
    def spread_pct(self) -> float | None:
        m = self.mid
        return (self.best_ask - self.best_bid) / m * 100.0 if m > 0 else None

    def usd(self, price: float, contracts: float) -> float:
        return contracts * self.contract_size * price

    def _apply_levels(self, levels, side: dict[float, float], is_bid: bool) -> None:
        vi = self.vol_idx
        best = self.best_bid if is_bid else self.best_ask
        recompute = False
        for lv in levels or ():
            try:
                p = float(lv[0])
                v = float(lv[vi])
            except (IndexError, TypeError, ValueError):
                continue
            if v <= 0:
                if side.pop(p, None) is not None and p == best:
                    recompute = True
            else:
                side[p] = v
                if best <= 0 or (p > best if is_bid else p < best):
                    best = p
        if recompute:
            best = (max(side) if is_bid else min(side)) if side else 0.0
        if is_bid:
            self.best_bid = best
        else:
            self.best_ask = best

    def _apply(self, d: dict) -> None:
        self._apply_levels(d.get("bids"), self.bids, True)
        self._apply_levels(d.get("asks"), self.asks, False)
        if self.best_bid and self.best_ask and self.best_bid >= self.best_ask:
            # Скрещенный стакан — признак рассинхронизации (или остатков после прунинга).
            self._uncross()
        cts = d.get("cts") or d.get("timestamp")
        if cts:
            self.last_update_ts = int(cts)
        self.applied += 1

    def _uncross(self) -> None:
        """Удаляем «застрявшие» уровни за лучшей ценой противоположной стороны, переснимем позже."""
        bb, ba = self.best_bid, self.best_ask
        for p in [p for p in self.bids if p >= ba]:
            del self.bids[p]
        self.best_bid = max(self.bids) if self.bids else 0.0
        if self.best_bid >= ba:
            for p in [p for p in self.asks if p <= bb]:
                del self.asks[p]
            self.best_ask = min(self.asks) if self.asks else 0.0
        self.needs_snapshot = True

    # ---------- снапшот ----------
    def apply_snapshot(self, snap: dict) -> None:
        self.bids.clear()
        self.asks.clear()
        self.best_bid = self.best_ask = 0.0
        self._apply_levels(snap.get("bids"), self.bids, True)
        self._apply_levels(snap.get("asks"), self.asks, False)
        self.version = int(snap.get("version") or 0)
        self.synced = True
        self.needs_snapshot = False
        self.needs_resync = False
        self.gap_since = None
        self.snapshot_at = time.monotonic()
        self.snapshots += 1
        ts = snap.get("timestamp")
        if ts:
            self.last_update_ts = int(ts)
        self._drain_pending()

    # ---------- инкрементальные обновления ----------
    def on_update(self, d: dict, now: float | None = None) -> str:
        """Возвращает: applied | stale | buffered | gap | resync."""
        b, e = self._range(d)
        if not self.synced:
            self._buffer(b, d)
            return "buffered"
        if e <= self.version:
            self.stale += 1
            return "stale"
        if b <= self.version + 1:
            self._apply(d)
            self.version = e
            self._drain_pending()
            return "applied"
        self._buffer(b, d)
        self.gaps += 1
        now = time.monotonic() if now is None else now
        if self.gap_since is None:
            self.gap_since = now
        elif now - self.gap_since > GAP_RESYNC_S:
            self.needs_resync = True
            return "resync"
        return "gap"

    def _buffer(self, b: int, d: dict) -> None:
        if len(self.pending) >= MAX_PENDING:
            self.pending.pop(min(self.pending))
        self.pending[b] = d

    def _drain_pending(self) -> None:
        if not self.pending:
            self.gap_since = None
            return
        progressed = True
        while progressed and self.pending:
            progressed = False
            for b in sorted(self.pending):
                d = self.pending[b]
                _, e = self._range(d)
                if e <= self.version:
                    del self.pending[b]
                    progressed = True
                elif b <= self.version + 1:
                    del self.pending[b]
                    self._apply(d)
                    self.version = e
                    progressed = True
                else:
                    break
        self.gap_since = None if not self.pending else (self.gap_since or time.monotonic())

    def apply_commits(self, commits: list[dict]) -> bool:
        """Resync по depth_commits (по возрастанию версий). False → нужен полный снапшот."""
        self.resyncs += 1
        todo = [c for c in commits if int(c.get("version") or 0) > self.version]
        if not todo:
            self._drain_pending()
            ok = not self.pending
            if ok:
                self.needs_resync = False
            return ok
        if int(todo[0].get("version") or 0) != self.version + 1:
            return False
        for c in todo:
            v = int(c.get("version") or 0)
            if v != self.version + 1:
                return False
            self._apply(c)
            self.version = v
        self._drain_pending()
        if self.pending:
            return False
        self.needs_resync = False
        return True

    def due_resnapshot(self, now: float | None = None) -> bool:
        now = time.monotonic() if now is None else now
        return self.needs_snapshot or not self.synced or now - self.snapshot_at > RESNAPSHOT_S

    def invalidate(self) -> None:
        """После переподключения WS: ждём новый снапшот, копим обновления."""
        self.synced = False
        self.needs_snapshot = True
        self.pending.clear()
        self.gap_since = None

    # ---------- выборки ----------
    def prune(self, keep_pct: float) -> int:
        m = self.mid
        if m <= 0:
            return 0
        lo = m * (1 - keep_pct / 100.0)
        hi = m * (1 + keep_pct / 100.0)
        n = 0
        for p in [p for p in self.bids if p < lo]:
            del self.bids[p]
            n += 1
        for p in [p for p in self.asks if p > hi]:
            del self.asks[p]
            n += 1
        return n

    def top(self, is_bid: bool, n: int) -> list[tuple[float, float]]:
        """n ближайших к середине уровней: [(цена, контракты)]."""
        side = self.bids if is_bid else self.asks
        keys = heapq.nlargest(n, side) if is_bid else heapq.nsmallest(n, side)
        return [(p, side[p]) for p in keys]

    def level(self, is_bid: bool, price: float) -> float:
        return (self.bids if is_bid else self.asks).get(price, 0.0)

    def levels_within(self, is_bid: bool, max_dist_pct: float) -> list[tuple[float, float]]:
        m = self.mid
        if m <= 0:
            return []
        side = self.bids if is_bid else self.asks
        if is_bid:
            lo = m * (1 - max_dist_pct / 100.0)
            return [(p, v) for p, v in side.items() if p >= lo]
        hi = m * (1 + max_dist_pct / 100.0)
        return [(p, v) for p, v in side.items() if p <= hi]

    def __len__(self) -> int:
        return len(self.bids) + len(self.asks)
