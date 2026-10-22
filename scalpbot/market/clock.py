"""Биржевое время: последнее ts из сообщений биржи + монотонное смещение (не идёт назад)."""
from __future__ import annotations

import time


class MarketClock:
    __slots__ = ("_ts", "_mono", "_last_out")

    def __init__(self) -> None:
        self._ts = 0
        self._mono = 0.0
        self._last_out = 0

    def update(self, ts_ms: int | float | None) -> None:
        if not ts_ms:
            return
        ts = int(ts_ms)
        if ts >= self._ts:
            self._ts = ts
            self._mono = time.monotonic()

    def now(self) -> int:
        if not self._ts:
            out = int(time.time() * 1000)
        else:
            out = self._ts + int((time.monotonic() - self._mono) * 1000)
        if out < self._last_out:
            out = self._last_out
        self._last_out = out
        return out

    @property
    def synced(self) -> bool:
        return self._ts > 0


clock = MarketClock()
