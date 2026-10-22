"""Token bucket для всех REST-клиентов и Telegram."""
from __future__ import annotations

import asyncio
import time
from typing import Callable


class TokenBucket:
    """`rate` токенов в секунду, ёмкость `capacity`. Часы можно подменить в тестах."""

    __slots__ = ("rate", "capacity", "tokens", "updated", "clock", "_lock", "waits")

    def __init__(self, rate: float, capacity: float, clock: Callable[[], float] = time.monotonic):
        if rate <= 0 or capacity <= 0:
            raise ValueError("rate и capacity должны быть > 0")
        self.rate = rate
        self.capacity = capacity
        self.tokens = capacity
        self.clock = clock
        self.updated = clock()
        self._lock = asyncio.Lock()
        self.waits = 0

    @classmethod
    def per(cls, n: float, seconds: float, burst: float | None = None, **kw) -> "TokenBucket":
        """n запросов за `seconds` секунд."""
        return cls(n / seconds, burst if burst is not None else n, **kw)

    def _refill(self) -> None:
        now = self.clock()
        if now > self.updated:
            self.tokens = min(self.capacity, self.tokens + (now - self.updated) * self.rate)
            self.updated = now

    def try_acquire(self, n: float = 1.0) -> bool:
        self._refill()
        if self.tokens >= n:
            self.tokens -= n
            return True
        return False

    def delay_for(self, n: float = 1.0) -> float:
        self._refill()
        return 0.0 if self.tokens >= n else (n - self.tokens) / self.rate

    async def acquire(self, n: float = 1.0) -> None:
        if n > self.capacity:
            raise ValueError("запрошено больше токенов, чем ёмкость")
        async with self._lock:
            while not self.try_acquire(n):
                self.waits += 1
                await asyncio.sleep(max(0.005, self.delay_for(n)))
