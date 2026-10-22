"""Здоровье процесса: RSS, лаг event loop, статусы и heartbeat источников."""
from __future__ import annotations

import asyncio
import gc
import logging
import os
import time
from collections import deque
from dataclasses import dataclass, field

log = logging.getLogger("health")


def rss_mb() -> float:
    """Текущий RSS в МБ. Linux: /proc/self/status; иначе psutil (если установлен)."""
    try:
        with open("/proc/self/status", "rb") as f:
            for line in f:
                if line.startswith(b"VmRSS:"):
                    return int(line.split()[1]) / 1024.0
    except OSError:
        pass
    try:
        import psutil  # type: ignore
        return psutil.Process(os.getpid()).memory_info().rss / 1e6
    except Exception:
        pass
    try:
        import resource  # type: ignore
        return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024.0
    except Exception:
        return 0.0


@dataclass
class SourceStatus:
    name: str
    title: str
    mode: str = "on"                 # on | auto | off
    state: str = "unknown"           # ok | blocked | error | disabled | unknown
    http_status: int | None = None
    latency_ms: float | None = None
    error: str = ""
    enabled: bool = True
    last_message_at: float = 0.0     # monotonic
    messages: int = 0
    outages: int = 0
    down_since: float | None = None
    warned: bool = False

    def beat(self, n: int = 1) -> None:
        self.last_message_at = time.monotonic()
        self.messages += n

    def mark(self) -> str:
        if not self.enabled:
            return "⛔"
        return {"ok": "✅", "blocked": "❌", "error": "⚠️"}.get(self.state, "❔")

    def short(self) -> str:
        s = f"{self.title} {self.mark()}"
        if self.state == "blocked" and self.http_status:
            s += f" ({self.http_status})"
        elif self.state == "error" and self.error:
            s += f" ({self.error[:40]})"
        elif not self.enabled and self.state == "disabled":
            s += " (выкл)"
        return s


@dataclass
class Health:
    memory_limit_mb: int = 450
    started_at: float = field(default_factory=time.monotonic)
    started_wall: float = field(default_factory=time.time)
    sources: dict[str, SourceStatus] = field(default_factory=dict)
    lag_samples: deque = field(default_factory=lambda: deque(maxlen=300))
    rss_peak_mb: float = 0.0
    rss_peak_day_mb: float = 0.0
    dropped_events: int = 0
    counters: dict[str, int] = field(default_factory=dict)

    def source(self, name: str, title: str | None = None, mode: str = "on") -> SourceStatus:
        st = self.sources.get(name)
        if st is None:
            st = SourceStatus(name=name, title=title or name, mode=mode)
            self.sources[name] = st
        return st

    def inc(self, key: str, n: int = 1) -> None:
        self.counters[key] = self.counters.get(key, 0) + n

    def uptime_s(self) -> float:
        return time.monotonic() - self.started_at

    def lag_p95_ms(self) -> float:
        if not self.lag_samples:
            return 0.0
        s = sorted(self.lag_samples)
        return s[min(len(s) - 1, int(0.95 * len(s)))]

    def sample_rss(self) -> float:
        r = rss_mb()
        self.rss_peak_mb = max(self.rss_peak_mb, r)
        self.rss_peak_day_mb = max(self.rss_peak_day_mb, r)
        return r

    async def lag_monitor(self, on_sustained_lag=None) -> None:
        """Спим 1 с и меряем дрейф. Предупреждаем, если p95 > 500 мс устойчиво."""
        loop = asyncio.get_running_loop()
        high_since: float | None = None
        warned_at = 0.0
        while True:
            t0 = loop.time()
            await asyncio.sleep(1.0)
            lag = max(0.0, (loop.time() - t0 - 1.0) * 1000.0)
            self.lag_samples.append(lag)
            now = time.monotonic()
            if lag > 500:
                high_since = high_since or now
                if now - high_since > 30 and now - warned_at > 3600 and on_sustained_lag:
                    warned_at = now
                    on_sustained_lag(self.lag_p95_ms())
            else:
                high_since = None


def free_memory() -> None:
    gc.collect()
