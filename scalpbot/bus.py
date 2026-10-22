"""Типизированные события и ограниченные очереди (при переполнении выбрасывается самое старое)."""
from __future__ import annotations

import asyncio
from collections import deque
from dataclasses import dataclass, field
from typing import Any, Generic, TypeVar

T = TypeVar("T")

# Семейства событий детекторов
IMPULSE = "IMPULSE"
EXHAUSTION = "EXHAUSTION"
DENSITY = "DENSITY"
ROBOT = "ROBOT"
ABSORPTION = "ABSORPTION"
PUSH = "PUSH"
SQUEEZE = "SQUEEZE"
FAMILIES = (IMPULSE, EXHAUSTION, DENSITY, ROBOT, ABSORPTION, PUSH, SQUEEZE)

LONG, SHORT, NEUTRAL = "long", "short", "neutral"


@dataclass(slots=True)
class DetectorEvent:
    symbol: str
    family: str
    direction: str            # long | short | neutral — торговый уклон события
    strength: float           # 0..1
    ts: int                   # биржевое время, мс
    details: dict[str, Any] = field(default_factory=dict)

    @property
    def tag(self) -> str:
        return str(self.details.get("tag", ""))


@dataclass(slots=True)
class MarketMsg:
    """Сообщение из WebSocket: канал, символ, данные, ts биржи."""
    channel: str
    symbol: str | None
    data: Any
    ts: int


class DropQueue(Generic[T]):
    """Очередь фиксированной длины: put никогда не блокирует, при переполнении теряется самое старое."""

    def __init__(self, maxlen: int, name: str = ""):
        self.name = name
        self._q: deque[T] = deque(maxlen=maxlen)
        self._ev = asyncio.Event()
        self.dropped = 0

    def put_nowait(self, item: T) -> None:
        if len(self._q) == self._q.maxlen:
            self.dropped += 1
        self._q.append(item)
        self._ev.set()

    async def get(self) -> T:
        while not self._q:
            self._ev.clear()
            await self._ev.wait()
        return self._q.popleft()

    def get_nowait(self) -> T | None:
        return self._q.popleft() if self._q else None

    def __len__(self) -> int:
        return len(self._q)
