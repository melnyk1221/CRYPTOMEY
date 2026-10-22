"""Лёгкие статистические примитивы без numpy: кольцевые буферы, EWMA, медианы, временные корзины."""
from __future__ import annotations

import math
from array import array
from collections import deque
from typing import Iterator


class Ring:
    """Кольцевой буфер (ts_ms, value) на массивах: 16 байт на точку вместо ~110 у deque кортежей."""

    __slots__ = ("cap", "ts", "val", "n", "head")

    def __init__(self, cap: int):
        self.cap = cap
        self.ts = array("q", [0]) * cap
        self.val = array("d", [0.0]) * cap
        self.n = 0
        self.head = 0  # индекс следующей записи

    def __len__(self) -> int:
        return self.n

    def clear(self) -> None:
        self.n = 0
        self.head = 0

    def append(self, ts: int, v: float) -> None:
        self.ts[self.head] = ts
        self.val[self.head] = v
        self.head = (self.head + 1) % self.cap
        if self.n < self.cap:
            self.n += 1

    def _phys(self, i: int) -> int:
        """Логический индекс (0 = самый старый) → физический."""
        return (self.head - self.n + i) % self.cap

    def at(self, i: int) -> tuple[int, float]:
        if i < 0:
            i += self.n
        p = self._phys(i)
        return self.ts[p], self.val[p]

    def last(self) -> tuple[int, float] | None:
        if not self.n:
            return None
        p = (self.head - 1) % self.cap
        return self.ts[p], self.val[p]

    def first(self) -> tuple[int, float] | None:
        return self.at(0) if self.n else None

    def index_at_or_before(self, t: int) -> int:
        """Последний логический индекс с ts <= t, или -1."""
        lo, hi = 0, self.n - 1
        res = -1
        while lo <= hi:
            mid = (lo + hi) >> 1
            if self.ts[self._phys(mid)] <= t:
                res = mid
                lo = mid + 1
            else:
                hi = mid - 1
        return res

    def value_at(self, t: int) -> float | None:
        """Значение последней точки с ts <= t; None, если истории не хватает."""
        i = self.index_at_or_before(t)
        return None if i < 0 else self.val[self._phys(i)]

    def iter_since(self, t: int) -> Iterator[tuple[int, float]]:
        """Точки с ts >= t, от старых к новым."""
        for k in range(self.index_at_or_before(t - 1) + 1, self.n):
            p = self._phys(k)
            yield self.ts[p], self.val[p]

    def values_since(self, t: int) -> list[float]:
        return [v for _, v in self.iter_since(t)]

    def nbytes(self) -> int:
        return self.ts.itemsize * self.cap + self.val.itemsize * self.cap


class Ewma:
    """Экспоненциальные среднее и дисперсия с периодом полураспада в отсчётах."""

    __slots__ = ("alpha", "mean", "var", "n")

    def __init__(self, halflife_samples: float):
        self.alpha = 1.0 - 0.5 ** (1.0 / halflife_samples)
        self.mean = 0.0
        self.var = 0.0
        self.n = 0

    def update(self, x: float) -> None:
        if self.n == 0:
            self.mean = x
            self.var = 0.0
        else:
            d = x - self.mean
            inc = self.alpha * d
            self.mean += inc
            self.var = (1.0 - self.alpha) * (self.var + d * inc)
        self.n += 1

    @property
    def std(self) -> float:
        return math.sqrt(self.var) if self.var > 0 else 0.0


def median(xs) -> float:
    s = sorted(xs)
    n = len(s)
    if n == 0:
        return float("nan")
    m = n // 2
    return s[m] if n % 2 else 0.5 * (s[m - 1] + s[m])


def mean(xs) -> float:
    xs = list(xs)
    return sum(xs) / len(xs) if xs else float("nan")


class RollingMedian:
    """Медиана последних maxlen значений (буферы маленькие, сортировка по запросу)."""

    __slots__ = ("buf",)

    def __init__(self, maxlen: int):
        self.buf: deque[float] = deque(maxlen=maxlen)

    def add(self, x: float) -> None:
        self.buf.append(x)

    def value(self) -> float:
        return median(self.buf)

    def __len__(self) -> int:
        return len(self.buf)


class SideBuckets:
    """Объём покупок/продаж тейкеров по 1-секундным корзинам (фиксированное кольцо)."""

    __slots__ = ("n", "buy", "sell", "sec", "last_sec")

    def __init__(self, seconds: int = 300):
        self.n = seconds
        self.buy = array("d", [0.0]) * seconds
        self.sell = array("d", [0.0]) * seconds
        self.sec = array("q", [-1]) * seconds
        self.last_sec = -1

    def add(self, ts_ms: int, side_buy: bool, usd: float) -> None:
        s = ts_ms // 1000
        i = s % self.n
        if self.sec[i] != s:
            self.sec[i] = s
            self.buy[i] = 0.0
            self.sell[i] = 0.0
        if side_buy:
            self.buy[i] += usd
        else:
            self.sell[i] += usd
        if s > self.last_sec:
            self.last_sec = s

    def window(self, now_ms: int, seconds: int) -> tuple[float, float]:
        """(покупки, продажи) за последние `seconds` секунд до now_ms включительно."""
        now_s = now_ms // 1000
        lo = now_s - seconds + 1
        b = s_ = 0.0
        for i in range(self.n):
            sec = self.sec[i]
            if lo <= sec <= now_s:
                b += self.buy[i]
                s_ += self.sell[i]
        return b, s_
