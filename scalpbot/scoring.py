"""Объединение событий детекторов в стадии, уровни алертов, кулдауны и часовой лимит."""
from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field

from .bus import (ABSORPTION, DENSITY, EXHAUSTION, IMPULSE, LONG, NEUTRAL, PUSH, ROBOT, SHORT, SQUEEZE,
                  DetectorEvent)
from .config import Settings

# Уровни алертов
L_IMPULSE, L_EXHAUSTION, L_SETUP, L_STRONG = "IMPULSE", "EXHAUSTION", "SETUP", "STRONG"
LEVELS = (L_IMPULSE, L_EXHAUSTION, L_SETUP, L_STRONG)
# Группа кулдауна и ранг: апгрейд SETUP → STRONG разрешён внутри кулдауна.
_COOLDOWN_GROUP = {L_IMPULSE: ("IMPULSE", 1), L_EXHAUSTION: ("EXHAUSTION", 1),
                   L_SETUP: ("STAGE", 1), L_STRONG: ("STAGE", 2)}

# Стадии схемы «плотность → роботы → толкание → вход» + контекст
STAGE_OF = {DENSITY: 1, ABSORPTION: 1, ROBOT: 2, PUSH: 3, SQUEEZE: 4, IMPULSE: 5, EXHAUSTION: 5}
STAGE_TOTAL = 5


@dataclass
class AlertDecision:
    symbol: str
    level: str
    direction: str
    ts: int
    events: list[DetectorEvent]
    score: dict[str, float] = field(default_factory=dict)
    families: list[str] = field(default_factory=list)

    @property
    def stages(self) -> int:
        return len({STAGE_OF.get(e.family, 0) for e in self.events})

    @property
    def strength(self) -> float:
        return max((e.strength for e in self.events), default=0.0)


class Scorer:
    def __init__(self, settings: Settings):
        self.s = settings
        self.window: dict[str, deque[DetectorEvent]] = {}

    def _prune(self, dq: deque[DetectorEvent], now: int) -> None:
        cutoff = now - int(self.s.STAGE_WINDOW_MIN * 60000)
        while dq and dq[0].ts < cutoff:
            dq.popleft()

    def recent(self, symbol: str, now: int) -> list[DetectorEvent]:
        dq = self.window.get(symbol)
        if not dq:
            return []
        self._prune(dq, now)
        return list(dq)

    def consensus(self, events: list[DetectorEvent]) -> tuple[str, dict[str, float], dict[str, set[str]]]:
        sums = {LONG: 0.0, SHORT: 0.0}
        fams: dict[str, set[str]] = {LONG: set(), SHORT: set()}
        spoofs = 0
        for e in events:
            if e.details.get("spoof_pulled"):
                spoofs += 1
                continue
            if e.direction in sums:
                sums[e.direction] += e.strength
                fams[e.direction].add(e.family)
        lead = LONG if sums[LONG] >= sums[SHORT] else SHORT
        other = SHORT if lead == LONG else LONG
        sums[lead] -= 0.5 * spoofs  # спуф-плотности штрафуют ведущую сторону
        if sums[lead] > 0 and sums[lead] >= 1.5 * sums[other]:
            return lead, sums, fams
        return NEUTRAL, sums, fams

    def add(self, ev: DetectorEvent) -> list[AlertDecision]:
        dq = self.window.setdefault(ev.symbol, deque(maxlen=200))
        dq.append(ev)
        self._prune(dq, ev.ts)
        out: list[AlertDecision] = []
        if ev.family == IMPULSE:
            if ev.strength >= self.s.IMPULSE_ALERT_MIN_STRENGTH and not ev.details.get("beta_btc"):
                out.append(AlertDecision(ev.symbol, L_IMPULSE, ev.direction, ev.ts, [ev]))
            return out
        if ev.family == EXHAUSTION:
            out.append(AlertDecision(ev.symbol, L_EXHAUSTION, ev.direction, ev.ts, [ev]))
        direction, sums, fams = self.consensus(list(dq))
        if direction == NEUTRAL:
            return out
        agreeing = fams[direction]
        level = None
        if len(agreeing) >= 3 and (ROBOT in agreeing or ABSORPTION in agreeing):
            level = L_STRONG
        elif len(agreeing) >= 2:
            level = L_SETUP
        if level:
            evs = [e for e in dq if e.direction == direction or e.family in (IMPULSE,)]
            out.append(AlertDecision(ev.symbol, level, direction, ev.ts, evs, sums, sorted(agreeing)))
        return out

    def forget(self, symbol: str) -> None:
        self.window.pop(symbol, None)


class AlertGate:
    """Кулдауны, часовой лимит, фильтры оборота/спреда, пауза."""

    def __init__(self, settings: Settings):
        self.s = settings
        self.cooldowns: dict[tuple[str, str], tuple[int, int]] = {}  # (symbol, group) -> (until, rank)
        self.sent_ts: deque[int] = deque(maxlen=1000)
        self.overflow: list[AlertDecision] = []

    def load(self, cooldowns: dict[tuple[str, str], int], sent_ts: list[int]) -> None:
        for (sym, level), until in cooldowns.items():
            group, rank = _COOLDOWN_GROUP.get(level, (level, 1))
            cur = self.cooldowns.get((sym, group))
            if cur is None or until > cur[0] or rank > cur[1]:
                self.cooldowns[(sym, group)] = (until, rank)
        for t in sorted(sent_ts):
            self.sent_ts.append(t)

    def sent_last_hour(self, now: int) -> int:
        while self.sent_ts and self.sent_ts[0] <= now - 3_600_000:
            self.sent_ts.popleft()
        return len(self.sent_ts)

    def in_cooldown(self, symbol: str, level: str, now: int) -> bool:
        group, rank = _COOLDOWN_GROUP.get(level, (level, 1))
        cur = self.cooldowns.get((symbol, group))
        return bool(cur and cur[0] > now and cur[1] >= rank)

    def check(self, d: AlertDecision, now: int, amount24: float, spread_pct: float | None) -> str | None:
        """None — можно отправлять; иначе причина отказа (алерт всё равно логируется)."""
        if amount24 < self.s.MIN_TURNOVER_24H_USD:
            return "turnover"
        if d.level in (L_SETUP, L_STRONG) and self.s.MAX_SPREAD_PCT > 0 and spread_pct is not None \
                and spread_pct > self.s.MAX_SPREAD_PCT:
            return "spread"
        if self.in_cooldown(d.symbol, d.level, now):
            return "cooldown"
        return None

    def set_cooldown(self, d: AlertDecision, now: int) -> int:
        group, rank = _COOLDOWN_GROUP.get(d.level, (d.level, 1))
        until = now + int(self.s.COOLDOWN_MIN * 60000)
        self.cooldowns[(d.symbol, group)] = (until, rank)
        if len(self.cooldowns) > 5000:
            self.cooldowns = {k: v for k, v in self.cooldowns.items() if v[0] > now}
        return until

    def over_cap(self, now: int) -> bool:
        return self.sent_last_hour(now) >= self.s.MAX_ALERTS_PER_HOUR

    def record_sent(self, now: int) -> None:
        self.sent_ts.append(now)
