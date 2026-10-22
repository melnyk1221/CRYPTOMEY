"""/stats, строка истории в алертах, дневной отчёт и CSV-экспорт."""
from __future__ import annotations

import csv
import io
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone

import orjson

from ..bus import LONG, SHORT
from ..storage.db import HORIZONS_MIN, Database
from ..market.stats import mean, median

MIN_RELIABLE_N = 30


def signed_sign(direction: str, move_sign: int) -> int:
    """+1/−1 для направленных; для нейтральных импульсов — знак движения (+ = продолжение)."""
    if direction == LONG:
        return 1
    if direction == SHORT:
        return -1
    return int(move_sign or 0)


@dataclass
class GroupStats:
    kind: str
    level: str
    n: int = 0
    neutral: bool = False
    net: dict[int, list[float]] = field(default_factory=lambda: {5: [], 15: [], 60: []})
    raw_abs15: list[float] = field(default_factory=list)
    mfe15: list[float] = field(default_factory=list)
    mae15: list[float] = field(default_factory=list)

    def share_pos(self, h: int) -> float | None:
        xs = self.net[h]
        return (sum(1 for x in xs if x > 0) / len(xs) * 100.0) if xs else None


def _pct(x: float | None, signed: bool = True, nd: int = 2) -> str:
    if x is None or x != x:
        return "—"
    return f"{x:+.{nd}f}%" if signed else f"{x:.{nd}f}%"


async def compute_stats(db: Database, days: float, cost_pct: float, flt: str | None = None,
                        now_ms: int | None = None) -> list[GroupStats]:
    await db.flush()
    since = (now_ms or int(time.time() * 1000)) - int(days * 86_400_000)
    rows = await db.fetchall(
        "SELECT e.kind, e.level, e.direction, o.move_sign, o.r5, o.r15, o.r60, o.mfe15, o.mae15 "
        "FROM events e JOIN outcomes o ON o.event_id = e.id WHERE e.ts >= ?", (since,))
    groups: dict[tuple[str, str], GroupStats] = {}
    f = flt.upper() if flt else None
    for r in rows:
        if f and r["level"].upper() != f:
            continue
        key = (r["kind"], r["level"])
        g = groups.get(key)
        if g is None:
            g = groups[key] = GroupStats(r["kind"], r["level"])
        g.n += 1
        sg = signed_sign(r["direction"], r["move_sign"])
        if sg == 0:
            g.neutral = True
            if r["r15"] is not None:
                g.raw_abs15.append(abs(r["r15"]))
            continue
        for h in (5, 15, 60):
            v = r[f"r{h}"]
            if v is not None:
                g.net[h].append(sg * v - cost_pct)
        if r["mfe15"] is not None and r["mae15"] is not None:
            mfe, mae = r["mfe15"], r["mae15"]
            if r["direction"] not in (LONG, SHORT) and sg < 0:
                mfe, mae = -mae, -mfe  # нейтральный дамп: благоприятно = вниз
            g.mfe15.append(mfe)
            g.mae15.append(mae)
    order = {"alert": 0, "event": 1}
    return sorted(groups.values(), key=lambda g: (order.get(g.kind, 2), g.level))


def format_stats(groups: list[GroupStats], days: float, cost_pct: float) -> str:
    if not groups:
        return f"📊 За {days:g} дн. данных пока нет. Статистика появится, когда пройдут горизонты 5/15/60 мин."
    lines = [f"📊 <b>Статистика за {days:g} дн.</b> (издержки {cost_pct:.2f}% на круг учтены)"]
    cur_kind = None
    for g in groups:
        if g.kind != cur_kind:
            cur_kind = g.kind
            lines.append("")
            lines.append("<b>Алерты</b>" if g.kind == "alert" else "<b>События детекторов</b> (включая неотправленные)")
        n15 = len(g.net[15])
        lines.append(f"<b>{g.level}</b> · n={g.n} (с исходом 15м: {n15 or len(g.raw_abs15)})")
        if n15:
            lines.append(f"  в плюс: 5м {_pct(g.share_pos(5), False, 0)} · 15м {_pct(g.share_pos(15), False, 0)}"
                         f" · 60м {_pct(g.share_pos(60), False, 0)}")
            lines.append(f"  нетто 15м: медиана {_pct(median(g.net[15]))} · среднее {_pct(mean(g.net[15]))}")
            if g.net[60]:
                lines.append(f"  нетто 60м: медиана {_pct(median(g.net[60]))}")
            if g.mfe15:
                lines.append(f"  MFE/MAE 15м: {_pct(mean(g.mfe15))} / {_pct(mean(g.mae15))}")
            if g.level == "IMPULSE":
                lines.append("  (знак «+» = цена продолжила движение импульса)")
        elif g.raw_abs15:
            lines.append(f"  нейтральные: медиана |движения| за 15м {_pct(median(g.raw_abs15), False)}")
        if max(n15, len(g.raw_abs15)) < MIN_RELIABLE_N:
            lines.append("  ⚠️ мало данных, выводы ненадёжны")
    return "\n".join(lines)


class LevelHistory:
    """Строка «История STRONG (30д): …» для алертов; кэш 10 минут."""

    def __init__(self, db: Database, ttl_s: float = 600):
        self.db = db
        self.ttl = ttl_s
        self._cache: dict[str, tuple[float, str]] = {}

    async def line(self, level: str, cost_pct: float) -> str:
        now = time.monotonic()
        c = self._cache.get(level)
        if c and now - c[0] < self.ttl:
            return c[1]
        groups = await compute_stats(self.db, 30, cost_pct, level)
        g = next((x for x in groups if x.kind == "alert"), None)
        if g is None or not g.net[15]:
            text = f"📊 История {level} (30д): статистики пока нет"
        else:
            text = (f"📊 История {level} (30д): n={len(g.net[15])} · медиана {_pct(median(g.net[15]))} за 15м "
                    f"после издержек · в плюс {_pct(g.share_pos(15), False, 0)}")
            if len(g.net[15]) < MIN_RELIABLE_N:
                text += " (мало данных)"
        self._cache[level] = (now, text)
        return text

    def invalidate(self) -> None:
        self._cache.clear()


CSV_COLS = (["id", "time_utc", "symbol", "kind", "level", "direction", "strength", "price", "ref_price",
             "spread_pct", "sent"] + [f"{p}{h}" for h in HORIZONS_MIN for p in ("r", "mfe", "mae")]
            + ["missed", "details"])


async def export_csv(db: Database, days: float, kinds: tuple[str, ...] = ("alert", "event"),
                     now_ms: int | None = None) -> tuple[bytes, int]:
    await db.flush()
    since = (now_ms or int(time.time() * 1000)) - int(days * 86_400_000)
    q = ",".join("?" for _ in kinds)
    outcome_cols = ", ".join(f"o.r{h}, o.mfe{h}, o.mae{h}" for h in HORIZONS_MIN)
    rows = await db.fetchall(
        f"SELECT e.id, e.ts, e.symbol, e.kind, e.level, e.direction, e.strength, e.price, e.ref_price, "
        f"e.spread_pct, e.sent, {outcome_cols}, o.missed, e.details FROM events e "
        f"LEFT JOIN outcomes o ON o.event_id = e.id WHERE e.ts >= ? AND e.kind IN ({q}) ORDER BY e.ts",
        (since, *kinds))
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(CSV_COLS)
    for r in rows:
        vals = list(r)
        vals[1] = datetime.fromtimestamp(vals[1] / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        w.writerow(vals)
    return buf.getvalue().encode("utf-8-sig"), len(rows)


async def daily_summary(db: Database, cost_pct: float, now_ms: int | None = None) -> dict:
    """Сводка за 24 ч: алерты по уровням, лучшие/худшие исходы."""
    await db.flush()
    now = now_ms or int(time.time() * 1000)
    since = now - 86_400_000
    rows = await db.fetchall(
        "SELECT e.symbol, e.level, e.direction, e.sent, o.move_sign, o.r60, o.r15 FROM events e "
        "LEFT JOIN outcomes o ON o.event_id = e.id WHERE e.kind='alert' AND e.ts >= ?", (since,))
    by_level: dict[str, list[int]] = {}
    scored = []
    for r in rows:
        c = by_level.setdefault(r["level"], [0, 0])
        c[0] += 1
        c[1] += int(r["sent"])
        sg = signed_sign(r["direction"], r["move_sign"] or 0)
        v = r["r60"] if r["r60"] is not None else r["r15"]
        if sg and v is not None:
            scored.append((sg * v - cost_pct, r["symbol"], r["level"]))
    n_events = await db.fetchone("SELECT COUNT(*) FROM events WHERE kind='event' AND ts >= ?", (since,))
    scored.sort()
    return {"by_level": by_level, "best": scored[-3:][::-1], "worst": scored[:3],
            "events": int(n_events[0]) if n_events else 0}


def format_daily(summary: dict, stats_text: str, rss_peak: float, outages: list[str]) -> str:
    lines = ["🗓 <b>Дневной отчёт (24 ч)</b>"]
    if summary["by_level"]:
        lines.append("Алерты: " + ", ".join(f"{lvl} {c[1]}/{c[0]}" for lvl, c in sorted(summary["by_level"].items()))
                     + " (отправлено/всего)")
    else:
        lines.append("Алертов не было.")
    lines.append(f"Событий детекторов: {summary['events']}")
    if summary["best"]:
        lines.append("Лучшие (нетто 60м): " + ", ".join(f"{s} {lvl} {_pct(v)}" for v, s, lvl in summary["best"]))
        lines.append("Худшие (нетто 60м): " + ", ".join(f"{s} {lvl} {_pct(v)}" for v, s, lvl in summary["worst"]))
    lines.append(f"Пик памяти: {rss_peak:.0f} МБ")
    lines.append("Сбои источников: " + (", ".join(outages) if outages else "не было"))
    lines.append("")
    lines.append(stats_text)
    lines.append("\nВо вложении — CSV за 7 дней (резервная копия).")
    return "\n".join(lines)


def details_json(d: dict) -> str:
    return orjson.dumps(d, default=str).decode()
