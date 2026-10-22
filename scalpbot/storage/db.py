"""SQLite (aiosqlite) на томе Railway: схема, миграции, пакетная запись, ретеншн."""
from __future__ import annotations

import asyncio
import logging
import os
import time
from typing import Any, Iterable

import aiosqlite
import orjson

log = logging.getLogger("db")

HORIZONS_MIN = (1, 5, 15, 60, 240)

_OUTCOME_COLS = ", ".join(
    f"r{h} REAL, mfe{h} REAL, mae{h} REAL" for h in HORIZONS_MIN
)

MIGRATIONS: list[str] = [
    # v1 — исходная схема
    f"""
    CREATE TABLE IF NOT EXISTS settings (key TEXT PRIMARY KEY, value TEXT NOT NULL);
    CREATE TABLE IF NOT EXISTS events (
        id INTEGER PRIMARY KEY,
        ts INTEGER NOT NULL,
        symbol TEXT NOT NULL,
        kind TEXT NOT NULL,          -- 'event' (событие детектора) | 'alert'
        level TEXT NOT NULL,         -- семейство события или уровень алерта
        direction TEXT NOT NULL,     -- long | short | neutral
        strength REAL,
        price REAL,                  -- середина (mid) на момент события
        ref_price REAL,              -- последняя цена (база для форвард-доходностей)
        spread_pct REAL,
        details TEXT,
        sent INTEGER NOT NULL DEFAULT 0
    );
    CREATE INDEX IF NOT EXISTS idx_events_ts ON events(ts);
    CREATE INDEX IF NOT EXISTS idx_events_level_ts ON events(level, ts);
    CREATE TABLE IF NOT EXISTS outcomes (
        event_id INTEGER PRIMARY KEY,
        symbol TEXT NOT NULL,
        direction TEXT NOT NULL,
        ts0 INTEGER NOT NULL,
        ref_price REAL NOT NULL,
        done INTEGER NOT NULL DEFAULT 0,
        missed INTEGER NOT NULL DEFAULT 0,   -- битовая маска пропущенных горизонтов
        run_max REAL, run_min REAL,
        {_OUTCOME_COLS}
    );
    CREATE INDEX IF NOT EXISTS idx_outcomes_open ON outcomes(done);
    CREATE TABLE IF NOT EXISTS cooldowns (
        symbol TEXT NOT NULL, level TEXT NOT NULL, until_ts INTEGER NOT NULL,
        PRIMARY KEY (symbol, level)
    );
    CREATE TABLE IF NOT EXISTS dex_map (
        symbol TEXT PRIMARY KEY, chain_id TEXT, address TEXT, manual INTEGER NOT NULL DEFAULT 0,
        ts INTEGER NOT NULL
    );
    CREATE TABLE IF NOT EXISTS dex_cache (symbol TEXT PRIMARY KEY, data TEXT NOT NULL, ts INTEGER NOT NULL);
    CREATE TABLE IF NOT EXISTS daily_summary (day TEXT PRIMARY KEY, data TEXT NOT NULL);
    """,
]


def now_ms() -> int:
    return int(time.time() * 1000)


class Database:
    def __init__(self, data_dir: str, filename: str = "scalpbot.db"):
        self.data_dir = data_dir
        self.path = os.path.join(data_dir, filename)
        self.conn: aiosqlite.Connection | None = None
        self.writable = False
        self._next_id = 1
        self._event_buf: list[tuple] = []
        self._outcome_new: list[tuple] = []
        self._outcome_upd: dict[int, dict[str, Any]] = {}
        self._sent_buf: list[tuple[int, int]] = []
        self._lock = asyncio.Lock()
        self.flush_errors = 0

    # ---------- открытие ----------
    @staticmethod
    def check_writable(path: str) -> bool:
        try:
            os.makedirs(path, exist_ok=True)
            probe = os.path.join(path, ".write_probe")
            with open(probe, "w") as f:
                f.write("ok")
            os.remove(probe)
            return True
        except OSError:
            return False

    async def open(self) -> None:
        self.writable = self.check_writable(self.data_dir)
        target = self.path if self.writable else ":memory:"
        if not self.writable:
            log.error("Каталог данных недоступен для записи — SQLite в памяти, данные НЕ сохранятся",
                      extra={"data_dir": self.data_dir})
        self.conn = await aiosqlite.connect(target)
        self.conn.row_factory = aiosqlite.Row
        await self.conn.execute("PRAGMA journal_mode=WAL")
        await self.conn.execute("PRAGMA synchronous=NORMAL")
        await self.conn.execute("PRAGMA temp_store=MEMORY")
        await self.conn.execute("PRAGMA cache_size=-4000")  # ~4 МБ кэша страниц
        await self._migrate()
        async with self.conn.execute("SELECT COALESCE(MAX(id), 0) FROM events") as cur:
            row = await cur.fetchone()
            self._next_id = int(row[0]) + 1

    async def _migrate(self) -> None:
        assert self.conn
        await self.conn.execute("CREATE TABLE IF NOT EXISTS schema_version (version INTEGER NOT NULL)")
        async with self.conn.execute("SELECT MAX(version) FROM schema_version") as cur:
            row = await cur.fetchone()
        cur_v = int(row[0] or 0)
        for i, sql in enumerate(MIGRATIONS, start=1):
            if i > cur_v:
                await self.conn.executescript(sql)
                await self.conn.execute("INSERT INTO schema_version(version) VALUES (?)", (i,))
                log.info("Миграция схемы применена", extra={"version": i})
        await self.conn.commit()

    async def close(self) -> None:
        if self.conn:
            try:
                await self.flush()
            finally:
                await self.conn.close()
                self.conn = None

    # ---------- события и исходы (пакетно) ----------
    def new_id(self) -> int:
        i = self._next_id
        self._next_id += 1
        return i

    def add_event(self, *, id: int, ts: int, symbol: str, kind: str, level: str, direction: str,
                  strength: float | None, price: float | None, ref_price: float | None,
                  spread_pct: float | None, details: dict, sent: bool) -> None:
        self._event_buf.append((id, ts, symbol, kind, level, direction, strength, price, ref_price,
                                spread_pct, orjson.dumps(details, default=str).decode(), int(sent)))

    def mark_sent(self, event_id: int, sent: bool = True) -> None:
        # Применяется после вставки событий в том же flush, поэтому порядок корректен.
        self._sent_buf.append((int(sent), event_id))

    def add_outcome(self, *, event_id: int, symbol: str, direction: str, ts0: int, ref_price: float) -> None:
        self._outcome_new.append((event_id, symbol, direction, ts0, ref_price, ref_price, ref_price))

    def update_outcome(self, event_id: int, **cols: Any) -> None:
        self._outcome_upd.setdefault(event_id, {}).update(cols)

    @property
    def pending_rows(self) -> int:
        return len(self._event_buf) + len(self._outcome_new) + len(self._outcome_upd) + len(self._sent_buf)

    async def flush(self) -> None:
        if not self.conn:
            return
        async with self._lock:
            ev, self._event_buf = self._event_buf, []
            on, self._outcome_new = self._outcome_new, []
            up, self._outcome_upd = self._outcome_upd, {}
            sent, self._sent_buf = self._sent_buf, []
            if not (ev or on or up or sent):
                return
            try:
                if ev:
                    await self.conn.executemany(
                        "INSERT OR REPLACE INTO events(id, ts, symbol, kind, level, direction, strength, price,"
                        " ref_price, spread_pct, details, sent) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)", ev)
                if on:
                    await self.conn.executemany(
                        "INSERT OR IGNORE INTO outcomes(event_id, symbol, direction, ts0, ref_price, run_max, run_min)"
                        " VALUES (?,?,?,?,?,?,?)", on)
                for eid, cols in up.items():
                    keys = list(cols)
                    await self.conn.execute(
                        f"UPDATE outcomes SET {', '.join(k + '=?' for k in keys)} WHERE event_id=?",
                        [cols[k] for k in keys] + [eid])
                if sent:
                    await self.conn.executemany("UPDATE events SET sent=? WHERE id=?", sent)
                await self.conn.commit()
            except Exception:
                self.flush_errors += 1
                log.exception("Ошибка записи пакета в SQLite")

    async def flush_loop(self, interval: float = 2.0, max_rows: int = 100) -> None:
        last = time.monotonic()
        while True:
            await asyncio.sleep(0.25)
            if self.pending_rows >= max_rows or time.monotonic() - last >= interval:
                await self.flush()
                last = time.monotonic()

    # ---------- общие запросы ----------
    async def fetchall(self, sql: str, params: Iterable[Any] = ()) -> list[aiosqlite.Row]:
        assert self.conn
        async with self.conn.execute(sql, tuple(params)) as cur:
            return list(await cur.fetchall())

    async def fetchone(self, sql: str, params: Iterable[Any] = ()) -> aiosqlite.Row | None:
        assert self.conn
        async with self.conn.execute(sql, tuple(params)) as cur:
            return await cur.fetchone()

    async def execute(self, sql: str, params: Iterable[Any] = ()) -> None:
        assert self.conn
        async with self._lock:
            await self.conn.execute(sql, tuple(params))
            await self.conn.commit()

    # ---------- настройки ----------
    async def load_settings(self) -> dict[str, str]:
        rows = await self.fetchall("SELECT key, value FROM settings")
        return {r["key"]: r["value"] for r in rows}

    async def save_setting(self, key: str, value: Any) -> None:
        await self.execute("INSERT OR REPLACE INTO settings(key, value) VALUES (?, ?)", (key, str(value)))

    # ---------- кулдауны ----------
    async def load_cooldowns(self, now: int) -> dict[tuple[str, str], int]:
        rows = await self.fetchall("SELECT symbol, level, until_ts FROM cooldowns WHERE until_ts > ?", (now,))
        return {(r["symbol"], r["level"]): int(r["until_ts"]) for r in rows}

    async def save_cooldown(self, symbol: str, level: str, until_ts: int) -> None:
        await self.execute("INSERT OR REPLACE INTO cooldowns(symbol, level, until_ts) VALUES (?,?,?)",
                           (symbol, level, until_ts))

    async def recent_sent_alert_ts(self, since_ms: int) -> list[int]:
        await self.flush()
        rows = await self.fetchall("SELECT ts FROM events WHERE kind='alert' AND sent=1 AND ts > ?", (since_ms,))
        return [int(r["ts"]) for r in rows]

    # ---------- исходы ----------
    async def load_open_outcomes(self) -> list[aiosqlite.Row]:
        return await self.fetchall("SELECT * FROM outcomes WHERE done=0 ORDER BY ts0")

    # ---------- DexScreener ----------
    async def get_dex_map(self, symbol: str) -> aiosqlite.Row | None:
        return await self.fetchone("SELECT * FROM dex_map WHERE symbol=?", (symbol,))

    async def set_dex_map(self, symbol: str, chain_id: str | None, address: str | None, manual: bool) -> None:
        await self.execute("INSERT OR REPLACE INTO dex_map(symbol, chain_id, address, manual, ts) VALUES (?,?,?,?,?)",
                           (symbol, chain_id, address, int(manual), now_ms()))

    async def get_dex_cache(self, symbol: str) -> tuple[dict, int] | None:
        r = await self.fetchone("SELECT data, ts FROM dex_cache WHERE symbol=?", (symbol,))
        return (orjson.loads(r["data"]), int(r["ts"])) if r else None

    async def set_dex_cache(self, symbol: str, data: dict) -> None:
        await self.execute("INSERT OR REPLACE INTO dex_cache(symbol, data, ts) VALUES (?,?,?)",
                           (symbol, orjson.dumps(data).decode(), now_ms()))

    # ---------- обслуживание ----------
    def size_mb(self) -> float:
        total = 0
        for suffix in ("", "-wal", "-shm"):
            try:
                total += os.path.getsize(self.path + suffix)
            except OSError:
                pass
        return total / 1e6

    async def retention(self, keep_days: int = 60) -> int:
        await self.flush()
        cutoff = now_ms() - keep_days * 86_400_000
        assert self.conn
        async with self._lock:
            cur = await self.conn.execute("DELETE FROM events WHERE ts < ?", (cutoff,))
            n = cur.rowcount
            await self.conn.execute("DELETE FROM outcomes WHERE ts0 < ?", (cutoff,))
            await self.conn.execute("DELETE FROM cooldowns WHERE until_ts < ?", (now_ms(),))
            await self.conn.execute("DELETE FROM dex_cache WHERE ts < ?", (now_ms() - 86_400_000,))
            await self.conn.commit()
        return n

    async def vacuum(self) -> None:
        await self.flush()
        assert self.conn
        async with self._lock:
            await self.conn.execute("VACUUM")
