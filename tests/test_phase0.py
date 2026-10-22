import asyncio
import json
import logging

import pytest

from scalpbot.alerts.sender import OutMsg, TelegramSender, split_text, truncate
from scalpbot.config import ConfigError, Settings, TUNABLES
from scalpbot.logging_setup import JsonFormatter, RateLimitFilter, register_secret
from scalpbot.ratelimit import TokenBucket
from scalpbot.storage.db import Database


class FakeClock:
    def __init__(self):
        self.t = 0.0

    def __call__(self):
        return self.t


# ---------- config ----------
def test_settings_defaults_and_env_override():
    s = Settings(env={"IMPULSE_Z": "5.5", "TIER2_MAX": "10", "TELEGRAM_CHAT_ID": ""})
    assert s.IMPULSE_Z == 5.5 and s.TIER2_MAX == 10
    assert s.COST_ROUND_TRIP_PCT == 0.15
    assert s.setup_mode is True


def test_settings_env_out_of_range_rejected():
    with pytest.raises(ConfigError):
        Settings(env={"IMPULSE_Z": "999"})


def test_set_runtime_validates_and_reports_old_new(settings):
    old, new = settings.set_runtime("impulse_z", "6")
    assert (old, new) == (4.0, 6.0)
    with pytest.raises(ConfigError):
        settings.set_runtime("IMPULSE_Z", "abc")
    with pytest.raises(ConfigError):
        settings.set_runtime("TELEGRAM_BOT_TOKEN", "x")
    with pytest.raises(ConfigError):
        settings.set_runtime("TIER2_MAX", "0")


def test_data_dir_falls_back_to_railway_volume():
    s = Settings(env={"RAILWAY_VOLUME_MOUNT_PATH": "/data"})
    assert s.DATA_DIR == "/data"
    assert Settings(env={}).DATA_DIR == "./data"


def test_all_tunables_defaults_within_range():
    for t in TUNABLES.values():
        assert t.min <= t.default <= t.max, t.name


def test_mexc_link_template(settings):
    assert settings.mexc_link("AGT_USDT") == "https://futures.mexc.com/exchange/AGT_USDT"


# ---------- logging ----------
def test_json_log_redacts_secret():
    register_secret("123456:SECRETTOKEN")
    rec = logging.LogRecord("x", logging.INFO, "", 0, "token=123456:SECRETTOKEN", None, None)
    out = json.loads(JsonFormatter().format(rec))
    assert "SECRETTOKEN" not in out["msg"]


def test_rate_limit_filter_suppresses_repeats():
    f = RateLimitFilter(burst=3, period=60)
    recs = [logging.LogRecord("x", logging.WARNING, "", 0, "same", None, None) for _ in range(10)]
    assert sum(f.filter(r) for r in recs) == 3


# ---------- rate limiter ----------
async def test_token_bucket_rate():
    clk = FakeClock()
    b = TokenBucket.per(20, 2.0, clock=clk)
    assert all(b.try_acquire() for _ in range(20))
    assert not b.try_acquire()
    clk.t += 0.1  # 10/с → +1 токен
    assert b.try_acquire()
    assert not b.try_acquire()
    assert abs(b.delay_for() - 0.1) < 1e-9


async def test_token_bucket_contract_detail_1_per_5s():
    clk = FakeClock()
    b = TokenBucket.per(1, 5.0, clock=clk)
    assert b.try_acquire()
    clk.t += 4.9
    assert not b.try_acquire()
    clk.t += 0.1
    assert b.try_acquire()


async def test_token_bucket_acquire_waits():
    b = TokenBucket(rate=100.0, capacity=1.0)
    await b.acquire()
    loop = asyncio.get_running_loop()
    t0 = loop.time()
    await b.acquire()
    assert loop.time() - t0 >= 0.005


# ---------- sender ----------
def test_truncate_and_split():
    assert len(truncate("а" * 5000)) == 4096
    parts = split_text("\n".join(["строка " * 50] * 40))
    assert all(len(p) <= 4096 for p in parts) and len(parts) > 1


async def test_sender_dry_run_and_min_interval(capsys):
    s = TelegramSender(None, None, dry_run=True, min_interval=0.05)
    results = []
    for i in range(3):
        s.send_text(f"msg {i}", on_result=results.append)
    task = asyncio.create_task(s.run())
    loop = asyncio.get_running_loop()
    t0 = loop.time()
    while len(results) < 3:
        await asyncio.sleep(0.01)
    task.cancel()
    assert results == [True, True, True]
    assert loop.time() - t0 >= 0.09  # 2 интервала по 0.05
    assert "msg 2" in capsys.readouterr().out


async def test_sender_handles_retry_after_and_network():
    from aiogram.exceptions import TelegramNetworkError, TelegramRetryAfter
    from aiogram.methods import SendMessage

    calls = []

    class Bot:
        async def send_message(self, chat_id, text):
            calls.append(text)
            if len(calls) == 1:
                raise TelegramRetryAfter(method=SendMessage(chat_id=1, text="x"), message="flood", retry_after=3)
            if len(calls) == 2:
                raise TelegramNetworkError(method=SendMessage(chat_id=1, text="x"), message="net")

    slept = []

    async def fake_sleep(d):
        slept.append(d)

    s = TelegramSender(Bot(), 1, dry_run=False)
    s.sleep = fake_sleep
    assert await s.deliver(OutMsg(text="hi")) is True
    assert len(calls) == 3 and slept[0] >= 3


async def test_sender_without_chat_does_not_send():
    s = TelegramSender(object(), None, dry_run=False)
    got = []
    assert s.send_text("x", on_result=got.append) is False and got == [False]


# ---------- db ----------
async def test_db_migrations_events_outcomes(tmp_path):
    db = Database(str(tmp_path))
    await db.open()
    assert db.writable
    eid = db.new_id()
    db.add_event(id=eid, ts=1000, symbol="AGT_USDT", kind="alert", level="IMPULSE", direction="neutral",
                 strength=0.7, price=1.0, ref_price=1.0, spread_pct=0.05, details={"a": 1}, sent=False)
    db.add_outcome(event_id=eid, symbol="AGT_USDT", direction="neutral", ts0=1000, ref_price=1.0)
    db.update_outcome(eid, r1=0.5, mfe1=0.7)
    db.mark_sent(eid)
    await db.flush()
    row = await db.fetchone("SELECT * FROM events WHERE id=?", (eid,))
    assert row["sent"] == 1 and json.loads(row["details"]) == {"a": 1}
    o = await db.fetchone("SELECT * FROM outcomes WHERE event_id=?", (eid,))
    assert o["r1"] == 0.5 and o["done"] == 0
    await db.save_setting("set:IMPULSE_Z", 6)
    await db.save_cooldown("AGT_USDT", "IMPULSE", 10**15)
    await db.close()
    # повторное открытие: миграции не применяются второй раз, id продолжается
    db2 = Database(str(tmp_path))
    await db2.open()
    assert db2.new_id() == eid + 1
    assert (await db2.load_settings())["set:IMPULSE_Z"] == "6"
    assert ("AGT_USDT", "IMPULSE") in await db2.load_cooldowns(0)
    v = await db2.fetchall("SELECT version FROM schema_version")
    assert len(v) == 1
    await db2.close()


async def test_db_unwritable_dir_falls_back_to_memory(tmp_path):
    f = tmp_path / "file"
    f.write_text("x")
    db = Database(str(f / "sub"))  # путь внутри файла — недоступен
    await db.open()
    assert db.writable is False
    await db.close()
