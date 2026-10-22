import asyncio
import types

import pytest

from helpers import Feed, make_settings, make_universe
from scalpbot.app import App
from scalpbot.bot.handlers import OwnerOnly
from scalpbot.engine import Engine
from scalpbot.market.tier1 import Tier1


def _app(tmp_path, **env):
    env = {"DRY_RUN": "1", "TELEGRAM_CHAT_ID": "42", "DATA_DIR": str(tmp_path), **{k: str(v) for k, v in env.items()}}
    from scalpbot.config import Settings
    return App(Settings(env=env))


async def _start_core(app):
    """Только БД и отправитель (без бирж)."""
    from scalpbot.alerts.sender import TelegramSender
    await app.db.open()
    app.sender = TelegramSender(None, app.settings.TELEGRAM_CHAT_ID, True)


# ---------- /set ----------
async def test_set_validates_persists_and_survives_restart(tmp_path):
    app = await _make(tmp_path)
    reply = await app.cmd_set(["IMPULSE_Z", "5.5"])
    assert "4.0 → 5.5" in reply
    assert "вне диапазона" in await app.cmd_set(["IMPULSE_Z", "100"])
    assert "Неизвестный" in await app.cmd_set(["TELEGRAM_BOT_TOKEN", "x"])
    assert "Использование" in await app.cmd_set([])
    await app.db.close()
    # «перезапуск»
    app2 = _app(tmp_path)
    await app2.db.open()
    persisted = {k[4:]: v for k, v in (await app2.db.load_settings()).items() if k.startswith("set:")}
    app2.settings.apply_persisted(persisted)
    assert app2.settings.IMPULSE_Z == 5.5
    await app2.db.close()


async def _make(tmp_path, **env):
    app = _app(tmp_path, **env)
    await _start_core(app)
    return app


async def test_digest_toggle_and_config(tmp_path):
    app = await _make(tmp_path)
    assert "выключен" in await app.cmd_digest(["off"])
    assert app.digest_enabled is False
    assert (await app.db.load_settings())["flag:digest"] == "0"
    lines = app.settings.describe()
    assert any(l.startswith("IMPULSE_Z=") for l in lines) and not any("SECRET" in l for l in lines)
    await app.db.close()


# ---------- сторож памяти ----------
async def test_memory_watchdog_actions(tmp_path):
    app = await _make(tmp_path, MEMORY_LIMIT_MB=400, TIER2_MAX=15)
    eng = Engine(app)
    u = make_universe({f"S{i}_USDT": 1.0 for i in range(12)})
    eng.universe = u
    eng.tier1 = Tier1(u)
    eng.tier2._subscribe = lambda *a: None
    eng.tier2._unsubscribe = lambda *a: None
    for i in range(10):
        eng.tier2.promote(u.get(f"S{i}_USDT"), "x", 0, manual=(i == 0))
    assert eng.memory_step(330, 0) == "high"                 # > 80%
    assert eng.tier2.capacity == 12
    assert eng.memory_step(390, 10) == "critical"            # > 95%
    assert len(eng.tier2) == 1 and "S0_USDT" in eng.tier2    # ручной остаётся
    assert eng.tier2.capacity == 5
    assert eng.memory_step(100, 20) == "calm"
    assert eng.memory_step(100, 700) == "relaxed" and eng.tier2.capacity == 8
    # предупреждение отправлено один раз за час
    assert app.sender.queue.qsize() == 1
    await app.db.close()


# ---------- безопасность: только владелец ----------
async def test_owner_only_middleware_ignores_strangers():
    mw = OwnerOnly(42)
    called = []

    async def handler(event, data):
        called.append(event.chat.id)
        return "ok"

    stranger = types.SimpleNamespace(chat=types.SimpleNamespace(id=7), text="/status")
    owner = types.SimpleNamespace(chat=types.SimpleNamespace(id=42), text="/status")
    assert await mw(handler, stranger, {}) is None
    assert await mw(handler, owner, {}) == "ok"
    assert called == [42]


async def test_setup_mode_replies_chat_id_only_to_start():
    mw = OwnerOnly(None)
    answers = []

    class Msg:
        def __init__(self, text):
            self.chat = types.SimpleNamespace(id=555)
            self.text = text

        async def answer(self, text):
            answers.append(text)

    async def handler(event, data):
        raise AssertionError("в режиме настройки команды не выполняются")

    await mw(handler, Msg("/start"), {})
    await mw(handler, Msg("/status"), {})
    assert len(answers) == 1 and "555" in answers[0] and "TELEGRAM_CHAT_ID" in answers[0]


# ---------- сквозной сценарий: импульс → алерт → запись → трекер ----------
async def test_end_to_end_impulse_alert_logged_and_tracked(tmp_path, capsys):
    app = await _make(tmp_path)
    eng = Engine(app)
    u = make_universe()
    eng.universe = u
    eng.tier1 = Tier1(u)
    eng.impulse.tier1 = eng.tier1
    eng.tier2._subscribe = lambda *a: None
    eng.tier2._unsubscribe = lambda *a: None
    app.engine = eng
    f = Feed(eng.tier1)
    f.noise(31, {"BTC_USDT": 80000.0, "AGT_USDT": 0.04})
    base = f.prices["AGT_USDT"]
    for k in range(1, 16):
        f.step({"AGT_USDT": base * (1 - 0.003 * k)})
        eng.on_tier1_frame(f.ts)
    await asyncio.sleep(0.05)
    assert eng.alerts_total == 1 and "AGT_USDT" in eng.tier2
    sender_task = asyncio.create_task(app.sender.run())
    await asyncio.sleep(0.2)
    sender_task.cancel()
    out = capsys.readouterr().out
    assert "ИМПУЛЬС вниз" in out and "AGT_USDT" in out and "Не торговый совет" in out
    await app.db.flush()
    rows = await app.db.fetchall("SELECT kind, level, sent FROM events ORDER BY id")
    assert [(r["kind"], r["level"]) for r in rows] == [("event", "IMPULSE"), ("alert", "IMPULSE")]
    assert rows[1]["sent"] == 1
    assert eng.tracker.count == 2
    # повторный импульс в кулдауне не отправляется
    for k in range(16, 40):
        f.step({"AGT_USDT": base * (1 - 0.003 * 15) * (1 - 0.004 * (k - 15))})
        eng.on_tier1_frame(f.ts)
    assert eng.alerts_total == 1
    await app.db.close()
