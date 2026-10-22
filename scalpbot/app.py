"""Сборка всех компонентов, надзор за задачами, корректная остановка."""
from __future__ import annotations

import asyncio
import logging
import signal
import time
from typing import Awaitable, Callable

from .alerts.sender import TelegramSender
from .config import Settings
from .health import Health
from .storage.db import Database
from .supervisor import supervise

log = logging.getLogger("app")


def fmt_uptime(sec: float) -> str:
    sec = int(sec)
    d, sec = divmod(sec, 86400)
    h, sec = divmod(sec, 3600)
    m, _ = divmod(sec, 60)
    return (f"{d}д " if d else "") + f"{h}ч {m}м"


class App:
    def __init__(self, settings: Settings):
        self.settings = settings
        self.health = Health(memory_limit_mb=settings.MEMORY_LIMIT_MB)
        self.db = Database(settings.DATA_DIR)
        self.bot = None
        self.dp = None
        self.sender: TelegramSender | None = None
        self.paused = False
        self.digest_enabled = True
        self._tasks: list[asyncio.Task] = []
        self._stop = asyncio.Event()

    # ---------- задачи ----------
    def spawn(self, name: str, factory: Callable[[], Awaitable[None]], supervised: bool = True) -> asyncio.Task:
        coro = supervise(name, factory) if supervised else factory()
        t = asyncio.create_task(coro, name=name)
        self._tasks.append(t)
        return t

    def request_stop(self) -> None:
        self._stop.set()

    def notify(self, text: str) -> None:
        """Служебное сообщение владельцу (не алерт, /pause не влияет)."""
        if self.sender:
            self.sender.send_text(text)

    # ---------- запуск ----------
    async def start(self) -> None:
        s = self.settings
        await self.db.open()
        errs = s.apply_persisted({k[4:]: v for k, v in (await self.db.load_settings()).items() if k.startswith("set:")})
        for e in errs:
            log.warning("Сохранённая настройка отклонена", extra={"err": e})
        flags = await self.db.load_settings()
        self.digest_enabled = flags.get("flag:digest", "1") == "1"
        self.paused = flags.get("flag:paused", "0") == "1"

        if s.TELEGRAM_BOT_TOKEN:
            from aiogram import Bot, Dispatcher
            from aiogram.client.default import DefaultBotProperties

            from .bot.handlers import build_router
            self.bot = Bot(s.TELEGRAM_BOT_TOKEN,
                           default=DefaultBotProperties(parse_mode="HTML", link_preview_is_disabled=True))
            self.dp = Dispatcher()
            self.dp.include_router(build_router(self))
        self.sender = TelegramSender(self.bot, s.TELEGRAM_CHAT_ID, s.DRY_RUN)
        if s.setup_mode and not s.DRY_RUN:
            log.warning("TELEGRAM_CHAT_ID пуст — режим настройки: отправьте /start боту, алерты не отправляются")

        self.spawn("db_flush", self.db.flush_loop)
        self.spawn("tg_sender", self.sender.run)
        self.spawn("loop_lag", lambda: self.health.lag_monitor(self._on_lag))
        if self.dp and self.bot:
            self.spawn("tg_polling", self._polling)
        if not self.db.writable:
            self.notify("⚠️ Данные не сохраняются: том не подключён или каталог DATA_DIR недоступен для записи.")
        await self.on_start()

    async def on_start(self) -> None:
        """Точка расширения для источников рынка (фазы 1+)."""
        self.notify("Бот запущен.")

    async def _polling(self) -> None:
        assert self.dp and self.bot
        await self.dp.start_polling(self.bot, handle_signals=False, close_bot_session=False,
                                    allowed_updates=["message"])

    def _on_lag(self, p95: float) -> None:
        self.notify(f"⚠️ Высокая задержка event loop: p95 ≈ {p95:.0f} мс. Возможно, не хватает CPU.")

    # ---------- цикл жизни ----------
    async def run(self) -> None:
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            try:
                loop.add_signal_handler(sig, self.request_stop)
            except (NotImplementedError, RuntimeError):
                pass  # Windows: остановка через KeyboardInterrupt
        try:
            await self.start()
            await self._stop.wait()
        finally:
            await self.shutdown()

    async def shutdown(self) -> None:
        log.info("Остановка…")
        t0 = time.monotonic()
        await self.on_shutdown()
        if self.sender and not self._stop_message_sent:
            self._stop_message_sent = True
            await self.sender.send_now("🛑 Бот остановлен (redeploy или остановка сервиса).", timeout=3.0)
        for t in self._tasks:
            t.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        try:
            await asyncio.wait_for(self.db.close(), timeout=max(0.5, 5.0 - (time.monotonic() - t0)))
        except Exception:
            log.exception("Ошибка закрытия БД")
        if self.bot:
            try:
                await self.bot.session.close()
            except Exception:
                pass
        log.info("Остановлено")

    _stop_message_sent = False

    async def on_shutdown(self) -> None:
        """Точка расширения: закрыть WS, сбросить состояния."""

    # ---------- /status ----------
    def status_text(self) -> str:
        h = self.health
        rss = h.sample_rss()
        lines = [
            "<b>Статус</b>",
            f"Аптайм: {fmt_uptime(h.uptime_s())}",
            f"Память: {rss:.0f} / {h.memory_limit_mb} МБ (пик {h.rss_peak_mb:.0f})",
            f"Лаг event loop p95: {h.lag_p95_ms():.0f} мс",
            f"БД: {self.db.size_mb():.1f} МБ" + ("" if self.db.writable else " ⚠️ НЕ на томе"),
            f"Алерты: {'⏸ на паузе' if self.paused else 'включены'} · дайджест {'вкл' if self.digest_enabled else 'выкл'}",
        ]
        if h.sources:
            lines.append("Источники: " + ", ".join(st.short() for st in h.sources.values()))
        lines.extend(self.status_extra())
        if self.sender:
            lines.append(f"Telegram: отправлено {self.sender.sent}, ошибок {self.sender.failed}, "
                         f"в очереди {self.sender.queue.qsize()}")
        lines.append("💰 Railway: ≈$10 за ГБ RAM в месяц + CPU; держим RSS ≈300 МБ.")
        return "\n".join(lines)

    def status_extra(self) -> list[str]:
        return []

    # ---------- флаги ----------
    async def set_flag(self, name: str, value: bool) -> None:
        await self.db.save_setting(f"flag:{name}", "1" if value else "0")
