"""Команды Telegram. Обрабатываются только сообщения из чата владельца (TELEGRAM_CHAT_ID)."""
from __future__ import annotations

import html
import logging
from typing import TYPE_CHECKING, Any, Awaitable, Callable

from aiogram import BaseMiddleware, Router
from aiogram.filters import Command, CommandObject
from aiogram.types import Message, TelegramObject

from ..alerts.sender import split_text

if TYPE_CHECKING:
    from ..app import App

log = logging.getLogger("bot")

HELP_TEXT = """<b>Сканер сигналов MEXC Futures</b>
Бот только присылает информацию и никогда не торгует.

<b>Команды</b>
/status — состояние: память, источники, символы tier 2
/stats [дни] [тип] — статистика сигналов после издержек
/top — топ-10 движений за 15 минут
/watch SYMBOL · /unwatch SYMBOL — принудительно следить детально
/pause · /resume — остановить/возобновить отправку алертов
/set KEY VALUE — изменить порог (см. /config)
/config — текущие настройки
/map SYMBOL CHAIN ADDRESS — привязка токена к DexScreener
/digest on|off — часовой дайджест
/export [дни] — CSV алертов и исходов
/help — эта справка"""

SETUP_TEXT = """👋 Режим настройки.
Ваш chat id: <code>{chat_id}</code>

Впишите этот id в переменную <b>TELEGRAM_CHAT_ID</b> на Railway (Variables) и перезапустите сервис (redeploy). После этого бот будет отвечать только в этот чат."""


class OwnerOnly(BaseMiddleware):
    """Молча игнорирует всё, что пришло не из чата владельца."""

    def __init__(self, owner_id: int | None):
        self.owner_id = owner_id

    async def __call__(self, handler: Callable[[TelegramObject, dict], Awaitable[Any]],
                       event: TelegramObject, data: dict) -> Any:
        chat = getattr(event, "chat", None)
        chat_id = getattr(chat, "id", None)
        if self.owner_id is None:
            # Режим настройки: отвечаем только на /start и /help с chat id.
            text = getattr(event, "text", "") or ""
            if text.startswith("/start") or text.startswith("/help"):
                await event.answer(SETUP_TEXT.format(chat_id=chat_id))  # type: ignore[attr-defined]
            return None
        if chat_id != self.owner_id:
            log.debug("Игнор сообщения не от владельца", extra={"chat_id": chat_id})
            return None
        return await handler(event, data)


def build_router(app: "App") -> Router:
    r = Router(name="owner")
    r.message.outer_middleware(OwnerOnly(app.settings.TELEGRAM_CHAT_ID))

    @r.message(Command("start", "help"))
    async def cmd_help(m: Message) -> None:
        await m.answer(HELP_TEXT)

    @r.message(Command("status"))
    async def cmd_status(m: Message) -> None:
        await m.answer(app.status_text())

    @r.message(Command("pause"))
    async def cmd_pause(m: Message) -> None:
        app.paused = True
        await app.set_flag("paused", True)
        await m.answer("⏸ Отправка алертов приостановлена. Детекция и запись в базу продолжаются. /resume — возобновить.")

    @r.message(Command("resume"))
    async def cmd_resume(m: Message) -> None:
        app.paused = False
        await app.set_flag("paused", False)
        await m.answer("▶️ Отправка алертов возобновлена.")

    @r.message(Command("config"))
    async def cmd_config(m: Message) -> None:
        lines = app.settings.describe()
        await m.answer("<b>Настройки</b>\n<code>" + html.escape("\n".join(lines)) + "</code>")

    for name, attr in (("stats", "cmd_stats"), ("top", "cmd_top"), ("watch", "cmd_watch"),
                       ("unwatch", "cmd_unwatch"), ("set", "cmd_set"), ("map", "cmd_map"),
                       ("digest", "cmd_digest"), ("export", "cmd_export")):
        _register_delegate(r, app, name, attr)
    return r


def _register_delegate(r: Router, app: "App", name: str, attr: str) -> None:
    """Команда делегируется методу App.<attr>(args) -> str | None."""

    @r.message(Command(name))
    async def _h(m: Message, command: CommandObject) -> None:
        fn = getattr(app, attr, None)
        if fn is None:
            await m.answer("Эта команда пока недоступна.")
            return
        try:
            reply = await fn((command.args or "").split())
        except Exception as e:  # noqa: BLE001
            log.exception("Ошибка команды", extra={"cmd": name})
            reply = f"⚠️ Ошибка: {html.escape(str(e))}"
        for chunk in split_text(reply or ""):
            await m.answer(chunk)

    _h.__name__ = f"cmd_{name}"
