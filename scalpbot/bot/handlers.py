"""Команды Telegram. Обрабатываются только сообщения из чата владельца (TELEGRAM_CHAT_ID).

На вход — сырой объект message из getUpdates; ответы идут через общую очередь отправки (≥ 1.1 с).
"""
from __future__ import annotations

import html
import logging
from typing import TYPE_CHECKING, Awaitable, Callable

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

# Команды, которые делегируются методам App.cmd_*(args) -> str | None
DELEGATES = ("stats", "top", "watch", "unwatch", "set", "map", "digest", "export")


def parse_command(text: str) -> tuple[str, list[str]] | None:
    """'/watch@MyBot AGT' → ('watch', ['AGT']). Не команда → None."""
    text = (text or "").strip()
    if not text.startswith("/") or len(text) < 2:
        return None
    head, *rest = text.split(maxsplit=1)
    cmd = head[1:].split("@", 1)[0].lower()
    if not cmd:
        return None
    return cmd, (rest[0].split() if rest else [])


def is_owner(owner_id: int | None, chat_id: int | None) -> bool:
    return owner_id is not None and chat_id == owner_id


class CommandRouter:
    def __init__(self, app: "App"):
        self.app = app
        self.handled = 0
        self.ignored = 0

    @property
    def owner_id(self) -> int | None:
        return self.app.settings.TELEGRAM_CHAT_ID

    def reply(self, chat_id: int, text: str) -> None:
        sender = self.app.sender
        if sender is None:
            return
        for chunk in split_text(text):
            sender.send_text(chunk, chat_id=chat_id, reply=True)

    async def handle(self, message: dict) -> None:
        chat_id = (message.get("chat") or {}).get("id")
        parsed = parse_command(message.get("text") or "")
        if self.owner_id is None:
            # Режим настройки: на /start и /help отвечаем chat id, остальное молча игнорируем.
            if parsed and parsed[0] in ("start", "help") and chat_id is not None:
                self.reply(chat_id, SETUP_TEXT.format(chat_id=chat_id))
            return
        if not is_owner(self.owner_id, chat_id):
            self.ignored += 1
            log.debug("Игнор сообщения не от владельца", extra={"chat_id": chat_id})
            return
        if parsed is None:
            return
        cmd, args = parsed
        self.handled += 1
        try:
            text = await self._run(cmd, args)
        except Exception as e:  # noqa: BLE001
            log.exception("Ошибка команды", extra={"cmd": cmd})
            text = f"⚠️ Ошибка: {html.escape(str(e))}"
        if text:
            self.reply(chat_id, text)

    async def _run(self, cmd: str, args: list[str]) -> str | None:
        app = self.app
        if cmd in ("start", "help"):
            return HELP_TEXT
        if cmd == "status":
            return app.status_text()
        if cmd == "pause":
            app.paused = True
            await app.set_flag("paused", True)
            return "⏸ Отправка алертов приостановлена. Детекция и запись в базу продолжаются. /resume — возобновить."
        if cmd == "resume":
            app.paused = False
            await app.set_flag("paused", False)
            return "▶️ Отправка алертов возобновлена."
        if cmd == "config":
            return "<b>Настройки</b>\n<code>" + html.escape("\n".join(app.settings.describe())) + "</code>"
        if cmd in DELEGATES:
            fn: Callable[[list[str]], Awaitable[str | None]] | None = getattr(app, f"cmd_{cmd}", None)
            if fn is None:
                return "Эта команда пока недоступна."
            return await fn(args)
        return "Неизвестная команда. /help — список команд."
