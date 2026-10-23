"""Очередь отправки в Telegram: ≥1.1 с между сообщениями, обработка 429 и сетевых ошибок."""
from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable

from ..ratelimit import TokenBucket

log = logging.getLogger("sender")

TG_MAX_LEN = 4096


def truncate(text: str, limit: int = TG_MAX_LEN) -> str:
    if len(text) <= limit:
        return text
    return text[: limit - 1] + "…"


def split_text(text: str, limit: int = TG_MAX_LEN - 96) -> list[str]:
    """Режет длинный текст по строкам на куски ≤ limit (HTML-теги внутри строк не разрываются)."""
    if not text:
        return []
    chunks, cur = [], ""
    for line in text.split("\n"):
        while len(line) > limit:
            if cur:
                chunks.append(cur)
                cur = ""
            chunks.append(line[:limit])
            line = line[limit:]
        if len(cur) + len(line) + 1 > limit:
            chunks.append(cur)
            cur = line
        else:
            cur = f"{cur}\n{line}" if cur else line
    if cur:
        chunks.append(cur)
    return chunks


@dataclass
class OutMsg:
    text: str | None = None
    document: bytes | None = None
    filename: str = ""
    caption: str | None = None
    on_result: Callable[[bool], Any] | None = None
    chat_id: int | None = None     # None → чат владельца
    reply: bool = False            # ответ на команду: уходит в Telegram и при DRY_RUN
    created: float = field(default_factory=time.monotonic)


class TelegramSender:
    """Единственная точка выхода сообщений. В DRY_RUN алерты печатаются в stdout."""

    def __init__(self, client: Any | None, chat_id: int | None, dry_run: bool,
                 min_interval: float = 1.1, max_retries: int = 5, queue_size: int = 300):
        self.client = client
        self.chat_id = chat_id
        self.dry_run = dry_run
        self.max_retries = max_retries
        self.bucket = TokenBucket(1.0 / min_interval, 1.0)
        self.queue: asyncio.Queue[OutMsg] = asyncio.Queue(maxsize=queue_size)
        self.sent = 0
        self.failed = 0
        self.dropped = 0
        self.sleep: Callable[[float], Awaitable[None]] = asyncio.sleep

    @property
    def can_deliver(self) -> bool:
        return self.dry_run or (self.client is not None and self.chat_id is not None)

    def _deliverable(self, msg: OutMsg) -> bool:
        if self.client is not None and (msg.chat_id or self.chat_id) is not None:
            return True
        return self.dry_run

    def _to_stdout(self, msg: OutMsg) -> bool:
        return self.dry_run and not (msg.reply and self.client is not None)

    def enqueue(self, msg: OutMsg) -> bool:
        if not self._deliverable(msg):
            log.info("Сообщение не отправлено: чат не настроен", extra={"text": (msg.text or msg.caption or "")[:200]})
            if msg.on_result:
                msg.on_result(False)
            return False
        try:
            self.queue.put_nowait(msg)
            return True
        except asyncio.QueueFull:
            self.dropped += 1
            log.warning("Очередь Telegram переполнена, сообщение отброшено")
            if msg.on_result:
                msg.on_result(False)
            return False

    def send_text(self, text: str, on_result: Callable[[bool], Any] | None = None,
                  chat_id: int | None = None, reply: bool = False) -> bool:
        return self.enqueue(OutMsg(text=truncate(text), on_result=on_result, chat_id=chat_id, reply=reply))

    def send_document(self, data: bytes, filename: str, caption: str | None = None) -> bool:
        return self.enqueue(OutMsg(document=data, filename=filename,
                                   caption=truncate(caption, 1024) if caption else None))

    async def run(self) -> None:
        while True:
            msg = await self.queue.get()
            # 409: бота опрашивает другой экземпляр — ничего не шлём, пока конфликт не устранён.
            while self.client is not None and getattr(self.client, "conflict", False) and not self._to_stdout(msg):
                await self.sleep(1.0)
            await self.bucket.acquire()
            ok = await self.deliver(msg)
            if msg.on_result:
                try:
                    msg.on_result(ok)
                except Exception:
                    log.exception("Ошибка в on_result")

    async def deliver(self, msg: OutMsg) -> bool:
        if self._to_stdout(msg):
            if msg.text is not None:
                print(f"\n----- [DRY_RUN] Telegram -----\n{msg.text}\n------------------------------", flush=True)
            else:
                print(f"\n----- [DRY_RUN] документ {msg.filename} ({len(msg.document or b'')} байт): "
                      f"{msg.caption or ''}", flush=True)
            self.sent += 1
            return True
        from ..bot.telegram_client import TgBadRequest, TgForbidden, TgNetworkError, TgRetryAfter

        chat_id = msg.chat_id or self.chat_id
        attempt = 0
        while True:
            try:
                if msg.text is not None:
                    await self.client.send_message(chat_id, msg.text)
                else:
                    await self.client.send_document(chat_id, msg.document or b"", msg.filename, msg.caption)
                self.sent += 1
                return True
            except TgRetryAfter as e:
                log.warning("Telegram 429, ждём", extra={"retry_after": e.retry_after})
                await self.sleep(float(e.retry_after) + 0.5)
            except (TgNetworkError, asyncio.TimeoutError, OSError) as e:
                attempt += 1
                if attempt > self.max_retries:
                    self.failed += 1
                    log.error("Telegram: сообщение не доставлено после повторов", extra={"err": str(e)})
                    return False
                await self.sleep(min(30.0, 2.0 ** attempt))
            except (TgBadRequest, TgForbidden) as e:
                self.failed += 1
                log.error("Telegram отклонил сообщение", extra={"err": str(e)})
                return False
            except Exception as e:  # noqa: BLE001 — в т.ч. TgConflict при отправке: не роняем очередь
                self.failed += 1
                log.error("Telegram: неожиданная ошибка отправки", extra={"err": str(e)[:200]})
                return False

    async def send_now(self, text: str, timeout: float = 5.0) -> bool:
        """Отправка в обход очереди (для остановки). Не бросает исключений."""
        if not self.can_deliver:
            return False
        try:
            return await asyncio.wait_for(self.deliver(OutMsg(text=truncate(text))), timeout)
        except Exception:
            return False
