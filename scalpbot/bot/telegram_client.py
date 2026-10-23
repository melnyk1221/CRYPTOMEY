"""Минимальный клиент Telegram Bot API на общей aiohttp-сессии (вместо aiogram: −130 МБ RSS).

Используются только getUpdates (long polling), sendMessage и sendDocument.
Ошибки API приводятся к небольшой иерархии исключений; повторы делают вызывающие
(поллер и очередь отправки), сам клиент ничего не повторяет.
"""
from __future__ import annotations

import asyncio
import logging
from typing import Any, Awaitable, Callable

import aiohttp
import orjson

from ..supervisor import backoff_delay

log = logging.getLogger("telegram")

API_BASE = "https://api.telegram.org"
POLL_TIMEOUT_S = 50
POLL_HTTP_TIMEOUT = aiohttp.ClientTimeout(total=65, connect=10)
CALL_TIMEOUT = aiohttp.ClientTimeout(total=30, connect=10)
UPLOAD_TIMEOUT = aiohttp.ClientTimeout(total=120, connect=10)


class TelegramError(Exception):
    def __init__(self, description: str, code: int | None = None):
        super().__init__(description)
        self.code = code
        self.description = description


class TgRetryAfter(TelegramError):
    """HTTP 429: ждать parameters.retry_after секунд."""

    def __init__(self, description: str, retry_after: float):
        super().__init__(description, 429)
        self.retry_after = retry_after


class TgConflict(TelegramError):
    """HTTP 409: этого бота опрашивает другой процесс (или включён webhook)."""


class TgNetworkError(TelegramError):
    """Сеть, таймаут или 5xx — можно повторить."""


class TgBadRequest(TelegramError):
    """400: запрос некорректен (например, битый HTML) — повтор не поможет."""


class TgForbidden(TelegramError):
    """401/403: неверный токен или бот заблокирован пользователем."""


def _raise_for(status: int, body: dict | None) -> None:
    body = body or {}
    code = int(body.get("error_code") or status)
    desc = str(body.get("description") or f"HTTP {status}")
    if code == 429:
        params = body.get("parameters") or {}
        raise TgRetryAfter(desc, float(params.get("retry_after") or 1))
    if code == 409:
        raise TgConflict(desc, code)
    if code >= 500:
        raise TgNetworkError(desc, code)
    if code in (401, 403, 404):
        raise TgForbidden(desc, code)
    raise TgBadRequest(desc, code)


class TelegramClient:
    def __init__(self, session: aiohttp.ClientSession, token: str, api_base: str = API_BASE):
        self.session = session
        self._url = f"{api_base.rstrip('/')}/bot{token}/"
        self.conflict = False      # пока True, очередь отправки ничего не шлёт
        self.calls = 0

    async def call(self, method: str, payload: dict | None = None, form: aiohttp.FormData | None = None,
                   timeout: aiohttp.ClientTimeout = CALL_TIMEOUT) -> Any:
        self.calls += 1
        kw: dict[str, Any] = {"timeout": timeout}
        if form is not None:
            kw["data"] = form
        else:
            kw["data"] = orjson.dumps(payload or {})
            kw["headers"] = {"Content-Type": "application/json"}
        try:
            async with self.session.post(self._url + method, **kw) as resp:
                raw = await resp.read()
                try:
                    body = orjson.loads(raw) if raw else None
                except orjson.JSONDecodeError:
                    body = None
                if resp.status == 200 and isinstance(body, dict) and body.get("ok"):
                    return body.get("result")
                _raise_for(resp.status, body if isinstance(body, dict) else None)
        except TelegramError:
            raise
        except (aiohttp.ClientError, asyncio.TimeoutError, OSError) as e:
            # Текст ошибки aiohttp может содержать URL с токеном — его вырежет фильтр логов.
            raise TgNetworkError(f"{type(e).__name__}: {e}" if str(e) else type(e).__name__) from e

    async def get_updates(self, offset: int | None, timeout: int = POLL_TIMEOUT_S) -> list[dict]:
        payload: dict[str, Any] = {"timeout": timeout, "allowed_updates": ["message"]}
        if offset is not None:
            payload["offset"] = offset
        res = await self.call("getUpdates", payload, timeout=POLL_HTTP_TIMEOUT)
        return list(res or [])

    async def send_message(self, chat_id: int, text: str) -> Any:
        return await self.call("sendMessage", {"chat_id": chat_id, "text": text, "parse_mode": "HTML",
                                               "disable_web_page_preview": True})

    async def send_document(self, chat_id: int, data: bytes, filename: str, caption: str | None = None) -> Any:
        form = aiohttp.FormData()
        form.add_field("chat_id", str(chat_id))
        if caption:
            form.add_field("caption", caption)
            form.add_field("parse_mode", "HTML")
        form.add_field("document", data, filename=filename, content_type="text/csv")
        return await self.call("sendDocument", form=form, timeout=UPLOAD_TIMEOUT)


OffsetStore = Callable[[int], Awaitable[None]]


class UpdatePoller:
    """Long polling. Никогда не падает: любые ошибки → пауза и новая попытка.

    update_id сохраняется в SQLite ДО обработки сообщения: после рестарта команда не выполнится
    повторно (at-most-once — для команд это безопаснее, чем повтор /set или /export).
    """

    def __init__(self, client: TelegramClient, dispatch: Callable[[dict], Awaitable[None]],
                 last_update_id: int | None, save_offset: OffsetStore,
                 sleep: Callable[[float], Awaitable[None]] = asyncio.sleep):
        self.client = client
        self.dispatch = dispatch
        self.last_update_id = last_update_id
        self.save_offset = save_offset
        self.sleep = sleep
        self.errors = 0
        self.processed = 0

    @property
    def offset(self) -> int | None:
        return None if self.last_update_id is None else self.last_update_id + 1

    async def poll_once(self) -> int:
        """Один запрос getUpdates + обработка. Возвращает число обработанных апдейтов."""
        updates = await self.client.get_updates(self.offset)
        if self.client.conflict:
            self.client.conflict = False
            log.warning("Telegram: конфликт опроса устранён, отправка возобновлена")
        n = 0
        for u in sorted(updates, key=lambda x: int(x.get("update_id", 0))):
            uid = int(u.get("update_id", 0))
            if self.last_update_id is not None and uid <= self.last_update_id:
                continue
            self.last_update_id = uid
            try:
                await self.save_offset(uid)
            except Exception:
                log.exception("Не удалось сохранить update_id")
            msg = u.get("message")
            if isinstance(msg, dict):
                try:
                    await self.dispatch(msg)
                except Exception:
                    log.exception("Ошибка обработки команды")
            n += 1
            self.processed += 1
        return n

    async def run(self) -> None:
        attempt = 0
        while True:
            try:
                await self.poll_once()
                attempt = 0
            except asyncio.CancelledError:
                raise
            except TgConflict as e:
                self.errors += 1
                if not self.client.conflict:
                    log.error("Telegram 409: этого бота опрашивает другой экземпляр (или задан webhook). "
                              "Отправка остановлена до устранения конфликта", extra={"err": e.description})
                self.client.conflict = True
                await self.sleep(backoff_delay(attempt, base=5.0, cap=60.0))
                attempt += 1
            except TgRetryAfter as e:
                self.errors += 1
                await self.sleep(e.retry_after)
            except TgForbidden as e:
                self.errors += 1
                log.error("Telegram отверг токен — проверьте TELEGRAM_BOT_TOKEN", extra={"err": e.description})
                await self.sleep(backoff_delay(attempt, base=10.0, cap=300.0))
                attempt += 1
            except Exception as e:  # noqa: BLE001 — сеть, 5xx, что угодно: цикл не должен падать
                self.errors += 1
                log.warning("Telegram getUpdates: ошибка, повтор", extra={"err": str(e)[:200]})
                await self.sleep(backoff_delay(attempt))
                attempt += 1
