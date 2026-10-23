"""Транспорт Telegram против имитации Bot API (настоящий aiohttp-сервер, без сети)."""
import asyncio

import aiohttp
import pytest
from aiohttp import web
from aiohttp.test_utils import TestServer

from scalpbot.alerts.sender import OutMsg, TelegramSender
from scalpbot.bot.telegram_client import (POLL_TIMEOUT_S, TelegramClient, TgConflict, TgNetworkError,
                                          TgRetryAfter, UpdatePoller)
from scalpbot.storage.db import Database

TOKEN = "123456:TEST-TOKEN"


class FakeTelegram:
    """Имитация https://api.telegram.org/bot<TOKEN>/<method>.

    responses[method] — очередь ответов (status, body); когда пуста — {"ok": true, "result": default}.
    """

    def __init__(self):
        self.requests: list[tuple[str, dict]] = []
        self.responses: dict[str, list[tuple[int, dict]]] = {}
        self.updates: list[dict] = []        # отдаются по offset, как это делает Telegram
        self.documents: list[dict] = []

    def queue(self, method, status, body):
        self.responses.setdefault(method, []).append((status, body))

    async def handle(self, request: web.Request) -> web.Response:
        token, method = request.match_info["token"], request.match_info["method"]
        if token != TOKEN:
            return web.json_response({"ok": False, "error_code": 401, "description": "Unauthorized"}, status=401)
        if request.content_type.startswith("multipart/"):
            form = {}
            reader = await request.multipart()
            async for part in reader:
                data = await part.read()
                form[part.name] = {"filename": part.filename, "data": data,
                                   "content_type": part.headers.get("Content-Type")}
            self.documents.append(form)
            payload = {k: v["data"].decode() if v["filename"] is None else v for k, v in form.items()}
        else:
            payload = await request.json()
        self.requests.append((method, payload))
        q = self.responses.get(method)
        if q:
            status, body = q.pop(0)
            return web.json_response(body, status=status)
        if method == "getUpdates":
            off = payload.get("offset")
            res = [u for u in self.updates if off is None or u["update_id"] >= off]
            return web.json_response({"ok": True, "result": res})
        return web.json_response({"ok": True, "result": {"message_id": len(self.requests)}})

    def calls(self, method):
        return [p for m, p in self.requests if m == method]


@pytest.fixture
async def tg():
    fake = FakeTelegram()
    app = web.Application()
    app.router.add_post("/bot{token}/{method}", fake.handle)
    srv = TestServer(app)
    await srv.start_server()
    session = aiohttp.ClientSession()
    client = TelegramClient(session, TOKEN, api_base=str(srv.make_url("")).rstrip("/"))
    yield fake, client
    await session.close()
    await srv.close()


def upd(uid, chat_id=42, text="/status"):
    return {"update_id": uid, "message": {"message_id": uid, "chat": {"id": chat_id, "type": "private"},
                                          "text": text}}


async def no_sleep(_):
    return None


# ---------- long polling и offset ----------
async def test_polling_offset_handling(tg):
    fake, client = tg
    fake.updates = [upd(10, text="/help"), upd(11, text="/status")]
    seen, saved = [], []

    async def dispatch(m):
        seen.append(m["text"])

    async def save(uid):
        saved.append(uid)

    p = UpdatePoller(client, dispatch, None, save)
    assert await p.poll_once() == 2
    assert seen == ["/help", "/status"] and saved == [10, 11] and p.offset == 12
    fake.updates.append(upd(12, text="/top"))
    assert await p.poll_once() == 1 and seen[-1] == "/top"
    assert await p.poll_once() == 0                     # подтверждённые апдейты больше не приходят
    first, second, third = fake.calls("getUpdates")
    assert "offset" not in first and second["offset"] == 12 and third["offset"] == 13
    assert first["timeout"] == POLL_TIMEOUT_S and first["allowed_updates"] == ["message"]


async def test_restart_does_not_reprocess(tg, tmp_path):
    fake, client = tg
    fake.updates = [upd(100, text="/pause"), upd(101, text="/set IMPULSE_Z 5")]
    db = Database(str(tmp_path))
    await db.open()

    async def save(uid):
        await db.save_setting("tg:last_update_id", uid)

    seen1 = []

    async def d1(m):
        seen1.append(m["text"])

    await UpdatePoller(client, d1, None, save).poll_once()
    assert seen1 == ["/pause", "/set IMPULSE_Z 5"]
    await db.close()

    # «Рестарт»: Telegram ещё не получил подтверждение и снова отдаёт те же апдейты, если offset не передать.
    db2 = Database(str(tmp_path))
    await db2.open()
    last = (await db2.load_settings())["tg:last_update_id"]
    seen2 = []

    async def d2(m):
        seen2.append(m["text"])

    p2 = UpdatePoller(client, d2, int(last), save)
    assert await p2.poll_once() == 0 and seen2 == []
    assert fake.calls("getUpdates")[-1]["offset"] == 102
    # Даже если сервер вернёт старый апдейт (offset проигнорирован) — повторной обработки нет.
    fake.queue("getUpdates", 200, {"ok": True, "result": [upd(101, text="/set IMPULSE_Z 5"), upd(102, text="/top")]})
    await p2.poll_once()
    assert seen2 == ["/top"]
    await db2.close()


async def test_poller_survives_errors_and_dispatch_exceptions(tg):
    fake, client = tg
    fake.queue("getUpdates", 502, {"ok": False, "error_code": 502, "description": "Bad Gateway"})
    fake.queue("getUpdates", 200, {"ok": False})                       # мусорный ответ
    fake.updates = [upd(1, text="/boom"), upd(2, text="/ok")]
    seen = []
    done = asyncio.Event()

    async def dispatch(m):
        seen.append(m["text"])
        if m["text"] == "/boom":
            raise RuntimeError("handler crashed")
        done.set()

    async def save(uid):
        pass

    p = UpdatePoller(client, dispatch, None, save, sleep=no_sleep)
    task = asyncio.create_task(p.run())
    await asyncio.wait_for(done.wait(), 5)
    assert not task.done()                              # цикл жив после 502, мусора и падения обработчика
    task.cancel()
    assert seen == ["/boom", "/ok"] and p.errors == 2


async def test_409_conflict_blocks_sending_until_resolved(tg):
    fake, client = tg
    fake.queue("getUpdates", 409, {"ok": False, "error_code": 409,
                                   "description": "Conflict: terminated by other getUpdates request"})
    sleeps = []

    async def rec_sleep(d):
        sleeps.append(d)

    p = UpdatePoller(client, lambda m: asyncio.sleep(0), None, lambda u: asyncio.sleep(0), sleep=rec_sleep)
    with pytest.raises(TgConflict):
        await p.poll_once()
    # run() обрабатывает 409: ставит флаг конфликта
    fake.queue("getUpdates", 409, {"ok": False, "error_code": 409, "description": "Conflict"})
    task = asyncio.create_task(p.run())
    for _ in range(100):
        if client.conflict:
            break
        await asyncio.sleep(0.01)
    assert client.conflict is True
    sender = TelegramSender(client, 42, dry_run=False, min_interval=0.01)
    sender.send_text("алерт")
    stask = asyncio.create_task(sender.run())
    await asyncio.sleep(0.2)
    assert fake.calls("sendMessage") == []                 # во время конфликта ничего не отправлено
    # следующий getUpdates успешен → конфликт снят → сообщение уходит
    for _ in range(300):
        if fake.calls("sendMessage"):
            break
        await asyncio.sleep(0.02)
    task.cancel()
    stask.cancel()
    assert client.conflict is False and fake.calls("sendMessage")[0]["text"] == "алерт"


# ---------- отправка ----------
async def test_send_message_params_and_429_retry_after(tg):
    fake, client = tg
    fake.queue("sendMessage", 429, {"ok": False, "error_code": 429, "description": "Too Many Requests: retry after 7",
                                    "parameters": {"retry_after": 7}})
    with pytest.raises(TgRetryAfter) as ei:
        await client.send_message(42, "x")
    assert ei.value.retry_after == 7
    fake.queue("sendMessage", 429, {"ok": False, "error_code": 429, "description": "flood",
                                    "parameters": {"retry_after": 3}})
    slept = []

    async def rec(d):
        slept.append(d)

    sender = TelegramSender(client, 42, dry_run=False)
    sender.sleep = rec
    assert await sender.deliver(OutMsg(text="<b>Импульс</b> &amp;")) is True
    assert slept == [3.5]
    calls = fake.calls("sendMessage")
    assert len(calls) == 3
    assert calls[-1] == {"chat_id": 42, "text": "<b>Импульс</b> &amp;", "parse_mode": "HTML",
                         "disable_web_page_preview": True}


async def test_network_errors_exponential_backoff_then_give_up(tg):
    fake, client = tg
    for _ in range(6):
        fake.queue("sendMessage", 503, {"ok": False, "error_code": 503, "description": "Service Unavailable"})
    slept = []

    async def rec(d):
        slept.append(d)

    sender = TelegramSender(client, 42, dry_run=False, max_retries=5)
    sender.sleep = rec
    assert await sender.deliver(OutMsg(text="x")) is False
    assert slept == [2.0, 4.0, 8.0, 16.0, 30.0] and sender.failed == 1


async def test_bad_request_not_retried(tg):
    fake, client = tg
    fake.queue("sendMessage", 400, {"ok": False, "error_code": 400, "description": "Bad Request: can't parse entities"})
    sender = TelegramSender(client, 42, dry_run=False)
    sender.sleep = no_sleep
    assert await sender.deliver(OutMsg(text="<b>")) is False
    assert len(fake.calls("sendMessage")) == 1


async def test_send_document_multipart(tg):
    fake, client = tg
    data = "id,symbol\n1,AGT_USDT\n".encode("utf-8-sig")
    sender = TelegramSender(client, 42, dry_run=False)
    assert await sender.deliver(OutMsg(document=data, filename="scalpbot_7d.csv", caption="Резервная копия")) is True
    form = fake.documents[0]
    assert form["chat_id"]["data"] == b"42"
    assert form["caption"]["data"].decode() == "Резервная копия"
    assert form["document"]["filename"] == "scalpbot_7d.csv"
    assert form["document"]["data"] == data and form["document"]["content_type"] == "text/csv"


async def test_unauthorized_token_and_network_failure(tg):
    fake, client = tg
    bad = TelegramClient(client.session, "999:WRONG", api_base=client._url.split("/bot")[0])
    from scalpbot.bot.telegram_client import TgForbidden
    with pytest.raises(TgForbidden):
        await bad.send_message(1, "x")
    dead = TelegramClient(client.session, TOKEN, api_base="http://127.0.0.1:9")
    with pytest.raises(TgNetworkError):
        await dead.send_message(1, "x")


async def test_reply_goes_to_telegram_even_in_dry_run(tg, capsys):
    fake, client = tg
    sender = TelegramSender(client, 42, dry_run=True, min_interval=0.01)
    sender.send_text("алерт")                          # DRY_RUN: алерт в stdout
    sender.send_text("ответ на /status", chat_id=42, reply=True)
    task = asyncio.create_task(sender.run())
    for _ in range(100):
        if sender.sent == 2:
            break
        await asyncio.sleep(0.02)
    task.cancel()
    assert "алерт" in capsys.readouterr().out
    assert [c["text"] for c in fake.calls("sendMessage")] == ["ответ на /status"]


async def test_end_to_end_command_via_fake_api(tg, tmp_path):
    """getUpdates → CommandRouter → очередь → sendMessage; чужой чат игнорируется."""
    fake, client = tg
    from scalpbot.app import App
    from scalpbot.bot.handlers import CommandRouter
    from scalpbot.config import Settings
    app = App(Settings(env={"TELEGRAM_CHAT_ID": "42", "DATA_DIR": str(tmp_path)}))
    await app.db.open()
    app.sender = TelegramSender(client, 42, dry_run=False, min_interval=0.01)
    router = CommandRouter(app)
    fake.updates = [upd(1, chat_id=7, text="/pause"), upd(2, text="/pause"), upd(3, text="/help")]
    p = UpdatePoller(client, router.handle, None, app._save_update_id)
    await p.poll_once()
    task = asyncio.create_task(app.sender.run())
    for _ in range(100):
        if len(fake.calls("sendMessage")) >= 2:
            break
        await asyncio.sleep(0.02)
    task.cancel()
    sent = fake.calls("sendMessage")
    assert app.paused is True
    assert [c["chat_id"] for c in sent] == [42, 42]
    assert "приостановлена" in sent[0]["text"] and "Команды" in sent[1]["text"]
    assert (await app.db.load_settings())["tg:last_update_id"] == "3"
    await app.db.close()
