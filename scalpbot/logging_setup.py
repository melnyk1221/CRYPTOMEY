"""Структурированное логирование: JSON-строки в stdout, без секретов, с подавлением повторов."""
from __future__ import annotations

import logging
import sys
import time

import orjson

_SECRETS: list[str] = []
_STD_ATTRS = set(vars(logging.LogRecord("", 0, "", 0, "", None, None))) | {"message", "asctime"}


def register_secret(value: str) -> None:
    """Значение будет заменено на *** во всех логах."""
    if value and len(value) >= 6 and value not in _SECRETS:
        _SECRETS.append(value)


def _redact(s: str) -> str:
    for sec in _SECRETS:
        if sec in s:
            s = s.replace(sec, "***")
    return s


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        out = {
            "ts": time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(record.created)) + f".{int(record.msecs):03d}Z",
            "lvl": record.levelname,
            "log": record.name,
            "msg": _redact(record.getMessage()),
        }
        for k, v in record.__dict__.items():
            if k not in _STD_ATTRS and not k.startswith("_"):
                out[k] = v
        if record.exc_info:
            out["exc"] = _redact(self.formatException(record.exc_info))
        return orjson.dumps(out, default=str).decode()


class RateLimitFilter(logging.Filter):
    """Не более `burst` одинаковых сообщений (по шаблону) за `period` секунд."""

    def __init__(self, burst: int = 5, period: float = 60.0, max_keys: int = 2000):
        super().__init__()
        self.burst, self.period, self.max_keys = burst, period, max_keys
        self._seen: dict[tuple, list] = {}

    def filter(self, record: logging.LogRecord) -> bool:
        if record.levelno >= logging.CRITICAL:
            return True
        key = (record.name, record.msg if isinstance(record.msg, str) else str(record.msg), record.levelno)
        now = time.monotonic()
        st = self._seen.get(key)
        if st is None or now - st[0] > self.period:
            if st is not None and st[2]:
                record.suppressed_before = st[2]
            if len(self._seen) >= self.max_keys:
                self._seen.clear()
            self._seen[key] = [now, 1, 0]
            return True
        st[1] += 1
        if st[1] <= self.burst:
            return True
        st[2] += 1
        return False


def setup_logging(level: str = "INFO") -> None:
    for stream in (sys.stdout, sys.stderr):
        try:
            stream.reconfigure(encoding="utf-8", errors="replace")  # type: ignore[attr-defined]
        except (AttributeError, ValueError):
            pass
    root = logging.getLogger()
    for h in list(root.handlers):
        root.removeHandler(h)
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter())
    handler.addFilter(RateLimitFilter())
    root.addHandler(handler)
    root.setLevel(getattr(logging, level.upper(), logging.INFO))
    # Сторонние библиотеки слишком болтливы на INFO.
    for noisy in ("aiohttp", "aiosqlite", "asyncio"):
        logging.getLogger(noisy).setLevel(logging.WARNING)
