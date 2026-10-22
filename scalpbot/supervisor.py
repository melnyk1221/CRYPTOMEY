"""Перезапуск упавших задач с экспоненциальной паузой и джиттером."""
from __future__ import annotations

import asyncio
import logging
import random
from typing import Awaitable, Callable

log = logging.getLogger("supervisor")


def backoff_delay(attempt: int, base: float = 1.0, cap: float = 60.0, jitter: float = 0.2) -> float:
    """1 → 2 → 4 … до cap секунд, ±jitter."""
    d = min(cap, base * (2 ** max(0, attempt)))
    return max(0.1, d * random.uniform(1 - jitter, 1 + jitter))


async def supervise(name: str, factory: Callable[[], Awaitable[None]],
                    on_error: Callable[[str, BaseException], None] | None = None,
                    healthy_after: float = 60.0) -> None:
    """Запускает factory() в цикле. Если задача проработала > healthy_after с, счётчик попыток сбрасывается."""
    loop = asyncio.get_running_loop()
    attempt = 0
    while True:
        started = loop.time()
        try:
            await factory()
            log.warning("Задача завершилась, перезапуск", extra={"task": name})
        except asyncio.CancelledError:
            raise
        except Exception as e:  # noqa: BLE001 — супервизор ловит всё
            if getattr(e, "expected", False):
                log.warning("Задача завершилась: %s", e, extra={"task": name})
            else:
                log.exception("Задача упала", extra={"task": name})
            if on_error:
                try:
                    on_error(name, e)
                except Exception:
                    pass
        if loop.time() - started > healthy_after:
            attempt = 0
        delay = backoff_delay(attempt)
        attempt += 1
        await asyncio.sleep(delay)
