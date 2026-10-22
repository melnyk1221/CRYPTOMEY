"""Точка входа: python -m scalpbot"""
from __future__ import annotations

import asyncio
import logging
import sys

from .config import ConfigError, Settings
from .logging_setup import register_secret, setup_logging


def main() -> int:
    try:
        settings = Settings()
    except ConfigError as e:
        print(f"Ошибка конфигурации: {e}", file=sys.stderr)
        return 2
    setup_logging(settings.LOG_LEVEL)
    register_secret(settings.TELEGRAM_BOT_TOKEN)
    log = logging.getLogger("main")
    if not settings.TELEGRAM_BOT_TOKEN and not settings.DRY_RUN:
        log.critical("TELEGRAM_BOT_TOKEN не задан. Задайте токен или DRY_RUN=1 для работы без Telegram.")
        return 2
    from .app import App
    try:
        import uvloop  # type: ignore
        runner = uvloop.run
    except ImportError:
        runner = asyncio.run
    try:
        runner(App(settings).run())
    except KeyboardInterrupt:
        pass
    return 0


if __name__ == "__main__":
    sys.exit(main())
