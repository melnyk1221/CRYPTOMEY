"""python -m scalpbot.probe — проверка доступности источников (без Telegram)."""
from __future__ import annotations

import asyncio

from .config import Settings
from .logging_setup import setup_logging
from .probes import format_table, run_probes
from .sources.common import make_session


async def _main() -> None:
    settings = Settings()
    async with make_session() as session:
        res = await run_probes(session, settings)
    print(format_table(res))


if __name__ == "__main__":
    setup_logging("WARNING")
    asyncio.run(_main())
