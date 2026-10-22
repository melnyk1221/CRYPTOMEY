import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import pytest  # noqa: E402

from scalpbot.config import Settings  # noqa: E402


@pytest.fixture
def settings():
    return Settings(env={"TELEGRAM_BOT_TOKEN": "", "TELEGRAM_CHAT_ID": "123", "DATA_DIR": "./data-test"})
