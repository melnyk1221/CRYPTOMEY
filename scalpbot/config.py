"""Конфигурация: переменные окружения + настраиваемые пороги.

Порядок приоритета: значение по умолчанию < переменная окружения < /set (сохраняется в SQLite).
Проценты везде задаются в процентах: 3.0 означает 3%.
"""
from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any, Callable, Mapping


@dataclass(frozen=True)
class Tunable:
    name: str
    type: type
    default: Any
    min: float
    max: float
    desc: str


def _t(name: str, typ: type, default: Any, lo: float, hi: float, desc: str) -> Tunable:
    return Tunable(name, typ, default, lo, hi, desc)


# Все пороги, которые можно менять командой /set. Диапазоны проверяются.
TUNABLES: dict[str, Tunable] = {t.name: t for t in [
    # Общие
    _t("COST_ROUND_TRIP_PCT", float, 0.15, 0.0, 2.0, "издержки на круг (комиссии+проскальзывание), %"),
    _t("MIN_TURNOVER_24H_USD", float, 300_000.0, 0.0, 1e10, "мин. оборот 24ч для алертов, $"),
    _t("TIER2_MAX", int, 15, 1, 40, "макс. символов в tier 2"),
    _t("TIER2_IDLE_MIN", float, 45.0, 5.0, 600.0, "понижение из tier 2 без событий, мин"),
    _t("MAX_ALERTS_PER_HOUR", int, 12, 1, 200, "лимит алертов в час"),
    _t("STAGE_WINDOW_MIN", float, 15.0, 1.0, 120.0, "окно сборки стадий, мин"),
    _t("COOLDOWN_MIN", float, 15.0, 1.0, 600.0, "кулдаун на символ и уровень, мин"),
    _t("MAX_SPREAD_PCT", float, 0.3, 0.0, 10.0, "макс. спред для SETUP/STRONG, % (0 = выкл)"),
    _t("BOOK_KEEP_PCT", float, 5.0, 0.5, 20.0, "хранить уровни стакана в пределах ±%"),
    # A: импульс
    _t("IMPULSE_MIN_PCT_60S", float, 3.0, 0.1, 50.0, "импульс: мин. движение за 60с, %"),
    _t("IMPULSE_MIN_PCT_5M", float, 6.0, 0.1, 80.0, "импульс: мин. движение за 5м, %"),
    _t("IMPULSE_Z", float, 4.0, 1.0, 20.0, "импульс: порог z-score"),
    _t("IMPULSE_WARMUP_MIN", float, 30.0, 0.0, 240.0, "прогрев перед импульс-алертами, мин"),
    _t("IMPULSE_REARM_MIN", float, 10.0, 1.0, 120.0, "повторный импульс того же знака не раньше, мин"),
    _t("IMPULSE_ALERT_MIN_STRENGTH", float, 0.5, 0.0, 1.0, "мин. сила импульса для алерта"),
    # Промоушен в tier 2
    _t("OI_JUMP_PCT", float, 8.0, 0.5, 500.0, "рост OI за 15м для tier 2, %"),
    _t("FUNDING_EXTREME", float, 0.10, 0.005, 5.0, "экстремальный funding за интервал, %"),
    # B: плотность
    _t("DENSITY_K", float, 5.0, 1.5, 100.0, "плотность: кратность к медиане топ-20"),
    _t("DENSITY_MIN_USD", float, 20_000.0, 100.0, 1e9, "плотность: мин. объём, $"),
    _t("DENSITY_TURNOVER_FRAC", float, 0.003, 0.0, 0.5, "плотность: мин. доля от оборота 24ч"),
    _t("DENSITY_MAX_DIST_PCT", float, 2.0, 0.05, 10.0, "плотность: макс. расстояние от середины, %"),
    _t("DENSITY_MIN_LIFETIME_S", float, 45.0, 1.0, 3600.0, "плотность: мин. время жизни, с"),
    # C: робот
    _t("ROBOT_WINDOW", int, 60, 12, 500, "робот: окно последних ордеров"),
    _t("ROBOT_SIZE_TOL", float, 0.08, 0.005, 0.5, "робот: допуск размера (доля)"),
    _t("ROBOT_CV_MAX", float, 0.25, 0.01, 2.0, "робот: макс. CV интервалов"),
    _t("ROBOT_MIN_COUNT", int, 12, 3, 500, "робот: мин. ордеров в серии"),
    _t("ROBOT_MIN_TOTAL_USD", float, 15_000.0, 100.0, 1e9, "робот: мин. сумма серии, $"),
    # D: поглощение
    _t("ABSORB_FRAC", float, 0.6, 0.1, 1.0, "поглощение: доля съеденного объёма"),
    # E: толкание
    _t("PUSH_MIN_STEPS", int, 3, 1, 50, "толкание: мин. перестановок за 10 мин"),
]}

# Ключи, которые /set менять не может (только переменные окружения).
STATIC_KEYS = (
    "TELEGRAM_BOT_TOKEN", "TELEGRAM_CHAT_ID", "TZ", "DATA_DIR", "LOG_LEVEL",
    "MEMORY_LIMIT_MB", "DRY_RUN", "MEXC_FUTURES_REST_BASE", "ENABLE_BYBIT",
    "ENABLE_HYPERLIQUID", "ENABLE_BINANCE", "DAILY_REPORT_HOUR", "MEXC_LINK_TEMPLATE",
)


class ConfigError(ValueError):
    pass


def _parse(t: Tunable, raw: Any) -> Any:
    try:
        if t.type is int:
            val: Any = int(float(str(raw).strip()))
        elif t.type is float:
            val = float(str(raw).strip().replace(",", "."))
        else:
            val = t.type(raw)
    except (TypeError, ValueError):
        raise ConfigError(f"{t.name}: ожидается число, получено {raw!r}")
    if not (t.min <= val <= t.max):
        raise ConfigError(f"{t.name}: значение {val} вне диапазона [{t.min}, {t.max}]")
    return val


def _tri(raw: str | None) -> str:
    v = (raw or "auto").strip().lower()
    if v in ("1", "true", "yes", "on"):
        return "on"
    if v in ("0", "false", "no", "off"):
        return "off"
    return "auto"


class Settings:
    """Все настройки бота. Пороги доступны как атрибуты: settings.IMPULSE_Z."""

    def __init__(self, env: Mapping[str, str] | None = None):
        env = os.environ if env is None else env
        self.TELEGRAM_BOT_TOKEN: str = env.get("TELEGRAM_BOT_TOKEN", "").strip()
        chat = env.get("TELEGRAM_CHAT_ID", "").strip()
        try:
            self.TELEGRAM_CHAT_ID: int | None = int(chat) if chat else None
        except ValueError:
            raise ConfigError(f"TELEGRAM_CHAT_ID должен быть числом, получено {chat!r}")
        self.TZ: str = env.get("TZ", "Europe/Kyiv").strip() or "Europe/Kyiv"
        self.DATA_DIR: str = (env.get("DATA_DIR", "").strip()
                              or env.get("RAILWAY_VOLUME_MOUNT_PATH", "").strip()
                              or "./data")
        self.LOG_LEVEL: str = env.get("LOG_LEVEL", "INFO").strip().upper() or "INFO"
        self.MEMORY_LIMIT_MB: int = int(env.get("MEMORY_LIMIT_MB", "450") or 450)
        self.DRY_RUN: bool = env.get("DRY_RUN", "0").strip() in ("1", "true", "yes")
        self.MEXC_FUTURES_REST_BASE: str = (env.get("MEXC_FUTURES_REST_BASE", "").strip()
                                            or "https://contract.mexc.com").rstrip("/")
        self.ENABLE_BYBIT: str = _tri(env.get("ENABLE_BYBIT"))
        self.ENABLE_HYPERLIQUID: str = _tri(env.get("ENABLE_HYPERLIQUID"))
        self.ENABLE_BINANCE: str = _tri(env.get("ENABLE_BINANCE"))
        self.DAILY_REPORT_HOUR: int = int(env.get("DAILY_REPORT_HOUR", "9") or 9) % 24
        self.MEXC_LINK_TEMPLATE: str = (env.get("MEXC_LINK_TEMPLATE", "").strip()
                                        or "https://futures.mexc.com/exchange/{symbol}")
        self._overrides: dict[str, Any] = {}
        self._listeners: list[Callable[[str, Any], None]] = []
        for t in TUNABLES.values():
            raw = env.get(t.name)
            setattr(self, t.name, _parse(t, raw) if raw not in (None, "") else t.default)

    # --- Runtime-изменения ---
    def set_runtime(self, name: str, raw: Any) -> tuple[Any, Any]:
        """Проверяет и применяет значение. Возвращает (старое, новое)."""
        name = name.strip().upper()
        t = TUNABLES.get(name)
        if t is None:
            raise ConfigError(f"Неизвестный или неизменяемый ключ: {name}")
        new = _parse(t, raw)
        old = getattr(self, name)
        setattr(self, name, new)
        self._overrides[name] = new
        for cb in self._listeners:
            cb(name, new)
        return old, new

    def apply_persisted(self, values: Mapping[str, str]) -> list[str]:
        """Применяет значения из SQLite; некорректные пропускаются. Возвращает ошибки."""
        errors = []
        for k, v in values.items():
            try:
                self.set_runtime(k, v)
            except ConfigError as e:
                errors.append(str(e))
        return errors

    def on_change(self, cb: Callable[[str, Any], None]) -> None:
        self._listeners.append(cb)

    @property
    def overrides(self) -> dict[str, Any]:
        return dict(self._overrides)

    @property
    def setup_mode(self) -> bool:
        return self.TELEGRAM_CHAT_ID is None

    def mexc_link(self, symbol: str) -> str:
        return self.MEXC_LINK_TEMPLATE.replace("{symbol}", symbol)

    def describe(self) -> list[str]:
        """Строки для /config (без секретов)."""
        lines = [
            f"TZ={self.TZ}", f"DATA_DIR={self.DATA_DIR}", f"LOG_LEVEL={self.LOG_LEVEL}",
            f"MEMORY_LIMIT_MB={self.MEMORY_LIMIT_MB}", f"DRY_RUN={int(self.DRY_RUN)}",
            f"MEXC_FUTURES_REST_BASE={self.MEXC_FUTURES_REST_BASE}",
            f"ENABLE_BYBIT={self.ENABLE_BYBIT}", f"ENABLE_HYPERLIQUID={self.ENABLE_HYPERLIQUID}",
            f"ENABLE_BINANCE={self.ENABLE_BINANCE}", f"DAILY_REPORT_HOUR={self.DAILY_REPORT_HOUR}",
            f"MEXC_LINK_TEMPLATE={self.MEXC_LINK_TEMPLATE}",
            f"TELEGRAM_BOT_TOKEN={'задан' if self.TELEGRAM_BOT_TOKEN else 'НЕ задан'}",
            f"TELEGRAM_CHAT_ID={self.TELEGRAM_CHAT_ID if self.TELEGRAM_CHAT_ID else 'пусто (режим настройки)'}",
        ]
        for t in TUNABLES.values():
            mark = " (/set)" if t.name in self._overrides else ""
            lines.append(f"{t.name}={getattr(self, t.name)}{mark}")
        return lines
