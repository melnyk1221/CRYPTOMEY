"""Сообщения Telegram на русском (HTML). Весь динамический текст экранируется."""
from __future__ import annotations

import html
import math
from typing import Callable

from ..bus import (ABSORPTION, DENSITY, EXHAUSTION, IMPULSE, LONG, PUSH, ROBOT, SHORT, SQUEEZE,
                   DetectorEvent)
from ..scoring import L_EXHAUSTION, L_IMPULSE, L_SETUP, L_STRONG, STAGE_TOTAL, AlertDecision

DISCLAIMER = "⚠️ Не торговый совет."
ALERT_MAX_LEN = 1200

e = html.escape


def money(x: float | None) -> str:
    if x is None or x != x:
        return "—"
    a = abs(x)
    sign = "−" if x < 0 else ""
    if a >= 1e9:
        return f"{sign}${a / 1e9:.2f}B"
    if a >= 1e6:
        return f"{sign}${a / 1e6:.1f}M"
    if a >= 1e3:
        return f"{sign}${a / 1e3:.1f}k" if a < 1e5 else f"{sign}${a / 1e3:.0f}k"
    return f"{sign}${a:.0f}"


def pct(x: float | None, nd: int = 1, signed: bool = True) -> str:
    if x is None or x != x:
        return "—"
    s = f"{abs(x):.{nd}f}%"
    if not signed:
        return s
    return ("+" if x > 0 else "−" if x < 0 else "") + s


def fmt_price(p: float | None, decimals: int | None = None) -> str:
    if p is None or p != p or p <= 0:
        return "—"
    if decimals is None:
        decimals = max(2, min(10, 4 - int(math.floor(math.log10(p)))))
    return f"{p:.{decimals}f}"


def dir_word(direction: str) -> str:
    return {"long": "лонг", "short": "шорт"}.get(direction, "нейтрально")


def funding_str(rate_pct: float | None, cycle_h: float | None) -> str:
    if rate_pct is None:
        return "—"
    return f"{pct(rate_pct, 3)} / {cycle_h:g}ч" if cycle_h else pct(rate_pct, 3)


def link(url: str, symbol: str) -> str:
    return f'🔗 MEXC: <a href="{e(url, quote=True)}">{e(symbol)}</a>'


def _clip(lines: list[str], limit: int = ALERT_MAX_LEN) -> str:
    """Собирает сообщение ≤ limit, выбрасывая второстепенные строки из середины."""
    text = "\n".join(lines)
    while len(text) > limit and len(lines) > 4:
        lines.pop(-4)  # последние три строки: история, ссылка, дисклеймер — сохраняем
        text = "\n".join(lines)
    return text[:4096]


# ---------- импульс ----------
def format_impulse(ev: DetectorEvent, *, price_str: str, spread_pct: float | None, history: str,
                   url: str) -> str:
    d = ev.details
    up = d.get("move_sign", 1) > 0
    head = "🔺 ИМПУЛЬС вверх" if up else "🔻 ИМПУЛЬС вниз"
    z = d.get("z")
    lines = [f"<b>{head}</b> · {e(ev.symbol)}",
             f"{pct(d.get('move_pct'))} за {e(str(d.get('window')))} (z={f'{abs(z):.1f}' if z is not None else '—'}, "
             f"порог {pct(d.get('threshold_pct'), 1, False)}) · цена {e(price_str)}"]
    btc = d.get("btc_move_pct")
    if d.get("beta_btc"):
        lines.append(f"⚠️ BTC {pct(btc, 2)} за то же окно — похоже на движение всего рынка")
    elif btc is not None:
        lines.append(f"BTC {pct(btc, 2)} за то же окно → движение собственное ✅")
    lines.append(f"OI 15м: {pct(d.get('oi_change_15m_pct'))} · funding "
                 f"{funding_str(d.get('funding_pct'), d.get('funding_cycle_h'))}")
    lines.append(f"Оборот 24ч {money(d.get('amount24'))} · спред {pct(spread_pct, 2, False)}"
                 f" · 15м {pct(d.get('change_15m_pct'))}")
    lines += [e(history), link(url, ev.symbol), DISCLAIMER]
    return _clip(lines)


# ---------- затухание ----------
def format_exhaustion(ev: DetectorEvent, *, price_str: str, spread_pct: float | None, history: str,
                      url: str) -> str:
    d = ev.details
    after_dump = d.get("impulse_tag") == "dump"
    head = "🔄 ЗАТУХАНИЕ после падения" if after_dump else "🔄 ЗАТУХАНИЕ после роста"
    side = "покупателей" if after_dump else "продавцов"
    lines = [f"<b>{head}</b> · {e(ev.symbol)} · {dir_word(ev.direction)}-контекст",
             f"Импульс {pct(d.get('impulse_move_pct'))} {d.get('minutes_since', 0):.0f} мин назад · "
             f"откат {pct(d.get('retrace_pct'), 0, False)} · цена {e(price_str)}",
             f"Скорость упала: 60с {pct(d.get('r60_pct'), 2)} (пик {pct(d.get('peak_r60_pct'), 2)})",
             f"Доля {side} за 60с: {pct(d.get('delta_share_pct'), 0, False)} · спред {pct(spread_pct, 2, False)}"]
    extra = []
    if d.get("oi_change_during_pct") is not None:
        extra.append(f"OI за импульс {pct(d['oi_change_during_pct'])}")
    if d.get("liq_usd"):
        extra.append(f"ликвидации (Bybit) {money(d['liq_usd'])}")
    if extra:
        lines.append(" · ".join(extra))
    lines += [e(history), link(url, ev.symbol), DISCLAIMER]
    return _clip(lines)


# ---------- SETUP / STRONG ----------
def event_line(ev: DetectorEvent, fp: Callable[[float | None], str]) -> str:
    d = ev.details
    ok = "✅" if ev.direction in (LONG, SHORT) else "⚪"
    if ev.family == DENSITY:
        side = "бид" if d.get("side") == "bid" else "аск"
        s = (f"{ok} Плотность ({side}) {money(d.get('notional_usd'))} на {e(fp(d.get('price')))} "
             f"({pct(d.get('dist_pct'))}), живёт {d.get('lifetime_s', 0) / 60:.0f} мин")
        if d.get("traded_share_pct"):
            s += f", съедено {pct(d['traded_share_pct'], 0, False)}"
        return s
    if ev.family == ABSORPTION:
        side = "бид" if d.get("side") == "bid" else "аск"
        state = "пробита" if d.get("broken") else "цена удержалась"
        return (f"{ok} Поглощение: {side}-плотность {money(d.get('peak_usd'))} на {e(fp(d.get('price')))} "
                f"съедена на {pct(d.get('traded_share_pct'), 0, False)}, {state}")
    if ev.family == ROBOT:
        side = "покупки" if d.get("side") == "buy" else "продажи"
        return (f"{ok} Робот: {side} ≈{money(d.get('size_usd'))} каждые {d.get('interval_s', 0):.1f} с "
                f"×{d.get('count')} ({d.get('duration_s', 0):.0f} с, всего {money(d.get('total_usd'))})")
    if ev.family == PUSH:
        what = "бид-стенка переставлена выше" if ev.direction == LONG else "аск-стенка переставлена ниже"
        return (f"{ok} Толкание на споте: {what} {d.get('steps')} раз за {d.get('minutes', 0):.0f} мин, "
                f"спот {pct(d.get('spot_move_pct'))}")
    if ev.family == SQUEEZE:
        parts = []
        if d.get("oi_to_mcap") is not None:
            parts.append(f"OI/капа {d['oi_to_mcap']:.2f}" + (" (FDV)" if d.get("mcap_source") == "fdv" else ""))
        if d.get("fut_spot_ratio") is not None:
            parts.append(f"фьюч/спот ≈{d['fut_spot_ratio']:.1f}×")
        parts.append(f"funding {funding_str(d.get('funding_pct'), d.get('funding_cycle_h'))}")
        return f"{ok} Сквиз: " + " · ".join(parts)
    if ev.family == IMPULSE:
        return f"⚪ Импульс {pct(d.get('move_pct'))} за {e(str(d.get('window')))}"
    if ev.family == EXHAUSTION:
        return f"{ok} Затухание после {'падения' if d.get('impulse_tag') == 'dump' else 'роста'}"
    return f"{ok} {e(ev.family)}"


def format_stage_alert(dec: AlertDecision, *, price_str: str, change_15m: float | None, amount24: float,
                       spread_pct: float | None, history: str, url: str,
                       fp: Callable[[float | None], str], stop_hint: str | None = None) -> str:
    icon = "🟢" if dec.direction == LONG else "🔴"
    lines = [f"{icon} <b>{dec.level}</b> · {e(dec.symbol)} · {dir_word(dec.direction)}-контекст "
             f"(стадии {dec.stages}/{STAGE_TOTAL})",
             f"Цена {e(price_str)} · 15м {pct(change_15m)} · оборот 24ч {money(amount24)} · "
             f"спред {pct(spread_pct, 2, False)}"]
    seen = set()
    for ev in sorted(dec.events, key=lambda x: -x.ts):
        key = (ev.family, ev.details.get("price"), ev.details.get("side"))
        if key in seen:
            continue
        seen.add(key)
        lines.append(event_line(ev, fp))
    lines += [e(history), link(url, dec.symbol),
              DISCLAIMER + (f" {e(stop_hint)}" if stop_hint else "")]
    return _clip(lines)


# ---------- дайджест ----------
def format_digest(events: list[DetectorEvent], overflow: list[AlertDecision], fp_for) -> str:
    lines = ["🧾 <b>Дайджест за час</b>"]
    if events:
        by_sym: dict[str, list[DetectorEvent]] = {}
        for ev in events:
            by_sym.setdefault(ev.symbol, []).append(ev)
        for sym, evs in sorted(by_sym.items(), key=lambda kv: -len(kv[1]))[:20]:
            fams = {}
            for ev in evs:
                fams[ev.family] = fams.get(ev.family, 0) + 1
            parts = ", ".join(f"{f}×{n}" if n > 1 else f for f, n in fams.items())
            last = max(evs, key=lambda x: x.ts)
            lines.append(f"• {e(sym)}: {e(parts)} — посл.: {event_line(last, fp_for(sym))}")
    if overflow:
        lines.append("")
        lines.append(f"Сверх лимита алертов в час (не отправлены): {len(overflow)}")
        for d in overflow[-10:]:
            lines.append(f"• {d.level} {e(d.symbol)} {dir_word(d.direction)}")
    if len(lines) == 1:
        return ""
    return "\n".join(lines)[:4000]


LEVEL_TITLES = {L_IMPULSE: "Импульс", L_EXHAUSTION: "Затухание", L_SETUP: "SETUP", L_STRONG: "STRONG"}
