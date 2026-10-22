"""Реестр контрактов и пересчёт единиц.

Главное правило (самый частый источник ошибок):
    контракты × contractSize = количество базовой монеты
    контракты × contractSize × цена = объём в USDT
Применяется к v в сделках, объёмам в стакане, holdVol (OI), volume24. amount24 уже в USDT.
"""
from __future__ import annotations

import logging
import math
from dataclasses import dataclass

log = logging.getLogger("universe")


@dataclass(slots=True)
class ContractSpec:
    symbol: str
    base: str
    contract_size: float
    price_unit: float
    state: int
    api_allowed: bool = True

    @property
    def price_decimals(self) -> int:
        if self.price_unit <= 0:
            return 6
        return max(0, min(12, int(round(-math.log10(self.price_unit)))))


def contracts_to_usd(contracts: float, contract_size: float, price: float) -> float:
    return float(contracts) * float(contract_size) * float(price)


def normalize_symbol(raw: str) -> str:
    """'agt', 'AGTUSDT', 'AGT_USDT' → 'AGT_USDT'."""
    s = raw.strip().upper().replace("-", "_").replace("/", "_")
    if s.endswith("_USDT"):
        return s
    if s.endswith("USDT") and len(s) > 4:
        return s[:-4] + "_USDT"
    return s + "_USDT"


def futures_to_spot(symbol: str) -> str:
    """'AGT_USDT' → 'AGTUSDT'."""
    return symbol.replace("_", "")


class Universe:
    """Все включённые USDT-перпетуалы MEXC (state == 0, символ оканчивается на _USDT)."""

    def __init__(self) -> None:
        self.specs: dict[str, ContractSpec] = {}
        self.loaded_at_ms = 0

    def __contains__(self, symbol: str) -> bool:
        return symbol in self.specs

    def __len__(self) -> int:
        return len(self.specs)

    def get(self, symbol: str) -> ContractSpec | None:
        return self.specs.get(symbol)

    def load(self, details: list[dict], now_ms: int = 0) -> tuple[set[str], set[str]]:
        """Загружает contract/detail. Возвращает (добавленные, удалённые)."""
        new: dict[str, ContractSpec] = {}
        for d in details:
            sym = d.get("symbol") or ""
            if not sym.endswith("_USDT") or d.get("state") != 0:
                continue
            if d.get("settleCoin") not in (None, "USDT") or d.get("quoteCoin") not in (None, "USDT"):
                continue
            cs = float(d.get("contractSize") or 0)
            if cs <= 0:
                continue
            new[sym] = ContractSpec(
                symbol=sym,
                base=str(d.get("baseCoin") or sym[:-5]),
                contract_size=cs,
                price_unit=float(d.get("priceUnit") or 0),
                state=0,
                api_allowed=bool(d.get("apiAllowed", True)),
            )
        if not new:
            raise ValueError("contract/detail вернул пустой список включённых контрактов")
        added = set(new) - set(self.specs)
        removed = set(self.specs) - set(new)
        self.specs = new
        self.loaded_at_ms = now_ms
        return added, removed

    def remove(self, symbol: str) -> None:
        self.specs.pop(symbol, None)

    def to_usd(self, symbol: str, contracts: float, price: float) -> float:
        spec = self.specs.get(symbol)
        if spec is None:
            raise KeyError(f"Неизвестный контракт {symbol}")
        return contracts_to_usd(contracts, spec.contract_size, price)

    def fmt_price(self, symbol: str, price: float | None) -> str:
        if price is None:
            return "—"
        spec = self.specs.get(symbol)
        dec = spec.price_decimals if spec else (6 if price < 1 else 4)
        return f"{price:.{dec}f}"
