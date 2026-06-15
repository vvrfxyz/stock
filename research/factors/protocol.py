from __future__ import annotations

from dataclasses import dataclass
from typing import ClassVar, Protocol, runtime_checkable

import pandas as pd
from sqlalchemy.engine import Engine


@dataclass(frozen=True)
class FactorContext:
    """因子计算的 PIT 上下文。"""

    engine: Engine
    dates: pd.DatetimeIndex
    security_universe: pd.Index
    as_of: pd.Timestamp | None = None


@runtime_checkable
class Factor(Protocol):
    """因子契约: name + compute。"""

    name: ClassVar[str]

    def compute(self, ctx: FactorContext) -> pd.DataFrame:
        """返回 index=ctx.dates, columns=ctx.security_universe 的 float64 宽表。"""
        ...


_REGISTRY: dict[str, Factor] = {}


def register(factor: Factor) -> Factor:
    """同名因子拒绝覆盖。"""
    if factor.name in _REGISTRY:
        raise ValueError(f"factor {factor.name!r} already registered")
    _REGISTRY[factor.name] = factor
    return factor


def get(name: str) -> Factor:
    return _REGISTRY[name]


def list_factors() -> list[str]:
    return sorted(_REGISTRY)
