from __future__ import annotations

from dataclasses import FrozenInstanceError, dataclass

import pandas as pd
import pytest

from research.factors import protocol as _proto
from research.factors.protocol import Factor, FactorContext, get, list_factors, register


@pytest.fixture(autouse=True)
def _isolate_registry():
    saved = dict(_proto._REGISTRY)
    yield
    _proto._REGISTRY.clear()
    _proto._REGISTRY.update(saved)


@dataclass(frozen=True)
class DummyFactor:
    name: str

    def compute(self, ctx: FactorContext) -> pd.DataFrame:
        return pd.DataFrame(index=ctx.dates, columns=ctx.security_universe, dtype="float64")


def test_register_and_get():
    factor = DummyFactor("dummy")

    register(factor)

    assert get("dummy") is factor


def test_register_duplicate_raises():
    register(DummyFactor("dummy_dup"))


    with pytest.raises(ValueError, match="factor 'dummy_dup' already registered"):
        register(DummyFactor("dummy_dup"))


def test_list_factors_sorted():
    register(DummyFactor("zeta"))
    register(DummyFactor("alpha"))
    register(DummyFactor("middle"))

    listed = list_factors()
    assert listed == sorted(listed)
    assert listed.index("alpha") < listed.index("middle") < listed.index("zeta")


def test_factor_context_is_frozen():
    ctx = FactorContext(
        engine=object(),
        dates=pd.DatetimeIndex(pd.to_datetime(["2026-01-02"])),
        security_universe=pd.Index([1], dtype="int64"),
    )

    with pytest.raises(FrozenInstanceError):
        ctx.as_of = pd.Timestamp("2026-01-03")


def test_factor_protocol_compliance():
    from research.factors.builtins.earnings_yield import EarningsYieldFactor
    from research.factors.builtins.size import SizeFactor

    assert isinstance(SizeFactor(), Factor)
    assert isinstance(EarningsYieldFactor(), Factor)


def test_factor_name_is_class_level():
    from research.factors.builtins.size import SizeFactor

    assert SizeFactor.name == "size"
    with pytest.raises(TypeError, match="unexpected keyword argument"):
        SizeFactor(name="custom")
