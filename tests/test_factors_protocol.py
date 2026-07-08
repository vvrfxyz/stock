from __future__ import annotations

from dataclasses import FrozenInstanceError, dataclass

import pandas as pd
import pytest

from research.factors import protocol as _proto
from research.factors.protocol import Factor, FactorContext, get, list_factors, register


@pytest.fixture(autouse=True)
def _isolate_registry():
    # 先触发全部 builtins 注册再快照——否则本文件在"builtins 尚未导入"的执行顺序下
    # （如手选子集从 protocol 起跑）快照为空；本文件测试体内的 import（size/earnings_yield）
    # 会注册这些因子，但 teardown 恢复到空快照把它们抹掉，且 Python 模块缓存使其无法再
    # 注册 —— 污染后续 test_research_adr_optin 的"恰好这些因子标 adr_unsafe"断言。
    # 全量 suite 侥幸通过是因为字母序里 test_factors_builtins 先跑、已把 builtins 注册满，
    # 首次快照即完整；只有 protocol 先于任何 builtin-导入测试时才触发。
    import research.evaluate  # noqa: F401  确保 _REGISTRY 完整后再快照
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
