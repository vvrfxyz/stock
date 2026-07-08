"""composite_v2 复合信号注册因子（骨架：composite_v1 三件套 + operating_profitability）。

【预注册（2026-07-08，roadmap §7；跑数前提交）】
- 成分**写死**：low_vol + high_52w 逐日残差 + size + operating_profitability（k_total=4，
  0.5 中性填补，主干 low_vol 在场门）。基本面腿取 OP 不取 earnings_yield 的依据：
  2026-07-08 六因子互 partial——EY|OP=.0047（衰减 75%）而 OP|EY 仅衰 38%，OP ⊃ EY
  （ledger wave-12 行）；不同时纳入两者（近亲成分双计权重=伪分散）。
- 复合机制原样继承 v1（残差化只做 high_52w 对 low_vol；不给 OP 加残差化——
  OP 三关全过、与各成分秩相关 0.25-0.37，无共线处置必要；新增任何正交化步骤
  都须另行预注册）。
- 判据（roadmap §7 双条件，同窗同口径）：composite_v2 的 IC IR(h5) 与 q5 净
  Sharpe(h21) **同时严格优于**（a）最优单成分与（b）composite_v1——v1 基线以
  2012+ 窗口、P2 退市注入新口径**同批重算**（旧 0.1462/0.718 是 2016+ 旧口径，
  不可直比）。
- 终审：retail_reality 双口径（判定以保守口径为准），PASS → 30 只月频纸面组合；
  FAIL → ledger 记账，不做参数挽救。
- 窗口：2012-01-03 ~ 2026-07-02（OP/XBRL 覆盖 + 留 TTM 暖机；v1 基线同窗重算）。

元属性：lookback_days=252（high_52w）；lag_days=1；adr_unsafe=True（含 size 腿）。
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import ClassVar

from research.factors.builtins.composite_v1 import (
    build_composite,
    composite_eligibility,
    eligible_component_ranks,
)
from research.factors.protocol import FactorContext, register

# 定案成分（写死，预注册见模块 docstring）。低波动是主干（在场门）。
COMPONENTS_V2: tuple[str, ...] = ("low_vol", "high_52w", "size", "operating_profitability")


@dataclass(frozen=True)
class CompositeV2Factor:
    name: ClassVar[str] = "composite_v2"
    lookback_days: ClassVar[int] = 252
    lag_days: ClassVar[int] = 1
    pit_guarantee: ClassVar[bool] = True
    adr_unsafe: ClassVar[bool] = True  # 含 size 腿（§E.3 同 composite_v1）

    def compute(self, ctx: FactorContext):
        eligible = composite_eligibility(ctx)
        ranks = eligible_component_ranks(ctx, eligible, COMPONENTS_V2)
        composite = build_composite(ranks, COMPONENTS_V2)
        return composite.reindex(index=ctx.dates, columns=ctx.security_universe).astype("float64")


register(CompositeV2Factor())
