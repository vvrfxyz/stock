"""分钟价差估计器（cs_spread / roll_spread）语义测试（roadmap §6）。

策略：本地测试栈起不了 ClickHouse，故分两层锁定——
1. **既有 15 列零改动**：断言 fc23b3b 版本的既有输出/聚合表达式子串在新模板中逐字符
   仍在（新列纯增量加入，既有列语义不动）。新加的 bars 投影列 high/low 不被任何既有
   表达式引用，对既有列值中性——见 test_bars_projection_addition_is_value_neutral 说明。
2. **新列 Python 参考实现金测试**：CS/Roll 公式在合成数据上手算对拍 + 病理分支 +
   量级校准（参考实现与 EXTRACT_SQL_TEMPLATE 逐式对应，供 253 只读 SELECT 抽样交叉校验复用）。
"""
from __future__ import annotations

import math

import numpy as np
import pytest

from research.factors.minute_loader import FEATURE_COLUMNS
from research.minute_features import (
    ADD_COLUMNS_DDL,
    CS_MIN_PAIRS,
    EXTRACT_SQL_TEMPLATE,
    ROLL_MIN_OBS,
    _corwin_schultz_spread,
    _roll_spread,
)

# fc23b3b 既有列的权威表达式子串（改动即失败——既有语义金锁）。
_EXISTING_OUTPUT_EXPRS = (
    "if(open_930 > 0 AND close_0959 > 0, close_0959 / open_930 - 1, 0) AS ret_first30,",
    "if(close_1529 > 0 AND close_1559 > 0, close_1559 / close_1529 - 1, 0) AS ret_last30,",
    "arraySum(x -> if(x > 0, x*x, 0), rets) AS rv_up,",
    "arraySum(x -> if(x < 0, x*x, 0), rets) AS rv_down,",
    "sqrt(n_sub) * arraySum(x -> x*x*x, rets) / pow(rv, 1.5), 0) AS rskew,",
    "arraySlice(rets, 2), arraySlice(rets, 1, length(rets) - 1))), 0) AS bipower,",
    "if(total_volume > 0, 1 - rth_volume / total_volume, 0) AS ext_volume_share,",
    "if(rth_volume > 0, vol_last30 / rth_volume, 0) AS vol_last30_share,",
)
_EXISTING_AGG_EXPRS = (
    "countIf(rth) AS n_bars,",
    "sumIf(volume, rth) AS rth_volume,",
    "sumIf(volume, rth AND md >= 930) AS vol_last30,",
    "sumIf(volume * close, rth) AS rth_dollar,",
    "sum(volume) AS total_volume,",
    "argMinIf(open, ts, rth) AS open_930,",
    "argMaxIf(close, ts, rth AND md < 600) AS close_0959,",
    "argMaxIf(close, ts, rth AND md < 930) AS close_1529,",
    "argMaxIf(close, ts, rth) AS close_1559,",
    "groupArrayIf((toUInt32(ts), close), rth AND md % 5 = 0))) AS sub,",
    "arraySlice(arrayDifference(arrayMap(c -> ln(c), sub)), 2), []) AS rets,",
    "arraySum(x -> x*x, rets) AS rv,",
)


class TestExistingColumnsUnchanged:
    @pytest.mark.parametrize("expr", _EXISTING_OUTPUT_EXPRS + _EXISTING_AGG_EXPRS)
    def test_existing_expression_verbatim(self, expr):
        assert expr in EXTRACT_SQL_TEMPLATE, f"既有表达式被改动: {expr!r}"

    def test_existing_insert_column_order_preserved(self):
        # 既有 15 列名在 INSERT 列清单里保持原序，新列 cs_spread/roll_spread 追加其后
        head = EXTRACT_SQL_TEMPLATE.split("SELECT", 1)[0]
        for col in ("n_bars", "rth_volume", "rth_dollar", "total_volume", "ret_first30",
                    "ret_last30", "rv", "rv_up", "rv_down", "rskew", "bipower",
                    "ext_volume_share", "vol_last30_share"):
            assert col in head
        assert head.index("vol_last30_share") < head.index("cs_spread") < head.index("roll_spread")

    def test_bars_projection_addition_is_value_neutral(self):
        # 唯一对 bars 投影的改动 = 增列 high, low（CS 桶聚合用）。既有列无一引用 high/low，
        # 故既有列值中性；此断言锁定"新增仅此两列"。
        assert "SELECT security_id, ts, open, close, high, low, volume," in EXTRACT_SQL_TEMPLATE
        for expr in _EXISTING_OUTPUT_EXPRS + _EXISTING_AGG_EXPRS:
            assert "high" not in expr and "low" not in expr

    def test_replacing_merge_tree_is_read_with_final(self):
        assert "FROM stock.minute_bars FINAL" in EXTRACT_SQL_TEMPLATE


class TestTemplateStructure:
    def test_new_columns_in_insert_and_select(self):
        assert "vol_last30_share, cs_spread, roll_spread)" in EXTRACT_SQL_TEMPLATE
        assert "AS cs_spread," in EXTRACT_SQL_TEMPLATE
        assert "AS roll_spread" in EXTRACT_SQL_TEMPLATE

    def test_renders_without_leftover_braces(self):
        sql = EXTRACT_SQL_TEMPLATE.format(
            year=2020, month=7, n_shards=4, shard=1, cs_min_pairs=CS_MIN_PAIRS, roll_min_obs=ROLL_MIN_OBS
        )
        assert "{" not in sql and "}" not in sql
        assert "toYear(ts) = 2020 AND toMonth(ts) = 7" in sql
        assert "cityHash64(security_id) % 4 = 1" in sql
        assert f"length(cs_valid) >= {CS_MIN_PAIRS}" in sql
        assert f"n_sub >= {ROLL_MIN_OBS}" in sql

    def test_memory_safe_bucket_preaggregation(self):
        # 内存约束：桶级 maxMap/minMap 预聚合（聚合态=桶数），绝不 groupArray 全 RTH high/low
        assert "maxMapIf([intDiv(md, 5)], [high], rth)" in EXTRACT_SQL_TEMPLATE
        assert "minMapIf([intDiv(md, 5)], [low], rth)" in EXTRACT_SQL_TEMPLATE
        assert "groupArrayIf((toUInt32(ts), high" not in EXTRACT_SQL_TEMPLATE

    def test_ddl_constant(self):
        assert "ADD COLUMN IF NOT EXISTS cs_spread Float64 DEFAULT nan" in ADD_COLUMNS_DDL
        assert "ADD COLUMN IF NOT EXISTS roll_spread Float64 DEFAULT nan" in ADD_COLUMNS_DDL


class TestRollReference:
    def test_perfect_bounce_recovers_spread(self):
        # ±1% 交替（纯 bid-ask bounce）：cov = -1e-4，spread = 2√1e-4 = 0.02
        r = np.array([0.01, -0.01] * 20)
        assert _roll_spread(r) == pytest.approx(0.02, abs=1e-9)

    def test_positive_autocov_is_nan(self):
        # 趋势（正自协方差）-> NaN（预注册：不 clip）
        assert math.isnan(_roll_spread(np.array([0.001] * 40)))

    def test_below_min_obs_is_nan(self):
        assert math.isnan(_roll_spread(np.array([0.01, -0.01] * 5)))  # n=10 < 30

    def test_demeaning_removes_drift(self):
        # 叠加常数漂移不改自协方差符号/量级（去均值）
        r = np.array([0.01, -0.01] * 20)
        assert _roll_spread(r + 0.05) == pytest.approx(_roll_spread(r), abs=1e-9)


class TestCorwinSchultzReference:
    def test_single_pair_hand_value(self):
        # 手算锚：桶0[100.5,100.0]、桶1[100.6,100.1] 相邻 -> S≈0.0025780
        cs = _corwin_schultz_spread([100.5, 100.6], [100.0, 100.1], [0, 1], min_pairs=1)
        assert cs == pytest.approx(0.0025780, abs=1e-6)

    def test_non_adjacent_buckets_skipped(self):
        # 桶下标差 != 1（中间有空桶）不配对 -> 无有效对 -> NaN
        assert math.isnan(
            _corwin_schultz_spread([100.5, 100.6], [100.0, 100.1], [0, 2], min_pairs=1)
        )

    def test_min_pairs_gate(self):
        assert math.isnan(
            _corwin_schultz_spread([100.5, 100.6], [100.0, 100.1], [0, 1], min_pairs=5)
        )

    def test_negative_estimate_dropped_not_zeroed(self):
        # 相邻桶间大跳空 -> γ >> β -> α<0 -> 该对剔除（非置 0）；唯一对剔除后 -> NaN
        assert math.isnan(
            _corwin_schultz_spread([100.1, 110.0], [100.0, 109.0], [0, 1], min_pairs=1)
        )

    def test_nonpositive_price_pair_skipped(self):
        assert math.isnan(
            _corwin_schultz_spread([100.5, 0.0], [100.0, 0.0], [0, 1], min_pairs=1)
        )

    def test_magnitude_calibration_large_vs_small_cap(self):
        n = 13
        bk = list(range(n))
        # 大票：恒定中价、极窄固定极差（~2bps/侧，波动小）-> bps 量级
        large = _corwin_schultz_spread([100.01] * n, [99.99] * n, bk, min_pairs=CS_MIN_PAIRS)
        # 小票：恒定中价、宽固定极差（~2%/侧）-> 数百 bps
        small = _corwin_schultz_spread([102.0] * n, [98.0] * n, bk, min_pairs=CS_MIN_PAIRS)
        assert np.isfinite(large) and np.isfinite(small)
        assert large == pytest.approx(0.0002, abs=5e-5)  # ~2 bps
        assert large < small
        assert large < 0.005  # 大票 CS 在 bps 量级（< 50 bps）


class TestLoaderColumns:
    def test_feature_columns_include_spreads(self):
        assert "cs_spread" in FEATURE_COLUMNS
        assert "roll_spread" in FEATURE_COLUMNS
        assert FEATURE_COLUMNS[-1] == "n_bars"  # n_bars 仍在末尾（load 层过滤锚）
