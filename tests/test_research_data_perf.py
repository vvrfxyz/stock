"""research.data 性能重构的语义锁定测试（2026-07）。

覆盖三件事：
- to_wide 从 pivot_table(aggfunc="last") 换 pivot：PK 唯一数据上输出逐位一致，
  重复键时回退 pivot_table 旧聚合语义。
- load_adjusted_panel 进程内记忆化：同 key 只装载一次；命中返回新 dict、
  同 DataFrame 对象；调用方 rebind dict 条目不污染缓存。
- load_price_long 列裁剪：白名单校验 + load_adjusted_panel 只请求 close/volume。
"""
from __future__ import annotations

from datetime import date

import numpy as np
import pandas as pd
import pytest

import research.data as rd
from research.data import clear_panel_cache, load_adjusted_panel, load_price_long, to_wide


@pytest.fixture(autouse=True)
def _fresh_panel_cache():
    clear_panel_cache()
    yield
    clear_panel_cache()


def _long_frame(n_sids: int = 5, n_days: int = 7, seed: int = 3) -> pd.DataFrame:
    """模拟 SQL `order by security_id, date` 的长表（含 NaN 洞、int32 id）。"""
    rng = np.random.default_rng(seed)
    dates = pd.bdate_range("2024-01-02", periods=n_days)
    rows = []
    for sid in range(1, n_sids + 1):
        for d in dates:
            rows.append({
                "security_id": sid,
                "date": d,
                "close": float(rng.uniform(10, 100)),
                "volume": float(rng.integers(1_000, 9_999)),
            })
    df = pd.DataFrame(rows)
    df["security_id"] = df["security_id"].astype(np.int32)
    # 打洞：停牌段 + 零星缺失（但不制造整列 NaN——那是 pivot/pivot_table 的已知边界差异）
    df.loc[3, "close"] = np.nan
    df.loc[11, "volume"] = np.nan
    return df.sort_values(["security_id", "date"]).reset_index(drop=True)


class TestToWidePivotEquivalence:
    def test_unique_keys_bitwise_identical_to_pivot_table(self):
        df = _long_frame()
        for column in ("close", "volume"):
            got = to_wide(df, column)
            ref = df.pivot_table(index="date", columns="security_id", values=column, aggfunc="last")
            pd.testing.assert_frame_equal(got, ref)

    def test_duplicate_keys_fall_back_to_pivot_table_last(self):
        df = _long_frame()
        dup = pd.concat([df, df.iloc[[0]].assign(close=999.0)], ignore_index=True)
        got = to_wide(dup, "close")
        ref = dup.pivot_table(index="date", columns="security_id", values="close", aggfunc="last")
        pd.testing.assert_frame_equal(got, ref)
        # aggfunc="last" 语义：重复键取长表中最后一行
        assert got.iloc[0, 0] == 999.0


class _FakeUrl:
    def __init__(self, tag: str) -> None:
        self._tag = tag

    def __str__(self) -> str:
        return f"postgresql://fake/{self._tag}"


class _FakeEngine:
    def __init__(self, tag: str = "db") -> None:
        self.url = _FakeUrl(tag)


@pytest.fixture()
def patched_loaders(monkeypatch):
    """monkeypatch 掉 DB 依赖，记录 load_price_long 的调用次数与 kwargs。"""
    calls: list[dict] = []

    def fake_load_price_long(engine, *, start, end, types=("CS",), include_inactive=True,
                             security_ids=None, columns=rd.PRICE_COLUMNS):
        calls.append({"columns": columns, "types": types, "include_inactive": include_inactive})
        df = _long_frame()
        return df[["security_id", "date", *columns]]

    empty_events = pd.DataFrame(columns=["security_id", "ex_date", "cumulative_factor"])
    monkeypatch.setattr(rd, "load_price_long", fake_load_price_long)
    monkeypatch.setattr(rd, "load_factor_events", lambda *a, **k: empty_events)
    return calls


class TestPanelMemoization:
    KW = dict(start=date(2024, 1, 2), end=date(2024, 1, 10))

    def test_same_key_loads_once_and_shares_frames_via_new_dict(self, patched_loaders):
        engine = _FakeEngine()
        p1 = load_adjusted_panel(engine, **self.KW)
        p2 = load_adjusted_panel(engine, **self.KW)

        assert len(patched_loaders) == 1
        assert p1 is not p2                                  # 新 dict
        for key in ("adj_close", "close", "volume", "dollar_volume"):
            assert p1[key] is p2[key]                        # 同 DataFrame 对象

    def test_caller_rebinding_does_not_corrupt_cache(self, patched_loaders):
        engine = _FakeEngine()
        p1 = load_adjusted_panel(engine, **self.KW)
        original_close = p1["close"]
        # 调用方惯用法（run_baselines/evaluate）：rebind dict 条目、不改宽表
        p1["close"] = p1["close"].drop(columns=[1])
        p1["adj_close"] = p1["adj_close"].loc[:, [2, 3]]

        p3 = load_adjusted_panel(engine, **self.KW)
        assert p3["close"] is original_close
        assert list(p3["close"].columns) == [1, 2, 3, 4, 5]

    def test_key_covers_all_load_parameters(self, patched_loaders):
        engine = _FakeEngine()
        load_adjusted_panel(engine, **self.KW)
        load_adjusted_panel(engine, **self.KW, include_inactive=False)
        load_adjusted_panel(engine, **self.KW, types=("CS", "ETF"))
        load_adjusted_panel(engine, **self.KW, as_of=date(2024, 1, 5))
        load_adjusted_panel(_FakeEngine("other"), **self.KW)
        assert len(patched_loaders) == 5

    def test_as_of_none_equals_as_of_end(self, patched_loaders):
        """effective_as_of = as_of or end：显式传 end 与缺省是同一口径、同一缓存项。"""
        engine = _FakeEngine()
        load_adjusted_panel(engine, **self.KW)
        load_adjusted_panel(engine, **self.KW, as_of=self.KW["end"])
        assert len(patched_loaders) == 1

    def test_cache_is_bounded(self, patched_loaders):
        engine = _FakeEngine()
        for day in (2, 3, 4, 5):
            load_adjusted_panel(engine, start=date(2024, 1, 2), end=date(2024, 1, day + 10))
        assert len(rd._ADJUSTED_PANEL_CACHE) <= rd._ADJUSTED_PANEL_CACHE_MAX

    def test_clear_panel_cache(self, patched_loaders):
        engine = _FakeEngine()
        load_adjusted_panel(engine, **self.KW)
        clear_panel_cache()
        load_adjusted_panel(engine, **self.KW)
        assert len(patched_loaders) == 2


class TestColumnPruning:
    def test_load_adjusted_panel_requests_close_volume_only(self, patched_loaders):
        panel = load_adjusted_panel(_FakeEngine(), start=date(2024, 1, 2), end=date(2024, 1, 10))

        assert patched_loaders[0]["columns"] == ("close", "volume")
        # 返回键形态不变，dollar_volume 仍是 close×volume
        assert set(panel) == {"adj_close", "close", "volume", "dollar_volume"}
        pd.testing.assert_frame_equal(panel["dollar_volume"], panel["close"] * panel["volume"])
        # 无复权事件时 adj_close == close
        pd.testing.assert_frame_equal(panel["adj_close"], panel["close"], check_names=False)

    def test_load_price_long_rejects_unknown_columns(self):
        with pytest.raises(ValueError, match="不支持的价格列"):
            load_price_long(None, start=date(2024, 1, 2), end=date(2024, 1, 10),
                            columns=("close", "trade_count"))
        with pytest.raises(ValueError, match="不能为空"):
            load_price_long(None, start=date(2024, 1, 2), end=date(2024, 1, 10), columns=())
