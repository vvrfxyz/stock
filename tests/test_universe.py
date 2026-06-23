"""Universe mask 构建的单元测试。

不依赖 PostgreSQL——直接构造面板验证 mask 逻辑。
"""
from datetime import date

import numpy as np
import pandas as pd

from research.universe import _build_listed_mask, build_universe_mask, universe_hash_from_ids


def _dates(*ds):
    return pd.DatetimeIndex([pd.Timestamp(d) for d in ds])


def test_listed_mask_respects_list_and_delist_dates():
    dates = _dates("2025-01-01", "2025-01-02", "2025-01-03", "2025-01-06", "2025-01-07")
    sec_dates = pd.DataFrame({
        "security_id": [1, 2, 3],
        "list_date": [pd.Timestamp("2025-01-01"), pd.Timestamp("2025-01-03"), pd.NaT],
        "delist_date": [pd.Timestamp("2025-01-06"), pd.NaT, pd.NaT],
    })
    mask = _build_listed_mask(sec_dates, dates, [1, 2, 3])

    # sec 1: listed 01-01 to 01-06, NOT on 01-07
    assert mask.loc["2025-01-01", 1] is np.True_
    assert mask.loc["2025-01-06", 1] is np.True_
    assert mask.loc["2025-01-07", 1] is np.False_

    # sec 2: listed from 01-03 onward
    assert mask.loc["2025-01-02", 2] is np.False_
    assert mask.loc["2025-01-03", 2] is np.True_
    assert mask.loc["2025-01-07", 2] is np.True_

    # sec 3: no list_date -> conservative, always listed
    assert mask.loc["2025-01-01", 3] is np.True_
    assert mask.loc["2025-01-07", 3] is np.True_


def test_listed_mask_empty_security_ids():
    dates = _dates("2025-01-01", "2025-01-02")
    sec_dates = pd.DataFrame({"security_id": [], "list_date": [], "delist_date": []})
    mask = _build_listed_mask(sec_dates, dates, [])
    assert mask.shape == (2, 0)


def test_universe_hash_deterministic():
    h1 = universe_hash_from_ids([3, 1, 2], date(2025, 1, 1), date(2025, 12, 31))
    h2 = universe_hash_from_ids([1, 2, 3], date(2025, 1, 1), date(2025, 12, 31))
    assert h1 == h2  # sorted, so order doesn't matter
    assert len(h1) == 16


def test_universe_hash_changes_with_different_ids():
    h1 = universe_hash_from_ids([1, 2, 3], date(2025, 1, 1), date(2025, 12, 31))
    h2 = universe_hash_from_ids([1, 2, 4], date(2025, 1, 1), date(2025, 12, 31))
    assert h1 != h2


def test_has_price_mask_filters_by_adj_close():
    dates = _dates("2025-01-01", "2025-01-02", "2025-01-03")
    adj_close = pd.DataFrame(
        {1: [100.0, np.nan, 102.0], 2: [np.nan, 200.0, 201.0]},
        index=dates,
    )
    sec_dates = pd.DataFrame({
        "security_id": [1, 2],
        "list_date": [pd.Timestamp("2025-01-01"), pd.Timestamp("2025-01-01")],
        "delist_date": [pd.NaT, pd.NaT],
    })
    listed = _build_listed_mask(sec_dates, dates, [1, 2])
    has_price = adj_close.notna() & listed

    assert has_price.loc["2025-01-01", 1] is np.True_
    assert has_price.loc["2025-01-02", 1] is np.False_  # no price
    assert has_price.loc["2025-01-01", 2] is np.False_  # no price
    assert has_price.loc["2025-01-02", 2] is np.True_
