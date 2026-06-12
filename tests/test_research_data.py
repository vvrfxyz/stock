"""research.data 批量复权与 utils.adjusted_prices 单标的读取层的口径一致性测试。"""
from datetime import date
from decimal import Decimal

import numpy as np
import pandas as pd

from research.data import apply_adjustment
from utils.adjusted_prices import factor_for_date


def _vector_factors(prices: pd.DataFrame, events: pd.DataFrame) -> np.ndarray:
    out = apply_adjustment(prices, events)
    return (out["adj_close"] / out["close"]).to_numpy()


def test_apply_adjustment_matches_reference_semantics():
    # 证券 7：两个事件；证券 9：无事件
    prices = pd.DataFrame(
        {
            "security_id": np.array([7, 7, 7, 7, 9, 9], dtype=np.int32),
            "date": pd.to_datetime(
                ["2024-01-02", "2024-02-01", "2024-03-01", "2024-04-01", "2024-01-02", "2024-02-01"]
            ),
            "close": [10.0, 11.0, 12.0, 13.0, 50.0, 51.0],
        }
    )
    events = pd.DataFrame(
        {
            "security_id": [7, 7],
            "ex_date": pd.to_datetime(["2024-02-15", "2024-03-15"]),
            "cumulative_factor": [0.5, 0.8],
        }
    )

    got = _vector_factors(prices, events)

    ref_events = [(date(2024, 2, 15), Decimal("0.5")), (date(2024, 3, 15), Decimal("0.8"))]
    expected_7 = [
        float(factor_for_date(ref_events, d))
        for d in [date(2024, 1, 2), date(2024, 2, 1), date(2024, 3, 1), date(2024, 4, 1)]
    ]
    assert np.allclose(got[:4], expected_7)  # 0.5, 0.5, 0.8, 1.0
    assert np.allclose(got[4:], 1.0)


def test_apply_adjustment_event_on_bar_date_uses_next_event():
    # ex_date 当天 bar 不再应用该事件（factor 取 ex_date > bar_date 的第一个事件）
    prices = pd.DataFrame(
        {
            "security_id": np.array([3, 3], dtype=np.int32),
            "date": pd.to_datetime(["2024-05-09", "2024-05-10"]),
            "close": [100.0, 25.0],
        }
    )
    events = pd.DataFrame(
        {"security_id": [3], "ex_date": pd.to_datetime(["2024-05-10"]), "cumulative_factor": [0.25]}
    )
    got = _vector_factors(prices, events)
    assert np.allclose(got, [0.25, 1.0])
