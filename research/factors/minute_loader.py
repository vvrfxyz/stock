"""分钟微观结构特征的因子层加载器（读 stock.minute_daily_features -> as-of 面板）。

PIT 口径：特征 d 日收盘即得（visible = d），因子在 t 日的值只用 d <= t 的特征行
（rolling 窗口天然满足）。流动性门槛：n_bars < min_bars 的日子置 NaN 不参与
rolling 均值（min_periods 控制有效天数下限）。

复权口径说明：这些特征全部是日内比率/矩量（收益率、份额、标准化偏度），
不跨除权日隔夜段，未复权原始价直接正确。
"""
from __future__ import annotations

from datetime import timedelta

import pandas as pd

from research.minute_bars import query_df

FEATURE_COLUMNS = ("ret_first30", "ret_last30", "rv", "rv_up", "rv_down",
                   "rskew", "bipower", "ext_volume_share", "cs_spread", "roll_spread",
                   "n_bars")


def load_minute_feature_panel(
    dates: pd.DatetimeIndex,
    security_ids: list[int],
    columns: tuple[str, ...],
    *,
    buffer_days: int = 45,
    min_bars: int = 100,
    url: str | None = None,
) -> dict[str, pd.DataFrame]:
    """按列名返回 {col: wide DataFrame(index=交易日, columns=security_id)}。

    dates 之前额外拉 buffer_days 个自然日供 rolling 窗口预热；
    n_bars < min_bars 的行所有特征置 NaN（矩量在稀疏 bar 下不可信）。
    """
    if not len(dates) or not security_ids:
        return {col: pd.DataFrame(index=dates) for col in columns}
    need = sorted(set(columns) | {"n_bars"})
    start = (dates[0] - timedelta(days=buffer_days)).date()
    end = dates[-1].date()
    ids = ",".join(str(int(x)) for x in security_ids)
    sql = f"""
        SELECT security_id, d, {", ".join(need)}
        FROM stock.minute_daily_features FINAL
        WHERE d >= '{start}' AND d <= '{end}' AND security_id IN ({ids})
        ORDER BY security_id, d
    """
    frame = query_df(sql, url)
    if frame.empty:
        return {col: pd.DataFrame(index=dates) for col in columns}
    frame["d"] = pd.to_datetime(frame["d"])
    sparse = frame["n_bars"] < min_bars
    for col in need:
        if col != "n_bars":
            frame.loc[sparse, col] = float("nan")
    out: dict[str, pd.DataFrame] = {}
    for col in columns:
        out[col] = frame.pivot_table(index="d", columns="security_id", values=col, aggfunc="last")
    return out
