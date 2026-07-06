"""分钟线 -> 日频微观结构特征（ClickHouse 内单遍聚合，供技术因子层使用）。

特征表 stock.minute_daily_features（ReplacingMergeTree，幂等重建）：
- n_bars / rth_volume / rth_dollar / total_volume：常规时段 bar 数与量额、全时段量
- ret_first30 / ret_last30：开盘半小时收益、尾盘半小时收益（日内动量文献口径）
- rv / rskew / bipower：5 分钟子采样对数收益的已实现方差 / 已实现偏度
  （Amaya et al. 2015 口径 sqrt(n)*Σr³/RV^1.5）/ 双幂变差（跳跃分离用）
- rv_up / rv_down：上/下行半方差（Bollerslev-Li-Zhao 2020 的 RSJ = (rv_up-rv_down)/rv 用）
- ext_volume_share：盘前盘后成交量占比（信息/关注度代理）
- vol_last30_share：尾盘半小时成交量占常规时段比（EOD 资金流强度，反转条件化用）

口径与 PIT：全部特征只用当日 ≤16:00 ET 的 bar，t 日收盘即可得，无前视。
5 分钟子采样（分钟数 %5==0 的 bar 收盘价）抗微观结构噪声；n_bars<100 或
子采样点 <30 的日子矩量置 0（因子层按 n_bars 过滤）。未复权原始价——日内
收益率不受复权影响（同日无除权断点；除权日隔夜段不进这些特征）。

用法（Mac 上直连 253，或 253 本机）：
    RESEARCH_CLICKHOUSE_URL=http://192.168.1.253:8123 \
        .venv/bin/python -m research.minute_features --years 2003-2026
"""
from __future__ import annotations

import argparse
import sys
import time
from datetime import timedelta

from research.minute_bars import clickhouse_url, query_df

EXTRACT_SQL_TEMPLATE = """
INSERT INTO stock.minute_daily_features
    (security_id, d, n_bars, rth_volume, rth_dollar, total_volume,
     ret_first30, ret_last30, rv, rv_up, rv_down, rskew, bipower, ext_volume_share,
     vol_last30_share)
SELECT
    security_id, d,
    n_bars, rth_volume, rth_dollar, total_volume,
    if(open_930 > 0 AND close_0959 > 0, close_0959 / open_930 - 1, 0) AS ret_first30,
    if(close_1529 > 0 AND close_1559 > 0, close_1559 / close_1529 - 1, 0) AS ret_last30,
    rv,
    arraySum(x -> if(x > 0, x*x, 0), rets) AS rv_up,
    arraySum(x -> if(x < 0, x*x, 0), rets) AS rv_down,
    if(rv > 0 AND n_sub >= 30,
       sqrt(n_sub) * arraySum(x -> x*x*x, rets) / pow(rv, 1.5), 0) AS rskew,
    if(n_sub >= 30,
       arraySum(arrayMap((a, b) -> abs(a) * abs(b),
                arraySlice(rets, 2), arraySlice(rets, 1, length(rets) - 1))), 0) AS bipower,
    if(total_volume > 0, 1 - rth_volume / total_volume, 0) AS ext_volume_share,
    if(rth_volume > 0, vol_last30 / rth_volume, 0) AS vol_last30_share
FROM (
    SELECT
        security_id,
        toDate(ts, 'America/New_York') AS d,
        countIf(rth) AS n_bars,
        sumIf(volume, rth) AS rth_volume,
        sumIf(volume, rth AND md >= 930) AS vol_last30,
        sumIf(volume * close, rth) AS rth_dollar,
        sum(volume) AS total_volume,
        argMinIf(open, ts, rth) AS open_930,
        argMaxIf(close, ts, rth AND md < 600) AS close_0959,
        argMaxIf(close, ts, rth AND md < 930) AS close_1529,
        argMaxIf(close, ts, rth) AS close_1559,
        arrayMap(x -> x.2, arraySort(x -> x.1,
            groupArrayIf((toUInt32(ts), close), rth AND md % 5 = 0))) AS sub,
        length(sub) AS n_sub_raw,
        if(n_bars >= 100 AND n_sub_raw >= 30,
           arraySlice(arrayDifference(arrayMap(c -> ln(c), sub)), 2), []) AS rets,
        length(rets) AS n_sub,
        arraySum(x -> x*x, rets) AS rv
    FROM (
        SELECT security_id, ts, open, close, volume,
               toHour(ts, 'America/New_York') * 60 + toMinute(ts, 'America/New_York') AS md,
               md BETWEEN 570 AND 959 AS rth
        FROM stock.minute_bars
        WHERE toYear(ts) = {year}
    )
    GROUP BY security_id, d
)
"""


def build_year(year: int) -> float:
    import requests

    start = time.monotonic()
    requests.post(
        clickhouse_url(),
        params={"query": f"ALTER TABLE stock.minute_daily_features DROP PARTITION {year}"},
        timeout=600,
    )
    response = requests.post(
        clickhouse_url(), data=EXTRACT_SQL_TEMPLATE.format(year=year).encode(), timeout=3600)
    if response.status_code != 200:
        raise RuntimeError(f"{year} 特征提取失败: {response.text[:400]}")
    return time.monotonic() - start


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="构建分钟级日频特征表。")
    parser.add_argument("--years", default="2003-2026", help="年份范围，如 2003-2026 或 2014。")
    args = parser.parse_args(argv)
    lo, _, hi = args.years.partition("-")
    years = range(int(lo), int(hi or lo) + 1)

    total_start = time.monotonic()
    for year in years:
        elapsed = build_year(year)
        count = query_df(f"SELECT count() c FROM stock.minute_daily_features WHERE toYear(d)={year}")
        print(f"{year}: {int(count.c.iloc[0]):>9,} 行  {elapsed:5.1f}s")
    total = query_df("SELECT count() c, min(d) lo, max(d) hi FROM stock.minute_daily_features")
    print(f"总计: {int(total.c.iloc[0]):,} 行  {total.lo.iloc[0]} ~ {total.hi.iloc[0]}  "
          f"耗时 {timedelta(seconds=time.monotonic() - total_start)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
