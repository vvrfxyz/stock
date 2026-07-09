"""分钟线 -> 日频微观结构特征（ClickHouse 内单遍聚合，供技术因子层使用）。

特征表 stock.minute_daily_features（ReplacingMergeTree，幂等重建）：
- n_bars / rth_volume / rth_dollar / total_volume：常规时段 bar 数与量额、全时段量
- ret_first30 / ret_last30：开盘半小时收益、尾盘半小时收益（日内动量文献口径）
- rv / rskew / bipower：5 分钟子采样对数收益的已实现方差 / 已实现偏度
  （Amaya et al. 2015 口径 sqrt(n)*Σr³/RV^1.5）/ 双幂变差（跳跃分离用）
- rv_up / rv_down：上/下行半方差（Bollerslev-Li-Zhao 2020 的 RSJ = (rv_up-rv_down)/rv 用）
- ext_volume_share：盘前盘后成交量占比（信息/关注度代理）
- vol_last30_share：尾盘半小时成交量占常规时段比（EOD 资金流强度，反转条件化用）
- cs_spread / roll_spread：bar-based 有效价差估计（散户成本面基建，roadmap §6）——
  分钟线无 bid/ask，用 OHLC 型估计器两法对拍。**均为相对价差（价格分数）**。

## cs_spread：Corwin-Schultz（2012）high-low 估计器

口径选择（roadmap §6 点名须写死理由）：
- **周期 = 5 分钟桶**：桶内 max(high)/min(low) 为该周期的高低价。用 maxMapIf/minMapIf
  在日级 GROUP BY 内按桶做 max/min 预聚合——聚合态 = 桶数（RTH ~78/日），不是 bar 数
  （~390/日），避免 groupArrayIf 收全 RTH bar 元组的 6-12GB 内存爆掉 11G 共享主机
  （roadmap 实measure 约束）。
- **相邻桶配对**：数组相邻两桶（且桶下标差 == 1，即时间真相邻，跳过缺 bar 的空桶断裂）
  为 CS 的两个连续周期。单周期极差 β = ln(H_i/L_i)² + ln(H_{i+1}/L_{i+1})²；
  两周期极差 γ = ln(max(H_i,H_{i+1}) / min(L_i,L_{i+1}))²（相邻桶**并集**高低）。
  α = (√(2β)−√β)/(3−2√2) − √(γ/(3−2√2))；S = 2(e^α−1)/(e^α+1) = 2·tanh(α/2)。
- **日内多对取均值**：当日全部有效对的 S 求均值。

## roll_spread：Roll（1984）估计器

- 复用现成 5 分钟子采样对数收益数组 rets 的**一阶自协方差**（去均值，除以对数 N−1）；
  spread = 2√(−cov)。Roll 假设无漂移，去均值消除日内漂移对协方差的偏置。
- 频率注记：这里 Roll 建在 5 分钟子采样上（与 rv 同频），bid-ask bounce 自协方差在
  5 分钟频率上会被衰减——**Roll 是对拍/交叉校验腿，CS 为主估计器**（roadmap 两法对拍）。

## 病理值处理（预注册，roadmap §6 写死——CS 负估计是 residual_vol clip(0) 同型陷阱）

- CS：单对负估计（α<0 <=> S<0）一律**剔除该对（nan）不 clip 不填 0**，日内均值只取
  非负对；有效对数 < CS_MIN_PAIRS 或 n_bars<100 -> 整日 nan。
  （注：CS 原文把负 2-周期估计置 0 再平均——那正是被预注册禁止的"填 0"，本实现改为剔除。）
- Roll：**正自协方差**（cov>=0，含 nan）一律 -> nan（不 clip）；n_sub < ROLL_MIN_OBS 亦 nan。
- 覆盖率损失并入"无分钟覆盖票"fallback 通道统计（因子层）。

口径与 PIT：全部特征只用当日 ≤16:00 ET 的 bar，t 日收盘即可得，无前视。
5 分钟子采样（分钟数 %5==0 的 bar 收盘价）抗微观结构噪声；n_bars<100 或
子采样点 <30 的日子矩量置 0（因子层按 n_bars 过滤）。未复权原始价——日内
收益率/极差不受复权影响（同日无除权断点；除权日隔夜段不进这些特征，故价差估计
天然日内安全，不跨除权断点）。

新列上线顺序（team-lead 编排；本模块只写码不执行 DDL/INSERT/重跑）：
先执行 ADD_COLUMNS_DDL 建列（DEFAULT nan——旧行未重跑前新列缺席而非 0），
再逐年 build_year 幂等重跑填值。

用法（Mac 上直连 253，或 253 本机）：
    RESEARCH_CLICKHOUSE_URL=http://192.168.1.253:8123 \
        .venv/bin/python -m research.minute_features --years 2003-2026
"""
from __future__ import annotations

import argparse

from dotenv import load_dotenv
import sys
import time
from datetime import timedelta

import numpy as np

from research.minute_bars import clickhouse_url, query_df

# 病理/样本量门槛（预注册，roadmap §6）。CS 需足够多有效对求稳日均；Roll 复用矩量的
# n_sub>=30 阈值（与 rskew/bipower 一致，>=29 个协方差配对）。
N_SHARDS = 4  # 月内 security_id 哈希分片数（聚合预留账面 ÷N）
CS_MIN_PAIRS = 10
ROLL_MIN_OBS = 30

# team-lead 执行：建列后再逐年重跑。Float64（NaN 是合法 IEEE 值，nan 直入无需 Nullable）。
# DEFAULT nan：CH 对旧行默认填 0，而 0 价差是"免费交易"病理值——未重跑年份会被
# 下游当真（与预注册"无效置 NaN"冲突）。DEFAULT nan 让未重跑行天然缺席。
ADD_COLUMNS_DDL = (
    "ALTER TABLE stock.minute_daily_features "
    "ADD COLUMN IF NOT EXISTS cs_spread Float64 DEFAULT nan AFTER vol_last30_share, "
    "ADD COLUMN IF NOT EXISTS roll_spread Float64 DEFAULT nan AFTER cs_spread"
)

# CS 常数 3 − 2√2 = 0.171572875253810。SQL 内写 (3 - 2 * sqrt(2)) 由 CH 求值。
_CS_K = 3.0 - 2.0 * np.sqrt(2.0)

EXTRACT_SQL_TEMPLATE = """
INSERT INTO stock.minute_daily_features
    (security_id, d, n_bars, rth_volume, rth_dollar, total_volume,
     ret_first30, ret_last30, rv, rv_up, rv_down, rskew, bipower, ext_volume_share,
     vol_last30_share, cs_spread, roll_spread)
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
    if(rth_volume > 0, vol_last30 / rth_volume, 0) AS vol_last30_share,
    -- cs_spread：非负有效对（>= {cs_min_pairs}）日均，n_bars<100 或不足 -> nan（预注册病理口径）
    if(n_bars >= 100 AND length(cs_valid) >= {cs_min_pairs}, arrayAvg(cs_valid), nan) AS cs_spread,
    -- roll_spread：负自协方差 -> 2√(−cov)，正/缺失 -> nan（预注册病理口径）
    if(isFinite(roll_cov) AND roll_cov < 0, 2 * sqrt(-roll_cov), nan) AS roll_spread
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
        arraySum(x -> x*x, rets) AS rv,
        -- 以下均为既有列之外的**追加别名**（cs/roll 派生），既有列表达式与取值不变。
        -- 复用 ClickHouse 同 SELECT 内别名链引用（如既有 sub->rets->rv 链），不加包裹层，
        -- 故既有行逐字符/缩进不动，diff 证明既有列零改动。
        arraySum(rets) AS rets_sum,
        if(n_sub >= 1, rets_sum / n_sub, 0.0) AS rmean,
        -- 5 分钟桶（intDiv(md,5)）内 max(high)/min(low)，maxMap/minMap 聚合态=桶数（非 bar 数）
        maxMapIf([intDiv(md, 5)], [high], rth) AS cs_hi_map,
        minMapIf([intDiv(md, 5)], [low], rth) AS cs_lo_map,
        -- 桶高低价转 Float64：minute_bars 的 high/low 是 Float32；CS 的 ln/sqrt/tanh 链在
        -- Float32 上数值质量差，Float64 提升质量（CH fast-math transcendentals 仍有 ~1e-4 残差）
        arrayMap(v -> toFloat64(v), cs_hi_map.2) AS cs_hi,
        arrayMap(v -> toFloat64(v), cs_lo_map.2) AS cs_lo,
        cs_hi_map.1 AS cs_bkt,
        -- 相邻桶（下标差 1）配对 CS：单对负/无效置 nan，isFinite 过滤后留非负对
        arrayFilter(x -> isFinite(x) AND x >= 0,
            arrayMap((a, c, b, d, k1, k2) ->
                if(a > 0 AND c > 0 AND b > 0 AND d > 0 AND (toInt32(k2) - toInt32(k1)) = 1,
                   2 * tanh(
                       ( (sqrt(2 * (pow(ln(a / c), 2) + pow(ln(b / d), 2)))
                          - sqrt(pow(ln(a / c), 2) + pow(ln(b / d), 2))) / (3 - 2 * sqrt(2))
                         - sqrt(pow(ln(greatest(a, b) / least(c, d)), 2) / (3 - 2 * sqrt(2))) ) / 2),
                   nan),
                arraySlice(cs_hi, 1, length(cs_hi) - 1),
                arraySlice(cs_lo, 1, length(cs_lo) - 1),
                arraySlice(cs_hi, 2),
                arraySlice(cs_lo, 2),
                arraySlice(cs_bkt, 1, length(cs_bkt) - 1),
                arraySlice(cs_bkt, 2))) AS cs_valid,
        -- Roll 一阶自协方差（去均值 rmean，除以配对数 n_sub-1）
        if(n_sub >= {roll_min_obs},
           arraySum(arrayMap((x, y) -> (x - rmean) * (y - rmean),
                    arraySlice(rets, 2), arraySlice(rets, 1, length(rets) - 1)))
           / toFloat64(n_sub - 1),
           nan) AS roll_cov
    FROM (
        SELECT security_id, ts, open, close, high, low, volume,
               toHour(ts, 'America/New_York') * 60 + toMinute(ts, 'America/New_York') AS md,
               md BETWEEN 570 AND 959 AS rth
        FROM stock.minute_bars
        WHERE toYear(ts) = {year} AND toMonth(ts) = {month}
          AND cityHash64(security_id) % {n_shards} = {shard}
    )
    GROUP BY security_id, d
)
-- 内存策略（2026-07-09，七轮实测定案）：数组聚合态（groupArrayIf/maxMapIf）不可外部溢盘
-- ——溢盘文件读回合并阶段（SourceFromNativeStream）集中物化数组，比不溢盘更炸且非确定。
-- 定案 = 逐月分块（单月聚合态 ~1.5G）+ max_threads=4 + query 4G ≤ 服务端 4.5G，不溢盘。
SETTINGS max_memory_usage = 4000000000, max_threads = 2
"""


def _corwin_schultz_spread(
    bucket_high: np.ndarray,
    bucket_low: np.ndarray,
    bucket_idx: np.ndarray,
    *,
    min_pairs: int = CS_MIN_PAIRS,
) -> float:
    """Corwin-Schultz 相对价差的 Python 参考实现（与 EXTRACT_SQL_TEMPLATE 逐式对应）。

    输入按桶升序排列的 5 分钟桶 max-high / min-low / 桶下标。相邻桶（下标差 1）
    配对求 S；负/无效对剔除；非负对 < min_pairs 返回 NaN。供金测试与 253 抽样交叉校验。
    """
    hi = np.asarray(bucket_high, dtype="float64")
    lo = np.asarray(bucket_low, dtype="float64")
    bk = np.asarray(bucket_idx, dtype="int64")
    k = 3.0 - 2.0 * np.sqrt(2.0)
    spreads: list[float] = []
    for i in range(len(hi) - 1):
        a, c, b, d = hi[i], lo[i], hi[i + 1], lo[i + 1]
        if not (a > 0 and c > 0 and b > 0 and d > 0 and int(bk[i + 1]) - int(bk[i]) == 1):
            continue
        beta = np.log(a / c) ** 2 + np.log(b / d) ** 2
        gamma = np.log(max(a, b) / min(c, d)) ** 2
        alpha = (np.sqrt(2 * beta) - np.sqrt(beta)) / k - np.sqrt(gamma / k)
        s = 2.0 * np.tanh(alpha / 2.0)
        if np.isfinite(s) and s >= 0:
            spreads.append(float(s))
    if len(spreads) < min_pairs:
        return float("nan")
    return float(np.mean(spreads))


def _roll_spread(rets: np.ndarray, *, min_obs: int = ROLL_MIN_OBS) -> float:
    """Roll（1984）相对价差的 Python 参考实现（与 EXTRACT_SQL_TEMPLATE 逐式对应）。

    rets 一阶去均值自协方差（除以配对数 N−1）；正/零/缺失自协方差 -> NaN；
    n < min_obs -> NaN；否则 2√(−cov)。
    """
    r = np.asarray(rets, dtype="float64")
    n = len(r)
    if n < min_obs:
        return float("nan")
    m = r.mean()
    cov = float(np.sum((r[1:] - m) * (r[:-1] - m)) / (n - 1))
    if not (np.isfinite(cov) and cov < 0):
        return float("nan")
    return 2.0 * np.sqrt(-cov)


def build_year(year: int) -> float:
    import requests

    start = time.monotonic()
    # 开局先还账：追踪器虚账跨查询/跨会话存续（第七轮实测：上一批失败查询留下的
    # 账面让 2025-01 开局即撞 4.5G），不能只在逐月间 purge。
    requests.post(clickhouse_url(), params={"query": "SYSTEM JEMALLOC PURGE"}, timeout=120)
    requests.post(
        clickhouse_url(),
        params={"query": f"ALTER TABLE stock.minute_daily_features DROP PARTITION {year}"},
        timeout=600,
    )
    # 按月分块 INSERT（2026-07-09 定案）：groupArrayIf 的数组聚合状态不可溢盘收缩
    # （external_group_by 对它无效——五轮实测：查询 RSS 压到 1.38G 总账仍 4.5G+），
    # 整年 ~250 万 (security,day) 组的子采样数组必须同驻内存。逐月把聚合态砍 12 倍，
    # DROP PARTITION 整年一次 + 12 次 INSERT，幂等语义不变。
    # 分片终案（2026-07-09 九轮实测）：groupArrayIf 状态按【预留容量】进追踪器总账
    # （账面精确爬到任意帽而 RSS 只及一半——虚拟预留非真实内存，参数/purge 皆不可治）。
    # 月内按 security_id 哈希 4 片，聚合组数与预留账面确定性 ÷4；GROUP BY 键分片不相交，
    # INSERT 追加语义与幂等（年分区 DROP 一次）不变。
    for month in range(1, 13):
        for shard in range(N_SHARDS):
            sql = EXTRACT_SQL_TEMPLATE.format(
                year=year, month=month, n_shards=N_SHARDS, shard=shard,
                cs_min_pairs=CS_MIN_PAIRS, roll_min_obs=ROLL_MIN_OBS
            )
            response = requests.post(clickhouse_url(), data=sql.encode(), timeout=3600)
            if response.status_code != 200:
                raise RuntimeError(
                    f"{year}-{month:02d} shard {shard}/{N_SHARDS} 特征提取失败: {response.text[:400]}")
        # jemalloc 把释放内存留在 arena，CH 总账跨查询累积虚高（六轮实测：逐月分块后
        # 前 4 月过、第 5 月被虚账杀——RSS 1.5G 账面 4.5G）。逐月强制还账。
        requests.post(clickhouse_url(), params={"query": "SYSTEM JEMALLOC PURGE"}, timeout=120)
    return time.monotonic() - start


def main(argv: list[str] | None = None) -> int:
    load_dotenv()  # systemd-run 洗净环境（run_research.sh 发射）下 .env 是唯一的连库配置来源
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
