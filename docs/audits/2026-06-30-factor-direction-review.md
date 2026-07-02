# 因子方向与分位收益复核

- 日期: 2026-06-30
- 状态: 初步复核完成
- 输入: `research/output/evaluate_*_2025-05-16_2026-06-26.md`
- 目的: 解释 IC 与多空 Sharpe 方向冲突，避免误读 `ls_q5_q1`

## 口径说明

评估报告里的 `ls_q5_q1` 是：

- 做多 q5, 即因子值最高的一组。
- 做空 q1, 即因子值最低的一组。

因此，`short_interest_ratio` 的 `ls_q5_q1` Sharpe 为正，含义是“高 short-interest-ratio 组跑赢低 short-interest-ratio 组”，不是“做空高 SI 有效”。如果要验证“做空高 SI”，应看反向组合 `q1 - q5`，其收益和 Sharpe 方向会与当前 `ls_q5_q1` 相反。

## h=10 复核表

| factor | IC h=10 | q1 ann_return | q5 ann_return | q5-q1 ann_return | q5-q1 Sharpe net | 判断 |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| short_interest_ratio | -0.0117 | 19.3% | 39.2% | 8.4% | 0.96 | IC 负但高 SI 篮子复合收益更高，不支持“做空高 SI”结论。 |
| earnings_yield | +0.0077 | 53.7% | 25.9% | -11.2% | -1.27 | 低 earnings yield 组显著跑赢，高 EY 不是好 long leg。 |
| size | +0.0033 | 32.6% | 20.4% | -6.4% | -0.84 | 小市值组跑赢大市值组，`size` 方向不适合作为大盘股 long 因子。 |
| delta_institutional_ownership | +0.0340 | 2.4% | -3.3% | -2.9% | -1.39 | 高机构持股环比增量组表现最差，原始方向不可交易化。 |

## h=21 复核表

| factor | q1 ann_return | q5 ann_return | q5-q1 ann_return | q5-q1 Sharpe net | 判断 |
| --- | ---: | ---: | ---: | ---: | --- |
| short_interest_ratio | 17.4% | 39.0% | 9.3% | 1.06 | 高 SI 篮子继续跑赢，且波动/回撤更大。 |
| earnings_yield | 50.7% | 24.3% | -10.9% | -1.23 | 低 EY 继续占优，方向冲突稳定存在。 |
| size | 33.0% | 20.0% | -6.6% | -0.87 | 小市值占优延续。 |
| delta_institutional_ownership | 2.3% | -0.2% | -1.3% | -1.31 | 高 delta ownership 没有形成可用 long leg。 |

## 结论

### short_interest_ratio

旧判断“IC 负但多空赚钱，做空高 SI 有效”应撤回。

当前评估口径下，多空赚钱来自 `q5 - q1`，也就是做多高 SI、做空低 SI。它和 rank IC 为负同时出现，说明这个因子在当前窗口内不是单调稳定的 alpha：

- 横截面 rank correlation 倾向认为高 SI 后续收益较低。
- 但高 SI 分位组合的复合收益更高，可能由高波动、高 beta、尾部反弹、行业/小盘暴露或少数赢家驱动。

下一步应拆组合暴露，而不是直接上反向交易。

### earnings_yield

IC 弱正但 q5-q1 为负，说明排序相关性和极端分位组合不一致。低 EY 组在窗口内显著跑赢，可能是成长/科技风格或亏损高弹性股票主导。该因子不应直接按“高 EY 做多”使用。

### size

大市值方向没有通过组合检验。`size` 的 h=1 IC 弱正，但 h=10/h=21 接近 0，q5-q1 为负，说明窗口内小市值/高弹性组跑赢。

### delta_institutional_ownership

虽然 h=10 IC 为正，但高 delta 组本身表现最差。该因子可能受稀疏季度更新、极端增量、分母很小和风格暴露影响，暂不适合直接交易化。

## 后续动作

1. 对 `short_interest_ratio` 做暴露拆解：q1/q5 的 size、volatility、dollar volume、行业分布。
2. 对四个冲突因子增加分位单调性诊断：q1-q5 ann_return 是否单调、q5-q1 与 IC 方向是否一致。
3. 暂不新增反向因子。先确认冲突来自风格暴露还是因子定义，再决定是否引入 `-factor` 版本。
4. 若继续研究 short interest，优先做 sector/size neutral long-short，而不是裸 q5-q1。
