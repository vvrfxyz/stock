# 全维度综合审阅报告（2026-07-07）

> 方法：23 个并行审查/核实智能体，8 个维度独立审查（本地仓库 HEAD 17cfef8 + 253 生产数据实测），
> 全部 critical/high 发现经对抗性二次核实（亲自重跑证据命令），最后一轮完整性批判补盲区。
> 共 420 次工具调用。所有数字为 2026-07-07 实测值。

## 一、总评

| 维度 | 评级 | 一句话 |
|---|---|---|
| 代码架构与工程质量 | **A-** | 架构纪律经 grep 实证全部贯彻，债务集中在几个巨石文件 |
| PostgreSQL 数据质量 | **B+** | 核心不变量全部成立，遗留项都有账可查 |
| ClickHouse 分钟线 | **A-** | 51.4 亿行无断层、抽样全绿、31ms 单票单月查询 |
| 研究方法论 | **A-** | PIT 纪律一流；台账跨机分裂是唯一实质漏洞（已修复） |
| 运维与部署 | **C+** | 管道健康，但**零备份**+安全露出拉到及格线 |
| 测试体系 | **A-** | 1,518 用例全绿 31 秒；缺 CI、缺两条写路径的锁 |
| 安全与密钥 | **C+** | 软件层卫生好；网络层两个 critical（见下） |
| 散户战略方向 | **B+** | 方法论护城河深；执行队列尚未跟着 07-07 的散户口径换挡 |

**系统本体是健康且纪律严明的**——事实表干净（水位零不一致、孤儿探针 5 项全 0、OHLC 卫生全 0）、
研究防线真实有效（预注册判据当夜斩落自家最强因子）。真正的风险全部在**系统外围**：
备份、网络暴露、单人单机。

## 二、必须知道的三个事实（经对抗性核实确认）

### 1. 【CRITICAL】零备份 —— owner 的灾备认知与事实不符

- 你以为保留的 **07-06 那份 9.7G PG 快照已不存在**。journalctl 证实 07-06 01:46:34 写入成功，
  但备份目录 8 分钟后（01:54）被清空——不是脚本 retention 干的（`-mtime +7` 删不到 0 天文件），
  是手工 rm。253 全盘 `find / -size +5G` 无任何快照。
- PG 102GB 是唯一副本。其中 13F/基本面理论可从 EDGAR 重建（数周），但 **2026-07 以来的人工裁决
  成果（孤行 398 删/8,440 allowlist、身份合并 137 组、delisting 归因）只活在这个库里，丢了就没了**。
- 恢复路径不对称：ClickHouse 85G 理论可再生，但唯一原料——137G 分钟线归档——只在 Mac
  `~/Documents/WithVW`，而 **Mac 的 Time Machine 是 "No destinations configured"，Mac 也零备份**。
  两块无冗余单盘，任一块坏都是不同性质的永久损失。
- 研究机构记忆同样裸奔：20 个决策记忆文件只在 Mac ~/.claude（不进 git）；trials.parquet 被
  .gitignore 排除。
- 253 上仅存的 dump 是 `corporate_actions_pre_adjudicate_20260707.dump`（21M、单表、同盘）——
  你的工作流本身依赖"高危写操作前先 dump"，说明备份需求真实且高频，但保护范围与暴露面完全不成比例。

**处方（小时级）**：手工跑一次 `backup_postgres.sh`（~13 分钟、产物 ~10G）并复制到 Mac；
把 Mac 的 137G WithVW 归档反向拷一份到 253（190G 余量够）；给 Mac 配 Time Machine 盘。
RPO 从无穷大降到"周"只需要一块移动硬盘。

### 2. 【CRITICAL】内网可写暴露 × 零备份 = 任意误操作都是永久损失

两项独立核实、且从 Mac 端到端复现成功：

- **ClickHouse**：default 用户**空密码** + `access_management=1`（可建持久管理员）+
  `0.0.0.0:8123/9000`。Docker 的 DNAT 走 FORWARD 链**绕过 ufw**（DOCKER-USER 链为空），
  LAN/tailnet 任何设备可无凭据 DROP/污染 50.6 亿行分钟线。`.env` 里的 CLICKHOUSE_PASSWORD
  非空但实际未生效（配置漂移）。
- **PostgreSQL**：超级用户密码 = `wenruifeng`（用户名即密码），`0.0.0.0:5432` +
  pg_hba `host all all all`。**这正是 2025-06 泄漏进 git、后被 filter-branch 清洗的那个密码——
  清洗后从未轮换**，任何清洗前 clone 的副本仍持有效凭据（本地 refs/original 里就完整留着）。
  核实员从 Mac 用该密码 `psql -U postgres` 认证成功。
- sshd：`PermitRootLogin yes` + 密码认证（Minis 面板管理的 drop-in，手工改会被覆写），无 fail2ban。
  缓解：ufw 限 22 到 LAN；公网面仅 80/443/BT/tailscale。属"内网信任边界内的严重暴露，未越公网红线"。

**处方（全部小时级）**：`ALTER USER postgres PASSWORD` 强随机 + 同步两端 .env；CH 设
password_sha256_hex 或建只读 research 用户、compose 删 9000 映射；端口改绑 127.0.0.1 或
tailscale IP（Mac 研究直连改走 SSH 隧道/tailscale，不受影响）；Minis 面板关 root 密码登录；
删 4 个含 API key 明文的 6 月历史日志；chmod 600 activation_value.txt。

### 3. 【已修复中】trials.parquet 台账跨机分裂，Bonferroni 分母系统性低估

- 253：8,165 行 / 31 trial / 20 因子；Mac：11,130 行 / 42 trial / 26 因子；**trial_id 交集 = 0**，
  合计 73 个 trial 分裂两机。253 上 `trials report --factor residual_vol` 报"无 trial"——
  正是 trials.py 注释自认的"假空账比报错更危险"场景。
- 门槛钉死 2.9，但 73 trial × 4 horizon 下应为 ~3.2+；wave-11 的 2.88 压线裁决恰好踩在这个缝上。
  方向是保守的（判死更多），属纪律瑕疵而非虚假发现源。
- 本次已启动修复：双机台账合并（trial_id 幂等去重）、253 固定为唯一台账、report 输出动态阈值。

## 三、各维度要点（严重度经核实调整后）

### 代码架构（A-）
- 豁免清单外裸 `UPDATE securities` **0 处**；fact 表纯净性零违规；`exc_info=True` 零违规；
  45 个脚本 `main(argv)` 全合规。测源比 0.77:1（24,794 测试行 / 32,356 源码行）。
- 债务：main.py ~350 行手工 argv 转发样板（已现 getattr 防御补丁，漂移发生过）；
  build_delisting_events.py 2,380 行、classify 单函数 255 行；sync_massive_universe.main 249 行
  承载 rename/recycle 主流程只能黑盒测。
- research/minute_features.py 向 CH 写派生特征，是"research 只读"的未挂号例外（本次补文档）。

### PG 数据质量（B+）
- 全绿项：水位一致 18,689 只零不一致；孤儿探针 5 项全 0；gate 剔除数精确 = 账面 325；
  13F 行级映射 87.1%（好于账面 77.5%）。
- 尾账（核实后降级为 medium/low）：
  - 1,986 只退市 >90 天归因 UNKNOWN（26.2%）——真实缺口，但 FUND_CLOSURE "0 覆盖"是伪证据
    （读取层 par=0 合成已覆盖 1,264 只）；处方仍是带全 fetch 旗标重建。
  - vendor mismatch "171 只未收敛"已过时：07-07 已腰斩至 91，且 c08f968 已改
    `--max-mismatch-rate 0.02` 裁决为存量口径差；样例 7/10 diff 在 1e-5 量级（经济上为零），
    仅 bny/cdzip 等 3 只材料化。**降为 low**。
  - daily_prices 缺 07-06 交易日（Massive 晨跑未返回）——**明天 07-08 跑批必须确认补齐**，
    连缺两日则升级。
  - ipw 双证券 2021 年 5 个月同 symbol 价格重叠，窗口化检查永远探不到，需一次性人工裁决
    （与 gogl/lazr 同型）。
  - fxho rename 冲突（sec 570 wto→fxho 被 265869 占用）是 07-05 起 sync_massive_universe
    每次非零退出的根因，已 QUARANTINE 未终裁。

### ClickHouse（A-）
- 51.4 亿行（2003-09-10 ~ 2026-07-02）逐年无断层；NVDA 拆分日复核干净；AAPL 分钟聚合与日线
  high/low 位级相等；OHLC 违规/vw 零值/重复率全部为 0；压缩 4.62x 仅 77.65 GiB。
- "周度调度从未成功"经核实**是误报因果**：update_minute_bars_weekly 由 c08f968（07-06）才引入，
  首个调度窗口是 **2026-07-11 周六**——建议人工盯一次首跑。
- 建议：分钟线 max(ts) 滞后天数纳入 health_report；load_minute_bars 加体量护栏；
  覆盖率口径（vs 日线 universe 约 88-94%，缺口为 OTC/yfinance 填缝票）补进文档。

### 研究方法论（A-）
- 强项实证：基本面 max(filed, accepted_ET)+1d 可见性、TTM 三分量同 concept 锁定、
  股本缝合 AAPL 金样本、evaluate 内建 as-of 重放防前视回归、50 个 PIT/回测测试全过。
- 除台账分裂外的实质发现：**IC 前向收益对退市按 ffill≈0% 处理，与分位回测的实测退市口径
  不一致**——同一份 EvaluationResult 两种终局口径，对小盘栖息地因子（退市密集）的排序显著性
  有未记账口径缝。建议 _forward_return 末段并入 delisting_return（进 params_hash）。
- 变现类 study 脚本（retail_reality/composite/size_neutral）只出 markdown 不入机器台账，
  garden-of-forking-paths 靠自觉——建议给 _trials_store 加 study 行。
- retail_reality 子组合（numpy 快循环，无停牌冻结/退市注入）与基准（run_backtest）双引擎
  口径差，复用到压线因子前先修。

### 运维（C+）
- 07-07 当日 10 数据步骤全部成功；PG/CH 容器零重启；磁盘 60%、增速 <10G/月可撑 1 年+。
- 表观 4/6 daily-run 失败中：仅 07-05 的 4 步是真实故障（SEC 403 是休市日边界、可预判跳过），
  07-04 是主机关机 17 小时，07-07 是 health_report 拿 exit=1 报 P1 的告警语义（本次修复）。
- OOM：07-07 上午 **5 次** global OOM 击杀 python 研究进程（RSS 5.7-6.4G，11G 主机与
  Plex/游戏服共存）。run_research.sh 的 MemoryMax=7G 帽子太高——全局 OOM 先于硬帽触发。
  建议：MemoryHigh 降 4.5G / MemoryMax 5.5G，并给 PG/CH 容器设内存上限（当前 Memory=0 裸奔）。
- health_report 应加"最近开市日 vs max(date) 落后 >1 session"的 P1 探针（本次 Massive 空返回
  只有一行 INFO 可见）。

### 测试（A-）
- 实跑全绿：非集成 1,264 passed / 7.72s；集成 254 passed / 23.72s。stock_test 账号对生产库
  CONNECT 实测 false（保险丝真实生效）。
- 缺口：**无 CI**（31 秒的测试套件却纯靠手动纪律，push→reset --hard 部署链无闸门）；
  repair_ohlc_violations 与 cleanup_us_universe 两个写事实表脚本零测试（前者本次补）；
  insert_backfilled_securities 是 29 个 securities 写 API 中唯一无语义锁的；
  ClickHouse 读写层零测试。

### 散户战略（B+）—— 前进方向见第四节

## 四、后续前进方向

### 立即（本周，全部小时级）
1. **pg_dump 一次 + 异机副本**（Mac 或移动盘）；137G WithVW 归档反向拷 253；Mac 配 Time Machine。
2. **安全包**：轮换 PG 密码（泄漏过的凭据仍在生产）、CH 设密码删 9000、端口改绑
   127.0.0.1/tailscale、面板关 root 密码登录、删 4 个含 key 日志、chmod 600。
3. 确认 07-08 跑批补齐 07-06/07-07 日线；07-11 周六盯 update_minute_bars_weekly 首跑。
4. fxho rename 冲突终裁 + ipw/kw 重叠裁决（半天件）。

### 研究主线（第 1-6 周，判据全部预注册）
按散户口径（$20k、月频、20-40 只、小盘栖息地是优势）的期望价值排序：

- **第 1 周：composite_v1 size 关卡重审 + 散户复审**（本次已在 253 拉起重跑）。
  过关 → retail_reality 双判据；FAIL → 主攻权重全转 earnings_yield。
- **第 1-2 周：earnings_yield 补三关对照 + 栖息地诊断 + 散户复审**。当前唯一长窗过线的
  非价格因子（2012+ h21 IC .027 / NW t 3.24），月频低换手天然适配散户；三模板全现成。
- **第 2 周插缝（半天）：EXCHANGE_DROP 读取层 fallback**（CRSP −30% 型，本次已实现为 opt-in）。
  小盘 q5 最常见死法目前按 0% 处理，直接虚增散户复审存活概率——新旧口径 q5 年化差 >2pp/年
  则此前小盘结论标注复核。
- **第 2-4 周：基本面族扩张战役**——从 sec_fundamental_facts 预注册 3-5 个质量/盈利因子
  （gross profitability、accruals、F-score 类）。ledger 开放问题#3 亲口承认"没动过的矿"，
  XBRL 2009+ PIT 就绪，单因子评估 15-20 分钟，边际成本极低。存活者进 composite_v2 成分池。
- **第 3-5 周：散户成本面**——用分钟线估逐股有效价差（quoted/effective spread），替换
  retail_reality 的 20/40/80bps 拍脑袋档。分钟线独有资产的已证实价值就在执行层。
  注意核实员的反证：residual_vol 三档全 FAIL 是符号级失败，成本精化不翻案——此项的价值在
  未来压线因子的判定精度，不在翻旧案。
- **第 5-6 周：composite_v2 + 30 只月频纸面组合**（eod_reversal_flow 做调仓执行叠加）。
  retail_reality 双判据 PASS → 纸面 1-2 个月核对成交假设 → $2-5k 实盘分批回填成本模型。

### 显式降级令（防执行惯性）
以下与小盘栖息地战略正交，**冻结进 backlog**，解冻条件写死：
- ADR 二期四任务（服务 BABA/TSM 类大中盘 opt-in 品种；解冻 = 某存活因子实证需要 ADR 样本）
- 拆表阶段 2/3（1a 已消除主事故通路；解冻 = 再发 securities 直写事故）
- tail_mismatch 207 身份手术（不影响任何因子 PIT 正确性）
- 13F FTD 任期审计
- liquidity_lambda 按机构口径的原设计（散户成本 = 价差一半，非 Kyle-λ 冲击；改造为价差面后再排）

### 第二梯队（登记勿丢，勿抢窗口）
- delta_IO 长窗稳定负号（t=−2.36）的反向利用专项
- insider CAR 事件研究口径（现只测过横截面 IC）
- 13F filer 条件化（小盘桶内高集中度基金行为）

### 经济账（完整性批判员的提醒，如实记录）
$20k 本金即使命中年化 10% 净 alpha 也只有 $2,000/年，低于任何合理的时间成本定价。
这盘棋的理性辩护是：a) 数据资产本身（20 年无幸存者偏差退市面板、51.4 亿分钟条、13F/insider
PIT 链）的价值可能超过其交易变现价值；b) 系统和技能可随本金增长复用。但"继续堆工程 vs
先变现一个信号"的机会成本应该有意识地权衡——这正是上面把纸面组合排进第 5-6 周的理由。

## 五、本次审阅随附的修复（已在工作流中执行）
1. 双机 trials.parquet 合并，253 固定为唯一台账；trials report 输出动态 Bonferroni 阈值 + 异地台账警告
2. composite_v1 size 关卡重审在 253 拉起（run_research.sh，同窗口同判据），结果回写 ledger
3. health_report / audit_security_identity 退出码语义拆分（P1-only → exit 0，消除每日假 FAILED）
4. repair_ohlc_violations 补 PG 集成测试（合成违规行 + 合法极值不误伤断言）
5. EXCHANGE_DROP 读取层 fallback（`--exchange-drop-fallback`，默认关，进 params_hash）
6. CLAUDE.md 挂号 minute_features CH 写例外 + 等价性容差订正；ta_combo 空切片显式掩码消 warning
