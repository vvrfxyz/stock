# Task #8a — SIC → FF12 行业静态映射

**来源**: `docs/audits/2026-06-13_deep_review_and_roadmap.md` 路线 Now 第 8 项(中性化原料的前半)

## 背景

研究层做横截面回归时需要**行业中性化**(在每个截面把因子残差从行业固定效应里剥离)。学界事实标准是 **Fama-French 12 行业分组(FF12)**,直接读 Ken French 的 [Industry Definitions](https://mba.tuck.dartmouth.edu/pages/faculty/ken.french/data_library.html)。

库里 `securities.sic_code` 已有 ~73% 覆盖(报告原话),够支持初版 FF12 中性化。**没覆盖的 ~27%(主要是 ETF + 部分新上市)在中性化阶段会被剔除**,这是已知缺口,任务的产出之一就是把缺口可量化。

**报告原话**: "SIC→FF12 行业静态映射(73% 覆盖,缺口记录在案)"

## 作用域

### 新增文件

```
research/industry.py             # 主模块
tests/test_research_industry.py  # 单元测试 (合成数据)
```

### 修改文件

- 无。绝不动 `data_models/`、`db_manager/`、`utils/`、其他 `research/*.py`

## 契约

### `research.industry.SIC_TO_FF12: dict[int, str]`

**静态字典**,SIC code → FF12 行业代码(12 个 + `Other`,共 13 个 bucket)。

FF12 的标准定义参见 Ken French 库:

| code | name | SIC ranges (示例,完整范围在原始定义里) |
|---|---|---|
| `NoDur` | Consumer NonDurables | 0100-0999, 2000-2399, 2700-2749, 2770-2799, 3100-3199, 3940-3989 |
| `Durbl` | Consumer Durables | 2500-2519, 2590-2599, 3630-3659, 3710-3711, 3714-3714, 3716-3716, 3750-3751, 3792-3792, 3900-3939, 3990-3999 |
| `Manuf` | Manufacturing | 2520-2589, 2600-2699, 2750-2769, 2800-2829, 2840-2899, 3000-3099, 3200-3569, 3580-3621, 3623-3629, 3700-3709, 3712-3713, 3715-3715, 3717-3749, 3752-3791, 3793-3799, 3860-3899 |
| `Enrgy` | Oil, Gas, and Coal | 1200-1399, 2900-2999 |
| `Chems` | Chemicals | 2830-2839, 2860-2899 |
| `BusEq` | Business Equipment | 3570-3579, 3622-3622, 3660-3692, 3694-3699, 3810-3839, 7370-7372, 7373-7373, 7374-7374, 7375-7375, 7376-7376, 7377-7377, 7378-7378, 7379-7379, 7391-7391, 8730-8734 |
| `Telcm` | Telecom | 4800-4899 |
| `Utils` | Utilities | 4900-4949 |
| `Shops` | Wholesale, Retail | 5000-5999, 7200-7299, 7600-7699 |
| `Hlth` | Healthcare | 2830-2839 (部分), 3693-3693, 3840-3859, 8000-8099 |
| `Money` | Finance | 6000-6999 |
| `Other` | Everything Else | 其余所有 SIC |

**注意**: `Chems` 和 `Hlth` 在 2830-2839 上有交集(化学制药),Ken French 的标准做法是 `Hlth` 优先(2830-2839 算 Healthcare)。Codex 实现时必须把这种顺序硬编码并加测试锁定。

权威范围列表去 [Ken French 网站](https://mba.tuck.dartmouth.edu/pages/faculty/ken.french/Data_Library/det_12_ind_port.html) 拿完整 SIC range,不要凭记忆补。**任务可接受的实现方式**:

1. 把完整 SIC range 表硬编码进模块(推荐,无运行时依赖)
2. 模块加载时从一个 `research/data/ff12_sic_ranges.csv` 文件读(可接受,数据来源必须文档化)

不接受运行时下载或网络依赖。

### `research.industry.sic_to_ff12(sic_code: int | str | None) -> str | None`

- 输入: SIC 代码,可以是 `int`、`str`(可能带前导零或多余空格)、`None`、`""`、`"N/A"`
- 输出: FF12 行业代码字符串(`"NoDur" | "Durbl" | ... | "Other"`),或 `None`(输入无效/不在范围)
- 不抛异常,内部 sanitize

### `research.industry.load_industry_panel(engine, *, security_ids: list[int] | None = None) -> pd.DataFrame`

- 一次 SQL 拉所有(或指定) `securities` 的 `(id, sic_code, type, is_active)`
- 返回长表 `["security_id", "sic_code", "ff12", "ff12_coverage_reason"]`,其中:
  - `ff12`: 映射结果(可能 `None`)
  - `ff12_coverage_reason`: `"mapped"` / `"no_sic"` / `"unmapped_sic"`(SIC 有值但映射不到任何 FF12 bucket,理论上是 bug,但实际上数据脏数据会出现)
- **不**做缓存,每次现查(库内 ~11k 行,< 100ms)

### `research.industry.coverage_report(panel: pd.DataFrame) -> dict`

- 输入: `load_industry_panel` 的返回
- 输出:

```python
{
  "total_securities": int,
  "mapped": int,
  "mapped_pct": float,           # 占比 (0-1)
  "no_sic": int,                 # sic_code IS NULL 的行数
  "unmapped_sic": int,           # sic_code 有值但映射不到
  "by_ff12": dict[str, int],     # 每 bucket 的行数
}
```

## 测试

`tests/test_research_industry.py` 必须包含至少以下用例:

### 单元 (纯合成,无 DB)

1. `test_sic_to_ff12_known_codes` — 13 个 bucket 每个至少 1 个代表性 SIC 测试:
   - `2010 -> NoDur`(食品)
   - `3711 -> Durbl`(机动车)
   - `2860 -> Hlth`(化学制药,验证 Hlth 优先于 Chems)
   - `2840 -> Chems`(2830-2839 之外的化学)
   - `3674 -> BusEq`(半导体)
   - `4812 -> Telcm`(电信)
   - `4911 -> Utils`(电力)
   - `5411 -> Shops`(超市)
   - `8062 -> Hlth`(医院)
   - `6020 -> Money`(银行)
   - `1311 -> Enrgy`(石油开采)
   - `5712 -> Shops`(家具零售)
   - `2300 -> NoDur`(服装)
   - `7011 -> Other`(酒店,不在 12 个明确 bucket 里)
2. `test_sic_to_ff12_handles_dirty_input`:
   - `None` / `""` / `"N/A"` / 负数 / 超界(99999)→ `None`
   - 字符串带前导零 `"0100"` 等于 `100`(int 化后映射成 `NoDur`)
   - 浮点 / 非数字字符串 → `None`
3. `test_chemicals_healthcare_overlap`:
   - 输入 2830, 2835, 2839(三个落在 Chems/Hlth 交集) → 必须返回 `Hlth`
4. `test_coverage_report_shape` — 合成 panel,断言所有字段、加总等于 total

### 集成 (`pytest.mark.integration`, 用 `pg_db` fixture)

5. `test_load_industry_panel_against_real_schema`:
   - 用 `pg_db` 插 4 条 securities(2 有 sic,1 无 sic,1 有未映射 sic)
   - 调 `load_industry_panel`,断言:
     - 返回 4 行
     - `mapped` / `no_sic` / `unmapped_sic` 各 1+ 行
     - `ff12_coverage_reason` 正确分类
6. `test_coverage_report_against_production_like_panel` — 同上,合成 100 行已知 SIC,断言 `coverage_report` 数字正确

## 验收

PR 通过的硬条件,Codex **必须**在 PR 描述里贴这些命令的输出 tail:

```bash
# 1. 单元测试全绿
python -m pytest tests/test_research_industry.py -q -m "not integration"

# 2. 集成测试全绿(需要本地 PG 或 conftest 临时集群)
python -m pytest tests/test_research_industry.py -q

# 3. 全套测试无回归
python -m pytest tests/ -q

# 4. 类型检查 (如果 mypy/pyright 配了,目前项目没强制,可选)
```

输出里必须显示:`tests/test_research_industry.py` 至少 9 个测试 passed,全套测试无新增 fail。

## 反需求 (绝不能做)

1. **不要动 `securities` 表 schema** — 这是只读派生工具,不写库,不加列
2. **不要引入新依赖**(no scikit-learn / no requests for download)
3. **不要做 SIC→FF48/FF49/FF17** — 报告只要 FF12,做多了是范围爆炸
4. **不要把映射做成 DB 表** — 静态字典即可,FF12 定义十年不变一次
5. **不要做 SIC 历史变更追踪** — 即便公司换 SIC 我们也按当前 `securities.sic_code` 做,这是有意为之的简化(企业 SIC 变更非常稀疏)
6. **不要在 `__init__.py` 注册** `from research.industry import *`,保持显式 import

## 实现建议(可选,Codex 自己判断)

- SIC range 用 `bisect` 而不是 dict 全枚举(SIC 有 10000+ 可能值,枚举太重)
- 或者用 `dict[tuple[int, int], str]` + 一次性扫一遍找命中区间(实现最直白)
- 性能不是瓶颈(全库一次 ~11k 行,任何写法都 < 100ms)

## 工作时长估算

3-5 小时(包括查 Ken French 完整 SIC range、写代码、写测试、自验)。
