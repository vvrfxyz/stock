"""Build the Wave 15 technical report artifact from the independent audit."""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

OUTPUT_DIR = Path(__file__).resolve().parent / "output"
STEM = "market_regime_overlay_v2_2007-07-02_2026-07-10"
AUDIT_PATH = OUTPUT_DIR / "wave15_market_regime_overlay_independent_audit.json"
ARTIFACT_PATH = OUTPUT_DIR / "wave15_market_regime_overlay_report_artifact.json"
NOTES_PATH = OUTPUT_DIR / "wave15_market_regime_overlay_report_notes.json"

RULE_LABELS = {
    "buy_and_hold": "满仓基线",
    "spy_10m_trend": "SPY 10月趋势",
    "breadth_200d": "200日 breadth",
    "trend_and_breadth": "趋势 + breadth",
}
ASSET_LABELS = {
    "spy": "SPY",
    "pit_cs_equal_weight": "PIT普通股等权",
}
SAMPLE_LABELS = {
    "stability": "2007-2015 稳定性",
    "primary": "2016-2026 主样本",
}


def _source(
    source_id: str,
    label: str,
    path: str,
    *,
    engine: str,
    sql: str,
    description: str,
    tables_used: list[str],
    filters: list[str],
    metric_definitions: list[str],
) -> dict[str, Any]:
    return {
        "id": source_id,
        "label": label,
        "path": path,
        "query": {
            "engine": engine,
            "language": "sql",
            "sql": sql,
            "description": description,
            "tables_used": tables_used,
            "filters": filters,
            "metric_definitions": metric_definitions,
        },
    }


def _monthly_equity(
    daily: pd.DataFrame,
    metrics: pd.DataFrame,
    asset: str,
) -> list[dict[str, Any]]:
    selected = daily[
        (daily["asset"] == asset)
        & (daily["date"] >= pd.Timestamp("2016-01-04"))
    ].copy()
    primary = metrics[
        (metrics["asset"] == asset)
        & (metrics["sample"] == "primary")
        & (metrics["cost_bps"] == (2.0 if asset == "spy" else 25.0))
    ]
    rows: list[dict[str, Any]] = []
    for rule, frame in selected.groupby("rule", sort=False):
        frame = frame.sort_values("date").copy()
        frame["equity"] = (1.0 + frame["primary_net_return"]).cumprod()
        frame["month"] = frame["date"].dt.to_period("M")
        monthly = frame.groupby("month", sort=True).agg(
            date=("date", "last"),
            equity=("equity", "last"),
            monthly_turnover=("turnover", "sum"),
            avg_target_exposure=("target_exposure", "mean"),
            avg_unresolved_missing_weight=("unresolved_missing_weight", "mean"),
        )
        if rule == "buy_and_hold":
            reference = primary.iloc[0]
            cagr = float(reference["baseline_cagr"])
            sharpe = float(reference["baseline_sharpe"])
            max_drawdown = float(reference["baseline_max_drawdown"])
        else:
            reference = primary.loc[primary["rule"] == rule].iloc[0]
            cagr = float(reference["cagr"])
            sharpe = float(reference["sharpe"])
            max_drawdown = float(reference["max_drawdown"])
        for month in monthly.itertuples(index=False):
            rows.append(
                {
                    "date": pd.Timestamp(month.date).date().isoformat(),
                    "asset": asset,
                    "asset_label": ASSET_LABELS[asset],
                    "rule": rule,
                    "rule_label": RULE_LABELS[rule],
                    "equity": float(month.equity),
                    "monthly_turnover": float(month.monthly_turnover),
                    "avg_target_exposure": float(month.avg_target_exposure),
                    "avg_unresolved_missing_weight": float(
                        month.avg_unresolved_missing_weight
                    ),
                    "full_sample_cagr": cagr,
                    "full_sample_sharpe": sharpe,
                    "full_sample_max_drawdown": max_drawdown,
                }
            )
    return rows


def _audit_checks(audit: dict[str, Any]) -> list[dict[str, Any]]:
    quality = audit["data_quality"]
    errors = audit["comparison_errors"]
    robustness = audit["robustness"]
    max_decision_error = max(errors["decision_cells"].values())
    max_crisis_error = max(errors["crisis_cells"].values())
    max_spread_error = max(errors["spread_summary"].values())
    ledger = audit["ledger_verdicts"]
    v1_rows = sum(row["study_version"].endswith("_v1") for row in ledger)
    v2_rows = sum(row["study_version"].endswith("_v2") for row in ledger)
    return [
        {
            "sort_order": 1,
            "check": "完整自然月信号",
            "result": "PASS",
            "detail": (
                f"收益截至 2026-07-10；最后信号日为 {quality['last_signal_date']}，"
                "未把 7 月中旬误当月末"
            ),
        },
        {
            "sort_order": 2,
            "check": "日频输出粒度",
            "result": "PASS",
            "detail": (
                f"{quality['daily_rows']:,} 行 = 预期 {quality['expected_daily_rows']:,} 行；"
                f"date × asset × rule 重复 {quality['daily_key_duplicates']}"
            ),
        },
        {
            "sort_order": 3,
            "check": "数值与边界",
            "result": "PASS",
            "detail": (
                f"非有限值 {quality['non_finite_numeric_values']}；负换手 "
                f"{quality['negative_turnover_rows']}；越界目标暴露 "
                f"{quality['out_of_range_target_exposure_rows']}"
            ),
        },
        {
            "sort_order": 4,
            "check": "成本恒等式",
            "result": "PASS",
            "detail": (
                "net = gross - turnover × bps / 10,000；最大绝对误差 "
                f"{quality['cost_identity_max_abs_error']:.1e}"
            ),
        },
        {
            "sort_order": 5,
            "check": "12 个裁决单元复算",
            "result": "PASS",
            "detail": f"CAGR、Sharpe、回撤及门槛差最大误差 {max_decision_error:.1e}",
        },
        {
            "sort_order": 6,
            "check": "24 个危机单元复算",
            "result": "PASS",
            "detail": f"累计收益、最大回撤、平均暴露最大误差 {max_crisis_error:.1e}",
        },
        {
            "sort_order": 7,
            "check": "8 行价差摘要复算",
            "result": "PASS",
            "detail": f"覆盖率、加权分位数和拖累最大误差 {max_spread_error:.1e}",
        },
        {
            "sort_order": 8,
            "check": "breadth 分母",
            "result": "PASS",
            "detail": (
                f"最小/中位/最大 = {quality['breadth_denominator_min']:,} / "
                f"{quality['breadth_denominator_median']:,.0f} / "
                f"{quality['breadth_denominator_max']:,}"
            ),
        },
        {
            "sort_order": 9,
            "check": "-30% 退市敏感性",
            "result": "PASS",
            "detail": (
                f"6 个股票主成本单元的 PASS/FAIL 变化 "
                f"{robustness['delisting_pass_fail_changes']}"
            ),
        },
        {
            "sort_order": 10,
            "check": "v1 → v2 合规修复",
            "result": "PASS",
            "detail": (
                f"裁决变化 {robustness['v1_v2_verdict_changes']}；最大 Sharpe 变化 "
                f"{robustness['v1_v2_max_abs_delta']['sharpe']:.6f}"
            ),
        },
        {
            "sort_order": 11,
            "check": "分钟价差新鲜度",
            "result": "CAVEAT",
            "detail": (
                f"特征事实截至 {quality['minute_spread_feature_end']}；最后可匹配成交 "
                f"{quality['measured_spread_trade_match_end']}"
            ),
        },
        {
            "sort_order": 12,
            "check": "研究台账留痕",
            "result": "PASS",
            "detail": f"v1 {v1_rows} 条 FAIL + v2 {v2_rows} 条 FAIL，旧记录未删除",
        },
    ]


def build_artifact() -> dict[str, Any]:
    with AUDIT_PATH.open(encoding="utf-8") as handle:
        audit = json.load(handle)
    daily = pd.read_parquet(OUTPUT_DIR / f"{STEM}_daily.parquet")
    daily["date"] = pd.to_datetime(daily["date"])
    metrics = pd.read_parquet(OUTPUT_DIR / f"{STEM}_metrics.parquet")
    with (OUTPUT_DIR / f"{STEM}.json").open(encoding="utf-8") as handle:
        study_payload = json.load(handle)

    decision_cells = pd.DataFrame(audit["decision_cells"])
    decision_cells["asset_label"] = decision_cells["asset"].map(ASSET_LABELS)
    decision_cells["sample_label"] = decision_cells["sample"].map(SAMPLE_LABELS)
    decision_cells["rule_label"] = decision_cells["rule"].map(RULE_LABELS)
    decision_cells["cell_label"] = (
        decision_cells["asset_label"] + " · " + decision_cells["sample_label"]
    )
    asset_order = {"spy": 0, "pit_cs_equal_weight": 1}
    sample_order = {"stability": 0, "primary": 1}
    rule_order = {rule: index for index, rule in enumerate(RULE_LABELS) if rule != "buy_and_hold"}
    decision_cells["sort_order"] = decision_cells.apply(
        lambda row: (
            sample_order[row["sample"]] * 100
            + asset_order[row["asset"]] * 10
            + rule_order[row["rule"]]
        ),
        axis=1,
    )
    decision_cells["cell_pass_label"] = np.where(
        decision_cells["cell_pass"], "PASS", "FAIL"
    )

    crisis = pd.DataFrame(audit["crisis_cells"])
    crisis["asset_label"] = crisis["asset"].map(ASSET_LABELS)
    crisis["rule_label"] = crisis["rule"].map(RULE_LABELS)
    crisis["asset_year"] = crisis["asset_label"] + " · " + crisis["year"].astype(str)
    crisis["sort_order"] = crisis.apply(
        lambda row: (
            (row["year"] - 2000) * 100
            + asset_order[row["asset"]] * 10
            + list(RULE_LABELS).index(row["rule"])
        ),
        axis=1,
    )

    spread = pd.DataFrame(audit["spread_summary"])
    spread["sample_label"] = spread["sample"].map(SAMPLE_LABELS)
    spread["rule_label"] = spread["rule"].map(RULE_LABELS)
    spread["pressure_cost_bps"] = 25.0
    spread["sort_order"] = spread.apply(
        lambda row: sample_order[row["sample"]] * 10 + list(RULE_LABELS).index(row["rule"]),
        axis=1,
    )

    primary_cells = decision_cells[decision_cells["sample"] == "primary"]
    best_primary_sharpe = float(primary_cells["sharpe_improvement"].max())
    trend_2008 = crisis[
        (crisis["asset"] == "spy")
        & (crisis["year"] == 2008)
        & (crisis["rule"] == "spy_10m_trend")
    ].iloc[0]
    baseline_2008 = crisis[
        (crisis["asset"] == "spy")
        & (crisis["year"] == 2008)
        & (crisis["rule"] == "buy_and_hold")
    ].iloc[0]
    max_recompute_error = max(
        value
        for group in audit["comparison_errors"].values()
        for value in group.values()
    )
    headline_metrics = [
        {"metric_id": "rule_pass_count", "value": 0, "comparison_value": 3},
        {
            "metric_id": "best_primary_sharpe",
            "value": best_primary_sharpe,
            "comparison_value": 0.10,
        },
        {
            "metric_id": "trend_2008",
            "value": float(trend_2008["total_return"]),
            "comparison_value": float(baseline_2008["total_return"]),
        },
        {
            "metric_id": "audit_error",
            "value": max_recompute_error,
            "comparison_value": 44,
        },
    ]

    audit_checks = _audit_checks(audit)
    spy_equity = _monthly_equity(daily, metrics, "spy")
    stock_equity = _monthly_equity(daily, metrics, "pit_cs_equal_weight")

    preregistration = _source(
        "preregistration",
        "Wave 15 预注册与合规勘误",
        "docs/wave15_market_regime_overlay_hypotheses.md",
        engine="repository document",
        sql="SELECT * FROM read_text('docs/wave15_market_regime_overlay_hypotheses.md');",
        description="冻结的三条规则、资产、样本、成本、PASS 门槛、停止条件与 v2 月末勘误。",
        tables_used=["docs/wave15_market_regime_overlay_hypotheses.md"],
        filters=[
            "仅 spy_10m_trend、breadth_200d、trend_and_breadth 三条规则",
            "稳定性样本 2007-07-02 至 2015-12-31；主样本 2016-01-04 至 2026-07-10",
            "信号只使用已结束自然月的最后一个 XNYS 交易日",
        ],
        metric_definitions=[
            "PASS 要求四个 asset × sample 单元全部满足：回撤改善 >=10pp、Sharpe 改善 >=0.10、CAGR 损失 <=2pp",
            "SPY 主成本 2bps/边；PIT 普通股等权主成本 25bps/边；现金按 DTB3 actual/360 计息",
        ],
    )
    audit_source = _source(
        "independent_audit",
        "Wave 15 独立复算审计",
        "research/output/wave15_market_regime_overlay_independent_audit.json",
        engine="DuckDB / Python pandas frozen snapshot",
        sql=(
            "SELECT * FROM read_json_auto("
            "'research/output/wave15_market_regime_overlay_independent_audit.json');"
        ),
        description="从保存的日频毛收益、换手、DTB3、成交权重和敏感性输出独立重算裁决与稳健性。",
        tables_used=[
            f"research/output/{STEM}_daily.parquet",
            f"research/output/{STEM}_metrics.parquet",
            f"research/output/{STEM}_crises.parquet",
            f"research/output/{STEM}_signals.parquet",
            f"research/output/{STEM}_measured_trades.parquet",
            f"research/output/{STEM}_spread_summary.parquet",
            f"research/output/{STEM}_delisting_sensitivity.parquet",
            "research/output/market_regime_overlay_2007-07-02_2026-07-10_metrics.parquet",
            "research/output/wave15_authoritative_trials_2026-07-12.parquet",
        ],
        filters=[
            "主裁决只取 SPY 2bps/边和股票 25bps/边",
            "危机年份固定为 2008、2020、2022 完整自然年",
            "实测价差只统计 cost_bps 非空的真实成交权重，不做缺失填补",
        ],
        metric_definitions=[
            "净收益 = 毛收益 - 换手 × 单边成本bps / 10,000",
            "CAGR 按 252 交易日年化；Sharpe 使用净收益减 DTB3；最大回撤包含样本起点净值 1.0",
            "价差覆盖率 = 有实测单边成本的成交绝对权重 / 全部成交绝对权重",
        ],
    )
    daily_source = _source(
        "daily_paths",
        "Wave 15 v2 日频组合路径",
        f"research/output/{STEM}_daily.parquet",
        engine="DuckDB / Python pandas frozen snapshot",
        sql=(
            "SELECT date, asset, rule, gross_return, turnover, primary_cost_bps, "
            "gross_return - turnover * primary_cost_bps / 10000.0 AS primary_net_return, "
            "risk_free_return, target_exposure, realized_stock_weight, "
            "unresolved_missing_weight FROM read_parquet("
            f"'research/output/{STEM}_daily.parquet');"
        ),
        description="两类底层资产、满仓基线与三条覆盖规则的保存日频路径；报告按自然月末抽取净值。",
        tables_used=[f"research/output/{STEM}_daily.parquet"],
        filters=[
            "净值图只展示主样本 2016-01-04 至 2026-07-10",
            "月度点为每个自然月最后一个已观察 XNYS 会话的累计净值",
        ],
        metric_definitions=[
            "累计净值 = cumprod(1 + primary_net_return)，样本起点归一化为 1",
            "月度换手为月内日换手之和；月度暴露与未决缺价权重为月内日均值",
        ],
    )
    roadmap_source = _source(
        "next_directions",
        "技术分析下一阶段研究路线图",
        "docs/research_next_directions_2026-07.md",
        engine="repository document",
        sql="SELECT * FROM read_text('docs/research_next_directions_2026-07.md');",
        description="已排序的技术研究方向、最小可证伪设计、成本口径与停止条件。",
        tables_used=["docs/research_next_directions_2026-07.md"],
        filters=[
            "Wave 16 限定为 SPY 与一个 PIT 市场组合的同日 intraday momentum",
            "不因 Wave 15 失败升级 HMM、change-point、波动目标或机器学习 regime",
        ],
        metric_definitions=[
            "Wave 16 主问题：09:30-10:00 收益是否预测 15:30-15:59 同方向收益",
            "发表前复现 2003-2018；发表后检验 2019-2026；成本 1/2/5bps/边",
        ],
    )
    sources = [preregistration, audit_source, daily_source, roadmap_source]

    cards = [
        {
            "id": "rule_pass_count",
            "description": "三条预注册规则均未在四个资产 × 样本单元同时通过。",
            "dataset": "headline_metrics",
            "sourceId": "independent_audit",
            "filter": {"metric_id": "rule_pass_count"},
            "metrics": [
                {"label": "通过规则", "field": "value", "format": "number"},
                {"label": "规则总数", "field": "comparison_value", "format": "number"},
            ],
        },
        {
            "id": "best_primary_sharpe",
            "description": "主样本六个规则 × 资产单元中最好的 Sharpe 改善，仍低于 +0.10。",
            "dataset": "headline_metrics",
            "sourceId": "independent_audit",
            "filter": {"metric_id": "best_primary_sharpe"},
            "metrics": [
                {"label": "主样本最佳 Sharpe 改善", "field": "value", "format": "number", "signed": True},
                {"label": "PASS 门槛", "field": "comparison_value", "format": "number", "signed": True},
            ],
        },
        {
            "id": "trend_2008",
            "description": "SPY 10 月趋势在 2008 年几乎全程现金，危机防御很强，但不能替代主样本裁决。",
            "dataset": "headline_metrics",
            "sourceId": "independent_audit",
            "filter": {"metric_id": "trend_2008"},
            "metrics": [
                {"label": "2008 趋势覆盖收益", "field": "value", "format": "percent", "signed": True},
                {"label": "SPY 满仓", "field": "comparison_value", "format": "percent", "signed": True},
            ],
        },
        {
            "id": "audit_error",
            "description": "12 个裁决、24 个危机和 8 个价差摘要单元均从冻结路径独立复算。",
            "dataset": "headline_metrics",
            "sourceId": "independent_audit",
            "filter": {"metric_id": "audit_error"},
            "metrics": [
                {"label": "最大复算误差", "field": "value", "format": "number"},
                {"label": "复算单元", "field": "comparison_value", "format": "number"},
            ],
        },
    ]

    charts = [
        {
            "id": "sharpe_improvement_by_cell",
            "title": "四个裁决单元的 Sharpe 改善",
            "subtitle": "虚线为 +0.10 门槛；2016-2026 的六个主样本单元全部未达标。",
            "type": "bar",
            "dataset": "decision_cells",
            "sourceId": "independent_audit",
            "intent": "comparison",
            "question": "三条覆盖规则的风险调整收益改善能否跨资产、跨时期稳定？",
            "rationale": "四个离散裁决单元和三条规则适合分组柱比较；阈值线直接呈现 PASS 距离。",
            "comparisonContext": {
                "baseline": "同资产、同样本、同成本的满仓基线",
                "grain": "资产 × 样本 × 规则",
                "unit": "Sharpe 改善",
            },
            "encodings": {
                "x": {"field": "cell_label", "type": "ordinal", "label": "裁决单元"},
                "y": {"field": "sharpe_improvement", "type": "quantitative", "label": "Sharpe 改善"},
                "color": {"field": "rule_label", "type": "nominal", "label": "规则"},
                "tooltip": [
                    {"field": "drawdown_improvement", "type": "quantitative", "label": "回撤改善", "format": "percent"},
                    {"field": "cagr_loss", "type": "quantitative", "label": "CAGR 损失", "format": "percent"},
                    {"field": "cost_bps", "type": "quantitative", "label": "单边成本", "unit": "bps"},
                    {"field": "cell_pass_label", "type": "text", "label": "单元裁决"},
                ],
            },
            "valueFormat": "number",
            "referenceLines": [
                {"axis": "y", "value": 0.10, "label": "PASS +0.10", "color": "neutral", "lineStyle": "dashed"}
            ],
            "palette": {"kind": "categorical", "name": "three-rule-comparison"},
            "layout": "full",
        },
        {
            "id": "spy_primary_equity",
            "title": "SPY 主样本累计净值",
            "subtitle": "2016年1月4日至2026年7月10日；2 bps/边，月末采样，起点为 1。",
            "type": "line",
            "dataset": "spy_primary_equity",
            "sourceId": "daily_paths",
            "intent": "trend",
            "question": "主样本中，覆盖层的长期防御是否足以补偿现金拖累？",
            "rationale": "十年主样本需要连续净值路径，月末采样保留长期形状并控制报告体量。",
            "comparisonContext": {"baseline": "满仓 SPY", "grain": "月末", "unit": "累计净值"},
            "encodings": {
                "x": {"field": "date", "type": "temporal", "label": "日期"},
                "y": {"field": "equity", "type": "quantitative", "label": "累计净值"},
                "color": {"field": "rule_label", "type": "nominal", "label": "路径"},
                "tooltip": [
                    {"field": "full_sample_cagr", "type": "quantitative", "label": "主样本 CAGR", "format": "percent"},
                    {"field": "full_sample_sharpe", "type": "quantitative", "label": "主样本 Sharpe"},
                    {"field": "full_sample_max_drawdown", "type": "quantitative", "label": "主样本最大回撤", "format": "percent"},
                    {"field": "avg_target_exposure", "type": "quantitative", "label": "当月平均目标暴露", "format": "percent"},
                ],
            },
            "valueFormat": "number",
            "palette": {"kind": "categorical", "name": "baseline-and-overlays"},
            "layout": "full",
        },
        {
            "id": "stock_primary_equity",
            "title": "PIT 普通股等权主样本累计净值",
            "subtitle": "2016年1月4日至2026年7月10日；25 bps/边，月末采样；绝对水平受未决退市结局限制。",
            "type": "line",
            "dataset": "stock_primary_equity",
            "sourceId": "daily_paths",
            "intent": "trend",
            "question": "在漂移权重、股票换手成本和缺价冻结下，覆盖层能否改善等权组合？",
            "rationale": "月末累计净值同时呈现长期复利和危机后的追赶缺口；tooltip 保留缺价权重。",
            "comparisonContext": {"baseline": "PIT 普通股等权满仓", "grain": "月末", "unit": "累计净值"},
            "encodings": {
                "x": {"field": "date", "type": "temporal", "label": "日期"},
                "y": {"field": "equity", "type": "quantitative", "label": "累计净值"},
                "color": {"field": "rule_label", "type": "nominal", "label": "路径"},
                "tooltip": [
                    {"field": "full_sample_cagr", "type": "quantitative", "label": "主样本 CAGR", "format": "percent"},
                    {"field": "full_sample_sharpe", "type": "quantitative", "label": "主样本 Sharpe"},
                    {"field": "full_sample_max_drawdown", "type": "quantitative", "label": "主样本最大回撤", "format": "percent"},
                    {"field": "avg_unresolved_missing_weight", "type": "quantitative", "label": "当月平均未决缺价权重", "format": "percent"},
                ],
            },
            "valueFormat": "number",
            "palette": {"kind": "categorical", "name": "baseline-and-overlays"},
            "layout": "full",
        },
        {
            "id": "crisis_total_returns",
            "title": "冻结危机年份的累计净收益",
            "subtitle": "2008、2020、2022 完整自然年；趋势规则在 2008 最强，breadth 在 2022 更有效。",
            "type": "bar",
            "dataset": "crisis_returns",
            "sourceId": "independent_audit",
            "intent": "comparison",
            "question": "覆盖规则在不同危机形态中是否持续优于满仓基线？",
            "rationale": "六个资产 × 年份类别与四条路径适合分组柱；零线区分收益和损失。",
            "comparisonContext": {"baseline": "同资产满仓路径", "grain": "资产 × 完整自然年", "unit": "累计净收益"},
            "encodings": {
                "x": {"field": "asset_year", "type": "ordinal", "label": "资产与年份"},
                "y": {"field": "total_return", "type": "quantitative", "label": "累计净收益", "format": "percent"},
                "color": {"field": "rule_label", "type": "nominal", "label": "路径"},
                "tooltip": [
                    {"field": "max_drawdown", "type": "quantitative", "label": "年内最大回撤", "format": "percent"},
                    {"field": "avg_target_exposure", "type": "quantitative", "label": "平均目标暴露", "format": "percent"},
                    {"field": "cost_bps", "type": "quantitative", "label": "单边成本", "unit": "bps"},
                ],
            },
            "valueFormat": "percent",
            "referenceLines": [
                {"axis": "y", "value": 0.0, "label": "零收益", "color": "neutral", "lineStyle": "solid"}
            ],
            "palette": {"kind": "categorical", "name": "baseline-and-overlays"},
            "layout": "full",
        },
    ]

    tables = [
        {
            "id": "decision_table",
            "title": "12 个主成本裁决单元",
            "subtitle": "每条规则必须四个单元全 PASS；回撤与 CAGR 以小数率保存并按百分比显示。",
            "dataset": "decision_cells",
            "sourceId": "independent_audit",
            "density": "comfortable",
            "defaultSort": {"field": "sort_order", "direction": "asc"},
            "columns": [
                {"field": "sort_order", "label": "序号", "type": "number"},
                {"field": "sample_label", "label": "样本", "type": "text"},
                {"field": "asset_label", "label": "底层资产", "type": "text"},
                {"field": "rule_label", "label": "规则", "type": "text"},
                {"field": "cost_bps", "label": "成本", "type": "number", "unit": "bps/边"},
                {"field": "drawdown_improvement", "label": "回撤改善", "type": "percent", "format": "percent", "semantic": "movement"},
                {"field": "sharpe_improvement", "label": "Sharpe 改善", "type": "number", "semantic": "movement"},
                {"field": "cagr_loss", "label": "CAGR 损失", "type": "percent", "format": "percent", "semantic": "movement"},
                {"field": "cell_pass_label", "label": "裁决", "type": "text"},
            ],
        },
        {
            "id": "spread_table",
            "title": "股票成交权重的实测价差诊断",
            "subtitle": "63 个 XNYS 会话、至少 20 个有效日；只统计可匹配成交，不填补缺失。",
            "dataset": "spread_diagnostics",
            "sourceId": "independent_audit",
            "density": "comfortable",
            "defaultSort": {"field": "sort_order", "direction": "asc"},
            "columns": [
                {"field": "sort_order", "label": "序号", "type": "number"},
                {"field": "sample_label", "label": "样本", "type": "text"},
                {"field": "rule_label", "label": "路径", "type": "text"},
                {"field": "trade_weight_coverage", "label": "成交权重覆盖", "type": "percent", "format": "percent"},
                {"field": "weighted_median_cost_bps", "label": "加权中位成本", "type": "number", "unit": "bps/边"},
                {"field": "weighted_mean_cost_bps", "label": "加权平均成本", "type": "number", "unit": "bps/边"},
                {"field": "weighted_p75_cost_bps", "label": "加权 P75", "type": "number", "unit": "bps/边"},
                {"field": "pressure_cost_bps", "label": "主压力档", "type": "number", "unit": "bps/边"},
            ],
        },
        {
            "id": "audit_table",
            "title": "独立复算与数据质量检查",
            "subtitle": "所有检查来自冻结 v2 文件；CAVEAT 表示不推翻总裁决但限制绝对解释。",
            "dataset": "audit_checks",
            "sourceId": "independent_audit",
            "density": "spacious",
            "defaultSort": {"field": "sort_order", "direction": "asc"},
            "columns": [
                {"field": "sort_order", "label": "序号", "type": "number"},
                {"field": "check", "label": "检查", "type": "text"},
                {"field": "result", "label": "结果", "type": "text"},
                {"field": "detail", "label": "证据", "type": "text"},
            ],
        },
    ]

    blocks = [
        {"id": "title", "type": "markdown", "body": "# Wave 15：市场趋势与 Price Breadth 覆盖层技术结论"},
        {
            "id": "technical_summary",
            "type": "markdown",
            "sourceId": "independent_audit",
            "body": (
                "## 技术摘要\n\n"
                "**结论：关闭 `market_regime_overlay` 简单规则家族，不部署，也不升级 HMM、change-point、波动目标或机器学习 regime。** "
                "三条预注册规则在 2016 年 1 月 4 日至 2026 年 7 月 10 日的主样本中，对 SPY 与 PIT 普通股等权两个底层资产全部 FAIL。最好的主样本 Sharpe 改善只有 **+0.046**，低于 +0.10 门槛；其余主单元为 +0.006 至 -0.288。\n\n"
                "历史稳定性并非完全无效：`spy_10m_trend` 在 2007-2015 的 SPY 与股票腿都 PASS，回撤分别改善 **36.19pp** 与 **34.58pp**，Sharpe 分别提高 **0.251** 与 **0.155**。但该效用没有延续到 2016+；SPY 主样本 CAGR 损失 **6.28pp**，股票腿损失 **5.32pp**。这正触发了预注册的“只靠 2008 改善、随后长期现金拖累”停止条件。\n\n"
                "2008 的防御非常醒目：SPY 满仓收益 **-36.79%**，10 月趋势覆盖为 **+1.41%**；股票满仓 **-37.13%**，趋势覆盖 **+1.23%**。但 2020 与 2022 的危机形态不同，单一慢速月频状态没有稳定复刻同样的效用。\n\n"
                "v2 修复了把 2026 年 7 月 10 日误当月末的时点错误，最后信号日现为 **2026 年 6 月 30 日**。修复前后没有任何 PASS/FAIL 变化；12 个裁决单元、24 个危机单元和 8 行价差摘要从冻结日频路径独立复算，最大误差为 **0**。股票腿主样本的未决缺价权重均值约 **16.6%-19.4%**，因此其绝对收益需强 caveat；但 SPY 自身已足以否决全部规则，总结论不依赖这一缺口。"
            ),
        },
        {"id": "headline_metrics_block", "type": "metric-strip", "cardIds": ["rule_pass_count", "best_primary_sharpe", "trend_2008", "audit_error"]},
        {
            "id": "sample_break_section",
            "type": "markdown",
            "sourceId": "independent_audit",
            "body": (
                "## 2016+ 打断了早期趋势覆盖层的有效性\n\n"
                "稳定性样本中，SPY 10 月趋势的两个底层资产都跨过三道门槛；`trend_and_breadth` 仅在 SPY 上通过，股票腿 Sharpe 改善 **0.090**，略低于 0.10；纯 breadth 在两类资产上都因 Sharpe 不足而失败。\n\n"
                "主样本更明确：没有一个单元达到 Sharpe +0.10。`breadth_200d` 在 SPY 上把最大回撤从 **-33.72%** 降至 **-16.28%**，但 Sharpe 只提高 **0.006**，同时丢失 **4.52pp** CAGR；组合规则在 SPY 上回撤改善 **10.64pp**，但 Sharpe 只提高 **0.046**、CAGR 损失 **2.62pp**。股票腿的三条规则 Sharpe 全部下降。\n\n"
                "下图只画 Sharpe 改善，因为这是跨四个单元最一致的失败维度；tooltip 同时保留回撤、CAGR 和成本，避免把单指标图误读成完整裁决。"
            ),
        },
        {"id": "sharpe_chart_block", "type": "chart", "chartId": "sharpe_improvement_by_cell", "layout": "full"},
        {
            "id": "decision_table_context",
            "type": "markdown",
            "sourceId": "independent_audit",
            "body": "精确裁决见下表。正的 CAGR 损失表示覆盖层跑输满仓；只有回撤改善、Sharpe 改善与 CAGR 损失三项同时满足阈值，单元才 PASS。",
        },
        {"id": "decision_table_block", "type": "table", "tableId": "decision_table", "layout": "full"},
        {
            "id": "equity_section",
            "type": "markdown",
            "sourceId": "daily_paths",
            "body": (
                "## 主样本的核心代价是长期降仓，而不是月末交易费\n\n"
                "SPY 主样本使用仅 **2 bps/边** 的成本，三条规则仍全部失败，因此不能把结论归因于股票成本假设。10 月趋势在主样本平均目标暴露约 **69.8%**，CAGR 从满仓的 **15.15%** 降至 **8.87%**；breadth 与组合规则保留更多上涨，但仍未同时满足风险调整收益与复利门槛。\n\n"
                "下图展示 2016+ 的月末累计净值。覆盖层在快速下跌时能拉开距离，但随后经常在反弹和长牛阶段以现金仓位归还优势。"
            ),
        },
        {"id": "spy_equity_chart_block", "type": "chart", "chartId": "spy_primary_equity", "layout": "full"},
        {
            "id": "stock_equity_context",
            "type": "markdown",
            "sourceId": "daily_paths",
            "body": (
                "股票等权腿呈现相同方向：25 bps 压力成本下，满仓主样本 CAGR **8.77%**，三条覆盖规则为 **3.45%、6.08%、6.86%**；Sharpe 均低于基线。图中绝对净值还受到永久缺价但无实测退市结局的冻结权重影响，主样本日均约 **16.6%-19.4%**，因此更适合比较相对形状，而不适合把终值当成精确可部署收益。"
            ),
        },
        {"id": "stock_equity_chart_block", "type": "chart", "chartId": "stock_primary_equity", "layout": "full"},
        {
            "id": "crisis_section",
            "type": "markdown",
            "sourceId": "independent_audit",
            "body": (
                "## 2008 的强防御没有跨危机稳定复现\n\n"
                "10 月趋势在 2008 几乎全年为现金，因而把两类资产约 **-37%** 的年度损失转成约 **+1%**。2020 中它显著降低最大回撤，但 SPY 年收益仍从 **18.33%** 降至 **14.54%**；股票腿反而从 **16.86%** 提高到 **19.31%**。2022 则是 breadth 最有效：SPY 年损失从 **-18.18%** 收窄至 **-9.26%**，股票腿从 **-17.40%** 收窄至 **-10.97%**。\n\n"
                "这说明规则对危机速度、反弹形态和持续时间敏感。2008 不是虚假改善，但也不能代表一个跨制度稳定的覆盖机制。"
            ),
        },
        {"id": "crisis_chart_block", "type": "chart", "chartId": "crisis_total_returns", "layout": "full"},
        {
            "id": "cost_section",
            "type": "markdown",
            "sourceId": "independent_audit",
            "body": (
                "## 实测价差低于压力档，但不足以复活规则\n\n"
                "股票腿主样本的成交权重覆盖约 **91.7%-93.8%**，稳定性样本约 **96.7%-97.8%**。已覆盖成交的加权中位单边成本约 **4.42-5.51 bps**，远低于 25 bps 主压力档。即使把股票成本降到 10 bps/边，主样本三条规则仍全部 FAIL；SPY 在 1 bps/边也全部 FAIL。\n\n"
                "因此 25 bps 是保守压力测试，但不是失败的唯一来源。另一方面，价差覆盖不是 100%，且 Corwin-Schultz 日级估计不包含冲击成本、排队和极端时段滑点，所以实测子样本只能作为成本诊断，不能生成完整逐股净收益。"
            ),
        },
        {"id": "spread_table_block", "type": "table", "tableId": "spread_table", "layout": "full"},
        {
            "id": "scope_section",
            "type": "markdown",
            "sourceId": "preregistration",
            "body": (
                "## 范围、数据与指标定义\n\n"
                "市场代理为 SPY 总收益价格；股票底层为按当时上市状态、价格和 63 日中位美元成交额筛选的 PIT 普通股等权组合。信号只在自然月最后一个 XNYS 交易日收盘后形成，从下一交易日收益开始生效。窗口未完整时保持 100% 基线暴露，不能把缺失解释为 risk-off。\n\n"
                "`spy_10m_trend` 在 SPY 月末总收益价高于包含当月的 10 月均线时满仓，否则持有 DTB3 现金；`breadth_200d` 在有效 200 日价格历史的 PIT 普通股中，高于各自 200 日均线的比例严格大于 50% 时满仓，否则现金；组合规则仅在两者同时 risk-on 时满仓，否则 50% 风险资产、50% 现金。\n\n"
                "每条规则在 SPY/股票等权和稳定性/主样本四个单元中，都必须同时满足最大回撤改善至少 10pp、Sharpe 提高至少 0.10、CAGR 损失不超过 2pp。任一单元失败即整条规则 FAIL。"
            ),
        },
        {
            "id": "method_section",
            "type": "markdown",
            "sourceId": "preregistration",
            "body": (
                "## 组合模拟与预注册裁决方法\n\n"
                "股票组合在月末再平衡前先按当日收益更新漂移权重，再以 `sum(abs(target - pretrade))` 计算股票换手；缺价持仓保持冻结，剩余风险预算只分给可交易成员。实测退市收益在首次永久缺价日注入并转入现金；缺少实测结局的持仓在主口径中不填收益，另做 -30% 终局敏感性。\n\n"
                "净收益逐日扣除固定单边成本；现金赚 DTB3 actual/360 日收益。CAGR 按 252 交易日年化，Sharpe 使用净收益减 DTB3，最大回撤将样本起点净值 1.0 纳入高水位。危机窗口固定为 2008、2020、2022 完整自然年，不允许按事后高低点裁剪。\n\n"
                "停止规则同样预注册：如果改善主要来自 2008，而 2016+ 被现金拖累至门槛失败，则该家族结案；不得继续搜索均线窗口、breadth 阈值、仓位比例或复杂 regime 模型。"
            ),
        },
        {
            "id": "robustness_section",
            "type": "markdown",
            "sourceId": "independent_audit",
            "body": (
                "## 独立复算确认结论，退市缺口限制股票绝对解释\n\n"
                "独立审计没有调用研究脚本的汇总或裁决函数，而是从保存的毛收益、换手、成本、DTB3、成交权重和敏感性文件重算。12 个裁决单元、24 个危机单元和 8 行价差摘要与冻结输出逐项一致，成本恒等式误差为 0。v1→v2 时点修复的最大 Sharpe 变化仅 **0.000111**，没有裁决翻转。\n\n"
                "-30% 未覆盖退市结局敏感性没有改变 6 个股票主成本单元的 PASS/FAIL，但主样本未决缺价权重仍然很高，最高日约 **24.7%-28.2%**。因此不能把股票腿的 CAGR 或终值宣传成精确可交易结果。更重要的是，SPY 路径没有这个缺口，却已经让三条规则在主样本失败，所以家族级 FAIL 仍然稳固。"
            ),
        },
        {
            "id": "audit_table_context",
            "type": "markdown",
            "sourceId": "independent_audit",
            "body": "下表将时点、粒度、数值、成本、复算、breadth 分母、价差新鲜度和台账留痕压缩为可审计检查。",
        },
        {"id": "audit_table_block", "type": "table", "tableId": "audit_table", "layout": "full"},
        {
            "id": "limitations_section",
            "type": "markdown",
            "sourceId": "independent_audit",
            "body": (
                "## 限制与不确定性\n\n"
                "**股票退市结局覆盖是最大数据限制。** 冻结权重使股票腿的绝对收益带有较强路径依赖；-30% 敏感性只验证裁决稳定，不等于恢复真实终局价格。\n\n"
                "**成本诊断仍是部分覆盖。** 分钟特征事实截至 2026 年 7 月 2 日，最后月末成交匹配到 6 月 30 日；日级价差估计不代表逐笔可执行报价，也没有建模市场冲击。\n\n"
                "**慢速月频状态天然会滞后。** 它能避开持续熊市，却可能在快速崩跌后错过 V 型修复；2020 和 2022 已展示这种制度差异。\n\n"
                "**这是描述性策略检验，不是因果结论。** PASS/FAIL 只针对冻结样本、规则与成本，不能证明市场状态造成后续收益，也不能保证未来危机复现。"
            ),
        },
        {
            "id": "next_section",
            "type": "markdown",
            "sourceId": "next_directions",
            "body": (
                "## 下一步：Wave 16 转向市场同日 Intraday Momentum\n\n"
                "下一条最值得研究的技术线不是更复杂的 regime，而是与现有日频横截面结果正交的 **市场同日 intraday momentum**：检验 09:30-10:00 的市场收益能否预测 15:30-15:59 的同方向收益。\n\n"
                "最小设计应只做 SPY 和一个 PIT 市值加权市场组合；主检验使用连续首半小时收益，交易检验只用 `sign(first30)`，不搜索阈值；2003-2018 做文献复现，2019-2026 做真正发表后检验；成本固定为 1/2/5 bps/边，并明确分钟 bar 不含 16:00 auction print。只有两个底层资产都成立，才允许扩展到行业或个股。\n\n"
                "停止条件也应先写死：发表后样本净收益不为正、结果只依赖极少数危机日、或毛 edge 不显著大于 5 bps 往返成本，则 `market_intraday_momentum` 家族结案。Wave 17 的 same-calendar-month 季节性可排在其后；Wave 15 的 HMM、波动目标和机器学习 regime 不进入待办。"
            ),
        },
        {
            "id": "further_questions",
            "type": "markdown",
            "body": (
                "## 进一步问题\n\n"
                "1. Wave 16 的 PIT 市场组合应采用可得流通市值还是总市值权重，才能避免把当前股本回看历史？\n"
                "2. 15:30-15:59 的退出价是否应固定为 15:59 bar close，还是单独处理 16:00 auction；两者必须在预注册中分开。\n"
                "3. 若 SPY 与市场组合结果分歧，应优先解释 ETF 微结构、指数成分权重还是数据覆盖，而不是直接扩展阈值搜索。"
            ),
        },
    ]

    generated_at = study_payload["generated_at"]
    artifact = {
        "surface": "report",
        "manifest": {
            "version": 1,
            "surface": "report",
            "title": "Wave 15：市场趋势与 Price Breadth 覆盖层技术结论",
            "description": "三条预注册市场风险覆盖规则的跨资产、跨样本、危机、成本、退市与独立复算技术审计。",
            "generatedAt": generated_at,
            "cards": cards,
            "charts": charts,
            "tables": tables,
            "sources": sources,
            "blocks": blocks,
        },
        "snapshot": {
            "version": 1,
            "generatedAt": generated_at,
            "status": "ready",
            "datasets": {
                "headline_metrics": headline_metrics,
                "decision_cells": json.loads(decision_cells.to_json(orient="records")),
                "spy_primary_equity": spy_equity,
                "stock_primary_equity": stock_equity,
                "crisis_returns": json.loads(crisis.to_json(orient="records")),
                "spread_diagnostics": json.loads(spread.to_json(orient="records")),
                "audit_checks": audit_checks,
            },
        },
        "sources": sources,
    }
    return artifact


def main() -> None:
    artifact = build_artifact()
    ARTIFACT_PATH.write_text(
        json.dumps(artifact, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    notes = {
        "audience": "technical",
        "delivery_mode": "canonical artifact; MCP app preferred, portable HTML fallback only if host renderer is unavailable",
        "required_structure_map": {
            "title": "title",
            "technical_summary": "technical_summary",
            "key_findings_with_visuals": [
                "sample_break_section",
                "equity_section",
                "crisis_section",
                "cost_section",
            ],
            "scope_data_metrics": "scope_section",
            "methodology": "method_section",
            "limitations_robustness": ["robustness_section", "limitations_section"],
            "recommended_next_steps": "next_section",
            "further_questions": "further_questions",
        },
        "chart_map": [
            {
                "section": "2016+ sample break",
                "question": "Do Sharpe improvements clear +0.10 across all four cells?",
                "type": "grouped bar",
                "dataset": "decision_cells",
                "claim": "All six primary cells miss the Sharpe threshold.",
            },
            {
                "section": "SPY primary path",
                "question": "Does defense compensate for cash drag?",
                "type": "multi-series line",
                "dataset": "spy_primary_equity",
                "claim": "Every overlay trails SPY over 2016+ after 2bps per side.",
            },
            {
                "section": "Stock primary path",
                "question": "Does the result generalize to PIT equal weight?",
                "type": "multi-series line",
                "dataset": "stock_primary_equity",
                "claim": "All overlays reduce Sharpe; absolute levels carry delisting caveats.",
            },
            {
                "section": "Frozen crisis years",
                "question": "Is crisis defense stable across crisis types?",
                "type": "grouped bar",
                "dataset": "crisis_returns",
                "claim": "2008 trend defense is exceptional; 2022 favors breadth instead.",
            },
        ],
        "omissions": {
            "cost_sensitivity_chart": "Exact fixed-cost tiers are secondary because SPY fails at 1-5bps and stock rules fail at 10-40bps; narrative plus measured-spread table is clearer.",
            "full_daily_equity": "Monthly endpoints retain the ten-year shape while keeping each chart dataset below the 2,000-row artifact limit.",
        },
    }
    NOTES_PATH.write_text(
        json.dumps(notes, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    print(ARTIFACT_PATH)
    print(NOTES_PATH)


if __name__ == "__main__":
    main()
