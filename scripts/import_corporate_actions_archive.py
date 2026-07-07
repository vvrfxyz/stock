"""Massive fundamentals 快照归档（2026-04-19）corporate actions parquet -> corporate_actions 历史回填。

数据形态：splits.parquet（id/ticker/execution_date/adjustment_type/split_from/split_to/
split_ratio/fetched_utc，2003 起 26,710 行）与 dividends.parquet（id/ticker/
ex_dividend_date/record_date/pay_date/declaration_date/cash_amount/currency/
distribution_type/frequency/split_adjusted_cash_amount/historical_adjustment_factor/
fetched_utc，699,947 行）。id 为 Massive 真实事件 id（E 前缀为主），与
corporate_actions.source_event_id 同源，可幂等 upsert。

定位：现有 MASSIVE 源事件受免费档 730 天窗口限制（最早 ex_date 2024-05-14，复权因子
可信下限由此而来）；本归档补齐 2003 起的真 vendor 事件。因子重建
（update_adjustment_factors 按 upper(source)='MASSIVE' 取事件）导入后自动向前延伸；
legacy POLYGON 源深历史行不参与因子链，本导入也不触碰（--retire-synthetic 除外）。

导入前审计结论（2026-07-05 六路对账，详见 docs/corp_actions_archive_2026-07.md）：
著名拆分召回 95.5%（唯一确认缺失：GOOGL 2014-04-03 C 类股拆分，须人工补 MANUAL 事件）；
cash_amount 为申报时名义值，与存量 POLYGON 合成行 165,394/165,395 位级一致；
vendor 的 split_adjusted_cash_amount / historical_adjustment_factor 在重复拆分行上
被证实损坏（CVX 2004 重复 2:1 导致除 4），两列一律不读、不写任何表。

清洗与归属规则（编号对应审计裁决 R1-R20）：
- R1 窗口：[--min-date 2003-01-01, --cutoff 2024-05-14)。上界起归 live sync 所有，
  归档绝不覆盖 live 窗口（快照旧值不能通过 upsert 冲掉更新鲜的 live 行）。
  全局上界仅对活跃证券成立：live actions 路径只选活跃证券，inactive 证券在
  [cutoff, delist_date] 的事件两条路径都不覆盖（存量退市行实证此洞），故逐证券
  放宽到 max(cutoff, delist_date+1)（exclusive，含退市日当天，与任期上界同口径）；
  inactive 且 delist_date 为 NULL 的行无可信上界，保守维持全局 cutoff。
  窗口过滤只知道 ticker，放宽是按 symbol 粗筛（取各任期上界最大值），精确的
  逐证券上界在归属后执行——回收 symbol 上的活跃现任绝不吸收 live 窗口内的归档事件。
- R3 拆股行 id 非 E 前缀（P% 156 行，IBM/Kyndryl、MMM/Solventum 等 spinoff 伪拆分，
  不是股份拆分）整体隔离；分红 P% 行正常导入。
- R4 只接受 ^[A-Z][A-Z0-9.]*$ 的 ticker（含小写字母的优先股/权证后缀非 CS/ETF，
  与 import_day_aggs 同口径直接跳过）。
- R7 分红精确重复组（同 ticker+ex_date+金额+币种+pay_date，vendor 双发脏数据，
  2,171 组）保留 prod 已存在的 id，否则字典序最小 id。
- R8 同日不同金额是真实多笔事件（特别+常规分红、ETF 收益+资本利得），全部保留。
- R9/R10 拆股精确重复保最小 id；同 (ticker, 日) 比例矛盾（176 组，含 44 组互为倒数）
  全组隔离。
- R11 不设比例量级过滤：极端比例抽样证实为真实 OTC 反向拆分；ratio>1000 仅计数示警。
- R5/R6 (ticker, 事件日) 按"代码任期"挂靠 security_id（复用 import_day_aggs 的
  任期索引：symbol_history 优先、整体裁剪到 [list_date, 退市上界]）；0 个命中记
  out_of_tenure / unmapped、多个记 ambiguous，均进隔离报告不入库——绝不猜。
  可人工恢复的隔离类别（out_of_tenure/ambiguous/conflicting_split/spinoff/
  before_min_date）另出行级明细报告（date+id+值，清算分红等可按明细补录）；
  unmapped_no_symbol（不在 CS/ETF universe，无归属对象）与 at_or_after_cutoff
  （归 live sync 所有）只做聚合计数，是对审计 R5 侧车要求的有意收窄。
- 结构性只插入：归属后跳过 (security_id, source_event_id) 已存在于 prod 的行
  （计 skipped_existing_id）——即使误用 --cutoff none 或 vendor 修订了 ex_date
  使事件跨越窗口边界，快照旧值也不可能通过 upsert 冲掉更新鲜的 live 行。
- R13 值冲突挂起：归属后与 prod 既有行（任意 source）同 (security_id, 类型, ex_date)
  但值不一致的事件不导入，双方写入 mismatch 报告；机器强制由
  research.data.securities_with_uncovered_events 承担——争议日只剩非 MASSIVE 孤行，
  该函数自动把证券剔出研究面板，人工裁决落库后自动放行
  （dry-run 实测挂起：CVI 2021 金额、CNHI 的 EUR 申报 vs USD 折算 8 条、FBL 2023 疑似错值）。
- currency 缺失按 USD 兜底（US 市场，与 update_massive_actions._infer_currency 同口径）。
- 不写 vendor_adjustment_factors（损坏列）；不触碰 actions_last_updated_at
  （归档导入不是 live 拉取，不能让增量同步误判已完成回填）。
- R19 记账断言：输入 = 入选 + 各类剔除，对不上即中止。
- 幂等：upsert 冲突键 (security_id, action_type, source, source_event_id)，重跑安全。

--retire-synthetic（导入后单独跑）：同 (security_id, 类型, ex_date) 上存在位级一致
E-id 行（分红金额+币种精确相等；拆股 to/from 比例 rtol 1e-6）的 POLYGON 合成行删除，
让 vendor id 成为唯一 source_event_id（CLAUDE.md 既定清理规则）；无 E 对应的合成行
保留（归档已证实有少量缺漏，删了会让真实除权日失配）。

--adjudicated-allowlist <tsv>（人工/半自动裁决恢复通道，2026-07 POLYGON 孤行裁决）：
只导入 allowlist 中 event_id 对应的归档行，归属直接用 allowlist 给的 security_id——
这些行当初正是因任期归属失败（out_of_tenure/ambiguous）被隔离，裁决已把归属定死，
故绕过任期归属与这两类隔离；导入前仍校验归档行 ticker（大小写不敏感）与 ex_date
和 allowlist 一致，不一致跳过并计数（allowlist_mismatch）。其余防线全部保留：
R7/R9/R10 重复检测、窗口/cutoff 上界、R13 值冲突挂起、结构性只插入。
allowlist 模式绝不组合 --retire-synthetic（直接拒绝），也绝不触碰 allowlist 之外的行。
allowlist 由 scripts/adjudicate_polygon_orphans.py 产出（列
event_id/security_id/ticker/ex_date/kind）。

用法（253 上）：
    python scripts/import_corporate_actions_archive.py --dir /home/wenruifeng/data/fundamentals/corporate_actions/US --dry-run
    python scripts/import_corporate_actions_archive.py --dir ...            # 正式导入
    python scripts/import_corporate_actions_archive.py --dir ... --retire-synthetic
导入后重建因子：
    python scripts/update_adjustment_factors.py --all --methodology-version raw_actions_v1
"""
import argparse
import sys
import time
from collections import Counter, defaultdict
from datetime import date, timedelta
from decimal import ROUND_HALF_UP, Decimal, InvalidOperation
from pathlib import Path

from loguru import logger

project_root = Path(__file__).resolve().parents[1]
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from db_manager import DatabaseManager
from scripts.import_day_aggs import IMPORTABLE_TICKER, load_tenures
from utils.script_logging import setup_logging as configure_script_logging

DEFAULT_MIN_DATE = date(2003, 1, 1)   # 与 20 年日线地板一致，更早无价格可复权
DEFAULT_CUTOFF = date(2024, 5, 14)    # live sync 窗口下限：该日起归 live 所有
EXTREME_RATIO_FLAG = Decimal(1000)    # R11：只示警不过滤
SPLIT_RATIO_RTOL = Decimal("0.000001")
CASH_QUANT = Decimal("1.0000000000")  # corporate_actions.cash_amount 列精度 Numeric(20,10)


def create_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="导入 Massive 归档拆股/分红到 corporate_actions。")
    parser.add_argument("--dir", required=True, help="存放 splits.parquet / dividends.parquet 的目录。")
    parser.add_argument("--min-date", default=DEFAULT_MIN_DATE.isoformat(),
                        help="只导入不早于该日(YYYY-MM-DD)的事件；默认 2003-01-01。")
    parser.add_argument("--cutoff", default=DEFAULT_CUTOFF.isoformat(),
                        help="活跃证券只导入早于该日(YYYY-MM-DD)的事件，该日起归 live sync；"
                             "inactive 证券自动放宽到 max(cutoff, delist_date+1)——live 路径"
                             "只选活跃证券，[cutoff, delist_date] 归档负责。"
                             "默认 2024-05-14，传 none 关闭。")
    parser.add_argument("--dry-run", action="store_true", help="只做清洗/映射统计与隔离报告，不写库。")
    parser.add_argument("--retire-synthetic", action="store_true",
                        help="导入后清理：删除已被位级一致 E-id 行确认的 POLYGON 合成行。")
    parser.add_argument("--adjudicated-allowlist", default=None,
                        help="裁决恢复通道：只导入该 TSV（event_id/security_id/ticker/ex_date/kind）"
                             "中的归档行，归属用 allowlist 的 security_id，绕过 out_of_tenure/"
                             "ambiguous 隔离；其余防线（重复检测/R13 值冲突/结构性只插入/窗口）"
                             "全部保留。不可与 --retire-synthetic 组合。")
    parser.add_argument("--quarantine-report",
                        default="logs/manual_backfill/corp_actions_archive_quarantine.tsv",
                        help="隔离事件汇总输出路径（相对项目根）。")
    parser.add_argument("--quarantine-detail-report",
                        default="logs/manual_backfill/corp_actions_archive_quarantine_detail.tsv",
                        help="可恢复类别的行级隔离明细输出路径（相对项目根）。")
    parser.add_argument("--mismatch-report",
                        default="logs/manual_backfill/corp_actions_archive_mismatch.tsv",
                        help="值冲突挂起明细输出路径（相对项目根）。")
    return parser


def _to_date(value) -> date | None:
    if value is None:
        return None
    if isinstance(value, date):
        return value
    try:
        return date.fromisoformat(str(value)[:10])
    except ValueError:
        return None


def _to_decimal(value) -> Decimal | None:
    if value is None:
        return None
    try:
        result = Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None
    return None if result.is_nan() else result


def _to_str(value) -> str | None:
    """parquet 空字符串列在 pandas 里是 float NaN（truthy），必须显式规范化。"""
    if value is None or (isinstance(value, float) and value != value):
        return None
    return str(value)


def _fmt(value: Decimal | None) -> str:
    return "" if value is None else format(value.normalize(), "f")


def _detail_record(row: dict, kind: str, reason: str) -> dict:
    """行级隔离明细（R6 人工恢复用）：日期+vendor id+值缺一不可。"""
    value = f"{_fmt(row['cash_amount'])} {row['currency']}" if kind == "dividend" \
        else f"{_fmt(row['split_from'])}:{_fmt(row['split_to'])}"
    return {"kind": kind, "ticker": row["ticker"], "reason": reason,
            "ex_date": row["ex_date"], "event_id": row["id"], "value": value}


def load_dividend_rows(path: Path, stats: Counter) -> list[dict]:
    """parquet -> 规范化分红行。日期转 date、金额转 Decimal、缺 currency 兜底 USD。"""
    import pandas as pd

    frame = pd.read_parquet(path, columns=[
        "id", "ticker", "ex_dividend_date", "record_date", "pay_date", "declaration_date",
        "cash_amount", "currency", "distribution_type", "frequency",
    ])
    rows = []
    for rec in frame.itertuples(index=False):
        event_id = _to_str(rec.id)
        ex_date = _to_date(rec.ex_dividend_date)
        cash = _to_decimal(rec.cash_amount)
        if not event_id or ex_date is None or cash is None or cash <= 0:
            stats["dividend_bad_row"] += 1
            continue
        currency = _to_str(rec.currency)
        if not currency:
            stats["dividend_currency_defaulted_usd"] += 1
        rows.append({
            "id": event_id,
            "ticker": _to_str(rec.ticker) or "",
            "ex_date": ex_date,
            "record_date": _to_date(rec.record_date),
            "pay_date": _to_date(rec.pay_date),
            "declaration_date": _to_date(rec.declaration_date),
            "cash_amount": cash,
            "currency": (currency or "USD").upper(),
            "distribution_type": _to_str(rec.distribution_type),
            "frequency": None if pd.isna(rec.frequency) else int(rec.frequency),
        })
    return rows


def load_split_rows(path: Path, stats: Counter, quarantine: Counter, detail: list[dict]) -> list[dict]:
    """parquet -> 规范化拆股行。R3：非 E 前缀 id 是 spinoff 伪拆分，整体隔离。"""
    import pandas as pd

    frame = pd.read_parquet(path, columns=[
        "id", "ticker", "execution_date", "adjustment_type", "split_from", "split_to",
    ])
    rows = []
    for rec in frame.itertuples(index=False):
        event_id = _to_str(rec.id)
        ex_date = _to_date(rec.execution_date)
        split_from = _to_decimal(rec.split_from)
        split_to = _to_decimal(rec.split_to)
        if not event_id or ex_date is None or not split_from or not split_to \
                or split_from <= 0 or split_to <= 0:
            stats["split_bad_row"] += 1
            continue
        ticker = _to_str(rec.ticker) or ""
        if not event_id.startswith("E"):
            row = {"id": event_id, "ticker": ticker or "?", "ex_date": ex_date,
                   "split_from": split_from, "split_to": split_to}
            quarantine[(row["ticker"], "spinoff_pseudo_split")] += 1
            detail.append(_detail_record(row, "split", "spinoff_pseudo_split"))
            stats["split_spinoff_quarantined"] += 1
            continue
        rows.append({
            "id": event_id,
            "ticker": ticker,
            "ex_date": ex_date,
            "split_from": split_from,
            "split_to": split_to,
            "adjustment_type": _to_str(rec.adjustment_type),
        })
    return rows


def _keep_rule(group: list[dict], existing_ids: set[str]) -> dict:
    """R7/R9 保留规则：prod 已存在的 id 优先，否则字典序最小 id。"""
    in_prod = sorted((r for r in group if r["id"] in existing_ids), key=lambda r: r["id"])
    if in_prod:
        return in_prod[0]
    return min(group, key=lambda r: r["id"])


def dedupe_dividends(rows: list[dict], existing_ids: set[str], stats: Counter) -> list[dict]:
    """R7：同 (ticker, ex_date, 金额, 币种, pay_date) 的重复组只留一行；R8：金额不同全保留。"""
    groups: dict[tuple, list[dict]] = defaultdict(list)
    for row in rows:
        key = (row["ticker"], row["ex_date"], _fmt(row["cash_amount"]), row["currency"], row["pay_date"])
        groups[key].append(row)
    deduped = []
    for group in groups.values():
        deduped.append(_keep_rule(group, existing_ids))
        stats["dividend_exact_duplicates_dropped"] += len(group) - 1
    return deduped


def sift_splits(rows: list[dict], existing_ids: set[str], stats: Counter,
                quarantine: Counter, detail: list[dict]) -> list[dict]:
    """R9 精确重复保留一行；R10 同 (ticker, 日) 比例矛盾全组隔离；R11 极端比例只示警。"""
    groups: dict[tuple, list[dict]] = defaultdict(list)
    for row in rows:
        groups[(row["ticker"], row["ex_date"])].append(row)
    kept = []
    for (ticker, _), group in groups.items():
        ratios = {(_fmt(r["split_from"]), _fmt(r["split_to"])) for r in group}
        if len(ratios) > 1:
            quarantine[(ticker, "conflicting_split")] += len(group)
            detail.extend(_detail_record(r, "split", "conflicting_split") for r in group)
            stats["split_conflicting_quarantined"] += len(group)
            continue
        row = _keep_rule(group, existing_ids)
        stats["split_exact_duplicates_dropped"] += len(group) - 1
        ratio = row["split_to"] / row["split_from"]
        if ratio > EXTREME_RATIO_FLAG or ratio < 1 / EXTREME_RATIO_FLAG:
            stats["split_extreme_ratio_flagged"] += 1  # R11：不过滤
        kept.append(row)
    return kept


def resolve_events(
    rows: list[dict],
    tenures: dict[str, list[tuple[int, date, date]]],
    stats: Counter,
    quarantine: Counter,
    kind: str,
    detail: list[dict],
    cutoff: date | None = None,
    security_cutoffs: dict[int, date] | None = None,
) -> dict[int, list[dict]]:
    """R5/R6：(ticker, 事件日) -> security_id；唯一任期命中才入选，其余隔离计数。

    unmapped_no_symbol 只聚合计数（不在 CS/ETF universe，无归属对象，行级明细
    无恢复价值且量大）；out_of_tenure / ambiguous 出行级明细供人工恢复。
    cutoff/security_cutoffs：窗口过滤按 symbol 粗筛放行后，这里执行精确的逐证券
    上界（默认全局 cutoff）——防回收 symbol 的活跃现任吸收 live 窗口内的归档事件。
    """
    by_security: dict[int, list[dict]] = defaultdict(list)
    for row in rows:
        ticker = row["ticker"]
        if not IMPORTABLE_TICKER.match(ticker):
            stats[f"{kind}_skipped_suffix_class"] += 1
            continue
        segs = tenures.get(ticker.lower())
        if not segs:
            stats[f"{kind}_unmapped_no_symbol"] += 1
            quarantine[(ticker, "unmapped_no_symbol")] += 1
            continue
        hits = {sid for sid, start, end in segs if start <= row["ex_date"] < end}
        if len(hits) == 1:
            sid = next(iter(hits))
            upper = (security_cutoffs or {}).get(sid, cutoff)
            if upper is not None and row["ex_date"] >= upper:
                stats[f"{kind}_at_or_after_cutoff"] += 1  # 归 live sync 所有，无须明细
                continue
            by_security[sid].append(row)
            stats[f"{kind}_mapped"] += 1
        elif not hits:
            stats[f"{kind}_out_of_tenure"] += 1
            quarantine[(ticker, "out_of_tenure")] += 1
            detail.append(_detail_record(row, kind, "out_of_tenure"))
        else:
            stats[f"{kind}_ambiguous"] += 1
            quarantine[(ticker, "ambiguous")] += 1
            detail.append(_detail_record(row, kind, "ambiguous"))
    return by_security


def effective_cutoff(global_cutoff: date, is_active: bool, delist_date: date | None) -> date:
    """逐证券归档窗口上界（exclusive，与任期 upper_exclusive 同口径：退市日当天归档负责）。

    live actions 路径只选活跃证券：活跃证券维持全局 cutoff（归档绝不侵入 live 窗口，
    方案 §A.5）；inactive 证券在 [cutoff, delist_date] 两条路径都不覆盖，放宽到
    max(cutoff, delist_date+1)；inactive 且 delist_date 为 NULL 无可信上界，保守回退
    全局 cutoff（其任期端点已按 max_bar+1 截断，不会额外放行）。
    """
    if is_active or delist_date is None:
        return global_cutoff
    return max(global_cutoff, delist_date + timedelta(days=1))


def extended_security_cutoffs(securities, global_cutoff: date) -> dict[int, date]:
    """(id, is_active, delist_date) 行 -> 仅含上界严格大于全局 cutoff 的差异项；
    不在返回集里的证券一律走全局 cutoff 默认。"""
    extended: dict[int, date] = {}
    for sid, is_active, delist_date in securities:
        bound = effective_cutoff(global_cutoff, is_active, delist_date)
        if bound > global_cutoff:
            extended[sid] = bound
    return extended


def load_security_cutoffs(db_manager: DatabaseManager, global_cutoff: date) -> dict[int, date]:
    """从库加载 inactive 证券的延长上界（见 effective_cutoff）。

    不按 type 过滤：任期索引之外的 security_id 永远不会被归属，多余条目无害，
    且避免与共享类型常量的演进耦合。
    """
    from sqlalchemy import text

    with db_manager.get_session() as session:
        rows = session.execute(text("""
            SELECT id, is_active, delist_date FROM securities
            WHERE upper(market) = 'US' AND is_active IS NOT TRUE AND delist_date IS NOT NULL
        """)).all()
    return extended_security_cutoffs(
        ((r.id, bool(r.is_active), r.delist_date) for r in rows), global_cutoff)


def build_ticker_cutoffs(
    tenures: dict[str, list[tuple[int, date, date]]],
    security_cutoffs: dict[int, date],
) -> dict[str, date]:
    """symbol（小写）-> 该 symbol 各任期证券延长上界的最大值。

    窗口过滤时还不知道 security_id，只能按 symbol 粗筛放行；精确的逐证券上界在
    resolve_events 归属后执行（回收 symbol 上的活跃现任仍守全局 cutoff）。
    """
    ticker_cutoffs: dict[str, date] = {}
    for symbol, segs in tenures.items():
        bounds = [security_cutoffs[sid] for sid, _, _ in segs if sid in security_cutoffs]
        if bounds:
            ticker_cutoffs[symbol] = max(bounds)
    return ticker_cutoffs


def _window_filter(rows: list[dict], min_date: date, cutoff: date | None,
                   stats: Counter, kind: str, detail: list[dict],
                   ticker_cutoffs: dict[str, date] | None = None) -> list[dict]:
    kept = []
    for row in rows:
        upper = cutoff
        if upper is not None and ticker_cutoffs:
            # 延长上界都 > cutoff（extended_security_cutoffs 保证），取到即放宽
            upper = ticker_cutoffs.get(row["ticker"].lower(), upper)
        if row["ex_date"] < min_date:
            stats[f"{kind}_before_min_date"] += 1
            detail.append(_detail_record(row, kind, "before_min_date"))
        elif upper is not None and row["ex_date"] >= upper:
            stats[f"{kind}_at_or_after_cutoff"] += 1  # 归 live sync 所有，无须明细
        else:
            kept.append(row)
    return kept


def _ratio_close(a: Decimal, b: Decimal) -> bool:
    if a == b:
        return True
    return abs(a - b) <= SPLIT_RATIO_RTOL * max(abs(a), abs(b))


def load_existing_actions(db_manager: DatabaseManager, security_ids: list[int]) -> dict[tuple, list[dict]]:
    """prod 既有事件（任意 source），键 (security_id, action_type, ex_date)，供 R13 值冲突检查。"""
    from sqlalchemy import text

    existing: dict[tuple, list[dict]] = defaultdict(list)
    if not security_ids:
        return existing
    with db_manager.get_session() as session:
        rows = session.execute(text("""
            SELECT security_id, action_type, ex_date, cash_amount, currency,
                   split_from, split_to, source, source_event_id
            FROM corporate_actions
            WHERE security_id = ANY(:ids) AND action_type IN ('DIVIDEND', 'SPLIT')
        """), {"ids": security_ids}).all()
    for r in rows:
        existing[(r.security_id, r.action_type, r.ex_date)].append({
            "cash_amount": _to_decimal(r.cash_amount),
            "currency": (r.currency or "").upper(),
            "split_from": _to_decimal(r.split_from),
            "split_to": _to_decimal(r.split_to),
            "source": r.source,
            "source_event_id": r.source_event_id,
        })
    return existing


def holdback_mismatches(
    by_security: dict[int, list[dict]],
    existing: dict[tuple, list[dict]],
    kind: str,
    stats: Counter,
    mismatches: list[dict],
) -> tuple[dict[int, list[dict]], set[int]]:
    """R13：与 prod 既有行同日但值不一致的事件挂起，证券进因子重建排除名单。

    同日已有任意一行与归档值一致（分红金额+币种精确相等 / 拆股比例 rtol 1e-6，
    含 id 相同的幂等重放）则放行；同日有既有行但全都不一致才挂起。
    """
    action_type = "DIVIDEND" if kind == "dividend" else "SPLIT"
    kept: dict[int, list[dict]] = defaultdict(list)
    excluded_securities: set[int] = set()
    for sid, rows in by_security.items():
        for row in rows:
            peers = existing.get((sid, action_type, row["ex_date"]), [])
            if peers and not any(_values_agree(row, peer, kind) for peer in peers):
                stats[f"{kind}_value_mismatch_held"] += 1
                excluded_securities.add(sid)
                mismatches.append({
                    "security_id": sid, "action_type": action_type,
                    "ticker": row["ticker"], "ex_date": row["ex_date"],
                    "archive_id": row["id"],
                    "archive_value": _fmt(row["cash_amount"]) if kind == "dividend"
                    else f"{_fmt(row['split_from'])}:{_fmt(row['split_to'])}",
                    "prod_values": "; ".join(
                        f"{p['source']}/{p['source_event_id']}="
                        + (_fmt(p["cash_amount"]) if kind == "dividend"
                           else f"{_fmt(p['split_from'])}:{_fmt(p['split_to'])}")
                        for p in peers
                    ),
                })
                continue
            kept[sid].append(row)
    return kept, excluded_securities


def _values_agree(row: dict, peer: dict, kind: str) -> bool:
    if kind == "dividend":
        # 归档是全精度值，prod 列是 Numeric(20,10)：两边量化到列精度再比，
        # 否则 0.35197648332 vs 0.3519764833 会被误判为值冲突。
        # PG numeric 是 round-half-away-from-zero，须用 HALF_UP 而非 Decimal 默认的银行家舍入。
        return peer["cash_amount"] is not None \
            and peer["cash_amount"].quantize(CASH_QUANT, rounding=ROUND_HALF_UP) \
            == row["cash_amount"].quantize(CASH_QUANT, rounding=ROUND_HALF_UP) \
            and peer["currency"] == row["currency"]
    if peer["split_from"] is None or peer["split_to"] is None:
        return False
    return _ratio_close(peer["split_to"] / peer["split_from"], row["split_to"] / row["split_from"])


def retire_confirmed_synthetic(db_manager: DatabaseManager, dry_run: bool) -> dict[str, int]:
    """C9：删除已被位级一致 MASSIVE E-id 行确认的 POLYGON 合成行（同证券同类型同 ex_date）。

    分红要求金额+币种精确相等；拆股要求 to/from 比例一致（rtol 1e-6，跨源表达可能不同）。
    无 E 对应的合成行一律保留（归档存在已证实的少量缺漏）。
    """
    from sqlalchemy import text

    counts = {}
    dividend_sql = """
        SELECT DISTINCT synthetic.id
        FROM corporate_actions AS synthetic
        JOIN corporate_actions AS real
          ON real.security_id = synthetic.security_id
         AND real.action_type = synthetic.action_type
         AND real.ex_date = synthetic.ex_date
         AND upper(real.source) = 'MASSIVE'
         AND real.source_event_id NOT LIKE 'massive-%'
        WHERE upper(synthetic.source) = 'POLYGON'
          AND synthetic.action_type = 'DIVIDEND'
          AND synthetic.cash_amount = real.cash_amount
          AND upper(coalesce(synthetic.currency, '')) = upper(coalesce(real.currency, ''))
    """
    split_sql = """
        SELECT DISTINCT synthetic.id
        FROM corporate_actions AS synthetic
        JOIN corporate_actions AS real
          ON real.security_id = synthetic.security_id
         AND real.action_type = synthetic.action_type
         AND real.ex_date = synthetic.ex_date
         AND upper(real.source) = 'MASSIVE'
         AND real.source_event_id NOT LIKE 'massive-%'
        WHERE upper(synthetic.source) = 'POLYGON'
          AND synthetic.action_type = 'SPLIT'
          AND synthetic.split_from > 0 AND synthetic.split_to > 0
          AND real.split_from > 0 AND real.split_to > 0
          AND abs(synthetic.split_to / synthetic.split_from - real.split_to / real.split_from)
              <= 0.000001 * greatest(synthetic.split_to / synthetic.split_from,
                                     real.split_to / real.split_from)
    """
    with db_manager.get_session() as session:
        for label, select_sql in (("dividend", dividend_sql), ("split", split_sql)):
            ids = [r[0] for r in session.execute(text(select_sql)).all()]
            counts[f"synthetic_{label}_confirmed"] = len(ids)
            if ids and not dry_run:
                session.execute(
                    text("DELETE FROM corporate_actions WHERE id = ANY(:ids)"), {"ids": ids}
                )
        if not dry_run:
            session.commit()
    return counts


def _dividend_item(row: dict) -> dict:
    return {
        "ex_dividend_date": row["ex_date"],
        "record_date": row["record_date"],
        "pay_date": row["pay_date"],
        "declaration_date": row["declaration_date"],
        "cash_amount": row["cash_amount"],
        "currency": row["currency"],
        "frequency": row["frequency"],
        "distribution_type": row["distribution_type"],
        "source_event_id": row["id"],
    }


def _split_item(row: dict) -> dict:
    return {
        "execution_date": row["ex_date"],
        "split_from": row["split_from"],
        "split_to": row["split_to"],
        "adjustment_type": row["adjustment_type"],
        "source_event_id": row["id"],
    }


def load_existing_vendor_pairs(db_manager: DatabaseManager) -> set[tuple[int, str]]:
    """prod 既有真 vendor 事件的 (security_id, source_event_id) 对。

    id 集合供 R7/R9 保留规则；pair 集合供结构性只插入过滤——同一 E-id 可合法挂在
    两只证券上（审计发现 28 例），必须按 pair 而非裸 id 判断"已存在"。
    """
    from sqlalchemy import text

    with db_manager.get_session() as session:
        rows = session.execute(text("""
            SELECT security_id, source_event_id FROM corporate_actions
            WHERE source_event_id NOT LIKE 'massive-%'
        """)).all()
    return {(r[0], r[1]) for r in rows}


def drop_already_imported(
    by_security: dict[int, list[dict]],
    existing_pairs: set[tuple[int, str]],
    kind: str,
    stats: Counter,
) -> dict[int, list[dict]]:
    """结构性只插入：(security_id, source_event_id) 已在 prod 的行不再送 upsert。

    upsert 是 update_on_conflict 语义、无保护列——若放行，2026-04-19 快照旧值会
    逐字段冲掉更新鲜的 live 行（含 vendor 事后修订 ex_date 跨越 cutoff 边界的
    情况：cutoff 按归档日期过滤，拦不住 id 冲突）。跳过即防线。"""
    kept: dict[int, list[dict]] = defaultdict(list)
    for sid, rows in by_security.items():
        for row in rows:
            if (sid, row["id"]) in existing_pairs:
                stats[f"{kind}_skipped_existing_id"] += 1
            else:
                kept[sid].append(row)
    return kept


def parse_adjudicated_allowlist(lines) -> dict[str, dict]:
    """allowlist TSV -> {event_id: {"security_id", "ticker", "ex_date", "kind"}}。

    列：event_id/security_id/ticker/ex_date/kind（adjudicate_polygon_orphans 产出）。
    同一 event_id 重复出现且字段不一致直接报错——多归属歧义必须回裁决层解决，
    绝不在导入层猜（与 R5/R6"绝不猜"同一纪律）。完全相同的重复行幂等收敛。
    """
    it = iter(lines)
    header = next(it, None)
    expected = ["event_id", "security_id", "ticker", "ex_date", "kind"]
    if header is None or header.rstrip("\n").split("\t") != expected:
        raise ValueError(f"allowlist 表头异常（期望 {expected}）。")
    allowlist: dict[str, dict] = {}
    for line in it:
        if not line.strip():
            continue
        parts = line.rstrip("\n").split("\t")
        if len(parts) != len(expected):
            raise ValueError(f"allowlist 行列数异常: {parts}")
        event_id, security_id, ticker, ex_date, kind = parts
        if kind not in ("dividend", "split"):
            raise ValueError(f"allowlist kind 非法: {kind}（须 dividend/split）")
        entry = {
            "security_id": int(security_id),
            "ticker": ticker.upper(),
            "ex_date": date.fromisoformat(ex_date),
            "kind": kind,
        }
        existing = allowlist.get(event_id)
        if existing is not None and existing != entry:
            raise ValueError(f"allowlist event_id 重复且归属不一致: {event_id}")
        allowlist[event_id] = entry
    return allowlist


def resolve_events_allowlist(
    rows: list[dict],
    allowlist: dict[str, dict],
    kind: str,
    stats: Counter,
    cutoff: date | None = None,
    security_cutoffs: dict[int, date] | None = None,
) -> dict[int, list[dict]]:
    """allowlist 归属：security_id 直接取 allowlist（绕过任期归属与 out_of_tenure/
    ambiguous 隔离），但归档行 ticker（大小写不敏感）与 ex_date 必须和 allowlist 一致，
    不一致跳过并计 {kind}_allowlist_mismatch。逐证券精确上界与 resolve_events 同口径
    保留——即使裁决通过，落到 live 窗口内的事件仍归 live sync 所有。
    """
    by_security: dict[int, list[dict]] = defaultdict(list)
    for row in rows:
        entry = allowlist.get(row["id"])
        if entry is None or entry["kind"] != kind:
            stats[f"{kind}_not_in_allowlist"] += 1
            continue
        if entry["ticker"] != row["ticker"].upper() or entry["ex_date"] != row["ex_date"]:
            stats[f"{kind}_allowlist_mismatch"] += 1
            logger.warning("allowlist 校验失败，跳过：event_id={} 归档 ticker/ex_date=({}, {}) "
                           "!= allowlist ({}, {})", row["id"], row["ticker"], row["ex_date"],
                           entry["ticker"], entry["ex_date"])
            continue
        sid = entry["security_id"]
        upper = (security_cutoffs or {}).get(sid, cutoff)
        if upper is not None and row["ex_date"] >= upper:
            stats[f"{kind}_at_or_after_cutoff"] += 1
            continue
        by_security[sid].append(row)
        stats[f"{kind}_mapped"] += 1
    return by_security


def main(argv: list[str] | None = None) -> int:
    start_time = time.monotonic()
    configure_script_logging("import_corporate_actions_archive")
    args = create_parser().parse_args(argv)

    data_dir = Path(args.dir)
    dividends_path = data_dir / "dividends.parquet"
    splits_path = data_dir / "splits.parquet"
    if not dividends_path.exists() or not splits_path.exists():
        logger.error("{} 下缺少 dividends.parquet / splits.parquet。", data_dir)
        return 1
    min_date = date.fromisoformat(args.min_date)
    cutoff = None if args.cutoff.lower() == "none" else date.fromisoformat(args.cutoff)

    allowlist: dict[str, dict] | None = None
    if args.adjudicated_allowlist:
        if args.retire_synthetic:
            logger.error("allowlist 模式绝不组合 --retire-synthetic（只导入裁决行，不做任何清理）。")
            return 1
        allowlist_path = Path(args.adjudicated_allowlist)
        if not allowlist_path.is_absolute():
            allowlist_path = project_root / allowlist_path
        if not allowlist_path.exists():
            logger.error("allowlist 不存在: {}", allowlist_path)
            return 1
        with allowlist_path.open() as f:
            allowlist = parse_adjudicated_allowlist(f)
        logger.info("allowlist 模式：{} 个裁决 event_id，只导入这些归档行。", len(allowlist))

    db_manager = None
    try:
        db_manager = DatabaseManager()
        stats: Counter = Counter()
        quarantine: Counter = Counter()
        detail: list[dict] = []
        mismatches: list[dict] = []

        existing_pairs = load_existing_vendor_pairs(db_manager)
        existing_ids = {event_id for _, event_id in existing_pairs}
        logger.info("prod 既有 vendor 事件：{} 个 (security, id) 对。", len(existing_pairs))

        dividends_raw = load_dividend_rows(dividends_path, stats)
        splits_raw = load_split_rows(splits_path, stats, quarantine, detail)
        input_counts = {"dividend": len(dividends_raw), "split": len(splits_raw)}

        tenures = load_tenures(db_manager)
        security_cutoffs = load_security_cutoffs(db_manager, cutoff) if cutoff is not None else {}
        ticker_cutoffs = build_ticker_cutoffs(tenures, security_cutoffs)
        if security_cutoffs:
            logger.info("逐证券上界：{} 只 inactive 证券放行到 delist_date+1（> 全局 cutoff {}；"
                        "活跃证券维持全局 cutoff）。", len(security_cutoffs), cutoff)

        dividends = _window_filter(
            dedupe_dividends(dividends_raw, existing_ids, stats),
            min_date, cutoff, stats, "dividend", detail, ticker_cutoffs)
        splits = _window_filter(
            sift_splits(splits_raw, existing_ids, stats, quarantine, detail),
            min_date, cutoff, stats, "split", detail, ticker_cutoffs)
        logger.info("窗口 [{}, {}) 内待归属：分红 {} 条、拆股 {} 条。",
                    min_date, cutoff or "∞", len(dividends), len(splits))

        dividends_by_sec = resolve_events(dividends, tenures, stats, quarantine, "dividend", detail,
                                          cutoff=cutoff, security_cutoffs=security_cutoffs) \
            if allowlist is None else \
            resolve_events_allowlist(dividends, allowlist, "dividend", stats,
                                     cutoff=cutoff, security_cutoffs=security_cutoffs)
        splits_by_sec = resolve_events(splits, tenures, stats, quarantine, "split", detail,
                                       cutoff=cutoff, security_cutoffs=security_cutoffs) \
            if allowlist is None else \
            resolve_events_allowlist(splits, allowlist, "split", stats,
                                     cutoff=cutoff, security_cutoffs=security_cutoffs)

        touched_ids = sorted(set(dividends_by_sec) | set(splits_by_sec))
        existing_actions = load_existing_actions(db_manager, touched_ids)
        dividends_by_sec, excluded_d = holdback_mismatches(
            dividends_by_sec, existing_actions, "dividend", stats, mismatches)
        splits_by_sec, excluded_s = holdback_mismatches(
            splits_by_sec, existing_actions, "split", stats, mismatches)
        dividends_by_sec = drop_already_imported(dividends_by_sec, existing_pairs, "dividend", stats)
        splits_by_sec = drop_already_imported(splits_by_sec, existing_pairs, "split", stats)
        excluded_securities = sorted(excluded_d | excluded_s)
        touched = sorted(set(dividends_by_sec) | set(splits_by_sec))
        logger.info("归属+冲突检查完成：{} 只证券入选，{} 只证券存在值冲突挂起。",
                    len(touched), len(excluded_securities))

        # R19 记账断言：每条清洗后的输入行必有唯一去向（mapped 含挂起行）。
        # allowlist 模式下任期归属三键恒为 0，改由 not_in_allowlist/allowlist_mismatch 承接；
        # 两组键互斥（Counter 缺键取 0），同一公式覆盖两种模式。
        for kind in ("dividend", "split"):
            accounted = (
                stats[f"{kind}_mapped"]
                + stats[f"{kind}_exact_duplicates_dropped"]
                + stats[f"{kind}_before_min_date"] + stats[f"{kind}_at_or_after_cutoff"]
                + stats[f"{kind}_skipped_suffix_class"] + stats[f"{kind}_unmapped_no_symbol"]
                + stats[f"{kind}_out_of_tenure"] + stats[f"{kind}_ambiguous"]
                + stats[f"{kind}_not_in_allowlist"] + stats[f"{kind}_allowlist_mismatch"]
                + (stats["split_conflicting_quarantined"] if kind == "split" else 0)
            )
            if accounted != input_counts[kind]:
                logger.error("{} 记账不平：输入 {} != 去向合计 {}。中止。",
                             kind, input_counts[kind], accounted)
                return 1

        if not args.dry_run:
            written = 0
            for index, sid in enumerate(touched, 1):
                dividend_items = [_dividend_item(r) for r in dividends_by_sec.get(sid, [])]
                split_items = [_split_item(r) for r in splits_by_sec.get(sid, [])]
                written += db_manager.upsert_dividends(sid, dividend_items) if dividend_items else 0
                written += db_manager.upsert_splits(sid, split_items) if split_items else 0
                if index % 1000 == 0:
                    logger.info("  进度 {}/{} 只证券…", index, len(touched))
            stats["rows_written_actions"] = written

        if args.retire_synthetic:
            counts = retire_confirmed_synthetic(db_manager, args.dry_run)
            stats.update(counts)
            if args.dry_run:
                logger.info("retire-synthetic dry-run 按当前库状态估算；正式导入前 E 行尚未入库，计数通常远小于导入后。")

        logger.info("--- 导入统计（dry-run={}）---", args.dry_run)
        for key, value in sorted(stats.items()):
            logger.info("  {}: {}", key, value)

        report_path = project_root / args.quarantine_report
        report_path.parent.mkdir(parents=True, exist_ok=True)
        with report_path.open("w") as f:
            f.write("ticker\treason\tevents\n")
            for (ticker, reason), n in quarantine.most_common():
                f.write(f"{ticker}\t{reason}\t{n}\n")
        logger.info("隔离报告: {}（{} 个 ticker×原因组）", report_path, len(quarantine))

        detail_path = project_root / args.quarantine_detail_report
        detail_path.parent.mkdir(parents=True, exist_ok=True)
        with detail_path.open("w") as f:
            f.write("kind\tticker\treason\tex_date\tevent_id\tvalue\n")
            for d in sorted(detail, key=lambda d: (d["reason"], d["ticker"], d["ex_date"])):
                f.write(f"{d['kind']}\t{d['ticker']}\t{d['reason']}\t{d['ex_date']}\t"
                        f"{d['event_id']}\t{d['value']}\n")
        logger.info("隔离行级明细: {}（{} 条，可按 date+id+值人工恢复）", detail_path, len(detail))

        mismatch_path = project_root / args.mismatch_report
        mismatch_path.parent.mkdir(parents=True, exist_ok=True)
        with mismatch_path.open("w") as f:
            f.write("security_id\taction_type\tticker\tex_date\tarchive_id\tarchive_value\tprod_values\n")
            for m in mismatches:
                f.write(f"{m['security_id']}\t{m['action_type']}\t{m['ticker']}\t{m['ex_date']}\t"
                        f"{m['archive_id']}\t{m['archive_value']}\t{m['prod_values']}\n")
        if excluded_securities:
            logger.warning(
                "值冲突挂起 {} 条（{} 只证券），须人工裁决: {}；在裁决落库前，"
                "research.data.securities_with_uncovered_events 会自动把这些证券剔出研究面板"
                "（争议日只剩非 MASSIVE 孤行）。",
                len(mismatches), len(excluded_securities), mismatch_path)

        if not args.dry_run and touched:
            logger.success("导入完成。请随后重建复权因子: python scripts/update_adjustment_factors.py --all")
        return 0
    except Exception as exc:
        logger.opt(exception=exc).critical("import_corporate_actions_archive 执行失败: {}", exc)
        return 1
    finally:
        if db_manager:
            db_manager.close()
        logger.info("耗时: {}", timedelta(seconds=time.monotonic() - start_time))


if __name__ == "__main__":
    raise SystemExit(main())
