"""跨立 POLYGON 孤行证据分桶裁决：DELETE 候选 / PROMOTE 候选（归档 allowlist）/ 人工队列。

背景：corporate_actions 中 upper(source)<>'MASSIVE' 且同证券同类型同 ex_date 无 MASSIVE
对应行的"孤行"不参与因子构建，是复权链上的洞；research.data.securities_with_uncovered_events
（straddle_v2 口径）会把带跨立孤行的证券整体剔出研究面板（2026-07 生产实测 gate 剔除
794 只，其中孤行分支贡献 792 只）。本脚本对每条跨立孤行按证据顺序分桶（落到第一个命中桶）：

1. tenure_violation（DELETE 候选）：ex_date < securities.list_date，或 inactive 且
   delist_date 非空且 ex_date > delist_date——ticker 回收污染（先例：CNHI 2003 拆股行）。
2. archive_match：与归档隔离明细（reason ∈ out_of_tenure/ambiguous）按 ticker+ex_date+
   kind+值匹配；ticker 匹配集合 = 证券现 symbol ∪ symbol_history 全部历史 symbol（大小写
   不敏感）。值匹配：分红金额差 <= 0.005 且币种一致；拆股 from/to 比值相对差 <= 1e-6。
   命中后细分：拆股还须过价格佐证（桶 3 判据）才 PROMOTE，否则 manual_residual；
   分红须 ex_date ∈ [list_date, coalesce(delist_date, today)] 才 PROMOTE，否则 manual_residual。
   同一归档 event_id 被推举到多只证券时全部降级人工（多归属歧义不进 allowlist）。
   PROMOTE 候选写 allowlist TSV（event_id/security_id/ticker/ex_date/kind），由
   import_corporate_actions_archive --adjudicated-allowlist 在 253 上落库——本脚本不落 PROMOTE。
3. split_price_test（无归档匹配的跨立拆股）：expected = split_from/split_to（新价/旧价，
   AAPL 2020 拆分 from=1,to=4 -> 价格变为 1/4）；realized 用 ex 前最近 close 与 ex 起首个
   close 之比，取与 expected 同一取向（新价/旧价 = next/prev；任务书的 prev/next 与
   from/to 直接相除互为倒数，|log| 判据在两种同取向写法下等价）。
   |log(realized/expected)| <= log(1.3) -> 真实事件但无 vendor id（manual_real_no_vendor_id，
   绝不凭空造 MASSIVE 行，留人工队列）；价格无跳变（|log(realized)| <= log(1.15)）且宣称
   比例显著（|log(expected)| >= log(1.5)）-> split_refuted（DELETE 候选）；其余 manual_residual。
4. dividend_price_test（无归档匹配的跨立分红）：yield = cash/prev_close >= 5% 才做落差
   检验；realized_drop = 1 - next_close/prev_close；realized_drop < 0.25*yield 且
   (yield - realized_drop) > 0.08 -> dividend_refuted（DELETE 候选）；其余（含 yield < 5%）
   manual_residual。非 USD 分红不与 USD 价格混算收益率，直接人工。删除宁缺毋滥：
   只删有正面反证的。

释放预测：以 gate（straddle_v2）当前剔除集为基线，假设 DELETE+PROMOTE 全部落地且
因子重建成功，剩余被剔除 = 有人工残留孤行的证券 ∪ MASSIVE 缺因子跨立证券（分支 1，
本脚本不处理）。预测是乐观上界：PROMOTE 行在导入时仍可能被 R13 位级值冲突挂起、
因子构建仍可能跳过个别事件（cash_ge_close / non_usd_no_fx）。

--apply 只执行 DELETE 桶：先把待删整行（corporate_actions 全列）写备份 TSV 再按 id 删除；
须 --yes 二次确认，并打印目标库 host。默认 dry-run 只读。

用法：
    python scripts/adjudicate_polygon_orphans.py \
        --quarantine-detail logs/manual_backfill/corp_actions_archive_quarantine_detail.tsv
    python scripts/adjudicate_polygon_orphans.py --apply --yes   # 253 上，确认 dry-run 后
删除/导入后重建因子：
    python scripts/update_adjustment_factors.py --all --methodology-version raw_actions_v1
"""
import argparse
import math
import sys
import time
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Iterable

from loguru import logger

project_root = Path(__file__).resolve().parents[1]
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from db_manager import DatabaseManager
from utils.script_logging import setup_logging as configure_script_logging

DEFAULT_START = date(2003, 1, 1)  # 与 FACTOR_TRUST_FLOOR / 20 年日线地板一致
DEFAULT_METHODOLOGY_VERSION = "raw_actions_v1"
DEFAULT_QUARANTINE_DETAIL = "logs/manual_backfill/corp_actions_archive_quarantine_detail.tsv"
DEFAULT_OUT_DIR = "logs/manual_backfill"
ARCHIVE_REASONS = frozenset({"out_of_tenure", "ambiguous"})

DIVIDEND_CASH_TOL = Decimal("0.005")      # 归档值匹配：金额绝对差上限
SPLIT_RATIO_RTOL = Decimal("0.000001")    # 归档值匹配：比例相对差上限（同 import 脚本口径）
CORROBORATE_LOG_TOL = math.log(1.3)       # 拆股价格佐证：实测/预期比 1.3x 内
REFUTE_REALIZED_LOG_MAX = math.log(1.15)  # 拆股反证：实测价格无跳变
REFUTE_EXPECTED_LOG_MIN = math.log(1.5)   # 拆股反证：宣称比例须显著才敢反证
DIVIDEND_YIELD_MIN = 0.05                 # 分红落差检验的最低名义收益率
DIVIDEND_DROP_FRACTION = 0.25             # 反证条件 1：实测落差 < 0.25*yield
DIVIDEND_GAP_MIN = 0.08                   # 反证条件 2：缺口 (yield-drop) > 8pp


@dataclass(frozen=True)
class Verdict:
    bucket: str        # tenure_violation / archive_match_promote / manual_real_no_vendor_id
                       # / split_refuted / dividend_refuted / manual_residual
    action: str        # DELETE / PROMOTE / MANUAL
    reason: str        # 细分依据（进明细报告）
    archive_event_id: str | None = None
    archive_ticker: str | None = None


def create_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="跨立 POLYGON 孤行证据分桶裁决（DELETE/PROMOTE/人工）。")
    parser.add_argument("--quarantine-detail", default=DEFAULT_QUARANTINE_DETAIL,
                        help="归档隔离行级明细 TSV（kind/ticker/reason/ex_date/event_id/value）。"
                             f"相对路径基于项目根；默认 {DEFAULT_QUARANTINE_DETAIL}。")
    parser.add_argument("--out-dir", default=DEFAULT_OUT_DIR,
                        help=f"报告输出目录（相对项目根）；默认 {DEFAULT_OUT_DIR}。")
    parser.add_argument("--start", default=DEFAULT_START.isoformat(),
                        help="孤行窗口下界(YYYY-MM-DD)；默认 2003-01-01。")
    parser.add_argument("--end", default=None, help="孤行窗口上界(YYYY-MM-DD)；默认今天。")
    parser.add_argument("--methodology-version", default=DEFAULT_METHODOLOGY_VERSION,
                        help="因子链版本（释放预测用）；默认 raw_actions_v1。")
    parser.add_argument("--apply", action="store_true",
                        help="执行 DELETE 桶（先写整行备份 TSV 再按 id 删除）；"
                             "PROMOTE 桶由 import_corporate_actions_archive --adjudicated-allowlist 落库。")
    parser.add_argument("--yes", action="store_true",
                        help="--apply 的二次确认旗标：不带 --yes 的 --apply 直接拒绝。")
    return parser


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    return create_parser().parse_args(argv)


# --------------------------------------------------------------------------
# 纯逻辑：值解析 / 匹配 / 价格检验 / 分桶（不连库，供单测）
# --------------------------------------------------------------------------

def _to_decimal(value) -> Decimal | None:
    if value is None:
        return None
    if isinstance(value, Decimal):
        return value
    try:
        result = Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None
    return None if result.is_nan() else result


def _fmt(value: Decimal | None) -> str:
    return "" if value is None else format(value.normalize(), "f")


def parse_quarantine_value(kind: str, value: str) -> dict | None:
    """隔离明细 value 列解析：分红 "0.2 USD" -> cash+currency；拆股 "15:1" -> from+to。"""
    if kind == "dividend":
        parts = value.strip().rsplit(" ", 1)
        if len(parts) != 2:
            return None
        cash = _to_decimal(parts[0])
        if cash is None or cash <= 0 or not parts[1]:
            return None
        return {"cash": cash, "currency": parts[1].upper()}
    if kind == "split":
        parts = value.strip().split(":")
        if len(parts) != 2:
            return None
        split_from, split_to = _to_decimal(parts[0]), _to_decimal(parts[1])
        if not split_from or not split_to or split_from <= 0 or split_to <= 0:
            return None
        return {"split_from": split_from, "split_to": split_to}
    return None


def parse_quarantine_detail(
    lines: Iterable[str],
    reasons: frozenset[str] = ARCHIVE_REASONS,
) -> dict[tuple[str, date, str], list[dict]]:
    """隔离明细 TSV -> {(TICKER 大写, ex_date, kind): [候选…]}；只收 reasons 内的行。"""
    it = iter(lines)
    header = next(it, None)
    if header is None:
        raise ValueError("隔离明细为空文件。")
    cols = header.rstrip("\n").split("\t")
    expected = ["kind", "ticker", "reason", "ex_date", "event_id", "value"]
    if cols != expected:
        raise ValueError(f"隔离明细表头异常：{cols}（期望 {expected}）。")
    index: dict[tuple[str, date, str], list[dict]] = defaultdict(list)
    for line in it:
        parts = line.rstrip("\n").split("\t")
        if len(parts) != len(expected):
            continue
        kind, ticker, reason, ex_str, event_id, value = parts
        if reason not in reasons or not event_id:
            continue
        try:
            ex_date = date.fromisoformat(ex_str)
        except ValueError:
            continue
        parsed = parse_quarantine_value(kind, value)
        if parsed is None:
            continue
        index[(ticker.upper(), ex_date, kind)].append(
            {"event_id": event_id, "ticker": ticker, **parsed})
    return index


def gather_candidates(
    index: dict[tuple[str, date, str], list[dict]],
    symbols: set[str],
    ex_date: date,
    kind: str,
) -> list[dict]:
    """按证券全部历史 symbol 收集同日同类型的归档候选；跨 symbol 按 event_id 去重。"""
    seen: set[str] = set()
    out: list[dict] = []
    for symbol in sorted(symbols):
        for cand in index.get((symbol.upper(), ex_date, kind), []):
            if cand["event_id"] not in seen:
                seen.add(cand["event_id"])
                out.append(cand)
    return out


def dividend_value_matches(cash: Decimal, currency: str | None, cand: dict) -> bool:
    if cash is None or cand.get("cash") is None:
        return False
    return abs(cash - cand["cash"]) <= DIVIDEND_CASH_TOL \
        and (currency or "USD").upper() == cand["currency"]


def _ratio_close(a: Decimal, b: Decimal) -> bool:
    if a == b:
        return True
    return abs(a - b) <= SPLIT_RATIO_RTOL * max(abs(a), abs(b))


def split_value_matches(split_from: Decimal | None, split_to: Decimal | None, cand: dict) -> bool:
    if not split_from or not split_to or split_from <= 0 or split_to <= 0:
        return False
    return _ratio_close(split_from / split_to, cand["split_from"] / cand["split_to"])


def is_tenure_violation(ex_date: date, list_date: date | None,
                        is_active: bool | None, delist_date: date | None) -> bool:
    """桶 1：ex_date 在证券任期之外（回收污染）。活跃证券残留 delist_date 不算——
    只有 inactive 且 delist_date 非空且 ex_date 晚于退市日才判违例。"""
    if list_date is not None and ex_date < list_date:
        return True
    return (is_active is not None and not is_active
            and delist_date is not None and ex_date > delist_date)


def split_price_verdict(split_from, split_to, prev_close, next_close) -> str:
    """拆股价格检验 -> corroborated / refuted / inconclusive。

    expected = split_from/split_to 是"新价/旧价"（AAPL 4:1 拆分 from=1,to=4 -> 1/4）；
    realized 取同一取向 next_close/prev_close。反证的两个 |log| 判据与取向无关。
    """
    split_from, split_to = _to_decimal(split_from), _to_decimal(split_to)
    prev_close, next_close = _to_decimal(prev_close), _to_decimal(next_close)
    if not split_from or not split_to or split_from <= 0 or split_to <= 0:
        return "inconclusive"
    if not prev_close or not next_close or prev_close <= 0 or next_close <= 0:
        return "inconclusive"
    expected = float(split_from) / float(split_to)
    realized = float(next_close) / float(prev_close)
    if abs(math.log(realized / expected)) <= CORROBORATE_LOG_TOL:
        return "corroborated"
    if abs(math.log(realized)) <= REFUTE_REALIZED_LOG_MAX \
            and abs(math.log(expected)) >= REFUTE_EXPECTED_LOG_MIN:
        return "refuted"
    return "inconclusive"


def dividend_price_verdict(cash, currency, prev_close, next_close) -> str:
    """分红落差检验 -> refuted / inconclusive（分红没有"corroborated 即真"的对称判据）。

    非 USD 现金额与 USD 价格不同币，收益率失真会放大误杀（1 NOK 分红在 $10 股上会被
    算成 10% 名义收益率），宁缺毋滥直接 inconclusive。
    """
    if (currency or "USD").upper() != "USD":
        return "inconclusive"
    cash = _to_decimal(cash)
    prev_close, next_close = _to_decimal(prev_close), _to_decimal(next_close)
    if not cash or cash <= 0 or not prev_close or not next_close \
            or prev_close <= 0 or next_close <= 0:
        return "inconclusive"
    dividend_yield = float(cash) / float(prev_close)
    if dividend_yield < DIVIDEND_YIELD_MIN:
        return "inconclusive"
    realized_drop = 1.0 - float(next_close) / float(prev_close)
    if realized_drop < DIVIDEND_DROP_FRACTION * dividend_yield \
            and (dividend_yield - realized_drop) > DIVIDEND_GAP_MIN:
        return "refuted"
    return "inconclusive"


def classify_orphan(row: dict, candidates: list[dict], today: date) -> Verdict:
    """顺序分桶（第一个命中桶生效）。

    row 键：kind / ex_date / cash_amount / currency / split_from / split_to /
    list_date / delist_date / is_active / prev_close / next_close。
    candidates：已按 ticker+ex_date+kind 命中的归档隔离候选（gather_candidates 输出）。
    """
    # 桶 1：任期违例
    if is_tenure_violation(row["ex_date"], row["list_date"], row["is_active"], row["delist_date"]):
        return Verdict("tenure_violation", "DELETE", "ex_date_outside_tenure")

    # 桶 2：归档匹配
    if row["kind"] == "dividend":
        matches = [c for c in candidates
                   if dividend_value_matches(row["cash_amount"], row["currency"], c)]
    else:
        matches = [c for c in candidates
                   if split_value_matches(row["split_from"], row["split_to"], c)]
    if matches:
        best = min(matches, key=lambda c: c["event_id"])  # 归档重复时取字典序最小 id（同 import 口径）
        if row["kind"] == "split":
            if split_price_verdict(row["split_from"], row["split_to"],
                                   row["prev_close"], row["next_close"]) == "corroborated":
                return Verdict("archive_match_promote", "PROMOTE",
                               "archive_split_price_corroborated",
                               best["event_id"], best["ticker"])
            return Verdict("manual_residual", "MANUAL", "archive_split_price_unconfirmed",
                           best["event_id"], best["ticker"])
        window_hi = row["delist_date"] or today
        if (row["list_date"] is None or row["list_date"] <= row["ex_date"]) \
                and row["ex_date"] <= window_hi:
            return Verdict("archive_match_promote", "PROMOTE", "archive_dividend_in_window",
                           best["event_id"], best["ticker"])
        return Verdict("manual_residual", "MANUAL", "archive_dividend_window_violation",
                       best["event_id"], best["ticker"])

    # 桶 3：无归档匹配的拆股价格检验
    if row["kind"] == "split":
        verdict = split_price_verdict(row["split_from"], row["split_to"],
                                      row["prev_close"], row["next_close"])
        if verdict == "corroborated":
            return Verdict("manual_real_no_vendor_id", "MANUAL", "split_price_corroborated")
        if verdict == "refuted":
            return Verdict("split_refuted", "DELETE", "split_price_refuted")
        return Verdict("manual_residual", "MANUAL", "split_price_inconclusive")

    # 桶 4：无归档匹配的分红落差检验
    if dividend_price_verdict(row["cash_amount"], row["currency"],
                              row["prev_close"], row["next_close"]) == "refuted":
        return Verdict("dividend_refuted", "DELETE", "dividend_drop_refuted")
    return Verdict("manual_residual", "MANUAL", "dividend_price_inconclusive")


def demote_promote_collisions(
    verdicts: dict[int, Verdict],
    security_of: dict[int, int],
) -> dict[int, Verdict]:
    """同一归档 event_id 被推举到多只证券 -> 多归属歧义，全部降级人工（allowlist 必须唯一归属）。"""
    securities_by_event: dict[str, set[int]] = defaultdict(set)
    for ca_id, verdict in verdicts.items():
        if verdict.action == "PROMOTE":
            securities_by_event[verdict.archive_event_id].add(security_of[ca_id])
    conflicted = {eid for eid, sids in securities_by_event.items() if len(sids) > 1}
    if not conflicted:
        return dict(verdicts)
    out: dict[int, Verdict] = {}
    for ca_id, verdict in verdicts.items():
        if verdict.action == "PROMOTE" and verdict.archive_event_id in conflicted:
            out[ca_id] = Verdict("manual_residual", "MANUAL", "archive_ambiguous_multi_security",
                                 verdict.archive_event_id, verdict.archive_ticker)
        else:
            out[ca_id] = verdict
    return out


# --------------------------------------------------------------------------
# DB 读取 / 落地
# --------------------------------------------------------------------------

STRADDLING_ORPHANS_SQL = """
    WITH orphans AS (
        SELECT ca.id, ca.security_id, ca.action_type, ca.ex_date,
               ca.cash_amount, ca.currency, ca.split_from, ca.split_to,
               ca.source, ca.source_event_id
        FROM corporate_actions ca
        WHERE ca.ex_date BETWEEN :start AND :end
          AND ca.action_type IN ('SPLIT', 'DIVIDEND')
          AND upper(ca.source) <> 'MASSIVE'
          AND NOT EXISTS (
              SELECT 1 FROM corporate_actions m
              WHERE m.security_id = ca.security_id
                AND m.action_type = ca.action_type
                AND m.ex_date = ca.ex_date
                AND upper(m.source) = 'MASSIVE')
    ),
    bounds AS (
        SELECT sec.security_id, b.min_date, b.max_date
        FROM (SELECT DISTINCT security_id FROM orphans) sec
        CROSS JOIN LATERAL (
            SELECT MIN(p.date) AS min_date, MAX(p.date) AS max_date
            FROM daily_prices p
            WHERE p.security_id = sec.security_id
        ) b
    )
    SELECT o.id, o.security_id, o.action_type, o.ex_date, o.cash_amount, o.currency,
           o.split_from, o.split_to, o.source, o.source_event_id,
           s.symbol, s.list_date, s.delist_date, s.is_active,
           prev.close AS prev_close, prev.date AS prev_close_date,
           nxt.close AS next_close, nxt.date AS next_close_date
    FROM orphans o
    JOIN securities s ON s.id = o.security_id
    JOIN bounds b ON b.security_id = o.security_id
    LEFT JOIN LATERAL (
        SELECT p.close, p.date FROM daily_prices p
        WHERE p.security_id = o.security_id AND p.date < o.ex_date
        ORDER BY p.date DESC LIMIT 1) prev ON TRUE
    LEFT JOIN LATERAL (
        SELECT p.close, p.date FROM daily_prices p
        WHERE p.security_id = o.security_id AND p.date >= o.ex_date
        ORDER BY p.date ASC LIMIT 1) nxt ON TRUE
    WHERE b.min_date < o.ex_date AND b.max_date >= o.ex_date
    ORDER BY s.symbol, o.ex_date, o.id
"""

BRANCH1_STRADDLE_SQL = """
    WITH uncovered AS (
        SELECT ca.security_id, ca.ex_date
        FROM corporate_actions ca
        WHERE ca.ex_date BETWEEN :start AND :end
          AND ca.action_type IN ('SPLIT', 'DIVIDEND')
          AND upper(ca.source) = 'MASSIVE'
          AND NOT EXISTS (
              SELECT 1 FROM computed_adjustment_factors f
              WHERE f.security_id = ca.security_id
                AND f.source_event_id = ca.source_event_id
                AND f.methodology_version = :mv)
    ),
    bounds AS (
        SELECT sec.security_id, b.min_date, b.max_date
        FROM (SELECT DISTINCT security_id FROM uncovered) sec
        CROSS JOIN LATERAL (
            SELECT MIN(p.date) AS min_date, MAX(p.date) AS max_date
            FROM daily_prices p
            WHERE p.security_id = sec.security_id
        ) b
    )
    SELECT DISTINCT u.security_id
    FROM uncovered u
    JOIN bounds b ON b.security_id = u.security_id
    WHERE b.min_date < u.ex_date AND b.max_date >= u.ex_date
"""


def fetch_straddling_orphans(db_manager: DatabaseManager, start: date, end: date) -> list[dict]:
    """gate 分支 2 的跨立孤行 + 每行的任期字段与前后 close（LATERAL 各取一条）。"""
    from sqlalchemy import text

    with db_manager.get_session() as session:
        rows = session.execute(text(STRADDLING_ORPHANS_SQL), {"start": start, "end": end}).all()
    out = []
    for r in rows:
        out.append({
            "id": r.id,
            "security_id": r.security_id,
            "kind": "dividend" if r.action_type == "DIVIDEND" else "split",
            "ex_date": r.ex_date,
            "cash_amount": _to_decimal(r.cash_amount),
            "currency": r.currency,
            "split_from": _to_decimal(r.split_from),
            "split_to": _to_decimal(r.split_to),
            "source": r.source,
            "source_event_id": r.source_event_id,
            "symbol": r.symbol,
            "list_date": r.list_date,
            "delist_date": r.delist_date,
            "is_active": r.is_active,
            "prev_close": _to_decimal(r.prev_close),
            "next_close": _to_decimal(r.next_close),
        })
    return out


def fetch_symbol_sets(db_manager: DatabaseManager, security_ids: list[int]) -> dict[int, set[str]]:
    """证券 -> 现 symbol ∪ symbol_history 全部历史 symbol（统一大写）。"""
    from sqlalchemy import text

    sets: dict[int, set[str]] = defaultdict(set)
    if not security_ids:
        return sets
    with db_manager.get_session() as session:
        current = session.execute(text(
            "SELECT id, symbol FROM securities WHERE id = ANY(:ids)"),
            {"ids": list(security_ids)}).all()
        history = session.execute(text(
            "SELECT security_id, symbol FROM security_symbol_history WHERE security_id = ANY(:ids)"),
            {"ids": list(security_ids)}).all()
    for sid, symbol in current:
        if symbol:
            sets[sid].add(symbol.upper())
    for sid, symbol in history:
        if symbol:
            sets[sid].add(symbol.upper())
    return sets


def fetch_branch1_straddle_securities(db_manager: DatabaseManager, start: date, end: date,
                                      methodology_version: str) -> set[int]:
    from sqlalchemy import text

    with db_manager.get_session() as session:
        rows = session.execute(text(BRANCH1_STRADDLE_SQL),
                               {"start": start, "end": end, "mv": methodology_version}).all()
    return {r[0] for r in rows}


def apply_deletes(db_manager: DatabaseManager, delete_ids: list[int], backup_path: Path) -> int:
    """先把待删整行（corporate_actions 全列）写备份 TSV，行数核对通过后按 id 删除。"""
    from sqlalchemy import text

    if not delete_ids:
        logger.info("DELETE 桶为空，无可删。")
        return 0
    with db_manager.get_session() as session:
        result = session.execute(text(
            "SELECT * FROM corporate_actions WHERE id = ANY(:ids) ORDER BY id"),
            {"ids": delete_ids})
        columns = list(result.keys())
        rows = result.all()
        if len(rows) != len(delete_ids):
            raise RuntimeError(f"备份行数 {len(rows)} != 待删 {len(delete_ids)}，中止删除。")
        backup_path.parent.mkdir(parents=True, exist_ok=True)
        with backup_path.open("w") as f:
            f.write("\t".join(columns) + "\n")
            for row in rows:
                f.write("\t".join("" if v is None else str(v) for v in row) + "\n")
        logger.info("备份 {} 行 -> {}", len(rows), backup_path)
        deleted = session.execute(text(
            "DELETE FROM corporate_actions WHERE id = ANY(:ids)"),
            {"ids": delete_ids}).rowcount
        if deleted != len(delete_ids):
            raise RuntimeError(f"实际删除 {deleted} != 期望 {len(delete_ids)}，回滚。")
        session.commit()
    return deleted


# --------------------------------------------------------------------------
# 报告
# --------------------------------------------------------------------------

def _value_display(row: dict) -> str:
    if row["kind"] == "dividend":
        return f"{_fmt(row['cash_amount'])} {(row['currency'] or 'USD').upper()}"
    return f"{_fmt(row['split_from'])}:{_fmt(row['split_to'])}"


def write_detail_report(path: Path, orphans: list[dict], verdicts: dict[int, Verdict]) -> None:
    columns = ["bucket", "action", "reason", "ca_id", "security_id", "symbol", "kind",
               "ex_date", "value", "source_event_id", "archive_event_id", "archive_ticker",
               "prev_close", "next_close"]
    ordered = sorted(orphans, key=lambda o: (verdicts[o["id"]].bucket, o["symbol"] or "",
                                             o["ex_date"], o["id"]))
    with path.open("w") as f:
        f.write("\t".join(columns) + "\n")
        for o in ordered:
            v = verdicts[o["id"]]
            f.write("\t".join([
                v.bucket, v.action, v.reason, str(o["id"]), str(o["security_id"]),
                o["symbol"] or "", o["kind"], o["ex_date"].isoformat(), _value_display(o),
                o["source_event_id"] or "", v.archive_event_id or "", v.archive_ticker or "",
                _fmt(o["prev_close"]), _fmt(o["next_close"]),
            ]) + "\n")


def write_allowlist(path: Path, orphans: list[dict], verdicts: dict[int, Verdict]) -> int:
    """PROMOTE 候选 -> allowlist TSV（event_id/security_id/ticker/ex_date/kind），
    (event_id, security_id) 去重（同证券同日重复 POLYGON 行推举同一归档事件时只写一行）。"""
    rows: set[tuple] = set()
    for o in orphans:
        v = verdicts[o["id"]]
        if v.action != "PROMOTE":
            continue
        rows.add((v.archive_event_id, o["security_id"], v.archive_ticker,
                  o["ex_date"].isoformat(), o["kind"]))
    with path.open("w") as f:
        f.write("event_id\tsecurity_id\tticker\tex_date\tkind\n")
        for row in sorted(rows):
            f.write("\t".join(str(x) for x in row) + "\n")
    return len(rows)


def write_summary(path: Path, bucket_rows: Counter, bucket_secs: dict[str, set[int]],
                  prediction: dict[str, int]) -> None:
    with path.open("w") as f:
        f.write("metric\trows\tsecurities\n")
        for bucket in sorted(bucket_rows):
            f.write(f"{bucket}\t{bucket_rows[bucket]}\t{len(bucket_secs[bucket])}\n")
        for key, value in prediction.items():
            f.write(f"{key}\t\t{value}\n")


# --------------------------------------------------------------------------
# 主流程
# --------------------------------------------------------------------------

def main(argv: list[str] | None = None) -> int:
    start_time = time.monotonic()
    configure_script_logging("adjudicate_polygon_orphans")
    args = parse_args(argv)

    start = date.fromisoformat(args.start)
    end = date.fromisoformat(args.end) if args.end else date.today()
    today = date.today()

    quarantine_path = Path(args.quarantine_detail)
    if not quarantine_path.is_absolute():
        quarantine_path = project_root / quarantine_path
    if not quarantine_path.exists():
        logger.error("隔离明细不存在: {}", quarantine_path)
        return 1
    out_dir = Path(args.out_dir)
    if not out_dir.is_absolute():
        out_dir = project_root / out_dir
    out_dir.mkdir(parents=True, exist_ok=True)

    db_manager = None
    try:
        db_manager = DatabaseManager()
        url = db_manager.engine.url
        logger.warning("目标库: host={} db={}（mode={}）", url.host, url.database,
                       "APPLY" if args.apply else "dry-run")
        if args.apply and not args.yes:
            logger.error("--apply 必须搭配 --yes 二次确认；请先核对 dry-run 输出与目标库 host。")
            return 1

        with quarantine_path.open() as f:
            archive_index = parse_quarantine_detail(f)
        candidate_count = sum(len(v) for v in archive_index.values())
        logger.info("归档隔离明细：{} 个 (ticker, ex_date, kind) 键 / {} 条候选（reason ∈ {}）。",
                    len(archive_index), candidate_count, sorted(ARCHIVE_REASONS))

        orphans = fetch_straddling_orphans(db_manager, start, end)
        population_secs = sorted({o["security_id"] for o in orphans})
        logger.info("跨立 POLYGON 孤行：{} 行 / {} 只证券（窗口 {}..{}）。",
                    len(orphans), len(population_secs), start, end)
        symbol_sets = fetch_symbol_sets(db_manager, population_secs)

        verdicts: dict[int, Verdict] = {}
        security_of: dict[int, int] = {}
        for orphan in orphans:
            candidates = gather_candidates(
                archive_index, symbol_sets.get(orphan["security_id"], set()),
                orphan["ex_date"], orphan["kind"])
            verdicts[orphan["id"]] = classify_orphan(orphan, candidates, today)
            security_of[orphan["id"]] = orphan["security_id"]
        verdicts = demote_promote_collisions(verdicts, security_of)

        bucket_rows: Counter = Counter()
        bucket_secs: dict[str, set[int]] = defaultdict(set)
        reason_rows: Counter = Counter()
        for ca_id, verdict in verdicts.items():
            bucket_rows[verdict.bucket] += 1
            bucket_secs[verdict.bucket].add(security_of[ca_id])
            reason_rows[(verdict.bucket, verdict.reason)] += 1
        logger.info("--- 分桶结果（行数 / 证券数）---")
        for bucket in sorted(bucket_rows):
            logger.info("  {}: {} 行 / {} 只", bucket, bucket_rows[bucket], len(bucket_secs[bucket]))
        for (bucket, reason), n in sorted(reason_rows.items()):
            logger.info("    {} :: {}: {}", bucket, reason, n)

        # 释放预测：DELETE+PROMOTE 落地并重建因子后，gate 还剩多少证券被剔除。
        from research.data import securities_with_uncovered_events

        unresolved_secs = {security_of[cid] for cid, v in verdicts.items() if v.action == "MANUAL"}
        branch1_secs = fetch_branch1_straddle_securities(db_manager, start, end,
                                                         args.methodology_version)
        current_gate = set(securities_with_uncovered_events(
            db_manager.engine, start=start, end=end,
            methodology_version=args.methodology_version))
        local_view = set(population_secs) | branch1_secs
        if local_view != current_gate:
            logger.warning("gate 交叉核对不一致：gate 独有 {} 只，本地独有 {} 只（口径漂移，须排查）。",
                           len(current_gate - local_view), len(local_view - current_gate))
        predicted_remaining = (unresolved_secs | branch1_secs) & (current_gate | local_view)
        prediction = {
            "gate_excluded_now": len(current_gate),
            "predicted_remaining_after_fix": len(predicted_remaining),
            "predicted_released": len(current_gate) - len(predicted_remaining & current_gate),
            "branch1_straddle_securities": len(branch1_secs),
        }
        logger.info("--- 释放预测（乐观上界：假设 DELETE+PROMOTE 全落地且因子重建成功；"
                    "PROMOTE 仍可能被导入端 R13 挂起）---")
        for key, value in prediction.items():
            logger.info("  {}: {}", key, value)

        detail_path = out_dir / "adjudicate_polygon_orphans_detail.tsv"
        write_detail_report(detail_path, orphans, verdicts)
        logger.info("明细报告: {}（{} 行）", detail_path, len(orphans))
        allowlist_path = out_dir / "adjudicate_polygon_orphans_allowlist.tsv"
        n_allow = write_allowlist(allowlist_path, orphans, verdicts)
        logger.info("PROMOTE allowlist: {}（{} 行；由 import_corporate_actions_archive "
                    "--adjudicated-allowlist 落库）", allowlist_path, n_allow)
        summary_path = out_dir / "adjudicate_polygon_orphans_summary.tsv"
        write_summary(summary_path, bucket_rows, bucket_secs, prediction)
        logger.info("汇总报告: {}", summary_path)

        delete_ids = sorted(cid for cid, v in verdicts.items() if v.action == "DELETE")
        if args.apply:
            backup_path = out_dir / (
                f"adjudicate_polygon_orphans_delete_backup_{datetime.now():%Y%m%d_%H%M%S}.tsv")
            deleted = apply_deletes(db_manager, delete_ids, backup_path)
            logger.success("已删除 {} 行（备份: {}）。请随后重建复权因子: "
                           "python scripts/update_adjustment_factors.py --all", deleted, backup_path)
        else:
            logger.warning("dry-run：未写库。DELETE 候选 {} 行、PROMOTE 候选 {} 行、人工 {} 行。",
                           len(delete_ids), n_allow,
                           sum(1 for v in verdicts.values() if v.action == "MANUAL"))
        return 0
    except Exception as exc:
        logger.opt(exception=exc).critical("adjudicate_polygon_orphans 执行失败: {}", exc)
        return 1
    finally:
        if db_manager:
            db_manager.close()
        logger.info("耗时: {}", timedelta(seconds=time.monotonic() - start_time))


if __name__ == "__main__":
    raise SystemExit(main())
