"""companies 初始归组：按 CIK 把 US CS 证券归入公司实体（PERMCO 等价物）。

第一期范围（todo_crsp_grade_2026-07 任务 2）：**只做 CS**（活跃 + 退市）。
ETF 发行人 CIK 与基金实体是两回事，绝不给 ETF 归组。cik 为 NULL 的 CS 留空
（多为老退市——任务 4 补 CIK 后自然入组）。

流程：
  (a) 从 securities 取全部 US CS（cik 非空）按 CIK 归组，公司名 seed 取
      最低 id 的活跃 common-equity 行，退级到最低 id 活跃行、再退级到最低 id 行；
  (b) upsert_companies 写公司行（冲突键 cik，重跑幂等）；
  (c) 集合式 UPDATE securities.company_id（只填 NULL——已挂到**不同**公司的
      行绝不静默改挂，进 reassign 报告，人工确认后加 --allow-reassign 执行）；
  (d) 报告落 logs/：覆盖率、同 CIK 名称分歧样本（改名世系，人工过目不阻塞）、
      多证券组拆成 真双类股名录 vs 工具行误标（common-equity 名称分类器与
      research/company_market_cap.py 共用同一函数，flag-don't-drop：误标行
      照挂 company_id，只是不进双类股名录、不计合并市值）；
  (e) 验收探针：goog+googl、brk.a+brk.b 各归同一 company。

用法：
    python scripts/build_companies.py                  # dry-run（默认，只报告不写库）
    python scripts/build_companies.py --apply          # 确认 dry-run 报告后执行
    python scripts/build_companies.py --apply --allow-reassign   # 含改挂（先人工过 reassign 报告）
"""
import argparse
import csv
import os
import re
import sys
import time
from datetime import timedelta

from loguru import logger
from sqlalchemy import text

project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from db_manager import DatabaseManager
from research.company_market_cap import is_common_equity_name
from utils.script_logging import setup_logging as configure_script_logging

DUAL_CLASS_REPORT = "companies_dual_class.tsv"
MISLABEL_REPORT = "companies_instrument_mislabel.tsv"
NAME_CONFLICT_REPORT = "companies_name_conflicts.tsv"
REASSIGN_CONFLICT_REPORT = "companies_reassign_conflicts.tsv"
SUMMARY_REPORT = "companies_grouping_summary.tsv"

# 验收探针：已知双类股，两腿必须归同一 company（symbol 库内小写）。
ACCEPTANCE_PAIRS = (("goog", "googl"), ("brk.a", "brk.b"))


def setup_logging():
    configure_script_logging("build_companies")


def create_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="companies 初始归组：按 CIK 把 US CS 证券归入公司实体。",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument("--dry-run", action="store_true", default=True,
                        help="只输出归组 plan 与报告，不写库（默认）。")
    parser.add_argument("--apply", action="store_true",
                        help="执行 upsert_companies + securities.company_id 回填。")
    parser.add_argument("--allow-reassign", action="store_true",
                        help="允许把已挂到不同 company 的证券改挂到按 CIK 计算的新公司。\n"
                             "默认这些行只进 reassign 报告绝不改写；须先人工确认报告。")
    parser.add_argument("--report-dir", default="logs/manual_backfill",
                        help="报告输出目录（默认 logs/manual_backfill）。")
    parser.add_argument("--sample-size", type=int, default=10,
                        help="控制台打印的各类样本行数（默认 10）。")
    return parser


# ------------------------------------------------------------------ #
# (a) 归组
# ------------------------------------------------------------------ #

def fetch_cs_rows(db_manager) -> list[dict]:
    """全部 US CS（cik 非空）——活跃 + 退市。ETF 从不进入本查询。"""
    sql = text(
        """
        select id, symbol, name, cik, is_active
        from securities
        where market = 'US' and type = 'CS'
          and cik is not null and btrim(cik) <> ''
        order by cik, id
        """
    )
    with db_manager.engine.connect() as conn:
        return [dict(row) for row in conn.execute(sql).mappings()]


def _seed_name(members: list[dict]) -> str | None:
    """公司名 seed：最低 id 活跃 common-equity 行 -> 最低 id 活跃行 -> 最低 id 行。

    members 已按 security_id 升序。每级内跳过空名称行（seed 一个 NULL 名没有
    意义）；全组无名称时返回 None（upsert 时省略 name 键，不覆盖已有值）。
    """
    tiers = (
        [m for m in members if m["is_active"] and m["is_common_equity"]],
        [m for m in members if m["is_active"]],
        members,
    )
    for tier in tiers:
        for member in tier:
            if (member["name"] or "").strip():
                return member["name"]
    return None


def build_grouping(rows: list[dict]) -> dict[str, dict]:
    """securities 行 -> {cik: {cik, name, members[]}}（纯逻辑，可单测）。

    cik 只做 strip，不做补零归一化：库内已验证统一 10 位零填充；万一出现
    异形 cik，宁可各自成组进报告，也不猜测两串字符串是同一实体。
    """
    groups: dict[str, dict] = {}
    for row in rows:
        cik = str(row["cik"]).strip()
        if not cik:
            continue
        member = {
            "security_id": row["id"],
            "symbol": row["symbol"],
            "name": row["name"],
            "is_active": bool(row["is_active"]),
            "is_common_equity": is_common_equity_name(row["name"]),
        }
        groups.setdefault(cik, {"cik": cik, "members": []})["members"].append(member)
    for group in groups.values():
        group["members"].sort(key=lambda m: m["security_id"])
        group["name"] = _seed_name(group["members"])
    return groups


# ------------------------------------------------------------------ #
# (d) 分类与报告
# ------------------------------------------------------------------ #

def _name_stem(name: str | None) -> str:
    """名称词干（首个字母数字 token，小写）——同 CIK 名称分歧的粗探测器。

    'Alphabet Inc. Class A' vs 'Alphabet Inc. Class C' 同词干不报；
    'New York Mortgage Trust' vs 'Adamas Trust'（改名世系）词干不同要报。
    """
    tokens = re.findall(r"[a-z0-9]+", (name or "").lower())
    return tokens[0] if tokens else ""


def classify_groups(groups: dict[str, dict]) -> tuple[list[dict], list[dict], list[dict]]:
    """多证券 CIK 组 -> (真双类股名录, 工具行误标名录, 同 CIK 名称分歧样本)。

    - 组内 common-equity 成员 >= 2 -> 这些成员进双类股名录（验收交付物）；
    - 组内非 common-equity 成员 -> 工具行误标名录（照挂 company_id，
      只是不进双类股名录、不计合并市值）；
    - common-equity 成员（无则全体成员）名称词干 > 1 种 -> 名称分歧样本
      （改名世系嫌疑，人工过目，不阻塞归组）。
    """
    dual_class: list[dict] = []
    mislabel: list[dict] = []
    name_conflicts: list[dict] = []
    for cik in sorted(groups):
        group = groups[cik]
        members = group["members"]
        if len(members) < 2:
            continue
        commons = [m for m in members if m["is_common_equity"]]
        instruments = [m for m in members if not m["is_common_equity"]]
        if len(commons) >= 2:
            for member in commons:
                dual_class.append({"cik": cik, "company_name": group["name"], **member})
        for member in instruments:
            mislabel.append({"cik": cik, "company_name": group["name"], **member})
        stems = {
            _name_stem(m["name"])
            for m in (commons or members)
            if (m["name"] or "").strip()
        }
        if len(stems) > 1:
            for member in members:
                name_conflicts.append({"cik": cik, **member})
    return dual_class, mislabel, name_conflicts


def find_reassign_conflicts(db_manager) -> list[dict]:
    """已挂 company_id 但所挂公司 cik 与证券 cik 不一致的行（改挂候选）。

    这类行默认绝不改写——进报告人工裁决；dry-run 与 apply 用同一定义。
    """
    sql = text(
        """
        select s.id as security_id, s.symbol, s.cik,
               s.company_id as old_company_id, oc.cik as old_company_cik
        from securities s
        join companies oc on oc.id = s.company_id
        where s.market = 'US' and s.type = 'CS'
          and s.cik is not null and btrim(s.cik) <> ''
          and oc.cik is distinct from btrim(s.cik)
        order by s.id
        """
    )
    with db_manager.engine.connect() as conn:
        return [dict(row) for row in conn.execute(sql).mappings()]


def _write_tsv(path: str, rows: list[dict], columns: list[str]) -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh, delimiter="\t")
        writer.writerow(columns)
        for row in rows:
            writer.writerow([("" if row.get(col) is None else row.get(col)) for col in columns])


MEMBER_COLUMNS = ["cik", "company_name", "security_id", "symbol", "name", "is_active"]
NAME_CONFLICT_COLUMNS = ["cik", "security_id", "symbol", "name", "is_active", "is_common_equity"]
REASSIGN_COLUMNS = ["security_id", "symbol", "cik", "old_company_id", "old_company_cik"]


def write_reports(report_dir: str, *, dual_class, mislabel, name_conflicts,
                  reassign_conflicts, summary: dict) -> None:
    _write_tsv(os.path.join(report_dir, DUAL_CLASS_REPORT), dual_class, MEMBER_COLUMNS)
    _write_tsv(os.path.join(report_dir, MISLABEL_REPORT), mislabel, MEMBER_COLUMNS)
    _write_tsv(os.path.join(report_dir, NAME_CONFLICT_REPORT), name_conflicts, NAME_CONFLICT_COLUMNS)
    _write_tsv(os.path.join(report_dir, REASSIGN_CONFLICT_REPORT), reassign_conflicts, REASSIGN_COLUMNS)
    summary_rows = [{"metric": key, "value": value} for key, value in summary.items()]
    _write_tsv(os.path.join(report_dir, SUMMARY_REPORT), summary_rows, ["metric", "value"])
    logger.info("报告已写入 {}/: {} {} {} {} {}", report_dir, DUAL_CLASS_REPORT,
                MISLABEL_REPORT, NAME_CONFLICT_REPORT, REASSIGN_CONFLICT_REPORT, SUMMARY_REPORT)


# ------------------------------------------------------------------ #
# (b)(c) 写入
# ------------------------------------------------------------------ #

def apply_grouping(db_manager, groups: dict[str, dict], *, allow_reassign: bool) -> tuple[int, int, int]:
    """写 companies + 回填 securities.company_id。返回 (公司 upsert 行数, 新挂行数, 改挂行数)。

    公司名缺失时省略 name 键（upsert_companies 冲突时不会把既有名覆盖成 NULL）。
    回填分两条腿：NULL -> 值 的新挂无条件执行；非 NULL -> 不同值 的改挂只在
    --allow-reassign 下执行（默认腿保证幂等重跑是 no-op）。UPDATE 限定
    type='CS' AND market='US'——ETF 即便同 cik 也绝不触碰。
    """
    company_rows = []
    for cik in sorted(groups):
        row: dict = {"cik": cik}
        name = groups[cik]["name"]
        if (name or "").strip():
            row["name"] = name
        company_rows.append(row)
    companies_written = db_manager.upsert_companies(company_rows)

    linked = 0
    reassigned = 0
    with db_manager.engine.connect() as conn:
        # btrim(s.cik) 与 build_grouping 的 Python strip 同口径——万一存在带
        # 空白的 cik，归组键与回填 join 键也不会分叉。
        linked = conn.execute(text(
            """
            update securities s
            set company_id = c.id
            from companies c
            where btrim(s.cik) = c.cik
              and s.type = 'CS' and s.market = 'US'
              and s.company_id is null
            """
        )).rowcount or 0
        if allow_reassign:
            reassigned = conn.execute(text(
                """
                update securities s
                set company_id = c.id
                from companies c
                where btrim(s.cik) = c.cik
                  and s.type = 'CS' and s.market = 'US'
                  and s.company_id is not null
                  and s.company_id <> c.id
                """
            )).rowcount or 0
        conn.commit()
    return companies_written, linked, reassigned


# ------------------------------------------------------------------ #
# 覆盖率与验收探针
# ------------------------------------------------------------------ #

def coverage_stats(db_manager) -> dict:
    sql = text(
        """
        select
          count(*) filter (where is_active) as active_cs,
          count(*) filter (where is_active and company_id is not null) as active_cs_linked,
          count(*) filter (where is_active
                           and (company_id is not null
                                or (cik is not null and btrim(cik) <> ''))) as active_cs_linkable,
          count(*) as total_cs,
          count(*) filter (where company_id is not null) as total_cs_linked,
          count(*) filter (where company_id is not null
                           or (cik is not null and btrim(cik) <> '')) as total_cs_linkable,
          count(*) filter (where cik is not null and btrim(cik) <> ''
                           and btrim(cik) !~ '^[0-9]{10}$') as malformed_cik
        from securities
        where market = 'US' and type = 'CS'
        """
    )
    with db_manager.engine.connect() as conn:
        return dict(conn.execute(sql).mappings().one())


def _pct(numerator: int, denominator: int) -> str:
    if not denominator:
        return "n/a"
    return f"{100.0 * numerator / denominator:.2f}%"


def run_acceptance_probes(db_manager, groups: dict[str, dict], *, applied: bool) -> bool:
    """goog+googl / brk.a+brk.b 必须归同一 company。

    applied=True 时查库里的 company_id；dry-run 时按计算出的 CIK 分组判定。
    符号缺失（本地/测试库）只告警不判失败；存在但不同组判失败（返回 False）。
    """
    if applied:
        symbols = sorted({symbol for pair in ACCEPTANCE_PAIRS for symbol in pair})
        sql = text(
            """
            select symbol, company_id from securities
            where symbol = any(:symbols) and is_active
            """
        )
        with db_manager.engine.connect() as conn:
            found = {row["symbol"]: row["company_id"] for row in conn.execute(
                sql, {"symbols": symbols}).mappings()}
    else:
        found = {}
        for cik, group in groups.items():
            for member in group["members"]:
                if member["is_active"]:
                    found[member["symbol"]] = cik

    all_ok = True
    for left, right in ACCEPTANCE_PAIRS:
        if left not in found or right not in found:
            logger.warning("验收探针跳过 {}+{}: 符号不在库中（本地/测试库？）", left, right)
            continue
        left_group, right_group = found[left], found[right]
        if left_group is None or right_group is None or left_group != right_group:
            logger.error("验收探针失败 {}+{}: {} vs {}", left, right, left_group, right_group)
            all_ok = False
        else:
            logger.success("验收探针通过 {}+{}: company={}", left, right, left_group)
    return all_ok


# ------------------------------------------------------------------ #
# 编排
# ------------------------------------------------------------------ #

def run(args: argparse.Namespace, db_manager) -> int:
    is_apply = args.apply
    mode = "apply" if is_apply else "dry-run"
    logger.info("build_companies 启动（{}，allow_reassign={}）", mode, args.allow_reassign)

    rows = fetch_cs_rows(db_manager)
    logger.info("US CS 且 cik 非空: {} 行", len(rows))
    groups = build_grouping(rows)
    multi = {cik: g for cik, g in groups.items() if len(g["members"]) > 1}
    dual_class, mislabel, name_conflicts = classify_groups(groups)
    dual_class_groups = len({r["cik"] for r in dual_class})
    mislabel_groups = len({r["cik"] for r in mislabel})
    name_conflict_groups = len({r["cik"] for r in name_conflicts})
    reassign_conflicts = find_reassign_conflicts(db_manager)

    logger.info("CIK 组: {}（多证券组 {}；真双类股组 {}；含工具行误标组 {}；名称分歧组 {}）",
                len(groups), len(multi), dual_class_groups, mislabel_groups, name_conflict_groups)
    for row in dual_class[: args.sample_size]:
        logger.info("  双类股: cik={} id={} {} | {}", row["cik"], row["security_id"],
                    row["symbol"], row["name"])
    for row in mislabel[: args.sample_size]:
        logger.info("  工具行误标: cik={} id={} {} | {}", row["cik"], row["security_id"],
                    row["symbol"], row["name"])
    for row in name_conflicts[: args.sample_size]:
        logger.info("  名称分歧: cik={} id={} {} | {}", row["cik"], row["security_id"],
                    row["symbol"], row["name"])
    if reassign_conflicts:
        logger.warning("发现 {} 行已挂到不同 company（绝不静默改挂，见 {}；"
                       "人工确认后加 --allow-reassign）",
                       len(reassign_conflicts), REASSIGN_CONFLICT_REPORT)
        for row in reassign_conflicts[: args.sample_size]:
            logger.warning("  改挂候选: id={} {} cik={} 现挂 company={}（cik={}）",
                           row["security_id"], row["symbol"], row["cik"],
                           row["old_company_id"], row["old_company_cik"])

    companies_written = linked = reassigned = 0
    if is_apply:
        companies_written, linked, reassigned = apply_grouping(
            db_manager, groups, allow_reassign=args.allow_reassign
        )
        logger.success("apply 完成: companies upsert {} 行, 新挂 company_id {} 行, 改挂 {} 行",
                       companies_written, linked, reassigned)

    stats = coverage_stats(db_manager)
    if stats["malformed_cik"]:
        logger.warning("发现 {} 行 cik 非 10 位零填充格式——各自成组不猜测，请人工核查。",
                       stats["malformed_cik"])
    if is_apply:
        logger.info("覆盖率: 活跃 CS company_id {} / {} = {}；全 CS {} / {} = {}",
                    stats["active_cs_linked"], stats["active_cs"],
                    _pct(stats["active_cs_linked"], stats["active_cs"]),
                    stats["total_cs_linked"], stats["total_cs"],
                    _pct(stats["total_cs_linked"], stats["total_cs"]))
    else:
        logger.info("预期覆盖率（apply 后）: 活跃 CS {} / {} = {}；全 CS {} / {} = {}",
                    stats["active_cs_linkable"], stats["active_cs"],
                    _pct(stats["active_cs_linkable"], stats["active_cs"]),
                    stats["total_cs_linkable"], stats["total_cs"],
                    _pct(stats["total_cs_linkable"], stats["total_cs"]))

    summary = {
        "mode": mode,
        "cs_rows_with_cik": len(rows),
        "cik_groups": len(groups),
        "multi_security_groups": len(multi),
        "dual_class_groups": dual_class_groups,
        "dual_class_securities": len(dual_class),
        "instrument_mislabel_groups": mislabel_groups,
        "instrument_mislabel_securities": len(mislabel),
        "name_conflict_groups": name_conflict_groups,
        "reassign_conflicts": len(reassign_conflicts),
        "companies_written": companies_written,
        "securities_linked": linked,
        "securities_reassigned": reassigned,
        "active_cs": stats["active_cs"],
        "active_cs_linked": stats["active_cs_linked"],
        "active_cs_linked_pct": _pct(stats["active_cs_linked"], stats["active_cs"]),
        "active_cs_linkable_pct": _pct(stats["active_cs_linkable"], stats["active_cs"]),
        "total_cs": stats["total_cs"],
        "total_cs_linked": stats["total_cs_linked"],
        "malformed_cik": stats["malformed_cik"],
    }
    write_reports(args.report_dir, dual_class=dual_class, mislabel=mislabel,
                  name_conflicts=name_conflicts, reassign_conflicts=reassign_conflicts,
                  summary=summary)

    probes_ok = run_acceptance_probes(db_manager, groups, applied=is_apply)
    if not is_apply:
        logger.warning("以上为 dry-run 输出。确认报告无误后加 --apply 执行。")
    if not probes_ok:
        logger.error("验收探针未全部通过，退出码 1。")
        return 1
    return 0


def main(argv: list[str] | None = None) -> int:
    start_time = time.monotonic()
    setup_logging()
    args = create_parser().parse_args(argv)

    db_manager = None
    try:
        db_manager = DatabaseManager()
        return run(args, db_manager)
    except Exception as exc:
        logger.opt(exception=exc).critical("build_companies 执行失败: {}", exc)
        return 1
    finally:
        if db_manager:
            db_manager.close()
        logger.info("耗时: {}", timedelta(seconds=time.monotonic() - start_time))


if __name__ == "__main__":
    raise SystemExit(main())
