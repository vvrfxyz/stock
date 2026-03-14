import os
import sys
import time
import argparse
import random
from datetime import date, datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import Counter
from decimal import Decimal, getcontext
from typing import Optional

import pandas as pd
from loguru import logger
from sqlalchemy import func
from tqdm import tqdm

# --- 路径设置 ---
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)
# --- 路径设置结束 ---

from db_manager import DatabaseManager
from data_models.models import Security, StockDividend, StockSplit
from utils.adj_factor import recalc_adj_factor_for_security


MAX_CONCURRENT_WORKERS = 4
UPSERT_BATCH_SIZE = 2000

DIVIDEND_QUANT = Decimal("1.0000000000")  # 10 decimals, aligned with StockDividend.cash_amount scale
SPLIT_QUANT = Decimal("1.0000000000")  # 10 decimals, aligned with StockSplit split_to/split_from scale


def setup_logging():
    logger.remove()
    log_format = (
        "<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
        "<level>{level: <8}</level> | "
        "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>"
    )
    logger.add(sys.stderr, level="INFO", format=log_format)
    log_dir = os.path.join(project_root, "logs")
    os.makedirs(log_dir, exist_ok=True)
    logger.add(
        os.path.join(log_dir, f"backfill_actions_from_yfinance_{{time}}.log"),
        rotation="10 MB",
        retention="10 days",
        level="DEBUG",
    )
    logger.info("日志记录器设置完成。")


def create_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "使用 YFinance 补全公司行动（分红/拆股）。\n"
            "\n"
            "注意：YFinance 的 dividends/splits 通常是按“当前股本口径”进行拆股调整后的数值。\n"
            "本脚本会使用 splits 序列将 dividends 反向还原到事件发生时的 raw 口径，以保持与 daily_prices.close（raw）一致。\n"
        ),
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument("symbols", nargs="*", help="要处理的股票代码列表 (e.g., 'aapl', 'nvda')。")
    parser.add_argument("--all", action="store_true", help="处理所有活跃股票。")
    parser.add_argument("--market", type=str, help="仅处理指定市场的股票 (例如: US, HK, CNA)。")
    parser.add_argument("--limit", type=int, default=0, help="限制处理的股票数量，用于测试。0 表示不限制。")
    parser.add_argument(
        "--workers",
        type=int,
        default=MAX_CONCURRENT_WORKERS,
        help=f"并发执行的线程数 (默认: {MAX_CONCURRENT_WORKERS})。",
    )
    parser.add_argument("--start-date", type=str, help="仅补全该日期及之后 (YYYY-MM-DD)。")
    parser.add_argument("--end-date", type=str, help="仅补全该日期及之前 (YYYY-MM-DD)。")
    recalc_group = parser.add_mutually_exclusive_group()
    recalc_group.add_argument(
        "--recalc-adj-factor",
        action="store_true",
        help="补全 actions 后强制重算 adj_factor（无论是否有新增 actions）。",
    )
    recalc_group.add_argument(
        "--skip-recalc-adj-factor",
        action="store_true",
        help="补全 actions 后不自动重算 adj_factor。",
    )
    return parser


def _parse_date(date_str: Optional[str]) -> Optional[date]:
    if not date_str:
        return None
    try:
        return datetime.strptime(date_str, "%Y-%m-%d").date()
    except ValueError:
        return None


def _iter_batches(rows: list[dict], batch_size: int):
    for i in range(0, len(rows), batch_size):
        yield rows[i:i + batch_size]


def get_securities_to_process(db_manager: DatabaseManager, args: argparse.Namespace) -> list[Security]:
    symbols = [s.lower() for s in args.symbols if s]
    with db_manager.get_session() as session:
        query = session.query(Security).filter(Security.is_active == True)

        if symbols:
            query = query.filter(Security.symbol.in_(symbols))
        elif args.market:
            query = query.filter(func.upper(Security.market) == args.market.upper())

        if args.limit > 0:
            query = query.limit(args.limit)

        return query.order_by(Security.symbol.asc()).all()


def _get_currency(security: Security) -> Optional[str]:
    if security.currency:
        return security.currency.upper()
    if (security.market or "").upper() == "US":
        return "USD"
    return None


def _to_date_decimal_series(series: pd.Series) -> list[tuple[date, Decimal]]:
    if series is None or series.empty:
        return []

    series = series.dropna()
    if series.empty:
        return []

    idx = pd.to_datetime(series.index, errors="coerce")
    series = series[~idx.isna()]
    if series.empty:
        return []

    series.index = pd.to_datetime(series.index, errors="coerce")
    series = series[~series.index.isna()].sort_index()

    results: list[tuple[date, Decimal]] = []
    for ts, value in series.items():
        d = ts.date()
        try:
            dec = Decimal(str(value))
        except Exception:
            continue
        results.append((d, dec))
    return results


def _compute_raw_dividend(
    dividend_date: date,
    dividend_adjusted: Decimal,
    splits: list[tuple[date, Decimal]],
) -> Decimal:
    """
    将“按当前股本口径拆股调整后的 dividend”还原为该日 raw 口径。

    经验规律（Yahoo/YFinance 常见口径）：
    - 历史 dividend 会被后续 splits 反向缩放（例如未来发生 4-for-1，则更早 dividend 会除以 4）。
    - 因此 raw = adjusted * Π(split_ratio for split_date > dividend_date)
    """
    ratio = Decimal("1")
    for split_date, split_ratio in splits:
        if split_date > dividend_date:
            ratio *= split_ratio
    return (dividend_adjusted * ratio).quantize(DIVIDEND_QUANT)


def _quantize_dividend(dividend: Decimal) -> Decimal:
    return dividend.quantize(DIVIDEND_QUANT)


def _choose_dividend_scaling(
    symbol: str,
    dividends_by_date: list[tuple[date, Decimal]],
    splits: list[tuple[date, Decimal]],
    existing_dividend_totals: dict[date, Decimal],
) -> bool:
    """
    YFinance dividends 口径可能存在差异（通常为“按当前股本口径拆股调整后”的数值）。
    这里通过与数据库中已存在的 dividend 进行交叉验证，自动选择更匹配的缩放方式：
    - 返回 True：认为 YFinance dividends 已被拆股调整，需要反向还原为 raw（乘以后续 split 比例积）
    - 返回 False：认为 YFinance dividends 已是 raw，无需缩放
    """
    overlap = [d for d, _ in dividends_by_date if d in existing_dividend_totals]
    if len(overlap) < 5:
        logger.info(f"[{symbol}] 与数据库分红记录交叉验证样本不足（overlap={len(overlap)}），默认按“拆股调整后”处理。")
        return True

    tol_abs = Decimal("0.0001")
    tol_rel = Decimal("0.0001")

    def _is_match(a: Decimal, b: Decimal) -> bool:
        diff = abs(a - b)
        tol = max(tol_abs, abs(b) * tol_rel)
        return diff <= tol

    mismatches_adjusted = 0
    mismatches_raw = 0
    total_err_adjusted = Decimal("0")
    total_err_raw = Decimal("0")

    div_map = {d: v for d, v in dividends_by_date}
    for d in overlap:
        y = div_map[d]
        db_total = existing_dividend_totals[d]
        cand_adjusted = _compute_raw_dividend(d, y, splits)
        cand_raw = _quantize_dividend(y)

        total_err_adjusted += abs(cand_adjusted - db_total)
        total_err_raw += abs(cand_raw - db_total)
        if not _is_match(cand_adjusted, db_total):
            mismatches_adjusted += 1
        if not _is_match(cand_raw, db_total):
            mismatches_raw += 1

    logger.info(
        f"[{symbol}] dividends 口径验证："
        f"assume_adjusted mismatches={mismatches_adjusted}/{len(overlap)} err={total_err_adjusted}; "
        f"assume_raw mismatches={mismatches_raw}/{len(overlap)} err={total_err_raw}"
    )

    if mismatches_adjusted < mismatches_raw:
        return True
    if mismatches_raw < mismatches_adjusted:
        return False
    # tie-breaker: smaller total error wins
    if total_err_adjusted <= total_err_raw:
        return True
    return False


def _fetch_existing_action_dates(session, security_id: int) -> tuple[set[date], set[date]]:
    existing_div_dates = {
        d for (d,) in session.query(StockDividend.ex_dividend_date).filter(StockDividend.security_id == security_id)
        if d
    }
    existing_split_dates = {
        d for (d,) in session.query(StockSplit.execution_date).filter(StockSplit.security_id == security_id)
        if d
    }
    return existing_div_dates, existing_split_dates


def _fetch_existing_dividend_totals(session, security_id: int) -> dict[date, Decimal]:
    rows = (
        session.query(StockDividend.ex_dividend_date, func.sum(StockDividend.cash_amount))
        .filter(StockDividend.security_id == security_id)
        .group_by(StockDividend.ex_dividend_date)
        .all()
    )
    totals: dict[date, Decimal] = {}
    for d, total in rows:
        if d and total is not None:
            totals[d] = Decimal(total)
    return totals


def process_security(
    security: Security,
    db_manager: DatabaseManager,
    start_date: Optional[date],
    end_date: Optional[date],
    force_recalc: bool,
    skip_recalc: bool,
) -> tuple[str, str, int, int]:
    symbol = security.symbol
    getcontext().prec = 28

    try:
        try:
            import yfinance as yf
        except Exception as e:
            logger.error(f"[{symbol}] 导入 yfinance 失败，请先安装依赖: {e}")
            return symbol, "ERROR_MISSING_DEP", 0, 0

        currency = _get_currency(security)
        if not currency:
            logger.warning(f"[{symbol}] 无法确定分红货币（Security.currency 为空且非 US 市场），跳过。")
            return symbol, "SKIP_NO_CURRENCY", 0, 0

        time.sleep(random.uniform(0.2, 0.6))
        ticker = yf.Ticker(symbol.upper())

        dividends_series = getattr(ticker, "dividends", None)
        splits_series = getattr(ticker, "splits", None)

        dividends = _to_date_decimal_series(dividends_series) if dividends_series is not None else []
        splits_pairs_all = _to_date_decimal_series(splits_series) if splits_series is not None else []

        # 过滤区间（dividends 直接过滤；splits 的区间过滤只影响“插入哪些 splits”，不影响 dividend 反向还原）
        if start_date:
            dividends = [(d, v) for d, v in dividends if d >= start_date]
        if end_date:
            dividends = [(d, v) for d, v in dividends if d <= end_date]

        # 同日重复记录处理：
        # - dividends：同日按金额求和
        # - splits：同日按比例求积
        dividends_by_date: dict[date, Decimal] = {}
        for d, v in dividends:
            dividends_by_date[d] = dividends_by_date.get(d, Decimal("0")) + v
        dividends = sorted(dividends_by_date.items(), key=lambda x: x[0])

        splits_by_date: dict[date, Decimal] = {}
        for d, r in splits_pairs_all:
            if r <= 0:
                continue
            splits_by_date[d] = splits_by_date.get(d, Decimal("1")) * r

        # splits 反向还原 dividend 到 raw 口径；并过滤无效 split_ratio
        splits: list[tuple[date, Decimal]] = []
        for d, r in sorted(splits_by_date.items(), key=lambda x: x[0]):
            splits.append((d, r.quantize(SPLIT_QUANT)))

        with db_manager.get_session() as session:
            existing_div_dates, existing_split_dates = _fetch_existing_action_dates(session, security.id)
            existing_dividend_totals = _fetch_existing_dividend_totals(session, security.id)

        assume_adjusted = _choose_dividend_scaling(
            symbol=symbol,
            dividends_by_date=dividends,
            splits=splits,
            existing_dividend_totals=existing_dividend_totals,
        )

        new_dividends: list[dict] = []
        for ex_date, div_adj in dividends:
            if ex_date in existing_div_dates:
                continue
            if div_adj <= 0:
                continue
            div_raw = (
                _compute_raw_dividend(ex_date, div_adj, splits)
                if assume_adjusted
                else _quantize_dividend(div_adj)
            )
            if div_raw <= 0:
                continue
            new_dividends.append(
                {
                    "ex_dividend_date": ex_date,
                    "declaration_date": None,
                    "record_date": None,
                    "pay_date": None,
                    "cash_amount": div_raw,
                    "currency": currency,
                    "frequency": None,
                }
            )

        new_splits: list[dict] = []
        for exec_date, ratio in splits:
            if start_date and exec_date < start_date:
                continue
            if end_date and exec_date > end_date:
                continue
            if exec_date in existing_split_dates:
                continue
            new_splits.append(
                {
                    "execution_date": exec_date,
                    "declaration_date": None,
                    "split_to": ratio,
                    "split_from": Decimal("1").quantize(SPLIT_QUANT),
                }
            )

        inserted_dividends = 0
        inserted_splits = 0
        for batch in _iter_batches(new_dividends, UPSERT_BATCH_SIZE):
            inserted_dividends += db_manager.upsert_dividends(security.id, batch)
        for batch in _iter_batches(new_splits, UPSERT_BATCH_SIZE):
            inserted_splits += db_manager.upsert_splits(security.id, batch)

        actions_changed = (inserted_dividends + inserted_splits) > 0
        if actions_changed:
            db_manager.update_security_timestamp_native_sql(security.id, "actions_last_updated_at")

        if skip_recalc:
            logger.info(f"[{symbol}] 已启用 --skip-recalc-adj-factor，跳过 adj_factor 重算。")
            return symbol, "SUCCESS_SKIP_RECALC", inserted_dividends, inserted_splits

        if force_recalc or actions_changed:
            rows = recalc_adj_factor_for_security(db_manager=db_manager, security_id=security.id, symbol=symbol)
            logger.success(
                f"[{symbol}] 补全 actions 完成：dividends 新增 {inserted_dividends}，splits 新增 {inserted_splits}；"
                f"adj_factor 重算行数 {rows}。"
            )
            return symbol, "SUCCESS", inserted_dividends, inserted_splits

        logger.success(f"[{symbol}] 无新增 actions，无需重算 adj_factor。")
        return symbol, "SUCCESS_NO_CHANGES", 0, 0

    except Exception as e:
        logger.error(f"[{symbol}] 补全 actions 失败: {e}", exc_info=True)
        return symbol, "ERROR", 0, 0


def main():
    start_time = time.monotonic()
    setup_logging()
    parser = create_parser()
    args = parser.parse_args()

    symbols = [s.lower() for s in args.symbols if s]
    if not any([symbols, args.all, args.market]):
        logger.warning("没有指定任何操作。请提供 symbols，或使用 --all / --market 标志。")
        parser.print_help()
        return

    start_date = _parse_date(args.start_date)
    end_date = _parse_date(args.end_date)
    if args.start_date and start_date is None:
        logger.error("--start-date 必须是 YYYY-MM-DD 格式。")
        return
    if args.end_date and end_date is None:
        logger.error("--end-date 必须是 YYYY-MM-DD 格式。")
        return

    db_manager = None
    try:
        db_manager = DatabaseManager()
        securities = get_securities_to_process(db_manager, args)
        if not securities:
            logger.success("✅ 根据您的条件，没有找到需要处理的股票。任务完成。")
            return

        total_count = len(securities)
        logger.info(f"共找到 {total_count} 支股票需要补全 actions，将使用最多 {args.workers} 个并发线程。")

        results_counter = Counter()
        total_new_dividends = 0
        total_new_splits = 0

        with ThreadPoolExecutor(max_workers=args.workers) as executor:
            future_to_security = {
                executor.submit(
                    process_security,
                    sec,
                    db_manager,
                    start_date,
                    end_date,
                    args.recalc_adj_factor,
                    args.skip_recalc_adj_factor,
                ): sec
                for sec in securities
            }

            for future in tqdm(as_completed(future_to_security), total=total_count, desc="补全 actions(YFinance)"):
                try:
                    _symbol, status, new_divs, new_spl = future.result()
                    results_counter[status] += 1
                    total_new_dividends += new_divs
                    total_new_splits += new_spl
                except Exception as exc:
                    sec = future_to_security[future]
                    logger.error(f"任务 {sec.symbol} 生成了未捕获的异常: {exc}", exc_info=True)
                    results_counter["FATAL_ERROR"] += 1

        logger.info("--- 任务执行统计 ---")
        logger.info(f"  成功: {results_counter['SUCCESS']}")
        logger.info(f"  成功(无变化): {results_counter['SUCCESS_NO_CHANGES']}")
        logger.info(f"  成功(跳过重算): {results_counter['SUCCESS_SKIP_RECALC']}")
        logger.info(f"  跳过(无货币): {results_counter['SKIP_NO_CURRENCY']}")
        logger.info(f"  错误(缺依赖): {results_counter['ERROR_MISSING_DEP']}")
        logger.info(f"  错误: {results_counter['ERROR'] + results_counter['FATAL_ERROR']}")
        logger.info(f"  actions 新增合计：dividends={total_new_dividends} splits={total_new_splits}")
        logger.info("----------------------")

    except Exception as e:
        logger.critical(f"脚本执行过程中遇到未处理的严重错误: {e}", exc_info=True)
    finally:
        if db_manager:
            db_manager.close()
        logger.info(f"🏁 脚本执行完毕。总耗时: {timedelta(seconds=time.monotonic() - start_time)}")


if __name__ == "__main__":
    main()
