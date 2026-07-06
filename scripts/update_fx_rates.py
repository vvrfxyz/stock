"""同步每日参考汇率到 fx_rates（ECB + FRED 两源）。

用途：非 USD 分红在重建复权因子时折算成 USD（见 update_adjustment_factors）。
- ECB：EUR 基参考汇率（CAD/NOK/ILS 等），全历史 CSV 一次请求 ~700KB。
- FRED：ECB 未覆盖币种的 USD 基直连系列（DEXTAUS = 1 USD 兑 TWD，TSM 分红依赖），
  原样入库为 (base=USD, quote=TWD, source=FRED)，倒数换算只在读取层做
  （utils/fx_rates.UsdFxConverter）。
两源均幂等 upsert；FRED 需要 FRED_API_KEY（与 update_risk_free_rates 共用）。
"""
import argparse
import sys
import time
from datetime import date, timedelta
from pathlib import Path

from loguru import logger

project_root = Path(__file__).resolve().parents[1]
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from data_sources.ecb_fx_source import fetch_ecb_fx_history
from data_sources.fred_source import fetch_fred_series
from db_manager import DatabaseManager
from utils.script_logging import setup_logging as configure_script_logging

# FRED H.10 汇率系列注册表：series_id -> (base_currency, quote_currency)。
# DEXTAUS 口径为 1 USD = rate TWD；行按 vendor 口径原样存，绝不在写入侧取倒数。
FRED_FX_SERIES: dict[str, tuple[str, str]] = {
    "DEXTAUS": ("USD", "TWD"),
}


def build_fred_fx_rows(series_id: str, observations: list[dict]) -> list[dict]:
    """把 fetch_fred_series 的通用观测行映射成 fx_rates 行（rate_pct 字段即汇率值）。"""
    base_currency, quote_currency = FRED_FX_SERIES[series_id]
    return [
        {
            "rate_date": row["date"],
            "base_currency": base_currency,
            "quote_currency": quote_currency,
            "source": "FRED",
            "rate": row["rate_pct"],
        }
        for row in observations
    ]


def create_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="同步 ECB/FRED 每日参考汇率。")
    parser.add_argument("--market", type=str, default="US", help="占位参数，保持调度接口一致。")
    parser.add_argument("--since", type=str, default=None,
                        help="只写入该日期（YYYY-MM-DD）之后的汇率；不传则全历史回填。")
    parser.add_argument("--skip-fred", action="store_true",
                        help="跳过 FRED USD 基系列（无 FRED_API_KEY 的环境只同步 ECB）。")
    return parser


def _sync_ecb(db_manager: DatabaseManager, since: date | None) -> int:
    try:
        rows = fetch_ecb_fx_history(since=since)
    except Exception as e:
        logger.opt(exception=e).error("ECB 汇率分支失败: {}", e)
        return 1
    if not rows:
        logger.warning("ECB 汇率源未返回任何行（since={}）。", since)
        return 0
    written = db_manager.upsert_fx_rates(rows)
    logger.info("ECB 汇率行写入/更新: {}（解析 {} 行，since={}）。", written, len(rows), since)
    return 0


def _sync_fred_fx(db_manager: DatabaseManager, since: date | None) -> int:
    exit_code = 0
    for series_id in FRED_FX_SERIES:
        try:
            observations = fetch_fred_series(series_id, since=since)
        except ValueError as e:
            if "contained no" in str(e):
                # H.10 周度滞后发布：增量窗口可能合法为空，不视为失败。
                logger.warning("FRED {} 增量窗口无行（since={}）。", series_id, since)
            else:
                logger.opt(exception=e).error("FRED {} 响应解析失败: {}", series_id, e)
                exit_code = 1
            continue
        except Exception as e:
            logger.opt(exception=e).error("FRED {} 汇率分支失败: {}", series_id, e)
            exit_code = 1
            continue
        rows = build_fred_fx_rows(series_id, observations)
        written = db_manager.upsert_fx_rates(rows)
        logger.info("FRED {} 汇率行写入/更新: {}（解析 {} 行，since={}）。",
                    series_id, written, len(rows), since)
    return exit_code


def main(argv: list[str] | None = None) -> int:
    start_time = time.monotonic()
    configure_script_logging("update_fx_rates")
    args = create_parser().parse_args(argv)
    since = date.fromisoformat(args.since) if args.since else None

    db_manager = None
    try:
        db_manager = DatabaseManager()
        exit_code = _sync_ecb(db_manager, since)
        if not args.skip_fred:
            exit_code = max(exit_code, _sync_fred_fx(db_manager, since))
        return exit_code
    except Exception as e:
        logger.opt(exception=e).critical("update_fx_rates 执行失败: {}", e)
        return 1
    finally:
        if db_manager:
            db_manager.close()
        logger.info("耗时: {}", timedelta(seconds=time.monotonic() - start_time))


if __name__ == "__main__":
    raise SystemExit(main())
