"""Massive 采集脚本的公共骨架。

收敛 update_massive_* 脚本共有的四块样板：
- build_standard_parser: symbols/--all/--market/--limit/--workers 标准参数；
- run_massive_task: 日志、限流器/数据源/数据库的构建与释放、顶层异常兜底、耗时统计；
- select_us_securities: US + CS/ETF 范围内按时间戳新鲜度选择证券；
- run_concurrently: 线程池 + 进度条 + 未捕获异常计入 FATAL_ERROR。

各脚本只保留差异部分：自有参数、选择过滤条件、process_* 业务逻辑与统计输出。
"""
import argparse
import sys
import time
from collections import Counter, deque
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from typing import Callable, Sequence

from loguru import logger
from sqlalchemy import func, or_
from tqdm import tqdm

from data_models.models import Security
from data_sources.massive_source import MassiveSource
from db_manager import DatabaseManager
from utils.key_rate_limiter import KeyRateLimiter
from utils.key_rate_limiter import waited_seconds as key_rate_limiter_waited_seconds
from utils.massive_config import (
    ALLOWED_US_SECURITY_TYPES,
    MASSIVE_RATE_LIMIT,
    MASSIVE_RATE_SECONDS,
    enforce_us_market,
    get_massive_api_keys,
)
from utils.script_logging import setup_logging


class TaskResult(int):
    """int 退出码 + 可选 stats 附件。

    对 `raise SystemExit(main())` 和 `== 0` 判断保持纯 int 语义；
    调度层（main.execute_script）通过 .stats 拿到统计并写入 pipeline_task_runs。
    """

    stats: dict | None

    def __new__(cls, exit_code: int, stats: dict | None = None) -> "TaskResult":
        obj = super().__new__(cls, exit_code)
        obj.stats = stats
        return obj


def build_standard_parser(
    description: str,
    *,
    default_workers: int,
    with_all: bool = True,
    all_help: str = "处理全部活跃保留类型证券。",
) -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=description,
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument("symbols", nargs="*", help="要处理的股票代码列表。")
    if with_all:
        parser.add_argument("--all", action="store_true", help=all_help)
    parser.add_argument("--market", type=str, default="US", help="当前仅支持 US。")
    parser.add_argument("--limit", type=int, default=0, help="限制处理数量。")
    parser.add_argument("--workers", type=int, default=default_workers, help="并发线程数。")
    return parser


def select_us_securities(
    db_manager: DatabaseManager,
    args: argparse.Namespace,
    *,
    type_scope: str = "always",
    active_scope: str = "always",
    staleness_column: str | None = None,
    staleness_days: int | None = None,
    skip_staleness: bool = False,
    extra_filter: Callable | None = None,
    order_column: str | None = None,
) -> list[Security]:
    """按市场/类型/活跃状态 + 可选新鲜度间隔选择证券。

    type_scope / active_scope:
    - "always": 无条件应用该过滤；
    - "unless_symbols": 显式传 symbols 时跳过（允许指名操作不在默认 universe 内的证券）。
    """
    has_symbols = bool(args.symbols)
    with db_manager.get_session() as session:
        query = session.query(Security).filter(
            func.upper(Security.market) == enforce_us_market(args.market)
        )
        if type_scope == "always" or not has_symbols:
            query = query.filter(func.upper(Security.type).in_(ALLOWED_US_SECURITY_TYPES))
        if active_scope == "always" or not has_symbols:
            query = query.filter(Security.is_active == True)
        if has_symbols:
            query = query.filter(Security.symbol.in_([item.lower() for item in args.symbols]))

        if staleness_column and staleness_days is not None and not skip_staleness:
            column = getattr(Security, staleness_column)
            update_before = datetime.now(timezone.utc) - timedelta(days=staleness_days)
            query = query.filter(or_(column.is_(None), column < update_before))

        if extra_filter is not None:
            query = extra_filter(query)

        order_name = order_column or staleness_column
        if order_name:
            query = query.order_by(
                getattr(Security, order_name).asc().nulls_first(), Security.symbol.asc()
            )
        else:
            query = query.order_by(Security.symbol.asc())
        if args.limit > 0:
            query = query.limit(args.limit)
        return query.all()


def _item_label(item) -> str:
    if isinstance(item, (list, tuple)) and item:
        return f"{item[0].symbol}-{item[-1].symbol}"
    return getattr(item, "symbol", repr(item))


def _fatal_cost(item) -> int:
    return len(item) if isinstance(item, (list, tuple)) else 1


def _rss_line() -> str:
    """当前 RSS（Linux /proc VmRSS；macOS 无 /proc 退化为 ru_maxrss 峰值）。

    刻意内联而非 import research.progress：utils 层不依赖 research 层。
    """
    import resource
    import sys as _sys
    try:
        with open("/proc/self/status") as fh:
            for line in fh:
                if line.startswith("VmRSS:"):
                    return f"rss {int(line.split()[1]) / 1024 ** 2:.1f}G"
    except OSError:
        pass
    peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    gb = peak / 1024 ** 2 if _sys.platform.startswith("linux") else peak / 1024 ** 3
    return f"rss {gb:.1f}G peak"


class _LineProgress:
    """非 TTY（systemd/nohup/管道）下替代 tqdm 的节流进度行。

    tqdm 在 journald 下退化成 \\r 残迹或静默——daily run 的 cron 日志全中招。
    每 ≥interval 秒打一行（同时规避 journald burst rate-limit）：
        [desc 0:12:34] 1234/5656 ok, 3 failed | 8.2 item/s | ETA ~09:41 | rss 2.1G | rate-wait 38%
    速率用最近 5 分钟滑窗（长尾证券全量回填会拖歪全程均值）；ETA 带 ~ 明示粗估；
    rate-wait = 本进程限速累计等待 / (墙钟 × 线程数)，是配额压力下界。
    """

    WINDOW_SECONDS = 300.0

    def __init__(self, desc: str, total: int, *, workers: int, interval: float = 30.0, out=None):
        self.desc = desc
        self.total = total
        self.workers = max(1, workers)
        self.interval = interval
        self._out = out if out is not None else sys.stderr
        self._t0 = time.monotonic()
        self._last_emit = self._t0
        self._done = 0
        self._failed = 0
        self._window: deque[float] = deque()  # 最近完成时刻
        self._wait0 = key_rate_limiter_waited_seconds()

    def update(self, *, failed: bool = False) -> None:
        now = time.monotonic()
        self._done += 1
        if failed:
            self._failed += 1
        self._window.append(now)
        while self._window and now - self._window[0] > self.WINDOW_SECONDS:
            self._window.popleft()
        if now - self._last_emit >= self.interval or self._done == self.total:
            self._emit(now)
            self._last_emit = now

    def _emit(self, now: float) -> None:
        elapsed = now - self._t0
        span = min(elapsed, self.WINDOW_SECONDS)
        rate = len(self._window) / span if span > 0 else 0.0
        eta = ""
        if rate > 0 and self._done < self.total:
            r = round((self.total - self._done) / rate)
            eta_txt = f"{r // 3600}:{r % 3600 // 60:02d}:{r % 60:02d}" if r >= 3600 else f"{r // 60:02d}:{r % 60:02d}"
            eta = f" | ETA ~{eta_txt}"
        rate_wait = ""
        waited = key_rate_limiter_waited_seconds() - self._wait0
        if elapsed > 0 and waited > 0:
            pct = min(1.0, waited / (elapsed * self.workers))
            rate_wait = f" | rate-wait {pct:.0%}"
        h, rem = divmod(int(elapsed), 3600)
        failed_part = f", {self._failed} failed" if self._failed else ""
        ok = self._done - self._failed
        print(
            f"[{self.desc} {h}:{rem // 60:02d}:{rem % 60:02d}] "
            f"{ok}/{self.total} ok{failed_part} | {rate:.1f} item/s{eta}"
            f" | {_rss_line()}{rate_wait}",
            file=self._out, flush=True,
        )


def run_concurrently(
    items: Sequence,
    worker: Callable,
    *,
    max_workers: int,
    desc: str,
) -> tuple[list, Counter]:
    """并发执行 worker(item)，返回 (成功返回值列表, 计数器)。

    item 可以是单个 Security 或一批 Security；worker 抛出的未捕获异常
    按 item 内证券数量计入 FATAL_ERROR，不中断其余任务。

    进度反馈按输出端分叉：stderr 是 TTY 时保留 tqdm（交互体验不变）；
    非 TTY（systemd/nohup/管道）切换为 _LineProgress 的 30s 节流逐行输出。
    """
    counter = Counter()
    outputs = []
    interactive = sys.stderr.isatty()
    line_prog = None if interactive else _LineProgress(desc, len(items), workers=max_workers)
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_item = {executor.submit(worker, item): item for item in items}
        completed = as_completed(future_to_item)
        if interactive:
            completed = tqdm(completed, total=len(future_to_item), desc=desc)
        for future in completed:
            item = future_to_item[future]
            failed = False
            try:
                outputs.append(future.result())
            except Exception as exc:
                failed = True
                logger.opt(exception=exc).error("任务 {} 发生未捕获异常: {}", _item_label(item), exc)
                counter["FATAL_ERROR"] += _fatal_cost(item)
            if line_prog is not None:
                line_prog.update(failed=failed)
    return outputs, counter


def run_massive_task(
    task_name: str,
    argv: list[str] | None,
    parser_factory: Callable[[], argparse.ArgumentParser],
    runner: Callable[[argparse.Namespace, MassiveSource, DatabaseManager], int | tuple[int, dict] | None],
) -> int:
    """脚本 main(argv) 的统一外壳：解析参数、构建/释放运行时、兜底异常与耗时。

    runner 可以返回:
    - int: 退出码
    - (int, dict): 退出码 + 统计摘要，以 TaskResult.stats 附件返回给调度层
      （execute_script 转交 finish_task_run 写入 pipeline_task_runs 供 health_report 展示）
    - None: 视为 0
    """
    start_time = time.monotonic()
    setup_logging(task_name)
    args = parser_factory().parse_args(argv)

    db_manager = None
    source = None
    try:
        enforce_us_market(getattr(args, "market", "US"))
        api_keys = get_massive_api_keys()
        rate_limiter = KeyRateLimiter(api_keys, MASSIVE_RATE_LIMIT, MASSIVE_RATE_SECONDS, scope="massive")
        source = MassiveSource(rate_limiter=rate_limiter)
        db_manager = DatabaseManager()
        result = runner(args, source, db_manager)
        if isinstance(result, tuple):
            exit_code, stats = result
            logger.info("任务统计: {}", stats)
            return TaskResult(exit_code, stats)
        return result if isinstance(result, int) else 0
    except Exception as e:
        logger.opt(exception=e).critical("{} 执行失败: {}", task_name, e)
        return 1
    finally:
        if source:
            source.close()
        if db_manager:
            db_manager.close()
        logger.info("耗时: {}", timedelta(seconds=time.monotonic() - start_time))
