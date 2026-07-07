"""研究脚本进度框架（2026-07-07，owner 要求：所有评测/计算脚本必须带进度）。

设计目标：零依赖、tail -f 友好（逐行 flush）、每步带耗时与 RSS 内存——
OOM 风险在被内核击杀前就在日志里看得见（253 只有 11G，6G 峰值即危险区）。
RSS 打双读数：当前值（Linux /proc/self/status 的 VmRSS，看得见 price_cache
清退是否生效、离死多远）+ 峰值（ru_maxrss，只涨不跌）；macOS 无 /proc，
当前值退化为峰值（本地开发容忍，生产 253 是 Linux）。

接入模板（新研究脚本照抄，evaluate / price_cache / data.load_adjusted_panel 已内置）：

    from research.progress import Progress

    prog = Progress("ta_zoo_eval", total=8, warn_gb=5.0)
    try:
        with prog.stage("载入面板"):
            panels = load(...)
        for i, name in enumerate(names, 1):
            with prog.stage(f"因子 {name}", item=i):
                ...
        # 高频循环（如 n_sims 模拟）不要逐次打行——journald 有 burst
        # rate-limit 会静默丢行，抽样打：
        #     if i % max(1, n // 20) == 0:
        #         prog.log(f"sim {i}/{n}", item=i)
    finally:
        prog.done()  # try/finally 保证异常路径也有收尾行（模板层解决，不进框架）

输出示例（stderr，不污染 stdout 数据流）：
    [ta_zoo_eval 00:12] (1/8) 因子 obv_slope ... | rss 3.1G now / 3.1G peak
    [ta_zoo_eval 00:20] (1/8) 因子 obv_slope ok 8.2s | rss 3.4G now / 3.6G peak | eta ~00:57

非目标（动手前读我——每条都有事故或判决背书，2026-07-07 对抗审查裁定）：
- 不进 DB / pipeline_task_runs：research/ 只读是铁律；进度是易逝数据，
  唯一消费者是 tail -f 的人，DB 写入引入连接/事务/失败处理三层新故障面。
- 不做 JSON/结构化输出：事后分析耗时趋势一年查不了一次，届时
  grep 'ok .*s | rss' 足够。
- 不做嵌套 stage 树：树要传 parent 引用污染函数签名；253 上研究作业串行跑，
  每屏最多两个前缀，人眼可分。
- 不引 tqdm/rich：tqdm 在 nohup/journald 下退化成 \r 刷屏或静默；rich 违反零依赖。
- 不做 TUI/Web dashboard/Prometheus：单人单机，常驻服务是新的要运维的东西。
- 主动通知不在 Python 层解决：用 scripts/run_research.sh（systemd-run 固定
  unit 名 + OnFailure 挂 notify_failure.sh），SIGKILL/OOM 下 Python 反正打不了行。
- 不借进度之名建 StudyRun harness：6 个研究脚本异构（ClickHouse 分钟面板/
  排名面板各走各路），骨架先例 massive_task 成立的前提是 15 个同构脚本。
- 台账写了没人读是本仓实证（load_trials 曾长期零调用方）——新增任何记录
  文件前先找到第一个消费者。
"""
from __future__ import annotations

import resource
import sys
import time
from contextlib import contextmanager

_PROC_STATUS = "/proc/self/status"


def _rss_peak_gb() -> float:
    peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    # Linux 返回 KB，macOS 返回 bytes
    return peak / 1024 ** 2 if sys.platform.startswith("linux") else peak / 1024 ** 3


def _parse_vmrss_kb(text: str) -> int | None:
    for line in text.splitlines():
        if line.startswith("VmRSS:"):
            return int(line.split()[1])
    return None


def _rss_now_gb() -> float:
    """当前 RSS（GB）。无 /proc（macOS）时退化为峰值——now==peak，可容忍。"""
    try:
        with open(_PROC_STATUS) as fh:
            kb = _parse_vmrss_kb(fh.read())
        if kb is not None:
            return kb / 1024 ** 2
    except OSError:
        pass
    return _rss_peak_gb()


def _fmt_clock(seconds: float) -> str:
    s = int(seconds)
    if s >= 3600:
        return f"{s // 3600}:{s % 3600 // 60:02d}:{s % 60:02d}"
    return f"{s // 60:02d}:{s % 60:02d}"


def _one_line(exc: BaseException, limit: int = 120) -> str:
    return " ".join(f"{type(exc).__name__}: {exc}".split())[:limit]


class Progress:
    def __init__(self, name: str, total: int | None = None, *, out=None,
                 warn_gb: float | None = 5.0):
        self.name = name
        self.total = total
        self._t0 = time.monotonic()
        self._out = out or sys.stderr
        self._warn_gb = warn_gb
        self._stages: list[tuple[str, float]] = []   # (label, 耗时) 供 done() 打 top3
        self._item_durations: list[float] = []       # 带 item 的 stage 耗时，供 ETA

    def _rss_suffix(self) -> str:
        now = _rss_now_gb()
        text = f"rss {now:.1f}G now / {_rss_peak_gb():.1f}G peak"
        if self._warn_gb is not None and now >= self._warn_gb:
            text += " ⚠MEM"
        return text

    def _emit(self, text: str) -> None:
        prefix = f"[{self.name} {_fmt_clock(time.monotonic() - self._t0)}]"
        print(f"{prefix} {text}", file=self._out, flush=True)

    def _counter(self, item: int | None) -> str:
        return f"({item}/{self.total}) " if item is not None and self.total else ""

    def log(self, text: str, item: int | None = None) -> None:
        self._emit(f"{self._counter(item)}{text}")

    @contextmanager
    def stage(self, label: str, item: int | None = None):
        counter = self._counter(item)
        # 进入行带当前 RSS：SIGKILL（OOM）时 tail -f 的最后一行就是死前水位
        self._emit(f"{counter}{label} ... | {self._rss_suffix()}")
        t = time.monotonic()
        try:
            yield
        except BaseException as exc:
            dt = time.monotonic() - t
            self._stages.append((label, dt))
            self._emit(f"{counter}{label} FAILED {_one_line(exc)} {dt:.1f}s | {self._rss_suffix()}")
            raise
        dt = time.monotonic() - t
        self._stages.append((label, dt))
        eta = ""
        if item is not None and self.total:
            self._item_durations.append(dt)
            avg = sum(self._item_durations) / len(self._item_durations)
            eta = f" | eta ~{_fmt_clock(max(0, self.total - item) * avg)}"
        self._emit(f"{counter}{label} ok {dt:.1f}s | {self._rss_suffix()}{eta}")

    def done(self) -> None:
        self._emit(f"完成 | 总耗时 {int(time.monotonic() - self._t0)}s | rss 峰值 {_rss_peak_gb():.1f}G")
        if self._stages:
            top = sorted(self._stages, key=lambda x: x[1], reverse=True)[:3]
            self._emit("top: " + ", ".join(f"{label} {dt:.1f}s" for label, dt in top))
