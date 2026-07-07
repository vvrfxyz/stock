"""研究脚本进度框架（2026-07-07，owner 要求：所有评测/计算脚本必须带进度）。

设计目标：零依赖、tail -f 友好（逐行 flush）、每步带耗时与 RSS 内存——
OOM 风险在被内核击杀前就在日志里看得见（253 只有 11G，6G 峰值即危险区）。

用法（框架化——evaluate 等共享入口已内置，新研究脚本照抄）：

    from research.progress import Progress

    prog = Progress("ta_zoo_eval", total=8)
    with prog.stage("载入面板"):
        panels = load(...)
    for i, name in enumerate(names, 1):
        with prog.stage(f"因子 {name}", item=i):
            ...
    prog.done()

输出示例（stderr，不污染 stdout 数据流）：
    [ta_zoo_eval 00:12] (1/8) 因子 obv_slope ... ok 8.2s | rss 3.1G
"""
from __future__ import annotations

import resource
import sys
import time
from contextlib import contextmanager


def _rss_gb() -> float:
    peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    # Linux 返回 KB，macOS 返回 bytes
    return peak / 1024 ** 2 if sys.platform.startswith("linux") else peak / 1024 ** 3


class Progress:
    def __init__(self, name: str, total: int | None = None, *, out=None):
        self.name = name
        self.total = total
        self._t0 = time.monotonic()
        self._out = out or sys.stderr

    def _emit(self, text: str) -> None:
        elapsed = int(time.monotonic() - self._t0)
        prefix = f"[{self.name} {elapsed // 60:02d}:{elapsed % 60:02d}]"
        print(f"{prefix} {text}", file=self._out, flush=True)

    def log(self, text: str, item: int | None = None) -> None:
        counter = f"({item}/{self.total}) " if item and self.total else ""
        self._emit(f"{counter}{text}")

    @contextmanager
    def stage(self, label: str, item: int | None = None):
        counter = f"({item}/{self.total}) " if item and self.total else ""
        self._emit(f"{counter}{label} ...")
        t = time.monotonic()
        try:
            yield
        except Exception:
            self._emit(f"{counter}{label} FAILED {time.monotonic() - t:.1f}s | rss {_rss_gb():.1f}G")
            raise
        self._emit(f"{counter}{label} ok {time.monotonic() - t:.1f}s | rss {_rss_gb():.1f}G")

    def done(self) -> None:
        self._emit(f"完成 | 总耗时 {int(time.monotonic() - self._t0)}s | rss 峰值 {_rss_gb():.1f}G")
