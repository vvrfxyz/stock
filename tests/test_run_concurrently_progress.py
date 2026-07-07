"""run_concurrently 非 TTY 进度行 + key_rate_limiter 等待计数的单元测试（2026-07-07）。

不起真线程池慢任务：_LineProgress 直接驱动 update()，时间与限速等待 monkeypatch。
"""
from __future__ import annotations

import io

from utils import key_rate_limiter
from utils import massive_task
from utils.massive_task import _LineProgress, run_concurrently


def test_waited_seconds_accumulates(monkeypatch):
    monkeypatch.setattr(key_rate_limiter, "_WAITED_SECONDS", 0.0)
    key_rate_limiter._record_wait(1.5)
    key_rate_limiter._record_wait(0.5)
    assert key_rate_limiter.waited_seconds() == 2.0


def _make_prog(monkeypatch, *, total=10, workers=4):
    clock = {"t": 100.0}
    waited = {"s": 0.0}
    monkeypatch.setattr(massive_task.time, "monotonic", lambda: clock["t"])
    monkeypatch.setattr(massive_task, "key_rate_limiter_waited_seconds", lambda: waited["s"])
    out = io.StringIO()
    prog = _LineProgress("update_x", total, workers=workers, interval=30.0, out=out)
    return prog, clock, waited, out


def test_line_progress_throttles(monkeypatch):
    prog, clock, _, out = _make_prog(monkeypatch)
    clock["t"] += 1.0
    prog.update()                       # 1s < 30s，不打行
    assert out.getvalue() == ""
    clock["t"] += 30.0
    prog.update()                       # 超过节流阈值
    lines = out.getvalue().splitlines()
    assert len(lines) == 1
    assert "2/10 ok" in lines[0]
    assert "item/s" in lines[0]
    assert "ETA ~" in lines[0]
    assert "rss" in lines[0]


def test_line_progress_final_item_always_emits(monkeypatch):
    prog, clock, _, out = _make_prog(monkeypatch, total=2)
    clock["t"] += 1.0
    prog.update()
    clock["t"] += 1.0
    prog.update()                       # done==total 强制打行
    lines = out.getvalue().splitlines()
    assert len(lines) == 1
    assert "2/2 ok" in lines[0]
    assert "ETA" not in lines[0]        # 完成后不再打 ETA


def test_line_progress_failed_and_rate_wait(monkeypatch):
    # 构造后新增等待 40s、elapsed=20s、workers=4 -> rate-wait 40/(20*4)=50%
    prog, clock, waited, out = _make_prog(monkeypatch, total=3, workers=4)
    clock["t"] += 20.0
    waited["s"] += 40.0
    prog.update(failed=True)
    prog.update()
    prog.update()
    line = out.getvalue().splitlines()[0]
    assert "2/3 ok, 1 failed" in line   # ok 不含失败项（审查修复锁定）
    assert "rate-wait 50%" in line


def test_line_progress_eta_hour_rollover(monkeypatch):
    # 2 item/300s 滑窗、剩 98 item -> ETA 14700s = 4:05:00（超 1 小时须带小时位）
    prog, clock, _, out = _make_prog(monkeypatch, total=100)
    clock["t"] += 300.0
    prog.update()
    clock["t"] += 300.0
    prog.update()
    line = out.getvalue().splitlines()[-1]
    assert "ETA ~4:05:00" in line


def test_line_progress_no_rate_wait_when_zero(monkeypatch):
    prog, clock, _, out = _make_prog(monkeypatch, total=1)
    clock["t"] += 5.0
    prog.update()
    assert "rate-wait" not in out.getvalue()


def test_run_concurrently_non_tty_uses_line_progress(monkeypatch):
    emitted = []

    class _Spy(_LineProgress):
        def _emit(self, now):
            emitted.append((self._done, self._failed))

    monkeypatch.setattr(massive_task, "_LineProgress", _Spy)
    monkeypatch.setattr(massive_task.sys.stderr, "isatty", lambda: False)

    def worker(item):
        if item == 2:
            raise RuntimeError("boom")
        return item * 10

    outputs, counter = run_concurrently([1, 2, 3], worker, max_workers=2, desc="t")
    assert sorted(outputs) == [10, 30]
    assert counter["FATAL_ERROR"] == 1
    assert emitted[-1] == (3, 1)        # 最后一次 update 是 done==total 强制打行


def test_run_concurrently_tty_keeps_tqdm(monkeypatch):
    used = {"tqdm": False, "line": False}

    def fake_tqdm(iterable, **kwargs):
        used["tqdm"] = True
        return iterable

    class _NoLine(_LineProgress):
        def __init__(self, *a, **k):
            used["line"] = True
            super().__init__(*a, **k)

    monkeypatch.setattr(massive_task, "tqdm", fake_tqdm)
    monkeypatch.setattr(massive_task, "_LineProgress", _NoLine)
    monkeypatch.setattr(massive_task.sys.stderr, "isatty", lambda: True)
    outputs, counter = run_concurrently([1, 2], lambda x: x, max_workers=1, desc="t")
    assert sorted(outputs) == [1, 2]
    assert used == {"tqdm": True, "line": False}
