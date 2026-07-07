"""research/progress.py 的单元测试——框架此前裸奔无测试，2026-07-07 补上。

不打真 stderr：所有用例注入 io.StringIO 断言输出行；RSS 读数 monkeypatch
伪造，macOS/Linux 分支都在纯函数层测。
"""
from __future__ import annotations

import io

import pytest

from research import progress as progress_mod
from research.progress import Progress, _fmt_clock, _one_line, _parse_vmrss_kb


# ---------- 纯函数 ----------

def test_parse_vmrss_kb():
    text = "Name:\tpython\nVmPeak:\t 6291456 kB\nVmRSS:\t 3145728 kB\nThreads: 8\n"
    assert _parse_vmrss_kb(text) == 3145728


def test_parse_vmrss_kb_missing():
    assert _parse_vmrss_kb("Name:\tpython\nThreads: 8\n") is None


def test_rss_now_falls_back_to_peak_without_proc(monkeypatch):
    # macOS 分支：/proc 不存在 -> 退化为峰值
    monkeypatch.setattr(progress_mod, "_PROC_STATUS", "/nonexistent/status")
    monkeypatch.setattr(progress_mod, "_rss_peak_gb", lambda: 4.25)
    assert progress_mod._rss_now_gb() == 4.25


def test_rss_now_reads_proc(tmp_path, monkeypatch):
    status = tmp_path / "status"
    status.write_text("VmRSS:\t 2097152 kB\n")
    monkeypatch.setattr(progress_mod, "_PROC_STATUS", str(status))
    assert progress_mod._rss_now_gb() == pytest.approx(2.0)


def test_fmt_clock():
    assert _fmt_clock(0) == "00:00"
    assert _fmt_clock(75) == "01:15"
    assert _fmt_clock(3600) == "1:00:00"
    assert _fmt_clock(3725) == "1:02:05"


def test_one_line_truncates_and_flattens():
    exc = ValueError("boom\nsecond line " + "x" * 300)
    text = _one_line(exc)
    assert text.startswith("ValueError: boom second line")
    assert "\n" not in text
    assert len(text) <= 120


# ---------- Progress 行为 ----------

@pytest.fixture
def rss(monkeypatch):
    monkeypatch.setattr(progress_mod, "_rss_now_gb", lambda: 3.1)
    monkeypatch.setattr(progress_mod, "_rss_peak_gb", lambda: 5.8)


def _run(prog_kwargs, body):
    out = io.StringIO()
    prog = Progress("t", out=out, **prog_kwargs)
    body(prog)
    return out.getvalue().splitlines()


def test_stage_ok_lines(rss):
    def body(prog):
        with prog.stage("载入", item=1):
            pass
        prog.done()

    lines = _run({"total": 2}, body)
    assert "(1/2) 载入 ... | rss 3.1G now / 5.8G peak" in lines[0]
    assert "(1/2) 载入 ok" in lines[1]
    assert "rss 3.1G now / 5.8G peak" in lines[1]
    assert "eta ~" in lines[1]           # 有 total+item 时 ok 行带 ETA
    assert "完成" in lines[2]
    assert lines[3].split("] ")[1].startswith("top: 载入")


def test_item_zero_not_swallowed(rss):
    # 旧实现 `if item and self.total` 会把 item=0 吞掉
    def body(prog):
        prog.log("x", item=0)

    lines = _run({"total": 5}, body)
    assert "(0/5) x" in lines[0]


def test_stage_failed_line_carries_exception(rss):
    def body(prog):
        with pytest.raises(ValueError):
            with prog.stage("pivot"):
                raise ValueError("bad frame")

    lines = _run({}, body)
    assert "pivot FAILED ValueError: bad frame" in lines[1]
    assert "rss 3.1G now" in lines[1]


def test_mem_warning_marker(monkeypatch):
    monkeypatch.setattr(progress_mod, "_rss_now_gb", lambda: 5.5)
    monkeypatch.setattr(progress_mod, "_rss_peak_gb", lambda: 5.5)
    out = io.StringIO()
    prog = Progress("t", out=out, warn_gb=5.0)
    with prog.stage("s"):
        pass
    assert "⚠MEM" in out.getvalue()

    out2 = io.StringIO()
    prog2 = Progress("t", out=out2, warn_gb=None)  # 可关
    with prog2.stage("s"):
        pass
    assert "⚠MEM" not in out2.getvalue()


def test_done_top3_sorted(rss, monkeypatch):
    clock = {"t": 0.0}
    monkeypatch.setattr(progress_mod.time, "monotonic", lambda: clock["t"])
    out = io.StringIO()
    prog = Progress("t", out=out)
    for label, dt in [("a", 5.0), ("b", 1.0), ("c", 9.0), ("d", 0.5)]:
        with prog.stage(label):
            clock["t"] += dt
    prog.done()
    top_line = out.getvalue().splitlines()[-1]
    # c(9s) > a(5s) > b(1s)，d(0.5s) 挤出 top3
    assert "top: c 9.0s, a 5.0s, b 1.0s" in top_line
