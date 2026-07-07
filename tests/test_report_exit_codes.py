"""health_report / audit_security_identity 退出码语义锁定（2026-07-07）。

此前"仅 advisory 告警"（health_report 的 P1-only、audit 的需人工甄别不阻塞）
也以 exit=1 结束，scheduled_update 把这两步每天记成 pipeline_task_runs FAILED、
systemd OnFailure 假告警。现语义：

- 硬问题（health_report P0>0 / audit 阻塞迁移的活跃行重复）→ exit=1；
- advisory-only → exit=0，且汇总在日志显著位置以 WARNING 输出（报告内容不变）。

改动这两个脚本的退出码逻辑必须过这组测试。
"""
from loguru import logger

from scripts.audit_security_identity import resolve_exit_code as audit_exit_code
from scripts.health_report import summarize as health_summarize


def _capture_logs():
    messages: list[str] = []
    handler_id = logger.add(messages.append, format="{level}|{message}", level="INFO")
    return messages, handler_id


# ---------------------------------------------------------------------------
# health_report.summarize
# ---------------------------------------------------------------------------

def test_health_report_p0_exits_nonzero():
    assert health_summarize(1, 0, 0) == 1
    # P0 与 P1 并存时以 P0 为准
    assert health_summarize(2, 5, 3) == 1


def test_health_report_p1_only_exits_zero_with_prominent_summary():
    messages, handler_id = _capture_logs()
    try:
        assert health_summarize(0, 4, 0) == 0
    finally:
        logger.remove(handler_id)
    joined = "\n".join(messages)
    # P1 汇总必须以 WARNING 级别显著输出，且报告的分层计数保留
    assert "WARNING|" in joined
    assert "P1 告警汇总: 4 项" in joined
    assert "P1 WARNING  : 4 项" in joined


def test_health_report_all_green_exits_zero():
    assert health_summarize(0, 0, 0) == 0


# ---------------------------------------------------------------------------
# audit_security_identity.resolve_exit_code
# ---------------------------------------------------------------------------

def test_audit_blocking_exits_nonzero():
    assert audit_exit_code(1, 0) == 1
    assert audit_exit_code(2, 7) == 1


def test_audit_advisory_only_exits_zero_with_warning():
    messages, handler_id = _capture_logs()
    try:
        assert audit_exit_code(0, 3) == 0
    finally:
        logger.remove(handler_id)
    joined = "\n".join(messages)
    assert "WARNING|" in joined
    assert "需人工甄别的存量身份异常: 3 组" in joined


def test_audit_clean_exits_zero():
    assert audit_exit_code(0, 0) == 0
