"""SEC EDGAR 配置。

SEC 公平使用政策硬性要求 User-Agent 标明身份（机构/邮箱）。
通过环境变量 SEC_USER_AGENT 配置，如：
    SEC_USER_AGENT="stock-pipeline contact@example.com"
未配置时拒绝启动，避免匿名 UA 被 SEC 封禁。
"""
from __future__ import annotations

import os


def get_sec_user_agent() -> str:
    user_agent = (os.getenv("SEC_USER_AGENT") or "").strip()
    if not user_agent:
        raise ValueError(
            "SEC_USER_AGENT 未配置。SEC 要求 User-Agent 标明身份，"
            '请在 .env 中设置，如 SEC_USER_AGENT="stock-pipeline you@example.com"。'
        )
    return user_agent
