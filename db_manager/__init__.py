"""数据库写入层。

原先的单文件 db_manager.py 按领域拆分为：
- core.py: 引擎/会话生命周期、序列同步、通用批量 upsert；
- securities.py: securities 表与 watermark 时间戳；
- corporate_actions.py: 分红/拆股与复权因子 reference/cache；
- market_data.py: 日线、股本/流通盘、空头数据；
- reference_data.py: 身份映射、symbol 历史、SEC filing/XBRL、新闻。

对外入口保持 `from db_manager import DatabaseManager` 不变。
"""
from .helpers import (
    ACTION_SOURCE_MASSIVE,
    _build_upsert_statement,
    _clean_for_model,
    _format_action_decimal,
    _group_rows_by_key_set,
    _normalize_batch_rows,
)
from .manager import DatabaseManager

__all__ = [
    "ACTION_SOURCE_MASSIVE",
    "DatabaseManager",
]
