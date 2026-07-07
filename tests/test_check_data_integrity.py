"""check_data_integrity 的全史 OHLC 不变量探针集成测试。

锁定 ratchet 语义与来源指纹分解：
- 干净数据（0 违规）通过，不计入退出码；
- 违规数超基线阻塞，返回超出量；
- 违规数等于基线只通报（返回 0）；
- 广义违规集（负成交量/非正价）也被捕获——旧 containment-only 探针会漏；
- 来源×类型分解按 yfinance/massive/flatfiles 指纹正确切分。

需要 PostgreSQL：GREATEST/LEAST/FILTER 是方言级语义，sqlite 无法替代。
"""
from datetime import date, timedelta

import pytest
from loguru import logger

from data_models.models import DailyPrice, Security
from scripts.check_data_integrity import check_ohlc_validity_full_history

pytestmark = pytest.mark.integration


def _insert_security(pg_db, security_id=1, symbol="aapl", **extra) -> int:
    row = {
        "id": security_id,
        "symbol": symbol,
        "current_symbol": symbol,
        "market": "US",
        "type": "CS",
        "is_active": True,
        "full_refresh_interval": 30,
        **extra,
    }
    with pg_db.get_session() as session:
        session.add(Security(**row))
        session.commit()
    return security_id


def _insert_prices(pg_db, security_id, rows: list[dict]) -> None:
    """rows 每个 dict 提供 open/high/low/close（可选 volume/vwap/trade_count），
    日期从 2020-01-01 顺延，避免主键冲突。"""
    base = date(2020, 1, 1)
    with pg_db.get_session() as session:
        for i, r in enumerate(rows):
            session.add(DailyPrice(
                security_id=security_id,
                date=base + timedelta(days=i),
                volume=r.get("volume", 1000),
                vwap=r.get("vwap"),
                trade_count=r.get("trade_count"),
                **{k: r[k] for k in ("open", "high", "low", "close")},
            ))
        session.commit()


def _run(pg_db, baseline: int) -> int:
    with pg_db.get_session() as session:
        return check_ohlc_validity_full_history(session, limit=20, baseline=baseline)


def _run_capturing(pg_db, baseline: int) -> tuple[int, str]:
    """同 _run，但把 loguru 输出收进字符串，便于断言来源×类型分解。"""
    messages: list[str] = []
    sink_id = logger.add(messages.append, level="DEBUG")
    try:
        result = _run(pg_db, baseline)
    finally:
        logger.remove(sink_id)
    return result, "".join(str(m) for m in messages)


# --- 干净的合法行（high 包住 open/close、low 被包住、价正、量非负） ---
_CLEAN = {"open": 10, "high": 12, "low": 9, "close": 11}
# high < open 且 high < close 的包含违规
_VIOL_HIGH = {"open": 10, "high": 9, "low": 8, "close": 12}


class TestOhlcFullHistoryRatchet:
    def test_clean_data_passes(self, pg_db):
        _insert_security(pg_db)
        _insert_prices(pg_db, 1, [
            {**_CLEAN},                                  # yfinance 指纹（双 NULL）
            {**_CLEAN, "trade_count": 100},              # flatfiles 指纹
            {**_CLEAN, "vwap": 10.5},                    # massive 指纹
        ])
        assert _run(pg_db, baseline=0) == 0

    def test_violations_above_baseline_block(self, pg_db):
        _insert_security(pg_db)
        _insert_prices(pg_db, 1, [
            {**_CLEAN},
            {**_VIOL_HIGH},
            {**_VIOL_HIGH, "open": 20, "high": 19, "low": 15, "close": 22},
            {**_VIOL_HIGH, "open": 30, "high": 29, "low": 25, "close": 32},
        ])
        # baseline 0，3 行违规 -> 返回超出量 3（阻塞）
        assert _run(pg_db, baseline=0) == 3

    def test_at_baseline_warns_only(self, pg_db):
        _insert_security(pg_db)
        _insert_prices(pg_db, 1, [
            {**_VIOL_HIGH},
            {**_VIOL_HIGH, "open": 20, "high": 19, "low": 15, "close": 22},
            {**_VIOL_HIGH, "open": 30, "high": 29, "low": 25, "close": 32},
        ])
        # 违规数恰等基线 -> 只通报，不计入退出码
        assert _run(pg_db, baseline=3) == 0

    def test_below_baseline_warns_only(self, pg_db):
        _insert_security(pg_db)
        _insert_prices(pg_db, 1, [{**_VIOL_HIGH}])
        assert _run(pg_db, baseline=5) == 0

    def test_negative_volume_is_flagged(self, pg_db):
        """负成交量：containment 全满足，旧 containment-only 探针会漏，广义集须捕获。"""
        _insert_security(pg_db)
        _insert_prices(pg_db, 1, [
            {**_CLEAN},
            {**_CLEAN, "volume": -5},
        ])
        assert _run(pg_db, baseline=0) == 1

    def test_nonpositive_price_is_flagged(self, pg_db):
        _insert_security(pg_db)
        _insert_prices(pg_db, 1, [
            {**_CLEAN},
            {"open": 10, "high": 12, "low": 0, "close": 11},   # low <= 0
        ])
        assert _run(pg_db, baseline=0) == 1

    def test_source_fingerprint_breakdown(self, pg_db):
        """来源×类型分解：2 行 yfinance + 1 行 massive 违规，按指纹正确切分。"""
        _insert_security(pg_db)
        _insert_prices(pg_db, 1, [
            {**_VIOL_HIGH},                                                  # yfinance
            {**_VIOL_HIGH, "open": 20, "high": 19, "low": 15, "close": 22},  # yfinance
            {**_VIOL_HIGH, "open": 30, "high": 29, "low": 25, "close": 32,
             "vwap": 30.0},                                                  # massive
        ])
        result, log = _run_capturing(pg_db, baseline=0)
        assert result == 3
        assert "来源=yfinance 合计=2" in log
        assert "来源=massive 合计=1" in log
