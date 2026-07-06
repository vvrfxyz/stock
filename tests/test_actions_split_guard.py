"""update_massive_actions 同日冲突拆股守卫（镜像归档导入 R9/R10）的行为锁定。

TSM 实测：vendor 会对同一 (ticker, 执行日) 双发比例互斥的拆股事件。
live 路径必须与 import_corporate_actions_archive.sift_splits 同语义：
- 同日比例位级一致的精确重复只保留最小 source_event_id 一条；
- 同日比例互斥（不同 (split_from, split_to) 规范形式）全组隔离不落库，响亮告警。
"""
from collections import Counter
from datetime import date
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from loguru import logger as loguru_logger

import scripts.update_massive_actions as actions

END_DATE = date(2026, 6, 11)


def _security(**extra):
    defaults = dict(
        id=1,
        symbol="tsm",
        currency="USD",
        exchange="XNYS",
        price_data_latest_date=None,
        info_last_updated_at=None,
        actions_last_updated_at=None,
        list_date=None,
        is_active=True,
        delist_date=None,
    )
    defaults.update(extra)
    return SimpleNamespace(**defaults)


def _split(event_id, ex_date, split_from, split_to):
    return {
        "execution_date": ex_date,
        "split_from": split_from,
        "split_to": split_to,
        "source_event_id": event_id,
        "adjustment_type": None,
        "historical_adjustment_factor": None,
    }


@pytest.fixture(autouse=True)
def quarantine_tsv(tmp_path, monkeypatch):
    """隔离 TSV 重定向到临时目录，测试不污染仓库 logs/。"""
    path = tmp_path / "split_conflict_quarantine.tsv"
    monkeypatch.setattr(actions, "SPLIT_QUARANTINE_TSV", str(path))
    return path


@pytest.fixture
def warnings_log():
    messages: list[str] = []
    handler_id = loguru_logger.add(
        lambda message: messages.append(str(message)), level="WARNING"
    )
    yield messages
    loguru_logger.remove(handler_id)


class TestSiftSameDaySplits:
    def test_same_day_same_ratio_duplicate_keeps_min_event_id(self):
        counter = Counter()
        kept = actions._sift_same_day_splits(
            _security(),
            [
                _split("E2", date(2025, 6, 10), Decimal("1"), Decimal("4")),
                _split("E1", date(2025, 6, 10), Decimal("1"), Decimal("4")),
            ],
            counter,
        )
        assert [item["source_event_id"] for item in kept] == ["E1"]
        assert counter["SPLIT_DUPLICATE_DROPPED"] == 1
        assert counter["SPLIT_CONFLICT_QUARANTINED"] == 0

    def test_equivalent_numeric_representations_are_not_conflict(self):
        # 15 与 15.0000 规范化后同形（归档 _fmt 口径），是精确重复而非冲突。
        counter = Counter()
        kept = actions._sift_same_day_splits(
            _security(),
            [
                _split("E1", date(2025, 6, 10), Decimal("15.0000"), Decimal("1.0000")),
                _split("E2", date(2025, 6, 10), 15, 1),
            ],
            counter,
        )
        assert [item["source_event_id"] for item in kept] == ["E1"]
        assert counter["SPLIT_CONFLICT_QUARANTINED"] == 0

    def test_same_day_conflicting_ratios_quarantine_whole_group(self, warnings_log):
        counter = Counter()
        kept = actions._sift_same_day_splits(
            _security(),
            [
                _split("E1", date(2025, 6, 10), Decimal("1"), Decimal("4")),
                _split("E2", date(2025, 6, 10), Decimal("4"), Decimal("1")),
            ],
            counter,
        )
        assert kept == []  # 不猜哪条对：全组隔离
        assert counter["SPLIT_CONFLICT_QUARANTINED"] == 2
        assert any("tsm" in msg and "人工裁决" in msg for msg in warnings_log)
        assert any("E1" in msg and "E2" in msg for msg in warnings_log)

    def test_conflict_writes_durable_quarantine_tsv(self, quarantine_tsv):
        # 归档路径有 quarantine_detail.tsv 人工裁决队列；live 路径必须有对等持久工件，
        # 否则隔离只存在于滚动日志里，真实拆股缺失会静默悬置。
        counter = Counter()
        actions._sift_same_day_splits(
            _security(id=7, symbol="tsm"),
            [
                _split("E1", date(2025, 6, 10), Decimal("1"), Decimal("4")),
                _split("E2", date(2025, 6, 10), Decimal("4"), Decimal("1")),
            ],
            counter,
        )
        lines = quarantine_tsv.read_text(encoding="utf-8").splitlines()
        assert lines[0].split("\t") == [
            "recorded_at_utc", "security_id", "symbol", "ex_date",
            "source_event_id", "split_from", "split_to",
        ]
        body = [line.split("\t") for line in lines[1:]]
        assert [(r[1], r[2], r[3], r[4]) for r in body] == [
            ("7", "tsm", "2025-06-10", "E1"),
            ("7", "tsm", "2025-06-10", "E2"),
        ]
        # 追加写：第二次冲突不覆盖已有裁决队列
        actions._sift_same_day_splits(
            _security(id=8, symbol="aaa"),
            [
                _split("E3", date(2025, 6, 11), Decimal("1"), Decimal("2")),
                _split("E4", date(2025, 6, 11), Decimal("2"), Decimal("1")),
            ],
            Counter(),
        )
        assert len(quarantine_tsv.read_text(encoding="utf-8").splitlines()) == 5

    def test_no_conflict_writes_no_tsv(self, quarantine_tsv):
        actions._sift_same_day_splits(
            _security(),
            [
                _split("E2", date(2025, 6, 10), Decimal("1"), Decimal("4")),
                _split("E1", date(2025, 6, 10), Decimal("1"), Decimal("4")),
            ],
            Counter(),
        )
        assert not quarantine_tsv.exists()

    def test_pair_identity_is_the_conflict_criterion(self):
        # 与归档 R10 同语义：冲突判据是 (from, to) 规范形式对，不做比值折算——
        # 4:2 与 2:1 视为冲突，交人工裁决（保守面永远优先于聪明面）。
        counter = Counter()
        kept = actions._sift_same_day_splits(
            _security(),
            [
                _split("E1", date(2025, 6, 10), Decimal("2"), Decimal("1")),
                _split("E2", date(2025, 6, 10), Decimal("4"), Decimal("2")),
            ],
            counter,
        )
        assert kept == []
        assert counter["SPLIT_CONFLICT_QUARANTINED"] == 2

    def test_conflict_on_one_day_does_not_block_other_days(self):
        counter = Counter()
        kept = actions._sift_same_day_splits(
            _security(),
            [
                _split("E1", date(2025, 6, 10), Decimal("1"), Decimal("4")),
                _split("E2", date(2025, 6, 10), Decimal("4"), Decimal("1")),
                _split("E3", date(2024, 3, 5), Decimal("1"), Decimal("2")),
            ],
            counter,
        )
        assert [item["source_event_id"] for item in kept] == ["E3"]
        assert counter["SPLIT_CONFLICT_QUARANTINED"] == 2

    def test_rows_without_execution_date_pass_through(self):
        # 无执行日的行维持现状（upsert_splits 自行丢弃），不参与同日分组。
        counter = Counter()
        no_date = _split("E9", None, Decimal("1"), Decimal("2"))
        kept = actions._sift_same_day_splits(
            _security(),
            [no_date, _split("E1", date(2025, 6, 10), Decimal("1"), Decimal("4"))],
            counter,
        )
        assert {item["source_event_id"] for item in kept} == {"E1", "E9"}
        assert counter["SPLIT_CONFLICT_QUARANTINED"] == 0


class TestRunLevelGuard:
    """经 run(args, source, db) 全链路验证守卫接线（打桩风格同 test_script_runs）。"""

    def _run(self, monkeypatch, splits):
        sec = _security()
        monkeypatch.setattr(actions, "get_last_completed_trading_date", lambda market: END_DATE)
        monkeypatch.setattr(actions, "get_securities_to_update", lambda db, args: [sec])
        source, db = Mock(), Mock()
        source.get_dividends_batch.return_value = []
        source.get_splits_batch.return_value = splits
        db.upsert_dividends.return_value = 0
        db.upsert_splits.return_value = 1
        db.upsert_vendor_adjustment_factors.return_value = 0
        result = actions.run(actions.create_parser().parse_args([]), source, db)
        return result, db

    def test_same_ratio_duplicate_written_once(self, monkeypatch):
        (exit_code, stats), db = self._run(monkeypatch, [
            {"ticker": "tsm", **_split("E2", date(2025, 6, 10), Decimal("1"), Decimal("4"))},
            {"ticker": "tsm", **_split("E1", date(2025, 6, 10), Decimal("1"), Decimal("4"))},
        ])
        assert exit_code == 0
        written = db.upsert_splits.call_args.args[1]
        assert [item["source_event_id"] for item in written] == ["E1"]
        assert stats["split_conflicts_quarantined"] == 0

    def test_conflicting_ratios_neither_written_and_surfaced(self, monkeypatch, warnings_log):
        (exit_code, stats), db = self._run(monkeypatch, [
            {"ticker": "tsm", **_split("E1", date(2025, 6, 10), Decimal("1"), Decimal("4"))},
            {"ticker": "tsm", **_split("E2", date(2025, 6, 10), Decimal("4"), Decimal("1"))},
        ])
        assert exit_code == 0  # 隔离是数据裁决事项，与归档一致不当作运行错误
        db.upsert_splits.assert_not_called()
        # 被隔离的拆股也不得写 vendor 因子行
        assert db.upsert_vendor_adjustment_factors.call_args.args[0] == []
        assert stats["split_conflicts_quarantined"] == 2
        assert any("人工裁决" in msg for msg in warnings_log)

    def test_quarantine_does_not_block_same_security_dividends(self, monkeypatch):
        sec = _security()
        monkeypatch.setattr(actions, "get_last_completed_trading_date", lambda market: END_DATE)
        monkeypatch.setattr(actions, "get_securities_to_update", lambda db, args: [sec])
        source, db = Mock(), Mock()
        source.get_dividends_batch.return_value = [
            {"ticker": "tsm", "ex_dividend_date": date(2025, 6, 12), "cash_amount": "0.50",
             "currency": "USD", "source_event_id": "D1", "historical_adjustment_factor": None},
        ]
        source.get_splits_batch.return_value = [
            {"ticker": "tsm", **_split("E1", date(2025, 6, 10), Decimal("1"), Decimal("4"))},
            {"ticker": "tsm", **_split("E2", date(2025, 6, 10), Decimal("4"), Decimal("1"))},
        ]
        db.upsert_dividends.return_value = 1
        db.upsert_splits.return_value = 0
        db.upsert_vendor_adjustment_factors.return_value = 0

        result = actions.run(actions.create_parser().parse_args([]), source, db)
        assert result[0] == 0
        dividends = db.upsert_dividends.call_args.args[1]
        assert [item["source_event_id"] for item in dividends] == ["D1"]
        db.upsert_splits.assert_not_called()
        db.update_security_timestamp.assert_called_once_with(1, "actions_last_updated_at")
