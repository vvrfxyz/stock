import unittest
from contextlib import contextmanager
from datetime import date
from types import SimpleNamespace

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from data_models.models import CorporateAction, HistoricalShare, Security
from scripts.update_massive_shares import (
    _attach_float_fields,
    _extract_total_shares,
    _needs_split_refresh,
    get_securities_to_process,
)


class MassiveSharesTests(unittest.TestCase):
    def test_extract_total_shares_prefers_share_class_over_weighted(self):
        overview = {
            "share_class_shares_outstanding": 314112458,
            "weighted_shares_outstanding": 333752708,
        }
        self.assertEqual(_extract_total_shares(overview), 314112458)

    def test_extract_total_shares_falls_back_to_weighted(self):
        overview = {
            "share_class_shares_outstanding": None,
            "weighted_shares_outstanding": 333752708,
        }
        self.assertEqual(_extract_total_shares(overview), 333752708)


class AttachFloatFieldsTests(unittest.TestCase):
    def test_uses_latest_float_effective_on_or_before_filing_date(self):
        rows = [
            {"security_id": 1, "filing_date": date(2025, 6, 30), "float_shares": None, "free_float_percent": None},
        ]
        floats_by_symbol = {
            "aapl": [
                {"effective_date": date(2025, 3, 1), "free_float": 100, "free_float_percent": 50},
                {"effective_date": date(2025, 6, 1), "free_float": 120, "free_float_percent": 60},
                {"effective_date": date(2025, 9, 1), "free_float": 140, "free_float_percent": 70},
            ]
        }

        matched = _attach_float_fields(rows, floats_by_symbol, {1: "aapl"})

        self.assertEqual(matched, 1)
        self.assertEqual(rows[0]["float_shares"], 120)
        self.assertEqual(rows[0]["free_float_percent"], 60)

    def test_does_not_backfill_future_float_into_historical_snapshot(self):
        rows = [
            {"security_id": 1, "filing_date": date(2024, 12, 31), "float_shares": None, "free_float_percent": None},
        ]
        floats_by_symbol = {
            "aapl": [
                {"effective_date": date(2025, 9, 1), "free_float": 140, "free_float_percent": 70},
            ]
        }

        matched = _attach_float_fields(rows, floats_by_symbol, {1: "aapl"})

        self.assertEqual(matched, 0)
        self.assertIsNone(rows[0]["float_shares"])
        self.assertIsNone(rows[0]["free_float_percent"])


class NeedsSplitRefreshTests(unittest.TestCase):
    END = date(2026, 7, 10)

    def test_no_snapshot_or_no_split_does_not_trigger(self):
        self.assertFalse(_needs_split_refresh(None, date(2026, 7, 5), self.END))
        self.assertFalse(_needs_split_refresh(date(2026, 7, 2), None, self.END))

    def test_split_after_snapshot_and_effective_triggers(self):
        self.assertTrue(_needs_split_refresh(date(2026, 7, 2), date(2026, 7, 8), self.END))
        # ex_date 恰为 end_date：已生效，触发
        self.assertTrue(_needs_split_refresh(date(2026, 7, 2), self.END, self.END))

    def test_split_on_or_before_snapshot_does_not_trigger(self):
        self.assertFalse(_needs_split_refresh(date(2026, 7, 8), date(2026, 7, 8), self.END))
        self.assertFalse(_needs_split_refresh(date(2026, 7, 8), date(2026, 7, 2), self.END))

    def test_future_split_does_not_trigger(self):
        self.assertFalse(_needs_split_refresh(date(2026, 7, 2), date(2026, 7, 20), self.END))


class FakeDbManager:
    def __init__(self):
        self.engine = create_engine("sqlite:///:memory:")
        # 只建三张表：全 metadata 里有 sqlite 不支持的 ARRAY 列
        Security.__table__.create(self.engine)
        HistoricalShare.__table__.create(self.engine)
        CorporateAction.__table__.create(self.engine)
        self._factory = sessionmaker(bind=self.engine)

    @contextmanager
    def get_session(self):
        session = self._factory()
        try:
            yield session
        finally:
            session.close()


def _args(symbols=(), market="US", limit=0, full_refresh=False):
    return SimpleNamespace(symbols=list(symbols), market=market, limit=limit, full_refresh=full_refresh)


class GetSecuritiesToProcessTests(unittest.TestCase):
    END_DATE = date(2026, 7, 10)  # 当季起点 2026-07-01

    def setUp(self):
        self.db = FakeDbManager()
        securities = [
            # id, symbol, active
            (1, "fresh", True),        # 本季已有快照、无拆股 -> 跳过（既有行为）
            (2, "lastq", True),        # 快照落在上季度 -> 选中（既有行为）
            (3, "splitafter", True),   # 本季快照后有已生效 SPLIT -> 强制刷新
            (4, "splitbefore", True),  # SPLIT 在快照之前 -> 跳过
            (5, "splitfuture", True),  # SPLIT ex_date 未到 end_date -> 跳过
            (6, "inactivesplit", False),  # 拆股分支不越过 is_active 守卫
            (7, "nosnap", True),       # 无任何快照 -> 选中（既有行为）
            (8, "multisplit", True),   # 多次 SPLIT 取最近一次与快照比较
        ]
        snapshots = [
            (1, date(2026, 7, 2)),
            (2, date(2026, 5, 15)),
            (3, date(2026, 7, 2)),
            (4, date(2026, 7, 8)),
            (5, date(2026, 7, 2)),
            (6, date(2026, 7, 2)),
            (8, date(2026, 7, 5)),
        ]
        splits = [
            (3, date(2026, 7, 8)),
            (4, date(2026, 7, 2)),
            (5, date(2026, 7, 20)),
            (6, date(2026, 7, 5)),
            (8, date(2026, 7, 2)),
            (8, date(2026, 7, 8)),
        ]
        with self.db.get_session() as session:
            for id_, symbol, active in securities:
                session.add(Security(
                    id=id_, symbol=symbol, current_symbol=symbol, market="US",
                    type="CS", is_active=active, full_refresh_interval=30,
                ))
            for index, (security_id, filing_date) in enumerate(snapshots, start=1):
                # sqlite 不给 BIGINT 主键自增，显式赋 id
                session.add(HistoricalShare(
                    id=index, security_id=security_id, filing_date=filing_date,
                    period_end_date=filing_date, total_shares=1_000_000, source="MASSIVE",
                ))
            for index, (security_id, ex_date) in enumerate(splits, start=1):
                session.add(CorporateAction(
                    id=index, security_id=security_id, action_type="SPLIT", ex_date=ex_date,
                    split_from=1, split_to=10, source="MASSIVE",
                    source_event_id=f"split-{index}",
                ))
            session.commit()

    def _selected_symbols(self, args):
        return [s.symbol for s in get_securities_to_process(self.db, args, self.END_DATE)]

    def test_incremental_forces_refresh_after_effective_split(self):
        selected = self._selected_symbols(_args())
        self.assertIn("splitafter", selected)
        self.assertIn("multisplit", selected)

    def test_incremental_keeps_existing_skip_semantics(self):
        selected = self._selected_symbols(_args())
        self.assertEqual(sorted(selected), ["lastq", "multisplit", "nosnap", "splitafter"])

    def test_full_refresh_selects_everything(self):
        selected = self._selected_symbols(_args(full_refresh=True))
        self.assertEqual(len(selected), 8)

    def test_explicit_symbols_bypass_split_branch(self):
        selected = self._selected_symbols(_args(symbols=["fresh"]))
        self.assertEqual(selected, ["fresh"])


if __name__ == "__main__":
    unittest.main()
