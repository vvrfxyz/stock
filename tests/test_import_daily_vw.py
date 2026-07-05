"""import_daily_vw 测试：文件迭代 + 行规范化/任期归属（纯逻辑）+ 三指纹守卫（PG 集成）。"""
from collections import Counter
from datetime import date
from decimal import Decimal
from types import SimpleNamespace

import pytest

from scripts.import_daily_vw import iter_parquet_files, normalize_rows, stage_and_update

TENURES = {
    "aapl": [(1, date(1980, 12, 12), date(9999, 1, 1))],
    "reuse": [(10, date(2003, 1, 1), date(2010, 6, 1)), (11, date(2015, 3, 1), date(9999, 1, 1))],
}


def _frame(rows):
    import pandas as pd
    frame = pd.DataFrame(rows, columns=["date", "symbol", "vw"])
    frame["close"] = [10.0, 20.0, 15.0, 1.0, 1.0, 190.0, 1.0, 1.0][: len(frame)]
    return frame


class TestIterParquetFiles:
    def test_main_tree_ordered_and_gapfill_last(self, tmp_path):
        for rel in ["US/year=2004/month=02", "US/year=2003/month=12", "补缺/year=2021"]:
            (tmp_path / rel).mkdir(parents=True)
            (tmp_path / rel / "data.parquet").touch()
        got = [str(p.relative_to(tmp_path)) for p in iter_parquet_files(tmp_path, None)]
        assert got == ["US/year=2003/month=12/data.parquet",
                       "US/year=2004/month=02/data.parquet",
                       "补缺/year=2021/data.parquet"]

    def test_years_filter(self, tmp_path):
        for rel in ["US/year=2003/month=12", "US/year=2010/month=01", "补缺/year=2021"]:
            (tmp_path / rel).mkdir(parents=True)
            (tmp_path / rel / "data.parquet").touch()
        got = [str(p.relative_to(tmp_path)) for p in iter_parquet_files(tmp_path, (2003, 2009))]
        assert got == ["US/year=2003/month=12/data.parquet"]


class TestNormalizeRows:
    def test_tenure_routing_and_filters(self):
        frame = _frame([
            ("2005-05-05", "REUSE", 10.5),          # 老主人任期
            ("2020-05-05", "REUSE", 20.5),          # 新主人任期
            ("2012-01-05", "REUSE", 15.0),          # 空档 -> out_of_tenure
            ("2005-05-05", "GHOST", 1.0),           # 不在 universe
            ("2005-05-05", "AAp", 1.0),             # 优先股后缀
            ("2024-01-02", "AAPL", 190.0),          # 时代钳制
            ("2005-05-05", "AAPL", float("nan")),   # 无效 vw
            ("2005-05-06", "AAPL", -1.0),           # 无效 vw
        ])
        stats, unmapped = Counter(), Counter()
        rows = normalize_rows(frame, date(2024, 1, 1), TENURES, stats, unmapped)
        assert rows == [(10, date(2005, 5, 5), 10.5, 10.0), (11, date(2020, 5, 5), 20.5, 20.0)]
        assert stats["mapped"] == 2
        assert stats["unmapped_out_of_tenure"] == 1
        assert stats["unmapped_no_symbol"] == 1
        assert stats["skipped_suffix_class"] == 1
        assert stats["skipped_after_max_date"] == 1
        assert stats["skipped_bad_vw"] == 2
        assert unmapped["GHOST"] == 1

    def test_date_objects_accepted(self):
        frame = _frame([(date(2005, 5, 5), "AAPL", 33.86)])
        rows = normalize_rows(frame, date(2024, 1, 1), TENURES, Counter(), Counter())
        assert rows == [(1, date(2005, 5, 5), 33.86, 10.0)]


@pytest.mark.integration
def test_stage_and_update_fingerprint_guards(pg_db):
    """三指纹守卫：只有 flat 行拿到 vwap；yfinance 双 NULL 与 Massive 行不动。"""
    from data_models.models import DailyPrice, Security

    with pg_db.get_session() as session:
        session.add(Security(id=1, symbol="t1", current_symbol="t1", market="US",
                             type="CS", is_active=True, full_refresh_interval=30))
        session.flush()
        day = date(2014, 3, 5)
        common = dict(security_id=1, open=Decimal(1), high=Decimal(2),
                      low=Decimal(1), close=Decimal(2), volume=100)
        session.add(DailyPrice(date=day, trade_count=5, vwap=None, **common))               # flat
        session.add(DailyPrice(date=date(2014, 3, 6), trade_count=None, vwap=None, **common))   # yfinance
        session.add(DailyPrice(date=date(2014, 3, 7), trade_count=9,
                               vwap=Decimal("1.5"), **common))                              # 已有 vwap
        session.commit()

    stats = Counter()
    rows = [(1, date(2014, 3, 5), 1.44, 2.0), (1, date(2014, 3, 6), 1.55, 2.0),
            (1, date(2014, 3, 7), 1.66, 2.0), (1, date(2014, 3, 8), 1.77, 2.0),  # 3-8 无 PG 行
            (1, date(2014, 3, 9), 1.88, 9.9)]  # close 不合 -> entity_mismatch（无 PG 行，落 no_pg_row）
    stage_and_update(pg_db, rows, dry_run=False, stats=stats)

    assert stats["updatable"] == 1
    assert stats["yfinance_untouchable"] == 1
    assert stats["already_has_vwap"] == 1
    assert stats["no_pg_row"] == 2
    assert stats["rows_updated"] == 1

    with pg_db.get_session() as session:
        prices = {p.date: p for p in session.query(DailyPrice).all()}
        assert prices[date(2014, 3, 5)].vwap == Decimal("1.44")
        assert prices[date(2014, 3, 6)].vwap is None      # yfinance 双 NULL 不动
        assert prices[date(2014, 3, 7)].vwap == Decimal("1.5")  # 既有 vwap 不覆盖

    # 幂等：重跑落 already_has_vwap 桶，零更新
    stats2 = Counter()
    stage_and_update(pg_db, rows[:1], dry_run=False, stats=stats2)
    assert stats2["rows_updated"] == 0 and stats2["already_has_vwap"] == 1


@pytest.mark.integration
def test_stage_and_update_entity_mismatch_never_writes(pg_db):
    """同实体守卫：close 不等（任期归属可疑）的行绝不写 vwap，落 entity_mismatch 桶。"""
    from data_models.models import DailyPrice, Security

    with pg_db.get_session() as session:
        session.add(Security(id=3, symbol="t3", current_symbol="t3", market="US",
                             type="CS", is_active=True, full_refresh_interval=30))
        session.flush()
        session.add(DailyPrice(security_id=3, date=date(2010, 1, 4), open=Decimal(1),
                               high=Decimal(2), low=Decimal(1), close=Decimal("50.07"),
                               volume=10, trade_count=3, vwap=None))
        session.commit()

    stats = Counter()
    stage_and_update(pg_db, [(3, date(2010, 1, 4), 2.27, 2.27)], dry_run=False, stats=stats)
    assert stats["entity_mismatch"] == 1 and stats["updatable"] == 0 and stats["rows_updated"] == 0
    with pg_db.get_session() as session:
        assert session.query(DailyPrice).one().vwap is None


@pytest.mark.integration
def test_stage_and_update_dry_run_writes_nothing(pg_db):
    from data_models.models import DailyPrice, Security

    with pg_db.get_session() as session:
        session.add(Security(id=2, symbol="t2", current_symbol="t2", market="US",
                             type="CS", is_active=True, full_refresh_interval=30))
        session.flush()
        session.add(DailyPrice(security_id=2, date=date(2010, 1, 4), open=Decimal(1),
                               high=Decimal(2), low=Decimal(1), close=Decimal(2),
                               volume=10, trade_count=3, vwap=None))
        session.commit()

    stats = Counter()
    stage_and_update(pg_db, [(2, date(2010, 1, 4), 1.5, 2.0)], dry_run=True, stats=stats)
    assert stats["updatable"] == 1 and stats["rows_updated"] == 0
    with pg_db.get_session() as session:
        assert session.query(DailyPrice).one().vwap is None
