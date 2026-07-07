"""import_corporate_actions_archive 纯逻辑单元测试：清洗规则（R3/R7-R11）+ 归属 + 值冲突挂起（R13）。"""
from collections import Counter
from datetime import date, timedelta
from decimal import Decimal

import pandas as pd
import pytest

from scripts.import_corporate_actions_archive import (
    build_ticker_cutoffs,
    dedupe_dividends,
    drop_already_imported,
    effective_cutoff,
    extended_security_cutoffs,
    holdback_mismatches,
    load_dividend_rows,
    load_split_rows,
    parse_adjudicated_allowlist,
    resolve_events,
    resolve_events_allowlist,
    sift_splits,
    _window_filter,
)


def _div(id, ticker, ex, cash, currency="USD", pay=None, record=None, decl=None,
         dtype="recurring", freq=4):
    return {"id": id, "ticker": ticker, "ex_date": ex, "record_date": record,
            "pay_date": pay, "declaration_date": decl, "cash_amount": Decimal(cash),
            "currency": currency, "distribution_type": dtype, "frequency": freq}


def _spl(id, ticker, ex, split_from, split_to, adjustment_type="forward_split"):
    return {"id": id, "ticker": ticker, "ex_date": ex,
            "split_from": Decimal(split_from), "split_to": Decimal(split_to),
            "adjustment_type": adjustment_type}


class TestLoadRows:
    def test_dividend_loader_normalizes_and_drops_bad_rows(self, tmp_path):
        frame = pd.DataFrame({
            "id": ["E1", "E2", "E3", "E4"],
            "ticker": ["AAPL", "AAPL", "AAPL", "AAPL"],
            "ex_dividend_date": ["2020-08-07", None, "2021-02-05", "2021-05-07"],
            "record_date": ["2020-08-10", None, None, None],
            "pay_date": ["2020-08-13", None, None, None],
            "declaration_date": [None, None, None, None],
            "cash_amount": [0.82, 0.82, None, -1.0],
            "currency": ["USD", "USD", "USD", "USD"],
            "distribution_type": ["recurring"] * 4,
            "frequency": pd.array([4, 4, None, 4], dtype="Int32"),
            "split_adjusted_cash_amount": [0.205, None, None, None],
            "historical_adjustment_factor": [0.99, None, None, None],
        })
        path = tmp_path / "dividends.parquet"
        frame.to_parquet(path)
        stats = Counter()
        rows = load_dividend_rows(path, stats)
        # E2 缺 ex_date、E3 缺金额、E4 负金额均剔除
        assert [r["id"] for r in rows] == ["E1"]
        assert stats["dividend_bad_row"] == 3
        assert rows[0]["ex_date"] == date(2020, 8, 7)
        assert rows[0]["cash_amount"] == Decimal("0.82")
        assert rows[0]["frequency"] == 4
        # 损坏列绝不进入行数据（R12）
        assert "historical_adjustment_factor" not in rows[0]

    def test_dividend_loader_defaults_missing_currency_to_usd(self, tmp_path):
        frame = pd.DataFrame({
            "id": ["E1"], "ticker": ["AAPL"], "ex_dividend_date": ["2020-08-07"],
            "record_date": [None], "pay_date": [None], "declaration_date": [None],
            "cash_amount": [0.82], "currency": [None], "distribution_type": ["recurring"],
            "frequency": pd.array([4], dtype="Int32"),
        })
        path = tmp_path / "dividends.parquet"
        frame.to_parquet(path)
        stats = Counter()
        rows = load_dividend_rows(path, stats)
        assert rows[0]["currency"] == "USD"
        assert stats["dividend_currency_defaulted_usd"] == 1

    def test_split_loader_quarantines_spinoff_pseudo_splits(self, tmp_path):
        frame = pd.DataFrame({
            "id": ["E1", "P1"],
            "ticker": ["AAPL", "IBM"],
            "execution_date": ["2020-08-31", "2021-11-04"],
            "adjustment_type": ["forward_split", "spinoff"],
            "split_from": [1.0, 1000.0],
            "split_to": [4.0, 1046.0],
        })
        path = tmp_path / "splits.parquet"
        frame.to_parquet(path)
        stats, quarantine = Counter(), Counter()
        detail = []
        rows = load_split_rows(path, stats, quarantine, detail)
        assert [r["id"] for r in rows] == ["E1"]  # R3：P 前缀 spinoff 隔离
        assert stats["split_spinoff_quarantined"] == 1
        assert quarantine[("IBM", "spinoff_pseudo_split")] == 1


class TestDedupeDividends:
    def test_exact_duplicates_keep_lex_min_id(self):
        # CVX 式 vendor 双发：除 id 外业务字段全同
        rows = [_div("E9", "CVX", date(2004, 2, 13), "0.365", pay=date(2004, 3, 10)),
                _div("E1", "CVX", date(2004, 2, 13), "0.365", pay=date(2004, 3, 10))]
        stats = Counter()
        deduped = dedupe_dividends(rows, set(), stats)
        assert [r["id"] for r in deduped] == ["E1"]
        assert stats["dividend_exact_duplicates_dropped"] == 1

    def test_prod_existing_id_wins_over_lex_min(self):
        rows = [_div("E9", "CVX", date(2004, 2, 13), "0.365"),
                _div("E1", "CVX", date(2004, 2, 13), "0.365")]
        deduped = dedupe_dividends(rows, {"E9"}, Counter())
        assert [r["id"] for r in deduped] == ["E9"]  # R7：prod 已有 id 优先

    def test_same_day_different_amount_all_kept(self):
        # Ford 式常规+特别分红同日：金额相同但 pay_date 不同也算不同事件
        rows = [_div("E1", "F", date(2025, 2, 18), "0.15", dtype="recurring"),
                _div("E2", "F", date(2025, 2, 18), "0.18", dtype="special")]
        deduped = dedupe_dividends(rows, set(), Counter())
        assert len(deduped) == 2  # R8

    def test_dedupe_key_ignores_distribution_type_and_frequency(self):
        # R7 键只有 5 字段：type/frequency 不同不阻止去重（vendor 双发常见差异）
        a = _div("E1", "T", date(2006, 1, 6), "0.3325", pay=date(2006, 2, 1), dtype="recurring")
        b = _div("E2", "T", date(2006, 1, 6), "0.3325", pay=date(2006, 2, 1), dtype="unknown", freq=None)
        deduped = dedupe_dividends([a, b], set(), Counter())
        assert len(deduped) == 1


class TestSiftSplits:
    def test_exact_duplicate_split_keeps_one(self):
        # CVX 2004-09-13 双发 2:1——不去重会让因子除 4 而非除 2
        rows = [_spl("E2", "CVX", date(2004, 9, 13), "1", "2"),
                _spl("E1", "CVX", date(2004, 9, 13), "1", "2")]
        stats = Counter()
        kept = sift_splits(rows, set(), stats, Counter(), [])
        assert [r["id"] for r in kept] == ["E1"]
        assert stats["split_exact_duplicates_dropped"] == 1

    def test_conflicting_ratios_quarantine_whole_group(self):
        rows = [_spl("E1", "NYC", date(2020, 8, 18), "10", "1"),
                _spl("E2", "NYC", date(2020, 8, 18), "1", "10")]
        stats, quarantine = Counter(), Counter()
        detail = []
        kept = sift_splits(rows, set(), stats, quarantine, detail)
        assert kept == []  # R10
        assert stats["split_conflicting_quarantined"] == 2
        assert quarantine[("NYC", "conflicting_split")] == 2

    def test_extreme_ratio_flagged_not_dropped(self):
        # R11：真实 OTC 100:1 反向拆分必须保留
        rows = [_spl("E1", "MULN", date(2023, 12, 21), "100000", "1")]
        stats = Counter()
        kept = sift_splits(rows, set(), stats, Counter(), [])
        assert len(kept) == 1
        assert stats["split_extreme_ratio_flagged"] == 1


class TestResolveEvents:
    TENURES = {
        "aapl": [(1, date(1980, 12, 12), date(9999, 1, 1))],
        # 回收代码：老主人 2003-2010，新主人 2015 起
        "reuse": [(10, date(2003, 1, 1), date(2010, 6, 1)), (11, date(2015, 3, 1), date(9999, 1, 1))],
        # 构造出的重叠任期（映射层必须拒绝）
        "over": [(20, date(2003, 1, 1), date(2020, 1, 1)), (21, date(2019, 1, 1), date(9999, 1, 1))],
    }

    def _run(self, rows, kind="dividend"):
        stats, quarantine = Counter(), Counter()
        by_sec = resolve_events(rows, self.TENURES, stats, quarantine, kind, [])
        return by_sec, stats, quarantine

    def test_tenure_routes_recycled_symbol_by_date(self):
        rows = [_div("E1", "REUSE", date(2005, 5, 5), "0.10"),
                _div("E2", "REUSE", date(2020, 5, 5), "0.20")]
        by_sec, stats, _ = self._run(rows)
        assert [r["id"] for r in by_sec[10]] == ["E1"]
        assert [r["id"] for r in by_sec[11]] == ["E2"]
        assert stats["dividend_mapped"] == 2

    def test_out_of_tenure_and_unmapped_quarantined(self):
        rows = [_div("E1", "REUSE", date(2012, 1, 1), "0.10"),  # 两任之间的空档
                _div("E2", "GHOST", date(2012, 1, 1), "0.10")]
        by_sec, stats, quarantine = self._run(rows)
        assert not by_sec
        assert stats["dividend_out_of_tenure"] == 1
        assert stats["dividend_unmapped_no_symbol"] == 1
        assert quarantine[("REUSE", "out_of_tenure")] == 1

    def test_ambiguous_overlap_skipped(self):
        rows = [_div("E1", "OVER", date(2019, 6, 1), "0.10")]
        by_sec, stats, quarantine = self._run(rows)
        assert not by_sec
        assert stats["dividend_ambiguous"] == 1
        assert quarantine[("OVER", "ambiguous")] == 1

    def test_lowercase_suffix_ticker_skipped(self):
        rows = [_div("E1", "AAp", date(2020, 1, 2), "0.10")]  # 优先股后缀，绝不 lowercase 归属
        by_sec, stats, _ = self._run(rows)
        assert not by_sec
        assert stats["dividend_skipped_suffix_class"] == 1


class TestWindowFilter:
    def test_min_inclusive_cutoff_exclusive(self):
        rows = [_div("E1", "A", date(2002, 12, 31), "1"),
                _div("E2", "A", date(2003, 1, 1), "1"),
                _div("E3", "A", date(2024, 5, 13), "1"),
                _div("E4", "A", date(2024, 5, 14), "1")]
        stats = Counter()
        kept = _window_filter(rows, date(2003, 1, 1), date(2024, 5, 14), stats, "dividend", [])
        assert [r["id"] for r in kept] == ["E2", "E3"]
        assert stats["dividend_before_min_date"] == 1
        assert stats["dividend_at_or_after_cutoff"] == 1

    def test_cutoff_none_keeps_future(self):
        rows = [_div("E1", "A", date(2027, 1, 1), "1")]
        kept = _window_filter(rows, date(2003, 1, 1), None, Counter(), "dividend", [])
        assert len(kept) == 1


class TestPerSecurityCutoff:
    """inactive 证券逐证券上界：live actions 只选活跃证券，[cutoff, delist_date]
    两条路径都不覆盖，归档负责到 delist_date+1（exclusive，与任期上界同口径）。"""

    CUTOFF = date(2024, 5, 14)
    FAR = date(9999, 1, 1)

    def _pipeline(self, rows, tenures, securities, cutoff=CUTOFF):
        """按 main() 的接线走一遍：延长上界 -> symbol 粗筛 -> 归属层精确上界。"""
        stats, quarantine, detail = Counter(), Counter(), []
        security_cutoffs = extended_security_cutoffs(securities, cutoff) if cutoff else {}
        ticker_cutoffs = build_ticker_cutoffs(tenures, security_cutoffs)
        kept = _window_filter(rows, date(2003, 1, 1), cutoff, stats, "dividend",
                              detail, ticker_cutoffs)
        by_sec = resolve_events(kept, tenures, stats, quarantine, "dividend", detail,
                                cutoff=cutoff, security_cutoffs=security_cutoffs)
        return by_sec, stats

    def test_effective_cutoff_formula(self):
        c = self.CUTOFF
        assert effective_cutoff(c, True, None) == c
        # 活跃行残留 delist_date（脏数据）也不放宽：live 窗口神圣不可侵入
        assert effective_cutoff(c, True, date(2025, 1, 1)) == c
        # inactive 且 delist_date NULL：无可信上界，保守回退全局 cutoff
        assert effective_cutoff(c, False, None) == c
        assert effective_cutoff(c, False, date(2025, 3, 10)) == date(2025, 3, 11)
        # max 语义：上界绝不早于全局 cutoff（cutoff 前退市的证券与旧口径无差别）
        assert effective_cutoff(c, False, date(2023, 6, 1)) == c

    def test_extended_map_only_contains_bounds_beyond_cutoff(self):
        secs = [(1, True, None),                    # 活跃
                (30, False, date(2025, 3, 10)),     # cutoff 后退市：放宽
                (40, False, date(2023, 6, 1)),      # cutoff 前退市：无差异项
                (50, False, None)]                  # NULL delist：回退全局
        assert extended_security_cutoffs(secs, self.CUTOFF) == {30: date(2025, 3, 11)}

    def test_build_ticker_cutoffs_takes_max_extended_bound(self):
        tenures = {"a": [(1, date(2003, 1, 1), date(2020, 1, 1)),
                         (2, date(2020, 1, 1), date(2025, 7, 2))],
                   "b": [(3, date(2003, 1, 1), self.FAR)]}
        cutoffs = {1: date(2024, 8, 1), 2: date(2025, 7, 2)}
        assert build_ticker_cutoffs(tenures, cutoffs) == {"a": date(2025, 7, 2)}

    def test_active_security_keeps_global_cutoff(self):
        tenures = {"aapl": [(1, date(1980, 12, 12), self.FAR)]}
        rows = [_div("E1", "AAPL", date(2024, 5, 13), "0.24"),
                _div("E2", "AAPL", date(2024, 5, 14), "0.24"),
                _div("E3", "AAPL", date(2025, 2, 10), "0.25")]
        by_sec, stats = self._pipeline(rows, tenures, [(1, True, None)])
        assert [r["id"] for r in by_sec[1]] == ["E1"]
        assert stats["dividend_at_or_after_cutoff"] == 2

    def test_inactive_delisted_after_cutoff_covered_through_delist_date(self):
        # 洞的主体：cutoff 后退市的证券，[cutoff, delist_date] 现在归档负责
        delist = date(2025, 3, 10)
        tenures = {"dead": [(30, date(2010, 1, 4), delist + timedelta(days=1))]}
        rows = [_div("E1", "DEAD", date(2024, 5, 14), "0.10"),   # cutoff 当天：导入
                _div("E2", "DEAD", date(2025, 3, 10), "0.10"),   # 退市日当天：导入
                _div("E3", "DEAD", date(2025, 3, 11), "0.10"),   # delist+1 起排除
                _div("E4", "DEAD", date(2025, 6, 1), "0.10")]
        by_sec, stats = self._pipeline(rows, tenures, [(30, False, delist)])
        assert [r["id"] for r in by_sec[30]] == ["E1", "E2"]
        assert stats["dividend_mapped"] == 2
        assert stats["dividend_at_or_after_cutoff"] == 2
        assert stats["dividend_out_of_tenure"] == 0

    def test_inactive_delisted_before_cutoff_nothing_beyond_delist(self):
        delist = date(2023, 6, 1)
        tenures = {"old": [(40, date(2010, 1, 4), delist + timedelta(days=1))]}
        rows = [_div("E1", "OLD", date(2023, 6, 1), "0.10"),    # 退市日：导入
                _div("E2", "OLD", date(2023, 8, 1), "0.10"),    # 退市后 cutoff 前：任期外
                _div("E3", "OLD", date(2024, 6, 1), "0.10")]    # cutoff 后
        by_sec, stats = self._pipeline(rows, tenures, [(40, False, delist)])
        assert [r["id"] for r in by_sec[40]] == ["E1"]
        assert stats["dividend_out_of_tenure"] == 1
        assert stats["dividend_at_or_after_cutoff"] == 1

    def test_inactive_null_delist_date_falls_back_to_global_cutoff(self):
        # 无可信退市日：不放宽；任期端点是 max_bar+1 代理（2024-08-01 有 bar）
        tenures = {"zomb": [(50, date(2010, 1, 4), date(2024, 8, 2))]}
        rows = [_div("E1", "ZOMB", date(2024, 4, 1), "0.10"),
                _div("E2", "ZOMB", date(2024, 6, 3), "0.10")]   # cutoff 后、任期内：不导入
        by_sec, stats = self._pipeline(rows, tenures, [(50, False, None)])
        assert [r["id"] for r in by_sec[50]] == ["E1"]
        assert stats["dividend_at_or_after_cutoff"] == 1

    def test_recycled_symbol_active_successor_never_absorbs_live_window(self):
        # §A.5：老主人退市 2025-03-10（放行到 03-11），新主人 03-11 起活跃；
        # symbol 粗筛的延长绝不能把 live 窗口事件塞给活跃现任
        delist = date(2025, 3, 10)
        tenures = {"reuse": [(30, date(2010, 1, 4), date(2025, 3, 11)),
                             (60, date(2025, 3, 11), self.FAR)]}
        rows = [_div("E1", "REUSE", date(2025, 3, 10), "0.10"),  # 老主人退市日
                _div("E2", "REUSE", date(2025, 3, 11), "0.10"),  # 新主人首日 = live 窗口
                _div("E3", "REUSE", date(2024, 8, 1), "0.10")]   # 洞内，归老主人
        by_sec, stats = self._pipeline(rows, tenures, [(30, False, delist), (60, True, None)])
        assert sorted(r["id"] for r in by_sec[30]) == ["E1", "E3"]
        assert 60 not in by_sec
        assert stats["dividend_at_or_after_cutoff"] == 1

    def test_resolve_level_backstop_enforces_active_cutoff(self):
        # 归属层精确上界兜底：即使粗筛放行（脏数据令 symbol 上界过宽），
        # 活跃证券也绝不吸收 >= 全局 cutoff 的归档事件
        tenures = {"aapl": [(1, date(1980, 12, 12), self.FAR)]}
        stats, quarantine = Counter(), Counter()
        by_sec = resolve_events([_div("E1", "AAPL", date(2024, 6, 3), "0.25")],
                                tenures, stats, quarantine, "dividend", [],
                                cutoff=self.CUTOFF, security_cutoffs={})
        assert not by_sec
        assert stats["dividend_at_or_after_cutoff"] == 1
        assert stats["dividend_mapped"] == 0

    def test_cutoff_none_disables_all_bounds(self):
        tenures = {"dead": [(30, date(2010, 1, 4), date(2025, 3, 11))]}
        by_sec, _ = self._pipeline([_div("E1", "DEAD", date(2025, 3, 10), "0.10")],
                                   tenures, [(30, False, date(2025, 3, 10))], cutoff=None)
        assert len(by_sec[30]) == 1


class TestHoldbackMismatches:
    def test_dividend_value_conflict_held_and_security_excluded(self):
        by_sec = {5: [_div("E1", "NVD", date(2023, 12, 27), "0.50")]}
        existing = {(5, "DIVIDEND", date(2023, 12, 27)): [
            {"cash_amount": Decimal("0.55"), "currency": "USD",
             "split_from": None, "split_to": None, "source": "POLYGON",
             "source_event_id": "massive-dividend:5:2023-12-27:0.55"}]}
        stats, mismatches = Counter(), []
        kept, excluded = holdback_mismatches(by_sec, existing, "dividend", stats, mismatches)
        assert not kept
        assert excluded == {5}
        assert stats["dividend_value_mismatch_held"] == 1
        assert mismatches[0]["archive_value"] == "0.5"

    def test_bitexact_match_passes_through(self):
        by_sec = {5: [_div("E1", "KO", date(2012, 3, 13), "0.51")]}
        existing = {(5, "DIVIDEND", date(2012, 3, 13)): [
            {"cash_amount": Decimal("0.51"), "currency": "USD",
             "split_from": None, "split_to": None, "source": "POLYGON",
             "source_event_id": "massive-dividend:..."}]}
        kept, excluded = holdback_mismatches(by_sec, existing, "dividend", Counter(), [])
        assert [r["id"] for r in kept[5]] == ["E1"]
        assert not excluded

    def test_full_precision_archive_vs_10dp_prod_column_agrees(self):
        # 归档全精度 vs prod Numeric(20,10) 存量：量化到列精度后视为一致（CNI/BCE 案例）
        by_sec = {6: [_div("E1", "CNI", date(2018, 3, 7), "0.35197648332")]}
        existing = {(6, "DIVIDEND", date(2018, 3, 7)): [
            {"cash_amount": Decimal("0.3519764833"), "currency": "USD",
             "split_from": None, "split_to": None, "source": "POLYGON",
             "source_event_id": "massive-dividend:..."}]}
        stats = Counter()
        kept, excluded = holdback_mismatches(by_sec, existing, "dividend", stats, [])
        assert len(kept[6]) == 1
        assert not excluded
        assert stats["dividend_value_mismatch_held"] == 0

    def test_half_up_rounding_matches_pg_numeric(self):
        # BCE 案例：0.55906186525 第 11 位恰为 5，PG 四舍五入存 …53；银行家舍入会误判冲突
        by_sec = {7: [_div("E1", "BCE", date(2017, 12, 14), "0.55906186525")]}
        existing = {(7, "DIVIDEND", date(2017, 12, 14)): [
            {"cash_amount": Decimal("0.5590618653"), "currency": "USD",
             "split_from": None, "split_to": None, "source": "POLYGON",
             "source_event_id": "massive-dividend:..."}]}
        kept, excluded = holdback_mismatches(by_sec, existing, "dividend", Counter(), [])
        assert len(kept[7]) == 1
        assert not excluded

    def test_split_ratio_expressed_differently_still_agrees(self):
        # POLYGON 侧 2:4 与归档 1:2 是同一比例，不得挂起
        by_sec = {7: [_spl("E1", "KO", date(2012, 8, 13), "1", "2")]}
        existing = {(7, "SPLIT", date(2012, 8, 13)): [
            {"cash_amount": None, "currency": "",
             "split_from": Decimal("2"), "split_to": Decimal("4"),
             "source": "POLYGON", "source_event_id": "massive-split:..."}]}
        kept, excluded = holdback_mismatches(by_sec, existing, "split", Counter(), [])
        assert len(kept[7]) == 1
        assert not excluded

    def test_split_ratio_conflict_held(self):
        by_sec = {8: [_spl("E1", "SLG", date(2020, 12, 14), "1.04331", "1")]}
        existing = {(8, "SPLIT", date(2020, 12, 14)): [
            {"cash_amount": None, "currency": "",
             "split_from": Decimal("1"), "split_to": Decimal("1.04331"),
             "source": "POLYGON", "source_event_id": "massive-split:..."}]}
        stats, mismatches = Counter(), []
        kept, excluded = holdback_mismatches(by_sec, existing, "split", stats, mismatches)
        assert not kept
        assert excluded == {8}

    def test_no_existing_rows_imports_freely(self):
        by_sec = {9: [_div("E1", "NEW", date(2010, 1, 5), "0.25")]}
        kept, excluded = holdback_mismatches(by_sec, {}, "dividend", Counter(), [])
        assert len(kept[9]) == 1
        assert not excluded


class TestDropAlreadyImported:
    def test_existing_pair_skipped_new_rows_pass(self):
        # 结构性只插入：(sid, id) 已在 prod 的行不再送 upsert（防快照旧值冲掉 live 行）
        by_sec = {5: [_div("E1", "AAPL", date(2020, 8, 7), "0.82"),
                      _div("E2", "AAPL", date(2020, 11, 6), "0.205")]}
        stats = Counter()
        kept = drop_already_imported(by_sec, {(5, "E1")}, "dividend", stats)
        assert [r["id"] for r in kept[5]] == ["E2"]
        assert stats["dividend_skipped_existing_id"] == 1

    def test_same_id_on_other_security_not_skipped(self):
        # 同一 E-id 可合法挂两只证券（审计 28 例）：pair 判断，裸 id 命中不算
        by_sec = {5: [_div("E1", "AAPL", date(2020, 8, 7), "0.82")]}
        kept = drop_already_imported(by_sec, {(6, "E1")}, "dividend", Counter())
        assert len(kept[5]) == 1


class TestQuarantineDetail:
    def test_out_of_tenure_detail_has_date_id_value(self):
        # R6 人工恢复（如清算分红）需要 date+id+值，聚合计数不够
        detail = []
        stats, quarantine = Counter(), Counter()
        rows = [_div("E7", "REUSE", date(2012, 1, 1), "9.50")]
        resolve_events(rows, TestResolveEvents.TENURES, stats, quarantine, "dividend", detail)
        assert detail == [{"kind": "dividend", "ticker": "REUSE", "reason": "out_of_tenure",
                           "ex_date": date(2012, 1, 1), "event_id": "E7", "value": "9.5 USD"}]

    def test_unmapped_no_symbol_has_no_detail_row(self):
        # 有意收窄：不在 universe 的 ticker 无归属对象，只聚合计数
        detail = []
        resolve_events([_div("E1", "GHOST", date(2012, 1, 1), "1")],
                       TestResolveEvents.TENURES, Counter(), Counter(), "dividend", detail)
        assert detail == []

    def test_window_filter_details_before_min_date_only(self):
        detail = []
        stats = Counter()
        rows = [_div("E1", "A", date(2002, 1, 1), "1"), _div("E2", "A", date(2025, 1, 1), "1")]
        _window_filter(rows, date(2003, 1, 1), date(2024, 5, 14), stats, "dividend", detail)
        assert [d["reason"] for d in detail] == ["before_min_date"]
        assert stats["dividend_at_or_after_cutoff"] == 1


ALLOWLIST_HEADER = "event_id\tsecurity_id\tticker\tex_date\tkind\n"


class TestParseAdjudicatedAllowlist:
    def test_parses_rows(self):
        lines = [ALLOWLIST_HEADER,
                 "E1\t10\tREUSE\t2012-01-01\tdividend\n",
                 "E2\t11\tAGFY\t2024-10-08\tsplit\n"]
        allowlist = parse_adjudicated_allowlist(lines)
        assert allowlist["E1"] == {"security_id": 10, "ticker": "REUSE",
                                   "ex_date": date(2012, 1, 1), "kind": "dividend"}
        assert allowlist["E2"]["kind"] == "split"

    def test_identical_duplicate_rows_converge(self):
        lines = [ALLOWLIST_HEADER,
                 "E1\t10\tREUSE\t2012-01-01\tdividend\n",
                 "E1\t10\tREUSE\t2012-01-01\tdividend\n"]
        assert len(parse_adjudicated_allowlist(lines)) == 1

    def test_conflicting_duplicate_event_id_raises(self):
        # 多归属歧义必须回裁决层解决，导入层绝不猜
        lines = [ALLOWLIST_HEADER,
                 "E1\t10\tREUSE\t2012-01-01\tdividend\n",
                 "E1\t11\tREUSE\t2012-01-01\tdividend\n"]
        with pytest.raises(ValueError):
            parse_adjudicated_allowlist(lines)

    def test_bad_header_raises(self):
        with pytest.raises(ValueError):
            parse_adjudicated_allowlist(["event_id\tticker\n", "E1\tA\n"])

    def test_bad_kind_raises(self):
        with pytest.raises(ValueError):
            parse_adjudicated_allowlist([ALLOWLIST_HEADER, "E1\t10\tA\t2012-01-01\tmerger\n"])


class TestResolveEventsAllowlist:
    ALLOWLIST = {
        "E1": {"security_id": 10, "ticker": "REUSE", "ex_date": date(2012, 1, 1), "kind": "dividend"},
        "E5": {"security_id": 20, "ticker": "OLDCO", "ex_date": date(2015, 6, 1), "kind": "split"},
    }

    def test_bypasses_tenure_and_maps_by_allowlist_security(self):
        # E1 落在 REUSE 两任之间的空档（resolve_events 会 out_of_tenure），allowlist 直接归属
        rows = [_div("E1", "REUSE", date(2012, 1, 1), "9.50")]
        stats = Counter()
        by_sec = resolve_events_allowlist(rows, self.ALLOWLIST, "dividend", stats)
        assert [r["id"] for r in by_sec[10]] == ["E1"]
        assert stats["dividend_mapped"] == 1

    def test_rows_not_in_allowlist_skipped(self):
        rows = [_div("E9", "AAPL", date(2020, 8, 7), "0.82")]
        stats = Counter()
        by_sec = resolve_events_allowlist(rows, self.ALLOWLIST, "dividend", stats)
        assert not by_sec
        assert stats["dividend_not_in_allowlist"] == 1

    def test_kind_mismatch_counts_as_not_in_allowlist(self):
        # E5 是拆股裁决，分红行撞上同 id 不放行
        rows = [_div("E5", "OLDCO", date(2015, 6, 1), "0.10")]
        stats = Counter()
        by_sec = resolve_events_allowlist(rows, self.ALLOWLIST, "dividend", stats)
        assert not by_sec
        assert stats["dividend_not_in_allowlist"] == 1

    def test_ticker_mismatch_skipped_with_count(self):
        rows = [_div("E1", "OTHER", date(2012, 1, 1), "9.50")]
        stats = Counter()
        by_sec = resolve_events_allowlist(rows, self.ALLOWLIST, "dividend", stats)
        assert not by_sec
        assert stats["dividend_allowlist_mismatch"] == 1

    def test_ticker_match_is_case_insensitive(self):
        allowlist = {"E1": {"security_id": 10, "ticker": "REUSE",
                            "ex_date": date(2012, 1, 1), "kind": "dividend"}}
        rows = [_div("E1", "reuse", date(2012, 1, 1), "9.50")]
        by_sec = resolve_events_allowlist(rows, allowlist, "dividend", Counter())
        assert len(by_sec[10]) == 1

    def test_ex_date_mismatch_skipped_with_count(self):
        rows = [_div("E1", "REUSE", date(2012, 1, 2), "9.50")]
        stats = Counter()
        by_sec = resolve_events_allowlist(rows, self.ALLOWLIST, "dividend", stats)
        assert not by_sec
        assert stats["dividend_allowlist_mismatch"] == 1

    def test_cutoff_still_enforced(self):
        # 裁决通过也不侵入 live 窗口：逐证券精确上界与 resolve_events 同口径
        allowlist = {"E1": {"security_id": 10, "ticker": "REUSE",
                            "ex_date": date(2024, 6, 3), "kind": "dividend"}}
        rows = [_div("E1", "REUSE", date(2024, 6, 3), "0.10")]
        stats = Counter()
        by_sec = resolve_events_allowlist(rows, allowlist, "dividend", stats,
                                          cutoff=date(2024, 5, 14), security_cutoffs={})
        assert not by_sec
        assert stats["dividend_at_or_after_cutoff"] == 1

    def test_per_security_extended_cutoff_applies(self):
        allowlist = {"E1": {"security_id": 10, "ticker": "DEAD",
                            "ex_date": date(2024, 6, 3), "kind": "dividend"}}
        rows = [_div("E1", "DEAD", date(2024, 6, 3), "0.10")]
        by_sec = resolve_events_allowlist(rows, allowlist, "dividend", Counter(),
                                          cutoff=date(2024, 5, 14),
                                          security_cutoffs={10: date(2025, 3, 11)})
        assert len(by_sec[10]) == 1

    def test_accounting_keys_partition_input(self):
        # R19：allowlist 模式下 mapped + not_in_allowlist + allowlist_mismatch = 输入行数
        rows = [_div("E1", "REUSE", date(2012, 1, 1), "9.50"),
                _div("E1", "OTHER", date(2012, 1, 1), "9.50"),
                _div("E9", "AAPL", date(2020, 8, 7), "0.82")]
        stats = Counter()
        resolve_events_allowlist(rows, self.ALLOWLIST, "dividend", stats)
        total = (stats["dividend_mapped"] + stats["dividend_not_in_allowlist"]
                 + stats["dividend_allowlist_mismatch"])
        assert total == len(rows)


class TestAllowlistReportRedirect:
    """allowlist 模式的报告改道：绝不覆盖全量导入产出的裁决输入工件。"""

    def test_redirect_appends_allowlist_suffix(self):
        from pathlib import Path
        from scripts.import_corporate_actions_archive import _allowlist_report_path

        p = Path("logs/manual_backfill/corp_actions_archive_quarantine_detail.tsv")
        assert _allowlist_report_path(p) == Path(
            "logs/manual_backfill/corp_actions_archive_quarantine_detail_allowlist.tsv")

    def test_redirect_preserves_parent_and_suffix(self):
        from pathlib import Path
        from scripts.import_corporate_actions_archive import _allowlist_report_path

        p = Path("/abs/dir/report.tsv")
        out = _allowlist_report_path(p)
        assert out.parent == p.parent
        assert out.suffix == ".tsv"
        assert out != p
