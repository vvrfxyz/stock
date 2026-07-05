"""import_day_aggs 纯逻辑单元测试：任期构建 + (ticker, date) 映射 + 后缀过滤。"""
from collections import Counter
from datetime import date
from types import SimpleNamespace

from scripts.import_day_aggs import FAR_FUTURE, OPEN_START_FLOOR, build_tenures, resolve_file_map


def _sec(id, symbol, *, list_date, delist_date=None, is_active=True, max_bar=None):
    return SimpleNamespace(id=id, symbol=symbol, list_date=list_date,
                           delist_date=delist_date, is_active=is_active, max_bar=max_bar)


def _hist(security_id, symbol, start, end=None):
    return SimpleNamespace(security_id=security_id, symbol=symbol,
                           start_date=start, end_date=end)


def _map(tenures, tickers, day):
    stats, unmapped = Counter(), Counter()
    result = resolve_file_map(tickers, day, tenures, stats, unmapped)
    return result, stats, unmapped


class TestBuildTenures:
    def test_plain_active_security_spans_list_date_to_future(self):
        tenures, skipped = build_tenures([_sec(1, "aapl", list_date=date(1980, 12, 12))], [])
        assert skipped == 0
        assert tenures["aapl"] == [(1, date(1980, 12, 12), FAR_FUTURE)]

    def test_null_list_date_excluded(self):
        tenures, skipped = build_tenures([_sec(1, "aapl", list_date=None)], [])
        assert skipped == 1 and tenures == {}

    def test_delisted_backfill_open_start_uses_floor(self):
        # 退市补录行（list_date NULL + delist_date）：无前任时起点取地板
        secs = [_sec(50, "dead1", list_date=None, delist_date=date(2011, 6, 3), is_active=False)]
        tenures, skipped = build_tenures(secs, [])
        assert skipped == 0
        assert tenures["dead1"] == [(50, OPEN_START_FLOOR, date(2011, 6, 4))]

    def test_delisted_backfill_inactive_without_delist_still_excluded(self):
        secs = [_sec(51, "dead2", list_date=None, is_active=False)]
        tenures, skipped = build_tenures(secs, [])
        assert skipped == 1 and tenures == {}

    def test_dead_chain_two_holders_split_at_prior_delist(self):
        # 同 symbol 两只退市补录：后任起点 = 前任终点（链式推断）
        secs = [
            _sec(60, "reuse", list_date=None, delist_date=date(2008, 5, 9), is_active=False),
            _sec(61, "reuse", list_date=None, delist_date=date(2019, 2, 1), is_active=False),
        ]
        tenures, _ = build_tenures(secs, [])
        segs = sorted(tenures["reuse"])
        assert segs[0] == (60, OPEN_START_FLOOR, date(2008, 5, 10))
        assert segs[1] == (61, date(2008, 5, 10), date(2019, 2, 2))

    def test_dead_chain_clipped_by_active_recycler(self):
        # 死代码被现役公司回收（有 list_date）：开口段终点被现役起点截断
        secs = [
            _sec(70, "recyc", list_date=None, delist_date=date(2015, 3, 20), is_active=False),
            _sec(71, "recyc", list_date=date(2021, 9, 1)),
        ]
        tenures, _ = build_tenures(secs, [])
        segs = sorted(tenures["recyc"])
        assert segs[0] == (70, OPEN_START_FLOOR, date(2015, 3, 21))  # 2021 起点在终点之后，不截
        assert segs[1] == (71, date(2021, 9, 1), FAR_FUTURE)
        # 反向：现役起点早于死者终点 → 死者段被截到现役起点，重叠日归现役
        secs2 = [
            _sec(72, "clash", list_date=None, delist_date=date(2015, 3, 20), is_active=False),
            _sec(73, "clash", list_date=date(2010, 1, 4)),
        ]
        tenures2, _ = build_tenures(secs2, [])
        segs2 = sorted(tenures2["clash"])
        assert segs2[0] == (72, OPEN_START_FLOOR, date(2010, 1, 4))
        assert segs2[1] == (73, date(2010, 1, 4), FAR_FUTURE)

    def test_dead_chain_fully_covered_open_segment_dropped(self):
        # 前任终点晚于本段终点：零长度开口段直接丢弃，不产生负区间
        secs = [
            _sec(80, "cover", list_date=date(2004, 1, 2), delist_date=date(2018, 7, 6), is_active=False),
            _sec(81, "cover", list_date=None, delist_date=date(2018, 7, 6), is_active=False),
        ]
        tenures, _ = build_tenures(secs, [])
        assert tenures["cover"] == [(80, date(2004, 1, 2), date(2018, 7, 7))]

    def test_rename_timeline_splits_tenures(self):
        # FB(2012-05-18) -> META(2022-06-09)：老代码任期止于改名日（半开）
        secs = [_sec(5, "meta", list_date=date(2012, 5, 18))]
        hist = [_hist(5, "fb", date(2012, 5, 18)), _hist(5, "meta", date(2022, 6, 9))]
        tenures, _ = build_tenures(secs, hist)
        assert tenures["fb"] == [(5, date(2012, 5, 18), date(2022, 6, 9))]
        assert tenures["meta"] == [(5, date(2022, 6, 9), FAR_FUTURE)]

    def test_recycled_symbol_two_securities_disjoint(self):
        # 2026-07 回收修复后的形态：老 Golden Ocean 有显式 end_date（闭区间），
        # 新 ETF 从 2026-06-26 起；两段不相交。
        secs = [
            _sec(1419, "gogl", list_date=date(1997, 2, 7), delist_date=date(2025, 8, 20), is_active=False),
            _sec(245113, "gogl", list_date=date(2026, 6, 26)),
        ]
        hist = [_hist(1419, "gogl", date(1997, 2, 7), end=date(2025, 8, 19)),
                _hist(245113, "gogl", date(2026, 6, 26))]
        tenures, _ = build_tenures(secs, hist)
        segs = sorted(tenures["gogl"])
        assert segs[0] == (1419, date(1997, 2, 7), date(2025, 8, 20))  # 显式闭区间 +1 天
        assert segs[1] == (245113, date(2026, 6, 26), FAR_FUTURE)

    def test_inactive_without_delist_uses_max_bar_inclusive(self):
        secs = [_sec(7, "dead", list_date=date(2010, 1, 4), is_active=False, max_bar=date(2020, 3, 31))]
        tenures, _ = build_tenures(secs, [])
        assert tenures["dead"] == [(7, date(2010, 1, 4), date(2020, 4, 1))]  # 末日当天可挂

    def test_history_start_before_list_date_clipped(self):
        secs = [_sec(9, "xyz", list_date=date(2015, 6, 1))]
        hist = [_hist(9, "xyz", date(2014, 1, 1))]  # 事件早于上市日：裁剪
        tenures, _ = build_tenures(secs, hist)
        assert tenures["xyz"] == [(9, date(2015, 6, 1), FAR_FUTURE)]

    def test_duplicate_history_rows_same_security_dedup_at_mapping(self):
        # MASSIVE + MANUAL 双来源写了同一任期：映射层用 set 去重不误判 ambiguous
        secs = [_sec(3, "abc", list_date=date(2020, 1, 2))]
        hist = [_hist(3, "abc", date(2020, 1, 2)), _hist(3, "abc", date(2020, 1, 2))]
        tenures, _ = build_tenures(secs, hist)
        result, stats, _ = _map(tenures, ["ABC"], date(2021, 5, 3))
        assert result == {"ABC": 3}
        assert stats["ambiguous"] == 0


class TestResolveFileMap:
    def _tenures(self):
        secs = [
            _sec(5, "meta", list_date=date(2012, 5, 18)),
            _sec(11, "aap", list_date=date(2001, 11, 29)),  # Advance Auto Parts
        ]
        hist = [_hist(5, "fb", date(2012, 5, 18)), _hist(5, "meta", date(2022, 6, 9))]
        tenures, _ = build_tenures(secs, hist)
        return tenures

    def test_date_aware_rename_mapping(self):
        tenures = self._tenures()
        result, stats, _ = _map(tenures, ["FB"], date(2013, 6, 3))
        assert result == {"FB": 5}
        result2, stats2, _ = _map(tenures, ["FB"], date(2023, 1, 3))
        assert result2 == {} and stats2["unmapped_out_of_tenure"] == 1  # 改名后 FB 已死

    def test_preferred_suffix_never_collides_with_real_ticker(self):
        # "AAp"（优先股）绝不能 lowercase 成 aap 挂到 Advance Auto Parts 上
        tenures = self._tenures()
        result, stats, _ = _map(tenures, ["AAp", "AAP"], date(2013, 6, 3))
        assert result == {"AAP": 11}
        assert stats["skipped_suffix_class"] == 1

    def test_dot_class_and_unknown(self):
        secs = [_sec(21, "brk.a", list_date=date(1990, 1, 2))]
        tenures, _ = build_tenures(secs, [])
        result, stats, unmapped = _map(tenures, ["BRK.A", "ZZZZ"], date(2005, 7, 1))
        assert result == {"BRK.A": 21}
        assert stats["unmapped_no_symbol"] == 1 and unmapped["ZZZZ"] == 1

    def test_pre_listing_bar_rejected(self):
        # 回收残留防护：现任持有者上市日前的 bar 属于旧主，不许挂
        secs = [_sec(30, "newco", list_date=date(2019, 9, 10))]
        tenures, _ = build_tenures(secs, [])
        result, stats, _ = _map(tenures, ["NEWCO"], date(2010, 4, 15))
        assert result == {} and stats["unmapped_out_of_tenure"] == 1

    def test_genuinely_ambiguous_overlap_skipped(self):
        # 两只证券的任期意外重叠（脏数据）：宁可跳过也不误挂
        secs = [_sec(1, "dup", list_date=date(2010, 1, 4)),
                _sec(2, "dup", list_date=date(2012, 1, 4))]
        tenures, _ = build_tenures(secs, [])
        result, stats, unmapped = _map(tenures, ["DUP"], date(2013, 3, 1))
        assert result == {}
        assert stats["ambiguous"] == 1 and unmapped["DUP(AMBIG)"] == 1
