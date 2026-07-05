"""sync_delisted_universe 纯逻辑单元测试：名单去重 + 既有行匹配分类。"""
from datetime import date
from types import SimpleNamespace

from scripts.sync_delisted_universe import _dedupe_entries, classify_entries


def _row(id, symbol, *, is_active=False, list_date=None, delist_date=None,
         cik=None, composite_figi=None, share_class_figi=None, name=None, exchange=None):
    return SimpleNamespace(id=id, symbol=symbol, is_active=is_active, list_date=list_date,
                           delist_date=delist_date, cik=cik, composite_figi=composite_figi,
                           share_class_figi=share_class_figi, name=name, exchange=exchange)


def _entry(symbol, delist, **kw):
    payload = {"symbol": symbol, "delist_date": delist, "cik": None, "composite_figi": None,
               "share_class_figi": None, "name": None, "exchange": None}
    payload.update(kw)
    return payload


class TestDedupeEntries:
    def test_same_figi_keeps_latest_delist(self):
        # 同一公司退市-复牌-再退市：一只证券一行，保留最后退市日
        a = {"ticker": "X", "composite_figi": "BBG1", "delisted_utc": "2010-01-05T05:00:00Z"}
        b = {"ticker": "X", "composite_figi": "BBG1", "delisted_utc": "2016-08-09T04:00:00Z"}
        kept = _dedupe_entries([a, b])
        assert len(kept) == 1 and kept[0]["delisted_utc"].startswith("2016")

    def test_ticker_only_entries_stay_separate(self):
        # 无 FIGI/CIK 的同 ticker 不同退市日：可能是两家公司，各留一行
        a = {"ticker": "Y", "delisted_utc": "2008-01-05T05:00:00Z"}
        b = {"ticker": "Y", "delisted_utc": "2019-08-09T04:00:00Z"}
        assert len(_dedupe_entries([a, b])) == 2


class TestClassifyEntries:
    def test_unmatched_entry_inserted(self):
        to_insert, to_fill, stats = classify_entries(
            [_entry("gone", date(2012, 3, 9))], [_row(1, "other")])
        assert len(to_insert) == 1 and not to_fill
        assert stats["new_delisted"] == 1

    def test_figi_match_fills_only_null_fields(self):
        existing = [_row(5, "dead", delist_date=None, cik=None,
                         composite_figi="BBG9", name="Old Name Inc")]
        entry = _entry("dead", date(2015, 6, 1), composite_figi="BBG9",
                       cik="0001234567", name="Vendor Name")
        to_insert, to_fill, stats = classify_entries([entry], existing)
        assert not to_insert and stats["matched_filled"] == 1
        sec_id, fills = to_fill[0]
        assert sec_id == 5
        assert fills["delist_date"] == date(2015, 6, 1) and fills["cik"] == "0001234567"
        assert "name" not in fills and "composite_figi" not in fills  # 已有值绝不覆盖

    def test_figi_match_to_active_row_is_rename_ghost(self):
        # FB→META 后 vendor 把 FB 记 delisted、FIGI 同 META 活跃行：跳过不动
        existing = [_row(9, "meta", is_active=True, composite_figi="BBGMETA")]
        to_insert, to_fill, stats = classify_entries(
            [_entry("fb", date(2022, 6, 9), composite_figi="BBGMETA")], existing)
        assert not to_insert and not to_fill
        assert stats["skipped_active_ghost"] == 1

    def test_symbol_fallback_respects_delist_tolerance(self):
        existing = [_row(7, "reuse", delist_date=date(2008, 5, 9))]
        near = _entry("reuse", date(2008, 5, 20))    # 11 天内 → 同一实体
        far = _entry("reuse", date(2019, 2, 1))      # 差 10 年 → 前任死代码的新主
        to_insert, to_fill, stats = classify_entries([near, far], existing)
        assert stats["matched_noop"] == 1            # near 命中但无可补字段
        assert stats["new_delisted"] == 1 and to_insert[0]["delist_date"] == date(2019, 2, 1)

    def test_symbol_fallback_ignores_active_rows(self):
        # 活跃同名行不参与 symbol 兜底（车牌已回收给现役公司）
        existing = [_row(11, "recyc", is_active=True, list_date=date(2021, 9, 1))]
        to_insert, _, stats = classify_entries([_entry("recyc", date(2015, 3, 20))], existing)
        assert stats["new_delisted"] == 1 and len(to_insert) == 1

    def test_two_entries_cannot_fill_same_row_twice(self):
        existing = [_row(13, "dup", delist_date=None)]
        entries = [_entry("dup", date(2010, 1, 5)), _entry("dup", date(2010, 1, 20))]
        to_insert, to_fill, stats = classify_entries(entries, existing)
        assert len(to_fill) == 1 and stats["dup_match_same_row"] == 1 and not to_insert
