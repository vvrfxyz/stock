"""FTD CUSIP 映射的解析、symbol 匹配与回填语义测试。"""
from datetime import date

from data_sources.sec_ftd_source import ftd_periods, parse_ftd_pairs
from scripts.sync_cusip_identifiers import build_symbol_maps, resolve_cusip_map

FTD_TEXT = """SETTLEMENT DATE|CUSIP|SYMBOL|QUANTITY (FAILS)|DESCRIPTION|PRICE
20260501|B9151N105|TTAM|17688|TITAN AMER SA COM|16.49
20260501|084670702|BRKB|6943|BERKSHIRE HATHWY INC(HLDG CO)B|473.60
20260504|084670702|BRKB|40697|BERKSHIRE HATHWY INC(HLDG CO)B|470.10
20260505|037833100|AAPL|100|APPLE INC|200.00
20260505|BADCUSIP|XXXX|1|BAD ROW|1.00
20260505|123456789||5|NO SYMBOL|1.00
"""


class TestParseFtdPairs:
    def test_pairs_deduped_and_invalid_skipped(self):
        pairs = parse_ftd_pairs(FTD_TEXT)
        assert pairs == {
            ("B9151N105", "ttam"),
            ("084670702", "brkb"),
            ("037833100", "aapl"),
        }


class TestFtdPeriods:
    def test_starts_from_previous_month_newest_first(self):
        periods = ftd_periods(2, today=date(2026, 6, 12))
        assert periods == [("202605", "b"), ("202605", "a"), ("202604", "b"), ("202604", "a")]

    def test_year_rollover(self):
        periods = ftd_periods(2, today=date(2026, 1, 15))
        assert periods[0][0] == "202512"
        assert periods[2][0] == "202511"


class _Sec:
    def __init__(self, id, symbol):
        self.id = id
        self.symbol = symbol


class TestSymbolMaps:
    def test_dotless_fallback_for_share_classes(self):
        exact, dotless = build_symbol_maps([_Sec(1, "brk.b"), _Sec(2, "aapl")])
        assert exact == {"brk.b": 1, "aapl": 2}
        assert dotless == {"brkb": 1}

    def test_dotless_collision_with_exact_symbol_dropped(self):
        # 库内既有 brkb 又有 brk.b：去点形式撞精确键，FTD 的 BRKB 只能信精确匹配
        exact, dotless = build_symbol_maps([_Sec(1, "brk.b"), _Sec(2, "brkb")])
        assert "brkb" not in dotless
        assert exact["brkb"] == 2

    def test_dotless_collision_between_dotted_symbols_dropped(self):
        exact, dotless = build_symbol_maps([_Sec(1, "ab.c"), _Sec(2, "a.bc")])
        assert dotless == {}


class TestResolveCusipMap:
    def test_resolution_and_ambiguity(self):
        exact = {"aapl": 1, "msft": 2}
        pairs = {
            ("037833100", "aapl"),
            ("594918104", "msft"),
            ("594918104", "aapl"),  # 同 CUSIP 指向两个 security -> 歧义
            ("000000000", "zzzz"),  # 未匹配 symbol
        }
        resolved, unmatched, ambiguous = resolve_cusip_map(pairs, exact, {})
        assert resolved == {"037833100": 1}
        assert unmatched == 1
        assert ambiguous == 1

    def test_same_cusip_same_security_via_both_paths_not_ambiguous(self):
        exact = {"brk.b": 1}
        dotless = {"brkb": 1}
        pairs = {("084670702", "brkb"), ("084670702", "brk.b")}
        resolved, unmatched, ambiguous = resolve_cusip_map(pairs, exact, dotless)
        assert resolved == {"084670702": 1}
        assert ambiguous == 0
