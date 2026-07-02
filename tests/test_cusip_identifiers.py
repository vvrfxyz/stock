"""FTD CUSIP 映射的解析、symbol 匹配与回填语义测试。"""
from datetime import date, datetime, timedelta, timezone

import pytest

from data_sources.sec_ftd_source import ftd_periods, parse_ftd_pairs
from scripts.sync_cusip_identifiers import (
    build_symbol_maps,
    ftd_period_start,
    load_unstable_symbols,
    resolve_cusip_map,
)

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


class TestFtdPeriodStart:
    def test_half_a_is_first_of_month(self):
        assert ftd_period_start("202605", "a") == date(2026, 5, 1)

    def test_half_b_is_sixteenth(self):
        assert ftd_period_start("202605", "b") == date(2026, 5, 16)


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

    def test_duplicate_exact_symbol_dropped_not_last_wins(self):
        # 同 symbol 多行（回收/脏数据）：与 dotless 口径一致，剔除而非静默 last-wins
        exact, dotless = build_symbol_maps([_Sec(1, "zzz"), _Sec(2, "zzz"), _Sec(3, "aapl")])
        assert "zzz" not in exact
        assert exact == {"aapl": 3}

    def test_dotless_form_of_ambiguous_exact_symbol_dropped(self):
        # 'zzz' 精确歧义被剔除后，'zz.z' 的去点形式也不得顶上（同一 FTD symbol 仍歧义）
        exact, dotless = build_symbol_maps([_Sec(1, "zzz"), _Sec(2, "zzz"), _Sec(3, "zz.z")])
        assert "zzz" not in exact
        assert "zzz" not in dotless

    def test_ambiguous_dotted_symbol_excluded_from_dotless(self):
        # 含点 symbol 本身精确歧义时，其去点形式同样不可信
        exact, dotless = build_symbol_maps([_Sec(1, "br.k"), _Sec(2, "br.k")])
        assert exact == {}
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
        resolved, start_dates, unmatched, ambiguous, skipped = resolve_cusip_map(pairs, exact, {})
        assert resolved == {"037833100": 1}
        assert start_dates == {}
        assert unmatched == 1
        assert ambiguous == 1
        assert skipped == 0

    def test_same_cusip_same_security_via_both_paths_not_ambiguous(self):
        exact = {"brk.b": 1}
        dotless = {"brkb": 1}
        pairs = {("084670702", "brkb"), ("084670702", "brk.b")}
        resolved, _, unmatched, ambiguous, skipped = resolve_cusip_map(pairs, exact, dotless)
        assert resolved == {"084670702": 1}
        assert ambiguous == 0

    def test_stale_symbol_of_renamed_security_stays_unmatched(self):
        # 陈旧 symbol：证券已改名（旧 symbol 不在活跃快照里），历史 FTD 观测不得错配
        exact = {"newco": 7}  # 改名后的当前 symbol
        pairs = {("111111111", "oldco")}  # 数月前 FTD 里的旧 symbol
        resolved, _, unmatched, ambiguous, skipped = resolve_cusip_map(pairs, exact, {})
        assert resolved == {}
        assert unmatched == 1

    def test_recycled_symbol_in_unstable_window_skipped(self):
        # 回收隔离期：symbol 有 RECYCLE/QUARANTINE 事件，虽然仍能匹配到旧行，也必须跳过
        exact = {"abc": 1, "aapl": 2}
        pairs = {("222222222", "abc"), ("037833100", "aapl")}
        resolved, _, unmatched, ambiguous, skipped = resolve_cusip_map(
            pairs, exact, {}, unstable_symbols={"abc"},
        )
        assert resolved == {"037833100": 2}
        assert skipped == 1
        assert unmatched == 0

    def test_unstable_dotless_form_also_skipped(self):
        # 身份事件记录的是库内形式 brk.b；FTD 的去点形式 brkb 同样要挡住
        exact = {"brk.b": 1}
        dotless = {"brkb": 1}
        pairs = {("084670702", "brkb")}
        resolved, _, _, _, skipped = resolve_cusip_map(
            pairs, exact, dotless, unstable_symbols={"brk.b", "brkb"},
        )
        assert resolved == {}
        assert skipped == 1

    def test_start_dates_take_earliest_matched_observation(self):
        exact = {"aapl": 1}
        pairs = {("037833100", "aapl")}
        first_seen = {("037833100", "aapl"): date(2026, 4, 16)}
        resolved, start_dates, *_ = resolve_cusip_map(
            pairs, exact, {}, pair_first_seen=first_seen,
        )
        assert resolved == {"037833100": 1}
        assert start_dates == {"037833100": date(2026, 4, 16)}

    def test_start_dates_min_across_matched_pairs_only(self):
        # 同 CUSIP 经两条路径匹配到同一 security：取参与匹配的最早覆盖期；
        # 未匹配 symbol 的观测日期不参与
        exact = {"brk.b": 1}
        dotless = {"brkb": 1}
        pairs = {("084670702", "brkb"), ("084670702", "brk.b"), ("084670702", "ghost")}
        first_seen = {
            ("084670702", "brkb"): date(2026, 5, 1),
            ("084670702", "brk.b"): date(2026, 4, 1),
            ("084670702", "ghost"): date(2026, 3, 1),  # 未匹配，不得拉低 start_date
        }
        resolved, start_dates, unmatched, *_ = resolve_cusip_map(
            pairs, exact, dotless, pair_first_seen=first_seen,
        )
        assert resolved == {"084670702": 1}
        assert start_dates == {"084670702": date(2026, 4, 1)}
        assert unmatched == 1

    def test_unstable_pair_does_not_contribute_start_date(self):
        # 身份不稳的观测被跳过后，其覆盖期也不得进入 start_date
        exact = {"aapl": 1}
        pairs = {("037833100", "aapl"), ("037833100", "abc")}
        first_seen = {
            ("037833100", "aapl"): date(2026, 5, 16),
            ("037833100", "abc"): date(2026, 3, 1),
        }
        resolved, start_dates, *_ = resolve_cusip_map(
            pairs, {**exact, "abc": 1}, {}, unstable_symbols={"abc"},
            pair_first_seen=first_seen,
        )
        assert resolved == {"037833100": 1}
        assert start_dates == {"037833100": date(2026, 5, 16)}


@pytest.mark.integration
class TestLoadUnstableSymbols:
    def _seed(self, pg_db, event_type, old_symbol, new_symbol, created_at):
        from data_models.models import Security, SecurityIdentityEvent

        with pg_db.get_session() as s:
            if not s.get(Security, 1):
                s.add(Security(
                    id=1, symbol="host", current_symbol="host", market="US",
                    type="CS", is_active=True, full_refresh_interval=30,
                ))
            s.add(SecurityIdentityEvent(
                security_id=1, event_type=event_type,
                old_symbol=old_symbol, new_symbol=new_symbol,
                resolution_source="AUTO", created_at=created_at,
            ))
            s.commit()

    def test_window_and_dotless_expansion(self, pg_db):
        now = datetime.now(timezone.utc)
        self._seed(pg_db, "RENAME", "old.co", "newco", now - timedelta(days=10))
        self._seed(pg_db, "RECYCLE", "hot", "hot", now - timedelta(days=30))
        self._seed(pg_db, "QUARANTINE", "warm", "warm", now - timedelta(days=400))  # 窗口外
        self._seed(pg_db, "NEW_LISTING", "fresh", "fresh", now - timedelta(days=1))  # 类型不涉

        with pg_db.get_session() as s:
            symbols = load_unstable_symbols(s, since=now - timedelta(days=138))
        assert {"old.co", "oldco", "newco", "hot"} <= symbols
        assert "warm" not in symbols
        assert "fresh" not in symbols


@pytest.mark.integration
class TestInsertIdentifiersWithStartDate:
    def test_start_date_written_and_insert_only_semantics_kept(self, pg_db):
        from data_models.models import Security
        from sqlalchemy import text

        with pg_db.get_session() as s:
            s.add(Security(
                id=1, symbol="aapl", current_symbol="aapl", market="US",
                type="CS", is_active=True, full_refresh_interval=30,
            ))
            s.commit()

        row = {
            "security_id": 1, "id_type": "CUSIP", "id_value": "037833100",
            "source": "SEC_FTD", "confidence": "ftd_symbol_match",
            "start_date": date(2026, 5, 1),
        }
        assert pg_db.insert_missing_security_identifiers([row]) == 1
        # 同一身份映射再次出现（不同观测期）：只插不改，首次插入的 start_date 保留
        assert pg_db.insert_missing_security_identifiers(
            [{**row, "start_date": date(2026, 6, 1)}]
        ) == 0
        with pg_db.engine.connect() as conn:
            rows = conn.execute(text(
                "SELECT start_date FROM security_identifiers "
                "WHERE id_type = 'CUSIP' AND id_value = '037833100'"
            )).all()
        assert [r.start_date for r in rows] == [date(2026, 5, 1)]
