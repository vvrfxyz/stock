"""adjudicate_polygon_orphans 纯逻辑单元测试：值解析 / 归档匹配 / 价格检验 / 顺序分桶。

全部 mock 行对象（dict），不连库。价格取向约定见脚本 docstring：
expected = split_from/split_to 与 realized = next_close/prev_close 同为"新价/旧价"。
"""
from datetime import date
from decimal import Decimal

import pytest

from scripts.adjudicate_polygon_orphans import (
    Verdict,
    classify_orphan,
    demote_promote_collisions,
    dividend_price_verdict,
    dividend_value_matches,
    gather_candidates,
    is_tenure_violation,
    parse_quarantine_detail,
    parse_quarantine_value,
    split_price_verdict,
    split_value_matches,
)

TODAY = date(2026, 7, 7)


def _orphan(kind="dividend", ex=date(2010, 5, 10), cash="0.25", currency="USD",
            split_from=None, split_to=None, list_date=date(2003, 1, 2), delist_date=None,
            is_active=True, prev_close="10", next_close="10", security_id=1, ca_id=100):
    return {
        "id": ca_id,
        "security_id": security_id,
        "kind": kind,
        "ex_date": ex,
        "cash_amount": None if cash is None else Decimal(cash),
        "currency": currency,
        "split_from": None if split_from is None else Decimal(split_from),
        "split_to": None if split_to is None else Decimal(split_to),
        "list_date": list_date,
        "delist_date": delist_date,
        "is_active": is_active,
        "prev_close": None if prev_close is None else Decimal(prev_close),
        "next_close": None if next_close is None else Decimal(next_close),
    }


def _div_cand(event_id="E1", ticker="ABC", cash="0.25", currency="USD"):
    return {"event_id": event_id, "ticker": ticker, "cash": Decimal(cash), "currency": currency}


def _spl_cand(event_id="E1", ticker="ABC", split_from="1", split_to="4"):
    return {"event_id": event_id, "ticker": ticker,
            "split_from": Decimal(split_from), "split_to": Decimal(split_to)}


class TestParseQuarantineValue:
    def test_dividend_value(self):
        assert parse_quarantine_value("dividend", "0.2 USD") == {
            "cash": Decimal("0.2"), "currency": "USD"}

    def test_dividend_foreign_currency(self):
        parsed = parse_quarantine_value("dividend", "0.53 cad")
        assert parsed["currency"] == "CAD"

    def test_split_value(self):
        assert parse_quarantine_value("split", "15:1") == {
            "split_from": Decimal("15"), "split_to": Decimal("1")}

    def test_split_fractional_ratio(self):
        parsed = parse_quarantine_value("split", "1.04331:1")
        assert parsed["split_from"] == Decimal("1.04331")

    @pytest.mark.parametrize("kind,value", [
        ("dividend", "abc USD"),
        ("dividend", "0.2"),
        ("dividend", "0 USD"),
        ("split", "1:0"),
        ("split", "-1:2"),
        ("split", "15"),
        ("merger", "1:2"),
    ])
    def test_bad_values_return_none(self, kind, value):
        assert parse_quarantine_value(kind, value) is None


class TestParseQuarantineDetail:
    HEADER = "kind\tticker\treason\tex_date\tevent_id\tvalue\n"

    def test_indexes_recoverable_reasons_only(self):
        lines = [
            self.HEADER,
            "dividend\tABL\tambiguous\t2025-12-02\tE1\t0.2 USD\n",
            "split\tAGFY\tout_of_tenure\t2024-10-08\tE2\t15:1\n",
            "split\tIBM\tspinoff_pseudo_split\t2021-11-04\tP1\t1000:1046\n",
            "split\tNYC\tconflicting_split\t2020-08-18\tE3\t10:1\n",
        ]
        index = parse_quarantine_detail(lines)
        assert ("ABL", date(2025, 12, 2), "dividend") in index
        assert ("AGFY", date(2024, 10, 8), "split") in index
        assert len(index) == 2  # spinoff / conflicting 不进恢复索引

    def test_key_is_upper_ticker(self):
        lines = [self.HEADER, "dividend\tAbl\tambiguous\t2025-12-02\tE1\t0.2 USD\n"]
        index = parse_quarantine_detail(lines)
        assert ("ABL", date(2025, 12, 2), "dividend") in index

    def test_bad_header_raises(self):
        with pytest.raises(ValueError):
            parse_quarantine_detail(["ticker\treason\n"])


class TestGatherCandidates:
    def test_collects_across_symbols_and_dedupes_event_id(self):
        ex = date(2010, 5, 10)
        index = {
            ("OLD", ex, "dividend"): [_div_cand("E1", "OLD")],
            ("NEW", ex, "dividend"): [_div_cand("E1", "NEW"), _div_cand("E2", "NEW")],
        }
        out = gather_candidates(index, {"NEW", "OLD"}, ex, "dividend")
        assert sorted(c["event_id"] for c in out) == ["E1", "E2"]

    def test_no_hit_returns_empty(self):
        assert gather_candidates({}, {"ABC"}, date(2010, 5, 10), "dividend") == []


class TestValueMatching:
    def test_dividend_tolerance_boundary(self):
        cand = _div_cand(cash="0.25")
        assert dividend_value_matches(Decimal("0.255"), "USD", cand)      # 差恰为 0.005
        assert not dividend_value_matches(Decimal("0.2551"), "USD", cand)

    def test_dividend_currency_must_match(self):
        assert not dividend_value_matches(Decimal("0.25"), "CAD", _div_cand(cash="0.25"))

    def test_dividend_null_currency_defaults_usd(self):
        assert dividend_value_matches(Decimal("0.25"), None, _div_cand(cash="0.25"))

    def test_split_ratio_expressed_differently_matches(self):
        # POLYGON 2:4 与归档 1:2 是同一比例
        assert split_value_matches(Decimal("2"), Decimal("4"), _spl_cand(split_from="1", split_to="2"))

    def test_split_reciprocal_ratio_rejected(self):
        assert not split_value_matches(Decimal("15"), Decimal("1"), _spl_cand(split_from="1", split_to="15"))


class TestTenureViolation:
    def test_before_list_date(self):
        assert is_tenure_violation(date(2003, 5, 1), date(2004, 1, 1), True, None)

    def test_inactive_after_delist(self):
        assert is_tenure_violation(date(2020, 1, 1), date(2003, 1, 1), False, date(2019, 6, 1))

    def test_active_with_stale_delist_not_violation(self):
        # 活跃证券残留 delist_date（脏数据）不判违例
        assert not is_tenure_violation(date(2020, 1, 1), date(2003, 1, 1), True, date(2019, 6, 1))

    def test_inactive_without_delist_not_violation(self):
        assert not is_tenure_violation(date(2020, 1, 1), date(2003, 1, 1), False, None)

    def test_inside_tenure(self):
        assert not is_tenure_violation(date(2010, 1, 1), date(2003, 1, 1), False, date(2019, 6, 1))


class TestSplitPriceVerdict:
    def test_forward_split_corroborated(self):
        # AAPL 2020 式 4:1：from=1,to=4，价格 500 -> 129（比例 0.258 vs 期望 0.25）
        assert split_price_verdict("1", "4", "500", "129") == "corroborated"

    def test_reverse_split_corroborated(self):
        # AGFY 式 15:1 反向拆分：价格 0.2 -> 2.9（14.5x vs 期望 15x）
        assert split_price_verdict("15", "1", "0.2", "2.9") == "corroborated"

    def test_flat_price_refutes_material_split(self):
        # 宣称 15:1 但价格纹丝不动 -> 正面反证
        assert split_price_verdict("15", "1", "10", "10.5") == "refuted"

    def test_partial_move_inconclusive(self):
        # 宣称 2:1（期望 2x）但只动了 1.3x：既不佐证也不敢反证
        assert split_price_verdict("2", "1", "10", "13") == "inconclusive"

    def test_small_claimed_ratio_never_refuted(self):
        # |log(expected)| < log(1.5) 的小比例拆股即使价格平坦也不反证（1.04331:1 型）
        assert split_price_verdict("1.2", "1", "10", "10") == "corroborated"

    def test_missing_price_inconclusive(self):
        assert split_price_verdict("1", "4", None, "129") == "inconclusive"
        assert split_price_verdict("1", "4", "500", None) == "inconclusive"

    def test_bad_ratio_inconclusive(self):
        assert split_price_verdict(None, "4", "500", "129") == "inconclusive"


class TestDividendPriceVerdict:
    def test_high_yield_no_drop_refuted(self):
        # 名义收益率 10% 但除权日毫无落差 -> 反证
        assert dividend_price_verdict("1", "USD", "10", "10") == "refuted"

    def test_low_yield_inconclusive(self):
        assert dividend_price_verdict("0.4", "USD", "10", "10") == "inconclusive"

    def test_real_drop_inconclusive(self):
        # 落差 10% 与收益率 10% 相称：真实分红
        assert dividend_price_verdict("1", "USD", "10", "9") == "inconclusive"

    def test_small_gap_inconclusive(self):
        # yield 6%、落差 0：缺口 0.06 <= 0.08，宁缺毋滥不删
        assert dividend_price_verdict("0.6", "USD", "10", "10") == "inconclusive"

    def test_non_usd_inconclusive(self):
        # 1 NOK 分红在 $10 股上名义收益率虚高，不做跨币种检验
        assert dividend_price_verdict("1", "NOK", "10", "10") == "inconclusive"

    def test_null_currency_treated_as_usd(self):
        assert dividend_price_verdict("1", None, "10", "10") == "refuted"

    def test_missing_price_inconclusive(self):
        assert dividend_price_verdict("1", "USD", None, "10") == "inconclusive"


class TestClassifyOrphan:
    def test_tenure_violation_wins_over_archive_match(self):
        row = _orphan(ex=date(2003, 5, 1), list_date=date(2004, 1, 1))
        verdict = classify_orphan(row, [_div_cand()], TODAY)
        assert verdict.bucket == "tenure_violation"
        assert verdict.action == "DELETE"

    def test_archive_dividend_in_window_promotes_min_event_id(self):
        row = _orphan()
        verdict = classify_orphan(row, [_div_cand("E9"), _div_cand("E2")], TODAY)
        assert verdict.bucket == "archive_match_promote"
        assert verdict.action == "PROMOTE"
        assert verdict.archive_event_id == "E2"

    def test_archive_dividend_outside_window_goes_manual(self):
        # 活跃证券带脏 delist_date（桶 1 不触发），窗口上界 = delist_date < ex_date
        row = _orphan(ex=date(2010, 5, 10), is_active=True, delist_date=date(2009, 12, 31))
        verdict = classify_orphan(row, [_div_cand()], TODAY)
        assert verdict.bucket == "manual_residual"
        assert verdict.reason == "archive_dividend_window_violation"
        assert verdict.archive_event_id == "E1"

    def test_archive_split_needs_price_corroboration(self):
        row = _orphan(kind="split", cash=None, currency=None, split_from="1", split_to="4",
                      prev_close="500", next_close="129")
        verdict = classify_orphan(row, [_spl_cand()], TODAY)
        assert verdict.bucket == "archive_match_promote"

    def test_archive_split_flat_price_goes_manual_not_delete(self):
        # 有归档佐证的拆股即使价格反证也不删——归档证据与价格证据冲突，须人工
        row = _orphan(kind="split", cash=None, currency=None, split_from="15", split_to="1",
                      prev_close="10", next_close="10")
        verdict = classify_orphan(row, [_spl_cand(split_from="15", split_to="1")], TODAY)
        assert verdict.bucket == "manual_residual"
        assert verdict.reason == "archive_split_price_unconfirmed"

    def test_unmatched_split_corroborated_is_manual_real(self):
        row = _orphan(kind="split", cash=None, currency=None, split_from="1", split_to="4",
                      prev_close="500", next_close="129")
        verdict = classify_orphan(row, [], TODAY)
        assert verdict.bucket == "manual_real_no_vendor_id"
        assert verdict.action == "MANUAL"

    def test_unmatched_split_refuted_is_delete(self):
        row = _orphan(kind="split", cash=None, currency=None, split_from="15", split_to="1",
                      prev_close="10", next_close="10")
        verdict = classify_orphan(row, [], TODAY)
        assert verdict.bucket == "split_refuted"
        assert verdict.action == "DELETE"

    def test_unmatched_dividend_refuted_is_delete(self):
        row = _orphan(cash="1", prev_close="10", next_close="10")
        verdict = classify_orphan(row, [], TODAY)
        assert verdict.bucket == "dividend_refuted"
        assert verdict.action == "DELETE"

    def test_unmatched_low_yield_dividend_is_manual(self):
        row = _orphan(cash="0.1", prev_close="10", next_close="10")
        verdict = classify_orphan(row, [], TODAY)
        assert verdict.bucket == "manual_residual"

    def test_value_mismatch_candidate_falls_through_to_price_test(self):
        # 候选金额差 0.05 > 0.005：视同无归档匹配，进落差检验
        row = _orphan(cash="1", prev_close="10", next_close="10")
        verdict = classify_orphan(row, [_div_cand(cash="1.05")], TODAY)
        assert verdict.bucket == "dividend_refuted"


class TestDemotePromoteCollisions:
    def _promote(self, event_id="E1"):
        return Verdict("archive_match_promote", "PROMOTE", "archive_dividend_in_window",
                       event_id, "ABC")

    def test_same_event_two_securities_demoted(self):
        verdicts = {100: self._promote(), 200: self._promote()}
        out = demote_promote_collisions(verdicts, {100: 1, 200: 2})
        assert out[100].bucket == "manual_residual"
        assert out[200].bucket == "manual_residual"
        assert out[100].reason == "archive_ambiguous_multi_security"

    def test_same_event_same_security_kept(self):
        # 同证券同日重复 POLYGON 行推举同一归档事件：合法，allowlist 会去重
        verdicts = {100: self._promote(), 200: self._promote()}
        out = demote_promote_collisions(verdicts, {100: 1, 200: 1})
        assert out[100].action == "PROMOTE"
        assert out[200].action == "PROMOTE"

    def test_unrelated_verdicts_untouched(self):
        keep = Verdict("manual_residual", "MANUAL", "dividend_price_inconclusive")
        verdicts = {100: self._promote("E1"), 200: self._promote("E2"), 300: keep}
        out = demote_promote_collisions(verdicts, {100: 1, 200: 2, 300: 3})
        assert out[100].action == "PROMOTE"
        assert out[200].action == "PROMOTE"
        assert out[300] is keep
