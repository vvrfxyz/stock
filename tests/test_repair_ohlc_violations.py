"""repair_ohlc_violations 语义锁定（PG 集成）：

- 只有违反包含不变量的行进入候选集；合法大振幅行绝不误伤（位级不变）。
- 分钟重构值满足不变量才 UPDATE，且只动 high/low 两列，其余列位级不变。
- 分钟无覆盖 / 分钟重构自身不满足不变量 / pre-2003 深历史行默认只搁置（hold），
  --clamp-unrepairable 才落保守 clamp。
- 默认 dry-run 绝不写库；TSV 审计报告按 action/reason 留痕。

ClickHouse 分钟查询以 monkeypatch 假实现替身（按 (security_id, date) 查表），
pre-floor 行不得触发分钟查询。
"""
from datetime import date
from decimal import Decimal

import pytest

import scripts.repair_ohlc_violations as repair_mod

FLOOR = repair_mod.MINUTE_FLOOR

# 行剧本：date -> (open, high, low, close, 分钟替身返回值)
#   BAD_HIGH   高点低于 close 的违规行，分钟重构健康 -> repair_minute
#   BAD_LOW    低点高于 open/close 的违规行，分钟重构健康 -> repair_minute
#   LEGIT      大振幅但满足不变量的合法极值行 -> 不进候选集，位级不动
#   NO_MINUTE  违规但分钟无覆盖 -> hold(no_minute_bars) / clamp
#   BAD_MINUTE 违规且分钟重构自身不满足不变量 -> hold(no_minute_bars) / clamp
#   PRE_FLOOR  违规但在分钟下限之前 -> hold(pre_minute_floor)，不得查分钟
BAD_HIGH = date(2015, 1, 5)
BAD_LOW = date(2015, 1, 6)
LEGIT = date(2015, 1, 7)
NO_MINUTE = date(2015, 1, 8)
BAD_MINUTE = date(2015, 1, 9)
PRE_FLOOR = date(1999, 5, 5)

ROWS = {
    BAD_HIGH: ("10", "9.5", "9.4", "10.2", (10.6, 9.3)),
    BAD_LOW: ("10", "11", "10.05", "10.5", (11.2, 9.9)),
    LEGIT: ("10", "25", "4", "12", None),
    NO_MINUTE: ("10", "9.5", "9", "10.2", None),
    BAD_MINUTE: ("10", "9.5", "9", "10.2", (9.0, 8.0)),  # m_high < max(o,c)
    PRE_FLOOR: ("10", "9.5", "9", "10.2", None),
}


def test_minute_extremes_reads_replacing_merge_tree_with_final(monkeypatch):
    seen = {}

    class Response:
        status_code = 200
        text = "12.0\t8.0\t10\n"

    def post(_url, **kwargs):
        seen["sql"] = kwargs["data"].decode()
        return Response()

    monkeypatch.setattr(repair_mod.requests, "post", post)
    monkeypatch.setattr(repair_mod, "clickhouse_url", lambda: "http://clickhouse")
    monkeypatch.setattr(repair_mod, "clickhouse_request_kwargs", lambda: {})

    assert repair_mod.minute_extremes(1, BAD_HIGH) == (12.0, 8.0)
    assert "FROM stock.minute_bars FINAL" in seen["sql"]


def _seed(pg_db):
    from data_models.models import DailyPrice, Security

    with pg_db.get_session() as session:
        session.add(Security(id=1, symbol="t1", current_symbol="t1", market="US",
                             type="CS", is_active=True, full_refresh_interval=30))
        session.flush()
        for day, (o, h, l, c, _) in ROWS.items():
            session.add(DailyPrice(security_id=1, date=day, open=Decimal(o),
                                   high=Decimal(h), low=Decimal(l), close=Decimal(c),
                                   volume=1000, vwap=Decimal("10.1"), trade_count=42))
        session.commit()


def _snapshot(pg_db) -> dict:
    """全列位级快照：date -> (open, high, low, close, volume, vwap, trade_count)。"""
    from data_models.models import DailyPrice

    with pg_db.get_session() as session:
        return {p.date: (p.open, p.high, p.low, p.close, p.volume, p.vwap, p.trade_count)
                for p in session.query(DailyPrice).all()}


def _run(monkeypatch, pg_db, pg_url, tmp_path, extra_args=()):
    """跑 main()：DatabaseManager 指向测试库，分钟查询走 ROWS 剧本替身。"""
    report = tmp_path / "report.tsv"
    monkeypatch.setattr(repair_mod, "DatabaseManager",
                        lambda *a, **kw: type(pg_db)(pg_url))

    def fake_minute_extremes(security_id: int, day: date):
        assert day >= FLOOR, f"pre-floor 行 {day} 不得触发分钟查询"
        assert day in ROWS, f"意外的分钟查询: {day}"
        return ROWS[day][4]

    monkeypatch.setattr(repair_mod, "minute_extremes", fake_minute_extremes)
    exit_code = repair_mod.main(["--report", str(report), *extra_args])
    assert exit_code == 0
    lines = report.read_text().rstrip("\n").split("\n")
    header, body = lines[0], [line.split("\t") for line in lines[1:]]
    assert header.split("\t") == ["action", "symbol", "date", "open", "high", "low",
                                  "close", "new_high", "new_low", "reason"]
    return {row[2]: row for row in body}  # date -> tsv row


@pytest.mark.integration
def test_dry_run_writes_nothing_but_reports(monkeypatch, pg_db, pg_url, tmp_path):
    _seed(pg_db)
    before = _snapshot(pg_db)
    tsv = _run(monkeypatch, pg_db, pg_url, tmp_path)

    assert _snapshot(pg_db) == before  # 默认 dry-run：全部位级不变

    # 审计留痕：两行可修 + 三行搁置；合法极值行不在报告里
    assert tsv[str(BAD_HIGH)][0] == "repair_minute"
    assert tsv[str(BAD_LOW)][0] == "repair_minute"
    assert (tsv[str(NO_MINUTE)][0], tsv[str(NO_MINUTE)][9]) == ("hold", "no_minute_bars")
    assert (tsv[str(BAD_MINUTE)][0], tsv[str(BAD_MINUTE)][9]) == ("hold", "no_minute_bars")
    assert (tsv[str(PRE_FLOOR)][0], tsv[str(PRE_FLOOR)][9]) == ("hold", "pre_minute_floor")
    assert str(LEGIT) not in tsv
    assert len(tsv) == 5


@pytest.mark.integration
def test_apply_repairs_only_violating_columns(monkeypatch, pg_db, pg_url, tmp_path):
    _seed(pg_db)
    before = _snapshot(pg_db)
    tsv = _run(monkeypatch, pg_db, pg_url, tmp_path, ["--apply"])
    after = _snapshot(pg_db)

    # 两行分钟修复：只动 high/low，open/close/volume/vwap/trade_count 位级不变
    for day in (BAD_HIGH, BAD_LOW):
        m_high, m_low = ROWS[day][4]
        o, h, l, c, vol, vwap, tc = after[day]
        assert (h, l) == (Decimal(str(m_high)), Decimal(str(m_low)))
        assert (o, c, vol, vwap, tc) == (
            before[day][0], before[day][3], before[day][4], before[day][5], before[day][6])
        assert tsv[str(day)][0] == "repair_minute"
        assert (float(tsv[str(day)][7]), float(tsv[str(day)][8])) == (m_high, m_low)

    # 合法极值行 + 三类搁置行：整行位级不变
    for day in (LEGIT, NO_MINUTE, BAD_MINUTE, PRE_FLOOR):
        assert after[day] == before[day]

    # 修复后不变量成立，幂等：重跑候选集只剩搁置行
    tsv2 = _run(monkeypatch, pg_db, pg_url, tmp_path, ["--apply"])
    assert {row[0] for row in tsv2.values()} == {"hold"}
    assert _snapshot(pg_db) == after


@pytest.mark.integration
def test_clamp_unrepairable_bounds_without_inventing_prices(monkeypatch, pg_db, pg_url, tmp_path):
    _seed(pg_db)
    before = _snapshot(pg_db)
    tsv = _run(monkeypatch, pg_db, pg_url, tmp_path, ["--apply", "--clamp-unrepairable"])
    after = _snapshot(pg_db)

    # 分钟治不了的三行落保守 clamp：high:=max(o,h,c)、low:=min(o,l,c)
    for day in (NO_MINUTE, BAD_MINUTE, PRE_FLOOR):
        o, h, l, c = (Decimal(v) for v in ROWS[day][:4])
        assert (after[day][1], after[day][2]) == (max(o, h, c), min(o, l, c))
        assert (after[day][0], after[day][3]) == (before[day][0], before[day][3])
        assert tsv[str(day)][0] == "repair_clamp"
    assert after[LEGIT] == before[LEGIT]
