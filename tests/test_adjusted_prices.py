"""复权读取层单测：用 AAPL 4:1 拆股形态验证因子应用、as_of 防未来函数。"""
from datetime import date
from decimal import Decimal

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from data_models.models import ComputedAdjustmentFactor, DailyPrice, Security
from utils.adjusted_prices import factor_for_date, get_adjusted_daily_bars


def _make_session():
    engine = create_engine("sqlite:///:memory:")
    Security.__table__.create(engine)
    DailyPrice.__table__.create(engine)
    ComputedAdjustmentFactor.__table__.create(engine)
    return sessionmaker(bind=engine)()


def _seed_split_scenario(session):
    """8/28 收 499，8/31 4:1 拆股后开 127.58。事件 cumulative=0.25。"""
    session.add(Security(id=1, symbol="aapl", current_symbol="aapl", market="US", type="CS", full_refresh_interval=30))
    session.add_all(
        [
            DailyPrice(security_id=1, date=date(2020, 8, 27), close=Decimal("500"), open=Decimal("498"),
                       high=Decimal("501"), low=Decimal("495"), volume=1000),
            DailyPrice(security_id=1, date=date(2020, 8, 28), close=Decimal("499"), open=Decimal("497"),
                       high=Decimal("500"), low=Decimal("494"), volume=1100),
            DailyPrice(security_id=1, date=date(2020, 8, 31), close=Decimal("129"), open=Decimal("127.58"),
                       high=Decimal("131"), low=Decimal("126"), volume=4400),
            DailyPrice(security_id=1, date=date(2020, 9, 1), close=Decimal("134"), open=Decimal("132"),
                       high=Decimal("135"), low=Decimal("130"), volume=4000),
        ]
    )
    session.add(
        ComputedAdjustmentFactor(
            id=1,
            security_id=1, date=date(2020, 8, 31), methodology_version="raw_actions_v1",
            factor_type="historical_adjustment", factor_key="split:test-4-1",
            source_event_id="test-4-1", action_type="SPLIT",
            single_event_factor=Decimal("0.25"), cumulative_factor=Decimal("0.25"),
            event_hash="x", as_of_date=date(2020, 9, 1),
        )
    )
    session.commit()


def test_factor_for_date_applies_to_bars_strictly_before_ex_date():
    events = [(date(2020, 8, 31), Decimal("0.25"))]
    assert factor_for_date(events, date(2020, 8, 28)) == Decimal("0.25")
    assert factor_for_date(events, date(2020, 8, 31)) == Decimal("1")  # ex 当日已是新价
    assert factor_for_date(events, date(2020, 9, 1)) == Decimal("1")
    assert factor_for_date([], date(2020, 8, 28)) == Decimal("1")


def test_adjusted_series_is_smooth_across_split():
    session = _make_session()
    _seed_split_scenario(session)

    bars = get_adjusted_daily_bars(session, "aapl", start=date(2020, 8, 27), end=date(2020, 9, 1))

    by_date = {bar.date: bar for bar in bars}
    # 拆股前的 bar 被乘 0.25：499 -> 124.75，与拆后 129 平滑衔接（无 4 倍断崖）
    assert by_date[date(2020, 8, 28)].close == Decimal("124.75")
    assert by_date[date(2020, 8, 28)].adjustment_factor == Decimal("0.25")
    assert by_date[date(2020, 8, 31)].close == Decimal("129")
    assert by_date[date(2020, 8, 31)].adjustment_factor == Decimal("1")
    # raw close 原样保留，volume 不做调整（raw fact）
    assert by_date[date(2020, 8, 28)].raw_close == Decimal("499")
    assert by_date[date(2020, 8, 28)].volume == 1100
    ratio = by_date[date(2020, 8, 31)].close / by_date[date(2020, 8, 28)].close
    assert Decimal("0.9") < ratio < Decimal("1.1")


def test_as_of_before_split_hides_future_event():
    session = _make_session()
    _seed_split_scenario(session)

    bars = get_adjusted_daily_bars(
        session, "aapl", start=date(2020, 8, 27), end=date(2020, 8, 28), as_of=date(2020, 8, 28)
    )

    # 8/28 时点拆股尚未发生：不允许用未来事件调整历史
    assert all(bar.adjustment_factor == Decimal("1") for bar in bars)
    assert bars[-1].close == Decimal("499")


def test_resolves_symbol_case_insensitively_and_unknown_raises():
    session = _make_session()
    _seed_split_scenario(session)

    assert get_adjusted_daily_bars(session, "AAPL", start=date(2020, 8, 27), end=date(2020, 8, 27))

    import pytest
    with pytest.raises(LookupError):
        get_adjusted_daily_bars(session, "nope", start=date(2020, 8, 27), end=date(2020, 8, 27))
