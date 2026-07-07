"""research.data 批量复权与 utils.adjusted_prices 单标的读取层的口径一致性测试。"""
from datetime import date
from decimal import Decimal

import numpy as np
import pandas as pd
import pytest

from research.data import apply_adjustment, securities_with_uncovered_events
from utils.adjusted_prices import factor_for_date


def _vector_factors(prices: pd.DataFrame, events: pd.DataFrame) -> np.ndarray:
    out = apply_adjustment(prices, events)
    return (out["adj_close"] / out["close"]).to_numpy()


def test_apply_adjustment_matches_reference_semantics():
    # 证券 7：两个事件；证券 9：无事件
    prices = pd.DataFrame(
        {
            "security_id": np.array([7, 7, 7, 7, 9, 9], dtype=np.int32),
            "date": pd.to_datetime(
                ["2024-01-02", "2024-02-01", "2024-03-01", "2024-04-01", "2024-01-02", "2024-02-01"]
            ),
            "close": [10.0, 11.0, 12.0, 13.0, 50.0, 51.0],
        }
    )
    events = pd.DataFrame(
        {
            "security_id": [7, 7],
            "ex_date": pd.to_datetime(["2024-02-15", "2024-03-15"]),
            "cumulative_factor": [0.5, 0.8],
        }
    )

    got = _vector_factors(prices, events)

    ref_events = [(date(2024, 2, 15), Decimal("0.5")), (date(2024, 3, 15), Decimal("0.8"))]
    expected_7 = [
        float(factor_for_date(ref_events, d))
        for d in [date(2024, 1, 2), date(2024, 2, 1), date(2024, 3, 1), date(2024, 4, 1)]
    ]
    assert np.allclose(got[:4], expected_7)  # 0.5, 0.5, 0.8, 1.0
    assert np.allclose(got[4:], 1.0)


def test_apply_adjustment_normalizes_future_event_polluted_chain():
    prices = pd.DataFrame(
        {
            "security_id": np.array([22, 22, 22], dtype=np.int32),
            "date": pd.to_datetime(["2026-01-23", "2026-01-26", "2026-06-10"]),
            "close": [100.0, 10.0, 12.0],
        }
    )
    events = pd.DataFrame(
        {
            "security_id": [22, 22],
            "ex_date": pd.to_datetime(["2026-01-26", "2026-06-12"]),
            "cumulative_factor": [300.0, 20.0],
        }
    )

    got = apply_adjustment(prices, events, as_of=date(2026, 6, 10))

    assert np.allclose((got["adj_close"] / got["close"]).to_numpy(), [15.0, 1.0, 1.0])


def test_apply_adjustment_event_on_bar_date_uses_next_event():
    # ex_date 当天 bar 不再应用该事件（factor 取 ex_date > bar_date 的第一个事件）
    prices = pd.DataFrame(
        {
            "security_id": np.array([3, 3], dtype=np.int32),
            "date": pd.to_datetime(["2024-05-09", "2024-05-10"]),
            "close": [100.0, 25.0],
        }
    )
    events = pd.DataFrame(
        {"security_id": [3], "ex_date": pd.to_datetime(["2024-05-10"]), "cumulative_factor": [0.25]}
    )
    got = _vector_factors(prices, events)
    assert np.allclose(got, [0.25, 1.0])


# ---------------------------------------------------------------------------
# securities_with_uncovered_events：事件级（source_event_id）匹配语义
# ---------------------------------------------------------------------------

@pytest.mark.integration
def test_uncovered_events_matches_at_event_level(pg_db):
    from data_models.models import ComputedAdjustmentFactor, CorporateAction, DailyPrice, Security

    day = date(2024, 6, 3)
    mv = "raw_actions_v1"
    with pg_db.get_session() as session:
        for sid in (1, 2, 3, 4, 5):
            session.add(
                Security(
                    id=sid,
                    symbol=f"t{sid}",
                    current_symbol=f"t{sid}",
                    market="US",
                    type="CS",
                    is_active=True,
                    full_refresh_interval=30,
                )
            )
        session.flush()
        # 所有证券给一对跨立事件日的价格行（min < ex_date <= max），
        # 使事件级匹配语义在默认 straddle 口径下原样可测。
        for sid in (1, 2, 3, 4, 5):
            session.add(DailyPrice(security_id=sid, date=date(2024, 5, 31), close=Decimal("10")))
            session.add(DailyPrice(security_id=sid, date=date(2024, 6, 4), close=Decimal("11")))
        # 证券 1：同日拆股已建因子 + 外币分红缺 FX 被跳过（无因子行）。
        # 旧的"同证券同日期存在任意因子行"匹配会被拆股因子行掩盖而漏报。
        session.add(
            CorporateAction(
                security_id=1, action_type="SPLIT", ex_date=day,
                source="massive", source_event_id="ev-split-1",
                split_from=Decimal("1"), split_to=Decimal("2"),
            )
        )
        session.add(
            CorporateAction(
                security_id=1, action_type="DIVIDEND", ex_date=day,
                source="massive", source_event_id="ev-div-1",
                cash_amount=Decimal("1.5"), currency="CAD",
            )
        )
        session.add(
            ComputedAdjustmentFactor(
                security_id=1, date=day, methodology_version=mv,
                factor_type="historical_adjustment", factor_key="split:ev-split-1",
                source_event_id="ev-split-1", action_type="SPLIT",
                cumulative_factor=Decimal("0.5"), event_hash="h1",
            )
        )
        # 证券 2：事件全覆盖，不应剔除
        session.add(
            CorporateAction(
                security_id=2, action_type="DIVIDEND", ex_date=day,
                source="MASSIVE", source_event_id="ev-div-2",
                cash_amount=Decimal("0.2"), currency="USD",
            )
        )
        session.add(
            ComputedAdjustmentFactor(
                security_id=2, date=day, methodology_version=mv,
                factor_type="historical_adjustment", factor_key="dividend:ev-div-2",
                source_event_id="ev-div-2", action_type="DIVIDEND",
                cumulative_factor=Decimal("0.9"), event_hash="h2",
            )
        )
        # 证券 3：事件完全无因子行（退市股缺口的原有语义）
        session.add(
            CorporateAction(
                security_id=3, action_type="SPLIT", ex_date=day,
                source="MASSIVE", source_event_id="ev-split-3",
                split_from=Decimal("1"), split_to=Decimal("10"),
            )
        )
        # 证券 4：非 MASSIVE 孤行（无同日 MASSIVE 对应行）= 复权链上的洞，须剔除
        # （2003 归档导入后 POLYGON 孤行是真实事件的唯一记录：R13 值冲突挂起、
        #  未确认保留的合成行、归档漏抓都落在这个形态）
        session.add(
            CorporateAction(
                security_id=4, action_type="DIVIDEND", ex_date=day,
                source="SEC", source_event_id="ev-div-4",
                cash_amount=Decimal("1.0"), currency="USD",
            )
        )
        # 证券 5：非 MASSIVE 行有同日同类型 MASSIVE 行接管（其因子已建），不剔除
        session.add(
            CorporateAction(
                security_id=5, action_type="DIVIDEND", ex_date=day,
                source="POLYGON", source_event_id="massive-dividend:5:legacy",
                cash_amount=Decimal("0.3"), currency="USD",
            )
        )
        session.add(
            CorporateAction(
                security_id=5, action_type="DIVIDEND", ex_date=day,
                source="MASSIVE", source_event_id="ev-div-5",
                cash_amount=Decimal("0.3"), currency="USD",
            )
        )
        session.add(
            ComputedAdjustmentFactor(
                security_id=5, date=day, methodology_version=mv,
                factor_type="historical_adjustment", factor_key="dividend:ev-div-5",
                source_event_id="ev-div-5", action_type="DIVIDEND",
                cumulative_factor=Decimal("0.95"), event_hash="h5",
            )
        )
        session.commit()

    got = securities_with_uncovered_events(
        pg_db.engine, start=date(2024, 6, 1), end=date(2024, 6, 30)
    )
    assert sorted(got) == [1, 3, 4]

    # 全部事件都跨立时，legacy 口径与 straddle 口径一致
    legacy = securities_with_uncovered_events(
        pg_db.engine, start=date(2024, 6, 1), end=date(2024, 6, 30), require_straddle=False
    )
    assert sorted(legacy) == [1, 3, 4]


@pytest.mark.integration
def test_uncovered_events_straddle_gate(pg_db):
    """跨立精化：只有跨立价格序列（min_date < ex_date <= max_date）的未覆盖事件计为洞。

    原理：因子只作用 ex_date 之前的价格行——事件前无价格则无可调整行，事件后无价格
    则全序列同乘常数、收益率不变；两种形态都放行。生产实测 2310 -> 794。
    """
    from data_models.models import CorporateAction, DailyPrice, Security

    with pg_db.get_session() as session:
        for sid in (11, 12, 13, 14, 15, 16, 17):
            session.add(
                Security(
                    id=sid,
                    symbol=f"s{sid}",
                    current_symbol=f"s{sid}",
                    market="US",
                    type="CS",
                    is_active=True,
                    full_refresh_interval=30,
                )
            )
        session.flush()

        def _split(sid: int, ex: date, eid: str, source: str = "MASSIVE") -> None:
            session.add(
                CorporateAction(
                    security_id=sid, action_type="SPLIT", ex_date=ex,
                    source=source, source_event_id=eid,
                    split_from=Decimal("1"), split_to=Decimal("2"),
                )
            )

        def _price(sid: int, d: date) -> None:
            session.add(DailyPrice(security_id=sid, date=d, close=Decimal("10")))

        # 11：MASSIVE 缺因子事件不晚于首根价格（ex_date == min_date，无 ex 前行）-> 放行
        _split(11, date(2024, 6, 3), "ev-11")
        _price(11, date(2024, 6, 3))
        _price(11, date(2024, 6, 10))
        # 12：MASSIVE 缺因子事件跨立 -> 剔除
        _split(12, date(2024, 6, 3), "ev-12")
        _price(12, date(2024, 5, 30))
        _price(12, date(2024, 6, 5))
        # 13：POLYGON 孤行全部在价格覆盖之前开始（价格都在 ex 之后）-> 放行
        _split(13, date(2024, 6, 3), "ev-13", source="POLYGON")
        _price(13, date(2024, 6, 4))
        _price(13, date(2024, 6, 5))
        # 14：MASSIVE 缺因子事件晚于最后一根价格（max_date < ex_date）-> 放行
        _split(14, date(2024, 6, 3), "ev-14")
        _price(14, date(2024, 5, 1))
        _price(14, date(2024, 5, 15))
        # 15：POLYGON 跨立孤行 -> 剔除
        _split(15, date(2024, 6, 3), "ev-15", source="POLYGON")
        _price(15, date(2024, 5, 30))
        _price(15, date(2024, 6, 3))  # max_date == ex_date 也算跨立（ex 前后都有行）
        # 16：混合——一条跨立 + 一条不跨立（晚于最后价格）-> 剔除
        _split(16, date(2024, 6, 5), "ev-16a")
        _split(16, date(2024, 6, 20), "ev-16b")
        _price(16, date(2024, 6, 1))
        _price(16, date(2024, 6, 10))
        # 17：有未覆盖事件但完全无价格行 -> 放行
        _split(17, date(2024, 6, 3), "ev-17")
        session.commit()

    window = dict(start=date(2024, 6, 1), end=date(2024, 6, 30))
    got = securities_with_uncovered_events(pg_db.engine, **window)
    assert sorted(got) == [12, 15, 16]

    # require_straddle=False 完全复现旧口径：任何未覆盖事件都剔除
    legacy = securities_with_uncovered_events(pg_db.engine, require_straddle=False, **window)
    assert sorted(legacy) == [11, 12, 13, 14, 15, 16, 17]
