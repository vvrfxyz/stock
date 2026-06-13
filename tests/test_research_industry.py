"""research.industry 的 SIC->FF12 静态映射语义测试。"""
from __future__ import annotations

import pandas as pd
import pytest
from sqlalchemy import text

from research.industry import SIC_TO_FF12, coverage_report, load_industry_panel, sic_to_ff12


def _panel(*rows) -> pd.DataFrame:
    return pd.DataFrame(rows, columns=["security_id", "sic_code", "ff12", "ff12_coverage_reason"])


def _insert_security(pg_db, security_id, symbol, sic_code, *, is_active=True):
    with pg_db.engine.connect() as conn:
        conn.execute(
            text(
                """
                insert into securities
                    (id, symbol, current_symbol, market, type, sic_code, is_active,
                     full_refresh_interval)
                values
                    (:id, :symbol, :symbol, 'US', 'CS', :sic_code, :is_active, 30)
                """
            ),
            {
                "id": security_id,
                "symbol": symbol,
                "sic_code": sic_code,
                "is_active": is_active,
            },
        )
        conn.commit()


def test_sic_to_ff12_known_codes():
    cases = {
        2010: "NoDur",
        3711: "Durbl",
        2860: "Chems",
        2825: "Chems",
        2830: "Hlth",
        2835: "Hlth",
        2839: "Hlth",
        3674: "BusEq",
        4812: "Telcm",
        4911: "Utils",
        5411: "Shops",
        8062: "Hlth",
        6020: "Money",
        1311: "Enrgy",
        5712: "Shops",
        2300: "NoDur",
        7011: "Other",
    }
    for sic_code, expected in cases.items():
        assert sic_to_ff12(sic_code) == expected


def test_sic_to_ff12_manuf_known_codes():
    assert sic_to_ff12(2520) == "Manuf"
    assert sic_to_ff12(3569) == "Manuf"
    assert sic_to_ff12(3799) == "Manuf"


def test_sic_to_ff12_handles_dirty_input():
    for sic_code in (None, "", "N/A", -1, 99999, 10.0, "abc"):
        assert sic_to_ff12(sic_code) is None
    assert sic_to_ff12(" 0100 ") == "NoDur"


def test_chems_does_not_swallow_hlth_subrange():
    assert sic_to_ff12(2829) == "Chems"
    assert sic_to_ff12(2830) == "Hlth"
    assert sic_to_ff12(2835) == "Hlth"
    assert sic_to_ff12(2839) == "Hlth"
    assert sic_to_ff12(2840) == "Chems"


def test_official_range_table_keeps_chems_split():
    chems_ranges = [(start, end) for start, end, bucket in SIC_TO_FF12 if bucket == "Chems"]
    hlth_ranges = [(start, end) for start, end, bucket in SIC_TO_FF12 if bucket == "Hlth"]
    assert chems_ranges == [(2800, 2829), (2840, 2899)]
    assert (2830, 2839) in hlth_ranges


def test_coverage_report_shape():
    panel = _panel(
        (1, "2010", "NoDur", "mapped"),
        (2, "2835", "Hlth", "mapped"),
        (3, None, None, "no_sic"),
        (4, "99999", None, "unmapped_sic"),
    )
    report = coverage_report(panel)
    assert set(report) == {
        "total_securities",
        "mapped",
        "mapped_pct",
        "no_sic",
        "unmapped_sic",
        "by_ff12",
    }
    assert report["total_securities"] == 4
    assert report["mapped"] + report["no_sic"] + report["unmapped_sic"] == 4
    assert report["mapped_pct"] == 0.5
    assert report["by_ff12"]["NoDur"] == 1
    assert report["by_ff12"]["Hlth"] == 1


def test_coverage_report_empty_panel():
    report = coverage_report(_panel())
    assert report["total_securities"] == 0
    assert report["mapped"] == 0
    assert report["mapped_pct"] == 0.0
    assert all(count == 0 for count in report["by_ff12"].values())


@pytest.mark.integration
def test_load_industry_panel_against_real_schema(pg_db):
    _insert_security(pg_db, 1, "aapl", "2010")
    _insert_security(pg_db, 2, "hotel", "7011")
    _insert_security(pg_db, 3, "nosic", None)
    _insert_security(pg_db, 4, "bad", "99999", is_active=False)

    panel = load_industry_panel(pg_db.engine)

    assert len(panel) == 4
    rows = panel.set_index("security_id")
    assert rows.loc[1, "ff12"] == "NoDur"
    assert rows.loc[2, "ff12"] == "Other"
    assert rows.loc[3, "ff12_coverage_reason"] == "no_sic"
    assert rows.loc[4, "ff12_coverage_reason"] == "unmapped_sic"
    assert set(panel["ff12_coverage_reason"]) == {"mapped", "no_sic", "unmapped_sic"}


@pytest.mark.integration
def test_load_industry_panel_filters_security_ids(pg_db):
    _insert_security(pg_db, 1, "aapl", "2010")
    _insert_security(pg_db, 2, "bank", "6020")
    _insert_security(pg_db, 3, "shop", "5411")

    panel = load_industry_panel(pg_db.engine, security_ids=[1, 3])

    assert panel["security_id"].tolist() == [1, 3]
    assert panel["ff12"].tolist() == ["NoDur", "Shops"]


@pytest.mark.integration
def test_coverage_report_against_production_like_panel(pg_db):
    sics = ["2010"] * 70 + ["6020"] * 20 + [None] * 5 + ["99999"] * 5
    for idx, sic_code in enumerate(sics, start=1):
        _insert_security(pg_db, idx, f"sec{idx}", sic_code)

    report = coverage_report(load_industry_panel(pg_db.engine))

    assert report["total_securities"] == 100
    assert report["mapped"] == 90
    assert report["mapped_pct"] == 0.9
    assert report["no_sic"] == 5
    assert report["unmapped_sic"] == 5
    assert report["by_ff12"]["NoDur"] == 70
    assert report["by_ff12"]["Money"] == 20
