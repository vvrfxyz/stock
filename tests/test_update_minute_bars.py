from datetime import date
from types import SimpleNamespace

import pytest

from scripts.update_minute_bars import MAX_BARS_PER_CALENDAR_DAY, create_parser, process_security, run


def test_default_workers_keep_api_fetch_concurrency():
    assert create_parser().parse_args([]).workers == 8


def test_rejects_implausibly_large_vendor_response():
    security = SimpleNamespace(symbol="aapl", list_date=None, delist_date=None, id=1)
    source = SimpleNamespace(
        get_minute_aggs=lambda *args: [{}] * (MAX_BARS_PER_CALENDAR_DAY + 1)
    )

    with pytest.raises(RuntimeError, match="分钟响应体量异常"):
        process_security(security, source, None, date(2026, 7, 8), date(2026, 7, 8))


def test_run_batches_rows_across_securities(monkeypatch):
    import scripts.update_minute_bars as minute

    securities = [SimpleNamespace(symbol=f"s{i}") for i in range(3)]
    inserted = []
    monkeypatch.setattr(minute, "CH_BATCH_ROWS", 3)
    monkeypatch.setattr(minute, "FETCH_CHUNK_SIZE", 2)
    monkeypatch.setattr(minute, "get_last_completed_trading_date", lambda market: date(2026, 7, 9))
    monkeypatch.setattr(minute, "get_securities_to_update", lambda db, args: securities)
    monkeypatch.setattr(
        minute,
        "prepare_security_rows",
        lambda security, source, start, end: ("SUCCESS", [security.symbol] * 2),
    )
    monkeypatch.setattr(minute, "ch_insert_rows", lambda rows: inserted.append(list(rows)))

    code, stats = run(
        SimpleNamespace(market="US", start="2026-07-02", lookback_days=8, workers=1),
        SimpleNamespace(),
        SimpleNamespace(),
    )

    assert code == 0
    assert stats == {"processed": 3, "written": 3, "failed": 0}
    assert inserted == [["s0", "s0", "s1"], ["s1", "s2", "s2"]]


def test_clickhouse_insert_disables_parallel_parsing(monkeypatch):
    import scripts.update_minute_bars as minute

    seen = {}

    class Response:
        status_code = 200
        text = ""

    monkeypatch.setattr(minute, "clickhouse_url", lambda: "http://clickhouse")
    monkeypatch.setattr(minute, "clickhouse_request_kwargs", lambda: {})
    monkeypatch.setattr("requests.post", lambda *args, **kwargs: (seen.update(kwargs) or Response()))

    minute.ch_insert_rows(["row"])

    assert seen["params"]["input_format_parallel_parsing"] == "0"
    assert seen["params"]["max_insert_threads"] == "1"
