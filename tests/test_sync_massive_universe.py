"""sync_massive_universe main() 编排测试（Mock source/db，不依赖 PostgreSQL）。

覆盖 rename 链的批内依赖排序与单条失败隔离：
- A→B、B→C 同批链式改名，两种 feed 顺序都能完整收敛；
- 单条 rename 占用冲突只隔离该条（QUARANTINE 事件 + exit=1），
  其余 rename / normal upsert / mark-missing 步骤照常执行。
"""
import json
from unittest.mock import MagicMock

import scripts.sync_massive_universe as sync_universe
from utils.security_identity import SecurityIdentityResolver, _SecurityRow


def _row(id, symbol, *, figi=None, cik=None, exchange=None, is_active=True):
    return _SecurityRow(id=id, symbol=symbol, current_symbol=symbol,
                        composite_figi=figi, share_class_figi=None, cik=cik,
                        exchange=exchange, is_active=is_active)


def _build_resolver(rows, history=None):
    by_figi = {}
    by_cik = {}
    by_symbol = {}
    by_symbol_all = {}
    for row in rows:
        if row.composite_figi:
            existing = by_figi.get(row.composite_figi.upper())
            if existing is None or (not existing.is_active and row.is_active):
                by_figi[row.composite_figi.upper()] = row
        if row.cik:
            by_cik.setdefault(row.cik, []).append(row)
        sym = row.symbol.lower()
        by_symbol_all.setdefault(sym, []).append(row)
        if row.is_active:
            by_symbol[sym] = row
    return SecurityIdentityResolver._from_indexes(by_figi, by_cik, by_symbol, by_symbol_all, history or {})


class FakeSource:
    def __init__(self, payloads):
        self.payloads = payloads

    def list_active_tickers(self, allowed_types=None):
        return self.payloads

    def _build_reference_payload(self, item):
        return item

    def close(self):
        pass


class FakeDB:
    """模拟 DatabaseManager：维护 active symbol 占用表，重现 rename_security 的占用防御。"""

    def __init__(self, active):
        self.active = dict(active)  # symbol -> security_id
        self.renames = []
        self.info_upserts = []
        self.identity_events = []
        self.symbol_upserts = []
        self.sessions = []

    def get_session(self):
        session = MagicMock()
        session.execute.return_value.rowcount = 0
        ctx = MagicMock()
        ctx.__enter__.return_value = session
        ctx.__exit__.return_value = False
        self.sessions.append(session)
        return ctx

    def rename_security(self, security_id, old_symbol, new_symbol, *, exchange=None, source="MASSIVE"):
        holder = self.active.get(new_symbol)
        if holder is not None and holder != security_id:
            raise ValueError(
                f"rename_security 失败: new_symbol={new_symbol} 已被 security_id={holder} 占用"
            )
        if self.active.get(old_symbol) == security_id:
            del self.active[old_symbol]
        self.active[new_symbol] = security_id
        self.renames.append((security_id, old_symbol, new_symbol))

    def upsert_security_info(self, row):
        self.info_upserts.append(row)

    def insert_identity_events(self, events):
        self.identity_events.extend(events)
        return len(events)

    def upsert_securities_by_symbol(self, rows, touch_info_timestamp=True):
        self.symbol_upserts.extend(rows)
        return len(rows)

    def close(self):
        pass


def _run_main(monkeypatch, payloads, resolver, db, argv=None):
    monkeypatch.setattr(sync_universe, "setup_logging", lambda: None)
    monkeypatch.setattr(sync_universe, "get_massive_api_keys", lambda: ["key"])
    monkeypatch.setattr(sync_universe, "KeyRateLimiter", lambda *args, **kwargs: object())
    monkeypatch.setattr(sync_universe, "MassiveSource", lambda rate_limiter: FakeSource(payloads))
    monkeypatch.setattr(sync_universe, "DatabaseManager", lambda: db)
    monkeypatch.setattr(sync_universe, "SecurityIdentityResolver", lambda session: resolver)
    return sync_universe.main(argv if argv is not None else [])


def _chain_fixtures():
    existing = [_row(1, "a", figi="BBG000AAA"), _row(2, "b", figi="BBG000BBB")]
    resolver = _build_resolver(existing)
    db = FakeDB({"a": 1, "b": 2})
    return resolver, db


class TestChainRenameConverges:
    def _assert_converged(self, code, db):
        assert code == 0
        assert db.active == {"b": 1, "c": 2}
        # B→C 先执行释放 b，A→B 才能成功
        assert db.renames == [(2, "b", "c"), (1, "a", "b")]
        assert [e["event_type"] for e in db.identity_events] == ["RENAME", "RENAME"]
        assert sorted(r["id"] for r in db.info_upserts) == [1, 2]

    def test_dependent_row_first_in_feed(self, monkeypatch):
        # 旧实现按 feed 顺序先跑 A→B，撞占用防御后整批中止
        payloads = [
            {"symbol": "b", "composite_figi": "BBG000AAA"},   # A→B
            {"symbol": "c", "composite_figi": "BBG000BBB"},   # B→C
        ]
        resolver, db = _chain_fixtures()
        code = _run_main(monkeypatch, payloads, resolver, db)
        self._assert_converged(code, db)

    def test_dependent_row_last_in_feed(self, monkeypatch):
        payloads = [
            {"symbol": "c", "composite_figi": "BBG000BBB"},   # B→C
            {"symbol": "b", "composite_figi": "BBG000AAA"},   # A→B
        ]
        resolver, db = _chain_fixtures()
        code = _run_main(monkeypatch, payloads, resolver, db)
        self._assert_converged(code, db)


class TestSingleRenameFailureIsolated:
    def _fixtures(self):
        # id=3 仍活跃占着 x（当日退市、不在 feed 里），A→X 必然撞占用防御
        existing = [
            _row(1, "a", figi="BBG000AAA"),
            _row(2, "b", figi="BBG000BBB"),
            _row(3, "x", figi="BBG000XXX"),
        ]
        resolver = _build_resolver(existing)
        db = FakeDB({"a": 1, "b": 2, "x": 3})
        payloads = [
            {"symbol": "x", "composite_figi": "BBG000AAA"},   # A→X，占用冲突
            {"symbol": "c", "composite_figi": "BBG000BBB"},   # B→C，应正常完成
            {"symbol": "new1"},                                 # 新上市，normal 路径
        ]
        return resolver, db, payloads

    def test_failure_quarantined_and_rest_of_batch_written(self, monkeypatch):
        resolver, db, payloads = self._fixtures()
        code = _run_main(monkeypatch, payloads, resolver, db)

        # 有跳过条目 → 警告级退出码
        assert code == 1
        # 失败条目未落库，其余 rename 正常写入
        assert db.renames == [(2, "b", "c")]
        assert db.active == {"a": 1, "c": 2, "x": 3}
        # 事件：成功的 RENAME + 失败的 QUARANTINE
        by_type = {e["event_type"]: e for e in db.identity_events}
        assert set(by_type) == {"RENAME", "QUARANTINE"}
        assert by_type["RENAME"]["security_id"] == 2
        quarantine = by_type["QUARANTINE"]
        assert quarantine["security_id"] == 1
        assert quarantine["new_symbol"] == "x"
        details = json.loads(quarantine["details"])
        assert "已被 security_id=3 占用" in details["error"]
        # 失败条目不做 upsert_security_info
        assert [r["id"] for r in db.info_upserts] == [2]
        # normal 路径不受影响
        assert db.symbol_upserts == [{"symbol": "new1"}]
        # mark-missing 步骤仍执行（get_session 第二次调用即该步骤）
        assert len(db.sessions) == 2
        assert db.sessions[1].execute.called
        assert db.sessions[1].commit.called

    def test_swap_cycle_both_quarantined_batch_survives(self, monkeypatch):
        # A↔B 互换成环：两条都撞占用防御，各自隔离，批处理继续
        existing = [_row(1, "a", figi="BBG000AAA"), _row(2, "b", figi="BBG000BBB")]
        resolver = _build_resolver(existing)
        db = FakeDB({"a": 1, "b": 2})
        payloads = [
            {"symbol": "b", "composite_figi": "BBG000AAA"},   # A→B
            {"symbol": "a", "composite_figi": "BBG000BBB"},   # B→A
            {"symbol": "new1"},
        ]
        code = _run_main(monkeypatch, payloads, resolver, db)
        assert code == 1
        assert db.renames == []
        assert db.active == {"a": 1, "b": 2}
        assert [e["event_type"] for e in db.identity_events] == ["QUARANTINE", "QUARANTINE"]
        assert db.symbol_upserts == [{"symbol": "new1"}]
