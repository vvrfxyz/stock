from types import SimpleNamespace
import unittest

from scripts.update_massive_events import process_security


class FakeSource:
    def get_ticker_events(self, _symbol):
        return {
            "events": [
                {
                    "type": "ticker_change",
                    "date": "2026-03-09",
                    "ticker_change": {"ticker": "RVPH"},
                },
                {
                    "type": "ticker_change",
                    "date": "2026-03-09",
                    "ticker_change": {"ticker": "RVPH"},
                },
            ]
        }


class FakeDatabaseManager:
    def __init__(self):
        self.rows = []
        self.touched_security_ids = []

    def upsert_symbol_history(self, rows):
        seen = set()
        for row in rows:
            key = (row["security_id"], row["symbol"], row["source"], row["start_date"])
            if key in seen:
                raise AssertionError("duplicate symbol-history upsert key")
            seen.add(key)
        self.rows.extend(rows)
        return len(rows)

    def update_security_timestamp(self, security_id, _field_name):
        self.touched_security_ids.append(security_id)


class UpdateMassiveEventsTests(unittest.TestCase):
    def test_process_security_deduplicates_repeated_ticker_change_events(self):
        security = SimpleNamespace(id=19571, symbol="rvph", exchange="XNAS")
        db_manager = FakeDatabaseManager()

        symbol, status, inserted = process_security(security, FakeSource(), db_manager)

        self.assertEqual(symbol, "rvph")
        self.assertEqual(status, "SUCCESS")
        self.assertEqual(inserted, 1)
        self.assertEqual(len(db_manager.rows), 1)
        self.assertEqual(db_manager.touched_security_ids, [19571])


if __name__ == "__main__":
    unittest.main()
