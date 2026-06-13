from datetime import date
from types import SimpleNamespace
from unittest.mock import Mock

import scripts.update_insider_transactions as insider


def test_process_filing_uses_issuer_cik_security_id(monkeypatch):
    filing = SimpleNamespace(
        id=10,
        security_id=1,
        accession_number="acc-1",
        filing_date=date(2026, 6, 1),
        primary_document_url="https://www.sec.gov/x.xml",
    )
    source = Mock()
    source.fetch_ownership_document.return_value = "<ownershipDocument/>"
    db = Mock()
    db.upsert_insider_transactions.return_value = 1
    monkeypatch.setattr(
        insider,
        "parse_ownership_document",
        lambda xml, accession: [
            {
                "source": "SEC_EDGAR",
                "accession_number": accession,
                "source_row_hash": "a" * 64,
                "issuer_cik": "0000899689",
            }
        ],
    )
    monkeypatch.setattr(
        insider,
        "resolve_issuer_security_id",
        lambda db_manager, issuer_cik, fallback_security_id: 2,
    )

    status, written = insider.process_filing(filing, source, db)

    assert (status, written) == ("SUCCESS", 1)
    rows = db.upsert_insider_transactions.call_args.args[0]
    assert rows[0]["filing_id"] == 10
    assert rows[0]["security_id"] == 2
    assert rows[0]["filing_date"] == date(2026, 6, 1)


def test_process_filing_falls_back_for_empty_xml():
    filing = SimpleNamespace(primary_document_url="url")
    source = Mock()
    source.fetch_ownership_document.return_value = None
    db = Mock()

    assert insider.process_filing(filing, source, db) == ("SKIPPED_NO_XML", 0)
    db.upsert_insider_transactions.assert_not_called()
