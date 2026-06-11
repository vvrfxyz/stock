import json

from scripts.run_massive_shares_chunks import load_completed_chunks


def _write_jsonl(path, rows):
    path.write_text(
        "".join(json.dumps(row, sort_keys=True) + "\n" for row in rows),
        encoding="utf-8",
    )


def test_load_completed_chunks_uses_latest_completion_status(tmp_path):
    progress = tmp_path / "progress.jsonl"
    _write_jsonl(
        progress,
        [
            {"event": "chunk_completed", "chunk_key": "0001:a:b", "returncode": 0},
            {"event": "chunk_completed", "chunk_key": "0001:a:b", "returncode": 1},
        ],
    )

    assert load_completed_chunks(progress) == set()


def test_load_completed_chunks_honors_invalidated_event(tmp_path):
    progress = tmp_path / "progress.jsonl"
    _write_jsonl(
        progress,
        [
            {"event": "chunk_completed", "chunk_key": "0001:a:b", "returncode": 0},
            {"event": "chunk_invalidated", "chunk_key": "0001:a:b"},
        ],
    )

    assert load_completed_chunks(progress) == set()
